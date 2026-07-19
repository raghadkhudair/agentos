from __future__ import annotations

import asyncio
import json
import ray
import structlog
from typing import Dict, Set, List
from agentos.messaging.dragonfly_bus import DragonflyBus
from agentos.messaging.events import Event, EventType

logger = structlog.get_logger()


@ray.remote(namespace="agentos")
class TriggerEngineActor:

    def __init__(self, dragonfly_url: str):
        self.bus = DragonflyBus(dragonfly_url)
        self.subscriptions: Dict[str, Set[str]] = {}
        self._allowed_producers: Dict[str, List[str]] = {}
        self.is_running = False

    async def register_subscription(self, event_type: str, agent_id: str) -> None:
        if event_type not in self.subscriptions:
            self.subscriptions[event_type] = set()
        self.subscriptions[event_type].add(agent_id)
        logger.info("subscription_registered", event_type=event_type, agent_id=agent_id)

    async def register_allowed_producer(self, event_type: str, agent_id: str) -> None:
        if event_type not in self._allowed_producers:
            self._allowed_producers[event_type] = []
        self._allowed_producers[event_type].append(agent_id)

    async def start_routing_loop(self, project_id: str) -> None:
        self.is_running = True
        
        # Define all mandated architectural topic categories
        topics = [
            f"project.{project_id}.events",
            f"project.{project_id}.tasks",
            f"project.{project_id}.contracts",
            f"project.{project_id}.reviews",
            f"project.{project_id}.tests",
            f"project.{project_id}.blockers",
            f"project.{project_id}.checkpoints",
            f"project.{project_id}.summaries",
            "squad.backend.events",
            "squad.frontend.events",
            "squad.platform.events",
            "squad.qa.events"
        ]
        
        group_name = "trigger_engine_group"
        consumer_name = f"engine_processor_{ray.get_runtime_context().get_actor_id()}"

        # Safe multi-stream group registration
        for topic in topics:
            try:
                await self.bus.redis.xgroup_create(topic, group_name, mkstream=True)
            except Exception:
                pass

        logger.info("trigger_engine_multi_topic_loop_activated", project_id=project_id, active_topics=topics)

        while self.is_running:
            try:
                # Read across all registered streams simultaneously using XREADGROUP
                # Passing ">" reads only new messages for our group
                streams_to_read = {topic: ">" for topic in topics}
                
                response = await self.bus.redis.xreadgroup(
                    groupname=group_name,
                    consumername=consumer_name,
                    streams=streams_to_read,
                    count=5,
                    block=1000
                )

                if not response:
                    await asyncio.sleep(0.1)
                    continue

                for stream_key, items in response:
                    for message_id, fields in items:
                        raw_event = fields.get("event")
                        if not raw_event:
                            continue

                        event_dict = json.loads(raw_event)
                        event = Event(**event_dict)
                        
                        await self._route_event(event)
                        
                        # Acknowledge the message was safely processed on this specific stream
                        await self.bus.redis.xack(stream_key, group_name, message_id)

            except asyncio.CancelledError:
                self.is_running = False
                break
            except Exception as e:
                logger.error("trigger_engine_loop_error", error=str(e))
                await asyncio.sleep(1.0)

    async def _route_event(self, event: Event) -> None:

        event_type_str = event.event_type.value if hasattr(event.event_type, "value") else str(event.event_type)
        if event_type_str != "PROJECT_CREATED":
            allowed = self._allowed_producers.get(event_type_str, [])
            if event.producer_agent_id not in allowed:
                logger.critical("unauthorized_communication_attempt_dropped", producer_agent_id=event.producer_agent_id, event_type=event_type_str)
                return
        if event_type_str == "SECURITY_ALERT" and "developer" in (event.producer_agent_id or "").lower():
            logger.critical("privilege_escalation_interception", producer=event.producer_agent_id, event=event_type_str)
            # Force route to security reviewer instead of intended targets
            event.target_agent_id = "security_reviewer-1"
        subscribers = set(self.subscriptions.get(event_type_str, set()))
        
        affected_artifact = event.payload.get("artifact_uri") or event.payload.get("file_path")
        if affected_artifact:
            logger.info("evaluating_downstream_artifact_consumers", path=affected_artifact)
          
        if event.target_agent_id:
            subscribers = subscribers.intersection({event.target_agent_id})
        elif event_type_str == "TASK_COMPLETED":
            review_subs = set(self.subscriptions.get("REVIEW_REQUEST", set()))
            if review_subs:
                subscribers = subscribers.union(review_subs)
                logger.info("agent_handoff_opportunity_detected", routing_to=list(review_subs))

        interrupt_level = "NORMAL"
        if event_type_str in {"SECURITY_ALERT", "BLOCKER_CREATED", "BLOCKER"}:
            interrupt_level = "CRITICAL_INTERRUPT"
            logger.warning("high_priority_interrupt_detected", event_type=event_type_str)

        for agent_id in subscribers:
            inbox_key = f"agent:{agent_id}:inbox"
            busy_lock_key = f"agent:{agent_id}:processing_lease"

            is_busy = await self.bus.redis.exists(busy_lock_key)
            
            await self.bus.redis.rpush(inbox_key, event.model_dump_json())

            if is_busy and interrupt_level == "NORMAL":
                logger.info("skipping_unnecessary_wakeup_agent_busy", agent_id=agent_id)
                continue

            wakeup_payload = {
                "signal": "NEW_EVENT",
                "interrupt_level": interrupt_level,
                "trigger_catchup": "TRUE" if not is_busy else "FALSE"
            }
            await self.bus.redis.publish(f"agent:{agent_id}:wakeup", json.dumps(wakeup_payload))
            logger.info("📡 event_dispatched_to_inbox", agent_id=agent_id, event_type=event_type_str)

    def stop(self) -> None:
        self.is_running = False