from __future__ import annotations

import asyncio
import json
from typing import Dict, Set, List
from agentos.messaging.dragonfly_bus import DragonflyBus
from agentos.messaging.events import Event, EventType


class TriggerEngine:
    """The event routing processor for AgentOS.

    Listens asynchronously to Dragonfly streams and wakes up agents 
    based on their topic registration and event subscription boundaries.
    """

    def __init__(self, bus: DragonflyBus):
        self.bus = bus
        # Maps event types to explicit agent roles/IDs (e.g., EventType.TASK_COMPLETED -> {"dev-test-1"})
        self.subscriptions: Dict[str, Set[str]] = {}
        self.is_running = False

    def register_subscription(self, event_type: EventType, agent_id: str) -> None:
        """Registers an agent's interest in a specific type of event."""
        if event_type not in self.subscriptions:
            self.subscriptions[event_type] = set()
        self.subscriptions[event_type].add(agent_id)

    async def start_routing_loop(self, project_id: str) -> None:
        """Starts the background listening daemon using Dragonfly consumer streams."""
        self.is_running = True
        stream_key = f"project:{project_id}:events"
        group_name = "trigger_engine_group"
        consumer_name = "main_engine_processor"

        # Initialize the Redis/Dragonfly Stream Consumer Group if it doesn't exist
        try:
            await self.bus.redis.xgroup_create(stream_key, group_name, mkstream=True)
        except Exception:
            pass  # Group already exists

        while self.is_running:
            try:
                # Read new unacknowledged events from the stream
                response = await self.bus.redis.xreadgroup(
                    groupname=group_name,
                    consumername=consumer_name,
                    streams={stream_key: ">"},
                    count=5,
                    block=1000
                )

                if not response:
                    await asyncio.sleep(0.1)
                    continue

                for _, items in response:
                    for message_id, fields in items:
                        raw_event = fields.get("event")
                        if not raw_event:
                            continue

                        event_dict = json.loads(raw_event)
                        event = Event(**event_dict)
                        
                        # Process the event and wake up subscribers
                        await self._route_event(event)
                        
                        # Acknowledge the stream processor that this item has been evaluated
                        await self.bus.redis.xack(stream_key, group_name, message_id)

            except asyncio.CancelledError:
                self.is_running = False
                break
            except Exception as e:
                print(f"Trigger Engine processing loop exception encountered: {e}")
                await asyncio.sleep(1.0)

    async def _route_event(self, event: Event) -> None:
        """Matches event headers against active agent inboxes."""
        subscribers = self.subscriptions.get(event.event_type, set())
        
        # If a target agent is explicitly named in the metadata envelope, isolate delivery
        if event.target_agent_id:
            subscribers = subscribers.intersection({event.target_agent_id})

        for agent_id in subscribers:
            inbox_key = f"agent:{agent_id}:inbox"
            # Append the event payload right into the agent's target coordination list
            await self.bus.redis.rpush(inbox_key, event.model_dump_json())
            
            # Wake up active subscribers via pub/sub notify channels
            await self.bus.redis.publish(f"agent:{agent_id}:wakeup", "NEW_EVENT")
            print(f"📡 [TRIGGER ENGINE]: Dispatched event {event.event_type} down to agent inbox: {agent_id}")

    def stop(self) -> None:
        """Gracefully halts the subscription dispatcher loop."""
        self.is_running = False