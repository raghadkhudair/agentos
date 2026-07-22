from __future__ import annotations

import math
import os
from collections.abc import Iterable
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import psutil
from pydantic import BaseModel, Field, model_validator

from agentos.config.settings import Settings


class TaskComplexity(StrEnum):
    LOW = "low"
    STANDARD = "standard"
    HIGH = "high"
    CRITICAL = "critical"


class AgentResourceAllocation(BaseModel):
    agent_id: str
    role: str
    cpu_cores: float = Field(gt=0)
    memory_bytes: int = Field(gt=0)
    max_concurrency: int = Field(ge=1)
    provider: str
    model: str
    model_routes: dict[TaskComplexity, str] = Field(default_factory=dict)
    complexity: TaskComplexity = TaskComplexity.STANDARD


class ResourceEnvelope(BaseModel):
    detected_cpu_cores: int = Field(ge=1)
    allocated_cpu_cores: int = Field(ge=1)
    reserved_cpu_cores: int = Field(ge=0)
    detected_memory_bytes: int = Field(gt=0)
    allocated_memory_bytes: int = Field(gt=0)
    reserved_memory_bytes: int = Field(ge=0)
    object_store_memory_bytes: int = Field(gt=0)
    system_cpu_cores: float = Field(gt=0)
    system_memory_bytes: int = Field(gt=0)
    max_active_agents: int = Field(ge=1)

    @model_validator(mode="after")
    def _host_headroom_is_real(self) -> ResourceEnvelope:
        if self.detected_cpu_cores > 1 and self.allocated_cpu_cores >= self.detected_cpu_cores:
            raise ValueError("AgentOS must leave at least one detected CPU core unallocated")
        if self.system_cpu_cores >= self.allocated_cpu_cores:
            raise ValueError("system actors consume the complete CPU envelope")
        if self.allocated_memory_bytes + self.reserved_memory_bytes > self.detected_memory_bytes:
            raise ValueError("allocated and reserved memory exceed detected host memory")
        if self.system_memory_bytes + self.object_store_memory_bytes >= self.allocated_memory_bytes:
            raise ValueError("system actors and the Ray object store leave no worker memory")
        return self


class RuntimeConfig(BaseModel):
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    environment: str
    envelope: ResourceEnvelope
    allocations: list[AgentResourceAllocation] = Field(default_factory=list)
    thread_environment: dict[str, str]

    @property
    def allocated_agent_cpu(self) -> float:
        return sum(item.cpu_cores for item in self.allocations)

    @model_validator(mode="after")
    def _allocations_fit_envelope(self) -> RuntimeConfig:
        agent_ids = [item.agent_id for item in self.allocations]
        if len(agent_ids) != len(set(agent_ids)):
            raise ValueError("runtime allocations must have unique agent IDs")
        if (
            self.allocated_agent_cpu + self.envelope.system_cpu_cores
            > self.envelope.allocated_cpu_cores + 1e-6
        ):
            raise ValueError("agent CPU allocations exceed the runtime envelope")
        agent_memory = sum(item.memory_bytes for item in self.allocations)
        if (
            agent_memory
            + self.envelope.object_store_memory_bytes
            + self.envelope.system_memory_bytes
            > self.envelope.allocated_memory_bytes
        ):
            raise ValueError(
                "agent and object-store memory allocations exceed the runtime envelope"
            )
        return self


class ResourcePlanner:
    """Deterministic resource planner used by the infrastructure agent.

    Ray's CPU resources are admission-control quantities rather than hard CPU affinity.
    The planner therefore also produces thread-limit environment variables and the
    deployment applies container CPU limits.
    """

    THREAD_ENV_KEYS = (
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
    )

    ROLE_COMPLEXITY: dict[str, TaskComplexity] = {
        "pm_tech_lead": TaskComplexity.HIGH,
        "solution_architect": TaskComplexity.HIGH,
        "backend_developer": TaskComplexity.STANDARD,
        "frontend_developer": TaskComplexity.STANDARD,
        "platform_engineer": TaskComplexity.HIGH,
        "qa_engineer": TaskComplexity.STANDARD,
        "code_reviewer": TaskComplexity.HIGH,
        "security_reviewer": TaskComplexity.CRITICAL,
        "infrastructure_agent": TaskComplexity.HIGH,
    }

    def __init__(self, settings: Settings):
        self.settings = settings

    @staticmethod
    def _read_cgroup_value(path: str) -> str | None:
        try:
            value = Path(path).read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError):
            return None
        return value or None

    @staticmethod
    def _cpuset_count(value: str | None) -> int | None:
        if not value:
            return None
        cpus: set[int] = set()
        try:
            for item in value.split(","):
                bounds = item.strip().split("-", 1)
                start = int(bounds[0])
                end = int(bounds[-1])
                if end < start:
                    return None
                cpus.update(range(start, end + 1))
        except ValueError:
            return None
        return len(cpus) or None

    @classmethod
    def effective_cpu_count(cls) -> int:
        """Return the CPU capacity visible through affinity and cgroup ceilings."""

        candidates = [max(1, os.cpu_count() or 1)]
        try:
            affinity = psutil.Process().cpu_affinity()
            if affinity:
                candidates.append(len(affinity))
        except (AttributeError, NotImplementedError, OSError, psutil.Error):
            pass

        cpuset = cls._read_cgroup_value("/sys/fs/cgroup/cpuset.cpus.effective")
        cpuset_count = cls._cpuset_count(cpuset)
        if cpuset_count:
            candidates.append(cpuset_count)

        cpu_max = cls._read_cgroup_value("/sys/fs/cgroup/cpu.max")
        if cpu_max:
            parts = cpu_max.split()
            if len(parts) == 2 and parts[0] != "max":
                try:
                    quota_value = int(parts[0])
                    period_value = int(parts[1])
                    if quota_value > 0 and period_value > 0:
                        candidates.append(max(1, math.floor(quota_value / period_value)))
                except ValueError:
                    pass
        else:
            quota_text = cls._read_cgroup_value("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
            period_text = cls._read_cgroup_value("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
            try:
                if quota_text and period_text and int(quota_text) > 0 and int(period_text) > 0:
                    candidates.append(max(1, math.floor(int(quota_text) / int(period_text))))
            except ValueError:
                pass
        return max(1, min(candidates))

    @classmethod
    def effective_memory_bytes(cls) -> int:
        """Return host memory intersected with the active cgroup limit."""

        detected = max(536_870_912, int(psutil.virtual_memory().total))
        candidates = [detected]
        for path in (
            "/sys/fs/cgroup/memory.max",
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        ):
            raw = cls._read_cgroup_value(path)
            if not raw or raw == "max":
                continue
            try:
                limit = int(raw)
            except ValueError:
                continue
            if 536_870_912 <= limit < 2**60:
                candidates.append(limit)
        return max(536_870_912, min(candidates))

    def supervisor_cpu(self, envelope: ResourceEnvelope | None = None) -> float:
        selected = envelope or self.build_envelope()
        return max(0.01, min(self.settings.system_actor_cpu, selected.system_cpu_cores * 0.20))

    def service_cpu(self, service_count: int, envelope: ResourceEnvelope | None = None) -> float:
        if service_count < 1:
            raise ValueError("service_count must be positive")
        selected = envelope or self.build_envelope()
        available = selected.system_cpu_cores - self.supervisor_cpu(selected)
        return max(0.001, available / service_count)

    def thread_environment(self) -> dict[str, str]:
        threads = min(
            self.settings.max_threads_per_agent,
            max(1, math.floor(self.settings.worker_cpu)),
        )
        return {key: str(threads) for key in self.THREAD_ENV_KEYS}

    def build_envelope(self) -> ResourceEnvelope:
        detected_cpu = self.effective_cpu_count()
        fraction_cpu = max(1, math.floor(detected_cpu * self.settings.cpu_usage_fraction))
        headroom_cpu = max(1, detected_cpu - self.settings.reserved_cpu_cores)
        configured_cpu = self.settings.max_cpu_cores or detected_cpu
        allocated_cpu = min(fraction_cpu, headroom_cpu, configured_cpu)
        if detected_cpu > 1:
            allocated_cpu = min(allocated_cpu, detected_cpu - 1)

        detected_memory = self.effective_memory_bytes()
        fraction_memory = int(detected_memory * self.settings.memory_usage_fraction)
        headroom_memory = max(268_435_456, detected_memory - self.settings.reserved_memory_bytes)
        configured_memory = self.settings.max_memory_bytes or detected_memory
        allocated_memory = min(fraction_memory, headroom_memory, configured_memory)

        object_store_memory = min(
            self.settings.object_store_memory_bytes,
            max(78_643_200, allocated_memory // 3),
        )
        system_cpu = min(
            max(0.10, allocated_cpu * 0.30),
            max(0.10, allocated_cpu - 0.10),
        )
        system_memory = min(
            max(67_108_864, allocated_memory // 8),
            max(67_108_864, allocated_memory // 3),
        )
        cpu_for_workers = allocated_cpu - system_cpu
        memory_for_workers = allocated_memory - object_store_memory - system_memory
        cpu_slots = math.floor(cpu_for_workers / self.settings.worker_cpu)
        memory_slots = memory_for_workers // self.settings.worker_memory_bytes
        if cpu_slots < 1 or memory_slots < 1:
            raise ValueError("host capacity cannot safely fit one worker plus AgentOS services")
        active_agents = min(self.settings.max_active_agents, cpu_slots, memory_slots)

        return ResourceEnvelope(
            detected_cpu_cores=detected_cpu,
            allocated_cpu_cores=allocated_cpu,
            reserved_cpu_cores=detected_cpu - allocated_cpu,
            detected_memory_bytes=detected_memory,
            allocated_memory_bytes=allocated_memory,
            reserved_memory_bytes=detected_memory - allocated_memory,
            object_store_memory_bytes=object_store_memory,
            system_cpu_cores=system_cpu,
            system_memory_bytes=system_memory,
            max_active_agents=active_agents,
        )

    def build_runtime_config(
        self,
        agents: Iterable[tuple[str, str]],
        *,
        provider_assignments: dict[str, tuple[str, str]] | None = None,
    ) -> RuntimeConfig:
        envelope = self.build_envelope()
        provider_assignments = provider_assignments or {}
        agent_list = list(agents)
        if len(agent_list) > self.settings.max_agents_total:
            raise ValueError("requested team exceeds AGENTOS_MAX_AGENTS_TOTAL")
        worker_cpu_budget = envelope.allocated_cpu_cores - envelope.system_cpu_cores
        if not agent_list:
            raise ValueError("resource planning requires at least one agent")
        per_agent_cpu = min(
            self.settings.worker_cpu,
            worker_cpu_budget / max(1, len(agent_list)),
        )
        worker_memory_budget = (
            envelope.allocated_memory_bytes
            - envelope.object_store_memory_bytes
            - envelope.system_memory_bytes
        )
        per_agent_memory = min(
            self.settings.worker_memory_bytes,
            worker_memory_budget // max(1, len(agent_list)),
        )
        if per_agent_memory < 134_217_728:
            raise ValueError("host memory cannot safely fit the requested independent agent team")
        allocations: list[AgentResourceAllocation] = []
        for agent_id, role in agent_list:
            provider, model = provider_assignments.get(
                agent_id, (self.settings.provider_default, "provider-default")
            )
            allocations.append(
                AgentResourceAllocation(
                    agent_id=agent_id,
                    role=role,
                    cpu_cores=max(0.001, round(per_agent_cpu, 4)),
                    memory_bytes=int(per_agent_memory),
                    max_concurrency=self.settings.max_threads_per_agent,
                    provider=provider,
                    model=model,
                    complexity=self.ROLE_COMPLEXITY.get(role, TaskComplexity.STANDARD),
                )
            )

        thread_environment = self.thread_environment()
        return RuntimeConfig(
            environment=self.settings.environment,
            envelope=envelope,
            allocations=allocations,
            thread_environment=thread_environment,
        )

    @staticmethod
    def apply_thread_limits(config: RuntimeConfig) -> None:
        for key, value in config.thread_environment.items():
            os.environ[key] = value
