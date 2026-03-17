import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional
from uuid import uuid4

from broker.redis_client import RedisClient

logger = logging.getLogger(__name__)


class Priority(Enum):
    """Priority lane for queued jobs."""

    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class QueueKeys:
    """Redis list keys for each priority lane."""

    HIGH = "queue:high"
    MEDIUM = "queue:medium"
    LOW = "queue:low"


@dataclass(frozen=True)
class EnqueuedJob:
    """Normalized representation of a job enqueued into Redis."""

    id: str
    type: str
    payload: dict[str, Any]
    priority: str


class QueueManager:
    """Redis-backed priority queue with three lanes (HIGH/MEDIUM/LOW)."""

    def __init__(self, redis_client: RedisClient) -> None:
        """Initialize the manager with a shared Redis client wrapper."""
        self.redis = redis_client

    async def enqueue(
        self,
        *,
        job_type: str,
        payload: dict[str, Any],
        priority: Priority = Priority.MEDIUM,
    ) -> str:
        """Enqueue a job and return its generated UUID."""
        job_id = str(uuid4())
        job: EnqueuedJob = EnqueuedJob(
            id=job_id,
            type=job_type,
            payload=payload,
            priority=priority.name,
        )

        key = self._key_for_priority(priority)
        client = await self.redis.get_client()
        await client.lpush(key, json.dumps(job.__dict__))
        logger.info("Enqueued job %s (%s) into %s", job_id, job_type, key)
        return job_id

    async def dequeue(self, priority: Priority) -> Optional[dict[str, Any]]:
        """Dequeue one job from a specific lane. Returns None if empty."""
        client = await self.redis.get_client()
        key = self._key_for_priority(priority)
        raw = await client.rpop(key)
        if not raw:
            return None
        return json.loads(raw)

    async def dequeue_any(self) -> Optional[dict[str, Any]]:
        """Dequeue from the highest available priority lane."""
        job = await self.dequeue(Priority.HIGH)
        if job is not None:
            return job
        job = await self.dequeue(Priority.MEDIUM)
        if job is not None:
            return job
        return await self.dequeue(Priority.LOW)

    async def queue_length(self, priority: Priority) -> int:
        """Return the number of jobs waiting in the given lane."""
        client = await self.redis.get_client()
        key = self._key_for_priority(priority)
        length = await client.llen(key)
        return int(length)

    @staticmethod
    def _key_for_priority(priority: Priority) -> str:
        """Map priority to Redis list key."""
        if priority == Priority.HIGH:
            return QueueKeys.HIGH
        if priority == Priority.MEDIUM:
            return QueueKeys.MEDIUM
        return QueueKeys.LOW