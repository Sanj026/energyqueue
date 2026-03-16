import json
import logging
import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from broker.redis_client import RedisClient

logger = logging.getLogger(__name__)

class Priority(Enum):
    HIGH   = "queue:high"
    MEDIUM = "queue:medium"
    LOW    = "queue:low"


class QueueManager:
    """Handles enqueuing and dequeuing jobs across priority lanes."""

    def __init__(self, redis_client: RedisClient):
        self.redis = redis_client

    async def enqueue(
        self,
        job_type: str,
        payload: dict,
        priority: Priority = Priority.MEDIUM,
    ) -> str:
        """Serialise a job to JSON and push it onto the correct priority queue."""
        job = {
            "id": str(uuid.uuid4()),
            "type": job_type,
            "payload": payload,
            "status": "queued",
            "retry_count": 0,
            "created_at": datetime.utcnow().isoformat(),
        }

        client = await self.redis.get_client()
        await client.rpush(priority.value, json.dumps(job))

        logger.info("Enqueued job %s (type=%s, priority=%s)", job["id"], job_type, priority.name)
        return job["id"]

    async def dequeue(self, priority: Priority) -> Optional[dict]:
        """Pop the next job from the given priority queue. Returns None if empty."""
        client = await self.redis.get_client()
        raw = await client.lpop(priority.value)

        if raw is None:
            return None

        job = json.loads(raw)
        logger.info("Dequeued job %s (type=%s)", job["id"], job["type"])
        return job

    async def dequeue_any(self) -> Optional[dict]:
        """Try HIGH first, then MEDIUM, then LOW. Returns first job found."""
        for priority in Priority:
            job = await self.dequeue(priority)
            if job is not None:
                return job
        return None

    async def queue_length(self, priority: Priority) -> int:
        """Return how many jobs are waiting in a given priority lane."""
        client = await self.redis.get_client()
        return await client.llen(priority.value)