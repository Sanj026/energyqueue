import asyncio
import logging
from broker.redis_client import RedisClient
from broker.queue_manager import QueueManager, Priority

logger = logging.getLogger(__name__)


class EnergyQueue:
    """Clean public SDK for submitting jobs to EnergyQueue."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self._redis = RedisClient()
        self._queue = None

    async def connect(self) -> None:
        """Connect to Redis and initialise the queue manager."""
        await self._redis.connect()
        self._queue = QueueManager(self._redis)
        logger.info("EnergyQueue SDK connected")

    async def disconnect(self) -> None:
        """Cleanly close the Redis connection."""
        await self._redis.disconnect()

    async def submit(
        self,
        job_type: str,
        payload: dict,
        priority: str = "medium",
    ) -> str:
        """Submit a job. Returns the job ID.
        
        Usage:
            eq = EnergyQueue()
            await eq.connect()
            job_id = await eq.submit("ml_training", {"epochs": 5}, priority="high")
        """
        priority_map = {
            "high": Priority.HIGH,
            "medium": Priority.MEDIUM,
            "low": Priority.LOW,
        }
        p = priority_map.get(priority.lower(), Priority.MEDIUM)
        return await self._queue.enqueue(job_type, payload, p)

    async def queue_depths(self) -> dict:
        """Return how many jobs are waiting in each priority lane."""
        return {
            "high": await self._queue.queue_length(Priority.HIGH),
            "medium": await self._queue.queue_length(Priority.MEDIUM),
            "low": await self._queue.queue_length(Priority.LOW),
        }

    async def __aenter__(self):
        """Allow usage as async context manager."""
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.disconnect()