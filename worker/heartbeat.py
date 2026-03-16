import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 5
HEARTBEAT_TTL = 15
DEAD_THRESHOLD = 3


class Heartbeat:
    """Worker pings Redis every 5s so the supervisor knows it's alive."""

    def __init__(self, worker_id: str, redis_client):
        self.worker_id = worker_id
        self.redis = redis_client
        self.running = False

    async def start(self) -> None:
        """Begin sending heartbeats in a loop."""
        self.running = True
        logger.info("Heartbeat started for worker %s", self.worker_id)

        while self.running:
            await self._ping()
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def stop(self) -> None:
        """Stop sending heartbeats and remove this worker from Redis."""
        self.running = False
        client = await self.redis.get_client()
        await client.delete(self._key())
        logger.info("Heartbeat stopped for worker %s", self.worker_id)

    async def _ping(self) -> None:
        """Write a timestamp to Redis with a TTL of 15 seconds."""
        client = await self.redis.get_client()
        await client.setex(
            self._key(),
            HEARTBEAT_TTL,
            datetime.utcnow().isoformat(),
        )

    def _key(self) -> str:
        return f"heartbeat:{self.worker_id}"

    @staticmethod
    async def get_live_workers(redis_client) -> list[str]:
        """Return IDs of all workers that have pinged in the last 15 seconds."""
        client = await redis_client.get_client()
        keys = await client.keys("heartbeat:*")
        return [k.replace("heartbeat:", "") for k in keys]