import os
import logging
import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


class RedisClient:
    """Manages the connection pool to Redis for the entire application."""

    def __init__(self):
        self.client = None

    async def connect(self) -> None:
        """Create the Redis connection pool and verify it with a ping."""
        self.client = await redis.from_url(
            REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
        )
        await self.client.ping()
        logger.info("Connected to Redis at %s", REDIS_URL)

    async def disconnect(self) -> None:
        """Gracefully close the Redis connection."""
        if self.client:
            await self.client.aclose()
            logger.info("Disconnected from Redis")

    async def get_client(self) -> redis.Redis:
        """Return the active Redis client. Raises if not connected."""
        if not self.client:
            raise ConnectionError("Redis client is not connected. Call connect() first.")
        return self.client