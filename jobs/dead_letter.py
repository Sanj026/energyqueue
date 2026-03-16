import logging
from datetime import datetime
from broker.redis_client import RedisClient

logger = logging.getLogger(__name__)

DEAD_LETTER_KEY = "queue:dead_letter"
MAX_RETRIES = 3


class DeadLetterQueue:
    """Stores jobs that have failed too many times to retry."""

    def __init__(self, redis_client: RedisClient):
        self.redis = redis_client

    async def send_to_dead_letter(self, job: dict, reason: str) -> None:
        """Move a failed job to the dead letter queue with failure metadata."""
        client = await self.redis.get_client()

        job["dead_letter_reason"] = reason
        job["dead_letter_at"] = datetime.utcnow().isoformat()
        job["status"] = "dead"

        import json
        await client.rpush(DEAD_LETTER_KEY, json.dumps(job))
        logger.warning("Job %s sent to dead letter queue. Reason: %s", job["id"], reason)

    async def should_retry(self, job: dict) -> bool:
        """Return True if job has retries remaining, False if it should die."""
        return job.get("retry_count", 0) < MAX_RETRIES

    async def increment_retry(self, job: dict) -> dict:
        """Bump retry count and calculate next retry delay using exponential backoff."""
        job["retry_count"] = job.get("retry_count", 0) + 1
        job["next_retry_delay"] = 2 ** job["retry_count"]
        logger.info(
            "Job %s retry %s/%s — next attempt in %ss",
            job["id"],
            job["retry_count"],
            MAX_RETRIES,
            job["next_retry_delay"],
        )
        return job

    async def get_dead_jobs(self) -> list[dict]:
        """Return all jobs sitting in the dead letter queue."""
        import json
        client = await self.redis.get_client()
        raw_jobs = await client.lrange(DEAD_LETTER_KEY, 0, -1)
        return [json.loads(j) for j in raw_jobs]

    async def dead_letter_count(self) -> int:
        """Return how many jobs are in the dead letter queue."""
        client = await self.redis.get_client()
        return await client.llen(DEAD_LETTER_KEY)