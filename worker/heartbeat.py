import asyncio
import json
import logging
from datetime import datetime

from broker.queue_manager import Priority, QueueManager

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 5
HEARTBEAT_TTL = 15
DEAD_THRESHOLD = 3
SUPERVISOR_INTERVAL = 15
WORKER_JOB_KEY_PREFIX = "worker_job:"
WORKER_JOB_DATA_KEY_PREFIX = "worker_job_data:"


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


async def _supervisor_loop(redis_client, queue_manager: QueueManager) -> None:
    """Scan heartbeat keys and recover jobs from dead workers."""
    while True:
        await _scan_for_dead_workers(redis_client, queue_manager)
        await asyncio.sleep(SUPERVISOR_INTERVAL)


async def _scan_for_dead_workers(redis_client, queue_manager: QueueManager) -> None:
    """Detect dead workers and recover in-flight jobs."""
    client = await redis_client.get_client()
    heartbeat_keys = await client.keys("heartbeat:*")
    now = datetime.utcnow()

    for heartbeat_key in heartbeat_keys:
        raw_timestamp = await client.get(heartbeat_key)
        if not raw_timestamp:
            continue

        try:
            last_seen = datetime.fromisoformat(raw_timestamp)
        except ValueError:
            logger.warning("Invalid heartbeat timestamp for key %s", heartbeat_key)
            continue

        worker_id = heartbeat_key.replace("heartbeat:", "")
        age_seconds = (now - last_seen).total_seconds()
        if age_seconds <= HEARTBEAT_TTL:
            continue

        logger.warning("Worker %s declared dead (last heartbeat %.1fs ago)", worker_id, age_seconds)
        await _recover_worker_job(client, queue_manager, worker_id)
        await client.delete(heartbeat_key)


async def _recover_worker_job(client, queue_manager: QueueManager, worker_id: str) -> None:
    """Re-enqueue an in-flight job for a dead worker, if present."""
    worker_job_key = f"{WORKER_JOB_KEY_PREFIX}{worker_id}"
    worker_job_data_key = f"{WORKER_JOB_DATA_KEY_PREFIX}{worker_id}"

    job_id = await client.get(worker_job_key)
    if not job_id:
        return

    raw_job_data = await client.get(worker_job_data_key)
    if not raw_job_data:
        logger.warning("No in-flight payload found for dead worker %s job %s", worker_id, job_id)
        await client.delete(worker_job_key)
        await client.delete(worker_job_data_key)
        return

    try:
        job_data = json.loads(raw_job_data)
    except (TypeError, ValueError):
        logger.warning("Corrupt in-flight payload for dead worker %s", worker_id)
        await client.delete(worker_job_key)
        await client.delete(worker_job_data_key)
        return

    priority_name = job_data.get("priority", Priority.MEDIUM.name)
    try:
        priority = Priority[priority_name]
    except KeyError:
        priority = Priority.MEDIUM

    queue_key = QueueManager._key_for_priority(priority)
    await client.lpush(queue_key, raw_job_data)
    logger.warning("Recovered job %s from dead worker %s into %s", job_id, worker_id, queue_key)

    await client.delete(worker_job_key)
    await client.delete(worker_job_data_key)


async def start_supervisor(redis_client, queue_manager: QueueManager) -> asyncio.Task[None]:
    """Start the dead-worker supervisor as a background task."""
    return asyncio.create_task(
        _supervisor_loop(redis_client, queue_manager),
        name="heartbeat-supervisor",
    )