import asyncio
import logging
from typing import List, Optional

from broker.queue_manager import QueueManager
from broker.redis_client import RedisClient
from worker.heartbeat import Heartbeat
from worker.worker import Worker

logger = logging.getLogger(__name__)


class WorkerPool:
    """Manage a pool of asynchronous workers and their heartbeats."""

    def __init__(
        self,
        *,
        queue: QueueManager,
        redis: RedisClient,
        size: int = 5,
    ) -> None:
        """Initialize a worker pool with the given size."""
        self.queue = queue
        self.redis = redis
        self.size = max(1, size)

        self._workers: list[Worker] = []
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._heartbeat: Optional[Heartbeat] = None
        self._heartbeat_task: Optional[asyncio.Task[None]] = None
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        """Start N workers and a heartbeat supervisor."""
        logger.info("Starting worker pool with %s workers", self.size)

        # Ensure Redis connection is ready before starting workers.
        await self.redis.connect()

        # Create workers and schedule their start coroutines.
        for idx in range(self.size):
            worker_id = f"worker-{idx+1}"
            worker = Worker(worker_id=worker_id, queue=self.queue)
            self._workers.append(worker)
            task = asyncio.create_task(worker.start(), name=f"worker-{worker_id}")
            self._worker_tasks.append(task)

        # Single heartbeat for the pool identity.
        self._heartbeat = Heartbeat(worker_id="pool", redis_client=self.redis)
        self._heartbeat_task = asyncio.create_task(self._heartbeat.start(), name="worker-pool-heartbeat")
        await asyncio.gather(*self._worker_tasks)
        
    async def stop(self) -> None:
        """Signal workers to stop and wait for in-flight jobs to finish."""
        if self._stopping.is_set():
            return

        logger.info("Stopping worker pool")
        self._stopping.set()

        # Stop heartbeat first so monitoring can see shutdown.
        if self._heartbeat and self._heartbeat_task:
            await self._heartbeat.stop()
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Ask workers to stop gracefully.
        for worker in self._workers:
            await worker.stop()

        # Wait for all worker tasks to complete current jobs.
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)

        # Disconnect Redis last.
        await self.redis.disconnect()
        logger.info("Worker pool fully stopped")

    def status(self) -> int:
        """Return the number of active worker tasks."""
        return sum(1 for t in self._worker_tasks if not t.done())

