import asyncio
import logging
from broker.queue_manager import QueueManager, Priority
from broker.redis_client import RedisClient
from jobs.base_job import BaseJob, JobResult
from jobs.ml_training_job import MLTrainingJob
from jobs.image_resize_job import ImageResizeJob

logger = logging.getLogger(__name__)

JOB_REGISTRY = {
    "ml_training": MLTrainingJob,
    "image_resize": ImageResizeJob,
}


class Worker:
    """Pulls jobs from the queue and executes them one at a time."""

    def __init__(self, worker_id: str, queue: QueueManager):
        self.worker_id = worker_id
        self.queue = queue
        self.running = False

    async def start(self) -> None:
        """Start the worker loop — keeps polling until stopped."""
        self.running = True
        logger.info("Worker %s started", self.worker_id)

        while self.running:
            job_data = await self.queue.dequeue_any()

            if job_data is None:
                await asyncio.sleep(1)
                continue

            await self._execute_job(job_data)

    async def stop(self) -> None:
        """Gracefully stop the worker after current job finishes."""
        self.running = False
        logger.info("Worker %s stopping", self.worker_id)

    async def _execute_job(self, job_data: dict) -> None:
        """Look up the job type, build the job object, run it."""
        job_type = job_data.get("type")
        job_class = JOB_REGISTRY.get(job_type)

        if job_class is None:
            logger.error("Unknown job type: %s", job_type)
            return

        job: BaseJob = job_class(
            job_id=job_data["id"],
            payload=job_data["payload"],
        )

        logger.info("Worker %s executing job %s", self.worker_id, job.job_id)
        result: JobResult = await job.execute()

        if result.success:
            logger.info("Job %s completed in %.2fs", job.job_id, result.duration_seconds)
        else:
            logger.error("Job %s failed: %s", job.job_id, result.error)