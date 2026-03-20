import asyncio
import logging
import json

from broker.queue_manager import QueueManager, Priority
from broker.redis_client import RedisClient
from jobs.base_job import BaseJob, JobResult
from jobs.ml_training_job import MLTrainingJob
from jobs.image_resize_job import ImageResizeJob
from jobs.inference_job import InferenceJob
from scheduler.scheduler import Scheduler, SchedulingDecision
from scheduler.carbon_client import CarbonClient
from scheduler.energy_budget import EnergyBudget
from worker.energy_profiler import EnergyProfiler

logger = logging.getLogger(__name__)
LOCK_TTL = 30 
WORKER_JOB_TTL_SECONDS = 60
JOB_REGISTRY = {
    "ml_training": MLTrainingJob,
    "image_resize": ImageResizeJob,
    "inference": InferenceJob,
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

    async def _re_enqueue_job(self, job_data: dict, delay_seconds: int = 0) -> None:
        """Re-enqueue a job into its priority lane, optionally after a delay."""
        if delay_seconds > 0:
            asyncio.create_task(self._re_enqueue_after_delay(job_data, delay_seconds))
            logger.info(
                "Job %s scheduled for re-enqueue in %ss",
                job_data.get("id"),
                delay_seconds,
            )
            return

        await self._enqueue_job_data(job_data)

    async def _re_enqueue_after_delay(self, job_data: dict, delay_seconds: int) -> None:
        """Wait before re-enqueuing a deferred job."""
        await asyncio.sleep(delay_seconds)
        await self._enqueue_job_data(job_data)

    async def _enqueue_job_data(self, job_data: dict) -> None:
        """Push raw job data back to the queue lane derived from priority."""
        priority_name = job_data.get("priority", Priority.MEDIUM.name)
        try:
            priority = Priority[priority_name]
        except KeyError:
            priority = Priority.MEDIUM

        client = await self.queue.redis.get_client()
        key = QueueManager._key_for_priority(priority)
        await client.lpush(key, json.dumps(job_data))
        logger.info("Re-enqueued job %s into %s", job_data.get("id"), key)

    
    async def _execute_job(self, job_data: dict) -> None:
        """Acquire a distributed lock, execute the job, release the lock."""
        job_id = job_data.get("id")
        lock_key = f"lock:{job_id}"
        worker_job_key = f"worker_job:{self.worker_id}"
        worker_job_data_key = f"worker_job_data:{self.worker_id}"

        client = await self.queue.redis.get_client()

        # Try to acquire the lock atomically
        acquired = await client.set(
            lock_key,
            self.worker_id,
            nx=True,
            ex=LOCK_TTL,
        )

        if not acquired:
            logger.warning("Job %s already locked by another worker — skipping", job_id)
            return

        try:
            job_type = job_data.get("type")
            job_class = JOB_REGISTRY.get(job_type)
            job_priority = job_data.get("priority", Priority.MEDIUM.name)

            if job_class is None:
                logger.error("Unknown job type: %s", job_type)
                return

            scheduler = Scheduler(
                queue=self.queue,
                carbon_client=CarbonClient(redis_client=self.queue.redis),
                energy_budget=EnergyBudget(redis_client=self.queue.redis),
            )
            decision = await scheduler.decide(job_data)

            if decision == SchedulingDecision.STOP:
                logger.info("Decision STOP for %s, re-enqueuing", job_id)
                await self._re_enqueue_job(job_data)
                return

            if decision == SchedulingDecision.DEFER:
                if job_priority == Priority.LOW.name:
                    logger.info("Decision DEFER for LOW job %s, delaying 5 minutes", job_id)
                    await self._re_enqueue_job(job_data, delay_seconds=300)
                else:
                    logger.info("Decision DEFER for %s job %s, re-enqueuing", job_priority, job_id)
                    await self._re_enqueue_job(job_data)
                return

            can_execute_now = (
                decision == SchedulingDecision.RUN
                or (
                    decision == SchedulingDecision.THROTTLE
                    and job_priority in {Priority.HIGH.name, Priority.MEDIUM.name}
                )
            )
            if not can_execute_now:
                logger.info(
                    "Decision %s for %s priority job %s, re-enqueuing",
                    decision,
                    job_priority,
                    job_id,
                )
                await self._re_enqueue_job(job_data)
                return

            job: BaseJob = job_class(
                job_id=job_data["id"],
                payload=job_data["payload"],
            )

            await client.set(worker_job_key, job_id, ex=WORKER_JOB_TTL_SECONDS)
            await client.set(worker_job_data_key, json.dumps(job_data), ex=WORKER_JOB_TTL_SECONDS)
            logger.info("Worker %s executing job %s", self.worker_id, job.job_id)
            result: JobResult = await job.execute()

            # Track energy consumption
            budget = EnergyBudget(redis_client=self.queue.redis)
            await budget.add_consumption(result.co2_grams or 0.0)
        
            # Store energy data in Redis
            await client.rpush("jobs:completed", json.dumps({
                "job_id": job.job_id,
                "type": job_data.get("type"),
                "duration": result.duration_seconds,
                "co2_grams": result.co2_grams or 0.0,
                "energy_kwh": result.energy_kwh or 0.0,
            }))
            await client.set(
                f"energy:{job.job_id}",
                json.dumps({"co2_grams": result.co2_grams or 0.0, 
                            "energy_kwh": result.energy_kwh or 0.0})
            )

            if result.success:
                logger.info("Job %s completed in %.2fs", job.job_id, result.duration_seconds)
                
            else:
                logger.error("Job %s failed: %s", job.job_id, result.error)

        finally:
            await client.delete(lock_key)
            await client.delete(worker_job_key)
            await client.delete(worker_job_data_key)
            logger.info("Lock released for job %s", job_id)