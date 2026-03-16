import asyncio
import logging
import time
from jobs.base_job import BaseJob, JobResult

logger = logging.getLogger(__name__)


class MLTrainingJob(BaseJob):
    """Simulates a PyTorch ML training job. Real implementation added in Week 3."""

    async def execute(self) -> JobResult:
        start = time.perf_counter()
        try:
            epochs = self.payload.get("epochs", 5)
            model = self.payload.get("model", "resnet18")

            logger.info("Training %s for %s epochs", model, epochs)

            for epoch in range(epochs):
                await asyncio.sleep(0.3)  # simulate one epoch
                logger.info("Epoch %s/%s complete", epoch + 1, epochs)

            duration = time.perf_counter() - start
            return JobResult(
                job_id=self.job_id,
                success=True,
                output={"epochs_completed": epochs, "model": model},
                duration_seconds=duration,
                energy_kwh=0.05 * epochs,
                co2_grams=20.0 * epochs,
            )
        except Exception as e:
            return JobResult(
                job_id=self.job_id,
                success=False,
                error=str(e),
                duration_seconds=time.perf_counter() - start,
            )

    def estimate_energy_cost(self) -> float:
        epochs = self.payload.get("epochs", 5)
        return 0.05 * epochs