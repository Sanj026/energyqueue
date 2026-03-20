import asyncio
import logging
import time

from jobs.base_job import BaseJob, JobResult

logger = logging.getLogger(__name__)


class InferenceJob(BaseJob):
    """Run a mock ML inference forward pass."""

    async def execute(self) -> JobResult:
        """Simulate inference latency and return mock predictions."""
        start = time.perf_counter()
        try:
            model_name = str(self.payload.get("model_name", "mock-model"))
            batch_size = int(self.payload.get("batch_size", 1))
            input_size = int(self.payload.get("input_size", 224))

            simulated_seconds = batch_size * 0.01
            await asyncio.sleep(simulated_seconds)

            duration_seconds = time.perf_counter() - start
            latency_ms = duration_seconds * 1000.0

            logger.info(
                "InferenceJob completed: model=%s batch_size=%s input_size=%s latency_ms=%.2f",
                model_name,
                batch_size,
                input_size,
                latency_ms,
            )

            return JobResult(
                job_id=self.job_id,
                success=True,
                output={
                    "predictions": [],
                    "latency_ms": latency_ms,
                    "model_name": model_name,
                    "batch_size": batch_size,
                    "input_size": input_size,
                },
                duration_seconds=duration_seconds,
                energy_kwh=self.estimate_energy_cost(),
                co2_grams=0.0,
            )
        except Exception as exc:
            logger.exception("InferenceJob failed: %s", str(exc))
            return JobResult(
                job_id=self.job_id,
                success=False,
                error=str(exc),
                duration_seconds=time.perf_counter() - start,
            )

    def estimate_energy_cost(self) -> float:
        """Return estimated inference energy cost in kWh."""
        batch_size = int(self.payload.get("batch_size", 1))
        return 0.001 * batch_size
