import logging
import time
import asyncio
from jobs.base_job import BaseJob, JobResult

logger = logging.getLogger(__name__)


class ImageResizeJob(BaseJob):
    """A lightweight job that simulates resizing an image."""

    async def execute(self) -> JobResult:
        start = time.perf_counter()
        try:
            width = self.payload.get("width", 800)
            height = self.payload.get("height", 600)

            logger.info("Resizing image to %sx%s", width, height)
            await asyncio.sleep(0.5)  # simulate work

            duration = time.perf_counter() - start
            return JobResult(
                job_id=self.job_id,
                success=True,
                output={"width": width, "height": height},
                duration_seconds=duration,
                energy_kwh=0.0001,
                co2_grams=0.05,
            )
        except Exception as e:
            return JobResult(
                job_id=self.job_id,
                success=False,
                error=str(e),
                duration_seconds=time.perf_counter() - start,
            )

    def estimate_energy_cost(self) -> float:
        return 0.0001