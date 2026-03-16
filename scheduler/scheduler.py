import logging
import os
from broker.queue_manager import QueueManager, Priority
from broker.redis_client import RedisClient
from scheduler.carbon_client import CarbonClient
from scheduler.energy_budget import EnergyBudget

logger = logging.getLogger(__name__)

CARBON_THRESHOLD = float(os.getenv("CARBON_THRESHOLD", 200))
GRID_ZONE = os.getenv("GRID_ZONE", "GB")


class SchedulingDecision:
    RUN = "RUN"
    DEFER = "DEFER"
    THROTTLE = "THROTTLE"
    STOP = "STOP"


class Scheduler:
    """The brain — decides whether to run, defer, or throttle jobs."""

    def __init__(
        self,
        queue: QueueManager,
        carbon_client: CarbonClient,
        energy_budget: EnergyBudget,
    ):
        self.queue = queue
        self.carbon = carbon_client
        self.budget = energy_budget

    async def decide(self, job: dict) -> str:
        """Return a SchedulingDecision for the given job."""

        # Check 1 — is the budget exhausted?
        if await self.budget.is_exhausted():
            logger.warning("Energy budget exhausted — stopping queue")
            return SchedulingDecision.STOP

        # Check 2 — what is the current carbon intensity?
        intensity = await self.carbon.get_carbon_intensity(GRID_ZONE)

        # Check 3 — is it a high priority job?
        job_priority = job.get("priority", Priority.MEDIUM.name)

        if intensity > CARBON_THRESHOLD * 1.5:
            # Grid is very dirty — only run HIGH priority
            if job_priority == Priority.HIGH.name:
                logger.info("Grid very dirty (%.0fg) but job is HIGH priority — running", intensity)
                return SchedulingDecision.RUN
            logger.info("Grid very dirty (%.0fg) — deferring %s job", intensity, job_priority)
            return SchedulingDecision.DEFER

        elif intensity > CARBON_THRESHOLD:
            # Grid is dirty — run HIGH and MEDIUM, defer LOW
            if job_priority == Priority.LOW.name:
                logger.info("Grid dirty (%.0fg) — deferring LOW priority job", intensity)
                return SchedulingDecision.DEFER
            return SchedulingDecision.THROTTLE

        else:
            # Grid is clean — run everything
            logger.info("Grid clean (%.0fg) — running job", intensity)
            return SchedulingDecision.RUN

    async def should_flush_deferred(self) -> bool:
        """Return True if conditions are good enough to run deferred jobs."""
        intensity = await self.carbon.get_carbon_intensity(GRID_ZONE)
        budget_ok = not await self.budget.is_exhausted()
        return intensity <= CARBON_THRESHOLD and budget_ok