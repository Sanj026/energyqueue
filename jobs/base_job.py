import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class JobResult:
    """Holds the outcome of a completed job execution."""
    job_id: str
    success: bool
    output: Any = None
    error: str = None
    duration_seconds: float = 0.0
    energy_kwh: float = 0.0
    co2_grams: float = 0.0


class BaseJob(ABC):
    """Abstract base class every job type must inherit from."""

    def __init__(self, job_id: str, payload: dict):
        self.job_id = job_id
        self.payload = payload
        self.created_at = datetime.utcnow().isoformat()

    @abstractmethod
    async def execute(self) -> JobResult:
        """Run the job. Every subclass MUST implement this."""
        ...

    @abstractmethod
    def estimate_energy_cost(self) -> float:
        """Return estimated kWh this job will consume. Used by scheduler."""
        ...

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} id={self.job_id}>"