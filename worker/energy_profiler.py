import json
import logging
import time
from typing import Any, Awaitable, Optional, TypeVar

try:
    from codecarbon import EmissionsTracker  # type: ignore
except Exception:  # pragma: no cover - optional dependency may be unavailable
    EmissionsTracker = None  # type: ignore[assignment]

from broker.redis_client import RedisClient
from jobs.base_job import BaseJob, JobResult

logger = logging.getLogger(__name__)

T = TypeVar("T")


class EnergyProfiler:
    """Wraps job execution with CodeCarbon to record energy and emissions.

    Results are persisted to Redis at:
    - energy:{job_id}              — per-job measurements
    - energy:profile:{job_type}    — aggregated totals per job type
    """

    def __init__(
        self,
        redis_client: Optional[RedisClient] = None,
        *,
        job_id: Optional[str] = None,
        job_type: Optional[str] = None,
    ) -> None:
        """Initialize the profiler.

        If a Redis client wrapper is provided, aggregated results can be stored
        via `run_with_profiling`. The lightweight `run` method performs
        measurement only.
        """
        self.redis_client = redis_client
        self.job_id = job_id
        self.job_type = job_type

    async def run(self, awaitable: Awaitable[T]) -> T:
        """Measure energy/emissions while awaiting an awaitable."""
        start = time.perf_counter()
        tracker: Any = None
        if EmissionsTracker is not None:
            try:
                tracker = EmissionsTracker(
                    measure_power_secs=1,
                    log_level="error",
                    save_to_file=False,
                )
                tracker.start()
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Energy tracking disabled (CodeCarbon init failed): %s", str(exc))
                tracker = None
        try:
            value = await awaitable
        finally:
            emissions_kg: Optional[float] = None
            if tracker is not None:
                try:
                    emissions_kg = tracker.stop()
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("EmissionsTracker.stop() failed: %s", str(exc))
                    emissions_kg = None

        duration = time.perf_counter() - start

        measured_energy_kwh: Optional[float] = None
        if tracker is not None:
            try:
                total_energy = getattr(tracker, "_total_energy", None)
                measured_energy_kwh = getattr(total_energy, "kwh", None)
            except Exception:  # pragma: no cover - internal structure may change
                measured_energy_kwh = None

        if isinstance(value, JobResult):
            energy_kwh = float(measured_energy_kwh) if measured_energy_kwh is not None else float(value.energy_kwh)
            co2_grams = float(emissions_kg) * 1000.0 if emissions_kg is not None else float(value.co2_grams)
            return JobResult(
                job_id=value.job_id,
                success=value.success,
                output=value.output,
                error=value.error,
                duration_seconds=duration,
                energy_kwh=energy_kwh,
                co2_grams=co2_grams,
            )  # type: ignore[return-value]

        return value

    async def run_with_profiling(self, job_type: str, job: BaseJob) -> JobResult:
        """Execute a job while measuring energy and emissions.

        The wrapped job's JobResult is returned, with energy and CO₂ fields
        updated from CodeCarbon measurements when available.
        """
        start = time.perf_counter()
        tracker: Any = None
        if EmissionsTracker is not None:
            try:
                tracker = EmissionsTracker(
                    measure_power_secs=1,
                    log_level="error",
                    save_to_file=False,
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Energy tracking disabled (CodeCarbon init failed): %s", str(exc))
                tracker = None

        logger.info("Starting energy profiling for job %s (%s)", job.job_id, job_type)

        if tracker is not None:
            try:
                tracker.start()
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Energy tracking disabled (CodeCarbon start failed): %s", str(exc))
                tracker = None
        result: JobResult
        try:
            result = await job.execute()
        finally:
            emissions_kg: Optional[float] = None
            if tracker is not None:
                try:
                    emissions_kg = tracker.stop()
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("EmissionsTracker.stop() failed: %s", str(exc))
                    emissions_kg = None

        duration = time.perf_counter() - start

        measured_energy_kwh: Optional[float] = None
        if tracker is not None:
            try:
                total_energy = getattr(tracker, "_total_energy", None)
                measured_energy_kwh = getattr(total_energy, "kwh", None)
            except Exception:  # pragma: no cover - internal structure may change
                measured_energy_kwh = None

        energy_kwh = float(measured_energy_kwh) if measured_energy_kwh is not None else result.energy_kwh
        co2_grams = (
            float(emissions_kg) * 1000.0
            if emissions_kg is not None
            else result.co2_grams
        )

        profiled_result = JobResult(
            job_id=result.job_id,
            success=result.success,
            output=result.output,
            error=result.error,
            duration_seconds=duration,
            energy_kwh=energy_kwh,
            co2_grams=co2_grams,
        )

        await self._store_results(job_type=job_type, result=profiled_result)

        logger.info(
            "Energy profile for job %s (%s): energy_kwh=%.6f, co2_grams=%.2f",
            job.job_id,
            job_type,
            energy_kwh,
            co2_grams,
        )

        return profiled_result

    async def _store_results(self, job_type: str, result: JobResult) -> None:
        """Persist per-job and aggregated metrics to Redis."""
        if self.redis_client is None:
            raise ValueError("redis_client is required to store profiling results")
        client = await self.redis_client.get_client()

        job_key = f"energy:{result.job_id}"
        profile_key = f"energy:profile:{job_type}"

        payload = {
            "job_id": result.job_id,
            "job_type": job_type,
            "success": result.success,
            "duration_seconds": result.duration_seconds,
            "energy_kwh": result.energy_kwh,
            "co2_grams": result.co2_grams,
        }

        await client.set(job_key, json.dumps(payload))

        pipe = client.pipeline(transaction=False)
        pipe.hincrbyfloat(profile_key, "energy_kwh", result.energy_kwh)
        pipe.hincrbyfloat(profile_key, "co2_grams", result.co2_grams)
        pipe.hincrby(profile_key, "jobs", 1)
        await pipe.execute()

