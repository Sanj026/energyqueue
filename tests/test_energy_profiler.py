from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from jobs.base_job import JobResult
from worker.energy_profiler import EnergyProfiler


def _make_redis_mocks() -> tuple[AsyncMock, AsyncMock, MagicMock]:
    redis_conn = AsyncMock()
    redis_pipe = AsyncMock()
    redis_conn.set = AsyncMock()
    redis_conn.pipeline = MagicMock(return_value=redis_pipe)
    redis_pipe.hincrbyfloat = MagicMock()
    redis_pipe.hincrby = MagicMock()
    redis_pipe.execute = AsyncMock()

    redis_client = AsyncMock()
    redis_client.get_client = AsyncMock(return_value=redis_conn)
    return redis_client, redis_conn, redis_pipe


@pytest.mark.asyncio
async def test_returns_jobresult_with_energy_values_after_execution() -> None:
    redis_client, redis_conn, _ = _make_redis_mocks()

    job = AsyncMock()
    job.job_id = "job-1"
    job.execute = AsyncMock(
        return_value=JobResult(job_id="job-1", success=True, duration_seconds=0.1, energy_kwh=0.0, co2_grams=0.0)
    )

    tracker = MagicMock()
    tracker.start = MagicMock()
    tracker.stop = MagicMock(return_value=0.012)  # kg
    tracker._total_energy = MagicMock(kwh=0.345)

    with (
        patch("worker.energy_profiler.EmissionsTracker", return_value=tracker),
        patch("worker.energy_profiler.time.perf_counter", side_effect=[1.0, 3.0]),
    ):
        profiler = EnergyProfiler(redis_client)
        result = await profiler.run_with_profiling(job_type="image_resize", job=job)

    assert isinstance(result, JobResult)
    assert result.energy_kwh == 0.345
    assert result.co2_grams == 12.0
    assert result.duration_seconds == 2.0


@pytest.mark.asyncio
async def test_stores_result_in_redis_at_energy_job_id() -> None:
    redis_client, redis_conn, redis_pipe = _make_redis_mocks()

    job = AsyncMock()
    job.job_id = "job-2"
    job.execute = AsyncMock(return_value=JobResult(job_id="job-2", success=True))

    tracker = MagicMock()
    tracker.start = MagicMock()
    tracker.stop = MagicMock(return_value=0.0)
    tracker._total_energy = MagicMock(kwh=0.0)

    with patch("worker.energy_profiler.EmissionsTracker", return_value=tracker):
        profiler = EnergyProfiler(redis_client)
        result = await profiler.run_with_profiling(job_type="ml_training", job=job)

    assert redis_conn.set.await_count == 1
    set_call_args = redis_conn.set.await_args.args
    assert set_call_args[0] == "energy:job-2"
    payload = json.loads(set_call_args[1])
    assert payload["job_id"] == "job-2"
    assert payload["job_type"] == "ml_training"
    assert payload["success"] is True
    assert "duration_seconds" in payload
    assert "energy_kwh" in payload
    assert "co2_grams" in payload

    redis_conn.pipeline.assert_called_once_with(transaction=False)
    redis_pipe.execute.assert_awaited_once()
    assert isinstance(result, JobResult)


@pytest.mark.asyncio
async def test_falls_back_gracefully_if_tracker_cannot_provide_measurements() -> None:
    redis_client, _, _ = _make_redis_mocks()

    job = AsyncMock()
    job.job_id = "job-3"
    job.execute = AsyncMock(
        return_value=JobResult(job_id="job-3", success=True, duration_seconds=0.0, energy_kwh=0.123, co2_grams=4.56)
    )

    with patch("worker.energy_profiler.EmissionsTracker", None):
        profiler = EnergyProfiler(redis_client)
        result = await profiler.run_with_profiling(job_type="image_resize", job=job)

    assert result.energy_kwh == 0.123
    assert result.co2_grams == 4.56


@pytest.mark.asyncio
async def test_duration_is_measured_correctly() -> None:
    redis_client, _, _ = _make_redis_mocks()

    job = AsyncMock()
    job.job_id = "job-4"
    job.execute = AsyncMock(return_value=JobResult(job_id="job-4", success=True))

    tracker = MagicMock()
    tracker.start = MagicMock()
    tracker.stop = MagicMock(return_value=None)
    tracker._total_energy = MagicMock(kwh=None)

    with (
        patch("worker.energy_profiler.EmissionsTracker", return_value=tracker),
        patch("worker.energy_profiler.time.perf_counter", side_effect=[10.0, 12.25]),
    ):
        profiler = EnergyProfiler(redis_client)
        result = await profiler.run_with_profiling(job_type="ml_training", job=job)

    assert result.duration_seconds == 2.25

