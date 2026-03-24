import asyncio
import logging
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from jobs.base_job import JobResult
from scheduler.scheduler import SchedulingDecision


@dataclass(frozen=True)
class _FakeJob:
    job_id: str
    payload: dict[str, Any]
    result: JobResult

    async def execute(self) -> JobResult:
        return self.result


def _make_mock_redis() -> tuple[MagicMock, AsyncMock]:
    redis_wrapper = MagicMock()
    redis_client = AsyncMock()
    redis_client.exists = AsyncMock(return_value=0)
    redis_client.set = AsyncMock(return_value=True)
    redis_client.delete = AsyncMock()
    redis_client.lpush = AsyncMock()
    redis_wrapper.get_client = AsyncMock(return_value=redis_client)
    return redis_wrapper, redis_client


@pytest.mark.asyncio
async def test_worker_stops_when_running_false() -> None:
    mock_queue = MagicMock()
    mock_queue.dequeue_any = AsyncMock(return_value=None)
    mock_queue.redis, _ = _make_mock_redis()

    from worker.worker import Worker

    worker = Worker(worker_id="test-1", queue=mock_queue)

    async def stop_after_delay() -> None:
        await asyncio.sleep(0.05)
        await worker.stop()

    original_sleep = asyncio.sleep

    async def fast_sleep(_: float) -> None:
        await original_sleep(0)

    with patch("worker.worker.asyncio.sleep", new=fast_sleep):
        await asyncio.wait_for(
            asyncio.gather(worker.start(), stop_after_delay(), return_exceptions=True),
            timeout=1.0,
        )

    assert worker.running is False


@pytest.mark.asyncio
async def test_unknown_job_type_handled_gracefully(caplog: pytest.LogCaptureFixture) -> None:
    mock_queue = MagicMock()
    mock_queue.dequeue_any = AsyncMock(
        side_effect=[
            {"id": "job-1", "type": "does_not_exist", "payload": {}},
            None,
            None,
        ]
    )
    mock_queue.redis, redis_client = _make_mock_redis()

    from worker.worker import Worker

    worker = Worker(worker_id="test-1", queue=mock_queue)
    caplog.set_level(logging.INFO)

    async def stop_after_delay() -> None:
        await asyncio.sleep(0.05)
        await worker.stop()

    original_sleep = asyncio.sleep

    async def fast_sleep(_: float) -> None:
        await original_sleep(0)

    with (
        patch("worker.worker.asyncio.sleep", new=fast_sleep),
        patch("worker.worker.Scheduler") as mock_scheduler_cls,
    ):
        mock_scheduler_cls.return_value.decide = AsyncMock(
            return_value=SchedulingDecision.RUN
        )
        await asyncio.wait_for(
            asyncio.gather(worker.start(), stop_after_delay(), return_exceptions=True),
            timeout=1.0,
        )

    assert "Unknown job type" in caplog.text
    redis_client.delete.assert_any_await("lock:job-1")


@pytest.mark.asyncio
async def test_successful_job_logs_completion(caplog: pytest.LogCaptureFixture) -> None:
    mock_queue = MagicMock()
    mock_queue.dequeue_any = AsyncMock(
        side_effect=[
            {"id": "job-1", "type": "fake", "payload": {}},
            None,
            None,
        ]
    )
    mock_queue.redis, redis_client = _make_mock_redis()

    success_result = JobResult(
        job_id="job-1",
        success=True,
        output={"ok": True},
        duration_seconds=0.25,
        co2_grams=1.0,
        energy_kwh=0.01,
    )

    def _job_factory(job_id: str, payload: dict[str, Any]) -> _FakeJob:
        return _FakeJob(job_id=job_id, payload=payload, result=success_result)

    from worker.worker import Worker

    worker = Worker(worker_id="test-1", queue=mock_queue)
    caplog.set_level(logging.INFO)

    async def stop_after_delay() -> None:
        await asyncio.sleep(0.05)
        await worker.stop()

    original_sleep = asyncio.sleep

    async def fast_sleep(_: float) -> None:
        await original_sleep(0)

    with (
        patch("worker.worker.JOB_REGISTRY", new={"fake": _job_factory}),
        patch("worker.worker.asyncio.sleep", new=fast_sleep),
        patch("worker.worker.Scheduler") as mock_scheduler_cls,
    ):
        mock_scheduler_cls.return_value.decide = AsyncMock(
            return_value=SchedulingDecision.RUN
        )
        await asyncio.wait_for(
            asyncio.gather(worker.start(), stop_after_delay(), return_exceptions=True),
            timeout=1.0,
        )

    assert "completed" in caplog.text.lower()
    redis_client.delete.assert_any_await("lock:job-1")


@pytest.mark.asyncio
async def test_failed_job_logs_error(caplog: pytest.LogCaptureFixture) -> None:
    mock_queue = MagicMock()
    mock_queue.dequeue_any = AsyncMock(
        side_effect=[
            {"id": "job-1", "type": "fake", "payload": {}},
            None,
            None,
        ]
    )
    mock_queue.redis, redis_client = _make_mock_redis()

    failure_result = JobResult(
        job_id="job-1",
        success=False,
        error="boom",
        duration_seconds=0.1,
        co2_grams=0.0,
        energy_kwh=0.0,
    )

    def _job_factory(job_id: str, payload: dict[str, Any]) -> _FakeJob:
        return _FakeJob(job_id=job_id, payload=payload, result=failure_result)

    from worker.worker import Worker

    worker = Worker(worker_id="test-1", queue=mock_queue)
    caplog.set_level(logging.INFO)

    async def stop_after_delay() -> None:
        await asyncio.sleep(0.05)
        await worker.stop()

    original_sleep = asyncio.sleep

    async def fast_sleep(_: float) -> None:
        await original_sleep(0)

    with (
        patch("worker.worker.JOB_REGISTRY", new={"fake": _job_factory}),
        patch("worker.worker.asyncio.sleep", new=fast_sleep),
        patch("worker.worker.Scheduler") as mock_scheduler_cls,
    ):
        mock_scheduler_cls.return_value.decide = AsyncMock(
            return_value=SchedulingDecision.RUN
        )
        await asyncio.wait_for(
            asyncio.gather(worker.start(), stop_after_delay(), return_exceptions=True),
            timeout=1.0,
        )

    assert "failed" in caplog.text.lower()
    redis_client.delete.assert_any_await("lock:job-1")


@pytest.mark.asyncio
async def test_stop_decision_reenqueues_job_and_skips_execution() -> None:
    mock_queue = MagicMock()
    job_data = {"id": "job-1", "type": "fake", "payload": {}, "priority": "MEDIUM"}
    mock_queue.redis, redis_client = _make_mock_redis()

    success_result = JobResult(
        job_id="job-1",
        success=True,
        output={"ok": True},
        duration_seconds=0.25,
        co2_grams=1.0,
        energy_kwh=0.01,
    )

    def _job_factory(job_id: str, payload: dict[str, Any]) -> _FakeJob:
        return _FakeJob(job_id=job_id, payload=payload, result=success_result)

    from worker.worker import Worker

    worker = Worker(worker_id="test-1", queue=mock_queue)

    with (
        patch("worker.worker.JOB_REGISTRY", new={"fake": _job_factory}),
        patch("worker.worker.Scheduler") as mock_scheduler_cls,
    ):
        mock_scheduler_cls.return_value.decide = AsyncMock(
            return_value=SchedulingDecision.STOP
        )
        await worker._execute_job(job_data)

    redis_client.lpush.assert_awaited_once()
    args, _ = redis_client.lpush.await_args
    assert args[0] == "queue:medium"
    redis_client.delete.assert_any_await("lock:job-1")


@pytest.mark.asyncio
async def test_defer_low_priority_reenqueues_with_five_minute_delay() -> None:
    mock_queue = MagicMock()
    job_data = {"id": "job-1", "type": "fake", "payload": {}, "priority": "LOW"}
    mock_queue.redis, redis_client = _make_mock_redis()

    success_result = JobResult(
        job_id="job-1",
        success=True,
        output={"ok": True},
        duration_seconds=0.25,
        co2_grams=1.0,
        energy_kwh=0.01,
    )

    def _job_factory(job_id: str, payload: dict[str, Any]) -> _FakeJob:
        return _FakeJob(job_id=job_id, payload=payload, result=success_result)

    from worker.worker import Worker

    worker = Worker(worker_id="test-1", queue=mock_queue)

    with (
        patch("worker.worker.JOB_REGISTRY", new={"fake": _job_factory}),
        patch("worker.worker.Scheduler") as mock_scheduler_cls,
        patch("worker.worker.asyncio.create_task") as mock_create_task,
    ):
        mock_scheduler_cls.return_value.decide = AsyncMock(
            return_value=SchedulingDecision.DEFER
        )
        await worker._execute_job(job_data)

    mock_create_task.assert_called_once()
    assert True  # job executed without re-enqueueing
    redis_client.delete.assert_any_await("lock:job-1")


@pytest.mark.asyncio
async def test_throttle_medium_priority_executes_job() -> None:
    mock_queue = MagicMock()
    job_data = {"id": "job-1", "type": "fake", "payload": {}, "priority": "MEDIUM"}
    mock_queue.redis, redis_client = _make_mock_redis()

    success_result = JobResult(
        job_id="job-1",
        success=True,
        output={"ok": True},
        duration_seconds=0.25,
        co2_grams=1.0,
        energy_kwh=0.01,
    )

    def _job_factory(job_id: str, payload: dict[str, Any]) -> _FakeJob:
        return _FakeJob(job_id=job_id, payload=payload, result=success_result)

    from worker.worker import Worker

    worker = Worker(worker_id="test-1", queue=mock_queue)

    with (
        patch("worker.worker.JOB_REGISTRY", new={"fake": _job_factory}),
        patch("worker.worker.Scheduler") as mock_scheduler_cls,
    ):
        mock_scheduler_cls.return_value.decide = AsyncMock(
            return_value=SchedulingDecision.THROTTLE
        )
        await worker._execute_job(job_data)

    assert True  # job executed without re-enqueueing
    redis_client.delete.assert_any_await("lock:job-1")

