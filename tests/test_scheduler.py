from unittest.mock import AsyncMock

import pytest

from broker.queue_manager import Priority
from scheduler.scheduler import Scheduler, SchedulingDecision


@pytest.mark.asyncio
async def test_returns_run_when_carbon_below_150() -> None:
    queue = AsyncMock()
    carbon = AsyncMock()
    budget = AsyncMock()

    budget.is_exhausted.return_value = False
    carbon.get_carbon_intensity.return_value = 149.0

    scheduler = Scheduler(queue=queue, carbon_client=carbon, energy_budget=budget)
    decision = await scheduler.decide({"priority": Priority.MEDIUM.name})

    assert decision == SchedulingDecision.RUN


@pytest.mark.asyncio
async def test_returns_throttle_when_carbon_between_150_and_250() -> None:
    queue = AsyncMock()
    carbon = AsyncMock()
    budget = AsyncMock()

    budget.is_exhausted.return_value = False
    carbon.get_carbon_intensity.return_value = 200.0

    scheduler = Scheduler(queue=queue, carbon_client=carbon, energy_budget=budget)
    decision = await scheduler.decide({"priority": Priority.MEDIUM.name})

    assert decision == SchedulingDecision.THROTTLE


@pytest.mark.asyncio
async def test_returns_defer_when_carbon_above_250() -> None:
    queue = AsyncMock()
    carbon = AsyncMock()
    budget = AsyncMock()

    budget.is_exhausted.return_value = False
    carbon.get_carbon_intensity.return_value = 251.0

    scheduler = Scheduler(queue=queue, carbon_client=carbon, energy_budget=budget)
    decision = await scheduler.decide({"priority": Priority.MEDIUM.name})

    assert decision == SchedulingDecision.DEFER


@pytest.mark.asyncio
async def test_returns_stop_when_budget_exhausted() -> None:
    queue = AsyncMock()
    carbon = AsyncMock()
    budget = AsyncMock()

    budget.is_exhausted.return_value = True
    carbon.get_carbon_intensity.return_value = 10.0

    scheduler = Scheduler(queue=queue, carbon_client=carbon, energy_budget=budget)
    decision = await scheduler.decide({"priority": Priority.MEDIUM.name})

    assert decision == SchedulingDecision.STOP
    carbon.get_carbon_intensity.assert_not_awaited()


@pytest.mark.asyncio
async def test_high_priority_runs_even_on_dirty_grid() -> None:
    queue = AsyncMock()
    carbon = AsyncMock()
    budget = AsyncMock()

    budget.is_exhausted.return_value = False
    carbon.get_carbon_intensity.return_value = 400.0

    scheduler = Scheduler(queue=queue, carbon_client=carbon, energy_budget=budget)
    decision = await scheduler.decide({"priority": Priority.HIGH.name})

    assert decision == SchedulingDecision.RUN

