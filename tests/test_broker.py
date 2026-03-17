import json
from collections import defaultdict, deque
from typing import Any, Callable
from unittest.mock import AsyncMock

import pytest

from broker.queue_manager import Priority, QueueKeys, QueueManager
from broker.redis_client import RedisClient


@pytest.fixture()
def mock_redis_client() -> tuple[RedisClient, AsyncMock]:
    redis_client = RedisClient()
    client = AsyncMock()
    redis_client.client = client
    return redis_client, client


def _make_fake_redis_lists() -> tuple[dict[str, deque[str]], Callable[[AsyncMock], None]]:
    lists: dict[str, deque[str]] = defaultdict(deque)

    async def lpush(key: str, value: str) -> int:
        lists[key].appendleft(value)
        return len(lists[key])

    async def rpop(key: str) -> Any:
        if not lists[key]:
            return None
        return lists[key].pop()

    async def llen(key: str) -> int:
        return len(lists[key])

    def bind(client: AsyncMock) -> None:
        client.lpush.side_effect = lpush
        client.rpop.side_effect = rpop
        client.llen.side_effect = llen

    return lists, bind


@pytest.mark.asyncio
async def test_enqueue_returns_valid_uuid(mock_redis_client: tuple[RedisClient, AsyncMock]) -> None:
    redis_client, client = mock_redis_client
    _, bind = _make_fake_redis_lists()
    bind(client)

    qm = QueueManager(redis_client)
    job_id = await qm.enqueue(job_type="ml_training", payload={"epochs": 1}, priority=Priority.HIGH)

    assert isinstance(job_id, str)
    assert len(job_id) == 36  # UUID4 canonical string length
    client.lpush.assert_awaited()


@pytest.mark.asyncio
async def test_dequeue_returns_none_on_empty_queue(mock_redis_client: tuple[RedisClient, AsyncMock]) -> None:
    redis_client, client = mock_redis_client
    _, bind = _make_fake_redis_lists()
    bind(client)

    qm = QueueManager(redis_client)
    job = await qm.dequeue(Priority.HIGH)

    assert job is None


@pytest.mark.asyncio
async def test_dequeue_any_respects_priority_order(mock_redis_client: tuple[RedisClient, AsyncMock]) -> None:
    redis_client, client = mock_redis_client
    lists, bind = _make_fake_redis_lists()
    bind(client)

    # Seed queues: LOW + MEDIUM + HIGH
    lists[QueueKeys.LOW].append(json.dumps({"id": "low-1"}))
    lists[QueueKeys.MEDIUM].append(json.dumps({"id": "med-1"}))
    lists[QueueKeys.HIGH].append(json.dumps({"id": "high-1"}))

    qm = QueueManager(redis_client)

    job1 = await qm.dequeue_any()
    assert job1 is not None and job1["id"] == "high-1"

    job2 = await qm.dequeue_any()
    assert job2 is not None and job2["id"] == "med-1"

    job3 = await qm.dequeue_any()
    assert job3 is not None and job3["id"] == "low-1"

    job4 = await qm.dequeue_any()
    assert job4 is None


@pytest.mark.asyncio
async def test_queue_length_returns_correct_count(mock_redis_client: tuple[RedisClient, AsyncMock]) -> None:
    redis_client, client = mock_redis_client
    lists, bind = _make_fake_redis_lists()
    bind(client)

    lists[QueueKeys.HIGH].extend([json.dumps({"id": "a"}), json.dumps({"id": "b"})])
    lists[QueueKeys.MEDIUM].extend([json.dumps({"id": "c"})])

    qm = QueueManager(redis_client)

    assert await qm.queue_length(Priority.HIGH) == 2
    assert await qm.queue_length(Priority.MEDIUM) == 1
    assert await qm.queue_length(Priority.LOW) == 0

