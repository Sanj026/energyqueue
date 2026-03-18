from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest defaults for this repository."""
    config.option.asyncio_mode = "strict"


@pytest.fixture
def mock_redis() -> MagicMock:
    """Mock a Redis connection with async methods used across tests."""
    redis = MagicMock()
    redis.get = AsyncMock()
    redis.set = AsyncMock()
    redis.setex = AsyncMock()
    redis.delete = AsyncMock()
    redis.lpop = AsyncMock()
    redis.rpush = AsyncMock()
    redis.lpush = AsyncMock()
    redis.llen = AsyncMock()
    redis.keys = AsyncMock()
    redis.incrbyfloat = AsyncMock()
    return redis


@pytest.fixture
def redis_client(mock_redis: MagicMock) -> MagicMock:
    """Mock RedisClient wrapper returning a mocked Redis connection."""
    client_wrapper = MagicMock()
    client_wrapper.get_client = AsyncMock(return_value=mock_redis)
    return client_wrapper

