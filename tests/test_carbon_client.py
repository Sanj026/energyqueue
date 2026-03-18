from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scheduler.carbon_client import CACHE_TTL, CARBON_CACHE_KEY, CarbonClient


@pytest.mark.asyncio
async def test_returns_cached_value_from_redis_if_available() -> None:
    redis_conn = AsyncMock()
    redis_conn.get = AsyncMock(return_value="123.4")
    redis_conn.setex = AsyncMock()

    redis_client = AsyncMock()
    redis_client.get_client = AsyncMock(return_value=redis_conn)

    client = CarbonClient(redis_client)

    with patch("scheduler.carbon_client.requests.get") as mock_get:
        value = await client.get_carbon_intensity(zone="GB")

    assert value == 123.4
    mock_get.assert_not_called()
    redis_conn.setex.assert_not_awaited()
    redis_conn.get.assert_awaited_once_with(f"{CARBON_CACHE_KEY}:GB")


@pytest.mark.asyncio
async def test_calls_api_when_cache_is_empty() -> None:
    redis_conn = AsyncMock()
    redis_conn.get = AsyncMock(return_value=None)
    redis_conn.setex = AsyncMock()

    redis_client = AsyncMock()
    redis_client.get_client = AsyncMock(return_value=redis_conn)

    client = CarbonClient(redis_client)

    response = MagicMock()
    response.raise_for_status = MagicMock()
    response.json = MagicMock(return_value={"carbonIntensity": 42.0})

    with patch("scheduler.carbon_client.requests.get", return_value=response) as mock_get:
        value = await client.get_carbon_intensity(zone="GB")

    assert value == 42.0
    mock_get.assert_called_once()
    redis_conn.setex.assert_awaited_once_with(f"{CARBON_CACHE_KEY}:GB", CACHE_TTL, "42.0")


@pytest.mark.asyncio
async def test_returns_fallback_when_api_fails() -> None:
    redis_conn = AsyncMock()
    redis_conn.get = AsyncMock(return_value=None)
    redis_conn.setex = AsyncMock()

    redis_client = AsyncMock()
    redis_client.get_client = AsyncMock(return_value=redis_conn)

    client = CarbonClient(redis_client)

    with patch("scheduler.carbon_client.requests.get", side_effect=Exception("boom")):
        value = await client.get_carbon_intensity(zone="GB")

    assert value == 200.0


@pytest.mark.asyncio
async def test_caches_api_result_in_redis_after_fetching() -> None:
    redis_conn = AsyncMock()
    redis_conn.get = AsyncMock(return_value=None)
    redis_conn.setex = AsyncMock()

    redis_client = AsyncMock()
    redis_client.get_client = AsyncMock(return_value=redis_conn)

    client = CarbonClient(redis_client)

    response = MagicMock()
    response.raise_for_status = MagicMock()
    response.json = MagicMock(return_value={"carbonIntensity": 88.8})

    with patch("scheduler.carbon_client.requests.get", return_value=response):
        value = await client.get_carbon_intensity(zone="GB")

    assert value == 88.8
    redis_conn.setex.assert_awaited_once_with(f"{CARBON_CACHE_KEY}:GB", CACHE_TTL, "88.8")


@pytest.mark.asyncio
async def test_handles_none_from_redis_gracefully() -> None:
    redis_conn = AsyncMock()
    redis_conn.get = AsyncMock(return_value=None)
    redis_conn.setex = AsyncMock()

    redis_client = AsyncMock()
    redis_client.get_client = AsyncMock(return_value=redis_conn)

    client = CarbonClient(redis_client)

    response = MagicMock()
    response.raise_for_status = MagicMock()
    response.json = MagicMock(return_value={"carbonIntensity": 10.0})

    with patch("scheduler.carbon_client.requests.get", return_value=response):
        value = await client.get_carbon_intensity(zone="GB")

    assert value == 10.0
    redis_conn.get.assert_awaited_once_with(f"{CARBON_CACHE_KEY}:GB")

