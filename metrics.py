import asyncio
import logging
import os
from typing import Optional

import requests
from prometheus_client import Gauge, start_http_server

from broker.redis_client import RedisClient

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 15
PRIORITY_QUEUES = {
    "high": "queue:high",
    "medium": "queue:medium",
    "low": "queue:low",
}

QUEUE_DEPTH = Gauge("queue_depth", "Current queue depth by priority", labelnames=["priority"])
JOBS_COMPLETED_TOTAL = Gauge("jobs_completed_total", "Total completed jobs (LLEN jobs:completed)")
CO2_CONSUMED_GRAMS = Gauge("co2_consumed_grams", "Total CO2 consumed in grams")
CARBON_INTENSITY_GCO2_KWH = Gauge(
    "carbon_intensity_gco2_kwh",
    "Cached carbon intensity (gCO2/kWh) for GB",
)


def _safe_float(raw: Optional[str]) -> float:
    """Convert a Redis string value to float safely."""
    if raw is None:
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


async def _update_metrics(redis_client: RedisClient) -> None:
    """Fetch latest values from Redis and set Prometheus metrics."""
    client = await redis_client.get_client()

    for priority, key in PRIORITY_QUEUES.items():
        depth = await client.llen(key)
        QUEUE_DEPTH.labels(priority=priority).set(depth)

    completed = await client.llen("jobs:completed")
    JOBS_COMPLETED_TOTAL.set(completed)

    consumed_raw = await client.get("energy:budget:consumed")
    CO2_CONSUMED_GRAMS.set(_safe_float(consumed_raw))

    intensity_raw = await client.get("cache:carbon_intensity:GB")
    intensity = _safe_float(intensity_raw)

    if intensity == 0.0:
        try:
            url = "https://api.electricitymap.org/v3/carbon-intensity/latest?zone=GB"
            headers = {"auth-token": os.getenv("ELECTRICITY_MAPS_API_KEY", "")}
            response = requests.get(url, headers=headers, timeout=5)
            response.raise_for_status()
            intensity = float(response.json()["carbonIntensity"])
            await client.set("cache:carbon_intensity:GB", str(intensity), ex=300)
            logger.info("Fetched carbon intensity from API: %.2f gCO2/kWh", intensity)
        except Exception:
            logger.exception("Failed to fetch carbon intensity from Electricity Maps API")

    CARBON_INTENSITY_GCO2_KWH.set(intensity)


async def main() -> None:
    """Run an HTTP Prometheus exporter and periodically refresh metrics."""
    logging.basicConfig(level=logging.INFO)
    start_http_server(8000)
    logger.info("Prometheus metrics server started on :8000")

    redis_client = RedisClient()
    await redis_client.connect()

    try:
        while True:
            try:
                await _update_metrics(redis_client)
            except Exception:
                logger.exception("Failed to update metrics from Redis")
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
    finally:
        await redis_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

