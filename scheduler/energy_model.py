import json
import logging
from typing import Any

from broker.redis_client import RedisClient

logger = logging.getLogger(__name__)

HISTORY_PREFIX = "energy:history:"
MAX_HISTORY_SAMPLES = 20
MIN_SAMPLES_FOR_AVERAGE = 3
DEFER_CO2_THRESHOLD_GRAMS = 50.0


class EnergyModel:
    """Store per-job energy history and provide simple predictions."""

    def __init__(self, redis_client: RedisClient):
        self.redis = redis_client

    @staticmethod
    async def record_job_energy(
        redis_client: RedisClient,
        job_type: str,
        kwh: float,
        co2_grams: float,
        duration: float,
    ) -> None:
        """Record one completed job sample and keep the latest N samples."""
        client = await redis_client.get_client()
        key = f"{HISTORY_PREFIX}{job_type}"
        payload = {
            "kwh": float(kwh),
            "co2_grams": float(co2_grams),
            "duration": float(duration),
        }
        await client.lpush(key, json.dumps(payload))
        await client.ltrim(key, 0, MAX_HISTORY_SAMPLES - 1)

    @staticmethod
    async def get_average_energy(
        redis_client: RedisClient, job_type: str
    ) -> dict[str, Any] | None:
        """Return rolling averages for a job type, or None when data is sparse."""
        client = await redis_client.get_client()
        key = f"{HISTORY_PREFIX}{job_type}"
        raw_samples = await client.lrange(key, 0, MAX_HISTORY_SAMPLES - 1)
        sample_size = len(raw_samples)
        if sample_size < MIN_SAMPLES_FOR_AVERAGE:
            return None

        total_kwh = 0.0
        total_co2 = 0.0
        total_duration = 0.0
        valid_samples = 0

        for raw in raw_samples:
            try:
                sample = json.loads(raw)
            except (TypeError, ValueError):
                continue
            total_kwh += float(sample.get("kwh", 0.0))
            total_co2 += float(sample.get("co2_grams", 0.0))
            total_duration += float(sample.get("duration", 0.0))
            valid_samples += 1

        if valid_samples < MIN_SAMPLES_FOR_AVERAGE:
            return None

        return {
            "avg_kwh": total_kwh / valid_samples,
            "avg_co2": total_co2 / valid_samples,
            "avg_duration": total_duration / valid_samples,
            "sample_size": valid_samples,
        }

    @staticmethod
    async def should_defer_based_on_history(
        redis_client: RedisClient, job_type: str, carbon_intensity: float
    ) -> bool:
        """Suggest defer when predicted CO2 is high under current intensity."""
        avg = await EnergyModel.get_average_energy(redis_client, job_type)
        if avg is None:
            return False

        predicted_co2_grams = float(avg["avg_kwh"]) * float(carbon_intensity)
        return predicted_co2_grams > DEFER_CO2_THRESHOLD_GRAMS

    async def should_defer(self, job_type: str, carbon_intensity: float) -> bool:
        """Instance wrapper around defer recommendation."""
        return await EnergyModel.should_defer_based_on_history(
            self.redis,
            job_type,
            carbon_intensity,
        )
