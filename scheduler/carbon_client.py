import logging
import os
import requests
from datetime import datetime

logger = logging.getLogger(__name__)

ELECTRICITY_MAPS_API_KEY = os.getenv("ELECTRICITY_MAPS_API_KEY", "")
CARBON_CACHE_KEY = "cache:carbon_intensity"
CACHE_TTL = 300  # 5 minutes


class CarbonClient:
    """Fetches real-time carbon intensity from the Electricity Maps API."""

    def __init__(self, redis_client):
        self.redis = redis_client

    async def get_carbon_intensity(self, zone: str = "GB") -> float:
        """Return grams of CO2 per kWh for the given grid zone.
        Checks Redis cache first — only hits the API if cache is stale.
        """
        cached = await self._get_cached(zone)
        if cached is not None:
            logger.info("Carbon intensity from cache: %.1f gCO2/kWh", cached)
            return cached

        fresh = await self._fetch_from_api(zone)
        await self._cache(zone, fresh)
        return fresh

    async def _fetch_from_api(self, zone: str) -> float:
        """Hit the Electricity Maps API and return carbon intensity."""
        url = f"https://api.electricitymap.org/v3/carbon-intensity/latest?zone={zone}"
        headers = {"auth-token": ELECTRICITY_MAPS_API_KEY}

        try:
            response = requests.get(url, headers=headers, timeout=5)
            response.raise_for_status()
            data = response.json()
            intensity = float(data["carbonIntensity"])
            logger.info("Live carbon intensity for %s: %.1f gCO2/kWh", zone, intensity)
            return intensity
        except Exception as e:
            logger.warning("Carbon API failed: %s — using fallback 200g", str(e))
            return 200.0

    async def _get_cached(self, zone: str):
        """Return cached carbon intensity or None if stale/missing."""
        client = await self.redis.get_client()
        raw = await client.get(f"{CARBON_CACHE_KEY}:{zone}")
        return float(raw) if raw else None

    async def _cache(self, zone: str, value: float) -> None:
        """Store carbon intensity in Redis for 5 minutes."""
        client = await self.redis.get_client()
        await client.setex(f"{CARBON_CACHE_KEY}:{zone}", CACHE_TTL, str(value))