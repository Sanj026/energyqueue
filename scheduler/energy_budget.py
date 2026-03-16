import logging
import os
from broker.redis_client import RedisClient

logger = logging.getLogger(__name__)

BUDGET_KEY = "energy:budget:consumed"
ENERGY_BUDGET_GRAMS = float(os.getenv("ENERGY_BUDGET_GRAMS", 500))


class EnergyBudget:
    """Tracks cumulative CO2 consumed this session against a total budget."""

    def __init__(self, redis_client: RedisClient):
        self.redis = redis_client

    async def add_consumption(self, co2_grams: float) -> None:
        """Add CO2 grams to the running total."""
        client = await self.redis.get_client()
        await client.incrbyfloat(BUDGET_KEY, co2_grams)
        logger.info("Added %.2fg CO2. Total: %.2fg / %.2fg",
                    co2_grams, await self.get_consumed(), ENERGY_BUDGET_GRAMS)

    async def get_consumed(self) -> float:
        """Return total CO2 consumed so far in grams."""
        client = await self.redis.get_client()
        raw = await client.get(BUDGET_KEY)
        return float(raw) if raw else 0.0

    async def get_remaining(self) -> float:
        """Return how many grams of CO2 budget are left."""
        return max(0.0, ENERGY_BUDGET_GRAMS - await self.get_consumed())

    async def is_exhausted(self) -> bool:
        """Return True if we've hit or exceeded the budget."""
        return await self.get_consumed() >= ENERGY_BUDGET_GRAMS

    async def budget_percentage_used(self) -> float:
        """Return 0.0–100.0 showing how much of the budget is used."""
        consumed = await self.get_consumed()
        return min(100.0, (consumed / ENERGY_BUDGET_GRAMS) * 100)

    async def reset(self) -> None:
        """Reset the budget counter — call at start of a new session."""
        client = await self.redis.get_client()
        await client.delete(BUDGET_KEY)
        logger.info("Energy budget reset")