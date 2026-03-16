import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

import altair as alt
import redis.asyncio as redis
import streamlit as st

logger = logging.getLogger(__name__)

REFRESH_SECONDS = 5

QUEUE_HIGH_KEY = "queue:high"
QUEUE_MEDIUM_KEY = "queue:medium"
QUEUE_LOW_KEY = "queue:low"

CARBON_INTENSITY_KEY_PREFIX = "cache:carbon_intensity"
DEFAULT_GRID_ZONE = "GB"

ENERGY_BUDGET_CONSUMED_KEY = "energy:budget:consumed"
DEFAULT_ENERGY_BUDGET_GRAMS = 500.0

COMPLETED_JOBS_KEY = "jobs:completed"
SCHEDULER_DECISION_LABELS = ("RUN", "DEFER", "THROTTLE", "STOP")


@dataclass(frozen=True)
class DashboardSnapshot:
    """A point-in-time view of queue + carbon + budget state."""

    queue_depth_high: int
    queue_depth_medium: int
    queue_depth_low: int
    carbon_intensity_g_per_kwh: Optional[float]
    energy_consumed_grams: float
    energy_budget_total_grams: float
    scheduler_decision: str
    completed_jobs: list[str]


def _get_env_float(name: str, default: float) -> float:
    """Return a float from environment variables, with fallback."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using default %.2f", name, raw, default)
        return default


def _carbon_color(intensity: Optional[float]) -> str:
    """Return a traffic-light color name for carbon intensity."""
    if intensity is None:
        return "gray"
    if intensity < 150:
        return "green"
    if intensity <= 250:
        return "orange"
    return "red"


def _scheduler_decision(
    *,
    carbon_intensity: Optional[float],
    carbon_threshold: float,
    budget_exhausted: bool,
) -> str:
    """Derive a scheduler decision badge from the current state."""
    if budget_exhausted:
        return "STOP"
    if carbon_intensity is None:
        return "DEFER"

    if carbon_intensity > carbon_threshold * 1.5:
        return "DEFER"
    if carbon_intensity > carbon_threshold:
        return "THROTTLE"
    return "RUN"


async def _connect_redis(redis_url: str) -> redis.Redis:
    """Connect to Redis and verify connectivity with a ping."""
    client: redis.Redis = await redis.from_url(
        redis_url,
        encoding="utf-8",
        decode_responses=True,
    )
    await client.ping()
    return client


async def _safe_int(client: redis.Redis, key: str) -> int:
    """Fetch integer-ish values from Redis, defaulting to 0."""
    try:
        raw = await client.get(key)
        return int(float(raw)) if raw else 0
    except Exception:
        logger.exception("Failed reading int key %s", key)
        return 0


async def _safe_float(client: redis.Redis, key: str) -> Optional[float]:
    """Fetch float-ish values from Redis, defaulting to None."""
    try:
        raw = await client.get(key)
        return float(raw) if raw else None
    except Exception:
        logger.exception("Failed reading float key %s", key)
        return None


async def _fetch_snapshot(client: redis.Redis) -> DashboardSnapshot:
    """Fetch dashboard state from Redis and environment configuration."""
    grid_zone = os.getenv("GRID_ZONE", DEFAULT_GRID_ZONE)
    carbon_threshold = _get_env_float("CARBON_THRESHOLD", 200.0)
    budget_total = _get_env_float("ENERGY_BUDGET_GRAMS", DEFAULT_ENERGY_BUDGET_GRAMS)

    carbon_key = f"{CARBON_INTENSITY_KEY_PREFIX}:{grid_zone}"
    carbon_intensity = await _safe_float(client, carbon_key)

    queue_high, queue_medium, queue_low = await client.llen(QUEUE_HIGH_KEY), await client.llen(
        QUEUE_MEDIUM_KEY
    ), await client.llen(QUEUE_LOW_KEY)

    consumed = await _safe_float(client, ENERGY_BUDGET_CONSUMED_KEY)
    consumed_grams = float(consumed) if consumed is not None else 0.0
    budget_exhausted = consumed_grams >= budget_total

    decision = _scheduler_decision(
        carbon_intensity=carbon_intensity,
        carbon_threshold=carbon_threshold,
        budget_exhausted=budget_exhausted,
    )

    try:
        completed = await client.lrange(COMPLETED_JOBS_KEY, -10, -1)
    except Exception:
        logger.exception("Failed reading completed jobs list %s", COMPLETED_JOBS_KEY)
        completed = []

    if decision not in SCHEDULER_DECISION_LABELS:
        decision = "DEFER"

    return DashboardSnapshot(
        queue_depth_high=int(queue_high),
        queue_depth_medium=int(queue_medium),
        queue_depth_low=int(queue_low),
        carbon_intensity_g_per_kwh=carbon_intensity,
        energy_consumed_grams=consumed_grams,
        energy_budget_total_grams=budget_total,
        scheduler_decision=decision,
        completed_jobs=[str(x) for x in completed],
    )


def _queue_depth_chart(snapshot: DashboardSnapshot) -> alt.Chart:
    """Build an Altair bar chart for queue depth by lane."""
    data = [
        {"lane": "HIGH", "depth": snapshot.queue_depth_high},
        {"lane": "MEDIUM", "depth": snapshot.queue_depth_medium},
        {"lane": "LOW", "depth": snapshot.queue_depth_low},
    ]
    return (
        alt.Chart(alt.Data(values=data))
        .mark_bar()
        .encode(
            x=alt.X("lane:N", title="Priority lane"),
            y=alt.Y("depth:Q", title="Jobs waiting"),
            color=alt.Color(
                "lane:N",
                scale=alt.Scale(domain=["HIGH", "MEDIUM", "LOW"], range=["#ff4b4b", "#ffa500", "#2e7d32"]),
                legend=None,
            ),
            tooltip=[alt.Tooltip("lane:N"), alt.Tooltip("depth:Q")],
        )
        .properties(height=240)
    )


def _decision_badge(decision: str) -> str:
    """Render a simple HTML badge for scheduler decision."""
    color_map = {
        "RUN": "#2e7d32",
        "DEFER": "#ff4b4b",
        "THROTTLE": "#ffa500",
        "STOP": "#6b7280",
    }
    bg = color_map.get(decision, "#6b7280")
    return (
        "<div style="
        "'display:inline-block;padding:6px 10px;border-radius:999px;"
        f"background:{bg};color:white;font-weight:700;font-size:12px;'>"
        f"{decision}"
        "</div>"
    )


async def _render_once(
    *,
    redis_url: str,
    header_box: st.delta_generator.DeltaGenerator,
    queue_box: st.delta_generator.DeltaGenerator,
    carbon_box: st.delta_generator.DeltaGenerator,
    budget_box: st.delta_generator.DeltaGenerator,
    decision_box: st.delta_generator.DeltaGenerator,
    completed_box: st.delta_generator.DeltaGenerator,
) -> None:
    """Render one dashboard frame into pre-allocated containers."""
    try:
        client = await _connect_redis(redis_url)
    except Exception:
        logger.exception("Redis connection failed")
        header_box.warning("Redis not connected")
        return

    try:
        snapshot = await _fetch_snapshot(client)
    finally:
        try:
            await client.aclose()
        except Exception:
            logger.exception("Failed closing Redis client")

    with header_box.container():
        st.title("EnergyQueue Dashboard")
        st.caption(f"Auto-refreshing every {REFRESH_SECONDS}s")

    with queue_box.container():
        st.subheader("1) Queue depth")
        st.altair_chart(_queue_depth_chart(snapshot), use_container_width=True)

    with carbon_box.container():
        st.subheader("2) Current carbon intensity (GB)")
        intensity = snapshot.carbon_intensity_g_per_kwh
        color = _carbon_color(intensity)
        label = "N/A" if intensity is None else f"{intensity:.0f} gCO₂/kWh"
        st.metric(label="Carbon intensity", value=label)
        st.markdown(
            f"<div style='margin-top:6px;font-weight:700;color:{color};'>"
            f"Status: {color.upper()}"
            "</div>",
            unsafe_allow_html=True,
        )

    with budget_box.container():
        st.subheader("3) Energy budget")
        consumed = snapshot.energy_consumed_grams
        total = snapshot.energy_budget_total_grams
        pct = 0.0 if total <= 0 else min(1.0, consumed / total)
        st.progress(pct)
        st.caption(f"{consumed:.1f} gCO₂ used / {total:.1f} gCO₂ total")

    with decision_box.container():
        st.subheader("4) Scheduler decision")
        st.markdown(_decision_badge(snapshot.scheduler_decision), unsafe_allow_html=True)
        st.caption("Derived from carbon intensity + remaining budget.")

    with completed_box.container():
        st.subheader("5) Last 10 completed jobs")
        if not snapshot.completed_jobs:
            st.caption("No completed jobs recorded yet.")
        else:
            st.code("\n".join(snapshot.completed_jobs))


async def _dashboard_loop() -> None:
    """Main Streamlit loop: periodically re-render into placeholders."""
    st.set_page_config(page_title="EnergyQueue Dashboard", layout="wide")

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")

    header_box = st.empty()
    col_left, col_right = st.columns(2)

    with col_left:
        queue_box = st.empty()
        completed_box = st.empty()

    with col_right:
        carbon_box = st.empty()
        budget_box = st.empty()
        decision_box = st.empty()

    while True:
        await _render_once(
            redis_url=redis_url,
            header_box=header_box,
            queue_box=queue_box,
            carbon_box=carbon_box,
            budget_box=budget_box,
            decision_box=decision_box,
            completed_box=completed_box,
        )
        await asyncio.sleep(REFRESH_SECONDS)


def main() -> None:
    """Entry point for `streamlit run dashboard/app.py`."""
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_dashboard_loop())


if __name__ == "__main__":
    main()
