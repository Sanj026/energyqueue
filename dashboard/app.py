import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import altair as alt
import redis.asyncio as redis
import requests
import streamlit as st
from streamlit import fragment

from dotenv import load_dotenv

st.set_page_config(layout="wide")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

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
QUEUE_DEAD_LETTER_KEY = "queue:dead_letter"
SCHEDULER_DECISION_LABELS = ("RUN", "DEFER", "THROTTLE", "STOP", "UNKNOWN")


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
    dead_letter_entries: list[str]


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
        return "UNKNOWN"
    if carbon_intensity < 150:
        return "RUN"
    if carbon_intensity < 250:
        return "THROTTLE"
    return "DEFER"


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


async def _fetch_live_carbon_intensity() -> Optional[float]:
    """Fetch current carbon intensity from Electricity Maps API."""
    api_key = os.getenv("ELECTRICITY_MAPS_API_KEY", "")
    if not api_key:
        logger.warning("ELECTRICITY_MAPS_API_KEY is not set; showing N/A for carbon intensity")
        return None

    url = "https://api.electricitymap.org/v3/carbon-intensity/latest"
    params = {"zone": DEFAULT_GRID_ZONE}
    headers = {"auth-token": api_key}

    def _do_request() -> Optional[float]:
        try:
            response = requests.get(url, headers=headers, params=params, timeout=5)
            response.raise_for_status()
            data: Any = response.json()
            value = data.get("carbonIntensity")
            return float(value) if value is not None else None
        except Exception as exc:
            logger.warning("Carbon API request failed: %s", str(exc))
            return None

    return await asyncio.to_thread(_do_request)


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

    try:
        completed = await client.lrange("jobs:completed", 0, 9)
    except Exception:
        logger.exception("Failed reading completed jobs list %s", COMPLETED_JOBS_KEY)
        completed = []

    try:
        dead_letter = await client.lrange(QUEUE_DEAD_LETTER_KEY, 0, -1)
    except Exception:
        logger.exception("Failed reading dead letter queue %s", QUEUE_DEAD_LETTER_KEY)
        dead_letter = []

    decision = _scheduler_decision(
        carbon_intensity=carbon_intensity,
        carbon_threshold=carbon_threshold,
        budget_exhausted=budget_exhausted,
    )

    return DashboardSnapshot(
        queue_depth_high=int(queue_high),
        queue_depth_medium=int(queue_medium),
        queue_depth_low=int(queue_low),
        carbon_intensity_g_per_kwh=carbon_intensity,
        energy_consumed_grams=consumed_grams,
        energy_budget_total_grams=budget_total,
        scheduler_decision=decision,
        completed_jobs=[str(x) for x in completed],
        dead_letter_entries=[str(x) for x in dead_letter],
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
        "RUN": "#16a34a",        # green
        "THROTTLE": "#f97316",   # orange
        "DEFER": "#ef4444",      # red
        "STOP": "#991b1b",       # dark red
        "UNKNOWN": "#6b7280",    # grey
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
    dead_letter_box: st.delta_generator.DeltaGenerator,
) -> None:
    """Render one dashboard frame into pre-allocated containers."""
    render_static = not st.session_state.get("dashboard_static_rendered", False)
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

    live_intensity = await _fetch_live_carbon_intensity()

    budget_exhausted = snapshot.energy_consumed_grams >= snapshot.energy_budget_total_grams
    if budget_exhausted:
        decision = "STOP"
    elif live_intensity is None:
        decision = "UNKNOWN"
    elif live_intensity < 150:
        decision = "RUN"
    elif live_intensity < 250:
        decision = "THROTTLE"
    else:
        decision = "DEFER"

    with header_box.container():
        if render_static:
            st.markdown(
                "<div style='display:flex;justify-content:space-between;align-items:flex-end;'>"
                "<div>"
                "<h2 style='margin-bottom:0;'>EnergyQueue Operations Console</h2>"
                "<p style='margin-top:4px;color:gray;'>Carbon-aware task queue — live grid & energy telemetry.</p>"
                "</div>"
                f"<div style='font-size:12px;color:gray;'>Auto-refresh: {REFRESH_SECONDS}s</div>"
                "</div>",
                unsafe_allow_html=True,
            )
        total_queued = (
            snapshot.queue_depth_high + snapshot.queue_depth_medium + snapshot.queue_depth_low
        )
        budget_used_pct = (
            0.0
            if snapshot.energy_budget_total_grams <= 0
            else min(100.0, (snapshot.energy_consumed_grams / snapshot.energy_budget_total_grams) * 100.0)
        )
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            st.metric("Queued jobs (all lanes)", value=total_queued)
        with col_b:
            label = "N/A" if live_intensity is None else f"{live_intensity:.0f} gCO₂/kWh"
            st.metric("Live carbon intensity", value=label)
        with col_c:
            st.metric("Energy budget used", value=f"{budget_used_pct:.1f}%")

    with queue_box.container():
        st.subheader("1) Queue depth")
        st.altair_chart(_queue_depth_chart(snapshot), use_container_width=True)
        st.caption("HIGH = urgent jobs, MEDIUM = normal, LOW = background tasks")

    with carbon_box.container():
        if render_static:
            st.subheader("2) Current carbon intensity (GB)")
        intensity = live_intensity
        color = _carbon_color(intensity)
        label = "N/A" if intensity is None else f"{intensity:.0f} gCO₂/kWh"
        st.markdown(
            f"<div style='margin-top:6px;font-weight:700;color:{color};'>"
            f"Status: {color.upper()}"
            "</div>",
            unsafe_allow_html=True,
        )
        if render_static:
            st.markdown(
                "🟢 **GREEN < 150g** — grid is clean, all jobs run  \n"
                "🟡 **ORANGE 150–250g** — grid is moderate, LOW jobs deferred  \n"
                "🔴 **RED > 250g** — grid is dirty, only HIGH jobs run",
            )

    with budget_box.container():
        st.subheader("3) Energy budget")
        consumed = snapshot.energy_consumed_grams
        total = snapshot.energy_budget_total_grams
        pct = 0.0 if total <= 0 else min(1.0, consumed / total)
        st.progress(pct)
        st.caption(f"{consumed:.1f} gCO₂ used / {total:.1f} gCO₂ total")
        st.caption(
            "Session budget: 500g CO₂ total. Equivalent to ~2.5km driven in a petrol car."
        )

    with decision_box.container():
        if render_static:
            st.subheader("4) Scheduler decision")
        st.markdown(_decision_badge(decision), unsafe_allow_html=True)
        if render_static:
            st.caption("Derived from carbon intensity + remaining budget.")
            st.markdown(
                "RUN = grid clean, all jobs processing normally  \n"
                "DEFER = grid too dirty, jobs waiting for cleaner energy  \n"
                "THROTTLE = grid moderate, low priority jobs paused  \n"
                "STOP = energy budget exhausted for this session",
            )

    with completed_box.container():
        st.subheader("5) Last 10 completed jobs")
        if not snapshot.completed_jobs:
            st.caption("No completed jobs recorded yet.")
        else:
            rows: list[dict[str, Any]] = []
            for raw in snapshot.completed_jobs:
                try:
                    obj = json.loads(raw)
                    if not isinstance(obj, dict):
                        raise ValueError("completed job entry is not a JSON object")
                    rows.append(
                        {
                            "job_id": str(obj.get("job_id", "")),
                            "type": str(obj.get("type", "")),
                            "duration": obj.get("duration"),
                            "co2_grams": obj.get("co2_grams"),
                            "energy_kwh": obj.get("energy_kwh"),
                        }
                    )
                except (json.JSONDecodeError, TypeError, ValueError) as exc:
                    logger.warning("Completed job entry JSON parse failed: %s", exc)
                    rows.append(
                        {
                            "job_id": "(parse error)",
                            "type": "—",
                            "duration": None,
                            "co2_grams": None,
                            "energy_kwh": None,
                        }
                    )
            st.dataframe(rows, use_container_width=True)

    with dead_letter_box.container():
        st.subheader("6) Dead Letter Queue")
        entries = snapshot.dead_letter_entries
        if not entries:
            st.markdown(
                "<span style='color:green;font-weight:600;'>✅ No failed jobs</span>",
                unsafe_allow_html=True,
            )
        else:
            n = len(entries)
            st.markdown(
                f"<span style='color:red;font-weight:600;'>⚠️ {n} jobs in dead letter queue</span>",
                unsafe_allow_html=True,
            )
            rows: list[dict[str, str]] = []
            for raw in entries:
                try:
                    obj = json.loads(raw)
                    if not isinstance(obj, dict):
                        raise ValueError("dead letter entry is not a JSON object")
                    rows.append(
                        {
                            "job_id": str(obj.get("job_id", "")),
                            "type": str(obj.get("type", "")),
                            "dead_letter_reason": str(obj.get("dead_letter_reason", "")),
                            "dead_letter_at": str(obj.get("dead_letter_at", "")),
                        }
                    )
                except (json.JSONDecodeError, TypeError, ValueError) as exc:
                    logger.warning("Dead letter entry JSON parse failed: %s", exc)
                    preview = raw if len(raw) <= 200 else f"{raw[:200]}…"
                    rows.append(
                        {
                            "job_id": "(parse error)",
                            "type": "—",
                            "dead_letter_reason": preview,
                            "dead_letter_at": "—",
                        }
                    )
            st.dataframe(rows, use_container_width=True)

    if render_static:
        st.session_state["dashboard_static_rendered"] = True


async def _render_live_data_once(
    *,
    redis_url: str,
    metrics_box: st.delta_generator.DeltaGenerator,
    queue_box: st.delta_generator.DeltaGenerator,
    carbon_box: st.delta_generator.DeltaGenerator,
    budget_box: st.delta_generator.DeltaGenerator,
    decision_box: st.delta_generator.DeltaGenerator,
    completed_box: st.delta_generator.DeltaGenerator,
    dead_letter_box: st.delta_generator.DeltaGenerator,
) -> None:
    """Render only the live (data) sections inside a fragment."""
    try:
        client = await _connect_redis(redis_url)
    except Exception:
        logger.exception("Redis connection failed")
        return

    try:
        snapshot = await _fetch_snapshot(client)
    finally:
        try:
            await client.aclose()
        except Exception:
            logger.exception("Failed closing Redis client")

    live_intensity = await _fetch_live_carbon_intensity()

    budget_exhausted = snapshot.energy_consumed_grams >= snapshot.energy_budget_total_grams
    if budget_exhausted:
        decision = "STOP"
    elif live_intensity is None:
        decision = "UNKNOWN"
    elif live_intensity < 150:
        decision = "RUN"
    elif live_intensity < 250:
        decision = "THROTTLE"
    else:
        decision = "DEFER"

    with metrics_box.container():
        total_queued = (
            snapshot.queue_depth_high + snapshot.queue_depth_medium + snapshot.queue_depth_low
        )
        budget_used_pct = (
            0.0
            if snapshot.energy_budget_total_grams <= 0
            else min(
                100.0,
                (snapshot.energy_consumed_grams / snapshot.energy_budget_total_grams) * 100.0,
            )
        )
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            st.metric("Queued jobs (all lanes)", value=total_queued)
        with col_b:
            label = "N/A" if live_intensity is None else f"{live_intensity:.0f} gCO₂/kWh"
            st.metric("Live carbon intensity", value=label)
        with col_c:
            st.metric("Energy budget used", value=f"{budget_used_pct:.1f}%")

    with queue_box.container():
        st.subheader("1) Queue depth")
        st.altair_chart(_queue_depth_chart(snapshot), use_container_width=True)
        st.caption("HIGH = urgent jobs, MEDIUM = normal, LOW = background tasks")

    with carbon_box.container():
        st.subheader("2) Current carbon intensity (GB)")
        intensity = live_intensity
        color = _carbon_color(intensity)
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
        st.caption(
            "Session budget: 500g CO₂ total. Equivalent to ~2.5km driven in a petrol car."
        )

    with decision_box.container():
        st.subheader("4) Scheduler decision")
        st.markdown(_decision_badge(decision), unsafe_allow_html=True)
        st.caption("Derived from carbon intensity + remaining budget.")

    with completed_box.container():
        st.subheader("5) Last 10 completed jobs")
        if not snapshot.completed_jobs:
            st.caption("No completed jobs recorded yet.")
        else:
            rows: list[dict[str, Any]] = []
            for raw in snapshot.completed_jobs:
                try:
                    obj = json.loads(raw)
                    if not isinstance(obj, dict):
                        raise ValueError("completed job entry is not a JSON object")
                    rows.append(
                        {
                            "job_id": str(obj.get("job_id", "")),
                            "type": str(obj.get("type", "")),
                            "duration": obj.get("duration"),
                            "co2_grams": obj.get("co2_grams"),
                            "energy_kwh": obj.get("energy_kwh"),
                        }
                    )
                except (json.JSONDecodeError, TypeError, ValueError) as exc:
                    logger.warning("Completed job entry JSON parse failed: %s", exc)
                    rows.append(
                        {
                            "job_id": "(parse error)",
                            "type": "—",
                            "duration": None,
                            "co2_grams": None,
                            "energy_kwh": None,
                        }
                    )
            st.dataframe(rows, use_container_width=True)

    with dead_letter_box.container():
        st.subheader("6) Dead Letter Queue")
        entries = snapshot.dead_letter_entries
        if not entries:
            st.markdown(
                "<span style='color:green;font-weight:600;'>✅ No failed jobs</span>",
                unsafe_allow_html=True,
            )
        else:
            n = len(entries)
            st.markdown(
                f"<span style='color:red;font-weight:600;'>⚠️ {n} jobs in dead letter queue</span>",
                unsafe_allow_html=True,
            )
            rows: list[dict[str, str]] = []
            for raw in entries:
                try:
                    obj = json.loads(raw)
                    if not isinstance(obj, dict):
                        raise ValueError("dead letter entry is not a JSON object")
                    rows.append(
                        {
                            "job_id": str(obj.get("job_id", "")),
                            "type": str(obj.get("type", "")),
                            "dead_letter_reason": str(obj.get("dead_letter_reason", "")),
                            "dead_letter_at": str(obj.get("dead_letter_at", "")),
                        }
                    )
                except (json.JSONDecodeError, TypeError, ValueError) as exc:
                    logger.warning("Dead letter entry JSON parse failed: %s", exc)
                    preview = raw if len(raw) <= 200 else f"{raw[:200]}…"
                    rows.append(
                        {
                            "job_id": "(parse error)",
                            "type": "—",
                            "dead_letter_reason": preview,
                            "dead_letter_at": "—",
                        }
                    )
            st.dataframe(rows, use_container_width=True)


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")

    # Static header (render once).
    st.markdown(
        "<div style='display:flex;justify-content:space-between;align-items:flex-end;'>"
        "<div>"
        "<h2 style='margin-bottom:0;'>EnergyQueue Operations Console</h2>"
        "<p style='margin-top:4px;color:gray;'>Carbon-aware task queue — live grid & energy telemetry.</p>"
        "</div>"
        f"<div style='font-size:12px;color:gray;'>Auto-refresh: {REFRESH_SECONDS}s</div>"
        "</div>",
        unsafe_allow_html=True,
    )

    metrics_box = st.empty()
    col_left, col_right = st.columns(2)

    with col_left:
        queue_box = st.empty()
        completed_box = st.empty()
        dead_letter_box = st.empty()

    with col_right:
        carbon_box = st.empty()
        carbon_legend_box = st.empty()
        budget_box = st.empty()
        decision_box = st.empty()
        decision_legend_box = st.empty()

    with carbon_legend_box:
        st.markdown(
            "🟢 **GREEN < 150g** — grid is clean, all jobs run  \n"
            "🟡 **ORANGE 150–250g** — grid is moderate, LOW jobs deferred  \n"
            "🔴 **RED > 250g** — grid is dirty, only HIGH jobs run",
        )

    with decision_legend_box:
        st.markdown(
            "RUN = grid clean, all jobs processing normally  \n"
            "DEFER = grid too dirty, jobs waiting for cleaner energy  \n"
            "THROTTLE = grid moderate, low priority jobs paused  \n"
            "STOP = energy budget exhausted for this session",
        )

    @fragment(run_every=5)
    def render_live_data() -> None:
        asyncio.run(
            _render_live_data_once(
                redis_url=redis_url,
                metrics_box=metrics_box,
                queue_box=queue_box,
                carbon_box=carbon_box,
                budget_box=budget_box,
                decision_box=decision_box,
                completed_box=completed_box,
                dead_letter_box=dead_letter_box,
            )
        )

    render_live_data()


if __name__ == "__main__":
    main()
