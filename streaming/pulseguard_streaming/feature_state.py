"""Per-customer streaming feature computation, as a single pure-Python
function with no Spark dependency.

Why pure Python: this exact function is called from two places -- the
Structured Streaming job's `applyInPandasWithState` operator (serving), and
the Phase 3 batch feature-backfill script that walks historical Delta
records to build a training set (training). Calling the *same function* in
both places is what eliminates training/serving skew; re-implementing "the
same logic" twice in two languages/frameworks is exactly how skew sneaks
back in, so this module is deliberately kept framework-agnostic.

State design: rather than maintaining a dozen separate incremental
counters, each customer's state carries one bounded event-history buffer
(pruned to the longest window needed, 7 days, and hard-capped at
MAX_HISTORY_EVENTS regardless of age) plus a handful of O(1) running
statistics (EMA mean/variance, running max, a city-frequency map, a device
last-seen map, a 24-hour-of-day histogram). Every velocity/diversity
feature is then a cheap filter+aggregate over that one buffer, computed
fresh on every event. This trades a little CPU for a lot of correctness
and readability, and keeps the state bounded no matter how bursty a single
customer's traffic gets -- the buffer is capped, and the streaming job
additionally sets a processing-time state timeout so a customer who goes
quiet for 10 days has their state dropped entirely rather than held
forever.
"""

from __future__ import annotations

import bisect
import math
from datetime import datetime, timedelta
from typing import Any

from pulseguard_common.geo import CITY_BY_NAME, haversine_km

MAX_HISTORY_DAYS = 7
MAX_HISTORY_EVENTS = 3000
EMA_ALPHA = 0.05
STATE_IDLE_TIMEOUT_DAYS = 10  # a customer silent this long has their state dropped

VELOCITY_WINDOWS_SECONDS = {
    "1Min": 60,
    "5Min": 300,
    "15Min": 900,
    "1Hour": 3600,
}
SUM_WINDOWS_SECONDS = {
    "5Min": 300,
    "1Hour": 3600,
    "24Hour": 86400,
}
DIVERSITY_WINDOWS_SECONDS = {
    "merchants": {"1Hour": 3600, "24Hour": 86400},
    "devices": {"24Hour": 86400, "7Day": 604800},
    "cities": {"24Hour": 86400},
}

# below this many observed transactions, "active hours" can't be judged
# with any confidence yet, so isOutsideActiveHours defaults to False.
MIN_HISTORY_FOR_ACTIVE_HOURS = 15
ACTIVE_HOUR_SHARE_THRESHOLD = 0.02  # an hour needs >=2% of history to count as "active"


def new_state() -> dict[str, Any]:
    return {
        "event_history": [],  # list of dicts, ascending by timestamp
        "visited_categories": set(),
        "known_devices": {},  # deviceId -> last-seen datetime
        "city_counts": {},  # city -> count, for modal city
        "trailing_mean": 0.0,
        "trailing_var": 0.0,
        "trailing_max": 0.0,
        "last_txn_time": None,
        "last_lat": None,
        "last_lon": None,
        "hour_histogram": [0] * 24,
        "n_seen": 0,
    }


def _prune_history(history: list[dict], now: datetime) -> list[dict]:
    cutoff = now - timedelta(days=MAX_HISTORY_DAYS)
    pruned = [e for e in history if e["timestamp"] >= cutoff]
    if len(pruned) > MAX_HISTORY_EVENTS:
        pruned = pruned[-MAX_HISTORY_EVENTS:]
    return pruned


def _count_since(history: list[dict], now: datetime, seconds: float) -> int:
    cutoff = now - timedelta(seconds=seconds)
    return sum(1 for e in history if e["timestamp"] >= cutoff)


def _sum_since(history: list[dict], now: datetime, seconds: float) -> float:
    cutoff = now - timedelta(seconds=seconds)
    return sum(e["amount"] for e in history if e["timestamp"] >= cutoff)


def _distinct_since(history: list[dict], now: datetime, seconds: float, field: str, current_value: str) -> int:
    cutoff = now - timedelta(seconds=seconds)
    values = {e[field] for e in history if e["timestamp"] >= cutoff}
    values.add(current_value)
    return len(values)


def _max_txns_in_any_60s_window_today(history: list[dict], now: datetime) -> int:
    """Two-pointer sliding-window max over today's timestamps, including
    the current event."""
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    times = sorted(e["timestamp"] for e in history if e["timestamp"] >= day_start)
    times.append(now)
    times.sort()
    best = 1
    left = 0
    for right, t in enumerate(times):
        while (t - times[left]).total_seconds() > 60:
            left += 1
        best = max(best, right - left + 1)
    return best


def _amount_percentile(history: list[dict], amount: float) -> float:
    """Fraction of the customer's own historical amounts that are <= this
    one. 0.0 with no history (nothing to compare against yet)."""
    if not history:
        return 0.0
    amounts = sorted(e["amount"] for e in history)
    idx = bisect.bisect_right(amounts, amount)
    return idx / len(amounts)


def _modal_city(city_counts: dict[str, int]) -> str | None:
    if not city_counts:
        return None
    return max(city_counts.items(), key=lambda kv: kv[1])[0]


def apply_event(state: dict[str, Any], event: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Advance `state` past `event` and return (new_state, features).

    `event` must have: timestamp (datetime, tz-aware), amount (float),
    merchantId, merchantCategory, deviceId, city, lat, lon (all str/float).
    """
    now = event["timestamp"]
    amount = float(event["amount"])
    history = _prune_history(state["event_history"], now)

    seconds_since_prev = (
        (now - state["last_txn_time"]).total_seconds() if state["last_txn_time"] is not None else None
    )

    if state["last_lat"] is not None:
        distance_km = haversine_km(state["last_lat"], state["last_lon"], event["lat"], event["lon"])
    else:
        distance_km = 0.0

    implied_speed_kmh = 0.0
    if seconds_since_prev and seconds_since_prev > 0:
        implied_speed_kmh = distance_km / (seconds_since_prev / 3600.0)

    modal_city = _modal_city(state["city_counts"])
    if modal_city and modal_city in CITY_BY_NAME:
        modal = CITY_BY_NAME[modal_city]
        distance_from_modal_km = haversine_km(modal.lat, modal.lon, event["lat"], event["lon"])
    else:
        distance_from_modal_km = 0.0

    n_seen = state["n_seen"]
    trailing_mean = state["trailing_mean"]
    trailing_var = state["trailing_var"]
    if n_seen == 0:
        new_mean = amount
        new_var = 0.0
    else:
        new_mean = (1 - EMA_ALPHA) * trailing_mean + EMA_ALPHA * amount
        diff = amount - trailing_mean
        new_var = (1 - EMA_ALPHA) * (trailing_var + EMA_ALPHA * diff * diff)
    trailing_std = math.sqrt(new_var)
    amount_z_score = (amount - trailing_mean) / trailing_std if n_seen > 0 and trailing_std > 1e-6 else 0.0

    new_max = amount if n_seen == 0 else max(state["trailing_max"] * 0.999, amount)
    amount_to_max_ratio = amount / state["trailing_max"] if n_seen > 0 and state["trailing_max"] > 0 else 1.0

    is_new_merchant_category = event["merchantCategory"] not in state["visited_categories"]

    hour_histogram = list(state["hour_histogram"])
    total_hist = sum(hour_histogram)
    is_outside_active_hours = False
    if total_hist >= MIN_HISTORY_FOR_ACTIVE_HOURS:
        hour_share = hour_histogram[now.hour] / total_hist
        is_outside_active_hours = hour_share < ACTIVE_HOUR_SHARE_THRESHOLD

    features = {
        "txnCount1Min": _count_since(history, now, VELOCITY_WINDOWS_SECONDS["1Min"]) + 1,
        "txnCount5Min": _count_since(history, now, VELOCITY_WINDOWS_SECONDS["5Min"]) + 1,
        "txnCount15Min": _count_since(history, now, VELOCITY_WINDOWS_SECONDS["15Min"]) + 1,
        "txnCount1Hour": _count_since(history, now, VELOCITY_WINDOWS_SECONDS["1Hour"]) + 1,
        "txnSum5Min": round(_sum_since(history, now, SUM_WINDOWS_SECONDS["5Min"]) + amount, 2),
        "txnSum1Hour": round(_sum_since(history, now, SUM_WINDOWS_SECONDS["1Hour"]) + amount, 2),
        "txnSum24Hour": round(_sum_since(history, now, SUM_WINDOWS_SECONDS["24Hour"]) + amount, 2),
        "secondsSincePrevTxn": seconds_since_prev,
        "maxTxnsInAny60SecWindowToday": _max_txns_in_any_60s_window_today(history, now),
        "amountZScore": round(amount_z_score, 4),
        "amountToTrailingMaxRatio": round(amount_to_max_ratio, 4),
        "amountPercentile": round(_amount_percentile(history, amount), 4),
        "distinctMerchants1Hour": _distinct_since(history, now, DIVERSITY_WINDOWS_SECONDS["merchants"]["1Hour"], "merchantId", event["merchantId"]),
        "distinctMerchants24Hour": _distinct_since(history, now, DIVERSITY_WINDOWS_SECONDS["merchants"]["24Hour"], "merchantId", event["merchantId"]),
        "distinctDevices24Hour": _distinct_since(history, now, DIVERSITY_WINDOWS_SECONDS["devices"]["24Hour"], "deviceId", event["deviceId"]),
        "distinctDevices7Day": _distinct_since(history, now, DIVERSITY_WINDOWS_SECONDS["devices"]["7Day"], "deviceId", event["deviceId"]),
        "distinctCities24Hour": _distinct_since(history, now, DIVERSITY_WINDOWS_SECONDS["cities"]["24Hour"], "city", event["city"]),
        "isNewMerchantCategory": is_new_merchant_category,
        "distanceFromPrevKm": round(distance_km, 3),
        "impliedSpeedKmh": round(implied_speed_kmh, 2),
        "distanceFromModalCityKm": round(distance_from_modal_km, 3),
        "hourOfDay": now.hour,
        "dayOfWeek": now.weekday(),
        "isOutsideActiveHours": is_outside_active_hours,
        "trailingMean": round(trailing_mean, 2),
        "trailingMax": round(state["trailing_max"], 2),
    }

    known_devices = dict(state["known_devices"])
    known_devices[event["deviceId"]] = now
    device_cutoff = now - timedelta(days=MAX_HISTORY_DAYS)
    known_devices = {d: t for d, t in known_devices.items() if t >= device_cutoff}

    city_counts = dict(state["city_counts"])
    city_counts[event["city"]] = city_counts.get(event["city"], 0) + 1

    hour_histogram[now.hour] += 1

    history.append(
        {
            "timestamp": now,
            "amount": amount,
            "merchantId": event["merchantId"],
            "merchantCategory": event["merchantCategory"],
            "deviceId": event["deviceId"],
            "city": event["city"],
        }
    )

    new_state_value = {
        "event_history": _prune_history(history, now),
        "visited_categories": state["visited_categories"] | {event["merchantCategory"]},
        "known_devices": known_devices,
        "city_counts": city_counts,
        "trailing_mean": new_mean,
        "trailing_var": new_var,
        "trailing_max": new_max,
        "last_txn_time": now,
        "last_lat": event["lat"],
        "last_lon": event["lon"],
        "hour_histogram": hour_histogram,
        "n_seen": n_seen + 1,
    }

    return new_state_value, features


FEATURE_NAMES: tuple[str, ...] = (
    "txnCount1Min",
    "txnCount5Min",
    "txnCount15Min",
    "txnCount1Hour",
    "txnSum5Min",
    "txnSum1Hour",
    "txnSum24Hour",
    "secondsSincePrevTxn",
    "maxTxnsInAny60SecWindowToday",
    "amountZScore",
    "amountToTrailingMaxRatio",
    "amountPercentile",
    "distinctMerchants1Hour",
    "distinctMerchants24Hour",
    "distinctDevices24Hour",
    "distinctDevices7Day",
    "distinctCities24Hour",
    "isNewMerchantCategory",
    "distanceFromPrevKm",
    "impliedSpeedKmh",
    "distanceFromModalCityKm",
    "hourOfDay",
    "dayOfWeek",
    "isOutsideActiveHours",
    "trailingMean",
    "trailingMax",
)
