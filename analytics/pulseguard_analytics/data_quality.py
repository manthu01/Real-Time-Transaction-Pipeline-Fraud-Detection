"""Data quality gates over the bronze table: null rates, range checks,
volume anomalies. "Fail loudly" per the build plan -- a gate here isn't a
soft metric, it's the thing that stops a silently-broken upstream (a
generator change, a schema drift, a stalled producer) from quietly
poisoning a night's worth of gold aggregates and PSI numbers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

# Range checks a violation of which indicates an actual bug upstream --
# these aren't business rules, they're invariants feature_state.py itself
# guarantees (see streaming/pulseguard_streaming/feature_state.py).
DEFAULT_RANGE_CHECKS: dict[str, tuple[float, float]] = {
    "amount": (0, float("inf")),
    "lat": (-90, 90),
    "lon": (-180, 180),
    "hourOfDay": (0, 23),
    "dayOfWeek": (0, 6),
    "amountPercentile": (0, 1),
}

DEFAULT_MAX_NULL_RATE = 0.01
DEFAULT_MAX_VOLUME_DEVIATION = 0.5  # today's row count vs. baseline may swing +/-50% before flagging


@dataclass
class QualityCheckResult:
    name: str
    passed: bool
    detail: str

    def __post_init__(self) -> None:
        # comparisons/reductions over a pandas Series yield numpy bool
        # scalars (np.True_/np.False_), not Python's `True`/`False` --
        # coerced here so every caller gets a plain bool regardless of
        # which check produced it, rather than leaking numpy's type into
        # `is True` checks or JSON serialization downstream.
        self.passed = bool(self.passed)


def check_null_rates(df: pd.DataFrame, columns: list[str], max_null_rate: float = DEFAULT_MAX_NULL_RATE) -> list[QualityCheckResult]:
    results = []
    for col in columns:
        if col not in df.columns:
            results.append(QualityCheckResult(f"null_rate:{col}", False, f"column {col} missing from bronze"))
            continue
        rate = df[col].isna().mean() if len(df) else 0.0
        results.append(
            QualityCheckResult(
                f"null_rate:{col}",
                rate <= max_null_rate,
                f"{rate:.2%} null (threshold {max_null_rate:.0%})",
            )
        )
    return results


def check_ranges(df: pd.DataFrame, range_checks: dict[str, tuple[float, float]] = None) -> list[QualityCheckResult]:
    range_checks = DEFAULT_RANGE_CHECKS if range_checks is None else range_checks
    results = []
    for col, (low, high) in range_checks.items():
        if col not in df.columns:
            continue
        series = df[col].dropna()
        out_of_range = ((series < low) | (series > high)).sum()
        results.append(
            QualityCheckResult(
                f"range:{col}",
                out_of_range == 0,
                f"{out_of_range} of {len(series)} rows outside [{low}, {high}]",
            )
        )
    return results


def check_volume(current_count: int, baseline_count: float, max_deviation: float = DEFAULT_MAX_VOLUME_DEVIATION) -> QualityCheckResult:
    if baseline_count <= 0:
        return QualityCheckResult("volume", True, "no baseline yet -- skipped")
    deviation = (current_count - baseline_count) / baseline_count
    return QualityCheckResult(
        "volume",
        abs(deviation) <= max_deviation,
        f"{current_count} rows vs baseline {baseline_count:.0f} ({deviation:+.1%})",
    )


def run_data_quality_gates(
    df: pd.DataFrame,
    required_columns: list[str],
    baseline_daily_volume: float | None = None,
    range_checks: dict[str, tuple[float, float]] | None = None,
) -> list[QualityCheckResult]:
    results = check_null_rates(df, required_columns)
    results.extend(check_ranges(df, range_checks))
    if baseline_daily_volume is not None:
        results.append(check_volume(len(df), baseline_daily_volume))
    return results


def results_to_dicts(results: list[QualityCheckResult]) -> list[dict[str, Any]]:
    return [{"name": r.name, "passed": r.passed, "detail": r.detail} for r in results]
