import numpy as np
import pandas as pd

from pulseguard_analytics.data_quality import (
    check_null_rates,
    check_ranges,
    check_volume,
    run_data_quality_gates,
)


def test_null_rate_passes_below_threshold():
    df = pd.DataFrame({"amount": [1.0] * 99 + [np.nan]})
    results = check_null_rates(df, ["amount"], max_null_rate=0.02)
    assert results[0].passed is True


def test_null_rate_fails_above_threshold():
    df = pd.DataFrame({"amount": [1.0] * 90 + [np.nan] * 10})
    results = check_null_rates(df, ["amount"], max_null_rate=0.02)
    assert results[0].passed is False


def test_null_rate_fails_loudly_on_missing_column():
    df = pd.DataFrame({"amount": [1.0, 2.0]})
    results = check_null_rates(df, ["amount", "missingColumn"])
    by_name = {r.name: r for r in results}
    assert by_name["null_rate:missingColumn"].passed is False


def test_range_check_passes_within_bounds():
    df = pd.DataFrame({"lat": [10.0, -50.0, 89.9]})
    results = check_ranges(df, {"lat": (-90, 90)})
    assert results[0].passed is True


def test_range_check_fails_outside_bounds():
    df = pd.DataFrame({"lat": [10.0, -95.0, 200.0]})
    results = check_ranges(df, {"lat": (-90, 90)})
    assert results[0].passed is False
    assert "2 of 3" in results[0].detail


def test_range_check_ignores_nulls():
    df = pd.DataFrame({"amount": [1.0, np.nan, 5.0]})
    results = check_ranges(df, {"amount": (0, float("inf"))})
    assert results[0].passed is True


def test_volume_check_passes_within_deviation():
    result = check_volume(current_count=1050, baseline_count=1000, max_deviation=0.5)
    assert result.passed is True


def test_volume_check_fails_on_large_drop():
    # a 90% drop in daily volume -- exactly the "stalled producer" scenario
    # these gates exist to catch.
    result = check_volume(current_count=100, baseline_count=1000, max_deviation=0.5)
    assert result.passed is False


def test_volume_check_skips_when_no_baseline():
    result = check_volume(current_count=500, baseline_count=0)
    assert result.passed is True
    assert "skipped" in result.detail


def test_run_data_quality_gates_combines_all_checks():
    df = pd.DataFrame({"amount": [100.0, -5.0, np.nan], "lat": [20.0, 20.0, 20.0]})
    results = run_data_quality_gates(df, required_columns=["amount", "lat"], baseline_daily_volume=3)
    names = {r.name for r in results}
    assert "null_rate:amount" in names
    assert "range:amount" in names
    assert "volume" in names
    range_amount = next(r for r in results if r.name == "range:amount")
    assert range_amount.passed is False  # the -5.0 violates amount >= 0
