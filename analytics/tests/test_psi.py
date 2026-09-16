import numpy as np
import pandas as pd

from pulseguard_analytics.psi import (
    bin_edges_from_reference,
    bin_frequencies,
    classify_psi,
    population_stability_index,
    psi_report,
)


def test_identical_distributions_have_near_zero_psi():
    rng = np.random.default_rng(0)
    reference = rng.normal(size=10_000)
    current = rng.normal(size=10_000)  # same distribution, different draw
    value = population_stability_index(reference, current)
    assert value < 0.05


def test_shifted_distribution_has_high_psi():
    rng = np.random.default_rng(1)
    reference = rng.normal(loc=0, scale=1, size=10_000)
    current = rng.normal(loc=3, scale=1, size=10_000)  # a large mean shift
    value = population_stability_index(reference, current)
    assert value > 0.25


def test_moderate_shift_lands_in_moderate_band():
    rng = np.random.default_rng(2)
    reference = rng.normal(loc=0, scale=1, size=20_000)
    current = rng.normal(loc=0.5, scale=1, size=20_000)
    value = population_stability_index(reference, current)
    assert 0.05 < value < 1.0  # a real shift, somewhere in the moderate-to-significant range


def test_classify_psi_thresholds():
    assert classify_psi(0.05) == "STABLE"
    assert classify_psi(0.099) == "STABLE"
    assert classify_psi(0.1) == "MODERATE_SHIFT"
    assert classify_psi(0.2) == "MODERATE_SHIFT"
    assert classify_psi(0.25) == "SIGNIFICANT_SHIFT"
    assert classify_psi(0.5) == "SIGNIFICANT_SHIFT"
    assert classify_psi(float("nan")) == "UNKNOWN"


def test_bin_edges_handle_constant_reference_without_crashing():
    reference = np.full(1000, 5.0)
    edges = bin_edges_from_reference(reference)
    # a constant reference collapses to a single bin after dedup
    assert len(edges) == 2
    assert edges[0] == -np.inf and edges[-1] == np.inf


def test_bin_frequencies_sum_to_one():
    rng = np.random.default_rng(3)
    reference = rng.normal(size=5000)
    edges = bin_edges_from_reference(reference, n_bins=10)
    freqs = bin_frequencies(reference, edges)
    assert abs(freqs.sum() - 1.0) < 1e-9


def test_empty_current_returns_nan_not_a_crash():
    reference = np.array([1.0, 2.0, 3.0, 4.0, 5.0] * 100)
    current = np.array([])
    value = population_stability_index(reference, current)
    assert np.isnan(value)


def test_psi_report_sorts_worst_drift_first():
    rng = np.random.default_rng(4)
    reference_df = pd.DataFrame(
        {
            "stable_feature": rng.normal(size=5000),
            "drifted_feature": rng.normal(size=5000),
        }
    )
    current_df = pd.DataFrame(
        {
            "stable_feature": rng.normal(size=5000),
            "drifted_feature": rng.normal(loc=4, size=5000),
        }
    )
    report = psi_report(reference_df, current_df, ["stable_feature", "drifted_feature"])
    assert report[0]["feature"] == "drifted_feature"
    assert report[0]["classification"] == "SIGNIFICANT_SHIFT"
    assert report[1]["classification"] == "STABLE"


def test_psi_report_skips_missing_features_without_crashing():
    reference_df = pd.DataFrame({"a": [1, 2, 3]})
    current_df = pd.DataFrame({"a": [1, 2, 3]})
    report = psi_report(reference_df, current_df, ["a", "nonexistent"])
    assert len(report) == 1
    assert report[0]["feature"] == "a"
