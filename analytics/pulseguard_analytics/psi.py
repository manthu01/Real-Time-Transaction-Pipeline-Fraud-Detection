"""Population Stability Index: is a feature's distribution this week
still shaped like the reference window it was profiled against?

PSI = sum((actual% - expected%) * ln(actual% / expected%))

PSI < 0.1 stable, 0.1-0.25 moderate shift, > 0.25 significant enough to
flag for retraining -- the exact thresholds and formula from the build
plan. Pure numpy/pandas, no Spark: the nightly job (nightly_job.py) reads
the reference and current windows out of the bronze Delta table and hands
plain arrays to these functions, which is also what makes them fast to
unit test with synthetic data instead of a live cluster.
"""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

EPSILON = 1e-4  # avoids log(0) / division-by-zero when a bin is empty in one window


def bin_edges_from_reference(reference: np.ndarray, n_bins: int = 10) -> np.ndarray:
    """Quantile-based bin edges (deciles by default) from the reference
    population, with -inf/+inf as the outer edges so a current value more
    extreme than anything seen in the reference window still lands in a
    bin instead of being dropped. Duplicate edges (a reference feature
    with many repeated values, e.g. a mostly-constant one) are collapsed
    -- a zero-width bin would otherwise make every value straddling it
    count as "outside" every bin."""
    quantiles = np.linspace(0, 1, n_bins + 1)
    edges = np.quantile(reference, quantiles)
    edges = np.unique(edges)
    if len(edges) < 2:
        # every quantile collapsed to the same value (a constant, or
        # near-constant, reference feature) -- one bin covering everything
        # is the only sensible boundary set, not a length-1 array with
        # both "outer edges" aliasing the same element.
        return np.array([-np.inf, np.inf])
    edges[0] = -np.inf
    edges[-1] = np.inf
    return edges


def bin_frequencies(values: np.ndarray, edges: np.ndarray) -> np.ndarray:
    """Fraction of `values` falling into each bin defined by `edges`."""
    if len(values) == 0:
        return np.zeros(len(edges) - 1)
    counts, _ = np.histogram(values, bins=edges)
    return counts / len(values)


def population_stability_index(reference: np.ndarray, current: np.ndarray, n_bins: int = 10) -> float:
    reference = np.asarray(reference, dtype=float)
    reference = reference[~np.isnan(reference)]
    current = np.asarray(current, dtype=float)
    current = current[~np.isnan(current)]

    if len(reference) == 0 or len(current) == 0:
        return float("nan")

    edges = bin_edges_from_reference(reference, n_bins)
    expected = bin_frequencies(reference, edges) + EPSILON
    actual = bin_frequencies(current, edges) + EPSILON

    return float(np.sum((actual - expected) * np.log(actual / expected)))


def classify_psi(value: float) -> str:
    if np.isnan(value):
        return "UNKNOWN"
    if value < 0.1:
        return "STABLE"
    if value < 0.25:
        return "MODERATE_SHIFT"
    return "SIGNIFICANT_SHIFT"


def psi_report(
    reference_df: pd.DataFrame, current_df: pd.DataFrame, feature_names: Iterable[str], n_bins: int = 10
) -> list[dict]:
    """One row per feature: the PSI value and its classification, sorted
    worst-drift-first so the nightly report leads with what needs
    attention."""
    rows = []
    for name in feature_names:
        if name not in reference_df.columns or name not in current_df.columns:
            continue
        value = population_stability_index(reference_df[name].to_numpy(), current_df[name].to_numpy(), n_bins)
        rows.append({"feature": name, "psi": round(value, 4) if not np.isnan(value) else None, "classification": classify_psi(value)})
    rows.sort(key=lambda r: (r["psi"] is None, -(r["psi"] or 0)))
    return rows
