"""Alert threshold selection calibrated to analyst review capacity.

The build plan is explicit that this is the part that separates someone
who has run a fraud pipeline from someone who has only trained a model:
don't pick the F1-optimal threshold, pick it from how many alerts a human
team can actually review in a day. This module is pure numpy/pandas so the
percentile math and the trade-off curve are unit-tested without needing a
trained model at all.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def percentile_threshold(scores: np.ndarray, target_alert_rate: float) -> float:
    """The score threshold such that roughly `target_alert_rate` fraction
    of transactions would be alerted on. E.g. target_alert_rate=0.001 finds
    the 99.9th percentile of the score distribution."""
    if not 0 < target_alert_rate < 1:
        raise ValueError("target_alert_rate must be in (0, 1)")
    return float(np.quantile(scores, 1 - target_alert_rate))


def recall_at_threshold(scores: np.ndarray, labels: np.ndarray, threshold: float) -> float:
    labels = np.asarray(labels).astype(bool)
    if labels.sum() == 0:
        return float("nan")
    flagged = scores >= threshold
    return float((flagged & labels).sum() / labels.sum())


def alert_rate_at_threshold(scores: np.ndarray, threshold: float) -> float:
    return float((scores >= threshold).mean())


def alerts_per_day_for_capacity(reviews_per_analyst_per_day: float, analyst_count: int) -> float:
    return reviews_per_analyst_per_day * analyst_count


def select_threshold_for_capacity(
    scores: np.ndarray, txns_per_day: float, reviews_per_analyst_per_day: float, analyst_count: int
) -> dict[str, float]:
    """The build plan's worked example, generalized: given how many
    transactions flow through per day and how many alerts the review team
    can handle per day, back out the alert rate and the score threshold
    that produces it."""
    target_alerts_per_day = alerts_per_day_for_capacity(reviews_per_analyst_per_day, analyst_count)
    target_alert_rate = min(target_alerts_per_day / txns_per_day, 1.0)
    threshold = percentile_threshold(scores, target_alert_rate)
    return {
        "target_alerts_per_day": target_alerts_per_day,
        "target_alert_rate": target_alert_rate,
        "threshold": threshold,
        "actual_alert_rate": alert_rate_at_threshold(scores, threshold),
    }


def trade_off_curve(
    scores: np.ndarray, labels: np.ndarray, txns_per_day: float, n_points: int = 50
) -> pd.DataFrame:
    """alerts/day vs fraud-caught (recall) across a sweep of thresholds --
    the curve the build plan says is worth more in a README than another
    model."""
    quantiles = np.linspace(0.999, 0.5, n_points)
    rows = []
    for q in quantiles:
        threshold = float(np.quantile(scores, q))
        rows.append(
            {
                "quantile": q,
                "threshold": threshold,
                "alert_rate": alert_rate_at_threshold(scores, threshold),
                "alerts_per_day": alert_rate_at_threshold(scores, threshold) * txns_per_day,
                "recall": recall_at_threshold(scores, labels, threshold),
            }
        )
    return pd.DataFrame(rows)
