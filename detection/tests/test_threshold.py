import numpy as np

from pulseguard_detection.threshold import (
    alert_rate_at_threshold,
    percentile_threshold,
    recall_at_threshold,
    select_threshold_for_capacity,
    trade_off_curve,
)


def test_percentile_threshold_matches_expected_alert_rate():
    rng = np.random.default_rng(0)
    scores = rng.normal(size=100_000)
    threshold = percentile_threshold(scores, target_alert_rate=0.01)
    rate = alert_rate_at_threshold(scores, threshold)
    assert abs(rate - 0.01) < 0.002


def test_recall_at_threshold_catches_all_high_scoring_fraud():
    scores = np.array([0.1, 0.2, 0.9, 0.95, 0.99])
    labels = np.array([0, 0, 1, 1, 0])
    # threshold below both fraud scores -> both caught -> recall 1.0
    assert recall_at_threshold(scores, labels, threshold=0.5) == 1.0
    # threshold above both fraud scores -> neither caught -> recall 0.0
    assert recall_at_threshold(scores, labels, threshold=0.999) == 0.0


def test_select_threshold_for_capacity_backs_out_correct_alert_rate():
    rng = np.random.default_rng(1)
    scores = rng.normal(size=1_000_000)
    result = select_threshold_for_capacity(
        scores, txns_per_day=4_000_000, reviews_per_analyst_per_day=40, analyst_count=5
    )
    # 200 alerts/day out of 4M txns/day = 0.005% target alert rate
    assert abs(result["target_alert_rate"] - 0.00005) < 1e-9
    assert abs(result["actual_alert_rate"] - 0.00005) < 0.00003


def test_trade_off_curve_recall_decreases_as_threshold_rises():
    rng = np.random.default_rng(2)
    n = 20_000
    labels = (rng.random(n) < 0.01).astype(int)
    # fraud gets a higher-mean score distribution so the curve is meaningful
    scores = rng.normal(loc=0, scale=1, size=n) + labels * 3
    curve = trade_off_curve(scores, labels, txns_per_day=100_000, n_points=10)
    # sorted by quantile descending (0.999 -> 0.5) means threshold falls and recall should rise
    assert curve["recall"].iloc[0] <= curve["recall"].iloc[-1]
    assert (curve["alerts_per_day"] >= 0).all()
