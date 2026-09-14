from pulseguard_streaming.feature_state import FEATURE_NAMES

from pulseguard_detection.rule_engine import load_rules
from pulseguard_detection.score import DetectionArtifacts, score_transaction, severity_for, top_contributing_features


class _FakeIsoModel:
    """Stands in for a trained IsolationForest: returns a fixed
    score_samples value so score.py's own logic (not sklearn's) is what
    gets exercised."""

    def __init__(self, normal_score: float):
        self._normal_score = normal_score

    def score_samples(self, X):
        return [self._normal_score] * len(X)


# every rule in rules.json is written against the real feature set, so a
# fake artifacts object still needs all of them present in context, even
# though a given test only cares about one or two.
DEFAULT_FEATURES = {name: 0.0 for name in FEATURE_NAMES}
DEFAULT_FEATURES.update({"isNewMerchantCategory": False, "isOutsideActiveHours": False})
FEATURE_STATS = {name: {"mean": 0.0, "std": 1.0} for name in FEATURE_NAMES}
FEATURE_STATS["impliedSpeedKmh"] = {"mean": 30.0, "std": 20.0}


def _artifacts(normal_score: float, threshold: float) -> DetectionArtifacts:
    return DetectionArtifacts(
        rules=load_rules(),
        feature_names=FEATURE_NAMES,
        feature_stats=FEATURE_STATS,
        threshold=threshold,
        iso_model=_FakeIsoModel(normal_score),
    )


def test_no_alert_when_nothing_fires_and_score_below_threshold():
    artifacts = _artifacts(normal_score=-0.1, threshold=0.9)  # anomaly score = 0.1, well below threshold
    features = dict(DEFAULT_FEATURES, amountZScore=0.2, impliedSpeedKmh=40, trailingMean=500)
    raw = {"txnId": "T1", "customerId": "C1", "amount": 500, "deviceIsNew": False}
    assert score_transaction(artifacts, features, raw) is None


def test_alert_when_score_clears_threshold():
    artifacts = _artifacts(normal_score=-0.95, threshold=0.9)  # anomaly score = 0.95
    features = dict(DEFAULT_FEATURES, amountZScore=0.2, impliedSpeedKmh=40, trailingMean=500)
    raw = {"txnId": "T1", "customerId": "C1", "amount": 500, "deviceIsNew": False}
    alert = score_transaction(artifacts, features, raw)
    assert alert is not None
    assert alert["triggeredRules"] == []
    assert alert["severity"] in ("MEDIUM", "HIGH")


def test_alert_when_a_rule_fires_even_with_low_anomaly_score():
    artifacts = _artifacts(normal_score=-0.01, threshold=0.99)  # anomaly score near zero, well below threshold
    features = dict(DEFAULT_FEATURES, impliedSpeedKmh=3000, distanceFromPrevKm=1200, trailingMean=500)
    raw = {"txnId": "T1", "customerId": "C1", "amount": 500, "deviceIsNew": False}
    alert = score_transaction(artifacts, features, raw)
    assert alert is not None
    assert "IMPOSSIBLE_TRAVEL" in alert["triggeredRules"]
    assert alert["severity"] == "HIGH"  # IMPOSSIBLE_TRAVEL's configured severity


def test_scoring_survives_nan_features_without_crashing():
    # Spark represents a nullable DoubleType column (e.g. secondsSincePrevTxn
    # on a customer's first-ever transaction) as NaN via pandas/Arrow, not
    # Python None -- this reached sklearn's score_samples and crashed with
    # "Input X contains NaN" in production before _is_missing() was fixed.
    artifacts = _artifacts(normal_score=-0.5, threshold=0.9)
    features = dict(DEFAULT_FEATURES, secondsSincePrevTxn=float("nan"), amountZScore=0.1)
    raw = {"txnId": "T1", "customerId": "C1", "amount": 500, "deviceIsNew": False}
    # must not raise
    score_transaction(artifacts, features, raw)
    top = top_contributing_features(artifacts, features)
    assert all(f["name"] != "secondsSincePrevTxn" for f in top)


def test_top_contributing_features_ranks_by_absolute_zscore():
    artifacts = _artifacts(normal_score=-0.5, threshold=0.9)
    features = dict(DEFAULT_FEATURES, amountZScore=0.1, impliedSpeedKmh=630.0, distinctDevices24Hour=1.2)  # speed is ~30 std devs out
    top = top_contributing_features(artifacts, features, k=2)
    assert top[0]["name"] == "impliedSpeedKmh"


def test_severity_escalates_for_high_model_only_scores():
    rules = load_rules()
    assert severity_for([], score=2.0, threshold=1.0) == "HIGH"
    assert severity_for([], score=1.1, threshold=1.0) == "MEDIUM"


def test_multiple_fired_rules_report_highest_severity():
    rules = [r for r in load_rules() if r.id in ("AMOUNT_SPIKE", "ATO_SIGNATURE")]  # MEDIUM and CRITICAL
    assert severity_for(rules, score=0.0, threshold=1.0) == "CRITICAL"
