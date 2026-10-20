import math

import lightgbm as lgb
import numpy as np
import pandas as pd
import pytest
from pulseguard_streaming.feature_state import FEATURE_NAMES

from pulseguard_detection.rule_engine import load_rules
from pulseguard_detection.score import (
    DetectionArtifacts,
    feature_matrix,
    score_batch,
    score_transaction,
    severity_for,
    shap_features,
    zscore_features,
)


class _FakeIsoModel:
    """Stands in for a trained IsolationForest: returns a fixed
    score_samples value so score.py's own logic (not sklearn's) is what
    gets exercised. Also rejects NaN the way sklearn does, so a NaN
    leaking into the shadow scorer fails the test instead of passing."""

    def __init__(self, normal_score: float = -0.5):
        self._normal_score = normal_score

    def score_samples(self, X):
        assert not np.isnan(np.asarray(X, dtype=float)).any(), "Input X contains NaN"
        return [self._normal_score] * len(X)


class _FakeBooster:
    """A lightgbm.Booster stand-in with a fixed probability, for tests
    about the alert decision itself rather than the model. Its "SHAP"
    output is all-zero contributions plus a bias equal to logit(p), which
    still satisfies additivity."""

    def __init__(self, probability: float):
        self._p = probability

    def feature_name(self):
        return list(FEATURE_NAMES)

    def predict(self, X, pred_contrib=False, raw_score=False):
        n = len(X)
        logit = math.log(self._p / (1 - self._p))
        if pred_contrib:
            out = np.zeros((n, len(FEATURE_NAMES) + 1))
            out[:, -1] = logit
            return out
        return np.full(n, logit if raw_score else self._p)


@pytest.fixture(scope="module")
def real_booster():
    """A small real LightGBM model on synthetic data, so the SHAP and NaN
    tests exercise LightGBM's actual TreeSHAP and missing-value handling.
    Fraud here = a burst (txnCount1Min high) or a very short gap since the
    previous transaction; first-ever transactions (secondsSincePrevTxn
    missing) are never fraud -- so a model that confused NaN with 0.0
    would visibly score them wrong."""
    rng = np.random.default_rng(0)
    n = 4000
    X = pd.DataFrame(rng.normal(size=(n, len(FEATURE_NAMES))), columns=list(FEATURE_NAMES))
    X["txnCount1Min"] = rng.integers(0, 6, n).astype(float)
    X["secondsSincePrevTxn"] = rng.uniform(0, 3600, n)
    first_txn = rng.random(n) < 0.1
    X.loc[first_txn, "secondsSincePrevTxn"] = np.nan
    y = ((X["txnCount1Min"] >= 4) | (X["secondsSincePrevTxn"] < 30)).astype(int)
    params = {"objective": "binary", "num_leaves": 8, "learning_rate": 0.2, "verbosity": -1, "seed": 0}
    return lgb.train(params, lgb.Dataset(X, y), num_boost_round=30)


# every rule in rules.json is written against the real feature set, so a
# fake artifacts object still needs all of them present in context, even
# though a given test only cares about one or two.
DEFAULT_FEATURES = {name: 0.0 for name in FEATURE_NAMES}
DEFAULT_FEATURES.update({"isNewMerchantCategory": False, "isOutsideActiveHours": False, "secondsSincePrevTxn": 600.0})
FEATURE_STATS = {name: {"mean": 0.0, "std": 1.0} for name in FEATURE_NAMES}
FEATURE_STATS["impliedSpeedKmh"] = {"mean": 30.0, "std": 20.0}
RAW = {"txnId": "T1", "customerId": "C1", "amount": 500, "deviceIsNew": False}


def _artifacts(booster, threshold: float) -> DetectionArtifacts:
    return DetectionArtifacts(
        rules=load_rules(),
        feature_names=FEATURE_NAMES,
        feature_stats=FEATURE_STATS,
        threshold=threshold,
        iso_model=_FakeIsoModel(),
        lgbm_model=booster,
        iso_threshold=0.6,
    )


def test_no_alert_when_nothing_fires_and_probability_below_threshold():
    artifacts = _artifacts(_FakeBooster(0.01), threshold=0.5)
    features = dict(DEFAULT_FEATURES, amountZScore=0.2, impliedSpeedKmh=40, trailingMean=500)
    assert score_transaction(artifacts, features, RAW) is None


def test_alert_when_probability_clears_threshold_carries_shadow_and_explanations():
    artifacts = _artifacts(_FakeBooster(0.9), threshold=0.5)
    features = dict(DEFAULT_FEATURES, amountZScore=0.2, impliedSpeedKmh=40, trailingMean=500)
    alert = score_transaction(artifacts, features, RAW)
    assert alert is not None
    assert alert["triggeredRules"] == []
    assert alert["score"] == 0.9
    assert alert["severity"] == "HIGH"  # model-only, p >= 0.5
    assert alert["isoScore"] == 0.5  # shadow score is reported, not used for the decision
    assert alert["baseValue"] == pytest.approx(math.log(0.9 / 0.1), abs=1e-4)
    assert alert["rawScore"] == pytest.approx(math.log(0.9 / 0.1), abs=1e-4)  # fake booster: all SHAP is bias
    assert isinstance(alert["unusualFeatures"], list)


def test_rules_only_alert_still_fires_when_probability_is_low():
    artifacts = _artifacts(_FakeBooster(0.0001), threshold=0.99)
    features = dict(DEFAULT_FEATURES, impliedSpeedKmh=3000, distanceFromPrevKm=1200, trailingMean=500)
    alert = score_transaction(artifacts, features, RAW)
    assert alert is not None
    assert "IMPOSSIBLE_TRAVEL" in alert["triggeredRules"]
    assert alert["severity"] == "HIGH"  # IMPOSSIBLE_TRAVEL's configured severity
    assert alert["score"] < artifacts.threshold


def test_shap_values_plus_base_value_sum_to_raw_margin(real_booster):
    # the defining SHAP property: base value + sum of per-feature SHAP
    # values reproduces the model's raw output (log-odds), exactly.
    artifacts = _artifacts(real_booster, threshold=0.5)
    features = dict(DEFAULT_FEATURES, txnCount1Min=5.0, secondsSincePrevTxn=12.0, amountZScore=1.5)
    X = feature_matrix(artifacts, [features])
    contrib = real_booster.predict(X, pred_contrib=True)[0]
    raw_margin = real_booster.predict(X, raw_score=True)[0]
    assert contrib.sum() == pytest.approx(raw_margin, abs=1e-9)
    p = real_booster.predict(X)[0]
    assert 1 / (1 + math.exp(-raw_margin)) == pytest.approx(p, abs=1e-9)


def test_alert_shap_features_are_ranked_signed_and_capped(real_booster):
    artifacts = _artifacts(real_booster, threshold=0.5)
    features = dict(DEFAULT_FEATURES, txnCount1Min=5.0, secondsSincePrevTxn=12.0)
    alert = score_transaction(artifacts, features, RAW)
    assert alert is not None
    top = alert["topFeatures"]
    assert 0 < len(top) <= 5
    assert [abs(f["shap"]) for f in top] == sorted((abs(f["shap"]) for f in top), reverse=True)
    for f in top:
        assert f["direction"] == ("up" if f["shap"] > 0 else "down")
    # the payload's rawScore is the full model output the waterfall ends at
    X = feature_matrix(artifacts, [features])
    assert alert["rawScore"] == pytest.approx(real_booster.predict(X, raw_score=True)[0], abs=1e-4)
    # both drivers of the synthetic label pushed toward fraud
    names_up = {f["name"] for f in top if f["direction"] == "up"}
    assert {"txnCount1Min", "secondsSincePrevTxn"} <= names_up


def test_missing_seconds_since_prev_txn_is_passed_as_nan_not_zero(real_booster):
    # Spark represents a nullable DoubleType column (secondsSincePrevTxn on
    # a customer's first-ever transaction) as NaN via pandas/Arrow. That
    # once crashed sklearn's score_samples in production. LightGBM handles
    # NaN natively and was trained on NaN, so it must get NaN -- 0.0 would
    # read as "zero seconds since the last transaction", i.e. card testing.
    artifacts = _artifacts(real_booster, threshold=0.5)
    first_txn = dict(DEFAULT_FEATURES, secondsSincePrevTxn=float("nan"))
    assert math.isnan(feature_matrix(artifacts, [first_txn])[0, FEATURE_NAMES.index("secondsSincePrevTxn")])
    assert math.isnan(feature_matrix(artifacts, [dict(first_txn, secondsSincePrevTxn=None)])[0, FEATURE_NAMES.index("secondsSincePrevTxn")])

    # must not raise, through either the decision or the (forced) explanation path
    assert score_transaction(artifacts, first_txn, RAW) is None
    forced = score_transaction(_artifacts(real_booster, threshold=0.0), first_txn, RAW)
    assert forced is not None
    nan_feature = [f for f in forced["topFeatures"] if f["name"] == "secondsSincePrevTxn"]
    assert all(f["value"] is None for f in nan_feature)  # serialisable, not NaN

    p_nan = real_booster.predict(feature_matrix(artifacts, [first_txn]))[0]
    p_zero = real_booster.predict(feature_matrix(artifacts, [dict(first_txn, secondsSincePrevTxn=0.0)]))[0]
    assert p_nan < 0.5 < p_zero


def test_score_batch_matches_per_row_scoring(real_booster):
    artifacts = _artifacts(real_booster, threshold=0.5)
    rows = [
        dict(DEFAULT_FEATURES, txnCount1Min=5.0),
        dict(DEFAULT_FEATURES),
        dict(DEFAULT_FEATURES, secondsSincePrevTxn=float("nan")),
        dict(DEFAULT_FEATURES, impliedSpeedKmh=3000, distanceFromPrevKm=1200),
    ]
    raws = [dict(RAW, txnId=f"T{i}") for i in range(len(rows))]
    batch = score_batch(artifacts, rows, raws)
    single = [score_transaction(artifacts, f, r) for f, r in zip(rows, raws)]
    assert batch == single
    assert [a is not None for a in batch] == [True, False, False, True]


def test_zscore_features_ranks_by_absolute_zscore_and_skips_nan():
    artifacts = _artifacts(_FakeBooster(0.5), threshold=0.9)
    features = dict(DEFAULT_FEATURES, amountZScore=0.1, impliedSpeedKmh=630.0, distinctDevices24Hour=1.2, secondsSincePrevTxn=float("nan"))
    top = zscore_features(artifacts, features, k=2)
    assert top[0]["name"] == "impliedSpeedKmh"  # ~30 std devs out
    assert all(f["name"] != "secondsSincePrevTxn" for f in zscore_features(artifacts, features))


def test_shap_features_skips_unused_features_and_the_bias_column():
    artifacts = _artifacts(_FakeBooster(0.5), threshold=0.9)
    contrib = np.zeros(len(FEATURE_NAMES) + 1)
    contrib[FEATURE_NAMES.index("amountZScore")] = -0.7
    contrib[FEATURE_NAMES.index("txnCount1Min")] = 1.2
    contrib[-1] = -6.0  # bias, never reported as a feature
    top = shap_features(artifacts, dict(DEFAULT_FEATURES, amountZScore=-2.0, txnCount1Min=4.0), contrib)
    assert top == [
        {"name": "txnCount1Min", "value": 4.0, "shap": 1.2, "direction": "up"},
        {"name": "amountZScore", "value": -2.0, "shap": -0.7, "direction": "down"},
    ]


def test_model_only_severity_uses_probability():
    assert severity_for([], probability=0.93) == "HIGH"
    assert severity_for([], probability=0.001) == "MEDIUM"


def test_confident_model_is_not_downgraded_by_a_weaker_rule():
    medium_rule = [r for r in load_rules() if r.id == "AMOUNT_SPIKE"]  # MEDIUM
    assert severity_for(medium_rule, probability=1.0) == "HIGH"
    # but a rule still sets severity on its own when the model isn't confident
    assert severity_for(medium_rule, probability=0.0007) == "MEDIUM"
    critical_rule = [r for r in load_rules() if r.id == "ATO_SIGNATURE"]
    assert severity_for(critical_rule, probability=0.99) == "CRITICAL"


def test_multiple_fired_rules_report_highest_severity():
    rules = [r for r in load_rules() if r.id in ("AMOUNT_SPIKE", "ATO_SIGNATURE")]  # MEDIUM and CRITICAL
    assert severity_for(rules, probability=0.0) == "CRITICAL"
