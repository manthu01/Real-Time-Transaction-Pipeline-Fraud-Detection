"""The scoring function: combines the rule engine and the LightGBM fraud
probability into one alert decision, with per-alert SHAP explanations,
and scores the Isolation Forest alongside it in shadow.

Same "one function, called from everywhere" principle as
`feature_state.apply_event`: this module has no Spark dependency, so it is
called identically by the live streaming job (wired into
`feature_job.py`'s output stream) and by any offline backtest or notebook
that wants to replay the exact same detection logic against historical
data. An alert without an explanation is an alert an analyst can't act on
-- so every alert this produces carries which rules fired, why the model
scored it the way it did (TreeSHAP), and which features were most
statistically unusual (z-scores), not just a bare score.

Champion / shadow:
  - LightGBM (champion) decides alerts: `rules fired OR p >= threshold`.
  - The Isolation Forest (shadow) scores every alerted transaction too,
    but never triggers one. It needs no labels, so it's the detector that
    can still notice a fraud pattern nobody has labelled yet; its score is
    carried on the alert so analysts can see when the two disagree.

Explanations come from LightGBM's own `predict(..., pred_contrib=True)`,
which is TreeSHAP: exact SHAP values for tree models, in log-odds, with
no extra dependency in the Spark image. The `shap` library is only used
offline to cross-check them.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from pulseguard_detection.rule_engine import Rule, evaluate_rules, load_rules

SEVERITY_RANK = {"CRITICAL": 3, "HIGH": 2, "MEDIUM": 1, "LOW": 0}
TOP_FEATURES_COUNT = 5
UNUSUAL_FEATURES_COUNT = 3
# model-only alert severity. The capacity-calibrated threshold sits far
# below this (3.3e-5 at 200 alerts/day -- see threshold.json), so
# "score >= 1.5 x threshold" (the old Isolation Forest rule) would mark
# nearly every model-only alert HIGH. LightGBM's p is an unweighted
# probability, so "the model thinks fraud is more likely than not" is a
# meaningful, threshold-independent line.
HIGH_SEVERITY_PROBABILITY = 0.5


@dataclass
class DetectionArtifacts:
    rules: list[Rule]
    feature_names: tuple[str, ...]
    feature_stats: dict[str, dict[str, float]]
    threshold: float  # on LightGBM probability
    iso_model: Any  # sklearn.ensemble.IsolationForest, kept untyped so this module never imports sklearn at module load time
    lgbm_model: Any  # lightgbm.Booster, untyped for the same reason
    iso_threshold: float | None = None  # shadow only: puts isoScore in context, never triggers an alert


def load_artifacts(artifacts_dir: str | Path) -> DetectionArtifacts:
    import joblib
    import lightgbm as lgb

    artifacts_dir = Path(artifacts_dir)
    feature_names = tuple(json.loads((artifacts_dir / "feature_names.json").read_text()))
    feature_stats = json.loads((artifacts_dir / "feature_stats.json").read_text())
    threshold_doc = json.loads((artifacts_dir / "threshold.json").read_text())
    iso_model = joblib.load(artifacts_dir / "isolation_forest.joblib")
    lgbm_model = lgb.Booster(model_file=str(artifacts_dir / "lightgbm.txt"))
    # the scorer builds its matrix in feature_names.json order; a booster
    # trained on a different column order would silently score garbage.
    if tuple(lgbm_model.feature_name()) != feature_names:
        raise ValueError("lightgbm.txt feature order does not match feature_names.json -- retrain both together")
    return DetectionArtifacts(
        rules=load_rules(),
        feature_names=feature_names,
        feature_stats=feature_stats,
        threshold=threshold_doc["threshold"],
        iso_model=iso_model,
        lgbm_model=lgbm_model,
        iso_threshold=threshold_doc.get("iso_threshold"),
    )


def _is_missing(value: Any) -> bool:
    """None covers a Python-side missing value; a nullable Spark
    DoubleType column comes through pandas/Arrow as NaN instead of None,
    and `value is None` silently fails to catch that -- which is exactly
    how a customer's first-ever transaction (secondsSincePrevTxn = null)
    reached sklearn as NaN and crashed score_samples in production."""
    if value is None:
        return True
    try:
        return math.isnan(value)
    except TypeError:
        return False


def feature_matrix(artifacts: DetectionArtifacts, feature_rows: list[dict[str, Any]]) -> np.ndarray:
    """One float64 row per transaction, in the booster's feature order.
    Missing values stay NaN -- deliberately not 0.0: LightGBM was trained
    on NaN (see train.train_lightgbm) and routes it down each split's
    learned missing-value branch, whereas 0.0 for secondsSincePrevTxn on
    a first-ever transaction would read as "zero seconds since the last
    one", the card-testing signature. Booleans become 0/1."""
    X = np.full((len(feature_rows), len(artifacts.feature_names)), np.nan, dtype=np.float64)
    for i, features in enumerate(feature_rows):
        for j, name in enumerate(artifacts.feature_names):
            value = features.get(name)
            if not _is_missing(value):
                X[i, j] = float(value)
    return X


def anomaly_scores(artifacts: DetectionArtifacts, X: np.ndarray) -> np.ndarray:
    """Isolation Forest shadow score, higher = more anomalous. The IF was
    trained on fillna(0.0) and sklearn's IsolationForest rejects NaN, so
    (unlike LightGBM) it gets zeros here."""
    import pandas as pd

    # a bare ndarray works but triggers an sklearn UserWarning since the
    # model was fit on a named DataFrame; passing one back keeps logs clean.
    frame = pd.DataFrame(np.nan_to_num(X, nan=0.0), columns=artifacts.feature_names)
    # score_samples: higher = more normal; flipped so higher = more anomalous,
    # the same convention used throughout training and threshold selection.
    return -np.asarray(artifacts.iso_model.score_samples(frame), dtype=np.float64)


def anomaly_score(artifacts: DetectionArtifacts, features: dict[str, Any]) -> float:
    return float(anomaly_scores(artifacts, feature_matrix(artifacts, [features]))[0])


def shap_features(artifacts: DetectionArtifacts, features: dict[str, Any], contrib_row: np.ndarray, k: int = TOP_FEATURES_COUNT) -> list[dict[str, Any]]:
    """Top-k features by |SHAP| from one row of `pred_contrib` output
    (one value per feature, bias in the last column -- excluded here).
    "up" means the feature pushed the log-odds toward fraud."""
    contributions = []
    for name, shap_value in zip(artifacts.feature_names, contrib_row[:-1]):
        shap_value = round(float(shap_value), 4)
        if shap_value == 0.0:
            # unused on this row's paths, or too small to show: a "0.0, up"
            # entry would only confuse the waterfall
            continue
        value = features.get(name)
        contributions.append(
            {
                "name": name,
                "value": None if _is_missing(value) else float(value),
                "shap": shap_value,
                "direction": "up" if shap_value > 0 else "down",
            }
        )
    contributions.sort(key=lambda c: abs(c["shap"]), reverse=True)
    return contributions[:k]


def zscore_features(artifacts: DetectionArtifacts, features: dict[str, Any], k: int = UNUSUAL_FEATURES_COUNT) -> list[dict[str, Any]]:
    """Ranks features by how many standard deviations they sit from the
    non-fraud training population's mean. Not a model attribution -- that's
    shap_features -- but the other question an analyst asks: "what about
    this transaction is statistically odd?" Shown alongside SHAP as
    `unusualFeatures`.
    """
    contributions = []
    for name in artifacts.feature_names:
        value = features.get(name)
        if isinstance(value, bool) or _is_missing(value):
            continue
        stats = artifacts.feature_stats.get(name)
        if not stats or not stats["std"]:
            continue
        z = (float(value) - stats["mean"]) / stats["std"]
        contributions.append({"name": name, "value": value, "zScore": round(z, 3)})
    contributions.sort(key=lambda c: abs(c["zScore"]), reverse=True)
    return contributions[:k]


def severity_for(fired_rules: list[Rule], probability: float) -> str:
    if fired_rules:
        return max((r.severity for r in fired_rules), key=lambda s: SEVERITY_RANK.get(s, -1))
    # model-only alert, no rule matched: scale severity by the model's own
    # confidence so analysts can still triage (see HIGH_SEVERITY_PROBABILITY).
    if probability >= HIGH_SEVERITY_PROBABILITY:
        return "HIGH"
    return "MEDIUM"


def score_batch(artifacts: DetectionArtifacts, feature_rows: list[dict[str, Any]], raws: list[dict[str, Any]]) -> list[dict[str, Any] | None]:
    """Scores a batch of transactions; returns one entry per input, an
    alert dict or None. The Booster is called once for probabilities and
    once for SHAP contributions over the whole batch -- per-row calls
    would pay LightGBM's per-call overhead thousands of times per
    micro-batch. `raws` supply the fields rules need beyond the computed
    feature vector (amount, deviceIsNew) plus identifiers for the alert.
    """
    if not feature_rows:
        return []
    X = feature_matrix(artifacts, feature_rows)
    probabilities = artifacts.lgbm_model.predict(X)
    fired_per_row = []
    for features, raw in zip(feature_rows, raws):
        context = {**{name: features.get(name) for name in artifacts.feature_names}, "amount": raw["amount"], "deviceIsNew": bool(raw.get("deviceIsNew"))}
        fired_per_row.append(evaluate_rules(artifacts.rules, context))

    alerted = [i for i, (p, fired) in enumerate(zip(probabilities, fired_per_row)) if fired or p >= artifacts.threshold]
    results: list[dict[str, Any] | None] = [None] * len(feature_rows)
    if not alerted:
        return results

    # explanations and the shadow score only for rows that alert -- most
    # transactions don't, and TreeSHAP is the expensive call here.
    X_alerted = X[alerted]
    contribs = artifacts.lgbm_model.predict(X_alerted, pred_contrib=True)
    iso_scores = anomaly_scores(artifacts, X_alerted)
    for row_idx, i in enumerate(alerted):
        p = float(probabilities[i])
        results[i] = {
            "txnId": raws[i]["txnId"],
            "customerId": raws[i]["customerId"],
            # 6 places, not 4: the capacity threshold is ~3e-5, so model-only
            # alerts near it would otherwise all display as score 0.0
            "score": round(p, 6),
            "severity": severity_for(fired_per_row[i], p),
            "triggeredRules": [r.id for r in fired_per_row[i]],
            "topFeatures": shap_features(artifacts, feature_rows[i], contribs[row_idx]),
            "baseValue": round(float(contribs[row_idx][-1]), 4),
            "isoScore": round(float(iso_scores[row_idx]), 4),
            "unusualFeatures": zscore_features(artifacts, feature_rows[i]),
        }
    return results


def score_transaction(artifacts: DetectionArtifacts, features: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any] | None:
    """Returns an alert dict if this transaction should be flagged
    (a rule fired, or the LightGBM probability cleared the calibrated
    threshold), else None. A one-row score_batch, so single-transaction
    callers run exactly the batch path the streaming job runs."""
    return score_batch(artifacts, [features], [raw])[0]
