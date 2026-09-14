"""The scoring function: combines the rule engine and the Isolation
Forest anomaly score into one alert decision, with explainability.

Same "one function, called from everywhere" principle as
`feature_state.apply_event`: this module has no Spark dependency, so it is
called identically by the live streaming job (wired into
`feature_job.py`'s output stream) and by any offline backtest or notebook
that wants to replay the exact same detection logic against historical
data. An alert without an explanation is an alert an analyst can't act on
-- so every alert this produces carries which rules fired and which
features were most unusual, not just a bare score.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pulseguard_detection.rule_engine import Rule, evaluate_rules, load_rules

SEVERITY_RANK = {"CRITICAL": 3, "HIGH": 2, "MEDIUM": 1, "LOW": 0}
TOP_FEATURES_COUNT = 3


@dataclass
class DetectionArtifacts:
    rules: list[Rule]
    feature_names: tuple[str, ...]
    feature_stats: dict[str, dict[str, float]]
    threshold: float
    iso_model: Any  # sklearn.ensemble.IsolationForest, kept untyped so this module never imports sklearn at module load time


def load_artifacts(artifacts_dir: str | Path) -> DetectionArtifacts:
    import joblib

    artifacts_dir = Path(artifacts_dir)
    feature_names = tuple(json.loads((artifacts_dir / "feature_names.json").read_text()))
    feature_stats = json.loads((artifacts_dir / "feature_stats.json").read_text())
    threshold = json.loads((artifacts_dir / "threshold.json").read_text())["threshold"]
    iso_model = joblib.load(artifacts_dir / "isolation_forest.joblib")
    return DetectionArtifacts(
        rules=load_rules(),
        feature_names=feature_names,
        feature_stats=feature_stats,
        threshold=threshold,
        iso_model=iso_model,
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


def anomaly_score(artifacts: DetectionArtifacts, features: dict[str, Any]) -> float:
    import pandas as pd

    values = {name: [0.0 if _is_missing(features.get(name)) else features.get(name)] for name in artifacts.feature_names}
    # a plain list-of-lists works but triggers an sklearn UserWarning since
    # the model was fit on a named DataFrame; passing one back keeps logs clean.
    row = pd.DataFrame(values, columns=artifacts.feature_names)
    # score_samples: higher = more normal; flipped so higher = more anomalous,
    # the same convention used throughout training and threshold selection.
    return float(-artifacts.iso_model.score_samples(row)[0])


def top_contributing_features(artifacts: DetectionArtifacts, features: dict[str, Any], k: int = TOP_FEATURES_COUNT) -> list[dict[str, Any]]:
    """Ranks features by how many standard deviations they sit from the
    non-fraud training population's mean. This is a deliberately simple
    explainability signal -- not a SHAP value -- chosen so the live
    scoring path has no extra heavy dependency; it answers "what looked
    unusual about this transaction," which is what an analyst needs, even
    if it isn't a formal per-model attribution.
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


def severity_for(fired_rules: list[Rule], score: float, threshold: float) -> str:
    if fired_rules:
        return max((r.severity for r in fired_rules), key=lambda s: SEVERITY_RANK.get(s, -1))
    # model-only alert, no rule matched: scale severity by how far past
    # the calibrated threshold the score sits, so analysts can still triage.
    if score >= threshold * 1.5:
        return "HIGH"
    return "MEDIUM"


def score_transaction(artifacts: DetectionArtifacts, features: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any] | None:
    """Returns an alert dict if this transaction should be flagged
    (a rule fired, or the anomaly score cleared the calibrated threshold),
    else None. `raw` supplies the fields rules need beyond the computed
    feature vector (amount, deviceIsNew) plus identifiers for the alert.
    """
    context = {**{name: features.get(name) for name in artifacts.feature_names}, "amount": raw["amount"], "deviceIsNew": bool(raw.get("deviceIsNew"))}
    fired = evaluate_rules(artifacts.rules, context)
    score = anomaly_score(artifacts, features)

    if not fired and score < artifacts.threshold:
        return None

    return {
        "txnId": raw["txnId"],
        "customerId": raw["customerId"],
        "score": round(score, 4),
        "severity": severity_for(fired, score, artifacts.threshold),
        "triggeredRules": [r.id for r in fired],
        "topFeatures": top_contributing_features(artifacts, features),
    }
