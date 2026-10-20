"""Nightly champion-vs-challenger check: retrain a candidate of each
deployed model on fresh bronze data, evaluate it on a comparable holdout,
and report it next to that model's recorded metrics. Report only -- this
never overwrites detection/artifacts/, matching the build plan's explicit
"evaluate against champion on a holdout, report but do not auto-promote."

Two deployed models, so two comparisons, each against its own number:
  - LightGBM, the champion that decides alerts, vs metrics.json's
    lightgbm.pr_auc
  - the Isolation Forest scoring in shadow, vs isolation_forest.pr_auc
Comparing an Isolation Forest challenger against "the champion" (as this
did before LightGBM was promoted) would now be comparing against the
wrong model -- and would leave the model that actually alerts unchecked.

Reuses pulseguard_detection.train's already-tested functions rather than
reimplementing training here: bronze's schema (timestamp + the 26 features
+ isFraud) is exactly what dataset.py's build_feature_dataset already
produces, since bronze *is* that same enriched feature stream written to
Delta instead of Redis/Kafka. One training implementation, read from two
different places (a JSONL export for the initial offline train, bronze
Delta for the nightly recheck) -- not two.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from sklearn.metrics import average_precision_score

from pulseguard_detection.dataset import FEATURE_NAMES
from pulseguard_detection.train import (
    isolation_forest_scores,
    lightgbm_scores,
    time_split,
    train_isolation_forest,
    train_lightgbm,
)

# metrics.json key -> the role that model plays in live scoring
DEPLOYED_MODELS = {"lightgbm": "champion", "isolation_forest": "shadow"}
LIGHTGBM_VAL_FRACTION = 0.15  # train_lightgbm's own early-stopping split


def load_deployed_pr_auc(artifacts_dir: str | Path, model: str) -> float | None:
    metrics_path = Path(artifacts_dir) / "metrics.json"
    if not metrics_path.exists():
        return None
    metrics = json.loads(metrics_path.read_text())
    return metrics.get(model, {}).get("pr_auc")


def load_champion_pr_auc(artifacts_dir: str | Path) -> float | None:
    return load_deployed_pr_auc(artifacts_dir, "lightgbm")


def _challenger_scores(model: str, train_df: pd.DataFrame, holdout_df: pd.DataFrame):
    if model == "lightgbm":
        return lightgbm_scores(train_lightgbm(train_df, val_fraction=LIGHTGBM_VAL_FRACTION), holdout_df)
    return isolation_forest_scores(train_isolation_forest(train_df), holdout_df)


def _compare_one(model: str, train_df: pd.DataFrame, holdout_df: pd.DataFrame, artifacts_dir: str | Path) -> dict:
    entry: dict = {"role": DEPLOYED_MODELS[model]}
    if model == "lightgbm":
        # early stopping scores a validation split carved from the tail of
        # train; with no fraud in it there is nothing to stop on
        _, val = time_split(train_df, 1 - LIGHTGBM_VAL_FRACTION)
        if val["isFraud"].sum() == 0:
            return {**entry, "status": "SKIPPED", "reason": "no fraud rows in LightGBM's early-stopping validation split"}

    scores = _challenger_scores(model, train_df, holdout_df)
    challenger_pr_auc = float(average_precision_score(holdout_df["isFraud"].astype(int), scores))
    deployed_pr_auc = load_deployed_pr_auc(artifacts_dir, model)
    entry.update(
        {
            "status": "EVALUATED",
            "challenger_pr_auc": round(challenger_pr_auc, 4),
            "deployed_pr_auc": round(deployed_pr_auc, 4) if deployed_pr_auc is not None else None,
        }
    )
    if deployed_pr_auc is not None:
        entry["challenger_better"] = challenger_pr_auc > deployed_pr_auc
    return entry


def retrain_and_compare(bronze_pdf: pd.DataFrame, champion_artifacts_dir: str | Path) -> dict:
    """`bronze_pdf` must have the same columns dataset.build_feature_dataset
    produces: timestamp, isFraud, and every name in FEATURE_NAMES."""
    missing = [c for c in (*FEATURE_NAMES, "timestamp", "isFraud") if c not in bronze_pdf.columns]
    if missing:
        return {"status": "SKIPPED", "reason": f"bronze data missing columns: {missing}"}

    if bronze_pdf["isFraud"].sum() < 5:
        return {"status": "SKIPPED", "reason": "fewer than 5 labelled fraud rows in the available bronze window"}

    train_df, holdout_df = time_split(bronze_pdf)
    if holdout_df["isFraud"].sum() == 0:
        return {"status": "SKIPPED", "reason": "holdout split has zero fraud examples"}

    return {
        "status": "EVALUATED",
        "holdout_size": int(len(holdout_df)),
        "holdout_fraud_count": int(holdout_df["isFraud"].sum()),
        "models": {model: _compare_one(model, train_df, holdout_df, champion_artifacts_dir) for model in DEPLOYED_MODELS},
        "recommendation": "report only -- no auto-promotion",
    }
