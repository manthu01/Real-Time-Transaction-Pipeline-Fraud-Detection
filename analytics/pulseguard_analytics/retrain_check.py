"""Nightly champion-vs-challenger check: retrain a candidate Isolation
Forest on fresh bronze data, evaluate it against the deployed champion's
recorded metrics on a comparable holdout, and report the comparison.
Report only -- this never overwrites detection/artifacts/, matching the
build plan's explicit "evaluate against champion on a holdout, report but
do not auto-promote."

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
from pulseguard_detection.train import isolation_forest_scores, time_split, train_isolation_forest


def load_champion_pr_auc(artifacts_dir: str | Path) -> float | None:
    metrics_path = Path(artifacts_dir) / "metrics.json"
    if not metrics_path.exists():
        return None
    metrics = json.loads(metrics_path.read_text())
    return metrics.get("isolation_forest", {}).get("pr_auc")


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

    challenger = train_isolation_forest(train_df)
    challenger_scores = isolation_forest_scores(challenger, holdout_df)
    challenger_pr_auc = float(average_precision_score(holdout_df["isFraud"].astype(int), challenger_scores))

    champion_pr_auc = load_champion_pr_auc(champion_artifacts_dir)

    result = {
        "status": "EVALUATED",
        "challenger_pr_auc": round(challenger_pr_auc, 4),
        "champion_pr_auc": round(champion_pr_auc, 4) if champion_pr_auc is not None else None,
        "holdout_size": int(len(holdout_df)),
        "holdout_fraud_count": int(holdout_df["isFraud"].sum()),
        "recommendation": "report only -- no auto-promotion",
    }
    if champion_pr_auc is not None:
        result["challenger_better"] = challenger_pr_auc > champion_pr_auc
    return result
