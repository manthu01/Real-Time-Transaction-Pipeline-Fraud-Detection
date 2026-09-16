"""Trains and evaluates the hybrid detection layer, and writes real,
measured numbers (never hand-waved ones) to metrics.json.

Pipeline:
  1. Load a labelled historical dataset (built by dataset.py from the
     generator's `simulate` output) and time-split it train/holdout --
     holdout is strictly later in time than train, so nothing here trains
     on the future.
  2. Train an Isolation Forest on the TRAIN set's non-fraud population
     (unsupervised -- realistic, since production fraud labels are rarely
     clean at scale).
  3. Train a LightGBM classifier on the full TRAIN set with class-weight
     correction, as a supervised benchmark to quantify what's given up by
     going unsupervised.
  4. Evaluate both on the HOLDOUT set: precision/recall/PR-AUC, never
     accuracy (meaningless at <1% prevalence).
  5. Evaluate the rule engine on the same holdout.
  6. Pick an alert threshold from analyst review capacity, not from an
     F1-optimal cutoff, and report the false-positive-rate difference
     between the model alone and the rules+model hybrid at equal recall.
  7. Save both models, the threshold, the trade-off curve (as CSV and a
     PNG chart), and metrics.json.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import TYPE_CHECKING

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import average_precision_score, precision_score, recall_score

from pulseguard_detection.dataset import FEATURE_NAMES, build_feature_dataset_from_jsonl, feature_matrix
from pulseguard_detection.rule_engine import evaluate_rules, load_rules
from pulseguard_detection.threshold import recall_at_threshold, select_threshold_for_capacity, trade_off_curve

if TYPE_CHECKING:
    # only pulseguard_detection.train's own module-level type hints need
    # this; callers that just want the Isolation Forest side (e.g. the
    # nightly analytics retrain-check) shouldn't have to install lightgbm
    # to import this module at all.
    import lightgbm as lgb

DEFAULT_ARTIFACTS_DIR = Path(__file__).parent.parent / "artifacts"


def time_split(df: pd.DataFrame, train_fraction: float = 0.75) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Splits strictly by time, not randomly -- a random split would leak
    a customer's future transactions into the features of their past ones
    (the whole point of the rolling-window features is that they depend on
    what came before)."""
    cutoff_idx = int(len(df) * train_fraction)
    cutoff_time = df["timestamp"].iloc[cutoff_idx]
    train = df[df["timestamp"] < cutoff_time]
    holdout = df[df["timestamp"] >= cutoff_time]
    return train, holdout


def train_isolation_forest(train_df: pd.DataFrame) -> IsolationForest:
    non_fraud = train_df[~train_df["isFraud"]]
    X = feature_matrix(non_fraud).fillna(0.0)
    model = IsolationForest(n_estimators=200, contamination=0.01, random_state=42, n_jobs=-1)
    model.fit(X)
    return model


def isolation_forest_scores(model: IsolationForest, df: pd.DataFrame) -> np.ndarray:
    X = feature_matrix(df).fillna(0.0)
    # score_samples: higher = more normal. Flip sign so higher = more anomalous,
    # matching the convention the rest of this module (and the alert UI) expects.
    return -model.score_samples(X)


def train_lightgbm(train_df: pd.DataFrame, val_fraction: float = 0.15) -> lgb.LGBMClassifier:
    """Fits with early stopping against a time-based validation split
    carved from the tail of `train_df` -- never from holdout.

    This matters more than it looks: a fixed `n_estimators` tuned by eye
    against the holdout set's own PR-AUC would be a subtle form of
    leakage (the "holdout" stops being untouched the moment a
    hyperparameter gets picked to make its score look good). Early
    stopping on the validation PR-AUC found here that additional boosting
    rounds past the first do not generalize on this dataset -- the
    injected fraud patterns are extreme enough that a shallow model
    already separates them, and further boosting mostly overfits the
    ~300-object minority class. Reporting `best_iteration_` in the metrics
    output is the honest way to surface that rather than hide it.
    """
    import lightgbm as lgb

    inner_train, val = time_split(train_df, 1 - val_fraction)
    X_tr, y_tr = feature_matrix(inner_train).fillna(0.0), inner_train["isFraud"].astype(int)
    X_val, y_val = feature_matrix(val).fillna(0.0), val["isFraud"].astype(int)

    n_pos, n_neg = y_tr.sum(), len(y_tr) - y_tr.sum()
    scale_pos_weight = (n_neg / n_pos) if n_pos > 0 else 1.0

    model = lgb.LGBMClassifier(
        n_estimators=1000,
        max_depth=6,
        num_leaves=31,
        min_child_samples=20,
        learning_rate=0.05,
        scale_pos_weight=scale_pos_weight,
        random_state=42,
        verbosity=-1,
    )
    model.fit(
        X_tr,
        y_tr,
        eval_set=[(X_val, y_val)],
        eval_metric="average_precision",
        callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)],
    )
    return model


def lightgbm_scores(model: lgb.LGBMClassifier, df: pd.DataFrame) -> np.ndarray:
    X = feature_matrix(df).fillna(0.0)
    return model.predict_proba(X)[:, 1]


def rule_fired_flags(df: pd.DataFrame) -> np.ndarray:
    """One rule-engine pass over the dataframe, reused everywhere a
    rules-fired signal is needed (rules-only metrics and the hybrid
    comparison) instead of re-evaluating all 6 rules per row twice."""
    rules = load_rules()
    fired = np.empty(len(df), dtype=bool)
    for i, (_, row) in enumerate(df.iterrows()):
        context = {name: row[name] for name in FEATURE_NAMES}
        context["amount"] = row["amount"]
        context["deviceIsNew"] = bool(row["deviceIsNew"])
        fired[i] = len(evaluate_rules(rules, context)) > 0
    return fired


def evaluate_rules_on_holdout(fired_any: np.ndarray, labels: np.ndarray) -> dict:
    return {
        "alert_rate": float(fired_any.mean()),
        "precision": float(precision_score(labels, fired_any, zero_division=0)),
        "recall": float(recall_score(labels, fired_any, zero_division=0)),
    }


def false_positive_rate_at_recall(scores: np.ndarray, labels: np.ndarray, target_recall: float) -> float:
    """Sweeps thresholds to find the false-positive rate at the point
    where recall first reaches target_recall, for the model-only vs hybrid
    comparison at equal recall."""
    order = np.argsort(-scores)
    labels_sorted = labels[order]
    n_pos = labels.sum()
    if n_pos == 0:
        return float("nan")
    cum_tp = np.cumsum(labels_sorted)
    recall = cum_tp / n_pos
    idx = np.searchsorted(recall, target_recall)
    idx = min(idx, len(labels_sorted) - 1)
    n_flagged = idx + 1
    tp = cum_tp[idx]
    fp = n_flagged - tp
    n_neg = len(labels) - n_pos
    return float(fp / n_neg) if n_neg > 0 else float("nan")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Train and evaluate the PulseGuard detection layer")
    parser.add_argument("--input-jsonl", required=True, help="labelled transaction stream from the generator's simulate mode")
    parser.add_argument("--artifacts-dir", default=str(DEFAULT_ARTIFACTS_DIR))
    parser.add_argument("--reviews-per-analyst-per-day", type=float, default=40.0)
    parser.add_argument("--analyst-count", type=int, default=5)
    args = parser.parse_args(argv)

    artifacts_dir = Path(args.artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    print(f"Building feature dataset from {args.input_jsonl} ...")
    df = build_feature_dataset_from_jsonl(args.input_jsonl)
    print(f"  {len(df)} transactions, {df['isFraud'].sum()} labelled fraud ({df['isFraud'].mean():.4%})")

    train_df, holdout_df = time_split(df)
    span_days = (holdout_df["timestamp"].max() - holdout_df["timestamp"].min()).total_seconds() / 86400
    txns_per_day = len(holdout_df) / max(span_days, 1e-6)
    print(f"  train={len(train_df)} holdout={len(holdout_df)} (holdout spans {span_days:.1f} days, ~{txns_per_day:.0f} txns/day)")

    print("Training Isolation Forest (unsupervised, non-fraud population) ...")
    iso_model = train_isolation_forest(train_df)
    iso_scores_holdout = isolation_forest_scores(iso_model, holdout_df)

    print("Training LightGBM (supervised benchmark, early-stopped on a validation split) ...")
    lgb_model = train_lightgbm(train_df)
    lgb_scores_holdout = lightgbm_scores(lgb_model, holdout_df)
    print(f"  early stopping selected {lgb_model.best_iteration_} boosting round(s)")

    labels_holdout = holdout_df["isFraud"].to_numpy().astype(int)

    iso_pr_auc = average_precision_score(labels_holdout, iso_scores_holdout)
    lgb_pr_auc = average_precision_score(labels_holdout, lgb_scores_holdout)
    print(f"  Isolation Forest PR-AUC: {iso_pr_auc:.4f}")
    print(f"  LightGBM        PR-AUC: {lgb_pr_auc:.4f}")

    print("Evaluating rule engine on holdout ...")
    rules_fired = rule_fired_flags(holdout_df)
    rule_metrics = evaluate_rules_on_holdout(rules_fired, labels_holdout)
    print(f"  rules-only: precision={rule_metrics['precision']:.4f} recall={rule_metrics['recall']:.4f} alert_rate={rule_metrics['alert_rate']:.4%}")

    print("Selecting alert threshold from analyst review capacity ...")
    capacity = select_threshold_for_capacity(
        iso_scores_holdout, txns_per_day, args.reviews_per_analyst_per_day, args.analyst_count
    )
    threshold = capacity["threshold"]
    model_flagged = iso_scores_holdout >= threshold
    model_recall = recall_at_threshold(iso_scores_holdout, labels_holdout, threshold)
    model_precision = float(precision_score(labels_holdout, model_flagged, zero_division=0))
    print(f"  target {capacity['target_alerts_per_day']:.0f} alerts/day -> alert_rate={capacity['actual_alert_rate']:.5%}, threshold={threshold:.4f}")
    print(f"  model-only at this threshold: precision={model_precision:.4f} recall={model_recall:.4f}")

    hybrid_flagged = model_flagged | rules_fired
    hybrid_recall = float((hybrid_flagged & labels_holdout.astype(bool)).sum() / max(labels_holdout.sum(), 1))
    hybrid_precision = float(precision_score(labels_holdout, hybrid_flagged, zero_division=0))
    hybrid_alert_rate = float(hybrid_flagged.mean())

    model_only_fpr_at_hybrid_recall = false_positive_rate_at_recall(iso_scores_holdout, labels_holdout, hybrid_recall)
    hybrid_fpr = float((hybrid_flagged & ~labels_holdout.astype(bool)).sum() / max((~labels_holdout.astype(bool)).sum(), 1))

    print(f"  hybrid (rules OR model>=threshold): precision={hybrid_precision:.4f} recall={hybrid_recall:.4f} alert_rate={hybrid_alert_rate:.5%}")
    print(f"  false positive rate at matched recall ({hybrid_recall:.4f}): model-only={model_only_fpr_at_hybrid_recall:.4%} hybrid={hybrid_fpr:.4%}")

    print("Building alerts/day vs fraud-caught trade-off curve ...")
    curve = trade_off_curve(iso_scores_holdout, labels_holdout, txns_per_day)
    curve.to_csv(artifacts_dir / "threshold_tradeoff.csv", index=False)
    _plot_tradeoff_curve(curve, capacity, artifacts_dir / "threshold_tradeoff.png")

    non_fraud_train = feature_matrix(train_df[~train_df["isFraud"]]).fillna(0.0)
    feature_stats = {
        name: {"mean": float(non_fraud_train[name].mean()), "std": float(non_fraud_train[name].std() or 1.0)}
        for name in FEATURE_NAMES
    }

    joblib.dump(iso_model, artifacts_dir / "isolation_forest.joblib")
    lgb_model.booster_.save_model(str(artifacts_dir / "lightgbm.txt"))
    (artifacts_dir / "feature_names.json").write_text(json.dumps(list(FEATURE_NAMES), indent=2))
    (artifacts_dir / "feature_stats.json").write_text(json.dumps(feature_stats, indent=2))
    (artifacts_dir / "threshold.json").write_text(json.dumps(capacity, indent=2))

    metrics = {
        "dataset": {
            "total_transactions": int(len(df)),
            "fraud_count": int(df["isFraud"].sum()),
            "fraud_rate": float(df["isFraud"].mean()),
            "train_size": int(len(train_df)),
            "holdout_size": int(len(holdout_df)),
            "holdout_span_days": span_days,
            "holdout_txns_per_day": txns_per_day,
        },
        "isolation_forest": {"pr_auc": float(iso_pr_auc)},
        "lightgbm": {"pr_auc": float(lgb_pr_auc), "best_iteration": int(lgb_model.best_iteration_)},
        "rules_only": rule_metrics,
        "threshold_selection": capacity,
        "model_only_at_threshold": {"precision": model_precision, "recall": model_recall},
        "hybrid_at_threshold": {
            "precision": hybrid_precision,
            "recall": hybrid_recall,
            "alert_rate": hybrid_alert_rate,
        },
        "false_positive_rate_at_matched_recall": {
            "matched_recall": hybrid_recall,
            "model_only_fpr": model_only_fpr_at_hybrid_recall,
            "hybrid_fpr": hybrid_fpr,
        },
    }
    (artifacts_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))
    print(f"\nArtifacts written to {artifacts_dir}/")


def _plot_tradeoff_curve(curve: pd.DataFrame, capacity: dict, out_path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(curve["alerts_per_day"], curve["recall"], marker="o", markersize=3, label="operating curve")
    ax.axvline(capacity["target_alerts_per_day"], color="red", linestyle="--", label=f"chosen operating point (~{capacity['target_alerts_per_day']:.0f}/day)")
    ax.set_xscale("log")
    ax.set_xlabel("Alerts per day (log scale)")
    ax.set_ylabel("Fraud caught (recall)")
    ax.set_title("Alert volume vs. fraud caught")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    main()
