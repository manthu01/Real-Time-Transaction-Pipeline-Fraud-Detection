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
     correction. LightGBM is the live champion (it drives alerts); the
     Isolation Forest scores in shadow alongside it, because it needs no
     labels and so is the one that can still notice a fraud pattern
     nobody has labelled yet.
  4. Evaluate both on the HOLDOUT set: precision/recall/PR-AUC, never
     accuracy (meaningless at <1% prevalence).
  5. Evaluate the rule engine on the same holdout.
  6. Pick an alert threshold from analyst review capacity, not from an
     F1-optimal cutoff -- for each model separately -- and report
     model-only vs rules+model hybrid precision/recall at that threshold,
     plus the false-positive-rate difference at equal recall.
  7. Save both models, both thresholds, the trade-off curves (as CSV and
     a PNG chart), and metrics.json.
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
    hyperparameter gets picked to make its score look good). Reporting
    `best_iteration_` in the metrics output keeps that choice visible.

    No class reweighting (scale_pos_weight). An earlier version set it to
    n_neg/n_pos (~560 here), and early stopping then picked a single
    boosting round: that first heavily-weighted tree already pushed its
    fraud leaves to p~1.0, so the model emitted only ~7 distinct
    probabilities. PR-AUC is rank-based, so the reweighting bought no
    ranking quality (validation PR-AUC was ~0.98-0.99 either way), but it
    broke two things the live path depends on: the capacity-based
    threshold (a quantile over 7 values can only land on "alert on almost
    nothing" or "alert on everything") and the meaning of p shown to
    analysts. Unweighted, early stopping keeps ~80 rounds, scores are
    continuous, and p stays a probability.

    Missing values are passed through as NaN, NOT filled with 0.0 (unlike
    the Isolation Forest, which can't take NaN at all). The main one is
    secondsSincePrevTxn on a customer's first-ever transaction: 0.0 there
    would claim "zero seconds since the previous transaction" -- the
    signature of card testing -- for a transaction that has no previous
    one. LightGBM learns a default direction for missing values at every
    split, so NaN keeps "no history" distinct from "very fast". The live
    scorer (score.py) passes NaN the same way; filling here but not there
    would be training/serving skew.
    """
    import lightgbm as lgb

    inner_train, val = time_split(train_df, 1 - val_fraction)
    X_tr, y_tr = lightgbm_feature_matrix(inner_train), inner_train["isFraud"].astype(int)
    X_val, y_val = lightgbm_feature_matrix(val), val["isFraud"].astype(int)

    model = lgb.LGBMClassifier(
        n_estimators=1000,
        max_depth=6,
        num_leaves=31,
        min_child_samples=20,
        learning_rate=0.05,
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


def lightgbm_feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """feature_matrix with nulls kept as NaN (see train_lightgbm) and the
    boolean features as 0/1 floats -- the same float64 matrix the live
    scorer hands the Booster."""
    return feature_matrix(df).astype("float64")


def lightgbm_scores(model: lgb.LGBMClassifier, df: pd.DataFrame) -> np.ndarray:
    return model.predict_proba(lightgbm_feature_matrix(df))[:, 1]


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


def detection_rate_per_pattern(holdout_df: pd.DataFrame, flagged_by: dict[str, np.ndarray]) -> dict:
    """Recall broken down by which of the 6 injected fraud patterns each
    holdout fraud row actually is -- the build plan asks for this
    specifically because an aggregate recall number hides whether a
    detector is strong everywhere or only on the easy patterns. Restricted
    to rows where isFraud is true, since fraudPattern is null otherwise.

    `flagged_by` maps a detector name (e.g. "rules_only", "lightgbm_hybrid")
    to its per-row flags over holdout_df; each becomes a `<name>_recall`
    entry, so every detector is compared on the same fraud rows."""
    fraud_rows = holdout_df[holdout_df["isFraud"]]
    fraud_idx = fraud_rows.index.to_numpy()
    hits = {name: pd.Series(flags, index=holdout_df.index).loc[fraud_idx] for name, flags in flagged_by.items()}

    result = {}
    for pattern, group in fraud_rows.groupby("fraudPattern"):
        idx = group.index
        n = len(idx)
        result[pattern] = {"holdout_count": int(n)}
        for name, hit in hits.items():
            result[pattern][f"{name}_recall"] = float(hit.loc[idx].sum() / n)
    return result


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


def operating_point(scores: np.ndarray, labels: np.ndarray, rules_fired: np.ndarray, threshold: float) -> dict:
    """Everything the review team would feel at one threshold: model-only
    and hybrid (rules OR score >= threshold) precision/recall/alert rate,
    plus the model-only false-positive rate at the hybrid's recall."""
    is_fraud = labels.astype(bool)
    model_flagged = scores >= threshold
    hybrid_flagged = model_flagged | rules_fired
    hybrid_recall = float((hybrid_flagged & is_fraud).sum() / max(is_fraud.sum(), 1))
    return {
        "model_flagged": model_flagged,
        "hybrid_flagged": hybrid_flagged,
        "model_only": {
            "precision": float(precision_score(labels, model_flagged, zero_division=0)),
            "recall": recall_at_threshold(scores, labels, threshold),
            "alert_rate": float(model_flagged.mean()),
        },
        "hybrid": {
            "precision": float(precision_score(labels, hybrid_flagged, zero_division=0)),
            "recall": hybrid_recall,
            "alert_rate": float(hybrid_flagged.mean()),
        },
        "false_positive_rate_at_matched_recall": {
            "matched_recall": hybrid_recall,
            "model_only_fpr": false_positive_rate_at_recall(scores, labels, hybrid_recall),
            "hybrid_fpr": float((hybrid_flagged & ~is_fraud).sum() / max((~is_fraud).sum(), 1)),
        },
    }


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

    print("Training LightGBM (champion, early-stopped on a validation split) ...")
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

    print("Selecting alert thresholds from analyst review capacity (per model) ...")
    capacity_args = (txns_per_day, args.reviews_per_analyst_per_day, args.analyst_count)
    capacities = {
        "lightgbm": select_threshold_for_capacity(lgb_scores_holdout, *capacity_args),
        "isolation_forest": select_threshold_for_capacity(iso_scores_holdout, *capacity_args),
    }
    holdout_scores = {"lightgbm": lgb_scores_holdout, "isolation_forest": iso_scores_holdout}
    print(f"  target {capacities['lightgbm']['target_alerts_per_day']:.0f} alerts/day -> target alert_rate={capacities['lightgbm']['target_alert_rate']:.5%}")

    operating_points = {}
    for name, capacity in capacities.items():
        op = operating_point(holdout_scores[name], labels_holdout, rules_fired, capacity["threshold"])
        operating_points[name] = op
        fpr = op["false_positive_rate_at_matched_recall"]
        print(f"  {name}: threshold={capacity['threshold']:.4f} actual alert_rate={capacity['actual_alert_rate']:.5%}")
        if capacity["actual_alert_rate"] > 2 * capacity["target_alert_rate"]:
            # tied scores at the quantile (a model with few distinct outputs)
            # make "score >= threshold" overshoot the capacity it was picked for
            print(f"    WARNING: actual alert rate is over 2x the capacity target -- {name}'s scores are too tied for a capacity threshold")
        print(f"    model-only: precision={op['model_only']['precision']:.4f} recall={op['model_only']['recall']:.4f}")
        print(f"    hybrid (rules OR model>=threshold): precision={op['hybrid']['precision']:.4f} recall={op['hybrid']['recall']:.4f} alert_rate={op['hybrid']['alert_rate']:.5%}")
        print(f"    false positive rate at matched recall ({fpr['matched_recall']:.4f}): model-only={fpr['model_only_fpr']:.4%} hybrid={fpr['hybrid_fpr']:.4%}")

    print("Breaking down detection rate per injected fraud pattern ...")
    per_pattern = detection_rate_per_pattern(
        holdout_df,
        {
            "rules_only": rules_fired,
            "isolation_forest_hybrid": operating_points["isolation_forest"]["hybrid_flagged"],
            "lightgbm_only": operating_points["lightgbm"]["model_flagged"],
            "lightgbm_hybrid": operating_points["lightgbm"]["hybrid_flagged"],
        },
    )
    for pattern, stats in sorted(per_pattern.items()):
        recalls = " ".join(f"{k}={v:.4f}" for k, v in stats.items() if k != "holdout_count")
        print(f"  {pattern}: n={stats['holdout_count']} {recalls}")

    print("Building alerts/day vs fraud-caught trade-off curves ...")
    curves = pd.concat(
        [trade_off_curve(holdout_scores[name], labels_holdout, txns_per_day).assign(model=name) for name in capacities],
        ignore_index=True,
    )
    curves.to_csv(artifacts_dir / "threshold_tradeoff.csv", index=False)
    _plot_tradeoff_curve(curves, capacities["lightgbm"], artifacts_dir / "threshold_tradeoff.png")

    non_fraud_train = feature_matrix(train_df[~train_df["isFraud"]]).fillna(0.0)
    feature_stats = {
        name: {"mean": float(non_fraud_train[name].mean()), "std": float(non_fraud_train[name].std() or 1.0)}
        for name in FEATURE_NAMES
    }

    # what the live scorer reads: LightGBM's threshold drives alerts,
    # iso_threshold is only used to put the shadow score in context.
    threshold_doc = {
        "model": "lightgbm",
        "threshold": capacities["lightgbm"]["threshold"],
        "iso_threshold": capacities["isolation_forest"]["threshold"],
        "target_alerts_per_day": capacities["lightgbm"]["target_alerts_per_day"],
        "target_alert_rate": capacities["lightgbm"]["target_alert_rate"],
        "actual_alert_rate": capacities["lightgbm"]["actual_alert_rate"],
        "iso_actual_alert_rate": capacities["isolation_forest"]["actual_alert_rate"],
    }

    joblib.dump(iso_model, artifacts_dir / "isolation_forest.joblib")
    lgb_model.booster_.save_model(str(artifacts_dir / "lightgbm.txt"))
    (artifacts_dir / "feature_names.json").write_text(json.dumps(list(FEATURE_NAMES), indent=2))
    (artifacts_dir / "feature_stats.json").write_text(json.dumps(feature_stats, indent=2))
    (artifacts_dir / "threshold.json").write_text(json.dumps(threshold_doc, indent=2))

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
        "champion_model": "lightgbm",
        "isolation_forest": {"pr_auc": float(iso_pr_auc)},
        "lightgbm": {"pr_auc": float(lgb_pr_auc), "best_iteration": int(lgb_model.best_iteration_)},
        "rules_only": rule_metrics,
        "threshold_selection": capacities,
        "at_capacity_threshold": {
            "isolation_forest_only": operating_points["isolation_forest"]["model_only"],
            "isolation_forest_hybrid": operating_points["isolation_forest"]["hybrid"],
            "lightgbm_only": operating_points["lightgbm"]["model_only"],
            "lightgbm_hybrid": operating_points["lightgbm"]["hybrid"],
        },
        "false_positive_rate_at_matched_recall": {
            name: op["false_positive_rate_at_matched_recall"] for name, op in operating_points.items()
        },
        "detection_rate_per_pattern": per_pattern,
    }
    (artifacts_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))
    print(f"\nArtifacts written to {artifacts_dir}/")


def _plot_tradeoff_curve(curves: pd.DataFrame, capacity: dict, out_path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels = {"lightgbm": "LightGBM (champion)", "isolation_forest": "Isolation Forest (shadow)"}
    fig, ax = plt.subplots(figsize=(8, 5))
    for model, curve in curves.groupby("model", sort=False):
        ax.plot(curve["alerts_per_day"], curve["recall"], marker="o", markersize=3, label=labels.get(model, model))
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
