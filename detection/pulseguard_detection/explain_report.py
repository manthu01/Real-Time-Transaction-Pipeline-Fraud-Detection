"""Offline SHAP report for the LightGBM champion.

    python -m pulseguard_detection.explain_report --input-jsonl data/training_raw.jsonl

Two jobs:
  1. Cross-check. The live scorer never imports `shap` -- it reads TreeSHAP
     values straight out of LightGBM (`predict(..., pred_contrib=True)`),
     which keeps the Spark image free of shap's dependency tree. This
     report recomputes the same values on the whole holdout set with an
     independent implementation and fails loudly if the two disagree
     beyond float error, so "the console's explanations are SHAP values"
     is something checked, not assumed.

     "Independent" needed care: handed a LightGBM Booster,
     `shap.TreeExplainer.shap_values` does NOT run shap's own algorithm --
     it calls `booster.predict(pred_contrib=True)`, the very function under
     test (the first version of this report "matched" with a difference of
     exactly 0.0, which is what gave it away). So the check here parses the
     trees out of `booster.dump_model()` into shap's generic tree format,
     translating LightGBM's missing-value routing into shap's default-child
     convention, and runs shap's own C++ TreeSHAP on that. The plots can
     use the Booster path -- after the check, its values are verified.
  2. Global picture. Per-alert waterfalls explain one transaction; the
     mean-|SHAP| importance chart and the beeswarm show what drives the
     model across the whole holdout set (and, separately, across its fraud
     rows), written to artifacts/ for the README.

Uses the same time split as train.py, so the holdout here is exactly the
holdout the reported metrics come from -- the model never trained on it.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from pulseguard_detection.dataset import FEATURE_NAMES, build_feature_dataset_from_jsonl
from pulseguard_detection.train import DEFAULT_ARTIFACTS_DIR, lightgbm_feature_matrix, time_split

# float64 accumulation in two different TreeSHAP implementations; anything
# near this is rounding, anything well past it means they disagree
MATCH_TOLERANCE = 1e-6
BEESWARM_SAMPLE_SIZE = 5000


def shap_values_from_library(booster, X: pd.DataFrame) -> tuple[np.ndarray, float]:
    """SHAP values (log-odds, one column per feature) and the base value,
    via shap's Booster path -- which delegates to LightGBM's pred_contrib
    (see the module docstring), so it's only used for plotting, after
    compare_with_pred_contrib has verified those values. shap has returned binary
    LightGBM output both as one array and as a [class0, class1] list
    across versions; both are normalised to the class-1 (fraud) array."""
    import shap

    explainer = shap.TreeExplainer(booster)
    values = explainer.shap_values(X)
    base = explainer.expected_value
    if isinstance(values, list):
        values = values[1]
    if np.ndim(base) > 0:
        base = np.ravel(base)[-1]
    return np.asarray(values, dtype=np.float64), float(base)


def _tree_to_shap_dict(tree_structure: dict) -> dict:
    """One LightGBM tree (from dump_model) as shap's generic tree dict:
    flat arrays indexed by node id, leaves marked by children == -1.

    The part that matters is missing values. shap's TreeSHAP sends a NaN
    feature down `children_default`; LightGBM decides per split by its
    `missing_type`:
      - "NaN":  NaN goes the learned default direction (`default_left`)
      - "None": the split never saw a missing value; LightGBM converts NaN
                to 0.0 and compares, so the default is wherever 0.0 goes
      - "Zero": 0.0 AND NaN are both "missing" -- not expressible in
                shap's format (0.0 isn't NaN), so it's refused rather than
                silently mistranslated
    Non-missing values: LightGBM goes left when x <= threshold, the same
    rule as shap's threshold type 0."""
    children_left, children_right, children_default = [], [], []
    features, thresholds, values, weights = [], [], [], []

    def add(node: dict) -> int:
        idx = len(features)
        for column in (children_left, children_right, children_default, features):
            column.append(-1)
        thresholds.append(0.0)
        values.append(0.0)
        weights.append(0.0)
        if "leaf_value" in node:
            values[idx] = float(node["leaf_value"])
            weights[idx] = float(node["leaf_count"])
            return idx
        if node["decision_type"] != "<=":
            raise NotImplementedError(f"unsupported LightGBM split {node['decision_type']!r} (categorical splits aren't translated)")
        if node["missing_type"] == "Zero":
            raise NotImplementedError("LightGBM missing_type 'Zero' (0.0 treated as missing) has no shap equivalent")
        features[idx] = int(node["split_feature"])
        thresholds[idx] = float(node["threshold"])
        weights[idx] = float(node["internal_count"])
        left = add(node["left_child"])
        right = add(node["right_child"])
        children_left[idx], children_right[idx] = left, right
        if node["missing_type"] == "NaN":
            children_default[idx] = left if node["default_left"] else right
        else:  # "None"
            children_default[idx] = left if 0.0 <= thresholds[idx] else right
        return idx

    add(tree_structure)
    return {
        "children_left": np.array(children_left),
        "children_right": np.array(children_right),
        "children_default": np.array(children_default),
        "features": np.array(features),
        "thresholds": np.array(thresholds, dtype=np.float64),
        "values": np.array(values, dtype=np.float64).reshape(-1, 1),
        "node_sample_weight": np.array(weights, dtype=np.float64),
    }


def booster_as_shap_model(booster) -> dict:
    """The whole booster in shap's dict-model format. Leaf values in the
    dump already include shrinkage and the boost-from-average offset, so
    the trees sum to the raw margin with base_offset 0."""
    trees = [_tree_to_shap_dict(t["tree_structure"]) for t in booster.dump_model()["tree_info"]]
    return {"trees": trees, "base_offset": 0.0, "tree_output": "log_odds", "objective": "binary_crossentropy"}


def independent_shap_values(booster, X: pd.DataFrame, shap_model: dict | None = None) -> tuple[np.ndarray, float]:
    """SHAP values computed by shap's own TreeSHAP over the parsed trees --
    no call into LightGBM's predict anywhere."""
    import shap

    explainer = shap.TreeExplainer(shap_model or booster_as_shap_model(booster))
    values = np.asarray(explainer.shap_values(X.to_numpy(dtype=np.float64)), dtype=np.float64)
    return values.reshape(len(X), -1), float(np.ravel(explainer.expected_value)[0])


def compare_with_pred_contrib(booster, X: pd.DataFrame, shap_model: dict | None = None) -> dict:
    """Max absolute difference between the independent TreeSHAP values and
    LightGBM's pred_contrib (per-feature and base value), plus how far
    each one's sum is from the model's raw margin (the additivity check)."""
    contrib = booster.predict(X, pred_contrib=True)
    margin = booster.predict(X, raw_score=True)
    lib_values, lib_base = independent_shap_values(booster, X, shap_model)
    return {
        "rows": int(len(X)),
        "rows_with_missing_values": int(X.isna().any(axis=1).sum()),
        "max_abs_diff_feature_shap": float(np.abs(lib_values - contrib[:, :-1]).max()),
        "max_abs_diff_base_value": float(np.abs(lib_base - contrib[:, -1]).max()),
        "max_additivity_error_pred_contrib": float(np.abs(contrib.sum(axis=1) - margin).max()),
        "max_additivity_error_shap_library": float(np.abs(lib_values.sum(axis=1) + lib_base - margin).max()),
        "base_value": float(contrib[0, -1]),
    }


def mean_abs_shap(values: np.ndarray) -> dict[str, float]:
    return dict(sorted(zip(FEATURE_NAMES, np.abs(values).mean(axis=0).tolist()), key=lambda kv: -kv[1]))


def beeswarm_sample(holdout_df: pd.DataFrame, size: int = BEESWARM_SAMPLE_SIZE, seed: int = 42) -> pd.DataFrame:
    """Every fraud row plus a random sample of the rest: at 0.13% fraud a
    plain random sample would show ~7 fraud dots, and fraud is what the
    plot is for. The plot says it's a sample; importances use every row."""
    fraud = holdout_df[holdout_df["isFraud"]]
    rest = holdout_df[~holdout_df["isFraud"]]
    rest = rest.sample(n=min(len(rest), max(size - len(fraud), 0)), random_state=seed)
    return pd.concat([fraud, rest])


def _plot_importance(importance_all: dict, importance_fraud: dict, out_path: Path, top_k: int = 15) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    names = list(importance_fraud)[:top_k][::-1]
    y = np.arange(len(names))
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(y + 0.2, [importance_fraud[n] for n in names], height=0.4, color="#d62728", label="fraud rows")
    ax.barh(y - 0.2, [importance_all[n] for n in names], height=0.4, color="#7f7f7f", label="all holdout rows")
    ax.set_yticks(y)
    ax.set_yticklabels(names)
    ax.set_xlabel("mean |SHAP value| (log-odds)")
    ax.set_title(f"LightGBM global feature importance (top {len(names)}, holdout)")
    ax.legend(loc="lower right")
    ax.grid(True, axis="x", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _plot_beeswarm(values: np.ndarray, X: pd.DataFrame, n_fraud: int, out_path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shap

    shap.summary_plot(values, X, max_display=15, show=False, plot_size=(9, 7))
    fig = plt.gcf()
    fig.axes[0].set_xlabel("SHAP value (log-odds; > 0 pushes toward fraud)")
    fig.suptitle(f"LightGBM SHAP beeswarm: all {n_fraud} holdout fraud rows + {len(X) - n_fraud} sampled others", fontsize=11)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main(argv: list[str] | None = None) -> None:
    import lightgbm as lgb

    parser = argparse.ArgumentParser(description="Offline SHAP report for the LightGBM champion")
    parser.add_argument("--input-jsonl", required=True, help="the same labelled stream train.py was run on")
    parser.add_argument("--artifacts-dir", default=str(DEFAULT_ARTIFACTS_DIR))
    args = parser.parse_args(argv)
    artifacts_dir = Path(args.artifacts_dir)

    booster = lgb.Booster(model_file=str(artifacts_dir / "lightgbm.txt"))
    print(f"Building feature dataset from {args.input_jsonl} ...")
    _, holdout_df = time_split(build_feature_dataset_from_jsonl(args.input_jsonl))
    X = lightgbm_feature_matrix(holdout_df)
    print(f"  holdout: {len(X)} rows, {int(holdout_df['isFraud'].sum())} fraud")

    print("Cross-checking shap's own TreeSHAP (on trees parsed from the model dump) against LightGBM pred_contrib ...")
    check = compare_with_pred_contrib(booster, X)
    for key, value in check.items():
        print(f"  {key}: {value}")
    mismatch = max(check["max_abs_diff_feature_shap"], check["max_abs_diff_base_value"])
    if mismatch > MATCH_TOLERANCE:
        raise SystemExit(f"shap and pred_contrib disagree by {mismatch:.3g} (> {MATCH_TOLERANCE}) -- the live explanations are not the SHAP values they claim to be")
    print(f"  OK: they agree within {MATCH_TOLERANCE} on every row")

    values, _ = shap_values_from_library(booster, X)
    is_fraud = holdout_df["isFraud"].to_numpy()
    importance_all = mean_abs_shap(values)
    importance_fraud = mean_abs_shap(values[is_fraud])
    print("Top features by mean |SHAP| on fraud rows:")
    for name, v in list(importance_fraud.items())[:8]:
        print(f"  {name:32s} fraud={v:.3f} all={importance_all[name]:.3f}")

    _plot_importance(importance_all, importance_fraud, artifacts_dir / "shap_importance.png")
    sample = beeswarm_sample(holdout_df)
    X_sample = lightgbm_feature_matrix(sample)
    sample_values, _ = shap_values_from_library(booster, X_sample)
    _plot_beeswarm(sample_values, X_sample, int(sample["isFraud"].sum()), artifacts_dir / "shap_beeswarm.png")

    report = {
        "cross_check": {
            **check,
            "method": "shap's own TreeSHAP over trees parsed from booster.dump_model(), vs LightGBM pred_contrib",
            "tolerance": MATCH_TOLERANCE,
            "passed": True,
        },
        "mean_abs_shap_all_holdout": importance_all,
        "mean_abs_shap_fraud_rows": importance_fraud,
        "beeswarm_sample": {"rows": int(len(sample)), "fraud_rows": int(sample["isFraud"].sum())},
    }
    (artifacts_dir / "shap_report.json").write_text(json.dumps(report, indent=2))
    print(f"\nWrote shap_importance.png, shap_beeswarm.png, shap_report.json to {artifacts_dir}/")


if __name__ == "__main__":
    main()
