import lightgbm as lgb
import numpy as np
import pandas as pd
import pytest

pytest.importorskip("shap")

from pulseguard_detection.explain_report import booster_as_shap_model, compare_with_pred_contrib, independent_shap_values


@pytest.fixture(scope="module")
def booster_and_data():
    """Trained with NaN in column "a" only, so its splits get LightGBM
    missing_type "NaN" (a learned default direction) while splits on "b"
    and "c" get missing_type "None" (NaN is treated as 0.0) -- the scored
    data then puts NaN in all three, exercising both translations."""
    rng = np.random.default_rng(1)
    n = 3000
    X = pd.DataFrame({"a": rng.normal(size=n), "b": rng.normal(size=n), "c": rng.uniform(-1, 1, n)})
    X.loc[rng.random(n) < 0.15, "a"] = np.nan
    y = ((X["a"].fillna(2.0) > 0.8) | (X["b"] * X["c"] > 0.3)).astype(int)
    booster = lgb.train({"objective": "binary", "num_leaves": 8, "verbosity": -1, "seed": 1}, lgb.Dataset(X, y), num_boost_round=25)

    X_score = pd.DataFrame({"a": rng.normal(size=500), "b": rng.normal(size=500), "c": rng.uniform(-1, 1, 500)})
    for col in X_score:
        X_score.loc[rng.random(500) < 0.2, col] = np.nan
    return booster, X_score


def test_model_exercises_both_missing_value_modes(booster_and_data):
    booster, _ = booster_and_data
    def missing_types(node):
        if "leaf_value" in node:
            return set()
        return {node["missing_type"]} | missing_types(node["left_child"]) | missing_types(node["right_child"])
    seen = set().union(*(missing_types(t["tree_structure"]) for t in booster.dump_model()["tree_info"]))
    assert {"NaN", "None"} <= seen


def test_independent_treeshap_matches_pred_contrib_including_nan_rows(booster_and_data):
    booster, X = booster_and_data
    check = compare_with_pred_contrib(booster, X)
    assert check["rows_with_missing_values"] > 0
    assert check["max_abs_diff_feature_shap"] < 1e-9
    assert check["max_abs_diff_base_value"] < 1e-9
    assert check["max_additivity_error_shap_library"] < 1e-9


def test_check_is_not_circular(booster_and_data):
    # if the "independent" values secretly came from LightGBM, editing the
    # parsed trees couldn't change them. It must.
    booster, X = booster_and_data
    tampered = booster_as_shap_model(booster)
    leaves = np.where(tampered["trees"][0]["children_left"] == -1)[0]
    tampered["trees"][0]["values"][leaves[0], 0] += 0.5
    values, _ = independent_shap_values(booster, X, tampered)
    contrib = booster.predict(X, pred_contrib=True)
    assert np.abs(values - contrib[:, :-1]).max() > 1e-3
