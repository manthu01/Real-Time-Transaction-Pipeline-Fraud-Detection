import json

import numpy as np
import pandas as pd

from pulseguard_detection.dataset import FEATURE_NAMES
from pulseguard_analytics.retrain_check import load_champion_pr_auc, retrain_and_compare


def _synthetic_bronze(n_rows: int = 2000, fraud_rate: float = 0.02, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    timestamps = pd.date_range("2026-01-01", periods=n_rows, freq="min", tz="UTC")
    is_fraud = rng.random(n_rows) < fraud_rate

    data = {name: rng.normal(size=n_rows) for name in FEATURE_NAMES}
    # make fraud rows genuinely separable so the model has something real to learn
    for name in FEATURE_NAMES:
        data[name] = np.where(is_fraud, data[name] + 5, data[name])

    df = pd.DataFrame(data)
    df["timestamp"] = timestamps
    df["isFraud"] = is_fraud
    return df


def test_skips_when_required_columns_missing():
    df = pd.DataFrame({"timestamp": [1, 2], "isFraud": [False, True]})
    result = retrain_and_compare(df, champion_artifacts_dir="/nonexistent")
    assert result["status"] == "SKIPPED"
    assert "missing columns" in result["reason"]


def test_skips_when_too_few_fraud_rows():
    df = _synthetic_bronze(n_rows=500, fraud_rate=0.001, seed=1)  # ~0-1 fraud rows
    result = retrain_and_compare(df, champion_artifacts_dir="/nonexistent")
    assert result["status"] == "SKIPPED"
    assert "fewer than 5" in result["reason"]


def test_evaluates_both_deployed_models_and_reports_without_promoting():
    df = _synthetic_bronze(n_rows=3000, fraud_rate=0.03, seed=2)
    result = retrain_and_compare(df, champion_artifacts_dir="/nonexistent")
    assert result["status"] == "EVALUATED"
    assert result["recommendation"] == "report only -- no auto-promotion"
    assert set(result["models"]) == {"lightgbm", "isolation_forest"}
    assert result["models"]["lightgbm"]["role"] == "champion"
    assert result["models"]["isolation_forest"]["role"] == "shadow"
    for entry in result["models"].values():
        assert entry["status"] == "EVALUATED"
        assert 0.0 <= entry["challenger_pr_auc"] <= 1.0
        assert entry["deployed_pr_auc"] is None  # no artifacts at that path
        assert "challenger_better" not in entry  # can't compare without a deployed number


def test_each_challenger_is_compared_against_its_own_deployed_model(tmp_path):
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    (artifacts_dir / "metrics.json").write_text(
        json.dumps({"lightgbm": {"pr_auc": 0.9}, "isolation_forest": {"pr_auc": 0.5}})
    )

    assert load_champion_pr_auc(artifacts_dir) == 0.9  # the champion is LightGBM, not the Isolation Forest

    df = _synthetic_bronze(n_rows=3000, fraud_rate=0.03, seed=3)
    result = retrain_and_compare(df, champion_artifacts_dir=artifacts_dir)
    assert result["models"]["lightgbm"]["deployed_pr_auc"] == 0.9
    assert result["models"]["isolation_forest"]["deployed_pr_auc"] == 0.5
    assert all("challenger_better" in entry for entry in result["models"].values())


def test_lightgbm_challenger_skips_when_its_validation_split_has_no_fraud():
    df = _synthetic_bronze(n_rows=3000, fraud_rate=0.03, seed=4)
    # clear fraud out of the slice train_lightgbm early-stops on (the tail of
    # the train split), keeping it everywhere else
    train_end = int(len(df) * 0.75)
    val_start = int(train_end * 0.85)
    df.loc[val_start - 5 : train_end + 5, "isFraud"] = False
    result = retrain_and_compare(df, champion_artifacts_dir="/nonexistent")
    assert result["models"]["lightgbm"]["status"] == "SKIPPED"
    assert "validation split" in result["models"]["lightgbm"]["reason"]
    assert result["models"]["isolation_forest"]["status"] == "EVALUATED"  # the other check still runs


def test_load_champion_pr_auc_missing_file_returns_none(tmp_path):
    assert load_champion_pr_auc(tmp_path / "does_not_exist") is None
