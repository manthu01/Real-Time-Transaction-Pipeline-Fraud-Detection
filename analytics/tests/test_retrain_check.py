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


def test_evaluates_and_reports_without_promoting():
    df = _synthetic_bronze(n_rows=3000, fraud_rate=0.03, seed=2)
    result = retrain_and_compare(df, champion_artifacts_dir="/nonexistent")
    assert result["status"] == "EVALUATED"
    assert 0.0 <= result["challenger_pr_auc"] <= 1.0
    assert result["champion_pr_auc"] is None  # no champion artifacts at that path
    assert result["recommendation"] == "report only -- no auto-promotion"
    assert "challenger_better" not in result  # can't compare without a champion number


def test_compares_against_champion_when_metrics_file_exists(tmp_path):
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    (artifacts_dir / "metrics.json").write_text(json.dumps({"isolation_forest": {"pr_auc": 0.5}}))

    assert load_champion_pr_auc(artifacts_dir) == 0.5

    df = _synthetic_bronze(n_rows=3000, fraud_rate=0.03, seed=3)
    result = retrain_and_compare(df, champion_artifacts_dir=artifacts_dir)
    assert result["status"] == "EVALUATED"
    assert result["champion_pr_auc"] == 0.5
    assert "challenger_better" in result


def test_load_champion_pr_auc_missing_file_returns_none(tmp_path):
    assert load_champion_pr_auc(tmp_path / "does_not_exist") is None
