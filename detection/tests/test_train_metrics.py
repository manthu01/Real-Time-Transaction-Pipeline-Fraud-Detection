import numpy as np

from pulseguard_detection.train import false_positive_rate_at_recall, time_split
import pandas as pd


def test_false_positive_rate_at_recall_perfect_separation():
    # fraud always scores higher than non-fraud -> catching all fraud (recall=1)
    # requires flagging exactly the fraud rows -> zero false positives
    scores = np.array([0.1, 0.2, 0.3, 0.9, 0.95])
    labels = np.array([0, 0, 0, 1, 1])
    fpr = false_positive_rate_at_recall(scores, labels, target_recall=1.0)
    assert fpr == 0.0


def test_false_positive_rate_at_recall_worst_case_ordering():
    # fraud always scores lowest -> catching all fraud means flagging everyone
    scores = np.array([0.9, 0.8, 0.7, 0.1, 0.05])
    labels = np.array([0, 0, 0, 1, 1])
    fpr = false_positive_rate_at_recall(scores, labels, target_recall=1.0)
    assert fpr == 1.0


def test_time_split_produces_strictly_ordered_train_before_holdout():
    df = pd.DataFrame({"timestamp": pd.date_range("2026-01-01", periods=100, freq="h")})
    train, holdout = time_split(df, train_fraction=0.75)
    assert train["timestamp"].max() <= holdout["timestamp"].min()
    assert len(train) + len(holdout) == len(df)
