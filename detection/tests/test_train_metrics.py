import numpy as np

from pulseguard_detection.train import detection_rate_per_pattern, false_positive_rate_at_recall, operating_point, time_split
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


def test_time_split_does_not_depend_on_row_order():
    # bronze rows come back from Spark unsorted; the split must still be
    # 75/25 by time, identical to splitting the sorted frame
    df = pd.DataFrame({"timestamp": pd.date_range("2026-01-01", periods=1000, freq="min")})
    shuffled = df.sample(frac=1.0, random_state=0)
    train, holdout = time_split(shuffled, train_fraction=0.75)
    assert len(train) == 750 and len(holdout) == 250
    assert train["timestamp"].max() < holdout["timestamp"].min()
    sorted_train, _ = time_split(df, train_fraction=0.75)
    assert set(train.index) == set(sorted_train.index)


def test_detection_rate_per_pattern_splits_recall_by_injected_pattern():
    holdout_df = pd.DataFrame(
        {
            "isFraud": [True, True, True, False, False],
            "fraudPattern": ["CARD_TESTING", "CARD_TESTING", "MULE_ACCOUNT", None, None],
        }
    )
    # hybrid catches both CARD_TESTING rows but misses the MULE_ACCOUNT row;
    # rules alone catch only one of the two CARD_TESTING rows.
    hybrid_flagged = np.array([True, True, False, False, True])
    rules_fired = np.array([True, False, False, False, False])

    result = detection_rate_per_pattern(holdout_df, {"hybrid": hybrid_flagged, "rules_only": rules_fired})

    assert result["CARD_TESTING"] == {"holdout_count": 2, "hybrid_recall": 1.0, "rules_only_recall": 0.5}
    assert result["MULE_ACCOUNT"] == {"holdout_count": 1, "hybrid_recall": 0.0, "rules_only_recall": 0.0}
    assert "None" not in result and None not in result


def test_operating_point_hybrid_adds_rule_only_catches():
    # model catches fraud row 3 only; rules catch fraud row 4 plus a false positive at row 0
    scores = np.array([0.1, 0.2, 0.3, 0.9, 0.4])
    labels = np.array([0, 0, 0, 1, 1])
    rules_fired = np.array([True, False, False, False, True])

    op = operating_point(scores, labels, rules_fired, threshold=0.5)

    assert op["model_only"] == {"precision": 1.0, "recall": 0.5, "alert_rate": 0.2}
    assert op["hybrid"]["recall"] == 1.0
    assert op["hybrid"]["precision"] == 2 / 3
    assert op["hybrid"]["alert_rate"] == 0.6
    # the two fraud rows are also the two highest scores, so the model alone
    # reaches the hybrid's recall with zero false positives
    assert op["false_positive_rate_at_matched_recall"]["model_only_fpr"] == 0.0
    assert op["false_positive_rate_at_matched_recall"]["hybrid_fpr"] == 1 / 3
