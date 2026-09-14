import pandas as pd

from pulseguard_detection.dataset import FEATURE_NAMES, build_feature_dataset, feature_matrix


def _record(customer_id, ts, amount=500.0, is_fraud=False, fraud_pattern=None, **overrides):
    base = {
        "txnId": f"txn-{ts}",
        "customerId": customer_id,
        "timestamp": ts,
        "amount": amount,
        "direction": "DEBIT",
        "channel": "UPI",
        "merchantId": "M001",
        "merchantCategory": "GROCERY",
        "deviceId": "D-1",
        "deviceIsNew": False,
        "lat": 19.0760,
        "lon": 72.8777,
        "city": "Mumbai",
        "isFraud": is_fraud,
        "fraudPattern": fraud_pattern,
    }
    base.update(overrides)
    return base


def test_builds_one_row_per_transaction_with_all_features():
    records = [
        _record("C1", "2026-03-10T10:00:00Z"),
        _record("C1", "2026-03-10T10:05:00Z"),
        _record("C2", "2026-03-10T10:01:00Z"),
    ]
    df = build_feature_dataset(records)
    assert len(df) == 3
    for name in FEATURE_NAMES:
        assert name in df.columns


def test_replays_each_customer_independently_in_timestamp_order():
    # out-of-order input for C1 -- must still be replayed chronologically
    records = [
        _record("C1", "2026-03-10T10:05:00Z", amount=999.0),
        _record("C1", "2026-03-10T10:00:00Z", amount=1.0),
    ]
    df = build_feature_dataset(records)
    first_row = df.iloc[0]
    assert first_row["amount"] == 1.0
    assert first_row["secondsSincePrevTxn"] is None or pd.isna(first_row["secondsSincePrevTxn"])


def test_feature_matrix_excludes_labels_and_raw_identifiers():
    records = [_record("C1", "2026-03-10T10:00:00Z", is_fraud=True, fraud_pattern="MULE_ACCOUNT")]
    df = build_feature_dataset(records)
    matrix = feature_matrix(df)
    assert "isFraud" not in matrix.columns
    assert "fraudPattern" not in matrix.columns
    assert "customerId" not in matrix.columns
    assert "txnId" not in matrix.columns
    assert list(matrix.columns) == list(FEATURE_NAMES)
