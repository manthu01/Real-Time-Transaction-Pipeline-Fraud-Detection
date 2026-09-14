"""Batch feature backfill: replays historical transactions through the
exact same per-customer feature function the streaming job uses.

This is the mechanism, not just the intent, behind "no training/serving
skew": `pulseguard_streaming.feature_state.apply_event` has no Spark
dependency, so this script imports and calls it directly -- the training
set is built from the identical function the live pipeline runs inside
`applyInPandasWithState`. There is no second implementation of "what a
feature means" anywhere in this codebase.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Iterable, Iterator

import pandas as pd

from pulseguard_streaming.feature_state import FEATURE_NAMES, apply_event, new_state

RAW_PASSTHROUGH_FIELDS = (
    "txnId",
    "customerId",
    "timestamp",
    "amount",
    "direction",
    "channel",
    "merchantId",
    "merchantCategory",
    "deviceId",
    "deviceIsNew",
    "city",
    "isFraud",
    "fraudPattern",
)


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def read_jsonl(path: str) -> Iterator[dict[str, Any]]:
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def build_feature_dataset(records: Iterable[dict[str, Any]]) -> pd.DataFrame:
    """Groups records by customerId, replays each customer's history in
    timestamp order through apply_event, and returns one row per
    transaction with all 26 features plus the raw/label columns."""
    by_customer: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        by_customer.setdefault(record["customerId"], []).append(record)

    rows: list[dict[str, Any]] = []
    for customer_id, events in by_customer.items():
        events.sort(key=lambda e: e["timestamp"])
        state = new_state()
        for raw in events:
            event = {
                "timestamp": _parse_timestamp(raw["timestamp"]),
                "amount": float(raw["amount"]),
                "merchantId": raw["merchantId"],
                "merchantCategory": raw["merchantCategory"],
                "deviceId": raw["deviceId"],
                "city": raw["city"],
                "lat": float(raw["lat"]),
                "lon": float(raw["lon"]),
            }
            state, features = apply_event(state, event)

            row = {field: raw.get(field) for field in RAW_PASSTHROUGH_FIELDS}
            row["timestamp"] = event["timestamp"]  # parsed datetime, not the raw ISO string
            row.update(features)
            rows.append(row)

    df = pd.DataFrame(rows)
    return df.sort_values("timestamp").reset_index(drop=True)


def build_feature_dataset_from_jsonl(path: str) -> pd.DataFrame:
    return build_feature_dataset(read_jsonl(path))


LABEL_COLUMNS = ("isFraud", "fraudPattern")
NON_FEATURE_COLUMNS = RAW_PASSTHROUGH_FIELDS  # everything not in FEATURE_NAMES


def feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """The actual matrix a model trains/scores on -- deliberately excludes
    every raw/label column. This is the one place label leakage would show
    up, so it's also where `test_dataset.py` checks for it."""
    return df[list(FEATURE_NAMES)]
