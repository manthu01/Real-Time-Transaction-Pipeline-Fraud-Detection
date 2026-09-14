"""Serializes the per-customer state dict (from feature_state.py) to/from a
single JSON string.

This is the key simplification that keeps the Spark-side plumbing
tractable: rather than mapping every nested Python structure (sets, dicts
of datetimes, lists of dicts) onto native Spark ArrayType/MapType/StructType
fields inside applyInPandasWithState's state schema -- and fighting
Arrow/Spark type-coercion edge cases for each one -- the Spark state schema
is a single StringType field holding a JSON blob. Everything past that
boundary is plain Python that's already covered by feature_state.py's own
unit tests. The cost is a bit of CPU and storage versus a binary encoding;
for this project's throughput targets that's the right trade, and it's
worth saying so out loud in an interview rather than pretending it's free.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any


def serialize_state(state: dict[str, Any]) -> str:
    payload = dict(state)
    payload["event_history"] = [
        {**e, "timestamp": e["timestamp"].isoformat()} for e in state["event_history"]
    ]
    payload["visited_categories"] = sorted(state["visited_categories"])
    payload["known_devices"] = {d: t.isoformat() for d, t in state["known_devices"].items()}
    payload["last_txn_time"] = state["last_txn_time"].isoformat() if state["last_txn_time"] else None
    return json.dumps(payload)


def deserialize_state(raw: str) -> dict[str, Any]:
    payload = json.loads(raw)
    payload["event_history"] = [
        {**e, "timestamp": datetime.fromisoformat(e["timestamp"])} for e in payload["event_history"]
    ]
    payload["visited_categories"] = set(payload["visited_categories"])
    payload["known_devices"] = {d: datetime.fromisoformat(t) for d, t in payload["known_devices"].items()}
    payload["last_txn_time"] = datetime.fromisoformat(payload["last_txn_time"]) if payload["last_txn_time"] else None
    return payload
