"""Spark-side adapter for pulseguard_detection.score.score_transaction.

The scoring function itself (rules + Isolation Forest + explainability)
lives in pulseguard_detection and has no Spark dependency -- the same "one
function, called from everywhere" pattern as feature_state.apply_event.
This module's only job is gluing that into `mapInPandas`: lazily loading
the trained artifacts once per worker process (not once per micro-batch --
unpickling an IsolationForest on every batch would be needless overhead)
and reshaping "maybe an alert" into the DataFrame `mapInPandas` expects.
"""

from __future__ import annotations

import uuid
from typing import Iterator

import pandas as pd

from pulseguard_streaming.output_schema import ALERT_COLUMN_NAMES

_ARTIFACTS_CACHE: dict[str, object] = {}


def _get_artifacts(artifacts_dir: str):
    if artifacts_dir not in _ARTIFACTS_CACHE:
        from pulseguard_detection.score import load_artifacts

        _ARTIFACTS_CACHE[artifacts_dir] = load_artifacts(artifacts_dir)
    return _ARTIFACTS_CACHE[artifacts_dir]


def make_alert_scoring_func(artifacts_dir: str, feature_names: tuple[str, ...]):
    def score_partitions(pdf_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        from pulseguard_detection.score import score_transaction

        artifacts = _get_artifacts(artifacts_dir)
        for pdf in pdf_iter:
            rows: list[dict] = []
            for row in pdf.itertuples(index=False):
                features = {name: getattr(row, name) for name in feature_names}
                raw = {
                    "txnId": row.txnId,
                    "customerId": row.customerId,
                    "amount": row.amount,
                    "deviceIsNew": row.deviceIsNew,
                }
                alert = score_transaction(artifacts, features, raw)
                if alert is None:
                    continue
                rows.append(
                    {
                        "alertId": str(uuid.uuid4()),
                        "txnId": alert["txnId"],
                        "customerId": alert["customerId"],
                        "timestamp": row.timestamp,
                        "score": alert["score"],
                        "severity": alert["severity"],
                        "triggeredRules": alert["triggeredRules"],
                        "topFeatures": alert["topFeatures"],
                        "trailingMean": features.get("trailingMean"),
                        "trailingMax": features.get("trailingMax"),
                        "amount": row.amount,
                        "channel": row.channel,
                        "merchantId": row.merchantId,
                        "merchantCategory": row.merchantCategory,
                        "city": row.city,
                        "lat": row.lat,
                        "lon": row.lon,
                    }
                )
            yield pd.DataFrame(rows, columns=ALERT_COLUMN_NAMES) if rows else pd.DataFrame(columns=ALERT_COLUMN_NAMES)

    return score_partitions
