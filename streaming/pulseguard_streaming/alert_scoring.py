"""Spark-side adapter for pulseguard_detection.score.score_batch.

The scoring function itself (rules + LightGBM + TreeSHAP + Isolation
Forest shadow) lives in pulseguard_detection and has no Spark dependency
-- the same "one function, called from everywhere" pattern as
feature_state.apply_event. This module's only job is gluing that into
`mapInPandas`: lazily loading the trained artifacts once per worker
process (not once per micro-batch -- parsing the LightGBM model and
unpickling an IsolationForest on every batch would be needless overhead),
handing each pandas batch to the scorer in one call (so LightGBM's
predict and pred_contrib run once per batch, not once per row), and
reshaping "maybe an alert" into the DataFrame `mapInPandas` expects.
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
        from pulseguard_detection.score import score_batch

        artifacts = _get_artifacts(artifacts_dir)
        for pdf in pdf_iter:
            feature_rows = pdf[list(feature_names)].to_dict("records")
            raws = pdf[["txnId", "customerId", "amount", "deviceIsNew"]].to_dict("records")
            alerts = score_batch(artifacts, feature_rows, raws)

            rows: list[dict] = []
            for row, features, alert in zip(pdf.itertuples(index=False), feature_rows, alerts):
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
                        "baseValue": alert["baseValue"],
                        "isoScore": alert["isoScore"],
                        "unusualFeatures": alert["unusualFeatures"],
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
