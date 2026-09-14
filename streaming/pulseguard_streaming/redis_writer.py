"""Writes computed feature vectors to the Redis online feature store.

Keyed `feat:{customerId}`, one Redis hash per customer, TTL-refreshed on
every write. `to_redis_hash` is pure and unit-tested without a Redis
connection; `write_partition` is the executor-side function wired into
`foreachPartition` so the write fan-out happens on the workers rather than
collecting every micro-batch back to the driver.
"""

from __future__ import annotations

from typing import Any, Iterable

DEFAULT_TTL_SECONDS = 3600  # a feature vector older than this without a refresh is stale
PIPELINE_FLUSH_EVERY = 500


def to_redis_hash(feature_row: dict[str, Any]) -> dict[str, str]:
    """Redis hash values must be strings/bytes/numbers -- coerce bools and
    None (e.g. secondsSincePrevTxn on a customer's first-ever transaction)
    into something a hash field can actually hold."""
    out: dict[str, str] = {}
    for key, value in feature_row.items():
        if value is None:
            out[key] = ""
        elif isinstance(value, bool):
            out[key] = "1" if value else "0"
        else:
            out[key] = str(value)
    return out


def write_partition(
    rows: Iterable[Any],
    feature_names: tuple[str, ...],
    redis_host: str,
    redis_port: int,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> None:
    import redis  # imported lazily: only needed on Spark executors, not for unit tests

    client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    pipe = client.pipeline(transaction=False)
    pending = 0
    try:
        for row in rows:
            record = row.asDict() if hasattr(row, "asDict") else dict(row)
            feature_row = {name: record.get(name) for name in feature_names}
            feature_row["lastTxnId"] = record.get("txnId")
            feature_row["lastUpdated"] = str(record.get("timestamp"))

            key = f"feat:{record['customerId']}"
            pipe.hset(key, mapping=to_redis_hash(feature_row))
            pipe.expire(key, ttl_seconds)
            pending += 1
            if pending >= PIPELINE_FLUSH_EVERY:
                pipe.execute()
                pending = 0
        if pending:
            pipe.execute()
    finally:
        client.close()
