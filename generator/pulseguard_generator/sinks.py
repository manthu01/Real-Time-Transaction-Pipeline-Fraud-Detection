"""Output sinks for the generated transaction stream.

KafkaSink is the real target. StdoutSink/JsonlSink exist so the generator
and fraud-pattern logic can be exercised and tested without Docker/Kafka
running at all.
"""

from __future__ import annotations

import json
import random
import sys
import time
from typing import Any, Protocol


class Sink(Protocol):
    def send(self, txn: dict[str, Any]) -> None: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...
    def stats(self) -> dict[str, Any]: ...


class StdoutSink:
    def __init__(self) -> None:
        self._count = 0

    def send(self, txn: dict[str, Any]) -> None:
        sys.stdout.write(json.dumps(txn) + "\n")
        self._count += 1

    def flush(self) -> None:
        sys.stdout.flush()

    def close(self) -> None:
        self.flush()

    def stats(self) -> dict[str, Any]:
        return {"sent": self._count}


class JsonlSink:
    def __init__(self, path: str) -> None:
        self._fh = open(path, "w", encoding="utf-8")
        self._count = 0

    def send(self, txn: dict[str, Any]) -> None:
        self._fh.write(json.dumps(txn) + "\n")
        self._count += 1

    def flush(self) -> None:
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()

    def stats(self) -> dict[str, Any]:
        return {"sent": self._count}


class KafkaSink:
    """Batched, keyed-by-customerId producer with delivery-report tracking.

    A `malformed_rate` fraction of records are deliberately corrupted before
    send (a required field dropped) so the Spark job's dead-letter path
    (Week 2/5) has something real to catch instead of a code path nobody
    ever exercises.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        malformed_rate: float = 0.0,
        rng: random.Random | None = None,
        producer_config: dict[str, Any] | None = None,
    ) -> None:
        from confluent_kafka import Producer  # imported lazily so stdout/jsonl sinks work without the dependency installed

        cfg = {
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": 20,
            "batch.num.messages": 10000,
            "compression.type": "lz4",
            "acks": "all",
            "enable.idempotence": True,
        }
        if producer_config:
            cfg.update(producer_config)

        self._producer = Producer(cfg)
        self._topic = topic
        self._malformed_rate = malformed_rate
        self._rng = rng or random.Random()

        self._sent = 0
        self._delivered = 0
        self._failed = 0
        self._start = time.monotonic()

    def _delivery_cb(self, err, _msg) -> None:
        if err is not None:
            self._failed += 1
        else:
            self._delivered += 1

    def send(self, txn: dict[str, Any]) -> None:
        key = txn.get("customerId", "").encode("utf-8")

        if self._rng.random() < self._malformed_rate:
            corrupt = dict(txn)
            corrupt.pop(random.choice(["amount", "customerId", "timestamp"]), None)
            payload = json.dumps(corrupt).encode("utf-8")
        else:
            payload = json.dumps(txn).encode("utf-8")

        while True:
            try:
                self._producer.produce(self._topic, key=key, value=payload, callback=self._delivery_cb)
                break
            except BufferError:
                self._producer.poll(0.1)

        self._sent += 1
        if self._sent % 1000 == 0:
            self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(30)

    def close(self) -> None:
        self.flush()

    def stats(self) -> dict[str, Any]:
        elapsed = max(time.monotonic() - self._start, 1e-9)
        return {
            "sent": self._sent,
            "delivered": self._delivered,
            "failed": self._failed,
            "elapsed_sec": round(elapsed, 2),
            "achieved_tps": round(self._sent / elapsed, 1),
        }
