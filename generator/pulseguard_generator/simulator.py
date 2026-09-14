"""Two ways of turning the customer population into a transaction stream:

`simulate_stream` -- a heap-scheduled, behaviourally-paced stream where each
customer's next transaction time is drawn from their own archetype rate,
shaped by hour-of-day and weekend activity. This is what you'd use to build
a labelled dataset for offline feature engineering and model training: the
timestamps mean something.

`loadtest_stream` -- ignores per-customer pacing entirely and just samples
customers to hit a target transactions/second, for Week 5 throughput and
failure-drill testing. Both share the same content-generation code
(`normal_transactions`, `fraud_patterns`) so "realistic" and "firehose"
traffic are never two different code paths that could drift apart.
"""

from __future__ import annotations

import heapq
import itertools
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterator

from .customers import Customer
from .fraud_patterns import inject_random_pattern
from .merchants import Merchant, index_by_city_category
from .normal_transactions import diurnal_multiplier, generate_normal_transaction

SECONDS_PER_DAY = 86400


@dataclass
class SimConfig:
    fraud_probability_per_customer_per_day: float = 0.003


def _customer_rate_per_second(customer: Customer, at: datetime) -> float:
    daily = customer.daily_txn_rate
    if at.weekday() >= 5:
        daily *= customer.weekend_multiplier
    daily *= diurnal_multiplier(at.hour, customer.active_hours)
    return max(daily / SECONDS_PER_DAY, 1e-6)


def simulate_stream(
    customers: list[Customer],
    catalog: list[Merchant],
    rng: random.Random,
    start_time: datetime,
    end_time: datetime,
    config: SimConfig | None = None,
) -> Iterator[dict[str, Any]]:
    config = config or SimConfig()
    by_city_category = index_by_city_category(catalog)
    counter = itertools.count()
    heap: list[tuple[datetime, int, str]] = []

    customers_by_id = {c.customer_id: c for c in customers}
    last_fraud_day: dict[str, int] = {}

    for c in customers:
        first = start_time + timedelta(seconds=rng.expovariate(_customer_rate_per_second(c, start_time)))
        heapq.heappush(heap, (first, next(counter), c.customer_id))

    pending_fraud: list[dict[str, Any]] = []

    while heap:
        if pending_fraud:
            yield pending_fraud.pop(0)
            continue

        t, _, cid = heapq.heappop(heap)
        if t > end_time:
            break
        customer = customers_by_id[cid]

        day_index = (t - start_time).days
        if last_fraud_day.get(cid, -1) != day_index:
            last_fraud_day[cid] = day_index
            if rng.random() < config.fraud_probability_per_customer_per_day:
                _pattern, burst = inject_random_pattern(customer, catalog, rng, t)
                pending_fraud.extend(burst)

        yield generate_normal_transaction(customer, catalog, by_city_category, rng, t)

        next_t = t + timedelta(seconds=rng.expovariate(_customer_rate_per_second(customer, t)))
        heapq.heappush(heap, (next_t, next(counter), cid))

    yield from pending_fraud


def loadtest_stream(
    customers: list[Customer],
    catalog: list[Merchant],
    rng: random.Random,
    fraud_probability: float = 0.003,
) -> Iterator[dict[str, Any]]:
    """Infinite generator of transactions at no particular pace -- the
    caller (kafka_producer) controls throughput. Timestamps are wall-clock
    "now" at yield time since this models ingestion load, not a historical
    day."""
    by_city_category = index_by_city_category(catalog)
    while True:
        customer = rng.choice(customers)
        now = datetime.now().astimezone()
        if rng.random() < fraud_probability:
            _pattern, burst = inject_random_pattern(customer, catalog, rng, now)
            yield from burst
        else:
            yield generate_normal_transaction(customer, catalog, by_city_category, rng, now)
