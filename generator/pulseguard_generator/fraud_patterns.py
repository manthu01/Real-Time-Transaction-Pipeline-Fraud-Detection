"""Injected fraud patterns.

Each function takes a customer mid-simulation and produces a short burst of
labelled transactions anchored near `anchor_time`. They deliberately reuse
`schema.build_transaction` so fraud and legitimate traffic are
indistinguishable at the wire-format level -- the label lives only in
isFraud/fraudPattern, which the feature pipeline must exclude from the
feature vector (see docs/label-leakage.md).
"""

from __future__ import annotations

import random
from datetime import timedelta
from typing import Any

from .customers import Customer
from .geo import CITIES, City, jitter_point
from .merchants import Merchant
from .schema import build_transaction

FRAUD_PATTERNS = (
    "CARD_TESTING",
    "ACCOUNT_TAKEOVER",
    "MULE_ACCOUNT",
    "GEO_IMPOSSIBLE_TRAVEL",
    "MERCHANT_CATEGORY_ANOMALY",
    "SALAMI_SLICING",
)


def _home_city(customer: Customer) -> str:
    return customer.home_cities[0]


def card_testing(customer: Customer, catalog: list[Merchant], rng: random.Random, anchor_time) -> list[dict[str, Any]]:
    """10-30 micro-transactions at one merchant within ~2 minutes, then one
    large transaction -- the pattern used to validate a stolen card before
    a real charge."""
    merchant = rng.choice(catalog)
    n_probe = rng.randint(10, 30)
    txns = []
    t = anchor_time
    device = customer.devices[0] if customer.devices else f"D-{rng.randrange(16**8):08x}"
    lat, lon = jitter_point(_city_obj(merchant.city), 2.0, rng)
    for _ in range(n_probe):
        t += timedelta(seconds=rng.uniform(0.5, 8))
        txns.append(
            build_transaction(
                customer_id=customer.customer_id,
                timestamp=t,
                amount=rng.uniform(1, 50),
                channel="CARD",
                merchant_id=merchant.merchant_id,
                merchant_category=merchant.category,
                device_id=device,
                device_is_new=False,
                lat=lat,
                lon=lon,
                city=merchant.city,
                is_fraud=True,
                fraud_pattern="CARD_TESTING",
            )
        )
    t += timedelta(seconds=rng.uniform(5, 30))
    txns.append(
        build_transaction(
            customer_id=customer.customer_id,
            timestamp=t,
            amount=rng.uniform(15000, 60000),
            channel="CARD",
            merchant_id=merchant.merchant_id,
            merchant_category=merchant.category,
            device_id=device,
            device_is_new=False,
            lat=lat,
            lon=lon,
            city=merchant.city,
            is_fraud=True,
            fraud_pattern="CARD_TESTING",
        )
    )
    return txns


def _city_obj(name: str) -> City:
    for c in CITIES:
        if c.name == name:
            return c
    return CITIES[0]


def account_takeover(customer: Customer, catalog: list[Merchant], rng: random.Random, anchor_time) -> list[dict[str, Any]]:
    """New device + new city + amount far above the customer's trailing
    mean, timed in the small hours when the real customer is asleep."""
    other_cities = [c for c in CITIES if c.name not in customer.home_cities] or CITIES
    city = rng.choice(other_cities)
    candidates = [m for m in catalog if m.city == city.name] or catalog
    merchant = rng.choice(candidates)
    new_device = f"D-{rng.randrange(16**8):08x}"
    lat, lon = jitter_point(city, 3.0, rng)

    hour = rng.choice([2, 3, 4])
    t = anchor_time.replace(hour=hour, minute=rng.randint(0, 59), second=rng.randint(0, 59))
    if t <= anchor_time:
        t += timedelta(days=1)

    multiplier = rng.uniform(8, 15)
    amount = max(customer.trailing_mean_amount * multiplier, 5000)

    return [
        build_transaction(
            customer_id=customer.customer_id,
            timestamp=t,
            amount=amount,
            channel=rng.choice(["UPI", "CARD"]),
            merchant_id=merchant.merchant_id,
            merchant_category=merchant.category,
            device_id=new_device,
            device_is_new=True,
            lat=lat,
            lon=lon,
            city=city.name,
            is_fraud=True,
            fraud_pattern="ACCOUNT_TAKEOVER",
        )
    ]


def mule_account(customer: Customer, catalog: list[Merchant], rng: random.Random, anchor_time) -> list[dict[str, Any]]:
    """A large incoming credit followed by rapid round-amount debits, the
    signature of a mule account being drained before it's flagged."""
    city = _home_city(customer)
    candidates = [m for m in catalog if m.city == city] or catalog
    device = customer.devices[0] if customer.devices else f"D-{rng.randrange(16**8):08x}"
    lat, lon = jitter_point(_city_obj(city), 1.0, rng)

    credit_amount = rng.uniform(150000, 500000)
    t = anchor_time
    txns = [
        build_transaction(
            customer_id=customer.customer_id,
            timestamp=t,
            amount=credit_amount,
            direction="CREDIT",
            channel="NEFT",
            merchant_id="BANK_TRANSFER",
            merchant_category="BANK_TRANSFER",
            device_id=device,
            device_is_new=False,
            lat=lat,
            lon=lon,
            city=city,
            is_fraud=True,
            fraud_pattern="MULE_ACCOUNT",
        )
    ]

    round_amounts = [45000, 47500, 49000, 49500, 50000]
    n_debits = rng.randint(5, 10)
    for _ in range(n_debits):
        t += timedelta(minutes=rng.uniform(2, 12))
        merchant = rng.choice(candidates)
        txns.append(
            build_transaction(
                customer_id=customer.customer_id,
                timestamp=t,
                amount=rng.choice(round_amounts) + rng.choice([0, 0, 0, 500]),
                direction="DEBIT",
                channel="NEFT",
                merchant_id=merchant.merchant_id,
                merchant_category=merchant.category,
                device_id=device,
                device_is_new=False,
                lat=lat,
                lon=lon,
                city=city,
                is_fraud=True,
                fraud_pattern="MULE_ACCOUNT",
            )
        )
    return txns


def geo_impossible_travel(customer: Customer, catalog: list[Merchant], rng: random.Random, anchor_time) -> list[dict[str, Any]]:
    """Two transactions ~1500km apart, ~20 minutes apart -- physically
    impossible for the same card holder."""
    city_a = _city_obj(_home_city(customer))
    far_cities = sorted(
        (c for c in CITIES if c.name != city_a.name),
        key=lambda c: -_distance(city_a, c),
    )[:5]
    city_b = rng.choice(far_cities) if far_cities else city_a

    merchant_a = rng.choice([m for m in catalog if m.city == city_a.name] or catalog)
    merchant_b = rng.choice([m for m in catalog if m.city == city_b.name] or catalog)
    device = customer.devices[0] if customer.devices else f"D-{rng.randrange(16**8):08x}"

    lat_a, lon_a = jitter_point(city_a, 2.0, rng)
    lat_b, lon_b = jitter_point(city_b, 2.0, rng)
    t_a = anchor_time
    t_b = anchor_time + timedelta(minutes=rng.uniform(15, 25))

    return [
        build_transaction(
            customer_id=customer.customer_id,
            timestamp=t_a,
            amount=rng.uniform(500, 5000),
            channel="CARD",
            merchant_id=merchant_a.merchant_id,
            merchant_category=merchant_a.category,
            device_id=device,
            device_is_new=False,
            lat=lat_a,
            lon=lon_a,
            city=city_a.name,
            is_fraud=True,
            fraud_pattern="GEO_IMPOSSIBLE_TRAVEL",
        ),
        build_transaction(
            customer_id=customer.customer_id,
            timestamp=t_b,
            amount=rng.uniform(500, 5000),
            channel="CARD",
            merchant_id=merchant_b.merchant_id,
            merchant_category=merchant_b.category,
            device_id=device,
            device_is_new=False,
            lat=lat_b,
            lon=lon_b,
            city=city_b.name,
            is_fraud=True,
            fraud_pattern="GEO_IMPOSSIBLE_TRAVEL",
        ),
    ]


def _distance(a: City, b: City) -> float:
    from .geo import haversine_km

    return haversine_km(a.lat, a.lon, b.lat, b.lon)


def merchant_category_anomaly(customer: Customer, catalog: list[Merchant], rng: random.Random, anchor_time) -> list[dict[str, Any]]:
    """A high-value purchase in a category this customer has never touched
    before -- the signal a pure velocity rule can't see."""
    unused = [cat for cat in {m.category for m in catalog} if cat not in customer.visited_categories]
    category = rng.choice(unused) if unused else rng.choice(list({m.category for m in catalog}))
    city = _home_city(customer)
    candidates = [m for m in catalog if m.city == city and m.category == category] or catalog
    merchant = rng.choice(candidates)
    device = customer.devices[0] if customer.devices else f"D-{rng.randrange(16**8):08x}"
    lat, lon = jitter_point(_city_obj(city), 1.0, rng)

    amount = max(customer.trailing_max_amount * rng.uniform(1.5, 3.0), 20000)

    return [
        build_transaction(
            customer_id=customer.customer_id,
            timestamp=anchor_time,
            amount=amount,
            channel=rng.choice(["CARD", "UPI"]),
            merchant_id=merchant.merchant_id,
            merchant_category=merchant.category,
            device_id=device,
            device_is_new=False,
            lat=lat,
            lon=lon,
            city=city,
            is_fraud=True,
            fraud_pattern="MERCHANT_CATEGORY_ANOMALY",
        )
    ]


def salami_slicing(customer: Customer, catalog: list[Merchant], rng: random.Random, anchor_time) -> list[dict[str, Any]]:
    """Many small debits, each just under a round reporting threshold,
    spread over the day so no single velocity rule sees them as a burst."""
    city = _home_city(customer)
    candidates = [m for m in catalog if m.city == city] or catalog
    device = customer.devices[0] if customer.devices else f"D-{rng.randrange(16**8):08x}"
    lat, lon = jitter_point(_city_obj(city), 1.0, rng)

    threshold = rng.choice([10000, 50000])
    n = rng.randint(6, 12)
    t = anchor_time
    txns = []
    for _ in range(n):
        t += timedelta(minutes=rng.uniform(20, 180))
        merchant = rng.choice(candidates)
        amount = threshold - rng.uniform(50, 500)
        txns.append(
            build_transaction(
                customer_id=customer.customer_id,
                timestamp=t,
                amount=amount,
                channel="UPI",
                merchant_id=merchant.merchant_id,
                merchant_category=merchant.category,
                device_id=device,
                device_is_new=False,
                lat=lat,
                lon=lon,
                city=city,
                is_fraud=True,
                fraud_pattern="SALAMI_SLICING",
            )
        )
    return txns


INJECTORS = {
    "CARD_TESTING": card_testing,
    "ACCOUNT_TAKEOVER": account_takeover,
    "MULE_ACCOUNT": mule_account,
    "GEO_IMPOSSIBLE_TRAVEL": geo_impossible_travel,
    "MERCHANT_CATEGORY_ANOMALY": merchant_category_anomaly,
    "SALAMI_SLICING": salami_slicing,
}


def inject_random_pattern(customer: Customer, catalog: list[Merchant], rng: random.Random, anchor_time) -> tuple[str, list[dict[str, Any]]]:
    pattern = rng.choice(FRAUD_PATTERNS)
    return pattern, INJECTORS[pattern](customer, catalog, rng, anchor_time)
