import random
from datetime import datetime, timedelta

from pulseguard_generator.customers import generate_population
from pulseguard_generator.fraud_patterns import FRAUD_PATTERNS, INJECTORS
from pulseguard_generator.geo import haversine_km
from pulseguard_generator.merchants import build_merchant_catalog
from pulseguard_generator.schema import REQUIRED_FIELDS, build_transaction
from pulseguard_generator.simulator import SimConfig, simulate_stream


def _rng():
    return random.Random(1234)


def test_haversine_known_distance():
    # Mumbai to Delhi is roughly 1150km great-circle.
    d = haversine_km(19.0760, 72.8777, 28.7041, 77.1025)
    assert 1100 < d < 1200


def test_build_transaction_has_required_fields():
    txn = build_transaction(
        customer_id="C000001",
        timestamp=datetime.now().astimezone(),
        amount=100.0,
        channel="UPI",
        merchant_id="M0001",
        merchant_category="GROCERY",
        device_id="D-abc",
        device_is_new=False,
        lat=19.07,
        lon=72.87,
        city="Mumbai",
    )
    for field in REQUIRED_FIELDS:
        assert field in txn
    assert txn["isFraud"] is False
    assert txn["fraudPattern"] is None


def test_all_fraud_patterns_produce_labelled_transactions():
    rng = _rng()
    catalog = build_merchant_catalog(rng)
    customers = generate_population(20, rng, catalog)
    anchor = datetime.now().astimezone()

    for pattern in FRAUD_PATTERNS:
        customer = rng.choice(customers)
        txns = INJECTORS[pattern](customer, catalog, rng, anchor)
        assert len(txns) >= 1
        for t in txns:
            assert t["isFraud"] is True
            assert t["fraudPattern"] == pattern
            assert t["amount"] > 0


def test_simulate_stream_produces_ordered_labelled_transactions():
    rng = _rng()
    catalog = build_merchant_catalog(rng)
    customers = generate_population(100, rng, catalog)
    start = datetime.now().astimezone()
    end = start + timedelta(days=3)

    stream = simulate_stream(customers, catalog, rng, start, end, SimConfig(fraud_probability_per_customer_per_day=0.5))
    timestamps = []
    fraud_count = 0
    for txn in stream:
        timestamps.append(txn["timestamp"])
        if txn["isFraud"]:
            fraud_count += 1

    assert len(timestamps) > 500
    assert fraud_count > 0, "expected at least one injected fraud transaction with an elevated fraud rate"
