"""Synthetic customer population generation."""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from .archetypes import ARCHETYPES, normalized_weights
from .geo import CITIES
from .merchants import Merchant, index_by_city_category


@dataclass
class Customer:
    customer_id: str
    archetype: str
    home_cities: list[str]
    recurring_merchants: list[str]
    devices: list[str]
    daily_txn_rate: float          # mean transactions/day for this customer
    amount_log_mean: float
    amount_log_sigma: float
    channel_weights: dict[str, float]
    active_hours: tuple[int, int]
    weekend_multiplier: float
    preferred_categories: tuple[str, ...]
    visited_categories: set[str] = field(default_factory=set)
    trailing_mean_amount: float = 0.0
    trailing_max_amount: float = 0.0


def _pick_channel(weights: dict[str, float], rng: random.Random) -> str:
    names = list(weights.keys())
    probs = list(weights.values())
    return rng.choices(names, weights=probs, k=1)[0]


def generate_population(num_customers: int, rng: random.Random, catalog: list[Merchant]) -> list[Customer]:
    by_city_category = index_by_city_category(catalog)
    weighted = normalized_weights()
    names = [n for n, _ in weighted]
    probs = [p for _, p in weighted]

    customers: list[Customer] = []
    for i in range(num_customers):
        archetype_name = rng.choices(names, weights=probs, k=1)[0]
        arche = ARCHETYPES[archetype_name]

        n_home_cities = rng.randint(*arche.home_city_count)
        home_cities = rng.sample([c.name for c in CITIES], k=n_home_cities)

        n_merchants = rng.randint(*arche.recurring_merchant_range)
        pool: list[Merchant] = []
        for city in home_cities:
            for cat in arche.preferred_categories:
                pool.extend(by_city_category.get((city, cat), []))
        if not pool:
            pool = catalog
        recurring = rng.sample(pool, k=min(n_merchants, len(pool)))

        n_devices = rng.randint(*arche.device_count_range)
        devices = [f"D-{rng.randrange(16**8):08x}" for _ in range(n_devices)]

        daily_rate = rng.uniform(*arche.daily_txn_range)
        amount_log_mean = arche.amount_log_mean + rng.uniform(-0.15, 0.15)

        customer = Customer(
            customer_id=f"C{i:06d}",
            archetype=archetype_name,
            home_cities=home_cities,
            recurring_merchants=[m.merchant_id for m in recurring],
            devices=devices,
            daily_txn_rate=daily_rate,
            amount_log_mean=amount_log_mean,
            amount_log_sigma=arche.amount_log_sigma,
            channel_weights=arche.channel_weights,
            active_hours=arche.active_hours,
            weekend_multiplier=arche.weekend_multiplier,
            preferred_categories=arche.preferred_categories,
        )
        # seed "history" so trailing-mean-based fraud injection has something
        # sensible to deviate from from t=0, without waiting for 30 days of sim.
        import math

        customer.trailing_mean_amount = math.exp(amount_log_mean + (arche.amount_log_sigma ** 2) / 2)
        customer.trailing_max_amount = customer.trailing_mean_amount * 3
        customers.append(customer)

    return customers
