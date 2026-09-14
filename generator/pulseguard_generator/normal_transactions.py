"""Generates a single legitimate transaction for a customer at a given
simulated timestamp, and updates the customer's rolling stats so later
fraud injection has a realistic trailing mean/max to deviate from."""

from __future__ import annotations

import random
from datetime import datetime
from typing import Any

from .customers import Customer
from .geo import CITY_BY_NAME, jitter_point
from .merchants import Merchant

# smoothing factor for the customer's running mean/max, so a handful of
# early transactions don't get permanently anchored as "trailing" forever
_EMA_ALPHA = 0.05


def _pick_merchant(customer: Customer, catalog: list[Merchant], by_city_category: dict, rng: random.Random) -> Merchant:
    roll = rng.random()
    if customer.recurring_merchants and roll < 0.75:
        merchant_id = rng.choice(customer.recurring_merchants)
        for m in catalog:
            if m.merchant_id == merchant_id:
                return m
    # occasionally step outside recurring merchants -- either a new merchant
    # in a preferred category (common) or a genuinely new category (rare,
    # feeds the "merchant category is new" feature honestly)
    city = rng.choice(customer.home_cities)
    if roll < 0.95:
        category = rng.choice(customer.preferred_categories)
    else:
        category = rng.choice(list({m.category for m in catalog}))
    pool = by_city_category.get((city, category)) or catalog
    return rng.choice(pool)


def generate_normal_transaction(
    customer: Customer,
    catalog: list[Merchant],
    by_city_category: dict,
    rng: random.Random,
    timestamp: datetime,
) -> dict[str, Any]:
    merchant = _pick_merchant(customer, catalog, by_city_category, rng)
    channel = rng.choices(
        list(customer.channel_weights.keys()), weights=list(customer.channel_weights.values()), k=1
    )[0]
    # the SENIOR archetype models a pseudo "ATM" channel; map it onto the
    # real schema's CARD channel + ATM_WITHDRAWAL category.
    if channel == "ATM":
        channel = "CARD"
        merchant_category = "ATM_WITHDRAWAL"
    else:
        merchant_category = merchant.category

    amount = rng.lognormvariate(customer.amount_log_mean, customer.amount_log_sigma)

    device_is_new = False
    if customer.devices and rng.random() < 0.005:
        device_id = f"D-{rng.randrange(16**8):08x}"
        customer.devices.append(device_id)
        device_is_new = True
    else:
        device_id = rng.choice(customer.devices) if customer.devices else f"D-{rng.randrange(16**8):08x}"

    city = CITY_BY_NAME.get(merchant.city) or CITY_BY_NAME[customer.home_cities[0]]
    lat, lon = jitter_point(city, 1.5, rng)

    is_salary_credit = customer.archetype == "SALARIED_URBAN" and timestamp.day == 1 and rng.random() < 0.9
    direction = "CREDIT" if is_salary_credit else "DEBIT"
    if is_salary_credit:
        amount = customer.trailing_mean_amount * rng.uniform(25, 45)
        merchant_category = "SALARY_CREDIT"

    customer.visited_categories.add(merchant_category)
    customer.trailing_mean_amount = (1 - _EMA_ALPHA) * customer.trailing_mean_amount + _EMA_ALPHA * amount
    customer.trailing_max_amount = max(customer.trailing_max_amount * 0.999, amount)

    from .schema import build_transaction

    return build_transaction(
        customer_id=customer.customer_id,
        timestamp=timestamp,
        amount=amount,
        channel=channel,
        merchant_id=merchant.merchant_id,
        merchant_category=merchant_category,
        device_id=device_id,
        device_is_new=device_is_new,
        lat=lat,
        lon=lon,
        city=merchant.city,
        direction=direction,
        is_fraud=False,
        fraud_pattern=None,
    )


def diurnal_multiplier(hour: int, active_hours: tuple[int, int]) -> float:
    """Cheap bell-ish curve: inside active hours ~1.0, outside ~0.08, so
    volume clusters realistically without a full-blown distribution."""
    start, end = active_hours
    if start <= end:
        active = start <= hour < end
    else:  # wraps past midnight, e.g. students active 9 -> 2am
        active = hour >= start or hour < end
    return 1.0 if active else 0.08
