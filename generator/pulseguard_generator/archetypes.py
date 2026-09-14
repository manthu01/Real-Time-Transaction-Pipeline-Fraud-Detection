"""Customer behavioural archetypes.

This is the part the build plan calls out as the make-or-break piece: a
generator that just emits random noise produces a model and a rule engine
that learn / trigger on nothing meaningful. Each archetype pins down a
transaction rate, an amount distribution, a merchant footprint, a channel
mix, and an active-hours profile so that "normal" has real structure for
the streaming features to pick up on.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Archetype:
    name: str
    # relative weight when sampling the customer population
    population_weight: float
    # transactions per day, sampled uniformly in this range per customer
    daily_txn_range: tuple[float, float]
    # number of distinct recurring merchants this customer regularly uses
    recurring_merchant_range: tuple[int, int]
    # lognormal(mean, sigma) parameters over amount in INR
    amount_log_mean: float
    amount_log_sigma: float
    # probability mass across channels: UPI, CARD, NEFT
    channel_weights: dict[str, float]
    # how many distinct home cities this customer legitimately transacts from
    home_city_count: tuple[int, int]
    # multiplier applied to daily_txn_range on Sat/Sun
    weekend_multiplier: float
    # active hours as (start_hour, end_hour), used to shape a diurnal curve
    active_hours: tuple[int, int]
    # number of distinct devices normally used
    device_count_range: tuple[int, int]
    # preferred merchant categories (drawn most of the time); other categories
    # are still occasionally visited so "new category" fraud features are meaningful
    preferred_categories: tuple[str, ...]


ARCHETYPES: dict[str, Archetype] = {
    "SALARIED_URBAN": Archetype(
        name="SALARIED_URBAN",
        population_weight=0.40,
        daily_txn_range=(3, 8),
        recurring_merchant_range=(5, 10),
        amount_log_mean=6.8,   # exp(6.8) ~= 900
        amount_log_sigma=0.65,
        channel_weights={"UPI": 0.70, "CARD": 0.25, "NEFT": 0.05},
        home_city_count=(1, 1),
        weekend_multiplier=0.8,
        active_hours=(7, 23),
        device_count_range=(1, 2),
        preferred_categories=("GROCERY", "FOOD_DELIVERY", "TRANSPORT", "UTILITIES", "ECOMMERCE"),
    ),
    "SMALL_BUSINESS": Archetype(
        name="SMALL_BUSINESS",
        population_weight=0.15,
        daily_txn_range=(20, 60),
        recurring_merchant_range=(15, 40),
        amount_log_mean=7.3,   # exp(7.3) ~= 1480
        amount_log_sigma=1.0,
        channel_weights={"UPI": 0.55, "CARD": 0.20, "NEFT": 0.25},
        home_city_count=(1, 1),
        weekend_multiplier=1.3,
        active_hours=(6, 23),
        device_count_range=(1, 3),
        preferred_categories=("ECOMMERCE", "ELECTRONICS", "GROCERY", "TRAVEL", "FUEL"),
    ),
    "STUDENT": Archetype(
        name="STUDENT",
        population_weight=0.20,
        daily_txn_range=(2, 5),
        recurring_merchant_range=(4, 8),
        amount_log_mean=5.6,   # exp(5.6) ~= 270
        amount_log_sigma=0.6,
        channel_weights={"UPI": 0.90, "CARD": 0.10, "NEFT": 0.0},
        home_city_count=(1, 1),
        weekend_multiplier=1.1,
        active_hours=(9, 2),  # wraps past midnight
        device_count_range=(1, 1),
        preferred_categories=("FOOD_DELIVERY", "TRANSPORT", "ENTERTAINMENT"),
    ),
    "SENIOR": Archetype(
        name="SENIOR",
        population_weight=0.15,
        daily_txn_range=(0.3, 2),
        recurring_merchant_range=(2, 4),
        amount_log_mean=6.5,
        amount_log_sigma=0.5,
        channel_weights={"UPI": 0.30, "CARD": 0.20, "NEFT": 0.10, "ATM": 0.40},
        home_city_count=(1, 1),
        weekend_multiplier=1.0,
        active_hours=(9, 18),
        device_count_range=(1, 1),
        preferred_categories=("ATM_WITHDRAWAL", "BRANCH", "PHARMACY", "GROCERY"),
    ),
    "FREQUENT_TRAVELLER": Archetype(
        name="FREQUENT_TRAVELLER",
        population_weight=0.10,
        daily_txn_range=(4, 12),
        recurring_merchant_range=(8, 20),
        amount_log_mean=7.0,
        amount_log_sigma=0.8,
        channel_weights={"UPI": 0.50, "CARD": 0.45, "NEFT": 0.05},
        home_city_count=(3, 6),
        weekend_multiplier=1.0,
        active_hours=(6, 23),
        device_count_range=(1, 2),
        preferred_categories=("TRAVEL", "FOOD_DELIVERY", "ECOMMERCE", "FUEL"),
    ),
}

# channel weights above use "ATM" as a pseudo-channel for SENIOR; the schema
# only has UPI/CARD/NEFT so it is mapped to CARD+ATM_WITHDRAWAL category at
# transaction-build time. Kept here because it reads clearly next to the others.


def normalized_weights() -> list[tuple[str, float]]:
    total = sum(a.population_weight for a in ARCHETYPES.values())
    return [(name, a.population_weight / total) for name, a in ARCHETYPES.items()]
