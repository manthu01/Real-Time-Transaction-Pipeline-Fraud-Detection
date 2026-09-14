"""Merchant catalog: categories, and a fixed pool of merchants per category
per city so customers can build "recurring merchant" habits."""

from __future__ import annotations

from dataclasses import dataclass

from .geo import CITIES

MERCHANT_CATEGORIES: list[str] = [
    "GROCERY",
    "FOOD_DELIVERY",
    "TRANSPORT",
    "UTILITIES",
    "ECOMMERCE",
    "ELECTRONICS",
    "FUEL",
    "PHARMACY",
    "ENTERTAINMENT",
    "TRAVEL",
    "JEWELLERY",
    "ATM_WITHDRAWAL",
    "BRANCH",
]

# Categories a small-business archetype is more likely to appear in as
# a *sender's* counterpart merchant spread (kept broad deliberately).
HIGH_VALUE_CATEGORIES = {"JEWELLERY", "ELECTRONICS", "TRAVEL"}


@dataclass(frozen=True)
class Merchant:
    merchant_id: str
    category: str
    city: str


def build_merchant_catalog(rng, per_category_per_city: int = 4) -> list[Merchant]:
    """A fixed catalog generated once per run. customerId-independent so
    multiple customers legitimately share merchants (as in real life)."""
    catalog: list[Merchant] = []
    counter = 0
    for city in CITIES:
        for category in MERCHANT_CATEGORIES:
            n = per_category_per_city if category not in ("ATM_WITHDRAWAL", "BRANCH") else 2
            for _ in range(n):
                counter += 1
                catalog.append(Merchant(merchant_id=f"M{counter:05d}", category=category, city=city.name))
    return catalog


def index_by_city_category(catalog: list[Merchant]) -> dict[tuple[str, str], list[Merchant]]:
    index: dict[tuple[str, str], list[Merchant]] = {}
    for m in catalog:
        index.setdefault((m.city, m.category), []).append(m)
    return index
