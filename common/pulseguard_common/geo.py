"""City catalog and geo-distance helpers.

Both the generator (which places transactions in these cities) and the
streaming feature job (which computes distance-from-modal-city) need the
exact same coordinates for a city name to mean the same thing on both
sides -- hence this lives in the shared package rather than being copied.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class City:
    name: str
    lat: float
    lon: float


CITIES: list[City] = [
    City("Mumbai", 19.0760, 72.8777),
    City("Delhi", 28.7041, 77.1025),
    City("Bengaluru", 12.9716, 77.5946),
    City("Hyderabad", 17.3850, 78.4867),
    City("Chennai", 13.0827, 80.2707),
    City("Kolkata", 22.5726, 88.3639),
    City("Pune", 18.5204, 73.8567),
    City("Ahmedabad", 23.0225, 72.5714),
    City("Surat", 21.1702, 72.8311),
    City("Jaipur", 26.9124, 75.7873),
    City("Lucknow", 26.8467, 80.9462),
    City("Kochi", 9.9312, 76.2673),
    City("Chandigarh", 30.7333, 76.7794),
    City("Bhopal", 23.2599, 77.4126),
    City("Guwahati", 26.1445, 91.7362),
]

CITY_BY_NAME: dict[str, City] = {c.name: c for c in CITIES}

EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in kilometres."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))
