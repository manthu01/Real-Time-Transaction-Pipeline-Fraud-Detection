"""Re-exports the shared city catalog/distance helpers, and adds the
jitter helper the generator needs to avoid emitting pixel-identical
coordinates for repeat visits to the same city."""

from __future__ import annotations

from pulseguard_common.geo import CITIES, CITY_BY_NAME, City, haversine_km

__all__ = ["City", "CITIES", "CITY_BY_NAME", "haversine_km", "jitter_point"]


def jitter_point(city: City, radius_km: float, rng) -> tuple[float, float]:
    """A lat/lon near `city`, so repeat visits to the same city aren't
    pixel-identical points."""
    import math

    # ~1 degree of latitude is ~111km; treat locally as flat for small radii.
    d_lat = rng.uniform(-radius_km, radius_km) / 111.0
    d_lon = rng.uniform(-radius_km, radius_km) / (111.0 * math.cos(math.radians(city.lat)) + 1e-6)
    return city.lat + d_lat, city.lon + d_lon
