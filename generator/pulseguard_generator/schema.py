"""Transaction record shape.

JSON with an explicit schema-version field for now (see the build plan's
note that Avro + schema registry is a ~half-day upgrade that reads well in
an interview but isn't required to make the pipeline correct). Keeping the
field names stable here means swapping to Avro later is a schema-definition
exercise, not a rewrite of the generator.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

SCHEMA_VERSION = 1


def build_transaction(
    *,
    customer_id: str,
    timestamp: datetime,
    amount: float,
    channel: str,
    merchant_id: str,
    merchant_category: str,
    device_id: str,
    device_is_new: bool,
    lat: float,
    lon: float,
    city: str,
    direction: str = "DEBIT",
    is_fraud: bool = False,
    fraud_pattern: str | None = None,
) -> dict[str, Any]:
    return {
        "schemaVersion": SCHEMA_VERSION,
        "txnId": str(uuid.uuid4()),
        "customerId": customer_id,
        "timestamp": timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "amount": round(amount, 2),
        "direction": direction,
        "channel": channel,
        "merchantId": merchant_id,
        "merchantCategory": merchant_category,
        "deviceId": device_id,
        "deviceIsNew": device_is_new,
        "lat": round(lat, 4),
        "lon": round(lon, 4),
        "city": city,
        "isFraud": is_fraud,
        "fraudPattern": fraud_pattern,
    }


REQUIRED_FIELDS = (
    "txnId",
    "customerId",
    "timestamp",
    "amount",
    "channel",
    "merchantId",
    "merchantCategory",
)
