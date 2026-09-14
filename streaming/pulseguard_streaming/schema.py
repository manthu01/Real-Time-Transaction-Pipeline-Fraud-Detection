"""Spark schema for the wire-format transaction record.

Mirrors generator/pulseguard_generator/schema.py field-for-field. Kept as
an explicit StructType (rather than schema inference) so a malformed or
older-version record fails to parse deterministically and can be routed to
dead.letter, instead of silently getting nulls in random columns.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TRANSACTION_SCHEMA = StructType(
    [
        StructField("schemaVersion", IntegerType(), nullable=False),
        StructField("txnId", StringType(), nullable=False),
        StructField("customerId", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("amount", DoubleType(), nullable=False),
        StructField("direction", StringType(), nullable=True),
        StructField("channel", StringType(), nullable=True),
        StructField("merchantId", StringType(), nullable=False),
        StructField("merchantCategory", StringType(), nullable=False),
        StructField("deviceId", StringType(), nullable=False),
        StructField("deviceIsNew", BooleanType(), nullable=True),
        StructField("lat", DoubleType(), nullable=False),
        StructField("lon", DoubleType(), nullable=False),
        StructField("city", StringType(), nullable=False),
        StructField("isFraud", BooleanType(), nullable=True),
        StructField("fraudPattern", StringType(), nullable=True),
    ]
)

# fields that must be non-null for a record to be usable downstream; anything
# failing this (or failing to parse as JSON at all) goes to dead.letter.
REQUIRED_NON_NULL_FIELDS = (
    "txnId",
    "customerId",
    "timestamp",
    "amount",
    "merchantId",
    "merchantCategory",
    "deviceId",
    "lat",
    "lon",
    "city",
)
