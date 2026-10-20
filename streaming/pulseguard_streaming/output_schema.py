"""Output schema for the enriched, feature-attached transaction stream
produced by the applyInPandasWithState operator."""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PASSTHROUGH_FIELDS = [
    StructField("customerId", StringType()),
    StructField("txnId", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("amount", DoubleType()),
    StructField("direction", StringType()),
    StructField("channel", StringType()),
    StructField("merchantId", StringType()),
    StructField("merchantCategory", StringType()),
    StructField("deviceId", StringType()),
    StructField("deviceIsNew", BooleanType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("city", StringType()),
    StructField("isFraud", BooleanType()),
    StructField("fraudPattern", StringType()),
]

# name -> Spark type, matching the Python types produced by feature_state.apply_event
FEATURE_FIELD_TYPES: dict[str, type] = {
    "txnCount1Min": IntegerType,
    "txnCount5Min": IntegerType,
    "txnCount15Min": IntegerType,
    "txnCount1Hour": IntegerType,
    "txnSum5Min": DoubleType,
    "txnSum1Hour": DoubleType,
    "txnSum24Hour": DoubleType,
    "secondsSincePrevTxn": DoubleType,
    "maxTxnsInAny60SecWindowToday": IntegerType,
    "amountZScore": DoubleType,
    "amountToTrailingMaxRatio": DoubleType,
    "amountPercentile": DoubleType,
    "distinctMerchants1Hour": IntegerType,
    "distinctMerchants24Hour": IntegerType,
    "distinctDevices24Hour": IntegerType,
    "distinctDevices7Day": IntegerType,
    "distinctCities24Hour": IntegerType,
    "isNewMerchantCategory": BooleanType,
    "distanceFromPrevKm": DoubleType,
    "impliedSpeedKmh": DoubleType,
    "distanceFromModalCityKm": DoubleType,
    "hourOfDay": IntegerType,
    "dayOfWeek": IntegerType,
    "isOutsideActiveHours": BooleanType,
    "trailingMean": DoubleType,
    "trailingMax": DoubleType,
}

FEATURE_FIELDS = [StructField(name, dtype(), nullable=True) for name, dtype in FEATURE_FIELD_TYPES.items()]

OUTPUT_SCHEMA = StructType(PASSTHROUGH_FIELDS + FEATURE_FIELDS)

OUTPUT_COLUMN_NAMES = [f.name for f in OUTPUT_SCHEMA.fields]

STATE_SCHEMA = StructType([StructField("state_json", StringType())])

# per-alert TreeSHAP attribution from the LightGBM champion; "direction"
# is "up" when the feature pushed the log-odds toward fraud
TOP_FEATURE_STRUCT = StructType(
    [
        StructField("name", StringType()),
        StructField("value", DoubleType()),
        StructField("shap", DoubleType()),
        StructField("direction", StringType()),
    ]
)

# z-score against the non-fraud training population ("what's statistically odd")
UNUSUAL_FEATURE_STRUCT = StructType(
    [
        StructField("name", StringType()),
        StructField("value", DoubleType()),
        StructField("zScore", DoubleType()),
    ]
)

ALERT_SCHEMA = StructType(
    [
        StructField("alertId", StringType()),
        StructField("txnId", StringType()),
        StructField("customerId", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("score", DoubleType()),
        StructField("severity", StringType()),
        StructField("triggeredRules", ArrayType(StringType())),
        StructField("topFeatures", ArrayType(TOP_FEATURE_STRUCT)),
        StructField("baseValue", DoubleType()),
        StructField("isoScore", DoubleType()),
        StructField("unusualFeatures", ArrayType(UNUSUAL_FEATURE_STRUCT)),
        StructField("trailingMean", DoubleType()),
        StructField("trailingMax", DoubleType()),
        StructField("amount", DoubleType()),
        StructField("channel", StringType()),
        StructField("merchantId", StringType()),
        StructField("merchantCategory", StringType()),
        StructField("city", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
    ]
)

ALERT_COLUMN_NAMES = [f.name for f in ALERT_SCHEMA.fields]
