"""Gold layer: batch aggregates computed from the bronze Delta table plus
a batch read of fraud.alerts / analyst.dispositions from Kafka.

Per-rule precision and the alert funnel duplicate what the NestJS alert
service's MetricsService already serves live from Postgres -- deliberately.
This is the offline analytical copy: computed independently over Delta +
Kafka rather than by querying the operational database, so a gold-table
consumer (a BI tool, a data scientist, this nightly job's own drift/quality
checks) never depends on the serving-layer Postgres instance being
reachable, and the numbers double as a cross-check that the two systems
agree.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

from pulseguard_streaming.output_schema import ALERT_SCHEMA

DISPOSITION_SCHEMA = StructType(
    [
        StructField("alertId", StringType()),
        StructField("customerId", StringType()),
        StructField("status", StringType()),
        StructField("note", StringType()),
        StructField("dispositionedAt", TimestampType()),
    ]
)

DISPOSITIONED_STATUSES = ("CONFIRMED_FRAUD", "FALSE_POSITIVE")


def read_bronze(spark: SparkSession, bronze_path: str) -> DataFrame:
    return spark.read.format("delta").load(bronze_path)


def _read_kafka_batch(spark: SparkSession, bootstrap_servers: str, topic: str) -> DataFrame:
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )


def read_alerts(spark: SparkSession, bootstrap_servers: str, topic: str = "fraud.alerts") -> DataFrame:
    raw = _read_kafka_batch(spark, bootstrap_servers, topic)
    return raw.select(F.from_json(F.col("value").cast("string"), ALERT_SCHEMA).alias("a")).select("a.*")


def read_dispositions(spark: SparkSession, bootstrap_servers: str, topic: str = "analyst.dispositions") -> DataFrame:
    """analyst.dispositions is a compacted topic, but a batch read can still
    observe more than one message per key if compaction hasn't run yet --
    deduplicated here by keeping the latest `dispositionedAt` per alertId,
    the same latest-wins semantics compaction itself provides, just made
    explicit rather than assumed."""
    raw = _read_kafka_batch(spark, bootstrap_servers, topic)
    parsed = raw.select(F.from_json(F.col("value").cast("string"), DISPOSITION_SCHEMA).alias("d")).select("d.*")
    ranked = parsed.withColumn(
        "_rank", F.row_number().over(Window.partitionBy("alertId").orderBy(F.col("dispositionedAt").desc()))
    )
    return ranked.filter(F.col("_rank") == 1).drop("_rank")


def daily_fraud_rate_by_channel_category(bronze: DataFrame) -> DataFrame:
    return (
        bronze.groupBy("date", "channel", "merchantCategory")
        .agg(F.count("*").alias("txnCount"), F.sum(F.col("isFraud").cast("int")).alias("fraudCount"))
        .withColumn("fraudRate", F.col("fraudCount") / F.col("txnCount"))
        .orderBy("date", "channel", "merchantCategory")
    )


def customer_risk_profiles(bronze: DataFrame) -> DataFrame:
    return bronze.groupBy("customerId").agg(
        F.count("*").alias("txnCount"),
        F.sum(F.col("isFraud").cast("int")).alias("fraudCount"),
        F.avg("amount").alias("avgAmount"),
        F.avg("amountZScore").alias("avgAmountZScore"),
        F.max("amountZScore").alias("maxAmountZScore"),
        F.countDistinct("city").alias("distinctCitiesEver"),
    )


def per_rule_precision(alerts: DataFrame, dispositions: DataFrame) -> DataFrame:
    dispositioned = dispositions.filter(F.col("status").isin(*DISPOSITIONED_STATUSES))
    exploded = alerts.select("alertId", F.explode("triggeredRules").alias("rule"))
    joined = exploded.join(dispositioned, on="alertId", how="inner")
    return (
        joined.groupBy("rule")
        .agg(
            F.sum(F.when(F.col("status") == "CONFIRMED_FRAUD", 1).otherwise(0)).alias("confirmedFraud"),
            F.sum(F.when(F.col("status") == "FALSE_POSITIVE", 1).otherwise(0)).alias("falsePositive"),
        )
        .withColumn("precision", F.col("confirmedFraud") / (F.col("confirmedFraud") + F.col("falsePositive")))
        .orderBy(F.col("precision").desc())
    )


def alert_funnel(alerts: DataFrame, dispositions: DataFrame) -> DataFrame:
    """Counts by severity x disposition status, including alerts still
    PENDING (a left join, not inner -- the funnel's whole point is showing
    how much of the queue analysts haven't reached yet)."""
    joined = alerts.join(dispositions.select("alertId", "status"), on="alertId", how="left")
    with_status = joined.withColumn("dispositionStatus", F.coalesce(F.col("status"), F.lit("PENDING")))
    return with_status.groupBy("severity", "dispositionStatus").agg(F.count("*").alias("count")).orderBy("severity", "dispositionStatus")
