"""PulseGuard Phase 2: Spark Structured Streaming feature computation job.

Reads raw.transactions from Kafka, splits parseable-and-complete records
from everything else (routed to dead.letter), computes 26 rolling
behavioural features per customer via a stateful per-key operator, writes
the feature vectors to the Redis online feature store, and runs a second,
independent windowed-aggregation query purely to demonstrate the
watermarked sliding-window idiom the build plan calls out explicitly.

Run via spark-submit (see docker-compose.yml's `spark` service), not
`python feature_job.py` directly -- it needs the spark-sql-kafka connector
on the classpath.
"""

from __future__ import annotations

import argparse
import functools
from typing import Iterator

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from pulseguard_streaming.feature_state import FEATURE_NAMES, apply_event, new_state
from pulseguard_streaming.output_schema import OUTPUT_COLUMN_NAMES, OUTPUT_SCHEMA, STATE_SCHEMA
from pulseguard_streaming.redis_writer import write_partition
from pulseguard_streaming.schema import REQUIRED_NON_NULL_FIELDS, TRANSACTION_SCHEMA
from pulseguard_streaming.state_codec import deserialize_state, serialize_state

WATERMARK_DELAY = "2 minutes"
STATE_IDLE_TIMEOUT_MS = 10 * 24 * 60 * 60 * 1000  # 10 days of no activity drops the customer's state


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PulseGuard streaming feature job")
    p.add_argument("--bootstrap-servers", default="kafka:19092")
    p.add_argument("--input-topic", default="raw.transactions")
    p.add_argument("--dead-letter-topic", default="dead.letter")
    p.add_argument("--redis-host", default="redis")
    p.add_argument("--redis-port", type=int, default=6379)
    p.add_argument("--redis-ttl-seconds", type=int, default=3600)
    p.add_argument("--checkpoint-dir", default="/opt/pulseguard/checkpoints")
    p.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    p.add_argument("--trigger-interval", default="5 seconds")
    p.add_argument("--enable-window-demo", action="store_true", default=True)
    p.add_argument("--no-window-demo", dest="enable_window_demo", action="store_false")
    return p.parse_args(argv)


def build_spark(app_name: str = "pulseguard-feature-job") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )


def read_raw_stream(spark: SparkSession, bootstrap_servers: str, topic: str, starting_offsets: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def split_valid_and_invalid(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Parses the Kafka value as JSON against TRANSACTION_SCHEMA. A record
    is invalid if it isn't parseable JSON at all, or if it's missing any
    field a downstream feature genuinely can't do without -- either way it
    goes to dead.letter instead of crashing the job or silently poisoning
    a customer's feature state with nulls."""
    with_json = raw.selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafkaTimestamp")
    parsed = with_json.withColumn("parsed", F.from_json(F.col("json_str"), TRANSACTION_SCHEMA))

    required_checks = [F.col(f"parsed.{field}").isNotNull() for field in REQUIRED_NON_NULL_FIELDS]
    is_valid = F.col("parsed").isNotNull() & functools.reduce(lambda a, b: a & b, required_checks)

    checked = parsed.withColumn("isValid", is_valid)
    valid = checked.filter(F.col("isValid")).select("parsed.*")
    invalid = checked.filter(~F.col("isValid")).select("json_str")
    return valid, invalid


def write_dead_letter(invalid: DataFrame, bootstrap_servers: str, topic: str, checkpoint_dir: str, trigger_interval: str):
    return (
        invalid.selectExpr("CAST(json_str AS STRING) AS value")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", topic)
        .option("checkpointLocation", f"{checkpoint_dir}/dead_letter")
        .trigger(processingTime=trigger_interval)
        .outputMode("append")
        .start()
    )


def start_window_demo_query(valid_with_watermark: DataFrame, trigger_interval: str):
    """Demonstrates the exact watermarked sliding-window idiom the build
    plan calls out: a 5-minute window sliding every 1 minute, so a burst is
    visible within a minute rather than up to five. This is a side query,
    independent of the stateful feature pipeline below -- the main feature
    vector gets its velocity counts from the bounded per-customer event
    buffer instead, since 1min/5min/15min/1hr/24hr counts over an
    arbitrary-length customer history don't map onto a single window spec."""
    window_counts = valid_with_watermark.groupBy(
        F.col("customerId"),
        F.window(F.col("timestamp"), "5 minutes", "1 minute"),
    ).agg(
        F.count("*").alias("windowTxnCount"),
        F.sum("amount").alias("windowTxnSum"),
        F.approx_count_distinct("merchantId").alias("windowDistinctMerchants"),
    )
    return (
        window_counts.selectExpr(
            "customerId", "window.start AS windowStart", "window.end AS windowEnd",
            "windowTxnCount", "windowTxnSum", "windowDistinctMerchants",
        )
        .writeStream.format("console")
        .option("truncate", "false")
        .outputMode("update")
        .trigger(processingTime=trigger_interval)
        .start()
    )


def _make_state_func(feature_names: tuple[str, ...]):
    def compute_features_with_state(
        key: tuple, pdf_iter: Iterator[pd.DataFrame], group_state: GroupState
    ) -> Iterator[pd.DataFrame]:
        if group_state.hasTimedOut:
            group_state.remove()
            return

        if group_state.exists:
            (raw_state,) = group_state.get
            internal_state = deserialize_state(raw_state)
        else:
            internal_state = new_state()

        output_rows: list[dict] = []
        for pdf in pdf_iter:
            if pdf.empty:
                continue
            for row in pdf.sort_values("timestamp").itertuples(index=False):
                event = {
                    "timestamp": pd.Timestamp(row.timestamp).to_pydatetime(),
                    "amount": float(row.amount),
                    "merchantId": row.merchantId,
                    "merchantCategory": row.merchantCategory,
                    "deviceId": row.deviceId,
                    "city": row.city,
                    "lat": float(row.lat),
                    "lon": float(row.lon),
                }
                internal_state, features = apply_event(internal_state, event)

                out = {
                    "customerId": row.customerId,
                    "txnId": row.txnId,
                    "timestamp": row.timestamp,
                    "amount": row.amount,
                    "direction": row.direction,
                    "channel": row.channel,
                    "merchantId": row.merchantId,
                    "merchantCategory": row.merchantCategory,
                    "deviceId": row.deviceId,
                    "deviceIsNew": row.deviceIsNew,
                    "lat": row.lat,
                    "lon": row.lon,
                    "city": row.city,
                    "isFraud": row.isFraud,
                    "fraudPattern": row.fraudPattern,
                }
                for name in feature_names:
                    out[name] = features[name]
                output_rows.append(out)

        group_state.update((serialize_state(internal_state),))
        group_state.setTimeoutDuration(STATE_IDLE_TIMEOUT_MS)

        if output_rows:
            yield pd.DataFrame(output_rows, columns=OUTPUT_COLUMN_NAMES)
        else:
            yield pd.DataFrame(columns=OUTPUT_COLUMN_NAMES)

    return compute_features_with_state


def compute_feature_stream(valid: DataFrame) -> DataFrame:
    state_func = _make_state_func(FEATURE_NAMES)
    return valid.groupBy("customerId").applyInPandasWithState(
        state_func,
        outputStructType=OUTPUT_SCHEMA,
        stateStructType=STATE_SCHEMA,
        outputMode="append",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
    )


def write_features_to_redis(
    feature_stream: DataFrame, redis_host: str, redis_port: int, redis_ttl_seconds: int, checkpoint_dir: str, trigger_interval: str
):
    def write_batch(batch_df: DataFrame, _batch_id: int) -> None:
        batch_df.foreachPartition(
            lambda rows: write_partition(rows, FEATURE_NAMES, redis_host, redis_port, redis_ttl_seconds)
        )

    return (
        feature_stream.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", f"{checkpoint_dir}/features")
        .trigger(processingTime=trigger_interval)
        .outputMode("append")
        .start()
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_raw_stream(spark, args.bootstrap_servers, args.input_topic, args.starting_offsets)
    valid, invalid = split_valid_and_invalid(raw)
    valid_with_watermark = valid.withWatermark("timestamp", WATERMARK_DELAY)

    write_dead_letter(invalid, args.bootstrap_servers, args.dead_letter_topic, args.checkpoint_dir, args.trigger_interval)

    if args.enable_window_demo:
        start_window_demo_query(valid_with_watermark, args.trigger_interval)

    feature_stream = compute_feature_stream(valid_with_watermark)
    write_features_to_redis(
        feature_stream, args.redis_host, args.redis_port, args.redis_ttl_seconds, args.checkpoint_dir, args.trigger_interval
    )

    # blocks until any one of the queries above stops (normally only on
    # failure or a driver shutdown) so a crash in one query surfaces instead
    # of the job silently limping along on the others.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
