"""The nightly analytics batch job -- the build plan's own "if Airflow is
too heavy, a cron-triggered Python script with clear DAG structure" cut.
There's no actual OS-level cron wired up inside this container: in a real
deployment this script is what cron or Airflow would invoke on a schedule;
here it's invoked manually (`docker compose run --rm nightly-batch`, see
the README) so the whole run is easy to watch and reason about.

Five explicit, numbered steps -- the "clear DAG structure" the plan asks
for, just expressed as a Python function calling five other functions in
order rather than an Airflow DAG object:
  1. Data quality gates over bronze (fail loudly).
  2. Gold aggregates: daily fraud rate, customer risk profiles, per-rule
     precision, alert funnel.
  3. PSI drift: an early reference window vs. the most recent window.
  4. Retrain-check: challenger vs. champion, report only.
  5. Write one JSON report tying all four together.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pulseguard_analytics import gold
from pulseguard_analytics.data_quality import results_to_dicts, run_data_quality_gates
from pulseguard_analytics.psi import psi_report
from pulseguard_analytics.retrain_check import retrain_and_compare
from pulseguard_detection.dataset import FEATURE_NAMES
from pulseguard_streaming.feature_job import build_spark


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PulseGuard nightly analytics batch job")
    p.add_argument("--bronze-path", default="/opt/pulseguard/delta/bronze")
    p.add_argument("--bootstrap-servers", default="kafka:19092")
    p.add_argument("--detection-artifacts-dir", default="/opt/pulseguard/detection-artifacts")
    p.add_argument("--output-dir", default="/opt/pulseguard/analytics-output")
    p.add_argument("--gold-path", default="/opt/pulseguard/delta/gold")
    p.add_argument("--reference-days", type=int, default=7, help="how many of the earliest available bronze days form the PSI reference window")
    p.add_argument("--current-days", type=int, default=1, help="how many of the most recent bronze days form the PSI current window")
    return p.parse_args(argv)


def _step(n: int, title: str) -> None:
    print(f"\n=== step {n}: {title} ===")


def spark_df_to_pandas(df: DataFrame) -> pd.DataFrame:
    """`DataFrame.toPandas()`'s Arrow-optimized path falls back to a
    legacy per-column conversion on this Spark/pyarrow combination, which
    calls pandas' now-removed unit-less `astype('datetime64')` and raises
    `TypeError: Casting to unit-less dtype 'datetime64' is not supported`
    against pandas>=2.0. Collecting rows and building the DataFrame
    directly sidesteps that version-compatibility trap entirely, and at
    this job's batch scale (a bronze window, not the live stream) the
    performance difference against Arrow-based conversion doesn't matter."""
    rows = [row.asDict() for row in df.collect()]
    return pd.DataFrame(rows)


def run_data_quality(bronze) -> list[dict]:
    bronze_pdf = spark_df_to_pandas(bronze)
    # secondsSincePrevTxn is null exactly once per customer -- their
    # first-ever transaction (see feature_state.apply_event) -- so a
    # blanket 1% null-rate threshold across every feature flags it as a
    # false "quality" failure at any real customer count. It's the one
    # feature excluded from the null-rate gate, not because nulls there
    # don't matter, but because a nonzero rate there is the *correct*
    # value, not a symptom of an upstream break.
    null_check_columns = [c for c in ("txnId", "customerId", "timestamp", "amount", *FEATURE_NAMES) if c != "secondsSincePrevTxn"]
    results = run_data_quality_gates(
        bronze_pdf,
        required_columns=null_check_columns,
        baseline_daily_volume=None,  # no historical baseline tracked yet in this demo-scale run
    )
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"  [{status}] {r.name}: {r.detail}")
    return results_to_dicts(results)


def run_gold_aggregates(spark, bronze, bootstrap_servers: str, gold_path: str) -> dict:
    fraud_by_channel = gold.daily_fraud_rate_by_channel_category(bronze)
    risk_profiles = gold.customer_risk_profiles(bronze)

    alerts = gold.read_alerts(spark, bootstrap_servers)
    dispositions = gold.read_dispositions(spark, bootstrap_servers)
    rule_precision = gold.per_rule_precision(alerts, dispositions)
    funnel = gold.alert_funnel(alerts, dispositions)

    for name, df in [
        ("daily_fraud_rate_by_channel_category", fraud_by_channel),
        ("customer_risk_profiles", risk_profiles),
        ("per_rule_precision", rule_precision),
        ("alert_funnel", funnel),
    ]:
        df.write.format("delta").mode("overwrite").save(f"{gold_path}/{name}")
        print(f"  wrote gold/{name} ({df.count()} rows)")

    return {
        "per_rule_precision": [r.asDict() for r in rule_precision.collect()],
        "alert_funnel": [r.asDict() for r in funnel.collect()],
    }


def run_psi_drift(bronze, reference_days: int, current_days: int) -> list[dict]:
    date_bounds = bronze.select(F.min("date").alias("min"), F.max("date").alias("max")).first()
    if date_bounds["min"] is None:
        print("  no bronze data yet -- skipping PSI")
        return []

    reference_cutoff = F.date_add(F.lit(date_bounds["min"]), reference_days)
    current_cutoff = F.date_sub(F.lit(date_bounds["max"]), current_days)

    reference_pdf = spark_df_to_pandas(bronze.filter(F.col("date") < reference_cutoff).select(*FEATURE_NAMES))
    current_pdf = spark_df_to_pandas(bronze.filter(F.col("date") >= current_cutoff).select(*FEATURE_NAMES))

    if reference_pdf.empty or current_pdf.empty:
        print(f"  insufficient data for a reference/current split (reference={len(reference_pdf)} rows, current={len(current_pdf)} rows) -- skipping PSI")
        return []

    report = psi_report(reference_pdf, current_pdf, FEATURE_NAMES)
    for row in report[:10]:
        print(f"  {row['classification']:>17}  psi={row['psi']}  {row['feature']}")
    flagged = [r for r in report if r["classification"] == "SIGNIFICANT_SHIFT"]
    if flagged:
        print(f"  ALERT: {len(flagged)} feature(s) show significant drift -- flag for retraining review")
    return report


def run_retrain_check(bronze, detection_artifacts_dir: str) -> dict:
    bronze_pdf = spark_df_to_pandas(bronze.select("timestamp", "isFraud", *FEATURE_NAMES))
    result = retrain_and_compare(bronze_pdf, detection_artifacts_dir)
    print(f"  {result}")
    return result


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    spark = build_spark("pulseguard-nightly-analytics")
    spark.sparkContext.setLogLevel("WARN")

    print(f"PulseGuard nightly analytics run -- {datetime.now(timezone.utc).isoformat()}")
    bronze = gold.read_bronze(spark, args.bronze_path)

    _step(1, "data quality gates")
    quality_results = run_data_quality(bronze)

    _step(2, "gold aggregates")
    gold_summary = run_gold_aggregates(spark, bronze, args.bootstrap_servers, args.gold_path)

    _step(3, "PSI drift")
    psi_results = run_psi_drift(bronze, args.reference_days, args.current_days)

    _step(4, "retrain check (challenger vs. champion, report only)")
    retrain_result = run_retrain_check(bronze, args.detection_artifacts_dir)

    _step(5, "writing report")
    report = {
        "runAt": datetime.now(timezone.utc).isoformat(),
        "dataQuality": quality_results,
        "goldSummary": gold_summary,
        "psiDrift": psi_results,
        "retrainCheck": retrain_result,
        "dataQualityPassed": all(r["passed"] for r in quality_results),
    }
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"nightly_report_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.json"
    report_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"  report written to {report_path}")

    if not report["dataQualityPassed"]:
        print("\nFAILING LOUDLY: one or more data quality gates failed -- see step 1 output above.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
