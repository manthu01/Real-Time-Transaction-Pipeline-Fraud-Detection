from __future__ import annotations

import argparse
import random
import sys
import time
from datetime import datetime, timedelta

from .customers import generate_population
from .merchants import build_merchant_catalog
from .simulator import SimConfig, loadtest_stream, simulate_stream
from .sinks import JsonlSink, KafkaSink, Sink, StdoutSink


def _build_sink(args) -> Sink:
    if args.sink == "kafka":
        return KafkaSink(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            malformed_rate=args.malformed_rate,
            rng=random.Random(args.seed),
        )
    if args.sink == "jsonl":
        if not args.out:
            raise SystemExit("--out is required for --sink jsonl")
        return JsonlSink(args.out)
    return StdoutSink()


def _common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--customers", type=int, default=5000, help="number of synthetic customers")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--sink", choices=["kafka", "stdout", "jsonl"], default="stdout")
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--topic", default="raw.transactions")
    p.add_argument("--out", help="output path when --sink jsonl")
    p.add_argument("--malformed-rate", type=float, default=0.0, help="fraction of records to deliberately corrupt (dead-letter testing)")
    p.add_argument("--report-every", type=int, default=50000, help="print a stats line every N sent records (0 to disable)")


def cmd_simulate(args) -> None:
    rng = random.Random(args.seed)
    catalog = build_merchant_catalog(rng)
    customers = generate_population(args.customers, rng, catalog)

    start = datetime.now().astimezone() - timedelta(days=args.days)
    end = start + timedelta(days=args.days)

    sink = _build_sink(args)
    config = SimConfig(fraud_probability_per_customer_per_day=args.fraud_rate)

    sent = 0
    fraud_sent = 0
    t0 = time.monotonic()
    for txn in simulate_stream(customers, catalog, rng, start, end, config):
        sink.send(txn)
        sent += 1
        if txn["isFraud"]:
            fraud_sent += 1
        if args.report_every and sent % args.report_every == 0:
            print(f"[simulate] sent={sent} fraud={fraud_sent} ({fraud_sent/sent:.4%})", file=sys.stderr)
        if args.limit and sent >= args.limit:
            break

    sink.flush()
    elapsed = time.monotonic() - t0
    print(
        f"[simulate] done. customers={len(customers)} sent={sent} fraud={fraud_sent} "
        f"({fraud_sent/max(sent,1):.4%}) wall_time_sec={elapsed:.1f} sink_stats={sink.stats()}",
        file=sys.stderr,
    )
    sink.close()


def cmd_loadtest(args) -> None:
    rng = random.Random(args.seed)
    catalog = build_merchant_catalog(rng)
    customers = generate_population(args.customers, rng, catalog)

    sink = _build_sink(args)
    stream = loadtest_stream(customers, catalog, rng, fraud_probability=args.fraud_rate)

    target_interval = 1.0 / args.rate if args.rate > 0 else 0.0
    sent = 0
    t_start = time.monotonic()
    next_report = t_start + 5.0
    next_due = t_start

    for txn in stream:
        if target_interval > 0:
            # Sleep in short bounded increments until next_due is actually
            # reached, rather than one capped attempt -- a single
            # `sleep(min(gap, 0.01))` followed by advancing next_due by a
            # full interval regardless silently under-slept (and the loop
            # raced ahead of schedule) for any target_interval > 10ms, i.e.
            # any rate below 100 tps. That's exactly the "measure your
            # actual rate, don't claim a number you haven't hit" trap the
            # build plan warns about, just inside our own tooling: rate=50
            # was measurably achieving ~93 tps before this fix.
            now = time.monotonic()
            while now < next_due:
                time.sleep(min(next_due - now, 0.05))
                now = time.monotonic()
            next_due += target_interval

        sink.send(txn)
        sent += 1

        now = time.monotonic()
        if now >= next_report:
            elapsed = now - t_start
            print(f"[loadtest] sent={sent} elapsed={elapsed:.1f}s achieved_tps={sent/elapsed:.0f}", file=sys.stderr)
            next_report = now + 5.0

        if args.duration and (time.monotonic() - t_start) >= args.duration:
            break
        if args.limit and sent >= args.limit:
            break

    sink.flush()
    elapsed = time.monotonic() - t_start
    print(
        f"[loadtest] done. sent={sent} elapsed_sec={elapsed:.1f} achieved_tps={sent/max(elapsed,1e-9):.1f} "
        f"sink_stats={sink.stats()}",
        file=sys.stderr,
    )
    sink.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pulseguard-generator", description="PulseGuard synthetic transaction generator")
    sub = parser.add_subparsers(dest="command", required=True)

    p_sim = sub.add_parser("simulate", help="behaviourally-paced stream for building a labelled dataset")
    _common_args(p_sim)
    p_sim.add_argument("--days", type=int, default=30, help="span of simulated history to generate")
    p_sim.add_argument("--fraud-rate", type=float, default=0.003, help="probability per customer per day of a fraud burst")
    p_sim.add_argument("--limit", type=int, default=0, help="stop after N records (0 = unlimited / full span)")
    p_sim.set_defaults(func=cmd_simulate)

    p_load = sub.add_parser("loadtest", help="firehose stream at a target transactions/sec, for Week 5 load testing")
    _common_args(p_load)
    p_load.add_argument("--rate", type=float, default=1000.0, help="target transactions/sec (0 = as fast as possible)")
    p_load.add_argument("--duration", type=float, default=60.0, help="seconds to run (0 = unlimited)")
    p_load.add_argument("--fraud-rate", type=float, default=0.003, help="probability per emitted event of being a fraud burst")
    p_load.add_argument("--limit", type=int, default=0, help="stop after N records (0 = unlimited)")
    p_load.set_defaults(func=cmd_loadtest)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
