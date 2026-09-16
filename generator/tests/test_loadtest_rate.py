import time

from pulseguard_generator.cli import build_parser


def _run_loadtest(tmp_path, rate: float, duration: float):
    out_path = tmp_path / "out.jsonl"
    parser = build_parser()
    args = parser.parse_args(
        [
            "loadtest",
            "--customers",
            "50",
            "--sink",
            "jsonl",
            "--out",
            str(out_path),
            "--rate",
            str(rate),
            "--duration",
            str(duration),
            "--report-every",
            "0",
        ]
    )
    t0 = time.monotonic()
    args.func(args)
    elapsed = time.monotonic() - t0
    sent = sum(1 for _ in open(out_path, encoding="utf-8"))
    return sent, elapsed


def test_loadtest_achieves_a_low_target_rate_within_tolerance(tmp_path):
    # Regression test: the pacing loop used to sleep at most 10ms per
    # message regardless of how much longer it needed to wait, then
    # advance the schedule by a full interval anyway -- silently racing
    # ahead of any target below 100 tps. rate=20 previously measured
    # ~84-93 achieved tps; it must now land close to the requested rate.
    sent, elapsed = _run_loadtest(tmp_path, rate=20, duration=3)
    achieved = sent / elapsed
    assert 15 <= achieved <= 26, f"achieved {achieved:.1f} tps, expected close to 20"


def test_loadtest_achieves_a_moderate_target_rate_within_tolerance(tmp_path):
    sent, elapsed = _run_loadtest(tmp_path, rate=60, duration=3)
    achieved = sent / elapsed
    assert 48 <= achieved <= 75, f"achieved {achieved:.1f} tps, expected close to 60"
