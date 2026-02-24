"""
Final aggregated summary table printed after all runs complete.
"""

from __future__ import annotations

from typing import Optional

from tabulate import tabulate

from core.metrics import AggregatedField, BenchmarkSummary


def _fmt(v: Optional[float], dp: int = 1, suffix: str = "") -> str:
    if v is None:
        return "—"
    if dp == 0:
        return f"{int(v):,}{suffix}"
    return f"{v:,.{dp}f}{suffix}"


def _agg_row(label: str, field: Optional[AggregatedField], dp: int = 1, suffix: str = "") -> list:
    if field is None:
        return [label, "—", "—", "—", "—", "—"]
    return [
        label,
        _fmt(field.mean, dp, suffix),
        _fmt(field.stddev, dp, suffix),
        _fmt(field.min, dp, suffix),
        _fmt(field.max, dp, suffix),
        _fmt(field.p50, dp, suffix),
    ]


def print_summary(summary: BenchmarkSummary) -> None:
    run_label = (
        f"{summary.runs_requested} runs "
        f"({summary.runs_completed} ok, {summary.runs_failed} failed)"
    )
    print()
    print("=" * 72)
    print(
        f"  Benchmark Summary  |  preset: {summary.preset}  |  {run_label}"
    )
    print(
        f"  broker: {summary.broker}  |  "
        f"msg_count: {summary.msg_count:,}  |  msg_size: {summary.msg_size:,} B  |  "
        f"client: {summary.client_impl}"
    )
    print("=" * 72)

    agg_headers = ["Metric", "mean", "stddev", "min", "max", "p50"]

    if summary.producer:
        p = summary.producer
        prod_rows = [
            _agg_row("msgs/s",         p.msgs_per_sec,       dp=0),
            _agg_row("MB/s",           p.mb_per_sec,         dp=2),
            _agg_row("DR lat avg (µs)", p.dr_latency_avg_us,  dp=1),
            _agg_row("DR lat p99 (µs)", p.dr_latency_p99_us,  dp=1),
            _agg_row("msgs failed",    p.msgs_failed,         dp=0),
        ]
        print()
        print(f"  Producer ({summary.client_impl})")
        print(tabulate(prod_rows, headers=agg_headers, tablefmt="simple"))

    if summary.consumer:
        c = summary.consumer
        cons_rows = [
            _agg_row("msgs/s",          c.msgs_per_sec,       dp=0),
            _agg_row("MB/s",            c.mb_per_sec,         dp=2),
            _agg_row("E2E lat avg (ms)", c.e2e_latency_avg_ms, dp=2),
            _agg_row("E2E lat p99 (ms)", c.e2e_latency_p99_ms, dp=2),
            _agg_row("lag at end",      c.consumer_lag_at_end, dp=0),
        ]
        print()
        print(f"  Consumer ({summary.client_impl})")
        print(tabulate(cons_rows, headers=agg_headers, tablefmt="simple"))

    print()
