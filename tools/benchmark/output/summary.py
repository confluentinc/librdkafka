"""
Final aggregated summary table printed after all runs complete.
"""

from __future__ import annotations

from typing import Optional

from tabulate import tabulate

from core.metrics import AggregatedField, BenchmarkSummary
from core.resource_monitor import ResourceMonitor


def _fmt(v: Optional[float], dp: int = 1, suffix: str = "") -> str:
    if v is None:
        return "—"
    if dp == 0:
        return f"{int(v):,}{suffix}"
    return f"{v:,.{dp}f}{suffix}"


def _agg_row(label: str, field: Optional[AggregatedField],
             dp: int = 1, suffix: str = "") -> list:
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


def _resource_note(client_impl: str) -> str:
    """Return a contextual accuracy note for CPU/memory metrics."""
    if "rdkafka_benchmark" in client_impl or "rdkafka_perf" in client_impl:
        return (
            "  * CPU/memory: process-tree measurement; closely reflects "
            "librdkafka's own usage (rdkafka_perf client)."
        )
    return (
        "  * CPU/memory: process-tree measurement includes Python interpreter "
        "overhead (~30-50 MB RSS baseline, ~5-15% CPU). "
        "For pure librdkafka figures use --client rdkafka_perf."
    )


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

    has_resource = False

    if summary.producer:
        p = summary.producer
        prod_rows = [
            _agg_row("msgs/s",          p.msgs_per_sec,      dp=0),
            _agg_row("MB/s",            p.mb_per_sec,        dp=2),
            _agg_row("DR lat avg (µs)", p.dr_latency_avg_us, dp=1),
            _agg_row("DR lat p99 (µs)", p.dr_latency_p99_us, dp=1),
            _agg_row("msgs failed",     p.msgs_failed,       dp=0),
        ]
        if p.cpu_avg_pct is not None:
            prod_rows += [
                _agg_row("CPU avg (%)*",  p.cpu_avg_pct,  dp=1),
                _agg_row("CPU max (%)*",  p.cpu_max_pct,  dp=1),
                _agg_row("mem peak (MB)*", p.mem_peak_mb, dp=1),
            ]
            has_resource = True
        print()
        print(f"  Producer ({summary.client_impl})")
        print(tabulate(prod_rows, headers=agg_headers, tablefmt="simple"))

    if summary.consumer:
        c = summary.consumer
        cons_rows = [
            _agg_row("msgs/s",           c.msgs_per_sec,       dp=0),
            _agg_row("MB/s",             c.mb_per_sec,         dp=2),
            _agg_row("E2E lat avg (ms)", c.e2e_latency_avg_ms, dp=2),
            _agg_row("E2E lat p99 (ms)", c.e2e_latency_p99_ms, dp=2),
            _agg_row("lag at end",       c.consumer_lag_at_end, dp=0),
        ]
        if c.cpu_avg_pct is not None:
            cons_rows += [
                _agg_row("CPU avg (%)*",  c.cpu_avg_pct,  dp=1),
                _agg_row("CPU max (%)*",  c.cpu_max_pct,  dp=1),
                _agg_row("mem peak (MB)*", c.mem_peak_mb, dp=1),
            ]
            has_resource = True
        print()
        print(f"  Consumer ({summary.client_impl})")
        print(tabulate(cons_rows, headers=agg_headers, tablefmt="simple"))

    if has_resource:
        print()
        print(_resource_note(summary.client_impl))

    if not ResourceMonitor.psutil_available():
        print()
        print("  [info] psutil not installed — CPU/memory metrics unavailable.")
        print("         Install with: pip install psutil")

    print()
