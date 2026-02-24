"""
Write per-run results to a CSV file (one row per run).
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from core.metrics import BenchmarkSummary, ConsumerResult, ProducerResult

_COLUMNS = [
    "benchmark_id",
    "preset",
    "run_id",
    "topic",
    "status",
    "error",
    # Producer
    "p_msgs_produced",
    "p_msgs_delivered",
    "p_msgs_failed",
    "p_bytes_delivered",
    "p_elapsed_sec",
    "p_msgs_per_sec",
    "p_mb_per_sec",
    "p_dr_lat_avg_us",
    "p_dr_lat_p50_us",
    "p_dr_lat_p95_us",
    "p_dr_lat_p99_us",
    "p_dr_lat_min_us",
    "p_dr_lat_max_us",
    # Consumer
    "c_msgs_consumed",
    "c_bytes_consumed",
    "c_elapsed_sec",
    "c_msgs_per_sec",
    "c_mb_per_sec",
    "c_e2e_lat_avg_ms",
    "c_e2e_lat_p50_ms",
    "c_e2e_lat_p95_ms",
    "c_e2e_lat_p99_ms",
    "c_e2e_lat_min_ms",
    "c_e2e_lat_max_ms",
    "c_consumer_lag_at_end",
]


def _p_fields(p: Optional[ProducerResult]) -> dict:
    if p is None:
        return {c: "" for c in _COLUMNS if c.startswith("p_")}
    return {
        "p_msgs_produced": p.msgs_produced,
        "p_msgs_delivered": p.msgs_delivered,
        "p_msgs_failed": p.msgs_failed,
        "p_bytes_delivered": p.bytes_delivered,
        "p_elapsed_sec": p.elapsed_sec,
        "p_msgs_per_sec": p.msgs_per_sec,
        "p_mb_per_sec": p.mb_per_sec,
        "p_dr_lat_avg_us": p.dr_latency_avg_us,
        "p_dr_lat_p50_us": p.dr_latency_p50_us,
        "p_dr_lat_p95_us": p.dr_latency_p95_us,
        "p_dr_lat_p99_us": p.dr_latency_p99_us,
        "p_dr_lat_min_us": p.dr_latency_min_us,
        "p_dr_lat_max_us": p.dr_latency_max_us,
    }


def _c_fields(c: Optional[ConsumerResult]) -> dict:
    if c is None:
        return {col: "" for col in _COLUMNS if col.startswith("c_")}
    return {
        "c_msgs_consumed": c.msgs_consumed,
        "c_bytes_consumed": c.bytes_consumed,
        "c_elapsed_sec": c.elapsed_sec,
        "c_msgs_per_sec": c.msgs_per_sec,
        "c_mb_per_sec": c.mb_per_sec,
        "c_e2e_lat_avg_ms": c.e2e_latency_avg_ms,
        "c_e2e_lat_p50_ms": c.e2e_latency_p50_ms,
        "c_e2e_lat_p95_ms": c.e2e_latency_p95_ms,
        "c_e2e_lat_p99_ms": c.e2e_latency_p99_ms,
        "c_e2e_lat_min_ms": c.e2e_latency_min_ms,
        "c_e2e_lat_max_ms": c.e2e_latency_max_ms,
        "c_consumer_lag_at_end": c.consumer_lag_at_end,
    }


def write(summary: BenchmarkSummary, output_dir: Path) -> Path:
    """Write per-run CSV. Returns the path to the written file."""
    output_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"benchmark_{summary.preset}_{ts}.csv"
    path = output_dir / filename

    benchmark_id = f"benchmark_{summary.preset}_{ts}"

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_COLUMNS)
        writer.writeheader()

        for run in summary.runs:
            row: dict = {
                "benchmark_id": benchmark_id,
                "preset": summary.preset,
                "run_id": run.run_id,
                "topic": run.topic,
                "status": run.status,
                "error": run.error or "",
            }
            row.update(_p_fields(run.producer))
            row.update(_c_fields(run.consumer))
            writer.writerow(row)

    return path
