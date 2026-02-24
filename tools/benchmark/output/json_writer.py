"""
Write full benchmark results to a JSON file.
"""

from __future__ import annotations

import dataclasses
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.metrics import BenchmarkSummary


def _to_dict(obj: Any) -> Any:
    """Recursively convert dataclasses and special types to JSON-serializable objects."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {k: _to_dict(v) for k, v in dataclasses.asdict(obj).items()}
    if isinstance(obj, (list, tuple)):
        return [_to_dict(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _to_dict(v) for k, v in obj.items()}
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj


def write(summary: BenchmarkSummary, config: dict, output_dir: Path) -> Path:
    """
    Serialize the full BenchmarkSummary + config snapshot to a JSON file.
    Returns the path to the written file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"benchmark_{summary.preset}_{ts}.json"
    path = output_dir / filename

    doc = {
        "schema_version": 1,
        "benchmark_id": filename.removesuffix(".json"),
        "preset": summary.preset,
        "broker": summary.broker,
        "client_impl": summary.client_impl,
        "runs_requested": summary.runs_requested,
        "runs_completed": summary.runs_completed,
        "runs_failed": summary.runs_failed,
        "msg_count": summary.msg_count,
        "msg_size": summary.msg_size,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "config": config,
        "runs": [_to_dict(r) for r in summary.runs],
        "aggregated": {
            "producer": _to_dict(summary.producer),
            "consumer": _to_dict(summary.consumer),
        },
    }

    with open(path, "w") as f:
        json.dump(doc, f, indent=2, default=str)

    return path
