"""
Live terminal table — prints one row per completed run, refreshing headers
every 20 rows so the column names stay visible in long runs.
"""

from __future__ import annotations

import sys
from typing import List, Optional

from tabulate import tabulate

from core.metrics import ConsumerResult, ProducerResult

_HEADERS = [
    "Run",
    "P msgs/s",
    "P MB/s",
    "DR avg(µs)",
    "DR p99(µs)",
    "C msgs/s",
    "C MB/s",
    "E2E avg(ms)",
    "E2E p99(ms)",
    "C-lag",
    "Status",
]

_ROW_INTERVAL = 20  # reprint header every N rows


def _fmt_int(v: Optional[float]) -> str:
    if v is None:
        return "—"
    return f"{int(v):,}"


def _fmt_float(v: Optional[float], dp: int = 1) -> str:
    if v is None:
        return "—"
    return f"{v:,.{dp}f}"


class LiveTable:
    def __init__(self) -> None:
        self._rows: List[list] = []
        self._printed_rows = 0

    def _print_header(self) -> None:
        header_line = tabulate([], headers=_HEADERS, tablefmt="simple")
        # Only the header part (first two lines of tabulate output)
        lines = header_line.split("\n")
        print(lines[0])  # column names
        print(lines[1])  # separator
        sys.stdout.flush()

    def add_run(
        self,
        run_id: int,
        producer: Optional[ProducerResult],
        consumer: Optional[ConsumerResult],
        status: str,
        error: Optional[str] = None,
    ) -> None:
        if status != "ok" or producer is None or consumer is None:
            label = f"FAILED: {error or 'unknown error'}"
            row = [run_id] + ["—"] * (len(_HEADERS) - 2) + [label]
        else:
            row = [
                run_id,
                _fmt_int(producer.msgs_per_sec),
                _fmt_float(producer.mb_per_sec),
                _fmt_float(producer.dr_latency_avg_us),
                _fmt_float(producer.dr_latency_p99_us),
                _fmt_int(consumer.msgs_per_sec),
                _fmt_float(consumer.mb_per_sec),
                _fmt_float(consumer.e2e_latency_avg_ms),
                _fmt_float(consumer.e2e_latency_p99_ms),
                _fmt_int(consumer.consumer_lag_at_end),
                "OK",
            ]

        self._rows.append(row)

        if self._printed_rows == 0 or (self._printed_rows % _ROW_INTERVAL == 0):
            self._print_header()

        # Print just the data row using tabulate for alignment
        print(
            tabulate([row], headers=[], tablefmt="plain")
        )
        sys.stdout.flush()
        self._printed_rows += 1
