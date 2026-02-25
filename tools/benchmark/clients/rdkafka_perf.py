"""
Client implementation that wraps the locally built rdkafka_benchmark binary.

This client directly exercises the librdkafka C library built from the local
source tree, making it suitable for benchmarking the impact of local changes
or a specific git commit.

Binary location: <repo_root>/examples/rdkafka_benchmark
Build with:      cd <repo_root> && ./configure && make

STDOUT protocol from the C binary:
  Consumer: prints "BENCHMARK_READY\\n" after partition assignment,
            then a JSON result object when done.
  Producer: prints a single JSON result object when done.

All diagnostic/progress output from the binary goes to its STDERR.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
import traceback
from multiprocessing import Event, Queue
from pathlib import Path
from typing import Optional

from clients.base import BenchmarkConsumer, BenchmarkProducer
from core.metrics import ConsumerResult, ProducerResult

# Path to the compiled binary, relative to this file:
# clients/rdkafka_perf.py → tools/benchmark/clients/ → tools/benchmark/ →
# tools/ → repo_root → examples/rdkafka_benchmark
_REPO_ROOT = Path(__file__).parent.parent.parent.parent
_BINARY    = _REPO_ROOT / "examples" / "rdkafka_benchmark"

# STDOUT sentinel printed by the C consumer on partition assignment
_READY_SENTINEL = "BENCHMARK_READY"


def _check_binary(binary: Path) -> Path:
    """Verify the binary exists; raise RuntimeError with a helpful message."""
    if not binary.exists():
        raise RuntimeError(
            f"Binary not found at {binary}\n"
            f"Build it with:\n"
            f"  cd {_REPO_ROOT}\n"
            f"  ./configure && make"
        )
    if not os.access(binary, os.X_OK):
        raise RuntimeError(
            f"{binary} exists but is not executable. "
            f"Re-run 'make' in {_REPO_ROOT}."
        )
    return binary



def _build_producer_cmd(
    binary: Path,
    broker: str,
    topic: str,
    msg_size: int,
    msg_count: int,
    duration_sec: Optional[int],
    kafka_config: dict,
) -> list:
    """Build the rdkafka_benchmark -P command line."""
    cmd = [str(binary), "-P", "-b", broker, "-t", topic, "-s", str(msg_size)]

    # Message count (-c): pass only if it's a real cap (not sys.maxsize)
    if msg_count and msg_count < sys.maxsize:
        cmd += ["-c", str(msg_count)]

    # Duration for time-driven mode
    if duration_sec:
        cmd += ["-d", str(duration_sec)]

    # All librdkafka config properties via -X, except bootstrap.servers
    # (already passed via -b)
    for k, v in kafka_config.items():
        if k != "bootstrap.servers":
            cmd += ["-X", f"{k}={v}"]

    return cmd


def _build_consumer_cmd(
    binary: Path,
    broker: str,
    topic: str,
    msg_count: int,
    kafka_config: dict,
) -> list:
    """Build the rdkafka_benchmark -C command line."""
    group_id = kafka_config.get("group.id", "rdkafka-benchmark-cg")
    cmd = [str(binary), "-C", "-b", broker, "-t", topic, "-G", group_id]

    # Message count cap
    if msg_count and msg_count < sys.maxsize:
        cmd += ["-c", str(msg_count)]

    # All librdkafka config except keys already set explicitly
    skip = {"bootstrap.servers", "group.id"}
    for k, v in kafka_config.items():
        if k not in skip:
            cmd += ["-X", f"{k}={v}"]

    return cmd


def _parse_producer_result(data: dict) -> ProducerResult:
    return ProducerResult(
        msgs_produced=data.get("msgs_produced", 0),
        msgs_delivered=data.get("msgs_delivered", 0),
        msgs_failed=data.get("msgs_failed", 0),
        bytes_delivered=data.get("bytes_delivered", 0),
        elapsed_sec=data.get("elapsed_sec", 0.0),
        msgs_per_sec=data.get("msgs_per_sec", 0.0),
        mb_per_sec=data.get("mb_per_sec", 0.0),
        dr_latency_avg_us=data.get("dr_latency_avg_us"),
        dr_latency_p50_us=data.get("dr_latency_p50_us"),
        dr_latency_p95_us=data.get("dr_latency_p95_us"),
        dr_latency_p99_us=data.get("dr_latency_p99_us"),
        dr_latency_min_us=data.get("dr_latency_min_us"),
        dr_latency_max_us=data.get("dr_latency_max_us"),
    )


def _parse_consumer_result(data: dict) -> ConsumerResult:
    return ConsumerResult(
        msgs_consumed=data.get("msgs_consumed", 0),
        bytes_consumed=data.get("bytes_consumed", 0),
        elapsed_sec=data.get("elapsed_sec", 0.0),
        msgs_per_sec=data.get("msgs_per_sec", 0.0),
        mb_per_sec=data.get("mb_per_sec", 0.0),
        e2e_latency_avg_ms=data.get("e2e_latency_avg_ms"),
        e2e_latency_p50_ms=data.get("e2e_latency_p50_ms"),
        e2e_latency_p95_ms=data.get("e2e_latency_p95_ms"),
        e2e_latency_p99_ms=data.get("e2e_latency_p99_ms"),
        e2e_latency_min_ms=data.get("e2e_latency_min_ms"),
        e2e_latency_max_ms=data.get("e2e_latency_max_ms"),
        consumer_lag_at_end=0,  # not computed by C binary
    )


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------


class RdkafkaPerfProducer(BenchmarkProducer):
    """Wraps the locally-built rdkafka_benchmark binary in producer mode."""

    def _binary_path(self) -> Path:
        """Return the path to the benchmark binary. Override in subclasses."""
        return _BINARY

    def client_name(self) -> str:
        return f"rdkafka_benchmark @ {self._binary_path().parent}"

    def run(
        self,
        msg_count: int,
        msg_size: int,
        result_queue: Queue,
        duration_sec: Optional[int] = None,
    ) -> None:
        result = ProducerResult()
        try:
            binary = _check_binary(self._binary_path())
            cmd = _build_producer_cmd(
                binary, self.broker, self.topic,
                msg_size, msg_count, duration_sec, self.config,
            )

            # Compute a Python-side timeout for the subprocess
            timeout = (duration_sec or 0) + 120  # duration + 2-min flush buffer

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            try:
                stdout_bytes, stderr_bytes = proc.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout_bytes, _ = proc.communicate()
                result.error = f"Producer binary timed out after {timeout}s"
                result_queue.put(result)
                return

            # Log any stderr output at debug level
            if stderr_bytes:
                for line in stderr_bytes.decode(errors="replace").splitlines():
                    pass  # Binary writes progress to stderr; ignore in runner

            if proc.returncode not in (0, 1):
                # Return code 1 means some delivery failures — still valid
                result.error = (
                    f"Producer binary exited with code {proc.returncode}"
                )
                result_queue.put(result)
                return

            # Parse JSON from stdout
            stdout = stdout_bytes.decode(errors="replace").strip()
            json_line = next(
                (l for l in reversed(stdout.splitlines()) if l.startswith("{")),
                None,
            )
            if not json_line:
                result.error = (
                    f"No JSON in producer output. stdout={stdout[:300]}"
                )
                result_queue.put(result)
                return

            data = json.loads(json_line)
            result = _parse_producer_result(data)

        except Exception:
            result.error = traceback.format_exc()

        result_queue.put(result)


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------


class RdkafkaPerfConsumer(BenchmarkConsumer):
    """Wraps the locally-built rdkafka_benchmark binary in consumer mode."""

    def _binary_path(self) -> Path:
        """Return the path to the benchmark binary. Override in subclasses."""
        return _BINARY

    def client_name(self) -> str:
        return f"rdkafka_benchmark @ {self._binary_path().parent}"

    def run(
        self,
        msg_count: int,
        result_queue: Queue,
        consumer_ready_event: Event,
        duration_sec: Optional[int] = None,
        producer_done: Optional[Event] = None,
        drain_timeout_sec: int = 30,
    ) -> None:
        result = ConsumerResult()
        proc = None

        try:
            binary = _check_binary(self._binary_path())
            cmd = _build_consumer_cmd(
                binary, self.broker, self.topic, msg_count, self.config,
            )

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            stdout_lines: list = []
            ready_seen = [False]

            def _read_stdout():
                """Read binary stdout line by line in a background thread.
                Sets consumer_ready_event when BENCHMARK_READY is seen.
                All other lines are collected for JSON parsing at the end.
                """
                for raw in proc.stdout:
                    line = raw.decode(errors="replace").strip()
                    if line == _READY_SENTINEL and not ready_seen[0]:
                        ready_seen[0] = True
                        consumer_ready_event.set()
                    else:
                        stdout_lines.append(line)

            reader = threading.Thread(target=_read_stdout, daemon=True)
            reader.start()

            # In time-driven mode: wait for producer_done, then give the C
            # binary a drain window before sending SIGTERM.
            if duration_sec is not None and producer_done is not None:
                def _drain_watcher():
                    producer_done.wait()
                    time.sleep(drain_timeout_sec)
                    if proc.poll() is None:
                        try:
                            proc.terminate()
                        except OSError:
                            pass

                threading.Thread(target=_drain_watcher, daemon=True).start()

            reader.join()  # blocks until C binary exits (stdout pipe closes)

            # Safety: if binary exited without printing BENCHMARK_READY
            # (e.g. it crashed), unblock the runner.
            if not ready_seen[0]:
                consumer_ready_event.set()

            proc.wait()

            # Find the JSON result line in collected output
            json_line = next(
                (l for l in reversed(stdout_lines) if l.startswith("{")),
                None,
            )
            if not json_line:
                # Try the binary's stderr for a clue
                stderr_preview = ""
                try:
                    stderr_preview = proc.stderr.read(500).decode(
                        errors="replace"
                    )
                except Exception:
                    pass
                result.error = (
                    f"No JSON from consumer binary "
                    f"(exit={proc.returncode}). "
                    f"stderr={stderr_preview[:200]}"
                )
                result_queue.put(result)
                return

            data = json.loads(json_line)
            result = _parse_consumer_result(data)

        except Exception:
            result.error = traceback.format_exc()
            if not consumer_ready_event.is_set():
                consumer_ready_event.set()
            if proc and proc.poll() is None:
                try:
                    proc.terminate()
                except OSError:
                    pass

        result_queue.put(result)
