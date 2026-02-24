"""
confluent-kafka-python producer and consumer implementations.

Both classes are designed to run in isolated multiprocessing.Process instances.
No state is shared across the process boundary — all results go via Queue.

Stopping modes:
  COUNT mode  (duration_sec=None): stop at msg_count messages.
  TIME mode   (duration_sec set):  producer stops when duration_sec elapses;
                                   consumer stops after producer_done fires
                                   + drain_timeout_sec drain window.
  COMBINED:   both limits active — whichever fires first stops the producer.
"""

from __future__ import annotations

import importlib.metadata
import struct
import time
import traceback
from multiprocessing import Event, Queue
from typing import List, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from clients.base import BenchmarkConsumer, BenchmarkProducer
from core.metrics import ConsumerResult, ProducerResult

# Payload layout:
#   bytes 0-7  : big-endian int64 nanosecond timestamp (time.perf_counter_ns())
#   bytes 8-N  : zero-fill padding
_TS_SIZE = 8
_TS_FORMAT = ">q"

# Sanity bounds for E2E latency (discard implausible samples)
_E2E_MIN_NS = 0
_E2E_MAX_NS = 5 * 60 * 1_000_000_000  # 5 minutes


def _client_version() -> str:
    try:
        ver = importlib.metadata.version("confluent-kafka")
    except Exception:
        ver = "unknown"
    return f"confluent-kafka-python {ver}"


def _compute_percentiles(values: List[float], percentiles: List[int]) -> List[Optional[float]]:
    """Return requested percentile values from a list."""
    if not values:
        return [None] * len(percentiles)
    try:
        import numpy as np
        return [float(np.percentile(values, p)) for p in percentiles]
    except ImportError:
        pass
    # Fallback: linear interpolation
    n = len(values)
    sv = sorted(values)
    results = []
    for p in percentiles:
        idx = (p / 100) * (n - 1)
        lo, hi = int(idx), min(int(idx) + 1, n - 1)
        frac = idx - lo
        results.append(sv[lo] + frac * (sv[hi] - sv[lo]))
    return results


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

class ConfluentProducer(BenchmarkProducer):
    """confluent-kafka Producer implementation."""

    def client_name(self) -> str:
        return _client_version()

    def run(
        self,
        msg_count: int,
        msg_size: int,
        result_queue: Queue,
        duration_sec: Optional[int] = None,
    ) -> None:
        result = ProducerResult()
        embed_ts = msg_size >= _TS_SIZE

        dr_latencies_us: List[float] = []
        delivered = [0]
        failed = [0]
        bytes_ok = [0]

        try:
            producer = Producer(self.config)
            is_transactional = "transactional.id" in self.config

            if is_transactional:
                producer.init_transactions()

            padding = b"\x00" * max(0, msg_size - _TS_SIZE)

            def on_delivery(err, msg):
                now_ns = time.perf_counter_ns()
                if err is not None:
                    failed[0] += 1
                else:
                    delivered[0] += 1
                    bytes_ok[0] += len(msg.value())
                    if embed_ts:
                        try:
                            ts_ns = struct.unpack(_TS_FORMAT, msg.value()[:_TS_SIZE])[0]
                            dr_latencies_us.append((now_ns - ts_ns) / 1_000.0)
                        except Exception:
                            pass

            if is_transactional:
                producer.begin_transaction()

            t_start = time.monotonic()
            msgs_produced = 0
            deadline = (t_start + duration_sec) if duration_sec is not None else None

            while msgs_produced < msg_count:
                # TIME mode: stop when duration_sec has elapsed
                if deadline is not None and time.monotonic() >= deadline:
                    break

                if embed_ts:
                    ts_bytes = struct.pack(_TS_FORMAT, time.perf_counter_ns())
                    payload = ts_bytes + padding
                else:
                    payload = padding if padding else b"\x00"

                while True:
                    try:
                        producer.produce(
                            self.topic,
                            value=payload,
                            on_delivery=on_delivery,
                        )
                        break
                    except KafkaException as e:
                        if e.args[0].code() == KafkaError._QUEUE_FULL:
                            producer.poll(0.01)
                        else:
                            raise

                msgs_produced += 1

                # Poll periodically to fire delivery report callbacks
                if msgs_produced % 10_000 == 0:
                    producer.poll(0)

            # Record when producing stopped (before flush) for elapsed calculation
            t_end_send = time.monotonic()

            # Wait for all in-flight messages
            producer.flush()

            if is_transactional:
                producer.commit_transaction()

            elapsed = max(t_end_send - t_start, 1e-9)

            p50, p95, p99 = _compute_percentiles(dr_latencies_us, [50, 95, 99])

            result = ProducerResult(
                msgs_produced=msgs_produced,
                msgs_delivered=delivered[0],
                msgs_failed=failed[0],
                bytes_delivered=bytes_ok[0],
                elapsed_sec=elapsed,
                msgs_per_sec=delivered[0] / elapsed,
                mb_per_sec=bytes_ok[0] / elapsed / 1_048_576,
                dr_latency_avg_us=sum(dr_latencies_us) / len(dr_latencies_us) if dr_latencies_us else None,
                dr_latency_p50_us=p50,
                dr_latency_p95_us=p95,
                dr_latency_p99_us=p99,
                dr_latency_min_us=min(dr_latencies_us) if dr_latencies_us else None,
                dr_latency_max_us=max(dr_latencies_us) if dr_latencies_us else None,
            )

        except Exception:
            result.error = traceback.format_exc()

        result_queue.put(result)


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------

class ConfluentConsumer(BenchmarkConsumer):
    """confluent-kafka Consumer implementation."""

    def client_name(self) -> str:
        return _client_version()

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
        e2e_latencies_ms: List[float] = []

        try:
            assigned = [False]

            def on_assign(consumer, partitions):
                if not assigned[0]:
                    assigned[0] = True
                    consumer_ready_event.set()

            consumer = Consumer(self.config)
            consumer.subscribe([self.topic], on_assign=on_assign)

            msgs_consumed = 0
            bytes_consumed = 0
            t_start: Optional[float] = None
            t_end: Optional[float] = None
            last_offset = -1
            drain_started_at: Optional[float] = None

            while True:
                now = time.monotonic()

                # --- Stopping conditions ---

                # COUNT mode: stop at msg_count
                if msgs_consumed >= msg_count:
                    break

                # TIME mode: stop based on producer_done + drain window
                if duration_sec is not None and producer_done is not None:
                    if producer_done.is_set():
                        if drain_started_at is None:
                            drain_started_at = now
                        elif now - drain_started_at >= drain_timeout_sec:
                            break  # drain window exhausted

                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    err_code = msg.error().code()
                    if err_code == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                now_ns = time.perf_counter_ns()

                if t_start is None:
                    t_start = now

                t_end = now
                msgs_consumed += 1
                val = msg.value()
                bytes_consumed += len(val) if val else 0
                last_offset = msg.offset()

                # E2E latency from payload timestamp
                if val and len(val) >= _TS_SIZE:
                    try:
                        ts_ns = struct.unpack(_TS_FORMAT, val[:_TS_SIZE])[0]
                        delta_ns = now_ns - ts_ns
                        if _E2E_MIN_NS < delta_ns < _E2E_MAX_NS:
                            e2e_latencies_ms.append(delta_ns / 1_000_000.0)
                    except Exception:
                        pass

            # Measure consumer lag
            lag = 0
            try:
                partitions = consumer.assignment()
                for tp in partitions:
                    tp.offset = last_offset
                    lo, hi = consumer.get_watermark_offsets(tp, timeout=5.0)
                    lag += max(0, hi - last_offset - 1)
            except Exception:
                lag = -1  # could not determine

            consumer.close()

            elapsed = max((t_end or time.monotonic()) - (t_start or time.monotonic()), 1e-9)

            p50, p95, p99 = _compute_percentiles(e2e_latencies_ms, [50, 95, 99])

            result = ConsumerResult(
                msgs_consumed=msgs_consumed,
                bytes_consumed=bytes_consumed,
                elapsed_sec=elapsed,
                msgs_per_sec=msgs_consumed / elapsed,
                mb_per_sec=bytes_consumed / elapsed / 1_048_576,
                e2e_latency_avg_ms=sum(e2e_latencies_ms) / len(e2e_latencies_ms) if e2e_latencies_ms else None,
                e2e_latency_p50_ms=p50,
                e2e_latency_p95_ms=p95,
                e2e_latency_p99_ms=p99,
                e2e_latency_min_ms=min(e2e_latencies_ms) if e2e_latencies_ms else None,
                e2e_latency_max_ms=max(e2e_latencies_ms) if e2e_latencies_ms else None,
                consumer_lag_at_end=lag,
            )

        except Exception:
            result.error = traceback.format_exc()
            if not consumer_ready_event.is_set():
                consumer_ready_event.set()

        result_queue.put(result)
