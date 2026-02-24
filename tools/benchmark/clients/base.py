"""
Abstract base classes for benchmark producer and consumer.

Design contract:
  - Implementations MUST be safe to run inside a multiprocessing.Process.
  - No shared mutable state is permitted across the process boundary.
  - All results are returned via multiprocessing.Queue.
  - The consumer MUST signal consumer_ready_event after its partition
    assignment is confirmed, so the runner knows it is safe to start
    the producer.

Stopping modes:
  COUNT mode  (duration_sec=None): stop when msg_count messages are produced/consumed.
  TIME mode   (duration_sec set):  stop when duration_sec has elapsed.
                                   msg_count acts as a safety cap in time mode.
  COMBINED:   both limits active — whichever fires first stops the producer.

To add a new client implementation (e.g. C++, Java):
  1. Subclass BenchmarkProducer and BenchmarkConsumer.
  2. Register them in clients/__init__.py under a new key.
  3. Select at runtime with: --client <key>
"""

from __future__ import annotations

import abc
from multiprocessing import Event, Queue
from typing import Optional


class BenchmarkProducer(abc.ABC):
    """
    Abstract producer. Subclasses must implement `run()` and `client_name()`.

    The `run()` method is called in a dedicated subprocess. It must:
      - In COUNT mode: produce exactly `msg_count` messages.
      - In TIME mode:  produce until `duration_sec` elapses (or `msg_count` cap is hit).
      - Embed an 8-byte big-endian int64 nanosecond timestamp at the start
        of every payload (for E2E latency measurement by the consumer).
        If msg_size < 8, the timestamp MUST be omitted and E2E latency
        fields in ProducerResult must be left as None.
      - Track delivery report latency per message.
      - Put exactly one ProducerResult onto `result_queue` when done,
        or a ProducerResult with error set on failure.
    """

    def __init__(self, broker: str, topic: str, config: dict):
        self.broker = broker
        self.topic = topic
        self.config = config

    @abc.abstractmethod
    def run(
        self,
        msg_count: int,
        msg_size: int,
        result_queue: Queue,
        duration_sec: Optional[int] = None,
    ) -> None:
        """
        Produce messages and push a ProducerResult onto result_queue.

        Args:
            msg_count:    Max number of messages to produce. In time mode this
                          is a safety cap; production stops at duration_sec first.
            msg_size:     Byte size of each message payload.
            result_queue: multiprocessing.Queue to push ProducerResult onto.
            duration_sec: If set, stop after this many seconds regardless of
                          msg_count. None = count-driven mode.
        """

    @abc.abstractmethod
    def client_name(self) -> str:
        """Return a human-readable implementation name, e.g. 'confluent-kafka-python 2.3.0'."""


class BenchmarkConsumer(abc.ABC):
    """
    Abstract consumer. Subclasses must implement `run()` and `client_name()`.

    The `run()` method is called in a dedicated subprocess. It must:
      - Subscribe to the topic and signal `consumer_ready_event` after
        receiving its first partition assignment (via on_assign callback).
      - In COUNT mode: consume exactly `msg_count` messages.
      - In TIME mode:  consume until `producer_done` is set, then drain for
                       `drain_timeout_sec` more seconds.
      - Compute E2E latency from the 8-byte timestamp prefix in each payload.
      - Put exactly one ConsumerResult onto `result_queue` when done,
        or a ConsumerResult with error set on failure.
    """

    def __init__(self, broker: str, topic: str, config: dict):
        self.broker = broker
        self.topic = topic
        self.config = config

    @abc.abstractmethod
    def run(
        self,
        msg_count: int,
        result_queue: Queue,
        consumer_ready_event: Event,
        duration_sec: Optional[int] = None,
        producer_done: Optional[Event] = None,
        drain_timeout_sec: int = 30,
    ) -> None:
        """
        Consume messages and push a ConsumerResult onto result_queue.

        Args:
            msg_count:             Max messages to consume (safety cap in time mode).
            result_queue:          multiprocessing.Queue to push ConsumerResult onto.
            consumer_ready_event:  Set after partition assignment is confirmed.
            duration_sec:          If set, time-driven mode is active.
            producer_done:         Event set by the runner when the producer process
                                   exits. Consumer uses this to enter drain phase.
            drain_timeout_sec:     Seconds to keep consuming after producer_done fires,
                                   to drain any in-flight messages.
        """

    @abc.abstractmethod
    def client_name(self) -> str:
        """Return a human-readable implementation name."""
