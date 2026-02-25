"""
C++ client — system-installed librdkafka (confluent_cpp).

Wraps examples/rdkafka_benchmark_cpp_sys compiled against the
system-installed librdkafka (-lrdkafka++ -lrdkafka).

This is the C++ equivalent of the 'confluent' Python client: it tests
the C++ API against a packaged/installed version of librdkafka rather
than your local source-tree build.

Accuracy note (yellow callout in HTML report):
  CPU and memory figures reflect the C++ benchmark process, which is a
  close measure of librdkafka's usage with minimal C++ layer overhead.
  However, since this links against the system-installed librdkafka
  (not your local build), it does NOT measure local source changes.
  Use --client rdkafka_perf_cpp to benchmark local changes.

Build:  cd <repo_root> && make -C examples rdkafka_benchmark_cpp_sys
Select: --client confluent_cpp
"""

from __future__ import annotations

from pathlib import Path

from clients.rdkafka_perf import (
    RdkafkaPerfConsumer,
    RdkafkaPerfProducer,
    _REPO_ROOT,
)

_BINARY_CPP_SYS = _REPO_ROOT / "examples" / "rdkafka_benchmark_cpp_sys"


class ConfluentCppProducer(RdkafkaPerfProducer):
    """C++ producer using the system-installed librdkafka."""

    def _binary_path(self) -> Path:
        return _BINARY_CPP_SYS

    def client_name(self) -> str:
        return "rdkafka_benchmark_cpp (system librdkafka)"


class ConfluentCppConsumer(RdkafkaPerfConsumer):
    """C++ consumer using the system-installed librdkafka."""

    def _binary_path(self) -> Path:
        return _BINARY_CPP_SYS

    def client_name(self) -> str:
        return "rdkafka_benchmark_cpp (system librdkafka)"
