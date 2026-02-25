"""
C++ client — local librdkafka build (rdkafka_perf_cpp).

Wraps examples/rdkafka_benchmark_cpp compiled against the local
src-cpp/librdkafka++.a + src/librdkafka.a.

Use this client to benchmark the impact of local changes to librdkafka
via the C++ API surface (RdKafka::Producer / RdKafka::KafkaConsumer).
Results accurately reflect the local librdkafka build with minimal
overhead from the C++ wrapper layer.

Build:  cd <repo_root> && make -C examples rdkafka_benchmark_cpp
Select: --client rdkafka_perf_cpp
"""

from __future__ import annotations

from pathlib import Path

from clients.rdkafka_perf import (
    RdkafkaPerfConsumer,
    RdkafkaPerfProducer,
    _REPO_ROOT,
)

_BINARY_CPP = _REPO_ROOT / "examples" / "rdkafka_benchmark_cpp"


class RdkafkaPerfCppProducer(RdkafkaPerfProducer):
    """C++ producer using the local librdkafka build."""

    def _binary_path(self) -> Path:
        return _BINARY_CPP

    def client_name(self) -> str:
        return f"rdkafka_benchmark_cpp @ {_BINARY_CPP.parent} (local build)"


class RdkafkaPerfCppConsumer(RdkafkaPerfConsumer):
    """C++ consumer using the local librdkafka build."""

    def _binary_path(self) -> Path:
        return _BINARY_CPP

    def client_name(self) -> str:
        return f"rdkafka_benchmark_cpp @ {_BINARY_CPP.parent} (local build)"
