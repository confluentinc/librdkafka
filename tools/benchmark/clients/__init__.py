"""
Client registry. Maps client name → (ProducerClass, ConsumerClass).

Imports are lazy so that --list-presets and config validation work
without confluent-kafka being installed.

Available clients:
  confluent      - confluent-kafka-python (bundles its own librdkafka)
  rdkafka_perf   - wraps examples/rdkafka_benchmark built from local source
                   (use this to benchmark local librdkafka changes)

To add a new client implementation:
  1. Create clients/<name>.py with classes subclassing BenchmarkProducer/Consumer.
  2. Add an entry to _REGISTRY below.
  3. Select at runtime with: --client <name>
"""

from __future__ import annotations

from typing import Dict, Tuple, Type

from clients.base import BenchmarkConsumer, BenchmarkProducer

# Registry: name → dotted module paths for (ProducerClass, ConsumerClass)
_REGISTRY: Dict[str, Tuple[str, str, str, str]] = {
    # "name": ("module", "ProducerClass", "module", "ConsumerClass")
    "confluent": (
        "clients.confluent", "ConfluentProducer",
        "clients.confluent", "ConfluentConsumer",
    ),
    "rdkafka_perf": (
        "clients.rdkafka_perf", "RdkafkaPerfProducer",
        "clients.rdkafka_perf", "RdkafkaPerfConsumer",
    ),
}

DEFAULT_CLIENT = "confluent"


def get_client(
    name: str,
) -> Tuple[Type[BenchmarkProducer], Type[BenchmarkConsumer]]:
    """Return (ProducerClass, ConsumerClass) for the named client."""
    if name not in _REGISTRY:
        raise ValueError(
            f"Unknown client '{name}'. Available: {list(_REGISTRY.keys())}"
        )
    prod_mod, prod_cls, cons_mod, cons_cls = _REGISTRY[name]

    import importlib
    pm = importlib.import_module(prod_mod)
    cm = importlib.import_module(cons_mod)
    return getattr(pm, prod_cls), getattr(cm, cons_cls)


# Expose a list of available names (without importing implementations)
CLIENTS = list(_REGISTRY.keys())
