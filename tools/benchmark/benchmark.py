#!/usr/bin/env python3
"""
Kafka Performance Benchmarking Tool
====================================
E2E producer + consumer benchmark with configurable presets and multi-run aggregation.

Usage:
    python benchmark.py --preset balanced --broker localhost:9092
    python benchmark.py --preset balanced --auto-broker
    python benchmark.py --preset custom   --broker localhost:9092
    python benchmark.py --list-presets
    python benchmark.py --preset high_throughput --broker localhost:9092 \\
        --runs 5 --msg-count 500000 --msg-size 2048

See tools/benchmark/presets/ for available presets.
See tools/benchmark/presets/custom.yaml for the fully documented template.
"""

from __future__ import annotations

import atexit
import argparse
import os
import sys
from pathlib import Path

# Make the package importable when run as: python benchmark.py
sys.path.insert(0, str(Path(__file__).parent))

from clients import CLIENTS, DEFAULT_CLIENT, get_client
from core import config as cfg_mod
from core.config import ConfigError, list_presets
from core.runner import Runner
from output import csv_writer, html_reporter, json_writer, summary as summary_mod


def _parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="benchmark.py",
        description="Kafka E2E performance benchmarking tool (librdkafka / confluent-kafka)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python benchmark.py --list-presets

  # With a running broker:
  python benchmark.py --preset balanced --broker localhost:9092
  python benchmark.py --preset high_throughput --broker localhost:9092 --runs 5
  python benchmark.py --preset balanced --broker localhost:9092 \\
      --runs 3 --msg-count 200000 --msg-size 2048

  # Auto-start a Docker broker (no manual broker setup needed):
  python benchmark.py --preset balanced --auto-broker
  python benchmark.py --preset sustained_throughput --auto-broker
  python benchmark.py --preset balanced --auto-broker --keep-broker

  # Time-driven run:
  python benchmark.py --preset balanced --broker localhost:9092 --duration 60
        """,
    )

    parser.add_argument(
        "--preset",
        metavar="NAME_OR_PATH",
        help=(
            "Preset name (e.g. 'balanced') or path to a .yaml file. "
            "Use --list-presets to see available presets."
        ),
    )

    # Broker source: manual address OR auto Docker (mutually exclusive)
    broker_group = parser.add_mutually_exclusive_group()
    broker_group.add_argument(
        "--broker",
        metavar="HOST:PORT",
        default=os.environ.get("KAFKA_BROKER"),
        help=(
            "Kafka bootstrap server(s). "
            "Default: $KAFKA_BROKER env var. "
            "Mutually exclusive with --auto-broker."
        ),
    )
    broker_group.add_argument(
        "--auto-broker",
        action="store_true",
        dest="auto_broker",
        help=(
            "Auto-start a single-node Kafka broker via Docker (requires Docker). "
            "Uses localhost:9092. Broker is stopped after the benchmark unless "
            "--keep-broker is also passed."
        ),
    )

    parser.add_argument(
        "--keep-broker",
        action="store_true",
        dest="keep_broker",
        help=(
            "With --auto-broker: leave the Docker broker running after the "
            "benchmark finishes. Useful for inspecting topics or re-running quickly."
        ),
    )
    parser.add_argument(
        "--runs",
        type=int,
        metavar="N",
        help="Number of benchmark runs (overrides preset value, default: 3).",
    )
    parser.add_argument(
        "--msg-count",
        type=int,
        metavar="N",
        dest="msg_count",
        help="Messages per run (overrides preset workload.msg_count).",
    )
    parser.add_argument(
        "--msg-size",
        type=int,
        metavar="BYTES",
        dest="msg_size",
        help="Message size in bytes (overrides preset workload.msg_size).",
    )
    parser.add_argument(
        "--duration",
        type=int,
        metavar="SECONDS",
        dest="duration_sec",
        help=(
            "Run each benchmark for this many seconds instead of a fixed message count "
            "(overrides preset workload.duration_sec). "
            "When set, msg_count becomes a safety cap."
        ),
    )
    parser.add_argument(
        "--client",
        choices=CLIENTS,
        default=DEFAULT_CLIENT,
        help=f"Client implementation to use (default: {DEFAULT_CLIENT}).",
    )
    parser.add_argument(
        "--output-dir",
        metavar="DIR",
        default=str(Path(__file__).parent / "results"),
        dest="output_dir",
        help="Directory for JSON and CSV output files (default: ./results/).",
    )
    parser.add_argument(
        "--list-presets",
        action="store_true",
        dest="list_presets",
        help="List available presets and exit.",
    )

    return parser.parse_args(argv)


def _print_presets() -> None:
    presets = list_presets()
    if not presets:
        print("No presets found in presets/ directory.")
        return

    print(f"\nAvailable presets ({len(presets)}):\n")
    max_name = max(len(n) for n in presets)
    for name, desc in presets.items():
        print(f"  {name:<{max_name}}  {desc}")
    print(
        "\nRun with:  python benchmark.py --preset <name> --broker <host:port>\n"
        "           python benchmark.py --preset <name> --auto-broker\n"
        "Custom:    edit presets/custom.yaml, then --preset custom\n"
    )


def main(argv=None) -> int:
    args = _parse_args(argv)

    if args.list_presets:
        _print_presets()
        return 0

    if not args.preset:
        print("Error: --preset is required. Use --list-presets to see options.")
        return 1

    # ------------------------------------------------------------------
    # Resolve broker address
    docker_mgr = None

    if args.auto_broker:
        from core.docker_broker import DockerBroker, DockerBrokerError
        docker_mgr = DockerBroker()
        try:
            broker = docker_mgr.start()
        except DockerBrokerError as e:
            print(f"\nError: Failed to start Docker broker:\n  {e}")
            return 1

        if not args.keep_broker:
            atexit.register(docker_mgr.stop)
        else:
            print("  --keep-broker: broker will remain running after benchmark.")
    else:
        # Fall back to localhost:9092 if neither --broker nor $KAFKA_BROKER is set
        broker = args.broker or "localhost:9092"

    # ------------------------------------------------------------------
    # Load and validate config
    try:
        config = cfg_mod.load(
            preset=args.preset,
            broker=broker,
            runs=args.runs,
            msg_count=args.msg_count,
            msg_size=args.msg_size,
            duration_sec=args.duration_sec,
        )
    except ConfigError as e:
        print(f"Configuration error: {e}")
        return 1

    num_runs = config["runs"]
    preset_name = config["meta"]["name"]

    try:
        producer_cls, consumer_cls = get_client(args.client)
    except (ValueError, ImportError) as e:
        print(f"Client error: {e}")
        print("Install dependencies with: pip install -r requirements.txt")
        return 1

    duration_sec = config["workload"].get("duration_sec")
    import sys as _sys
    msg_count_display = (
        "unlimited (time-driven)"
        if config["workload"]["msg_count"] == _sys.maxsize
        else f"{config['workload']['msg_count']:,}"
    )

    broker_label = f"{broker} (Docker)" if args.auto_broker else broker

    print(f"\nKafka Benchmark")
    print(f"  preset   : {preset_name}  ({config['meta']['description']})")
    print(f"  broker   : {broker_label}")
    print(f"  client   : {producer_cls(broker, '', {}).client_name()}")
    print(f"  runs     : {num_runs}")
    print(f"  mode     : {'time-driven (' + str(duration_sec) + 's per run)' if duration_sec else 'count-driven'}")
    print(f"  msg_count: {msg_count_display}")
    print(f"  msg_size : {config['workload']['msg_size']:,} bytes")
    print(f"  compress : {config['workload']['compression']}")
    print(f"  acks     : {config['producer']['acks']}")
    print(f"  EOS      : {'yes' if config['producer'].get('transactional_id') else 'no'}")
    print()

    runner = Runner(
        config=config,
        broker=broker,
        preset_name=preset_name,
        num_runs=num_runs,
        producer_cls=producer_cls,
        consumer_cls=consumer_cls,
    )

    summary = runner.run()

    # Print summary
    summary_mod.print_summary(summary)

    # Write output files
    output_dir = Path(args.output_dir)
    json_path = json_writer.write(summary, config, output_dir)
    csv_path  = csv_writer.write(summary, output_dir)
    html_path = html_reporter.write(summary, config, output_dir)

    print(f"Results written to:")
    print(f"  {json_path}")
    print(f"  {csv_path}")
    print(f"  {html_path}")
    print()

    return 0 if summary.runs_failed < summary.runs_requested else 1


if __name__ == "__main__":
    sys.exit(main())
