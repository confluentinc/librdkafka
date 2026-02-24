# Kafka Performance Benchmarking Tool

E2E Kafka performance benchmarking tool built on [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) (which wraps librdkafka natively).

Runs a producer and consumer concurrently, repeats for N runs, aggregates metrics (mean/stddev/p50/p95/p99), and writes structured JSON + CSV output.

Supports two stopping modes:
- **Count-driven** (default): run until N messages are produced and consumed
- **Time-driven**: run for a fixed number of seconds, report what was achieved

---

## Requirements

- Python 3.8+
- Kafka broker: either provide your own (`--broker`) or let the tool start one automatically via Docker (`--auto-broker`)
- Dependencies: `pip install -r requirements.txt`

---

## Quick Start

```bash
cd tools/benchmark
pip install -r requirements.txt

# List available presets
python benchmark.py --list-presets

# Zero-config: auto-start a Docker broker (requires Docker)
python benchmark.py --preset balanced --auto-broker

# With a running broker:
python benchmark.py --preset balanced --broker localhost:9092

# Time-driven: run for 60 seconds, report what was achieved
python benchmark.py --preset sustained_throughput --auto-broker

# Run your custom configuration
# 1. Edit presets/custom.yaml with your settings
# 2. Run:
python benchmark.py --preset custom --auto-broker
```

---

## Available Presets

| Preset | Mode | Focus | msg_count | duration | msg_size | compression | acks | EOS |
|--------|------|-------|-----------|----------|----------|-------------|------|-----|
| `high_throughput` | count | Max msgs/s | 5,000,000 | — | 512 B | lz4 | 1 | no |
| `low_latency` | count | Min E2E latency | 100,000 | — | 128 B | none | 1 | no |
| `balanced` | count | Durability + throughput | 1,000,000 | — | 1 KB | snappy | all | idempotent |
| `eos` | count | EOS overhead | 500,000 | — | 512 B | none | all | transactional |
| `sustained_throughput` | **time** | Sustained throughput | no cap | **60s** | 1 KB | lz4 | 1 | no |
| `custom` | either | User-defined | configurable | configurable | configurable | configurable | configurable | configurable |

---

## Stopping Modes

### Count-driven (default)

The run ends when exactly `msg_count` messages have been produced and consumed. Use this when you want consistent, comparable runs with a known volume.

```bash
python benchmark.py --preset balanced --broker localhost:9092
python benchmark.py --preset balanced --broker localhost:9092 --msg-count 500000
```

### Time-driven

The producer runs for `duration_sec` seconds at full speed, then stops. The consumer drains any remaining buffered messages. Use this for sustained throughput testing where you want to measure the maximum rate achievable over a fixed window, regardless of message count.

```bash
# Use the dedicated preset (60 second runs)
python benchmark.py --preset sustained_throughput --broker localhost:9092

# Add a time limit to any existing preset at runtime
python benchmark.py --preset high_throughput --broker localhost:9092 --duration 30

# Combined: time limit with a safety count cap
python benchmark.py --preset balanced --broker localhost:9092 --duration 60 --msg-count 5000000
```

When `duration_sec` is set and `msg_count` is null (no cap), the tool reports the actual number of messages produced within the window. When both are set, whichever limit is hit first stops the producer.

---

## CLI Reference

```
python benchmark.py [OPTIONS]

Broker (choose one):
  --broker HOST:PORT      Use an existing Kafka broker (default: $KAFKA_BROKER or localhost:9092)
  --auto-broker           Auto-start a single-node Kafka broker via Docker.
                          Broker is stopped after the benchmark (unless --keep-broker).

Options:
  --preset NAME_OR_PATH   Preset name or path to a .yaml file (required)
  --keep-broker           With --auto-broker: leave the Docker broker running after the benchmark
  --runs N                Number of benchmark runs (overrides preset, default: 3)
  --msg-count N           Messages per run (overrides preset workload.msg_count)
  --msg-size BYTES        Message size in bytes (overrides preset workload.msg_size)
  --duration SECONDS      Run for this many seconds instead of a fixed count
                          (overrides preset workload.duration_sec).
                          msg_count becomes a safety cap when this is set.
  --client NAME           Client implementation: confluent, rdkafka_perf (default: confluent)
  --output-dir DIR        Output directory for results files (default: ./results/)
  --list-presets          List available presets and exit
```

---

## Custom Preset

Edit `presets/custom.yaml` — every field has an inline comment explaining its purpose, valid values, and performance tradeoff. Both `msg_count` and `duration_sec` are documented there. Then run:

```bash
python benchmark.py --preset custom --broker localhost:9092
```

You can also point `--preset` at any `.yaml` file:

```bash
python benchmark.py --preset /path/to/my_scenario.yaml --broker localhost:9092
```

---

## Output

After each run, a live table row is printed:

```
Run  P msgs/s    P MB/s  DR avg(µs)  DR p99(µs)  C msgs/s    C MB/s  E2E avg(ms)  E2E p99(ms)  C-lag  Status
  1  843,201    826.3       312.4      1,204.1     791,003    774.4        15.2         48.7        0    OK
  2  851,045    833.0       298.1      1,103.8     843,001    825.0        14.1         46.2        0    OK
```

After all runs, an aggregated summary is printed and three files are written to `results/`:

- `benchmark_<preset>_<timestamp>.json` — full metadata, config snapshot, per-run results, and aggregated stats
- `benchmark_<preset>_<timestamp>.csv` — one row per run, all metrics as flat columns (suitable for spreadsheets)
- `benchmark_<preset>_<timestamp>.html` — visual report with charts and tables (open in any browser)

### HTML Report

The HTML report includes:
- **Summary cards** — preset, broker, client, message size at a glance
- **Configuration panel** — key librdkafka settings used for the run
- **Bar charts** — throughput (msgs/s, MB/s) and latency (DR avg/p99, E2E avg/p99) per run, colour-coded green/red by pass/fail
- **Per-run table** — all metrics for every individual run
- **Aggregated tables** — mean, stddev, min, max, p50 across all successful runs

> Charts require an internet connection to load Chart.js from CDN (`cdn.jsdelivr.net`). Tables render fully offline.

### Metrics Explained

**Producer**
| Metric | Description |
|--------|-------------|
| `P msgs/s` | Messages delivered per second |
| `P MB/s` | Delivered throughput in MB/s |
| `DR avg/p99 (µs)` | Delivery report latency — time from `produce()` call to delivery report callback |

**Consumer**
| Metric | Description |
|--------|-------------|
| `C msgs/s` | Messages consumed per second |
| `C MB/s` | Consumed throughput in MB/s |
| `E2E avg/p99 (ms)` | End-to-end latency — from message production timestamp to consumer receipt |
| `C-lag` | Consumer lag (messages behind high watermark) at end of run |

> **E2E latency note**: Measured using a nanosecond timestamp embedded in the first 8 bytes of each message. Valid only when producer and consumer run on the same host or use NTP-synchronized clocks. Messages smaller than 8 bytes disable E2E measurement.

---

## Broker Setup

### Option 1: Auto-start via Docker (recommended for quick testing)

Pass `--auto-broker` and the tool handles everything — start, readiness wait, teardown:

```bash
# Fully automatic: starts broker, runs benchmark, stops broker
python benchmark.py --preset balanced --auto-broker

# Keep the broker running after the benchmark (faster re-runs)
python benchmark.py --preset balanced --auto-broker --keep-broker

# Stop a kept broker manually
docker compose -f tools/benchmark/docker-compose.yml down
```

Requirements: Docker must be installed and the Docker daemon must be running.
The broker uses `apache/kafka:latest` in KRaft mode (no ZooKeeper) on `localhost:9092`.

### Option 2: Manual Docker (if you want control over the container)

```bash
# Start
docker compose -f tools/benchmark/docker-compose.yml up -d

# Run benchmark
python benchmark.py --preset balanced --broker localhost:9092

# Stop
docker compose -f tools/benchmark/docker-compose.yml down
```

### Option 3: Remote broker

```bash
export KAFKA_BROKER="broker1.example.com:9092,broker2.example.com:9092"
python benchmark.py --preset high_throughput
```

For SSL or SASL authentication, add the relevant librdkafka properties to your preset's `producer.extra_config` and `consumer.extra_config` sections:

```yaml
producer:
  extra_config:
    security.protocol: SASL_SSL
    sasl.mechanism: PLAIN
    sasl.username: myuser
    sasl.password: mypassword
    ssl.ca.location: /path/to/ca.pem

consumer:
  extra_config:
    security.protocol: SASL_SSL
    sasl.mechanism: PLAIN
    sasl.username: myuser
    sasl.password: mypassword
    ssl.ca.location: /path/to/ca.pem
```

---

## Benchmarking Local librdkafka Changes (`--client rdkafka_perf`)

The `rdkafka_perf` client wraps `examples/rdkafka_benchmark` — a C binary built directly from the local source tree. Use this when you want to measure the performance impact of a local code change or a specific git commit, since the binary uses **whatever librdkafka you have built**, not a bundled pip package.

### Build the binary

```bash
cd /path/to/librdkafka
./configure && make
# Binary produced at: examples/rdkafka_benchmark
```

### Run against local build

```bash
cd tools/benchmark
python benchmark.py --preset balanced --broker localhost:9092 --client rdkafka_perf
```

### Benchmark a specific commit

```bash
cd /path/to/librdkafka

# Checkout and build commit A
git checkout <commit-A-sha>
./configure && make
python tools/benchmark/benchmark.py --preset balanced \
    --broker localhost:9092 --client rdkafka_perf \
    --output-dir tools/benchmark/results/commit-A

# Checkout and build commit B
git checkout <commit-B-sha>
./configure && make
python tools/benchmark/benchmark.py --preset balanced \
    --broker localhost:9092 --client rdkafka_perf \
    --output-dir tools/benchmark/results/commit-B

# Compare: results/commit-A/*.json vs results/commit-B/*.json
```

### Benchmark current working-tree changes

```bash
cd /path/to/librdkafka
# Edit src/...
make   # incremental rebuild
python tools/benchmark/benchmark.py --preset high_throughput \
    --broker localhost:9092 --client rdkafka_perf
```

### Differences from the `confluent` client

| Feature | `confluent` | `rdkafka_perf` |
|---------|-------------|-----------------|
| librdkafka source | bundled pip package | **local build** |
| Tests local changes | no | **yes** |
| Build step required | no | `make` before each run |
| EOS / transactional | yes | no |
| Consumer lag metric | yes | no |

---

## Adding a New Client Implementation

1. Create `clients/<name>.py` with classes subclassing `BenchmarkProducer` and `BenchmarkConsumer` from `clients/base.py`.
2. Register in `clients/__init__.py` under `_REGISTRY`.
3. Select at runtime: `--client <name>`

The abstract base classes in [clients/base.py](clients/base.py) document the full contract each implementation must satisfy, including the stopping mode behaviour that all implementations must support.

---

## Project Structure

```
tools/benchmark/
├── benchmark.py          # CLI entrypoint
├── requirements.txt
├── README.md
├── docker-compose.yml    # Single-node Kafka broker (used by --auto-broker)
├── core/
│   ├── config.py         # YAML loading, validation, CLI-override merge
│   ├── runner.py         # N-run orchestration, process lifecycle
│   ├── metrics.py        # Result dataclasses and aggregation
│   ├── topic_manager.py  # Admin API: create/delete/verify topics
│   └── docker_broker.py  # Docker broker lifecycle (start/wait/stop)
├── clients/
│   ├── base.py           # Abstract BenchmarkProducer + BenchmarkConsumer
│   ├── confluent.py      # confluent-kafka-python implementation
│   └── rdkafka_perf.py   # Wraps examples/rdkafka_benchmark (local build)
├── output/
│   ├── live_table.py     # Per-run terminal table
│   ├── summary.py        # Final aggregated stats table
│   ├── json_writer.py    # JSON file writer
│   ├── csv_writer.py     # CSV file writer
│   └── html_reporter.py  # HTML report with Chart.js charts
├── presets/
│   ├── high_throughput.yaml
│   ├── low_latency.yaml
│   ├── balanced.yaml
│   ├── eos.yaml
│   ├── sustained_throughput.yaml  # Time-driven preset (60s per run)
│   └── custom.yaml                # Fully documented template
└── results/              # Auto-created output directory (gitignored)
```
