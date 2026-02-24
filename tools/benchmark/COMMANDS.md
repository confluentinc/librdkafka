# Benchmark Tool — Command Reference

All commands are run from `tools/benchmark/` unless otherwise noted.

---

## Setup

```bash
cd tools/benchmark
pip install -r requirements.txt
```

---

## List available presets

```bash
python benchmark.py --list-presets
```

---

## Broker options

Every benchmark command requires a broker. Choose one:

```bash
# Option 1: Auto-start a Docker broker (starts, runs, stops automatically)
python benchmark.py --preset balanced --auto-broker

# Option 2: Auto-start, keep broker running after benchmark (faster re-runs)
python benchmark.py --preset balanced --auto-broker --keep-broker

# Option 3: Use an already-running broker
python benchmark.py --preset balanced --broker localhost:9092

# Option 4: Use $KAFKA_BROKER environment variable
export KAFKA_BROKER="localhost:9092"
python benchmark.py --preset balanced

# Stop a kept Docker broker manually
docker compose -f docker-compose.yml down
```

---

## Running a single preset

```bash
# Default settings (3 runs, preset msg_count, confluent client)
python benchmark.py --preset balanced --auto-broker

# Specific number of runs
python benchmark.py --preset balanced --auto-broker --runs 5

# Override message count
python benchmark.py --preset balanced --auto-broker --msg-count 500000

# Override message size
python benchmark.py --preset balanced --auto-broker --msg-size 4096

# Override compression codec
python benchmark.py --preset balanced --auto-broker \
    --msg-count 100000 --msg-size 512

# Custom output directory
python benchmark.py --preset balanced --auto-broker \
    --output-dir ./results/my_test
```

---

## Time-driven mode (run for N seconds)

```bash
# Run for exactly 60 seconds regardless of message count
python benchmark.py --preset high_throughput --auto-broker --duration 60

# Use the built-in sustained_throughput preset (already 60s)
python benchmark.py --preset sustained_throughput --auto-broker

# Time-driven with explicit cap (stop at 5M msgs OR 60s, whichever first)
python benchmark.py --preset high_throughput --auto-broker \
    --duration 60 --msg-count 5000000
```

---

## Client selection

### confluent (default)
Uses `confluent-kafka-python` with a bundled librdkafka. No build step required.

```bash
python benchmark.py --preset balanced --auto-broker --client confluent
```

### rdkafka_perf (local librdkafka build)
Wraps `examples/rdkafka_benchmark` compiled from the local source tree.
Accurately measures librdkafka's own CPU and memory. Build required.

```bash
# Build once (or after any source change)
cd /path/to/librdkafka && make -C examples rdkafka_benchmark

cd tools/benchmark

# Run against local build
python benchmark.py --preset balanced --auto-broker --client rdkafka_perf

# Benchmark current working-tree changes
make -C ../../examples rdkafka_benchmark   # incremental rebuild
python benchmark.py --preset high_throughput --auto-broker --client rdkafka_perf
```

---

## Benchmarking specific git commits

```bash
cd /path/to/librdkafka

# Commit A
git checkout <commit-A-sha>
./configure && make
python tools/benchmark/benchmark.py --preset balanced \
    --broker localhost:9092 --client rdkafka_perf \
    --output-dir tools/benchmark/results/commit-A

# Commit B
git checkout <commit-B-sha>
./configure && make
python tools/benchmark/benchmark.py --preset balanced \
    --broker localhost:9092 --client rdkafka_perf \
    --output-dir tools/benchmark/results/commit-B

# Compare the two JSON files
diff tools/benchmark/results/commit-A/*.json \
     tools/benchmark/results/commit-B/*.json
```

---

## Running all presets (wrapper script)

```bash
# All presets, confluent client, all defaults
./run_all_presets.sh

# All presets, local librdkafka build (rebuilds binary first; skips eos)
./run_all_presets.sh --client rdkafka_perf

# All presets, time-driven, 1 run each
./run_all_presets.sh --duration 60 --runs 1

# All presets, local build, 60s each, 1 run each
./run_all_presets.sh --client rdkafka_perf --duration 60 --runs 1

# Use a running broker instead of Docker
./run_all_presets.sh --broker localhost:9092

# Custom output directory
./run_all_presets.sh --output-dir /tmp/my_results

# All options can be combined
./run_all_presets.sh --client rdkafka_perf --duration 60 --runs 1 \
    --output-dir ./results/release_v2
```

`run_all_presets.sh` automatically generates `combined_report.html` at the end.

---

## Generating a combined report manually

After any session where multiple presets were run to the same directory:

```bash
# Generate combined_report.html from all JSON files in a directory
python combine_results.py ./results/run_all_20260224_120000

# Generate and open in browser
python combine_results.py ./results/run_all_20260224_120000 --open
```

---

## Custom preset

```bash
# 1. Edit presets/custom.yaml with your settings (every field is documented)
#    Key fields: msg_count, msg_size, duration_sec, compression, acks,
#                linger_ms, batch_size, enable_idempotence, transactional_id

# 2. Run
python benchmark.py --preset custom --auto-broker

# Or point --preset at any .yaml file anywhere on disk
python benchmark.py --preset /path/to/my_scenario.yaml --auto-broker
```

---

## Remote or authenticated broker

Add librdkafka properties to `producer.extra_config` / `consumer.extra_config`
in your preset YAML:

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

Then run:
```bash
python benchmark.py --preset my_secure_preset --broker broker.example.com:9093
```

---

## Output files

Every benchmark run writes three files to the output directory:

| File | Contents |
|------|----------|
| `benchmark_<preset>_<timestamp>.json` | Full metadata, config, per-run results, aggregated stats |
| `benchmark_<preset>_<timestamp>.csv`  | One row per run, flat columns (spreadsheet-friendly) |
| `benchmark_<preset>_<timestamp>.html` | Visual report: charts + tables (open in any browser) |

When using `run_all_presets.sh`, a fourth file is added:

| File | Contents |
|------|----------|
| `combined_report.html` | Cross-preset comparison: charts + overview table + per-preset detail |

---

## All CLI flags

```
python benchmark.py [OPTIONS]

Broker (choose one):
  --broker HOST:PORT    Use an existing broker (default: $KAFKA_BROKER or localhost:9092)
  --auto-broker         Auto-start a Docker broker (apache/kafka KRaft, localhost:9092)

Options:
  --preset NAME|PATH    Preset name or path to a .yaml file (required)
  --keep-broker         With --auto-broker: leave the broker running after the benchmark
  --runs N              Number of benchmark runs (default: 3)
  --msg-count N         Messages per run (overrides preset; disabled when --duration is set)
  --msg-size BYTES      Message size in bytes (overrides preset)
  --duration SECONDS    Run for N seconds (switches to time-driven mode; disables preset msg_count)
  --client NAME         confluent (default) | rdkafka_perf
  --output-dir DIR      Output directory for result files (default: ./results/)
  --list-presets        List available presets and exit
```

---

## Preset quick reference

| Preset | Mode | Focus | msg_count | duration | compression | acks | EOS |
|--------|------|-------|-----------|----------|-------------|------|-----|
| `high_throughput` | count | Max msgs/s | 5,000,000 | — | lz4 | 1 | no |
| `low_latency` | count | Min E2E latency | 100,000 | — | none | 1 | no |
| `balanced` | count | Durability + throughput | 1,000,000 | — | snappy | all | idempotent |
| `eos` | count | EOS overhead | 500,000 | — | none | all | transactional |
| `sustained_throughput` | **time** | Sustained rate | no cap | **60s** | lz4 | 1 | no |
| `custom` | either | User-defined | configurable | configurable | configurable | configurable | configurable |

> `eos` preset is only supported with `--client confluent`. Skip it when using `--client rdkafka_perf`.
