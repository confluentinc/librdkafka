# librdkafka Benchmark Tool — Overview

## What It Is

A Python-orchestrated E2E performance benchmarking tool for Kafka producers and consumers,
built as part of the librdkafka repository. It runs a producer and a consumer concurrently
against a real Kafka broker, repeats the run N times, aggregates results across runs, and
produces structured JSON + CSV output alongside a live terminal summary.

---

## Problem It Solves

Before this tool, performance testing in librdkafka required:

- Manually invoking `examples/rdkafka_performance` once per configuration
- Manually running producer and consumer separately and correlating results
- No multi-run aggregation or statistical comparison (mean, stddev, percentiles)
- No structured output — only human-readable stdout
- No way to compare two librdkafka builds side-by-side with the same methodology

---

## Architecture

```
benchmark.py (CLI)
    │
    ├── core/config.py        Loads YAML preset, validates, merges CLI overrides
    ├── core/topic_manager.py Creates/deletes Kafka topic via Admin API
    ├── core/runner.py        Orchestrates N runs; manages process lifecycle
    │       │
    │       ├── multiprocessing.Process → BenchmarkProducer.run()
    │       │       └── puts ProducerResult on result_queue
    │       │
    │       └── multiprocessing.Process → BenchmarkConsumer.run()
    │               └── signals consumer_ready_event on group join
    │               └── puts ConsumerResult on result_queue
    │
    ├── core/metrics.py       ProducerResult, ConsumerResult, aggregation
    └── output/               Live table, summary, JSON writer, CSV writer
```

**Key design choice:** Producer does not start until the consumer signals
`consumer_ready_event` (i.e., Kafka group assignment is confirmed). This eliminates
the race condition where messages are produced before the consumer is subscribed.

---

## Two Client Implementations

### `--client confluent` (default)

Uses [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python),
which bundles a pre-built librdkafka inside the pip package.

| Attribute | Detail |
|-----------|--------|
| librdkafka source | Bundled in pip package (pinned version) |
| Tests local C changes | No |
| EOS / transactional | Yes |
| Consumer lag metric | Yes |
| Build step required | No |

**Use for:** General Kafka performance testing, configuration sweeps, regression
testing against a known librdkafka release.

---

### `--client rdkafka_perf`

Wraps `examples/rdkafka_benchmark` — a C binary compiled directly from the
local source tree via the standard `./configure && make` build.

| Attribute | Detail |
|-----------|--------|
| librdkafka source | Local build (whatever is in `src/`) |
| Tests local C changes | **Yes** |
| EOS / transactional | No |
| Consumer lag metric | No |
| Build step required | `make` before each benchmark |

The C binary outputs a JSON result object to stdout (not human-readable text),
which the Python wrapper parses into the same `ProducerResult`/`ConsumerResult`
dataclasses used by the confluent client. All progress/diagnostic output from
the binary goes to stderr so it does not interfere with JSON parsing.

**Use for:** Measuring the performance impact of a local code change or a
specific git commit on librdkafka's C internals.

---

## Key Capabilities

### Preset-based configuration

Six YAML presets ship with the tool, each targeting a specific scenario:

| Preset | Focus |
|--------|-------|
| `high_throughput` | Maximum msgs/s — lz4, acks=1, large batches |
| `low_latency` | Minimum E2E latency — no batching, linger=0 |
| `balanced` | Durability + throughput — acks=all, snappy, idempotent |
| `eos` | Exactly-once semantics overhead measurement |
| `sustained_throughput` | Time-driven 60s run — measures peak sustained rate |
| `custom` | Fully documented user-editable template |

All preset values are overridable from the CLI (`--msg-count`, `--msg-size`,
`--duration`, `--runs`).

### Multi-run aggregation

Running with `--runs N` repeats the full producer+consumer cycle N times and
aggregates results across successful runs:

- mean, stddev, min, max, p50 for each metric
- Failed runs counted separately; excluded from numeric aggregation
- Exit code 1 if all runs fail

### Two stopping modes

| Mode | How to activate | How it stops |
|------|----------------|--------------|
| **Count-driven** (default) | `msg_count` in preset or `--msg-count N` | When N messages are produced and consumed |
| **Time-driven** | `duration_sec` in preset or `--duration N` | After N seconds; consumer drains residual messages |
| **Combined** | Both set | Whichever limit is hit first |

### Structured output

Every benchmark run writes two files to `results/`:

- `benchmark_<preset>_<timestamp>.json` — full metadata, config snapshot,
  per-run results, and aggregated statistics
- `benchmark_<preset>_<timestamp>.csv` — one row per run, flat columns,
  suitable for spreadsheet analysis or CI comparison scripts

---

## Testing Benefits

### 1. Apples-to-apples comparison across librdkafka builds

```bash
git checkout commit-A && make
python tools/benchmark/benchmark.py --preset balanced \
    --client rdkafka_perf --output-dir results/A

git checkout commit-B && make
python tools/benchmark/benchmark.py --preset balanced \
    --client rdkafka_perf --output-dir results/B
```

Both runs use the same preset, same broker, same number of runs, same message
size — only the librdkafka binary differs.

### 2. Statistical confidence via multi-run

A single run can be noisy. Running `--runs 5` gives mean ± stddev across 5
independent runs, making regressions visible even when per-run variance is high.

### 3. Configuration sensitivity analysis

The preset system lets you quickly sweep configurations by running the same
workload under different settings and comparing the JSON output:

```bash
for preset in high_throughput balanced low_latency; do
    python benchmark.py --preset $preset --broker localhost:9092 \
        --output-dir results/$preset
done
```

### 4. E2E latency measurement

Both clients embed a nanosecond/microsecond wall-clock timestamp in every
message payload. The consumer extracts it on receipt, computing true end-to-end
latency (producer → broker → consumer) with p50/p95/p99 percentiles.

### 5. Pluggable for future clients

The abstract `BenchmarkProducer` / `BenchmarkConsumer` base classes in
`clients/base.py` define a contract that any implementation must satisfy.
Adding a Java, Go, or .NET client requires only:
1. A new `clients/<name>.py` implementing the two abstract classes
2. A one-line registration in `clients/__init__.py`

---

## Quick Reference

```bash
# Setup (one-time)
cd tools/benchmark && pip install -r requirements.txt

# List presets
python benchmark.py --list-presets

# Standard benchmark (confluent client, 3 runs)
python benchmark.py --preset balanced --broker localhost:9092

# Benchmark local librdkafka changes (build first)
cd ../.. && make && cd tools/benchmark
python benchmark.py --preset balanced --broker localhost:9092 --client rdkafka_perf

# Time-driven run (60 seconds)
python benchmark.py --preset sustained_throughput --broker localhost:9092

# Add time limit to any preset
python benchmark.py --preset high_throughput --broker localhost:9092 --duration 30

# Custom scenario
# Edit presets/custom.yaml, then:
python benchmark.py --preset custom --broker localhost:9092
```

---

## Files Added to the Repository

```
examples/rdkafka_benchmark.c          C benchmark binary (JSON output, local librdkafka)
examples/Makefile                     + rdkafka_benchmark build rule

tools/benchmark/
├── benchmark.py                      CLI entrypoint
├── requirements.txt                  Python dependencies
├── core/
│   ├── config.py                     Preset loading and validation
│   ├── runner.py                     Multi-run orchestration
│   ├── metrics.py                    Result dataclasses and aggregation
│   └── topic_manager.py              Kafka Admin API topic lifecycle
├── clients/
│   ├── base.py                       Abstract pluggable client interface
│   ├── confluent.py                  confluent-kafka-python implementation
│   └── rdkafka_perf.py               rdkafka_benchmark binary wrapper
├── output/
│   ├── live_table.py                 Per-run terminal table
│   ├── summary.py                    Aggregated summary table
│   ├── json_writer.py                JSON result writer
│   └── csv_writer.py                 CSV result writer
└── presets/
    ├── high_throughput.yaml
    ├── low_latency.yaml
    ├── balanced.yaml
    ├── eos.yaml
    ├── sustained_throughput.yaml
    └── custom.yaml                   Fully documented user template
```
