# Verifiable Clients for Apache Kafka System Tests

Two C binaries that implement Apache Kafka's "verifiable client" contract
so the Ducktape-based Python system tests in `apache/kafka` can exercise
librdkafka the same way they exercise the Java client:

- `verifiable_producer/` — produces messages with optional throttling
- `verifiable_share_consumer/` — KIP-932 share consumer

Each binary accepts a standardized CLI and emits newline-delimited JSON
events on stdout. The Python harness in
`apache/kafka/tests/kafkatest/services/verifiable_*.py` parses those events.

## Build

Only required if you're modifying the verifiable clients themselves
. For just running the
tests, skip this.

Assumes librdkafka itself has already been built at the repo root. To
build the verifiable-client binaries, from the repo root:

```
make -C kafkatest
```

Binaries land at:

- `kafkatest/verifiable_producer/verifiable_producer`
- `kafkatest/verifiable_share_consumer/verifiable_share_consumer`

They statically link `../src/librdkafka.a`, so each is a single
self-contained file that can be copied to a test node.

## How System Tests Work

Apache Kafka's system tests run on [Ducktape](https://github.com/confluentinc/ducktape).
A test driver container orchestrates worker containers over SSH:
spinning up brokers, starting producer/consumer binaries, and
collecting their output. Each client binary emits newline-delimited
JSON events on stdout (`startup_complete`, `partitions_assigned`,
`records_consumed`, `offsets_acknowledged`, `shutdown_complete`, …)
which the driver parses to track progress and verify correctness.

To plug in a custom client, point Ducktape at a `--globals` file.
The Python harness resolves `VerifiableProducer` /
`VerifiableShareConsumer` to `VerifiableClientApp`, which on each
worker:

1. Runs the `deploy` script once before the first test.
2. Invokes `exec_cmd` (with the harness-supplied flags appended) and
   parses its stdout as JSON events.

Our `globals.json` wires both bindings to the binaries built by
`kafkatest/Makefile`, statically linked against `../src/librdkafka.a`.
Paths assume the librdkafka repo is mounted at `/librdkafka` inside
each worker.

`deploy.sh` apt-installs build deps and builds librdkafka + the
verifiable binaries (`make libs && make -C kafkatest`). It honors
`LIBRDKAFKA_DIR` (default `/librdkafka`) and
`DEPLOY_SKIP_APT=1` (for pre-provisioned images), and uses a flock'd
sentinel so concurrent deploy invocations on the shared mount don't
race.

## Running Share Consumer Tests

Step-by-step. Assumes Docker is running with ~10 GB of free RAM
available for containers.

### Prerequisites

- A checkout of `confluentinc/librdkafka`
- A checkout of `apache/kafka`

```bash
export LIBRDKAFKA_DIR=/path/to/your/librdkafka
export KAFKA_DIR=/path/to/your/apache/kafka
```

### 1. Compile apache/kafka's system-test libraries

```bash
cd $KAFKA_DIR
./gradlew systemTestLibs
```

First run takes ~10 minutes; subsequent runs are fast.

### 2. Patch ducker-ak to mount librdkafka

Open `$KAFKA_DIR/tests/docker/ducker-ak`, find the `docker_run()`
function (near line 340), and add a `-v` flag for librdkafka:

```diff
     must_do -v ${container_runtime} run --init --privileged \
         -d -t -h "${node}" --network ducknet "${expose_ports}" \
         --memory=${docker_run_memory_limit} ... \
-        -v "${kafka_dir}:/opt/kafka-dev" --name "${node}" -- "${image_name}"
+        -v "${kafka_dir}:/opt/kafka-dev" \
+        ${LIBRDKAFKA_DIR:+-v "${LIBRDKAFKA_DIR}:/librdkafka"} \
+        --name "${node}" -- "${image_name}"
```

This is a one-time local edit.

### 3. Make `deploy.sh` executable
```bash
chmod +x $LIBRDKAFKA_DIR/kafkatest/deploy.sh
```

### 4. Bring up the cluster and run the tests

```bash
cd $KAFKA_DIR
LIBRDKAFKA_DIR=$LIBRDKAFKA_DIR ./tests/docker/ducker-ak up

LIBRDKAFKA_DIR=$LIBRDKAFKA_DIR ./tests/docker/ducker-ak test \
  tests/kafkatest/tests/client/share_consumer_test.py \
  -- --globals /librdkafka/kafkatest/globals.json
```

The harness runs `deploy.sh` once per worker before the first test;
that script handles building librdkafka and the verifiable binaries.
The first test starts ~3-5 minutes after launch (deploy time);
subsequent tests reuse the build.

To run a single variant, bypass `ducker-ak test`'s shell wrapper
(which would strip the embedded quotes in the test symbol) and call
ducktape directly:

```bash
docker exec -w /opt/kafka-dev ducker01 ducktape \
  --cluster-file /opt/kafka-dev/tests/docker/build/cluster.json \
  --globals /librdkafka/kafkatest/globals.json \
  'tests/kafkatest/tests/client/share_consumer_test.py::ShareConsumerTest.test_share_single_topic_partition@{"metadata_quorum":"ISOLATED_KRAFT","use_share_groups":true}'
```

### 5. View results

```bash
cat $KAFKA_DIR/results/latest/report.txt
```

Per-test stdout (debugging):

```bash
find $KAFKA_DIR/results/latest -name "verifiable_*.stdout"
```

### 6. Tear down

```bash
cd $KAFKA_DIR
./tests/docker/ducker-ak down
```
