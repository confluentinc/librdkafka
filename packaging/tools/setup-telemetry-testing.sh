#!/bin/bash
#
# Self-contained end-to-end verification for share-consumer KIP-714 telemetry.
#
# Pipeline exercised:
#   librdkafka share consumer (enable.metrics.push=true)
#     --PushTelemetryRequest--> Kafka broker (Apache Kafka 4.2)
#     --plugin gRPC--> OTel Collector (docker container)
#     --Kafka exporter--> topic client-telemetry-metrics (same broker)
#     --kafka-console-consumer--> verification step
#
# Verification: every share-consumer metric name declared in
# src/rdkafka_telemetry_encode.h must appear at least once in the topic
# during the run.
#
# Intended to be invoked from .semaphore/run-share-consumer-telemetry-verification.yml
# but is fully self-contained and runnable locally on Linux or macOS (requires
# Docker, JDK 17, wget).

set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
VERIFY_DIR=${ROOT}/tests/share_telemetry_verify
WORK_DIR=${VERIFY_DIR}/.work

KAFKA_VERSION=${KAFKA_VERSION:-4.2.0}
KAFKA_DIR=${WORK_DIR}/kafka_2.13-${KAFKA_VERSION}
KAFKA_DATA=${WORK_DIR}/kafka-data
TUTORIALS_REPO=${WORK_DIR}/tutorials
PLUGIN_JAR=${WORK_DIR}/client-telemetry-reporter-plugin.jar
DOCKER_COMPOSE=${TUTORIALS_REPO}/client-telemetry-reporter-plugin/kafka/docker-compose.yml

TOPIC=client-telemetry-metrics
SUBSCRIPTION_NAME=share-consumer-ci

mkdir -p ${WORK_DIR}

###############################################################################
# 1. Build the OTLP reporter plugin from confluentinc/tutorials (cached)
###############################################################################
if [ ! -f "${PLUGIN_JAR}" ]; then
    echo "[setup] cloning + building client-telemetry-reporter-plugin..."
    if [ ! -d "${TUTORIALS_REPO}" ]; then
        git clone --depth 1 https://github.com/confluentinc/tutorials.git ${TUTORIALS_REPO}
    fi
    # Patch shadow plugin for newer Gradle/JDK compatibility
    sed -i.bak \
        "s|id 'com.github.johnrengelman.shadow' version '8.1.1'|id 'com.gradleup.shadow' version '8.3.6'|" \
        ${TUTORIALS_REPO}/client-telemetry-reporter-plugin/kafka/build.gradle
    (cd ${TUTORIALS_REPO} && ./gradlew --no-daemon clean \
        :client-telemetry-reporter-plugin:kafka:build)
    cp ${TUTORIALS_REPO}/client-telemetry-reporter-plugin/kafka/build/libs/client-telemetry-reporter-plugin.jar \
        ${PLUGIN_JAR}
else
    echo "[setup] reusing cached plugin jar at ${PLUGIN_JAR}"
fi

###############################################################################
# 2. Download Kafka and configure dual-listener broker (cached)
###############################################################################
if [ ! -d "${KAFKA_DIR}" ]; then
    echo "[setup] downloading Kafka ${KAFKA_VERSION}..."
    wget -q https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz \
        -O ${WORK_DIR}/kafka.tgz
    tar -C ${WORK_DIR} -xzf ${WORK_DIR}/kafka.tgz

    # Patch server.properties:
    #   - dual listeners: PLAINTEXT on :9092 (host clients),
    #                     DOCKER   on :19092 (otel container)
    #   - log.dirs:        persistent path in the work dir
    #   - metric.reporters: enable the OTLP reporter plugin
    SP=${KAFKA_DIR}/config/server.properties
    sed -i.bak \
        -e 's|^listeners=PLAINTEXT://:9092,CONTROLLER://:9093|listeners=PLAINTEXT://:9092,DOCKER://:19092,CONTROLLER://:9093|' \
        -e 's|^advertised.listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093|advertised.listeners=PLAINTEXT://localhost:9092,DOCKER://host.docker.internal:19092,CONTROLLER://localhost:9093|' \
        -e 's|^listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL|listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,DOCKER:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL|' \
        -e "s|^log.dirs=/tmp/kraft-combined-logs|log.dirs=${KAFKA_DATA}|" \
        ${SP}
    {
        echo ''
        echo '# KIP-714 client telemetry plugin'
        echo 'metric.reporters=io.confluent.developer.ClientOtlpMetricsReporter'
    } >> ${SP}

    cp ${PLUGIN_JAR} ${KAFKA_DIR}/libs/

    mkdir -p ${KAFKA_DATA}
    CLUSTER_ID=$(${KAFKA_DIR}/bin/kafka-storage.sh random-uuid)
    ${KAFKA_DIR}/bin/kafka-storage.sh format --standalone \
        -t ${CLUSTER_ID} -c ${SP}
else
    echo "[setup] reusing cached Kafka at ${KAFKA_DIR}"
fi

###############################################################################
# 3. Override OTel Collector config with the Kafka-exporter-only version
###############################################################################
echo "[setup] installing OTel Collector config..."
cp ${VERIFY_DIR}/otel-collector-config.yaml \
   ${TUTORIALS_REPO}/client-telemetry-reporter-plugin/kafka/otel-collector-config.yaml

###############################################################################
# 4. Cleanup trap so failures don't leak processes / containers / test.conf
###############################################################################
TEST_CONF_BAK=${ROOT}/tests/test.conf.bak.$$
cleanup() {
    local rc=$?
    set +e
    echo "[teardown] cleaning up..."
    ${KAFKA_DIR}/bin/kafka-server-stop.sh 2>/dev/null
    docker compose -f ${DOCKER_COMPOSE} down 2>/dev/null
    if [ -f "${TEST_CONF_BAK}" ]; then
        mv ${TEST_CONF_BAK} ${ROOT}/tests/test.conf
    fi
    exit ${rc}
}
trap cleanup EXIT

###############################################################################
# 5. Start OTel Collector (we only need the otel-collector service)
###############################################################################
echo "[run] starting OTel Collector container..."
docker compose -f ${DOCKER_COMPOSE} up -d otel-collector

###############################################################################
# 6. Start broker (background daemon) and wait until it answers
###############################################################################
echo "[run] starting Kafka broker..."
OTEL_EXPORTER_OTLP_ENDPOINT=127.0.0.1:4317 \
    ${KAFKA_DIR}/bin/kafka-server-start.sh -daemon \
    ${KAFKA_DIR}/config/server.properties

for i in $(seq 1 60); do
    if ${KAFKA_DIR}/bin/kafka-broker-api-versions.sh \
        --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        echo "[run] broker is up after ${i}s"
        break
    fi
    sleep 1
done

###############################################################################
# 7. Create the telemetry sink topic + share-consumer metrics subscription
###############################################################################
${KAFKA_DIR}/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic ${TOPIC} --partitions 1 --replication-factor 1 --if-not-exists
${KAFKA_DIR}/bin/kafka-client-metrics.sh --bootstrap-server localhost:9092 --alter \
    --name ${SUBSCRIPTION_NAME} \
    --metrics "org.apache.kafka.consumer.share." \
    --interval 5000

###############################################################################
# 8. Build librdkafka + tests, append enable.metrics.push to test.conf
###############################################################################
echo "[build] configure + make librdkafka..."
(cd ${ROOT} && ./configure --install-deps --enable-werror --enable-devel)
make -j -C ${ROOT}
make -j -C ${ROOT}/tests build

if [ -f ${ROOT}/tests/test.conf ]; then
    cp ${ROOT}/tests/test.conf ${TEST_CONF_BAK}
fi
cat > ${ROOT}/tests/test.conf <<'EOF'
metadata.broker.list=localhost:9092
security.protocol=plaintext
enable.metrics.push=true
EOF

###############################################################################
# 9. Run a share-consumer test (0170 subscription) — creates a share consumer
#    with enable.metrics.push=true, which triggers telemetry pushes every 5s.
#    Test runner return code is tolerated; we care about the topic contents,
#    not test-runner cleanup timing.
###############################################################################
START_OFFSET=$(${KAFKA_DIR}/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic ${TOPIC} 2>/dev/null \
    | cut -d: -f3)
START_OFFSET=${START_OFFSET:-0}
echo "[run] starting share-consumer test 0170 from topic offset ${START_OFFSET}..."
(cd ${ROOT}/tests && TESTS=0170 ./run-test.sh) || \
    echo "[run] test runner exited non-zero; continuing to verification"

###############################################################################
# 10. Wait for the final push interval + verify topic coverage
###############################################################################
echo "[verify] waiting 15s for trailing telemetry pushes..."
sleep 15

EXPECTED=$(grep -oE 'consumer\.share\.[a-z0-9._]+' \
           ${ROOT}/src/rdkafka_telemetry_encode.h | sort -u)
SEEN=$(${KAFKA_DIR}/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic ${TOPIC} --partition 0 --offset ${START_OFFSET} \
    --max-messages 100 --timeout-ms 30000 2>/dev/null \
    | strings | grep -oE 'consumer\.share\.[a-z0-9._]+' | sort -u)

EXPECTED_CNT=$(echo "${EXPECTED}" | grep -c .)
SEEN_CNT=$(echo "${SEEN}" | grep -c .)
echo "[verify] expected ${EXPECTED_CNT} share-consumer metric names, saw ${SEEN_CNT} in topic"

MISSING=$(comm -23 <(echo "${EXPECTED}") <(echo "${SEEN}"))
if [ -n "${MISSING}" ]; then
    echo "FAIL: missing metric names in topic:"
    echo "${MISSING}" | sed 's|^|  |'
    exit 1
fi
echo "PASS: all ${EXPECTED_CNT} share-consumer metric names present in topic"
