#!/bin/bash
set -euo pipefail

# Defaults. Override at trigger time by setting these as env vars.
DURATION_SECONDS=${DURATION_SECONDS:-300}
KAFKA_VERSION=${KAFKA_VERSION:-4.2.0}
METRIC_NAMES=${METRIC_NAMES:-*}
MSG_SIZE=${MSG_SIZE:-1000}
PRODUCER_RATE=${PRODUCER_RATE:-1000}

ROOT=$(git rev-parse --show-toplevel)
WORK_DIR=${ROOT}/tests/share_perf_compare/.work
TUTORIALS_REPO=${WORK_DIR}/tutorials
PLUGIN_JAR=${WORK_DIR}/client-telemetry-reporter-plugin.jar
KAFKA_DIR=${WORK_DIR}/kafka_2.13-${KAFKA_VERSION}
KAFKA_DATA=${WORK_DIR}/kafka-data
OTEL_CONFIG=${TUTORIALS_REPO}/client-telemetry-reporter-plugin/kafka/otel-collector-config.yaml
PROM_CONFIG=${TUTORIALS_REPO}/client-telemetry-reporter-plugin/kafka/prometheus.yml

TOPIC=perf-share
SUBSCRIPTION_NAME=share-perf-cmp
LIBRDKAFKA_GROUP=librdkafka-share
JAVA_GROUP=java-share

mkdir -p ${WORK_DIR}

echo "==> Parameters"
echo "    DURATION_SECONDS = ${DURATION_SECONDS}"
echo "    KAFKA_VERSION    = ${KAFKA_VERSION}"
echo "    METRIC_NAMES     = ${METRIC_NAMES}"
echo "    MSG_SIZE         = ${MSG_SIZE}"
echo "    PRODUCER_RATE    = ${PRODUCER_RATE}"

if [ ! -f "${PLUGIN_JAR}" ]; then
    echo "==> Building OTLP reporter plugin"
    if [ ! -d "${TUTORIALS_REPO}" ]; then
        git clone --depth 1 https://github.com/confluentinc/tutorials.git ${TUTORIALS_REPO}
    fi
    sed -i.bak \
        "s|id 'com.github.johnrengelman.shadow' version '8.1.1'|id 'com.gradleup.shadow' version '8.3.6'|" \
        ${TUTORIALS_REPO}/client-telemetry-reporter-plugin/kafka/build.gradle
    (cd ${TUTORIALS_REPO} && ./gradlew --no-daemon clean \
        :client-telemetry-reporter-plugin:kafka:build)
    cp ${TUTORIALS_REPO}/client-telemetry-reporter-plugin/kafka/build/libs/client-telemetry-reporter-plugin.jar \
        ${PLUGIN_JAR}
fi

if [ ! -d "${KAFKA_DIR}" ]; then
    echo "==> Setting up Kafka broker ${KAFKA_VERSION}"
    wget -q https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz \
        -O ${WORK_DIR}/kafka.tgz
    tar -C ${WORK_DIR} -xzf ${WORK_DIR}/kafka.tgz

    SP=${KAFKA_DIR}/config/server.properties
    # No second listener needed — OTel Collector talks to the broker only via
    # the OTLP gRPC receiver (port 4317), not as a Kafka client.
    sed -i.bak \
        -e "s|^log.dirs=/tmp/kraft-combined-logs|log.dirs=${KAFKA_DATA}|" \
        ${SP}
    echo 'metric.reporters=io.confluent.developer.ClientOtlpMetricsReporter' >> ${SP}

    cp ${PLUGIN_JAR} ${KAFKA_DIR}/libs/
fi

echo "==> Formatting fresh broker storage"
rm -rf ${KAFKA_DATA}
mkdir -p ${KAFKA_DATA}
CLUSTER_ID=$(${KAFKA_DIR}/bin/kafka-storage.sh random-uuid)
${KAFKA_DIR}/bin/kafka-storage.sh format --standalone \
    -t ${CLUSTER_ID} -c ${KAFKA_DIR}/config/server.properties

TEST_CONF_BAK=${ROOT}/tests/test.conf.bak.$$
PRODUCER_PID=""
LIBRDKAFKA_CONSUMER_PID=""
JAVA_CONSUMER_PID=""

cleanup() {
    local rc=$?
    set +e
    echo "==> Cleanup"
    [ -n "${PRODUCER_PID}" ] && kill -TERM ${PRODUCER_PID} 2>/dev/null
    [ -n "${LIBRDKAFKA_CONSUMER_PID}" ] && kill -TERM ${LIBRDKAFKA_CONSUMER_PID} 2>/dev/null
    [ -n "${JAVA_CONSUMER_PID}" ] && kill -TERM ${JAVA_CONSUMER_PID} 2>/dev/null
    sleep 2
    [ -n "${PRODUCER_PID}" ] && kill -KILL ${PRODUCER_PID} 2>/dev/null
    [ -n "${LIBRDKAFKA_CONSUMER_PID}" ] && kill -KILL ${LIBRDKAFKA_CONSUMER_PID} 2>/dev/null
    [ -n "${JAVA_CONSUMER_PID}" ] && kill -KILL ${JAVA_CONSUMER_PID} 2>/dev/null
    ${KAFKA_DIR}/bin/kafka-server-stop.sh 2>/dev/null
    docker stop perf-otel-collector perf-prometheus 2>/dev/null
    docker rm   perf-otel-collector perf-prometheus 2>/dev/null
    if [ -f "${TEST_CONF_BAK}" ]; then
        mv ${TEST_CONF_BAK} ${ROOT}/tests/test.conf
    fi
    exit ${rc}
}
trap cleanup EXIT

echo "==> Starting OpenTelemetry Collector + Prometheus containers"
docker rm -f perf-otel-collector perf-prometheus 2>/dev/null || true
docker network create perf-share-net 2>/dev/null || true
docker run -d --name perf-otel-collector \
    --network perf-share-net --network-alias otel-collector \
    --add-host=host.docker.internal:host-gateway \
    -p 4317:4317 -p 8889:8889 \
    -v ${OTEL_CONFIG}:/etc/otelcol-contrib/config.yaml \
    otel/opentelemetry-collector-contrib
docker run -d --name perf-prometheus \
    --network perf-share-net \
    --add-host=host.docker.internal:host-gateway \
    -p 9090:9090 \
    -v ${PROM_CONFIG}:/etc/prometheus/prometheus.yml \
    prom/prometheus
# The tutorials' prometheus.yml scrapes "otel-collector:8889" by service name;
# the --network-alias above keeps that resolvable from Prometheus's container.

echo "==> Starting Kafka broker"
OTEL_EXPORTER_OTLP_ENDPOINT=127.0.0.1:4317 \
    ${KAFKA_DIR}/bin/kafka-server-start.sh -daemon \
    ${KAFKA_DIR}/config/server.properties

for i in $(seq 1 60); do
    if ${KAFKA_DIR}/bin/kafka-broker-api-versions.sh \
        --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        break
    fi
    sleep 1
done

echo "==> Creating topic + metrics subscription"
${KAFKA_DIR}/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic ${TOPIC} --partitions 3 --replication-factor 1 --if-not-exists
${KAFKA_DIR}/bin/kafka-client-metrics.sh --bootstrap-server localhost:9092 --alter \
    --name ${SUBSCRIPTION_NAME} \
    --metrics "org.apache.kafka.consumer.share." \
    --interval 5000

echo "==> Building librdkafka"
(cd ${ROOT} && ./configure --install-deps --enable-devel)
make -j -C ${ROOT}

if [ -f ${ROOT}/tests/test.conf ]; then
    cp ${ROOT}/tests/test.conf ${TEST_CONF_BAK}
fi
cat > ${ROOT}/tests/test.conf <<'EOF'
metadata.broker.list=localhost:9092
security.protocol=plaintext
enable.metrics.push=true
EOF

# Unique group ids per run so we always start fresh and don't inherit
# uncommitted offsets / stale members from a prior invocation.
RUN_TAG=$(date +%s)
LIBRDKAFKA_GROUP_ID=${LIBRDKAFKA_GROUP}-${RUN_TAG}
JAVA_GROUP_ID=${JAVA_GROUP}-${RUN_TAG}

echo "==> Launching perf clients (duration ${DURATION_SECONDS}s)"
PERF_BIN=${ROOT}/examples/rdkafka_performance

# (a) librdkafka producer (continuous feed)
${PERF_BIN} -P -t ${TOPIC} \
    -s ${MSG_SIZE} -r ${PRODUCER_RATE} \
    -X file=${ROOT}/tests/test.conf \
    -X enable.metrics.push=true \
    -d telemetry,broker \
    > ${WORK_DIR}/producer.log 2>&1 &
PRODUCER_PID=$!
echo "    producer PID=${PRODUCER_PID}"

# (b) librdkafka share consumer
# Note: deliberately do NOT override fetch.wait.max.ms here. Java's perf test
# uses the default 500 ms, and overriding only on the librdkafka side would
# skew fetch-latency / fetch-rate comparisons.
${PERF_BIN} -S ${LIBRDKAFKA_GROUP_ID} -t ${TOPIC} \
    -X file=${ROOT}/tests/test.conf \
    -X auto.offset.reset=earliest \
    -X enable.metrics.push=true \
    -d telemetry \
    > ${WORK_DIR}/librdkafka-share.log 2>&1 &
LIBRDKAFKA_CONSUMER_PID=$!
echo "    librdkafka share PID=${LIBRDKAFKA_CONSUMER_PID}"

# (c) Java share consumer perf
${KAFKA_DIR}/bin/kafka-share-consumer-perf-test.sh \
    --bootstrap-server localhost:9092 \
    --topic ${TOPIC} \
    --group ${JAVA_GROUP_ID} \
    --num-records 1000000 \
    --timeout 300000 \
    --reporting-interval 1000 \
    --show-detailed-stats \
    > ${WORK_DIR}/java-share.log 2>&1 &
JAVA_CONSUMER_PID=$!
echo "    java share PID=${JAVA_CONSUMER_PID}"

sleep ${DURATION_SECONDS}

echo "==> Stopping perf clients, waiting for trailing telemetry push"
kill -TERM ${PRODUCER_PID} ${LIBRDKAFKA_CONSUMER_PID} ${JAVA_CONSUMER_PID} 2>/dev/null || true
sleep 10

echo "==> Waiting for Prometheus to scrape the final telemetry batch"
for i in $(seq 1 30); do
    n=$(curl -s 'http://localhost:9090/api/v1/label/__name__/values' \
        | jq -r '.data | map(select(startswith("kip-714"))) | length' 2>/dev/null)
    if [ -n "${n}" ] && [ "${n}" -gt 0 ]; then
        echo "    Prometheus has ${n} kip-714_* metric names (after ${i} polls)"
        break
    fi
    sleep 2
done

# Build list of metric names to query. Two entries in the header
# (acknowledgements.send.{rate,total}) are split across lines using C string
# concatenation, so we flatten the file and collapse adjacent string literals
# before grepping.
ALL_NAMES=$(
    tr '\n' ' ' < ${ROOT}/src/rdkafka_telemetry_encode.h \
        | sed 's/" *"//g' \
        | grep -oE 'consumer\.share\.[a-z0-9._]+' \
        | sort -u
)
if [ "${METRIC_NAMES}" = "*" ]; then
    NAMES="${ALL_NAMES}"
else
    NAMES=$(echo "${METRIC_NAMES}" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
fi

prom_query() {
    local selector=$1
    local encoded
    encoded=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "${selector}")
    curl -s "http://localhost:9090/api/v1/query?query=${encoded}" \
        | jq -r '.data.result[0].value[1] // "—"'
}

echo "==> Building report"
REPORT_FILE=${ROOT}/REPORT.md
{
    echo "# Share consumer perf comparison"
    echo ""
    echo "**Duration:** ${DURATION_SECONDS}s  |  **Kafka:** ${KAFKA_VERSION}  |  **Msg size:** ${MSG_SIZE}B  |  **Producer rate:** ${PRODUCER_RATE}/s"
    echo ""
    echo "| Metric | librdkafka | Java |"
    echo "|---|---:|---:|"
    while IFS= read -r metric; do
        [ -z "${metric}" ] && continue
        # consumer.share.fetch.manager.fetch.rate
        #   -> org.apache.kafka.consumer.share.fetch.manager.fetch.rate  (registry prefix)
        #   -> kip-714_org_apache_kafka_consumer_share_fetch_manager_fetch_rate  (OTel namespace + dots->underscores)
        prom_name="kip-714_org_apache_kafka_$(echo "${metric}" | tr '.' '_')"
        librdkafka_val=$(prom_query "{__name__=\"${prom_name}\", client_software_name=\"librdkafka\"}")
        java_val=$(prom_query "{__name__=\"${prom_name}\", client_software_name=\"apache-kafka-java\"}")
        printf '| `%s` | %s | %s |\n' "${metric}" "${librdkafka_val}" "${java_val}"
    done <<< "${NAMES}"
} > ${REPORT_FILE}

cat ${REPORT_FILE}

if command -v artifact >/dev/null 2>&1; then
    artifact push job -f -d .semaphore/REPORT.md ${REPORT_FILE} || true
fi

exit 0
