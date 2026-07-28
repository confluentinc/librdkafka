#!/bin/bash
#
# Manual, opt-in end-to-end check for the IPv6 broker-nodename fix over SASL.
#
# NOT part of the automated suite (it needs Docker + host networking and is a
# .sh, so the test-runner's "[08]*-*.c" glob never picks it up). The automated
# coverage lives in the `broker` / `sasl` unit tests and mock test 0191.
#
# What it proves, against a real Kafka broker listening on the IPv6 loopback
# "[::1]:9092" with SASL_PLAINTEXT/PLAIN:
#   1. Regression: SASL authentication + produce/consume still work with the
#      refactored rd_kafka_sasl_client_new().
#   2. IPv6 fix: the SASL client hostname is extracted as the bare "::1" from
#      the bracketed nodename "[::1]:9092" (the pre-fix strchr() code truncated
#      it to "[").
#
# A/B the fix: run once with a lib built from this branch (expect PASS), then
# rebuild from origin/master and run again (expect the SASL hostname to be "["
# and this script to FAIL on that assertion).
#
# Usage:   tests/manual-ipv6-sasl-repro.sh
# Env:     LIBDIR   (default: ../src)   librdkafka .so to link the example to
#          IMAGE    (default: apache/kafka:4.0.0)
#          ADDR     (default: [::1])    IPv6 host the broker binds/advertises
#
set -u

LIBDIR="${LIBDIR:-$(cd "$(dirname "$0")/../src" && pwd)}"
EXDIR="$(cd "$(dirname "$0")/../examples" && pwd)"
IMAGE="${IMAGE:-apache/kafka:4.0.0}"
ADDR="${ADDR:-[::1]}"
CONTAINER="kafka-sasl-ipv6-repro"
TOPIC="ipv6sasl-$$"
# Keep the payload short (<=16 bytes) so it fits on a single line of the
# example client's consumer hexdump, keeping the round-trip grep simple.
PAYLOAD="ipv6ok$$"
USER=user
PASS=userpw
CLIENT="$EXDIR/rdkafka_example"
COMMON=(-X security.protocol=SASL_PLAINTEXT -X sasl.mechanism=PLAIN \
        -X sasl.username=$USER -X sasl.password=$PASS)

fail() { echo "FAIL: $*" >&2; exit 1; }
cleanup() { docker rm -f "$CONTAINER" >/dev/null 2>&1; }
trap cleanup EXIT

command -v docker >/dev/null || fail "docker not found"
[ -x "$CLIENT" ] || fail "example client not built: run 'make -C src && make -C examples rdkafka_example'"

echo "## Starting SASL broker on SASL_PLAINTEXT://$ADDR:9092 ($IMAGE)"
cleanup
docker run -d --name "$CONTAINER" --network host \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS="SASL://$ADDR:9092,CONTROLLER://$ADDR:9093" \
  -e KAFKA_ADVERTISED_LISTENERS="SASL://$ADDR:9092" \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=SASL \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP='SASL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT' \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@$ADDR:9093" \
  -e KAFKA_SASL_ENABLED_MECHANISMS=PLAIN \
  -e KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN \
  -e KAFKA_LISTENER_NAME_SASL_PLAIN_SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\" user_admin=\"admin-secret\" user_$USER=\"$PASS\";" \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
  "$IMAGE" >/dev/null || fail "failed to start broker container"

echo "## Waiting for broker to accept SASL produce"
plog=$(mktemp)
ready=0
for i in $(seq 1 30); do
        if echo "$PAYLOAD" | LD_LIBRARY_PATH="$LIBDIR" timeout 20 \
              "$CLIENT" -P -b "$ADDR:9092" -t "$TOPIC" -p 0 "${COMMON[@]}" \
              -X debug=security,broker >"$plog" 2>&1; then
                ready=1; break
        fi
        sleep 2
done
[ "$ready" = 1 ] || { docker logs "$CONTAINER" 2>&1 | tail -20; fail "broker never accepted a SASL produce"; }

echo "## Assertion 1 (regression): SASL authentication reached UP"
grep -q "AUTH_REQ -> UP" "$plog" || fail "SASL auth did not reach UP"

echo "## Assertion 2 (IPv6 fix): SASL hostname is the bare '::1', not '['"
grep "Initializing SASL client" "$plog" | grep -q "hostname ::1," \
        || { grep "Initializing SASL client" "$plog" >&2; \
             fail "SASL hostname was not extracted as '::1' (pre-fix bug: 'hostname [')"; }
! grep "Initializing SASL client" "$plog" | grep -q "hostname \[" \
        || fail "SASL hostname was mangled to '[' (pre-fix strchr bug present)"
! grep -q ":::" "$plog" || fail "malformed ':::' nodename present (nodename bracket fix missing)"

echo "## Assertion 3 (regression): message round-trips over SASL"
clog=$(mktemp)
LD_LIBRARY_PATH="$LIBDIR" timeout 20 \
  "$CLIENT" -C -b "$ADDR:9092" -t "$TOPIC" -p 0 -o beginning -e "${COMMON[@]}" \
  >"$clog" 2>&1
grep -aq "$PAYLOAD" "$clog" || { cat "$clog" >&2; fail "produced message not consumed back over SASL"; }

echo
echo "PASS: SASL auth + produce/consume over [::1] OK; SASL hostname correctly '::1'"
rm -f "$plog" "$clog"
