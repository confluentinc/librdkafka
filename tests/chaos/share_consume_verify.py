#!/usr/bin/env python3
#
# confluent-kafka-python share-consumer verification helper.
#
# Python port of share_consume_verify.c. Subscribes to one or more
# topics as a KIP-932 share consumer in EXPLICIT acknowledgement mode.
# For every record it emits one JSON event to stdout for the consume
# side and one JSON event per record for the ack side, in the EXACT
# format the chaos orchestrator (tests/chaos/chaos.py) expects, so this
# binary is a drop-in workload for --consume-mode share-consumer-verify.
#
# Event lines (one JSON object per line, stdout):
#
#   {"e":"consumed","t":"<topic>","id":"<topic_id_b64>","p":<part>,
#      "o":<offset>,"dc":<deliv_cnt>}
#   {"e":"acked","t":"<topic>","id":"<topic_id_b64>","p":<part>,
#      "o":<offset>,"err":<resp_err_int>}
#
# Differences from the C version, all forced by the Python binding's
# current API surface (branch dev_kip-932_queues-for-kafka):
#
#   * No topic_id accessor. The C binary captures rd_kafka_topic_id()
#     per record (base64) so the orchestrator can keep OLD and NEW
#     topic generations separate under --topic-chaos recreate-*. The
#     Python Message has no topic_id() accessor yet (it's noted as
#     TODO in ShareConsumer.c), so this binary emits the all-zero UUID
#     base64 string ("AAAAAAAAAAAAAAAAAAAAAA") as `id`, which the
#     orchestrator treats the same as an absent id (falls back to "").
#     CONSEQUENCE: do NOT trust per-generation bookkeeping under
#     --topic-chaos recreate-* with this workload — OLD and NEW
#     generations both start at offset 0 and will collide on the
#     (topic, id, partition, offset) key. Broker-roll / leader-change /
#     reassign / delete chaos are unaffected.
#
#   * Ack error reporting. commit_sync() returns
#     Dict[TopicPartition, Optional[KafkaError]] (per-partition), not a
#     per-record list. We map each acked record's (topic, partition) to
#     its partition-level KafkaError and emit that as `err`, matching
#     the C binary's behaviour (which also only has partition-level err
#     from commit_sync). On a commit_sync that raises (top-level
#     failure), we surface that error code for every record we just
#     acked, exactly like the C version's top-level-err branch.
#
# stderr is reserved for librdkafka debug logs and diagnostics.
#
# Usage:
#   share_consume_verify.py [-d <debug>] [-X k=v]... \
#                           <bootstrap> <group.id> <topic1> [topic2 ..]
#
#   -d <debug>   librdkafka debug contexts (e.g.
#                broker,fetch,cgrp,protocol,topic). Sets the `debug`
#                conf property. Same idiom as rdkafka_performance -d.
#   -X k=v       Set arbitrary rdkafka conf property (repeatable).
#                Mirrors rdkafka_performance -X / the C binary's -X.
#                Default-set: share.acknowledgement.mode=explicit
#                Override by passing it explicitly.
#
# SIGINT triggers a clean shutdown (close()), so a --rebalance-mid-roll
# removal leaves the share group cleanly.

import signal
import sys

from confluent_kafka import AcknowledgeType, ShareConsumer

# All-zero UUID, base64-encoded the same way rd_kafka_Uuid_base64str
# would render it (16 zero bytes -> 22 base64 chars, no padding). The
# orchestrator falls back to "" for this, keeping the JSON shape
# identical to the C binary's output.
_ZERO_TOPIC_ID_B64 = "AAAAAAAAAAAAAAAAAAAAAA"

# commit_sync() poll budget, mirrors the C binary's 30000 ms.
_COMMIT_TIMEOUT_S = 30.0
# poll() batch timeout, mirrors the C binary's 1000 ms consume_batch.
_POLL_TIMEOUT_S = 1.0

_run = True


def _stop(signum, frame):
    global _run
    _run = False


def _usage(prog):
    sys.stderr.write(
        "Usage: %s [-d <debug>] [-X k=v]... "
        "<broker> <group.id> <topic1> [topic2 ..]\n" % prog
    )


def _parse_args(argv):
    """Parse the C-binary-compatible flag set: -d <debug>, -X k=v
    (repeatable), -h. Returns (conf, topics) or exits; bootstrap.servers
    and group.id are folded into conf.

    Hand-rolled rather than argparse so the flag semantics match the C
    getopt() loop exactly (flags before positionals, -X repeatable)."""
    conf = {
        # The one conf this binary really requires: explicit-ack drives
        # the per-record bookkeeping. Default it; -X can override.
        "share.acknowledgement.mode": "explicit",
    }
    debug = None
    i = 1
    n = len(argv)
    while i < n and argv[i].startswith("-"):
        opt = argv[i]
        if opt == "-h":
            _usage(argv[0])
            sys.exit(0)
        elif opt == "-d":
            if i + 1 >= n:
                _usage(argv[0])
                sys.exit(1)
            debug = argv[i + 1]
            i += 2
        elif opt == "-X":
            if i + 1 >= n:
                _usage(argv[0])
                sys.exit(1)
            kv = argv[i + 1]
            eq = kv.find("=")
            if eq <= 0:
                sys.stderr.write("%% -X %s: expected key=value\n" % kv)
                sys.exit(1)
            conf[kv[:eq]] = kv[eq + 1:]
            i += 2
        else:
            _usage(argv[0])
            sys.exit(1)

    if debug is not None:
        conf["debug"] = debug

    rest = argv[i:]
    if len(rest) < 3:
        _usage(argv[0])
        sys.exit(1)

    conf["bootstrap.servers"] = rest[0]
    conf["group.id"] = rest[1]
    topics = rest[2:]
    return conf, topics


def _emit_consumed(topic, topic_id, partition, offset, dc):
    # delivery_count() can be None on the Python side; the C binary
    # always has an int16. Coerce None -> 0 so the JSON stays numeric.
    sys.stdout.write(
        '{"e":"consumed","t":"%s","id":"%s","p":%d,"o":%d,"dc":%d}\n'
        % (topic, topic_id, partition, offset, dc if dc is not None else 0)
    )


def _emit_acked(topic, topic_id, partition, offset, err):
    sys.stdout.write(
        '{"e":"acked","t":"%s","id":"%s","p":%d,"o":%d,"err":%d}\n'
        % (topic, topic_id, partition, offset, err)
    )


def main(argv):
    conf, topics = _parse_args(argv)

    sc = ShareConsumer(conf)
    sc.subscribe(topics)

    sys.stderr.write("%% verify: subscribed to %d topic(s)\n" % len(topics))

    # Clean shutdown on SIGINT (matches the C binary; lets a
    # --rebalance-mid-roll removal leave the group cleanly).
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        while _run:
            try:
                messages = sc.poll(timeout=_POLL_TIMEOUT_S)
            except Exception as e:  # noqa: BLE001 - mirror C's loose handling
                sys.stderr.write("%% poll error: %s\n" % e)
                continue

            if not messages:
                continue

            # (topic, partition, offset, topic_id) we acked this batch,
            # so we can pair the per-record "acked" event with the
            # per-partition err from commit_sync. Cleared per batch.
            acked = []

            for msg in messages:
                if msg.error():
                    sys.stderr.write(
                        "%% rkm err on %s [%s]: %s\n"
                        % (msg.topic(), msg.partition(), msg.error())
                    )
                    continue

                topic = msg.topic()
                partition = msg.partition()
                offset = msg.offset()
                # No topic_id() accessor on the Python Message yet; use
                # the zero-UUID sentinel (orchestrator treats as "").
                topic_id = _ZERO_TOPIC_ID_B64

                _emit_consumed(topic, topic_id, partition, offset,
                               msg.delivery_count())

                try:
                    sc.acknowledge(msg, AcknowledgeType.ACCEPT)
                except Exception as e:  # noqa: BLE001
                    sys.stderr.write(
                        "%% acknowledge err for %s [%s] @ %s: %s\n"
                        % (topic, partition, offset, e)
                    )
                    continue

                acked.append((topic, partition, offset, topic_id))

            if not acked:
                continue

            # commit_sync() -> Dict[TopicPartition, Optional[KafkaError]].
            # On a top-level raise we can't tell partition outcomes, so
            # surface the top-level err to every record we just acked
            # (mirrors the C binary's commit_sync-error branch).
            try:
                results = sc.commit_sync(timeout=_COMMIT_TIMEOUT_S)
            except Exception as e:  # noqa: BLE001
                top_err = _err_code(e)
                sys.stderr.write("%% commit_sync error: %s\n" % e)
                for topic, partition, offset, topic_id in acked:
                    _emit_acked(topic, topic_id, partition, offset, top_err)
                continue

            # Build a (topic, partition) -> err-int map from the
            # per-partition result dict, then emit one "acked" per
            # record with the matching err (0 == accepted).
            part_err = {}
            for tp, kerr in (results or {}).items():
                part_err[(tp.topic, tp.partition)] = (
                    0 if kerr is None else kerr.code()
                )

            for topic, partition, offset, topic_id in acked:
                err = part_err.get((topic, partition), 0)
                _emit_acked(topic, topic_id, partition, offset, err)
    finally:
        sys.stderr.write("%% verify: shutting down\n")
        sc.close()
    return 0


def _err_code(exc):
    """Best-effort extraction of an integer error code from whatever
    commit_sync / acknowledge raised. KafkaException wraps a KafkaError
    with .code(); fall back to -1 for anything else."""
    try:
        args = getattr(exc, "args", None)
        if args:
            kerr = args[0]
            code = getattr(kerr, "code", None)
            if callable(code):
                return code()
    except Exception:  # noqa: BLE001
        pass
    return -1


if __name__ == "__main__":
    # Line-buffer stdout so each JSON object reaches the orchestrator
    # immediately (matches the C binary's setvbuf _IOLBF).
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except AttributeError:
        pass
    sys.exit(main(sys.argv))