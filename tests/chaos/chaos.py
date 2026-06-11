#!/usr/bin/env python3
"""Reproduce KIP-932 share-consumer chaos-test failures locally.

Brings up a real Kafka KRaft cluster via trivup (broker conf mirrors
.semaphore/semaphore.yml + tests/interactive_broker_version.py), spawns
a producer and N share-consumers (all via examples/rdkafka_performance),
then rolls the brokers sequentially. Each share-consumer captures its
own stdout + stderr to a dedicated directory, matching the layout used
by the Kafka project's system tests
(kafkatest/services/verifiable_share_consumer.py):

    <log-dir>/
        consumer-0/
            consumer.stdout
            consumer.stderr
        consumer-1/
            ...
        summary.txt          # gap-signature counts across all consumers

Producer output is discarded.

Usage:
    python3 tests/chaos/chaos.py [--consumers 3] [--cycles 3] ...

Requires:
    - examples/rdkafka_performance built
    - trivup installed (pip install -r tests/requirements.txt)
"""

import argparse
import json
import logging
import logging.handlers
import os
import random
import re
import signal
import subprocess
import sys
import threading
import time

# Lives in tests/chaos/ alongside share_consume_verify(.c).
# cluster_testing.py and the scenarios/ dir are one level up in tests/.
_HERE = os.path.dirname(os.path.abspath(__file__))
_TESTS_DIR = os.path.dirname(_HERE)
sys.path.insert(0, _TESTS_DIR)
from cluster_testing import (  # noqa: E402
    LibrdkafkaTestCluster, read_scenario_conf)


# Broker properties matching CI share-consumer runs
# (packaging/tools/run-share-consumer-tests.sh) plus the property
# tests/interactive_broker_version.py always appends.
# Note: group.share.min.record.lock.duration.ms is intentionally NOT
# listed here — cluster_testing.py already sets it for Kafka >= 4.2.0
# and we keep its broker default behaviour for this run.
CI_BROKER_PROPS = [
    'transaction.state.log.replication.factor=1',
    'transaction.state.log.min.isr=1',
    'log.retention.bytes=1000000000',
]


# ---------------------------------------------------------------------------
# Per-consumer log capture
# ---------------------------------------------------------------------------

def make_rotating_logger(name, path, budget_bytes, backups=4):
    """RotatingFileHandler logger sized to fit within `budget_bytes` total."""
    max_bytes = max(budget_bytes // (backups + 1), 1 << 20)
    h = logging.handlers.RotatingFileHandler(
        path, maxBytes=max_bytes, backupCount=backups)
    h.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(h)
    logger.propagate = False
    return logger


def pipe_to_logger(stream, logger, echo_re=None, echo_prefix=''):
    """Read `stream` line by line and route each line through logger.

    If `echo_re` is provided, any line matching it is also printed to
    orchestrator stdout, prefixed with `echo_prefix`. Used to surface
    consumer progress (e.g. periodic `% N messages consumed` lines)
    while the workload runs.
    """
    for raw in iter(stream.readline, b''):
        try:
            text = raw.decode('utf-8', errors='replace').rstrip()
        except Exception:
            text = repr(raw)
        logger.info('%s', text)
        if echo_re is not None and echo_re.search(text):
            print(f'{echo_prefix}{text}', flush=True)
    stream.close()


class LoggedProcess:
    """Subprocess with rotating stdout + stderr capture to a dedicated dir."""

    def __init__(self, name, log_dir, budget_bytes,
                 stdout_share=0.5, stderr_share=0.5):
        self.name = name
        self.dir = os.path.join(log_dir, name)
        os.makedirs(self.dir, exist_ok=True)
        self.stdout_path = os.path.join(self.dir, f'{name}.stdout')
        self.stderr_path = os.path.join(self.dir, f'{name}.stderr')
        self.stdout_logger = make_rotating_logger(
            f'{name}-stdout', self.stdout_path,
            int(budget_bytes * stdout_share))
        self.stderr_logger = make_rotating_logger(
            f'{name}-stderr', self.stderr_path,
            int(budget_bytes * stderr_share))
        self.proc = None

    # Subclasses can set this to a compiled regex; matching lines from
    # stdout will be tee'd to orchestrator stdout (prefixed with name).
    echo_stdout_re = None

    def start(self, cmd, env):
        print(f"[orch] starting {self.name}: {' '.join(cmd)}", flush=True)
        self.proc = subprocess.Popen(
            cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        threading.Thread(
            target=pipe_to_logger,
            args=(self.proc.stdout, self.stdout_logger),
            kwargs={'echo_re': self.echo_stdout_re,
                    'echo_prefix': f'[{self.name}] '},
            daemon=True).start()
        threading.Thread(target=pipe_to_logger,
                         args=(self.proc.stderr, self.stderr_logger),
                         daemon=True).start()

    def stop(self):
        if self.proc is None:
            return
        try:
            self.proc.send_signal(signal.SIGINT)
            self.proc.wait(timeout=30)
        except (subprocess.TimeoutExpired, ProcessLookupError):
            try:
                self.proc.kill()
            except ProcessLookupError:
                pass

    def close_logs(self):
        for lg in (self.stdout_logger, self.stderr_logger):
            for h in lg.handlers:
                h.flush()
                h.close()


class ShareConsumer(LoggedProcess):
    """rdkafka_performance -S workload. stderr is debug-heavy; bias budget."""

    # Tee periodic `% N messages (...) consumed in...` stats lines to
    # orchestrator stdout so the operator can watch progress live.
    echo_stdout_re = re.compile(r'% \d+ messages \(\d+ bytes\) consumed')

    def __init__(self, idx, log_dir, budget_bytes):
        # stdout is the JSON event stream (consumed/acked records) and
        # is the test's verification data — it MUST NOT be rotated out
        # mid-run. stderr is forensic debug. Give them equal space so
        # the data stream gets a 5× larger share than the legacy
        # split. (Original layout assumed rdkafka_performance -G mode
        # where stdout is just a stats trickle; share_consume_verify
        # inverts that: stdout >> stderr in importance.)
        super().__init__(f'consumer-{idx}', log_dir, budget_bytes,
                         stdout_share=0.5, stderr_share=0.5)
        self.idx = idx
        # librdkafka stats JSON (-T <ms> + -Y "cat >> ...") goes here; one
        # JSON object per line. Used by the partition-coverage report.
        self.stats_path = os.path.join(self.dir, 'consumer.stats.json')
        self.pre_obs_blob = None  # last stats blob seen at mark_pre_obs()

    def mark_pre_obs(self):
        """Snapshot the most recent stats blob right before the
        observation window starts. Used to diff against the final
        snapshot so we can ask 'did every partition see new records
        during the observation window?'"""
        self.pre_obs_blob = self._last_stats_blob()

    def _last_stats_blob(self):
        if not os.path.exists(self.stats_path):
            return None
        last = None
        with open(self.stats_path, 'r', errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    last = json.loads(line)
                except json.JSONDecodeError:
                    continue
        return last

    def post_obs_blob(self):
        return self._last_stats_blob()


class Producer(LoggedProcess):
    """rdkafka_performance -P workload. Producer runs without -d; stdout
    carries the periodic stats and the final 'messages produced ...
    delivered' summary line we need to verify conservation.

    One Producer per test topic — `idx` is the topic index so each
    producer gets its own `producer-<idx>` log directory and the
    bookkeeping report can sum produced/delivered counts across all
    of them.
    """

    def __init__(self, idx, log_dir, budget_bytes):
        super().__init__(f'producer-{idx}', log_dir, budget_bytes,
                         stdout_share=0.9, stderr_share=0.1)
        self.idx = idx


# ---------------------------------------------------------------------------
# Cluster operations
# ---------------------------------------------------------------------------

def build_cluster_conf(scenario, reauth_ms, extra_props):
    """Combine scenario conf + interactive_broker_version + CI broker props."""
    conf = {}
    conf.update(read_scenario_conf(scenario))
    if 'conf' not in conf:
        conf['conf'] = []
    conf['conf'].append(f'connections.max.reauth.ms={reauth_ms}')
    for p in CI_BROKER_PROPS:
        conf['conf'].append(p)
    conf['conf'].extend(extra_props)
    return conf


_DESCRIBE_LEADER_RE = re.compile(r'Partition:\s*(\d+).*?Leader:\s*(-?\d+)')


def describe_leaders(cluster, topics):
    """Query the broker for the current leader of each partition.

    Returns dict[(topic, partition_id)] -> leader_broker_id (or -1 if no
    leader). On error returns an empty dict so the caller can still
    proceed.
    """
    broker = cluster.brokers[0]
    bindir = broker.conf['bindir']
    bootstrap = cluster.bootstrap_servers()
    result = {}
    for t in topics:
        cmd = [
            os.path.join(bindir, 'kafka-topics.sh'),
            '--bootstrap-server', bootstrap,
            '--describe', '--topic', t,
        ]
        try:
            output = subprocess.check_output(
                cmd, stderr=subprocess.DEVNULL,
                timeout=10).decode(errors='replace')
        except Exception as e:
            print(f'[orch] describe_leaders({t}) failed: {e}', flush=True)
            continue
        for line in output.splitlines():
            m = _DESCRIBE_LEADER_RE.search(line)
            if m:
                result[(t, int(m.group(1)))] = int(m.group(2))
    return result


def _diff_leaders(before, after):
    """Return [(topic, part, before_leader, after_leader)] for keys that
    differ between the two snapshots."""
    out = []
    for key in before:
        if key in after and before[key] != after[key]:
            out.append((key[0], key[1], before[key], after[key]))
    return out


def _format_leader_diff(diffs):
    if not diffs:
        return 'no-leader-changes'
    return ', '.join(f'{t}[{p}]:{a}->{b}' for t, p, a, b in diffs)


# ---------------------------------------------------------------------------
# Reassignment / leader-change helpers
# ---------------------------------------------------------------------------

# Parses lines of `kafka-topics.sh --describe` like:
#   Topic: foo  Partition: 3  Leader: 2  Replicas: 2,3,4  Isr: 2,3,4
_DESCRIBE_REPLICAS_RE = re.compile(
    r'Topic:\s*(\S+).*?Partition:\s*(\d+).*?Leader:\s*(-?\d+)'
    r'.*?Replicas:\s*([\d,]+).*?Isr:\s*([\d,]*)')


def describe_topic_replicas(cluster, topics):
    """Return dict[(topic, partition)] -> {leader, replicas, isr}.

    Wrapper around `kafka-topics.sh --describe` that captures the
    full replica + ISR set, not just the leader (which
    `describe_leaders` already gives).
    """
    broker = cluster.brokers[0]
    bindir = broker.conf['bindir']
    bootstrap = cluster.bootstrap_servers()
    result = {}
    for t in topics:
        cmd = [
            os.path.join(bindir, 'kafka-topics.sh'),
            '--bootstrap-server', bootstrap,
            '--describe', '--topic', t,
        ]
        try:
            output = subprocess.check_output(
                cmd, stderr=subprocess.DEVNULL,
                timeout=10).decode(errors='replace')
        except Exception as e:
            print(f'[orch] describe_topic_replicas({t}) failed: {e}',
                  flush=True)
            continue
        for line in output.splitlines():
            m = _DESCRIBE_REPLICAS_RE.search(line)
            if not m:
                continue

            def _ids(s):
                return [int(x) for x in s.split(',') if x]

            result[(m.group(1), int(m.group(2)))] = {
                'leader': int(m.group(3)),
                'replicas': _ids(m.group(4)),
                'isr': _ids(m.group(5)),
            }
    return result


# --- Pure planners (no Kafka calls) ---------------------------------------


def _pick_change_leader(current_replicas):
    """`[L, b, c]` -> `[b, c, L]`: rotate so the next replica becomes
    the preferred leader. Replica membership UNCHANGED, just reorder."""
    if len(current_replicas) < 2:
        raise ValueError('RF=1: no other replica to promote')
    return current_replicas[1:] + current_replicas[:1]


def _pick_reassign_partition(current_replicas, all_broker_ids):
    """`[L, b, c]` + cluster `[L, b, c, d, e]` -> `[d, b, c]`: replace
    the current leader with the lowest-id broker not in the replica
    set. Returns (new_replicas, saturated_fallback_used).

    If no broker is available outside the current replica set, falls
    back to `_pick_change_leader` (just reorder)."""
    outside = sorted(b for b in all_broker_ids
                     if b not in current_replicas)
    if not outside:
        return _pick_change_leader(current_replicas), True
    new_leader = outside[0]
    return [new_leader] + current_replicas[1:], False


def _pick_drain_leader(current_replicas, drain_id):
    """Reorder so `drain_id` is last (no longer preferred leader).
    Replica membership UNCHANGED. Used by auto-mode change-leader
    drain."""
    return [b for b in current_replicas if b != drain_id] + [drain_id]


def _pick_drain_replicas(current_replicas, drain_id, all_broker_ids, rf):
    """Build a new replica list that excludes `drain_id`, padding
    with brokers not already present to maintain `rf`. If the
    cluster is saturated (no brokers outside current replicas),
    fall back to `_pick_drain_leader`. Used by auto-mode
    reassign-partitions drain."""
    kept = [b for b in current_replicas if b != drain_id]
    pool = [b for b in all_broker_ids
            if b != drain_id and b not in kept]
    new = list(kept)
    for b in pool:
        if len(new) >= rf:
            break
        new.append(b)
    if len(new) < rf:
        # Saturated: can't drop drain_id without losing RF.
        return _pick_drain_leader(current_replicas, drain_id)
    return new


# --- Kafka-side submission ------------------------------------------------


def _verify_plan_leaders(cluster, plan, timeout_s=10):
    """Poll `describe_leaders` until every partition in `plan`
    shows its first listed replica as the actual leader, or the
    timeout elapses. Returns (ok, failures) where failures is a
    list of (topic, partition, expected_leader, actual_leader).

    This is the orchestrator-side verification that the
    reassignment was not just accepted by the controller (which
    `kafka-reassign-partitions.sh --verify` already confirms) but
    that leadership actually transferred — they're independent
    steps in the controller and can complete at different times.
    """
    expected = {(p['topic'], p['partition']): p['replicas'][0]
                for p in plan.get('partitions', [])}
    if not expected:
        return True, []
    topics = sorted({t for (t, _) in expected})
    deadline = time.time() + timeout_s
    failures = []
    while True:
        leaders = describe_leaders(cluster, topics)
        failures = []
        for key, exp in expected.items():
            actual = leaders.get(key)
            if actual != exp:
                failures.append((key[0], key[1], exp, actual))
        if not failures:
            return True, []
        if time.time() >= deadline:
            return False, failures
        time.sleep(0.5)


def _submit_reassign(cluster, plan):
    """Write the plan to a temp file, run kafka-reassign-partitions
    --execute, poll --verify until completion, then verify that
    leadership actually transferred. Returns True on success."""
    import json
    import tempfile

    if not plan.get('partitions'):
        return True

    broker = cluster.brokers[0]
    bindir = broker.conf['bindir']
    bootstrap = cluster.bootstrap_servers()
    script = os.path.join(bindir, 'kafka-reassign-partitions.sh')

    with tempfile.NamedTemporaryFile('w', suffix='.json',
                                     delete=False) as f:
        json.dump(plan, f)
        plan_path = f.name

    try:
        # Execute the plan
        try:
            subprocess.run(
                [script, '--bootstrap-server', bootstrap,
                 '--reassignment-json-file', plan_path,
                 '--execute'],
                check=False, timeout=30,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT)
        except Exception as e:
            print(f'[orch] reassign --execute failed: {e}', flush=True)
            return False

        # Poll --verify up to ~30s; reassignments here are tiny so
        # they usually complete in well under a second.
        plan_completed = False
        for _ in range(30):
            try:
                out = subprocess.run(
                    [script, '--bootstrap-server', bootstrap,
                     '--reassignment-json-file', plan_path,
                     '--verify'],
                    capture_output=True, timeout=10).stdout.decode(
                        errors='replace')
            except Exception:
                time.sleep(1)
                continue
            if 'still in progress' not in out:
                plan_completed = True
                break
            time.sleep(1)
        if not plan_completed:
            print('[orch] reassign --verify timed out after 30s',
                  flush=True)
            return False

        # Reassignment plan completed at the controller level.
        # Now verify leadership actually transferred — these are
        # separate steps and can complete at different moments.
        ok, fails = _verify_plan_leaders(cluster, plan, timeout_s=10)
        n = len(plan['partitions'])
        if ok:
            print(f'[verify] OK — all {n} partition(s) show '
                  f'expected leader after reassignment',
                  flush=True)
            return True
        print(f'[verify] FAIL — {len(fails)} of {n} partition(s) '
              f'did not get the expected leader within 10s:',
              flush=True)
        for t, p, exp, actual in fails[:10]:
            actual_s = 'no-leader' if actual is None else f'broker {actual}'
            print(f'           {t}[{p}] expected leader=broker {exp}, '
                  f'actual={actual_s}', flush=True)
        if len(fails) > 10:
            print(f'           ... and {len(fails) - 10} more',
                  flush=True)
        return False
    finally:
        try:
            os.unlink(plan_path)
        except OSError:
            pass


# --- Partition-scope wrappers (used by manual REPL) -----------------------


def change_leader_for_partition(cluster, topic, partition):
    """Rotate the replica list for one partition so the next replica
    becomes leader. No data movement."""
    info = describe_topic_replicas(cluster, [topic])
    meta = info.get((topic, partition))
    if not meta:
        print(f'[orch] no such (topic, partition): ({topic}, '
              f'{partition})', flush=True)
        return False
    try:
        new = _pick_change_leader(meta['replicas'])
    except ValueError as e:
        print(f'[orch] change-leader failed: {e}', flush=True)
        return False
    print(f'[orch] change-leader {topic}[{partition}] '
          f'{meta["replicas"]} -> {new}', flush=True)
    return _submit_reassign(cluster, {
        'version': 1,
        'partitions': [{'topic': topic, 'partition': partition,
                        'replicas': new}],
    })


def reassign_partition(cluster, topic, partition, all_broker_ids):
    """Swap the current leader out for a non-replica broker; data
    moves as part of the reassignment. Falls back to reorder if the
    cluster is saturated."""
    info = describe_topic_replicas(cluster, [topic])
    meta = info.get((topic, partition))
    if not meta:
        print(f'[orch] no such (topic, partition): ({topic}, '
              f'{partition})', flush=True)
        return False
    new, fallback = _pick_reassign_partition(meta['replicas'],
                                             all_broker_ids)
    note = ' (saturated; fell back to reorder)' if fallback else ''
    print(f'[orch] reassign {topic}[{partition}] {meta["replicas"]} '
          f'-> {new}{note}', flush=True)
    return _submit_reassign(cluster, {
        'version': 1,
        'partitions': [{'topic': topic, 'partition': partition,
                        'replicas': new}],
    })


# --- Broker-scope wrappers (used by auto-mode reassign_roll) --------------


def drain_broker_change_leader(cluster, topics, drain_broker_id):
    """Reorder replicas on every partition currently led by
    `drain_broker_id` so it's no longer the preferred leader. One
    batched reassignment plan."""
    info = describe_topic_replicas(cluster, topics)
    parts = []
    for (t, p), meta in info.items():
        if meta['leader'] != drain_broker_id:
            continue
        new = _pick_drain_leader(meta['replicas'], drain_broker_id)
        parts.append({'topic': t, 'partition': p, 'replicas': new})
    if not parts:
        return True
    print(f'[orch] change-leader drain broker={drain_broker_id}: '
          f'reordering {len(parts)} partitions', flush=True)
    return _submit_reassign(cluster, {'version': 1, 'partitions': parts})


def drain_broker_reassign(cluster, topics, drain_broker_id,
                          all_broker_ids, rf):
    """Build a new replica set excluding `drain_broker_id` on every
    partition where it currently appears in the replica set. One
    batched reassignment plan."""
    info = describe_topic_replicas(cluster, topics)
    parts = []
    for (t, p), meta in info.items():
        if drain_broker_id not in meta['replicas']:
            continue
        new = _pick_drain_replicas(meta['replicas'], drain_broker_id,
                                   all_broker_ids, rf)
        parts.append({'topic': t, 'partition': p, 'replicas': new})
    if not parts:
        return True
    print(f'[orch] reassign drain broker={drain_broker_id}: moving '
          f'{len(parts)} partition replica sets', flush=True)
    return _submit_reassign(cluster, {'version': 1, 'partitions': parts})


# --- Pretty-print helper for the manual REPL `show` command --------------


def format_topic_replicas(info):
    if not info:
        return '  (no topics)'
    lines = []
    for (t, p), meta in sorted(info.items()):
        lines.append(f'  {t}[{p}]  leader={meta["leader"]:<3} '
                     f'replicas={meta["replicas"]}  '
                     f'isr={meta["isr"]}')
    return '\n'.join(lines)


_HWM_RE = re.compile(r'^(\S+):(\d+):(\d+)$')


def get_partition_hwms(cluster, topics):
    """Query the broker for each partition's high-water-mark.

    Returns dict[(topic, partition)] -> hwm_offset. Uses
    kafka-get-offsets.sh --time -1 which returns the latest offset
    per partition (i.e. how many records the broker has on that
    partition). Returns an empty dict on failure.
    """
    broker = cluster.brokers[0]
    bindir = broker.conf['bindir']
    bootstrap = cluster.bootstrap_servers()
    result = {}
    for t in topics:
        cmd = [
            os.path.join(bindir, 'kafka-get-offsets.sh'),
            '--bootstrap-server', bootstrap,
            '--topic', t, '--time', '-1',
        ]
        try:
            output = subprocess.check_output(
                cmd, stderr=subprocess.DEVNULL,
                timeout=10).decode(errors='replace')
        except Exception as e:
            print(f'[orch] get_partition_hwms({t}) failed: {e}',
                  flush=True)
            continue
        # Format per line: <topic>:<partition>:<offset>
        for line in output.splitlines():
            m = _HWM_RE.match(line.strip())
            if m:
                result[(m.group(1), int(m.group(2)))] = int(m.group(3))
    return result


def create_topic(cluster, topic, partitions, replication):
    broker = cluster.brokers[0]
    bindir = broker.conf['bindir']
    bootstrap = cluster.bootstrap_servers()
    cmd = [
        os.path.join(bindir, 'kafka-topics.sh'),
        '--bootstrap-server', bootstrap,
        '--create', '--topic', topic,
        '--partitions', str(partitions),
        '--replication-factor', str(replication),
        '--if-not-exists',
    ]
    print(f"[orch] creating topic: {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)


def delete_topic(cluster, topic):
    """Delete a topic via kafka-topics.sh. Returns True on success,
    False on broker error (e.g. topic already gone) — the chaos
    flow logs and continues either way."""
    broker = cluster.brokers[0]
    bindir = broker.conf['bindir']
    bootstrap = cluster.bootstrap_servers()
    cmd = [
        os.path.join(bindir, 'kafka-topics.sh'),
        '--bootstrap-server', bootstrap,
        '--delete', '--topic', topic,
    ]
    print(f"[orch] deleting topic: {' '.join(cmd)}", flush=True)
    rc = subprocess.run(cmd).returncode
    return rc == 0


def recreate_topic(cluster, topic, partitions, replication,
                   dwell_s=0):
    """Delete then re-create `topic` with the same partition count +
    replication factor.

    `dwell_s` > 0 leaves the topic absent for that many seconds
    between delete and re-create (the "delayed recreate" path —
    exercises the topic's S_NOTEXISTS -> S_EXISTS transition on the
    client). dwell_s == 0 issues delete + create back-to-back (the
    "immediate" path — exercises the in-place topic_id mutation).
    """
    delete_topic(cluster, topic)
    if dwell_s > 0:
        print(f'[orch] topic-chaos: dwelling {dwell_s}s before '
              f'recreating {topic}', flush=True)
        time.sleep(dwell_s)
    create_topic(cluster, topic, partitions, replication)


def set_share_group_offset_reset(cluster, group, strategy='earliest'):
    """Set share.auto.offset.reset on the share group.

    The broker-side default for new share groups is `latest`
    (`group-coordinator/.../GroupConfig.java`), which makes
    consumers skip every record produced before their first
    ShareFetch landed on a partition. For this harness we always
    want `earliest` so the consumer + producer counts are directly
    comparable.

    Safe to call before the group exists: Kafka accepts the
    ConfigRecord and applies it when the group is first created.
    """
    broker = cluster.brokers[0]
    bindir = broker.conf['bindir']
    bootstrap = cluster.bootstrap_servers()
    cmd = [
        os.path.join(bindir, 'kafka-configs.sh'),
        '--bootstrap-server', bootstrap,
        '--entity-type', 'groups',
        '--entity-name', group,
        '--alter',
        '--add-config', f'share.auto.offset.reset={strategy}',
    ]
    print(f"[orch] setting share.auto.offset.reset={strategy} on "
          f"group={group}", flush=True)
    try:
        subprocess.run(cmd, check=True, timeout=30,
                       stdout=subprocess.DEVNULL,
                       stderr=subprocess.STDOUT)
    except Exception as e:
        print(f'[orch] WARN: failed to set group config: {e}',
              flush=True)


def add_consumer(consumers, spawn_fn):
    """Spawn a new share-consumer with the next available consumer-N
    log dir; append to `consumers`. Returns the new instance."""
    new_idx = max((c.idx for c in consumers), default=-1) + 1
    new_c = spawn_fn(new_idx)
    consumers.append(new_c)
    return new_c


def remove_consumer(consumers, rng=None, idx=None):
    """Stop one running share-consumer. With `idx` set, targets that
    specific consumer; otherwise picks uniformly at random across
    currently-running consumers. Floors at 1 to keep at least one
    consumer alive (returns None instead of dropping to zero).

    The stopped consumer stays in `consumers` so its bookkeeping
    (per-record events, stats) is still aggregated into the final
    verify.txt / summary.txt reports.
    """
    active = [c for c in consumers
              if c.proc is not None and c.proc.poll() is None]
    if not active:
        print('[orch] remove-consumer: no running consumers',
              flush=True)
        return None
    if len(active) <= 1:
        print('[orch] remove-consumer: only one consumer running; '
              'refusing to drop to zero', flush=True)
        return None
    if idx is not None:
        target = next((c for c in active if c.idx == idx), None)
        if target is None:
            print(f'[orch] remove-consumer: consumer-{idx} not '
                  f'running', flush=True)
            return None
    else:
        target = (rng or random).choice(active)
    print(f'[orch] removing consumer-{target.idx}', flush=True)
    target.stop()
    return target


_MANUAL_HELP = """\
manual> commands:
  stop <i>           graceful stop (SIGTERM, wait for exit) of broker i
  kill <i>           unclean stop (SIGKILL, wait for reap) of broker i
  start <i>          start broker i and wait until operational
  change-leader <t> <p>
                     reorder replicas of (topic t, partition p) so the
                     next replica becomes leader. No data movement.
  reassign <t> <p>   replace partition's current leader with a non-
                     replica broker (data moves). Falls back to reorder
                     when the cluster is saturated.
  add-consumer       spawn a new share-consumer subprocess
                     (consumer-K where K is the next free index)
  remove-consumer [k]
                     stop one running share-consumer. With k, targets
                     consumer-k; without k, picks uniformly at random.
                     Floors at 1 so at least one stays alive.
  delete-topic <t>   kafka-topics.sh --delete on topic t
  recreate-topic <t> delete t and immediately re-create with the
                     same partition + replication; exercises the
                     in-place topic_id mutation path on the client
  show               full replica + ISR view (kafka-topics.sh --describe)
  leaders            shorthand: just current leader per partition
  status             broker up/down state for all brokers
  sleep <sec>        pause for N seconds (records keep flowing meanwhile)
  help               this help
  done | quit        exit manual mode and proceed to cooldown + drain
"""


def manual_roll(cluster, topics, leader_log_path=None,
                consumers=None, spawn_consumer_fn=None, rng=None,
                partitions=None, replication=None):
    """Interactive REPL: user controls per-broker stop/start.

    The cluster, producer, and consumers are already running. Returns
    when the user issues 'done' / 'quit' / EOF. Logs every command +
    its leader-state diff to leader_log_path (if provided), mirroring
    what `roll()` does in auto mode.

    `consumers` + `spawn_consumer_fn` + `rng` enable the
    `add-consumer` / `remove-consumer` REPL commands. If any of them
    is None, those commands print a notice and do nothing — used by
    the bring-your-own-workload manual mode where the orchestrator
    doesn't own a consumer pool.

    `partitions` + `replication` enable `recreate-topic <t>` by
    giving it the original config to re-create with.
    """
    leader_log = open(leader_log_path, 'w') if leader_log_path else None
    if leader_log:
        leader_log.write('# manual cluster roll log\n')

    def _log(line):
        if leader_log:
            leader_log.write(line + '\n')
            leader_log.flush()

    print('[manual] cluster is running. Type "help" for commands, '
          '"done" to proceed to drain.', flush=True)
    print(_MANUAL_HELP, flush=True)

    while True:
        try:
            line = input('manual> ').strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not line:
            continue
        parts = line.split()
        cmd = parts[0].lower()

        if cmd in ('quit', 'done', 'exit'):
            break

        if cmd == 'help':
            print(_MANUAL_HELP)
            continue

        if cmd == 'status':
            for i, b in enumerate(cluster.brokers):
                pid = b.pid()
                state = f'up (pid={pid})' if pid else 'down'
                print(f'  broker {i} appid={b.appid} {state}')
            continue

        if cmd == 'leaders':
            ls = describe_leaders(cluster, topics)
            if not ls:
                print('  (no leaders reported; cluster reachable?)')
            for (t, p), broker_id in sorted(ls.items()):
                print(f'  {t}[{p}] -> broker {broker_id}')
            continue

        if cmd == 'show':
            info = describe_topic_replicas(cluster, topics)
            print(format_topic_replicas(info))
            continue

        if cmd in ('change-leader', 'reassign'):
            if len(parts) < 3:
                print(f'usage: {cmd} <topic> <partition>')
                continue
            t = parts[1]
            try:
                p = int(parts[2])
            except ValueError:
                print('partition must be an integer')
                continue
            leaders_pre = describe_leaders(cluster, topics)
            if cmd == 'change-leader':
                change_leader_for_partition(cluster, t, p)
            else:  # reassign
                all_ids = [c.appid for c in cluster.brokers]
                reassign_partition(cluster, t, p, all_ids)
            leaders_post = describe_leaders(cluster, topics)
            diffs = _diff_leaders(leaders_pre, leaders_post)
            print(f'[manual] {cmd} leader changes: '
                  f'{_format_leader_diff(diffs)}')
            _log(f'         {cmd} {t}[{p}]: '
                 f'{_format_leader_diff(diffs)}')
            continue

        if cmd in ('delete-topic', 'recreate-topic'):
            if len(parts) < 2:
                print(f'usage: {cmd} <topic>')
                continue
            t = parts[1]
            ts = time.strftime('%H:%M:%S')
            try:
                if cmd == 'delete-topic':
                    delete_topic(cluster, t)
                    print(f'[manual {ts}] deleted topic {t}',
                          flush=True)
                    _log(f'[{ts}] delete-topic {t}')
                else:
                    if partitions is None or replication is None:
                        print('[manual] recreate-topic needs '
                              'partitions + replication; not '
                              'available in this mode.')
                        continue
                    recreate_topic(cluster, t, partitions,
                                   replication, dwell_s=0)
                    print(f'[manual {ts}] recreated topic {t} '
                          f'(partitions={partitions} '
                          f'replication={replication})', flush=True)
                    _log(f'[{ts}] recreate-topic {t} '
                         f'partitions={partitions} '
                         f'replication={replication}')
            except Exception as e:
                print(f'[manual] {cmd} failed: {e}', flush=True)
            continue

        if cmd in ('add-consumer', 'remove-consumer'):
            if consumers is None or spawn_consumer_fn is None:
                print(f'[manual] {cmd} not available in '
                      f'bring-your-own-workload manual mode '
                      f'(no orchestrator-owned consumer pool).')
                continue
            ts = time.strftime('%H:%M:%S')
            if cmd == 'add-consumer':
                new_c = add_consumer(consumers, spawn_consumer_fn)
                print(f'[manual {ts}] added consumer-{new_c.idx}',
                      flush=True)
                _log(f'[{ts}] add-consumer -> consumer-{new_c.idx}')
            else:
                target_idx = None
                if len(parts) >= 2:
                    try:
                        target_idx = int(parts[1])
                    except ValueError:
                        print('usage: remove-consumer [k]')
                        continue
                removed = remove_consumer(consumers, rng=rng,
                                          idx=target_idx)
                if removed is not None:
                    print(f'[manual {ts}] removed '
                          f'consumer-{removed.idx}', flush=True)
                    _log(f'[{ts}] remove-consumer -> '
                         f'consumer-{removed.idx}')
            continue

        if cmd == 'sleep':
            if len(parts) < 2:
                print('usage: sleep <seconds>')
                continue
            try:
                secs = float(parts[1])
            except ValueError:
                print('sleep arg must be numeric')
                continue
            print(f'[manual] sleeping {secs}s...')
            time.sleep(secs)
            continue

        if cmd in ('stop', 'kill', 'start'):
            if len(parts) < 2:
                print(f'usage: {cmd} <broker_index>')
                continue
            try:
                i = int(parts[1])
                b = cluster.brokers[i]
            except (ValueError, IndexError):
                print(f'invalid broker index; valid: '
                      f'0..{len(cluster.brokers) - 1}')
                continue

            ts = time.strftime('%H:%M:%S')
            leaders_pre = describe_leaders(cluster, topics)
            if cmd == 'stop':
                print(f'[manual {ts}] stopping broker {i} '
                      f'appid={b.appid}', flush=True)
                b.stop(wait_term=True, force=False)
                _log(f'[{ts}] stop  broker={i} appid={b.appid}')
            elif cmd == 'kill':
                print(f'[manual {ts}] killing broker {i} '
                      f'appid={b.appid}', flush=True)
                b.stop(wait_term=True, force=True)
                _log(f'[{ts}] kill  broker={i} appid={b.appid}')
            elif cmd == 'start':
                print(f'[manual {ts}] starting broker {i} '
                      f'appid={b.appid}', flush=True)
                b.start()
                b.wait_operational(timeout=60)
                _log(f'[{ts}] start broker={i} appid={b.appid}')
            leaders_post = describe_leaders(cluster, topics)
            diffs = _diff_leaders(leaders_pre, leaders_post)
            print(f'[manual] leader changes: '
                  f'{_format_leader_diff(diffs)}')
            _log(f'         leader changes: {_format_leader_diff(diffs)}')
            continue

        print(f'unknown command: {cmd}. type "help".')

    if leader_log:
        leader_log.close()
    print('[manual] exiting manual mode', flush=True)


def _roll_log(cycle, idx, b, phase):
    ts = time.strftime('%H:%M:%S')
    print(f'[roll {ts}] cycle={cycle} broker={idx} appid={b.appid} '
          f'{phase}', flush=True)


def leave_broker_down(cluster, topics, idx, unclean, stop_s,
                      leader_log_path):
    """Stop broker `idx` once and never restart it.

    Run before the regular roll so the broker is already out of the
    rotation when `roll()` starts. Honours `unclean` (SIGKILL vs
    SIGTERM) and waits `stop_s` after the stop so leader migration
    has time to register before the post-stop sample.
    """
    if idx < 0 or idx >= len(cluster.brokers):
        print(f'[orch] FATAL: --leave-broker-down {idx} out of range '
              f'(have {len(cluster.brokers)} brokers)',
              file=sys.stderr, flush=True)
        sys.exit(2)
    b = cluster.brokers[idx]
    mode = 'UNCLEAN (SIGKILL)' if unclean else 'clean (SIGTERM)'
    leaders_pre = describe_leaders(cluster, topics)
    ts = time.strftime('%H:%M:%S')
    print(f'[orch {ts}] leaving broker {idx} ({b.appid}) '
          f'permanently down ({mode})', flush=True)
    b.stop(wait_term=True, force=unclean)
    time.sleep(stop_s)
    leaders_down = describe_leaders(cluster, topics)
    migrated = _diff_leaders(leaders_pre, leaders_down)
    now = time.strftime('%H:%M:%S')
    print(f'[orch {now}] broker {idx} ({b.appid}) is down. '
          f'migrated_away: {_format_leader_diff(migrated)}',
          flush=True)
    if leader_log_path:
        # Open 'w' to truncate any stale log from a prior run; roll()
        # will then append to it instead of truncating again.
        with open(leader_log_path, 'w') as f:
            f.write(f'# chaos harness leader-change log '
                    f'(mode={mode})\n')
            f.write(f'\n## permanently-stopped broker={idx} '
                    f'({b.appid}) ({mode}) at {now}\n')
            f.write(f'  migrated_away: '
                    f'{_format_leader_diff(migrated)}\n')


def roll(cluster, cycles, stop_s, up_s, rng, topics, unclean,
         leader_log_path=None, exclude_indices=(),
         rebalance_add_cycle=None, rebalance_remove_cycle=None,
         consumers=None, spawn_consumer_fn=None):
    """Roll brokers; order is shuffled each cycle for diversity.

    If `unclean` is True, brokers are killed (SIGKILL) instead of being
    asked to shut down gracefully. Around each bounce we sample the
    broker's per-partition leader so we can verify the leader actually
    moved (and later came back).

    `exclude_indices` pins the listed broker indices out of the roll
    (used by `--leave-broker-down` to keep an already-killed broker
    out of the rotation).

    `rebalance_add_cycle` / `rebalance_remove_cycle` (when paired with
    `consumers` + `spawn_consumer_fn`) fire add_consumer at the start
    of the add cycle and remove_consumer at the start of the remove
    cycle, to force a share-group rebalance concurrent with the broker
    roll. The remove picks uniformly at random via `rng`.
    """
    excluded = set(exclude_indices)
    indices = [i for i in range(len(cluster.brokers))
               if i not in excluded]
    mode = 'UNCLEAN (SIGKILL)' if unclean else 'clean (SIGTERM)'
    # When leave_broker_down() ran first it has already truncated the
    # file and written the header; append in that case so we don't
    # clobber its permanently-stopped block.
    if leader_log_path:
        log_mode = 'a' if excluded else 'w'
        leader_log = open(leader_log_path, log_mode)
        if log_mode == 'w':
            leader_log.write(f'# chaos harness leader-change log '
                             f'(mode={mode})\n')
    else:
        leader_log = None

    def _log(line):
        if leader_log:
            leader_log.write(line + '\n')
            leader_log.flush()

    for c in range(cycles):
        if (rebalance_add_cycle == c
                and consumers is not None
                and spawn_consumer_fn is not None):
            new_c = add_consumer(consumers, spawn_consumer_fn)
            ts = time.strftime('%H:%M:%S')
            print(f'[roll {ts}] cycle={c} rebalance-add: '
                  f'spawned consumer-{new_c.idx}', flush=True)
            _log(f'  [{ts}] rebalance-add cycle={c} '
                 f'-> consumer-{new_c.idx}')
        if (rebalance_remove_cycle == c
                and consumers is not None):
            removed = remove_consumer(consumers, rng=rng)
            if removed is not None:
                ts = time.strftime('%H:%M:%S')
                print(f'[roll {ts}] cycle={c} rebalance-remove: '
                      f'stopped consumer-{removed.idx}', flush=True)
                _log(f'  [{ts}] rebalance-remove cycle={c} '
                     f'-> consumer-{removed.idx}')
        order = indices[:]
        rng.shuffle(order)
        ts = time.strftime('%H:%M:%S')
        print(f'[roll {ts}] cycle={c} order={order} mode={mode}',
              flush=True)
        _log(f'\n## cycle {c} order={order} at {ts}')
        for i in order:
            b = cluster.brokers[i]
            leaders_pre = describe_leaders(cluster, topics)

            _roll_log(c, i, b, 'stop')
            if unclean:
                # SIGKILL but still wait until the process is reaped so
                # the port can be reclaimed cleanly on start.
                b.stop(wait_term=True, force=True)
            else:
                b.stop(wait_term=True, force=False)
            _roll_log(c, i, b, 'stopped')

            # Leader migration happens between stop and start; sample
            # while the broker is still down to capture which
            # partitions moved off it.
            time.sleep(stop_s)
            leaders_down = describe_leaders(cluster, topics)
            migrated = _diff_leaders(leaders_pre, leaders_down)
            now = time.strftime('%H:%M:%S')
            print(f'[roll {now}] cycle={c} broker={i} migrated_away: '
                  f'{_format_leader_diff(migrated)}', flush=True)
            _log(f'  [{now}] broker={i} stopped; migrated_away: '
                 f'{_format_leader_diff(migrated)}')

            _roll_log(c, i, b, 'start')
            b.start()
            b.wait_operational(timeout=60)
            _roll_log(c, i, b, 'started')

            # Sample again to see if preferred-replica election (or
            # natural ISR catch-up) moved any leadership back to the
            # restarted broker.
            time.sleep(up_s)
            leaders_back = describe_leaders(cluster, topics)
            came_back = _diff_leaders(leaders_down, leaders_back)
            now = time.strftime('%H:%M:%S')
            print(f'[roll {now}] cycle={c} broker={i} '
                  f'after_start_changes: '
                  f'{_format_leader_diff(came_back)}', flush=True)
            _log(f'  [{now}] broker={i} started; after_start_changes: '
                 f'{_format_leader_diff(came_back)}')

    if leader_log:
        leader_log.close()


def reassign_roll(cluster, cycles, stop_s, up_s, rng, topics, rf,
                  mode, leader_log_path=None):
    """Auto-mode counterpart to `roll()` that triggers leader
    changes via `kafka-reassign-partitions.sh` instead of bouncing
    brokers.

    `mode` is `'change-leader'` (cheap, no data movement) or
    `'reassign-partitions'` (drains replicas, data moves).

    `stop_s` and `up_s` are honoured as dwell windows between
    drains so the run timing stays comparable to broker-roll, even
    though no process is actually stopped.
    """
    indices = list(range(len(cluster.brokers)))
    all_broker_ids = [b.appid for b in cluster.brokers]
    leader_log = open(leader_log_path, 'w') if leader_log_path else None
    if leader_log:
        leader_log.write(
            f'# cluster reassign-roll leader-change log '
            f'(mode={mode})\n')

    def _log(line):
        if leader_log:
            leader_log.write(line + '\n')
            leader_log.flush()

    for c in range(cycles):
        order = indices[:]
        rng.shuffle(order)
        ts = time.strftime('%H:%M:%S')
        print(f'[roll {ts}] cycle={c} order={order} mode={mode}',
              flush=True)
        _log(f'\n## cycle {c} order={order} at {ts}')

        for i in order:
            b = cluster.brokers[i]
            leaders_pre = describe_leaders(cluster, topics)

            now = time.strftime('%H:%M:%S')
            phase = ('reassign-drain' if mode == 'reassign-partitions'
                     else 'change-leader-drain')
            print(f'[roll {now}] cycle={c} broker={i} '
                  f'appid={b.appid} {phase}', flush=True)

            if mode == 'reassign-partitions':
                drain_broker_reassign(cluster, topics, b.appid,
                                      all_broker_ids, rf)
            else:
                drain_broker_change_leader(cluster, topics, b.appid)

            time.sleep(stop_s)
            leaders_after = describe_leaders(cluster, topics)
            diffs = _diff_leaders(leaders_pre, leaders_after)
            now = time.strftime('%H:%M:%S')
            print(f'[roll {now}] cycle={c} broker={i} drained: '
                  f'{_format_leader_diff(diffs)}', flush=True)
            _log(f'  [{now}] broker={i} appid={b.appid} {phase}; '
                 f'drained: {_format_leader_diff(diffs)}')

            # up-dwell: idle window after the drain settles. No
            # "after_start_changes" diff is meaningful since the
            # broker was never down, but we honour the timing so
            # the run length is comparable to broker-roll.
            time.sleep(up_s)

    if leader_log:
        leader_log.close()


# ---------------------------------------------------------------------------
# Log understanding
# ---------------------------------------------------------------------------

# (label, regex). Loose patterns because debug log lines vary by context.
GAP_SIGNATURES = [
    # Stale PARTITION_LEAVE bail-out (rdkafka_broker.c:3541, rktp_broker
    # != rkb). NOT a bug indicator — race-exposure signal. The bail-out
    # is the correct response when a stale LEAVE arrives on a broker
    # that doesn't currently own the rktp; the chained JOIN's F_REMOVE
    # check at broker.c:3419 prevents any ghost-membership outcome.
    # Count = number of times the assignment-shrink-during-migration
    # interleaving fired in this run.
    ('Stale PARTITION_LEAVE bail-out (benign race signal)',
     r'TOPBRK.*ignoring PARTITION_LEAVE: not delegated'),
    # ShareFetch per-partition error names: the wire-protocol error name
    # `NOT_LEADER_FOR_PARTITION` is what appears in logs; librdkafka's
    # internal alias `NOT_LEADER_OR_FOLLOWER` only appears in source.
    # Both are listed so this pattern keeps working across renames.
    ('ShareFetch per-partition leader-change err',
     r'SHAREFETCH.*per-partition fetch error '
     r'(NOT_LEADER_FOR_PARTITION|NOT_LEADER_OR_FOLLOWER|'
     r'FENCED_LEADER_EPOCH|KAFKA_STORAGE_ERROR|'
     r'REPLICA_NOT_AVAILABLE|OFFSET_NOT_AVAILABLE|'
     r'UNKNOWN_TOPIC_OR_PART|UNKNOWN_TOPIC_ID|'
     r'INCONSISTENT_TOPIC_ID)'),
    ('ShareAck per-partition err',
     r'SHAREACK.*partition.*error'),
    ('Top-level session reset (SHARE_SESSION_NOT_FOUND / EPOCH)',
     r'(SHARE_SESSION_NOT_FOUND|INVALID_SHARE_SESSION_EPOCH|'
     r'session_reset)'),
    # Actual log line:
    #   "Ack batch for <topic> [<p>] dropped locally: leader changed
    #    since records were acquired (was X, now Y)"
    # — written by rd_kafka_share_ack_batch_resolve_leader_or_fail_acks.
    ('Stale-leader ack drop (segregate_acks)',
     r'dropped locally: leader changed|response_leader_id'),
    ('INVALID_RECORD_STATE (re-delivery indicator)',
     r'INVALID_RECORD_STATE'),
    ('Top-level metadata refresh on Share* failure',
     r'(FetchRequest failed:|ShareAcknowledge failed:)'),
    # Internal sentinel is printed in logs as `_TRANSPORT` (single
    # underscore) although the C source defines `__TRANSPORT`.
    ('_TRANSPORT (broker connection drop)',
     r'_TRANSPORT|Disconnected: connection closed|Receive failed'),
    ('toppar_leader_unavailable',
     r'leader_unavailable|LEADER_UNAVAIL'),
    ('Metadata refresh triggered',
     r'triggering metadata refresh|broker down'),
]


def iter_log_files(base):
    """Yield log files in chronological order (oldest → newest).

    RotatingFileHandler numbering convention: `base` is the currently
    written file, `base.1` is the most recent backup, `base.N` is the
    oldest. Returning them oldest-first matters for any consumer of
    these files that depends on event order (notably the per-record
    bookkeeping report).
    """
    backups = []
    for i in range(1, 20):
        p = f'{base}.{i}'
        if os.path.exists(p):
            backups.append((i, p))
    # Highest index = oldest; iterate descending so chronologically
    # earliest events come first.
    for _, p in sorted(backups, reverse=True):
        yield p
    if os.path.exists(base):
        yield base


def scan_consumer(consumer, patterns):
    counts = {lbl: 0 for lbl, _ in GAP_SIGNATURES}
    first_ts = {lbl: None for lbl, _ in GAP_SIGNATURES}
    last_ts = {lbl: None for lbl, _ in GAP_SIGNATURES}
    ts_re = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')
    for base in (consumer.stdout_path, consumer.stderr_path):
        for path in iter_log_files(base):
            with open(path, 'r', errors='replace') as f:
                for line in f:
                    for label, pat in patterns:
                        if pat.search(line):
                            counts[label] += 1
                            m = ts_re.match(line)
                            if m:
                                if first_ts[label] is None:
                                    first_ts[label] = m.group(1)
                                last_ts[label] = m.group(1)
    return counts, first_ts, last_ts


def summarize(consumers, out_path):
    patterns = [(label, re.compile(pat)) for label, pat in GAP_SIGNATURES]
    per_consumer = {c.idx: scan_consumer(c, patterns) for c in consumers}

    width = max(len(lbl) for lbl, _ in GAP_SIGNATURES)
    lines = ['Gap-signature summary (per consumer)', '=' * 72]
    for c in consumers:
        counts, first_ts, last_ts = per_consumer[c.idx]
        lines.append(f'\n# consumer-{c.idx}  ({c.dir})')
        for label, _ in GAP_SIGNATURES:
            cnt = counts[label]
            first = first_ts[label] or '-'
            last = last_ts[label] or '-'
            lines.append(f'  {label:<{width}}  count={cnt:<6} '
                         f'first={first}  last={last}')

    lines.append('\n# totals across all consumers')
    for label, _ in GAP_SIGNATURES:
        total = sum(per_consumer[c.idx][0][label] for c in consumers)
        lines.append(f'  {label:<{width}}  count={total}')

    text = '\n'.join(lines) + '\n'
    with open(out_path, 'w') as f:
        f.write(text)
    print('\n' + text)
    print(f'[orch] summary written to {out_path}')


# ---------------------------------------------------------------------------
# Where did each metadata refresh get triggered from?
# ---------------------------------------------------------------------------
#
# librdkafka log lines that trigger a metadata refresh look like:
#
#   %7|...|SHAREFETCH|...| <broker>: <topic> [<p>]: ShareFetch failed:
#   NOT_LEADER_FOR_PARTITION: : triggering metadata refresh
#
#   %7|...|METADATA|...|: ... broker down ...
#
# We want to know which module fired the refresh and what reason
# string accompanied it. This tells us whether refreshes came from
# the share-fetch reply path (gap #1 territory), the broker-fail
# path, the top-level error path, or something else.

_META_MODULE_RE = re.compile(r'\|([A-Z_]+)\|')
# Common reason names we expect to see in trigger lines.
_META_REASON_RE = re.compile(
    r'(NOT_LEADER_FOR_PARTITION|NOT_LEADER_OR_FOLLOWER|'
    r'FENCED_LEADER_EPOCH|UNKNOWN_TOPIC_OR_PART|UNKNOWN_TOPIC_ID|'
    r'INCONSISTENT_TOPIC_ID|KAFKA_STORAGE_ERROR|OFFSET_NOT_AVAILABLE|'
    r'REPLICA_NOT_AVAILABLE|broker down|sharefetch|'
    r'FetchRequest failed|ShareAcknowledge failed)')


def metadata_trigger_report(consumers, out_path):
    """Group metadata-refresh triggers by (module, reason)."""
    counters = {}  # (consumer_idx, module, reason) -> count
    totals = {}    # (module, reason) -> count
    for c in consumers:
        for base in (c.stdout_path, c.stderr_path):
            for path in iter_log_files(base):
                with open(path, 'r', errors='replace') as f:
                    for line in f:
                        if ('triggering metadata refresh' not in line
                                and 'broker down' not in line):
                            continue
                        m = _META_MODULE_RE.search(line)
                        module = m.group(1) if m else 'UNKNOWN'
                        m2 = _META_REASON_RE.search(line)
                        reason = m2.group(1) if m2 else 'OTHER'
                        counters[(c.idx, module, reason)] = (
                            counters.get((c.idx, module, reason), 0) + 1)
                        totals[(module, reason)] = (
                            totals.get((module, reason), 0) + 1)

    lines = ['Metadata refresh trigger sources (per consumer)',
             '=' * 72]
    for c in consumers:
        lines.append(f'\n# consumer-{c.idx}')
        rows = [(m, r, n) for (i, m, r), n in counters.items()
                if i == c.idx]
        if not rows:
            lines.append('  (no metadata refresh triggers)')
        for module, reason, n in sorted(rows, key=lambda x: -x[2]):
            lines.append(f'  {module:<14}  reason={reason:<28}  '
                         f'count={n}')

    lines.append('\n# totals across all consumers')
    if not totals:
        lines.append('  (no metadata refresh triggers)')
    for (module, reason), n in sorted(totals.items(),
                                      key=lambda x: -x[1]):
        lines.append(f'  {module:<14}  reason={reason:<28}  count={n}')

    text = '\n'.join(lines) + '\n'
    with open(out_path, 'w') as f:
        f.write(text)
    print('\n' + text)
    print(f'[orch] metadata-trigger report written to {out_path}')


# ---------------------------------------------------------------------------
# Conservation check (Option A from broker-movement analysis)
# ---------------------------------------------------------------------------
#
# rdkafka_performance prints one summary line on exit (see print_stats with
# _OTYPE_FORCE at end of main):
#
#   Producer (stdout):
#     % <produced> messages produced (<bytes> bytes), <delivered> delivered
#     (offset <X>, <failed> failed) in <ms>ms: ...
#
#   Consumer (stdout):
#     % <consumed> messages (<bytes> bytes) consumed in <ms>ms: ...
#
# Multiple summary lines may appear in the log (-T stats interval + the
# forced one at exit). We take the LAST match per file so the final tally
# is what gets compared.

_PRODUCER_RE = re.compile(
    r'% (\d+) messages produced .*?(\d+) delivered')
_CONSUMER_RE = re.compile(
    r'% (\d+) messages \(\d+ bytes\) consumed')


def _scan_last_match(base_path, pattern):
    """Return last regex match's groups across base_path + rotated siblings."""
    last = None
    for path in iter_log_files(base_path):
        with open(path, 'r', errors='replace') as f:
            for line in f:
                m = pattern.search(line)
                if m:
                    last = m.groups()
    return last


def _scan_producer_cumulative(prod):
    """Walk one producer's stdout/rotated files and sum produced /
    delivered counts across producer-process restarts. Each
    rdkafka_performance process emits monotonically-increasing
    counters per session; on restart the counter drops to ~0. We
    track a running max per session and lock it in when we see a
    fresh-start signature (large drop to a value below ~1000).

    A small late drop is NOT a restart — rdkafka_performance also
    emits a final exit-summary line where the `messages produced`
    field reports the cumulative delivered count rather than the
    cumulative produced count, so the number can be a few records
    smaller than the previous periodic line. Treating that as a
    restart would double-count the session.

    Returns (produced, delivered) or (None, None) if no stats lines
    were seen."""
    total_produced = 0
    total_delivered = 0
    session_max_p = 0
    session_max_d = 0
    any_found = False
    for path in iter_log_files(prod.stdout_path):
        with open(path, 'r', errors='replace') as f:
            for line in f:
                m = _PRODUCER_RE.search(line)
                if not m:
                    continue
                any_found = True
                p = int(m.group(1))
                d = int(m.group(2))
                # Real restart: counter drops to a fresh-start value
                # (below ~1000) AND well below the running max.
                if (p < session_max_p // 2 and p < 1000
                        and session_max_p >= 1000):
                    total_produced += session_max_p
                    total_delivered += session_max_d
                    session_max_p = p
                    session_max_d = d
                else:
                    if p > session_max_p:
                        session_max_p = p
                    if d > session_max_d:
                        session_max_d = d
    if not any_found:
        return None, None
    total_produced += session_max_p
    total_delivered += session_max_d
    return total_produced, total_delivered


def _sum_producer_counts(producers):
    """Walk every producer's stdout for the final 'N messages
    produced ... D delivered' line and sum across producers,
    handling per-producer restarts. Returns (produced, delivered)
    or (None, None) if no producer summary was found."""
    total_produced = 0
    total_delivered = 0
    any_found = False
    for prod in producers:
        p, d = _scan_producer_cumulative(prod)
        if p is not None:
            total_produced += p
            total_delivered += d
            any_found = True
    if not any_found:
        return None, None
    return total_produced, total_delivered


def conservation_report(producers, consumers, out_path):
    produced, delivered = _sum_producer_counts(producers)

    per_consumer = {}
    for c in consumers:
        groups = _scan_last_match(c.stdout_path, _CONSUMER_RE)
        per_consumer[c.idx] = int(groups[0]) if groups else None

    total_consumed = sum(v for v in per_consumer.values() if v is not None)

    lines = ['Conservation check', '=' * 60]
    if produced is None:
        lines.append('PRODUCER: no summary line found in stdout')
    else:
        lines.append(f'producer produced  : {produced}')
        lines.append(f'producer delivered : {delivered}')
    for c in consumers:
        v = per_consumer[c.idx]
        lines.append(f'consumer-{c.idx} consumed : '
                     f'{v if v is not None else "(no summary)"}')
    lines.append(f'sum across consumers: {total_consumed}')

    # Verdict
    lines.append('')
    if delivered is None or total_consumed == 0:
        lines.append('VERDICT: unable to evaluate (missing summary lines)')
    elif total_consumed < delivered:
        lines.append(f'VERDICT: DATA LOSS — consumed {total_consumed} '
                     f'< delivered {delivered} (missing '
                     f'{delivered - total_consumed})')
    elif total_consumed > 2 * delivered:
        lines.append(f'VERDICT: EXCESSIVE DUPLICATION — consumed '
                     f'{total_consumed} > 2x delivered {delivered}')
    else:
        dup_ratio = total_consumed / delivered
        lines.append(f'VERDICT: OK — consumed/delivered = {dup_ratio:.3f} '
                     f'(>=1.0 expected; tail above 1.0 = re-deliveries)')

    text = '\n'.join(lines) + '\n'
    with open(out_path, 'w') as f:
        f.write(text)
    print('\n' + text)
    print(f'[orch] conservation report written to {out_path}')


# ---------------------------------------------------------------------------
# Partition coverage during observation window
# ---------------------------------------------------------------------------
#
# librdkafka stats JSON layout (the bits we care about):
#
#   {
#     "ts": <int>,
#     "topics": {
#        "<topic>": {
#           "partitions": {
#               "<partition>": {"partition": <int>, "rxmsgs": <int>, ...},
#               ...
#           }
#        }, ...
#     }, ...
#   }
#
# `rxmsgs` is cumulative messages delivered to the application for that
# partition on that consumer. We want: per (topic, partition), did the
# sum across all consumers grow during the observation window? If not,
# that partition was orphaned by the cluster roll.


def _extract_rxmsgs(blob, topics):
    """Return {(topic, partition): rxmsgs} from a librdkafka stats blob.

    Only the partitions for the topics we care about are returned.
    Missing entries default to 0 so diff arithmetic stays simple.
    """
    out = {}
    if not blob:
        return out
    topics_obj = blob.get('topics') or {}
    for t in topics:
        t_obj = topics_obj.get(t) or {}
        parts = t_obj.get('partitions') or {}
        for pid_str, p_obj in parts.items():
            try:
                pid = int(pid_str)
            except ValueError:
                continue
            if pid < 0:  # librdkafka uses -1 for the "internal" entry
                continue
            out[(t, pid)] = int(p_obj.get('rxmsgs', 0))
    return out


def partition_coverage_report(consumers, topics, partitions_per_topic,
                              out_path):
    expected = [(t, p) for t in topics
                for p in range(partitions_per_topic)]

    # Aggregate rxmsgs at pre-obs and post-obs across all consumers.
    pre = {key: 0 for key in expected}
    post = {key: 0 for key in expected}
    per_consumer_post = {c.idx: {} for c in consumers}

    for c in consumers:
        pre_blob = c.pre_obs_blob
        post_blob = c.post_obs_blob()
        pre_part = _extract_rxmsgs(pre_blob, topics)
        post_part = _extract_rxmsgs(post_blob, topics)
        per_consumer_post[c.idx] = post_part
        for key in expected:
            pre[key] += pre_part.get(key, 0)
            post[key] += post_part.get(key, 0)

    lines = ['Partition coverage during observation window (cooldown+drain)',
             '=' * 72,
             'For each (topic, partition): rxmsgs summed across consumers.',
             'diff = rxmsgs(end) - rxmsgs(pre-observation).',
             '']

    orphaned_in_window = []  # diff == 0
    never_consumed = []      # post == 0
    for key in expected:
        d = post[key] - pre[key]
        marker = ''
        if post[key] == 0:
            marker = '   <-- NEVER CONSUMED'
            never_consumed.append(key)
        elif d == 0:
            marker = '   <-- ORPHANED IN OBSERVATION WINDOW'
            orphaned_in_window.append(key)
        lines.append(f'  {key[0]} [{key[1]}]: pre={pre[key]} '
                     f'post={post[key]} diff={d}{marker}')

    lines.append('')
    lines.append('Per-consumer end-of-run rxmsgs (sanity):')
    for c in consumers:
        bits = [f'{t}[{p}]={per_consumer_post[c.idx].get((t, p), 0)}'
                for (t, p) in expected]
        lines.append(f'  consumer-{c.idx}: ' + ' '.join(bits))

    lines.append('')
    if not expected:
        lines.append('VERDICT: no topics/partitions to check')
    elif never_consumed:
        lines.append(f'VERDICT: FAIL — {len(never_consumed)} partition(s) '
                     f'NEVER consumed any record: {never_consumed}')
    elif orphaned_in_window:
        lines.append(f'VERDICT: FAIL — {len(orphaned_in_window)} '
                     f'partition(s) consumed nothing during the '
                     f'observation window: {orphaned_in_window}')
    else:
        lines.append(f'VERDICT: OK — all {len(expected)} partition(s) '
                     f'saw new records during the observation window')

    text = '\n'.join(lines) + '\n'
    with open(out_path, 'w') as f:
        f.write(text)
    print('\n' + text)
    print(f'[orch] partition coverage written to {out_path}')


# ---------------------------------------------------------------------------
# Per-record bookkeeping (Option B verification)
# ---------------------------------------------------------------------------
#
# The share_consume_verify binary emits one JSON event per consumed
# record and one per acked record on its stdout:
#
#   {"e":"consumed","t":"<topic>","p":<part>,"o":<offset>,"dc":<broker_dc>}
#   {"e":"acked","t":"<topic>","p":<part>,"o":<offset>,"err":<resp_err_int>}
#
# We aggregate across consumers (a record can move between consumers in
# the share group across reassignments) and report:
#
#   * dc_distribution[k] = #(t,p,o) acked after being delivered k times
#                          (k = max broker-side delivery_count across all
#                          consume events for that record)
#   * pending           = (t,p,o) consumed at least once but never acked
#                         (the smoking gun for data loss)
#   * ack_errors        = per-error-code count of failed acks
#   * orphan_acks       = ack events with no prior local 'consumed'

_LINE_TS_RE = re.compile(
    r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d+)')


def _parse_ts(line):
    """Return epoch-seconds (float) for the orchestrator-prefixed log
    line, or None if no timestamp present."""
    m = _LINE_TS_RE.match(line)
    if not m:
        return None
    try:
        t = time.strptime(m.group(1), '%Y-%m-%d %H:%M:%S')
        return time.mktime(t) + int(m.group(2)) / 1000.0
    except (ValueError, OverflowError):
        return None


def _iter_event_lines(consumer):
    """Yield (epoch_seconds_or_None, decoded_json_dict) per stdout line."""
    for path in iter_log_files(consumer.stdout_path):
        with open(path, 'r', errors='replace') as f:
            for line in f:
                idx = line.find('{')
                if idx < 0:
                    continue
                ts = _parse_ts(line[:idx])
                try:
                    ev = json.loads(line[idx:])
                except (json.JSONDecodeError, ValueError):
                    continue
                yield ts, ev


def bookkeeping_report(producers, consumers, topics,
                       partitions_per_topic,
                       hwms, drain_end_ts, out_path,
                       chaos_state=None):
    import collections
    # Cross-consumer state keyed by (topic, partition, offset).
    #
    # record_events[key] keeps the FULL trail of deliveries and ack
    # callbacks for that record across all redeliveries (broker
    # caps redeliveries at share.delivery.count.limit, default 5).
    #
    # Classification rules at end of run:
    # - never_acked: 'consumed' event(s) AND no 'acked' event of any
    #   kind. Consumer-pipeline data loss — callback never fired so
    #   the consumer never confirmed completion of the ack flow.
    #   Drives VERDICT: FAIL.
    # - acked_ok: at least one 'acked' event with err == 0. Broker
    #   definitely received and processed the ack at some point.
    # - acked_with_err: at least one 'acked' event, but every
    #   callback had err != 0. Broker MAY have received the ack
    #   (response lost in transit) OR will redeliver on lock
    #   expiry. NOT consumer-side loss; reported as ambiguous.
    #
    # ack_errors_total: histogram of all err codes seen in any ack
    # callback (across all deliveries).
    # record_events: key -> {'consumes': [(dc, ts), ...],
    #                         'acks':     [(err, ts), ...]}
    record_events = {}
    ack_errors_total = collections.Counter()
    orphan_acks = 0
    consumed_per_part = collections.Counter()   # (t, p) -> consume events
    delivered_per_consumer = collections.Counter()  # consumer_idx -> events

    # Per-(topic, partition) first/last consume timestamps
    first_consume_ts = {}
    last_consume_ts = {}

    # Topic-chaos bucketing: count consume events per topic that fall
    # before the chaos delete (pre-delete generation) vs after the
    # recreate (new generation). Events with ts in the delete/recreate
    # gap are limbo and excluded from both counts. Display-only;
    # boundary jitter can move a few events between buckets without
    # affecting correctness — the real verdict uses distinct-offset
    # coverage per (topic_id, partition) below.
    chaos_state = chaos_state or {}
    consumed_pre_delete_per_topic = collections.Counter()
    consumed_post_recreate_per_topic = collections.Counter()
    # Distinct offsets consumed per (topic, topic_id, partition) across
    # all consumers. Used to check post-recreate coverage — the real
    # FAIL signal under --topic-chaos.
    chaos_offsets_by_id_part = collections.defaultdict(
        lambda: collections.defaultdict(set))

    for c in consumers:
        for ts, ev in _iter_event_lines(c):
            e = ev.get('e')
            t = ev.get('t')
            p = ev.get('p')
            o = ev.get('o')
            if not e or t is None or p is None or o is None:
                continue
            # Topic id is a base64 string snapshot from
            # rd_kafka_topic_id() taken at consume time, emitted by
            # share_consume_verify. With --topic-chaos recreate, the
            # OLD and NEW topic generations both restart from offset
            # 0; including the id in the distinct-key keeps the two
            # generations apart. '' is fine as a fallback for legacy
            # events that didn't carry an id field.
            tid = ev.get('id', '')
            key = (t, tid, p, o)
            tp = (t, p)
            if e == 'consumed':
                d = int(ev.get('dc', 1))
                rec = record_events.setdefault(
                    key, {'consumes': [], 'acks': []})
                rec['consumes'].append((d, ts))
                consumed_per_part[tp] += 1
                delivered_per_consumer[c.idx] += 1
                if ts is not None:
                    if tp not in first_consume_ts:
                        first_consume_ts[tp] = ts
                    last_consume_ts[tp] = ts
                cs = chaos_state.get(t)
                if cs is not None:
                    chaos_offsets_by_id_part[t][(tid, int(p))].add(
                        int(o))
                    if ts is not None:
                        if ts < cs['delete_ts']:
                            consumed_pre_delete_per_topic[t] += 1
                        elif (cs.get('recreate_ts')
                              and ts > cs['recreate_ts']):
                            consumed_post_recreate_per_topic[t] += 1
            elif e == 'acked':
                err = int(ev.get('err', 0))
                rec = record_events.get(key)
                if rec is None:
                    orphan_acks += 1
                    continue
                rec['acks'].append((err, ts))
                if err != 0:
                    ack_errors_total[err] += 1

    dc_distribution = collections.Counter()
    never_acked = []          # (key, max_dc) — consumed, zero ack events
    acked_with_err = []       # (key, max_dc, [errs]) — all acks errored
    acked_ok_keys = set()
    for key, rec in record_events.items():
        if not rec['consumes']:
            # 'acked' arrived with no 'consumed' (would have been
            # counted as orphan_acks above; defensive).
            continue
        max_dc = max(d for d, _ in rec['consumes'])
        ack_errs = [e for e, _ in rec['acks']]
        if not ack_errs:
            never_acked.append((key, max_dc))
        elif any(e == 0 for e in ack_errs):
            acked_ok_keys.add(key)
            dc_distribution[max_dc] += 1
        else:
            acked_with_err.append((key, max_dc, ack_errs))

    total_consumed_distinct = len(record_events)
    total_consumed_events = sum(consumed_per_part.values())
    total_acked = len(acked_ok_keys)
    total_never_acked = len(never_acked)
    total_ack_with_err = len(acked_with_err)

    # Producer-side count: sum `produced` + `delivered` across every
    # producer's final "% N messages produced ... D delivered" line.
    prod_sum = _sum_producer_counts(producers)
    if prod_sum and prod_sum[0] is not None:
        produced, delivered = prod_sum
    else:
        produced = delivered = None

    lines = ['Per-record verification (Option B bookkeeping)',
             '=' * 72]
    if delivered is None:
        lines.append('producer summary missing from log; cannot '
                     'cross-check produced vs consumed')
    else:
        lines.append(f'producer produced          : {produced}')
        lines.append(f'producer delivered (broker): {delivered}')
    lines.append(f'distinct records consumed : {total_consumed_distinct}')
    lines.append(f'total consume events       : {total_consumed_events}')
    lines.append(f'distinct records acked OK  : {total_acked}'
                 f'   (>=1 ack callback with err=0)')
    lines.append(f'ack-with-err only          : {total_ack_with_err}'
                 f'   (>=1 ack callback, all err!=0; ambiguous, '
                 f'NOT data loss)')
    lines.append(f'never-acked (no callback)  : {total_never_acked}'
                 f'   <-- effective data loss (consumer-side)')
    lines.append(f'orphan acks (ack w/o cons.): {orphan_acks}')
    if delivered is not None:
        # Use total consume events (not distinct keys) so the count
        # is correct under --topic-chaos: OLD and NEW topic-id
        # generations both restart from offset 0, so distinct keys
        # under-counts. Each consume event represents one delivery
        # from broker to consumer.
        missing = delivered - total_consumed_events
        lines.append(f'never-consumed             : {missing}'
                     f'   <-- delivered by broker but no consume '
                     f'event seen')
    lines.append('')

    lines.append('Delivery-count distribution (k = max broker dc, '
                 'only records eventually acked OK):')
    for k in sorted(dc_distribution):
        lines.append(f'  dc[{k}] = {dc_distribution[k]}  '
                     f'records acked after {k} delivery'
                     f'{"s" if k != 1 else ""}')

    if ack_errors_total:
        lines.append('')
        lines.append('Ack callback errors (per-code, ALL callbacks '
                     'across redeliveries):')
        for code, n in sorted(ack_errors_total.items(),
                              key=lambda x: -x[1]):
            lines.append(f'  err={code}  count={n}')

    if never_acked:
        lines.append('')
        lines.append(f'Sample of {min(15, len(never_acked))} '
                     f'never-acked records (real loss):')
        # key = (topic, topic_id_b64, partition, offset)
        for (k, d) in never_acked[:15]:
            lines.append(f'  {k[0]}[{k[2]}] offset={k[3]} '
                         f'id={k[1] or "-"} last_dc={d}')

    if acked_with_err:
        # Full trail for every record whose ack history had any
        # non-zero err. Per user request: print all errored ones,
        # not just a sample, so we can audit each ack flow.
        lines.append('')
        lines.append(f'Per-record ack trail for {len(acked_with_err)} '
                     f'records with err in their ack history '
                     f'(ambiguous, not loss):')
        lines.append('  format: t[p] offset=O id=<id> '
                     'consumes=[dc1,dc2,...] acks=[err1,err2,...]')
        # key = (topic, topic_id_b64, partition, offset)
        for (k, _max_dc, errs) in acked_with_err:
            rec = record_events[k]
            consumes_fmt = '[' + ','.join(
                str(d) for d, _ in rec['consumes']) + ']'
            acks_fmt = '[' + ','.join(
                str(e) for e, _ in rec['acks']) + ']'
            lines.append(f'  {k[0]}[{k[2]}] offset={k[3]} '
                         f'id={k[1] or "-"} '
                         f'consumes={consumes_fmt} acks={acks_fmt}')

    lines.append('')
    lines.append('Per-consumer consume-event totals:')
    for c in consumers:
        lines.append(f'  consumer-{c.idx}: '
                     f'{delivered_per_consumer.get(c.idx, 0)}')

    lines.append('')
    lines.append('Per-(topic, partition) coverage:')
    lines.append('  format: consumed | broker_hwm | missing | '
                 'first_consume_at | last_consume_at | '
                 'sec_idle_at_drain_end')
    expected = [(t, p) for t in topics
                for p in range(partitions_per_topic)]
    total_partition_missing = 0
    for tp in expected:
        cnt = consumed_per_part.get(tp, 0)
        hwm = hwms.get(tp) if hwms else None
        missing_str = '?'
        if hwm is not None:
            missing = hwm - cnt
            total_partition_missing += max(missing, 0)
            missing_str = str(missing)

        def _fmt_ts(ts):
            if ts is None:
                return '-'
            return time.strftime('%H:%M:%S', time.localtime(ts))

        first = _fmt_ts(first_consume_ts.get(tp))
        last = _fmt_ts(last_consume_ts.get(tp))
        idle = '-'
        if drain_end_ts is not None and tp in last_consume_ts:
            idle = f'{drain_end_ts - last_consume_ts[tp]:.1f}'

        marker = ''
        if cnt == 0:
            marker = '  <-- NEVER CONSUMED (orphan partition)'
        elif hwm is not None and (hwm - cnt) > 0:
            marker = '  <-- under-consumed'
        lines.append(f'  {tp[0]}[{tp[1]}] '
                     f'consumed={cnt} hwm={hwm if hwm is not None else "?"}'
                     f' missing={missing_str} '
                     f'first={first} last={last} '
                     f'idle@drain_end={idle}s{marker}')

    if hwms:
        lines.append(f'\nSum missing across partitions: '
                     f'{total_partition_missing} (broker hwm − '
                     f'consume events)')

    # Topic-chaos accounting: for each topic that was deleted (and
    # optionally re-created) mid-run, compute the expected-loss
    # window so the final VERDICT can ignore noise from records that
    # were on the broker at delete time and gone afterwards. For
    # recreate modes, also check post-recreate coverage per
    # (topic_id, partition) — the new generation's [0..hwm-1] must
    # be fully consumed across the share group; a gap there IS a
    # library bug, not chaos noise.
    chaos_expected_loss = 0
    chaos_per_partition_loss = []
    if chaos_state:
        lines.append('')
        lines.append('Topic-chaos accounting:')
        lines.append('  Per chaos\'d topic: pre_delete_hwm is the '
                     'broker offset sum at delete time; post_hwm is '
                     'the sum at end-of-test (== post-recreate '
                     'generation, since old data is gone). Expected '
                     'loss = pre_delete_hwm − consumed_before_delete. '
                     'Post-recreate coverage is checked per '
                     '(topic_id, partition) — see lines below.')
        for t, cs in sorted(chaos_state.items()):
            pre_hwm = sum(cs['pre_delete_hwm'].values()) \
                if cs.get('pre_delete_hwm') else 0
            post_hwm = sum(h for (tt, _p), h in hwms.items()
                           if tt == t) if hwms else 0
            pre_consumed = consumed_pre_delete_per_topic.get(t, 0)
            post_consumed = consumed_post_recreate_per_topic.get(t, 0)
            expected_loss_t = max(0, pre_hwm - pre_consumed)
            chaos_expected_loss += expected_loss_t
            lines.append(f'  {t} mode={cs["mode"]} '
                         f'pre_delete_hwm={pre_hwm} '
                         f'consumed_pre_delete={pre_consumed} '
                         f'expected_loss={expected_loss_t} '
                         f'post_hwm={post_hwm} '
                         f'consumed_post_recreate~={post_consumed} '
                         f'(events; boundary-jittery, display only)')
            if cs['mode'] in ('recreate-immediate',
                              'recreate-delayed') \
                    and cs.get('recreate_ts'):
                # Per-partition coverage check: for each partition,
                # find the topic_id whose distinct-offset set has the
                # largest max offset (= new generation) and verify
                # it covers [0..hwm-1]. Aggregating across consumers
                # is what catches records consumed by a re-joining
                # consumer during the share-group rebalance.
                topic_offsets = chaos_offsets_by_id_part.get(t, {})
                partitions = sorted({p for (_tid, p)
                                     in topic_offsets})
                cov_lines = []
                for p in partitions:
                    hwm_p = hwms.get((t, p)) if hwms else None
                    if hwm_p is None or hwm_p == 0:
                        continue
                    candidates = [(tid, s) for (tid, pp), s in
                                  topic_offsets.items() if pp == p]
                    if not candidates:
                        chaos_per_partition_loss.append(
                            (t, p, hwm_p, []))
                        cov_lines.append(
                            f'    [{p}] hwm={hwm_p} '
                            f'covered=0 missing={hwm_p} '
                            f'(no consumer events)')
                        continue
                    new_tid, new_set = max(
                        candidates,
                        key=lambda x: (max(x[1]) if x[1] else -1))
                    expected = set(range(hwm_p))
                    missing = sorted(expected - new_set)
                    cov_lines.append(
                        f'    [{p}] hwm={hwm_p} '
                        f'covered={len(new_set & expected)} '
                        f'missing={len(missing)}')
                    if missing:
                        runs = []
                        start = missing[0]
                        prev = start
                        for x in missing[1:]:
                            if x == prev + 1:
                                prev = x
                            else:
                                runs.append((start, prev))
                                start = x
                                prev = x
                        runs.append((start, prev))
                        chaos_per_partition_loss.append(
                            (t, p, len(missing), runs[:3]))
                if cov_lines:
                    lines.append('  per-partition new-generation '
                                 'coverage (across all consumers):')
                    lines.extend(cov_lines)
        lines.append(f'  total expected_loss across chaos\'d '
                     f'topics: {chaos_expected_loss}')

    lines.append('')
    if total_consumed_distinct == 0:
        lines.append('VERDICT: no events parsed (workload not in '
                     'verify mode?)')
    else:
        # Sub-verdicts. Real loss = consume event with no ack
        # callback ever. ack_with_err is ambiguous (broker may have
        # processed or will redeliver) and does NOT fail the run.
        consumer_loss_ok = (total_never_acked == 0)
        # Compare producer.delivered against TOTAL consume events
        # (not distinct keys). With --topic-chaos recreate, the
        # OLD and NEW topic generations both restart from offset 0
        # so their (t,p,o) keys collide and total_consumed_distinct
        # under-counts. total_consumed_events is the right measure
        # — each consume event represents one record delivered from
        # broker to consumer, regardless of generation. Redeliveries
        # (dc[2+]) only inflate events, never decrease them, so the
        # >= check stays correct without chaos too.
        # Topic-chaos expected loss is subtracted from the raw
        # delivered-vs-consumed diff so chaos noise doesn't trip
        # FAIL on its own.
        if delivered is None:
            delivery_ok = None
            effective_missing = 0
        else:
            raw_missing = delivered - total_consumed_events
            effective_missing = raw_missing - chaos_expected_loss
            delivery_ok = (effective_missing <= 0)

        ack_with_err_note = ''
        if total_ack_with_err > 0:
            ack_with_err_note = (
                f' [ack-with-err: {total_ack_with_err} '
                f'records had err in ack history but at least one '
                f'callback fired — ambiguous, not loss]')

        chaos_note = ''
        if chaos_state and chaos_expected_loss > 0:
            chaos_note = (f' [topic-chaos expected_loss='
                          f'{chaos_expected_loss} subtracted]')

        if chaos_per_partition_loss:
            details_parts = []
            for t, p, n_missing, runs in chaos_per_partition_loss:
                if runs:
                    run_str = ','.join(f'[{a}..{b}]'
                                       for a, b in runs)
                    details_parts.append(
                        f'{t}[{p}]:{n_missing} (runs: {run_str})')
                else:
                    details_parts.append(f'{t}[{p}]:{n_missing}')
            details = ', '.join(details_parts)
            lines.append(f'VERDICT: FAIL — post-recreate '
                         f'new-generation coverage gap on chaos\'d '
                         f'topic(s): {details} (the broker has these '
                         f'offsets but no consumer in the share '
                         f'group ever consumed them — library bug, '
                         f'not chaos noise)')
        elif consumer_loss_ok and delivery_ok is True:
            tail = (sorted(dc_distribution.items())[-1]
                    if dc_distribution else 'n/a')
            lines.append(f'VERDICT: OK — produced={delivered} '
                         f'consume_events={total_consumed_events} '
                         f'(distinct={total_consumed_distinct}) '
                         f'acked={total_acked} '
                         f'(dc tail: {tail}){ack_with_err_note}'
                         f'{chaos_note}')
        elif delivery_ok is False:
            lines.append(f'VERDICT: FAIL — {effective_missing} '
                         f'record(s) lost beyond the expected '
                         f'chaos window (produced={delivered} '
                         f'consume_events={total_consumed_events} '
                         f'chaos_expected_loss={chaos_expected_loss}'
                         f')')
        elif consumer_loss_ok and delivery_ok is None:
            tail = (sorted(dc_distribution.items())[-1]
                    if dc_distribution else 'n/a')
            lines.append(f'VERDICT: OK (consumer-only) — every '
                         f'consumed record received at least one '
                         f'ack callback (dc tail: {tail}). Producer '
                         f'summary unavailable, could not '
                         f'cross-check.{ack_with_err_note}')
        elif total_never_acked > 0:
            lines.append(f'VERDICT: FAIL — {total_never_acked} '
                         f'record(s) consumed but no ack callback '
                         f'fired (real consumer-pipeline loss)')
        elif orphan_acks > 0:
            lines.append(f'VERDICT: ORPHAN ACKS — {orphan_acks} '
                         f'ack(s) fired without matching consume; '
                         f'book-keeping inconsistency')

    text = '\n'.join(lines) + '\n'
    with open(out_path, 'w') as f:
        f.write(text)
    print('\n' + text)
    print(f'[orch] bookkeeping report written to {out_path}')


# ---------------------------------------------------------------------------
# Background leader watcher
# ---------------------------------------------------------------------------

def _leader_watcher(cluster, topics, stop_event, poll_s, log_path):
    """Poll `describe_leaders` every `poll_s` seconds, print every
    observed leader transition and append it to `log_path`.

    Catches leadership changes that happen between (or outside) our
    explicit operations: preferred-leader timer firing,
    ISR-shrinkage-driven re-election, etc. Independent of broker-roll
    / reassign cycles."""
    prev = None
    log_file = open(log_path, 'w') if log_path else None
    if log_file:
        log_file.write('# leader history (live per-partition '
                       'transitions during the run)\n')
        log_file.flush()
    while not stop_event.is_set():
        try:
            current = describe_leaders(cluster, topics)
        except Exception:
            stop_event.wait(poll_s)
            continue
        if prev is not None and current:
            diffs = _diff_leaders(prev, current)
            if diffs:
                ts = time.strftime('%H:%M:%S')
                msg = (f'[leader {ts}] '
                       f'{_format_leader_diff(diffs)}')
                print(msg, flush=True)
                if log_file:
                    log_file.write(msg + '\n')
                    log_file.flush()
        if current:
            prev = current
        stop_event.wait(poll_s)
    if log_file:
        log_file.close()


def start_leader_watcher(cluster, topics, poll_s, log_dir):
    """Start a daemon thread that polls leader state every poll_s
    seconds. Returns (stop_event, thread) — caller must set
    stop_event and join the thread at teardown.

    Returns (None, None) when poll_s <= 0."""
    if poll_s <= 0:
        return None, None
    stop_event = threading.Event()
    log_path = os.path.join(log_dir, 'leader_history.log')
    t = threading.Thread(
        target=_leader_watcher,
        args=(cluster, topics, stop_event, poll_s, log_path),
        daemon=True)
    t.start()
    print(f'[orch] leader watcher polling every {poll_s}s '
          f'(history: {log_path})', flush=True)
    return stop_event, t


# Random-timing windows for --topic-chaos. Tweakable here rather
# than via the CLI to keep the flag surface minimal.
_TOPIC_CHAOS_FIRE_MIN_S = 15
_TOPIC_CHAOS_FIRE_MAX_S = 45
_TOPIC_CHAOS_DELAYED_DWELL_MIN_S = 10
_TOPIC_CHAOS_DELAYED_DWELL_MAX_S = 30


def _topic_chaos_thread(cluster, topics, partitions, replication,
                        mode, rng, stop_event, leader_log_path,
                        chaos_state, restart_producer_fn):
    fire_after = rng.uniform(_TOPIC_CHAOS_FIRE_MIN_S,
                             _TOPIC_CHAOS_FIRE_MAX_S)
    if stop_event.wait(fire_after):
        return
    if not topics:
        return
    topic = rng.choice(topics)
    # Capture pre-delete HWM so the verify report can compute the
    # expected loss window for this topic.
    pre_delete_hwm = {}
    try:
        all_hwms = get_partition_hwms(cluster, [topic])
        pre_delete_hwm = {p: h for (t, p), h in all_hwms.items()
                          if t == topic}
    except Exception as e:
        print(f'[topic-chaos] pre-delete HWM capture failed: {e}',
              flush=True)
    delete_ts = time.time()
    chaos_state[topic] = {
        'mode': mode,
        'delete_ts': delete_ts,
        'recreate_ts': None,
        'pre_delete_hwm': pre_delete_hwm,
    }
    ts = time.strftime('%H:%M:%S')
    pre_hwm_sum = sum(pre_delete_hwm.values()) if pre_delete_hwm else 0
    print(f'[topic-chaos {ts}] mode={mode} target={topic} '
          f'(fired after {fire_after:.1f}s, '
          f'pre-delete-hwm-sum={pre_hwm_sum})', flush=True)
    if leader_log_path:
        with open(leader_log_path, 'a') as f:
            f.write(f'\n## topic-chaos [{ts}] mode={mode} '
                    f'target={topic} fire_after={fire_after:.1f}s '
                    f'pre_delete_hwm_sum={pre_hwm_sum}\n')
    try:
        if mode == 'delete':
            delete_topic(cluster, topic)
            # No restart: topic stays gone.
        elif mode == 'recreate-immediate':
            recreate_topic(cluster, topic, partitions, replication,
                           dwell_s=0)
            chaos_state[topic]['recreate_ts'] = time.time()
            if restart_producer_fn is not None:
                restart_producer_fn(topic)
        elif mode == 'recreate-delayed':
            dwell = rng.uniform(_TOPIC_CHAOS_DELAYED_DWELL_MIN_S,
                                _TOPIC_CHAOS_DELAYED_DWELL_MAX_S)
            if stop_event.is_set():
                return
            recreate_topic(cluster, topic, partitions, replication,
                           dwell_s=dwell)
            chaos_state[topic]['recreate_ts'] = time.time()
            if restart_producer_fn is not None:
                restart_producer_fn(topic)
    except Exception as e:
        print(f'[topic-chaos] action failed: {e}', flush=True)


def start_topic_chaos(cluster, topics, partitions, replication,
                      mode, rng, leader_log_path, chaos_state,
                      restart_producer_fn):
    """Start a daemon thread that, after a random delay, executes the
    chosen topic-chaos action on a randomly picked topic. Returns
    (stop_event, thread). `mode` is one of 'delete',
    'recreate-immediate', 'recreate-delayed' (or None to disable).

    On recreate modes, `restart_producer_fn(topic)` is invoked after
    the re-create completes so the producer rebinds to the new
    topic_id (rdkafka_performance exits on UNKNOWN_TOPIC and won't
    self-recover). `chaos_state` is populated with the chaos
    timestamps + pre-delete HWM snapshot so the verify report can
    bucket consume events and compute the expected loss window."""
    if mode is None:
        return None, None
    stop_event = threading.Event()
    t = threading.Thread(
        target=_topic_chaos_thread,
        args=(cluster, topics, partitions, replication, mode, rng,
              stop_event, leader_log_path, chaos_state,
              restart_producer_fn),
        daemon=True)
    t.start()
    print(f'[orch] topic-chaos thread armed (mode={mode}, '
          f'fire in {_TOPIC_CHAOS_FIRE_MIN_S}-'
          f'{_TOPIC_CHAOS_FIRE_MAX_S}s)', flush=True)
    return stop_event, t


# ---------------------------------------------------------------------------
# Manual mode: just cluster + REPL, no built-in workload
# ---------------------------------------------------------------------------

def _run_manual_mode(args, cluster_conf):
    """Bring up the cluster, optionally pre-create topics, print
    bootstrap servers, and drop the user into the broker REPL. The
    orchestrator does NOT spawn any producer or consumer in this mode;
    the user runs their own workload (e.g. their own script) against
    the printed bootstrap from another shell.
    """
    cluster = LibrdkafkaTestCluster(
        version=args.version, conf=cluster_conf,
        num_brokers=args.brokers, scenario=args.scenario, kraft=True)
    cluster.deploy()
    cluster.start(timeout=30)

    watcher_stop = None
    watcher_thread = None
    try:
        bootstrap = cluster.bootstrap_servers()
        replication = args.replication_factor or min(args.brokers, 3)
        if replication > args.brokers:
            print(f'[orch] FATAL: --replication-factor={replication} > '
                  f'--brokers={args.brokers}', file=sys.stderr)
            sys.exit(2)
        if args.num_topics == 1:
            topics = [args.topic]
        else:
            topics = [f'{args.topic}_{i}'
                      for i in range(args.num_topics)]
        for t in topics:
            create_topic(cluster, t, args.partitions, replication)
        set_share_group_offset_reset(cluster, args.group, 'earliest')

        watcher_stop, watcher_thread = start_leader_watcher(
            cluster, topics, args.leader_poll_s, args.log_dir)

        kafka_bin = cluster.brokers[0].conf['bindir']

        print('')
        print('=' * 72)
        print('  CLUSTER UP — manual mode')
        print('=' * 72)
        print(f'  bootstrap servers : {bootstrap}')
        print(f'  topics            : {", ".join(topics)} '
              f'(partitions={args.partitions}, replication={replication})')
        print(f'  group id          : {args.group}')
        print(f'  kafka bin dir     : {kafka_bin}')
        print(f'  log dir           : {args.log_dir}')
        print('')
        print('  Run your own producer/consumer against the bootstrap '
              'from another shell.')
        print('  The REPL below controls broker stop/start; "done" or '
              'EOF exits and tears down the cluster.')
        print('=' * 72)
        print('')

        manual_roll(
            cluster, topics,
            leader_log_path=os.path.join(args.log_dir,
                                         'leader_changes.txt'),
            partitions=args.partitions, replication=replication)
    finally:
        if watcher_stop is not None:
            watcher_stop.set()
            if watcher_thread is not None:
                watcher_thread.join(timeout=5)
        print('[orch] stopping cluster (graceful)...', flush=True)
        try:
            cluster.stop(force=False)
        except Exception as e:
            print(f'[orch] graceful stop failed ({e}); forcing',
                  flush=True)
            try:
                cluster.stop(force=True)
            except Exception as e2:
                print(f'[orch] forced stop also failed: {e2}',
                      flush=True)
        try:
            cluster.cleanup(keeptypes=['perm', 'log'])
        except Exception as e:
            print(f'[orch] cluster cleanup failed: {e}', flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument('--version', default='4.2.0',
                    help='Kafka broker version (>= 4.2.0 for KIP-932)')
    ap.add_argument('--brokers', type=int, default=5)
    ap.add_argument('--consumers', type=int, default=1,
                    help='Number of share-consumer instances')
    ap.add_argument('--num-topics', type=int, default=1,
                    help='Number of test topics. With >1, --topic is used '
                         'as a prefix and topics are named <topic>_0, '
                         '<topic>_1, ...')
    ap.add_argument('--partitions', type=int, default=6,
                    help='Partition count for each test topic')
    ap.add_argument('--replication-factor', type=int, default=None,
                    help='Replication factor for each test topic. '
                         'Default: min(--brokers, 3).')
    ap.add_argument('--topic', default='chaos_test',
                    help='Topic name (or prefix if --num-topics > 1)')
    ap.add_argument('--group', default='chaos_grp')
    ap.add_argument('--scenario', default='default',
                    help='trivup scenario (see tests/scenarios/)')
    ap.add_argument('--reauth-ms', type=int, default=10000,
                    help='connections.max.reauth.ms on the brokers '
                         '(matches interactive_broker_version.py)')
    ap.add_argument('--extra-broker-prop', action='append', default=[],
                    metavar='KEY=VALUE',
                    help='Extra broker property (repeatable). Appended '
                         'after the CI defaults.')
    ap.add_argument('--produce-rate', type=int, default=100,
                    help='Producer msgs/sec (-r in rdkafka_performance)')
    ap.add_argument('--msg-size', type=int, default=100,
                    help='Message size in bytes')
    ap.add_argument('--warmup-s', type=int, default=10,
                    help='Seconds to let producer/consumers settle')
    ap.add_argument('--pre-roll-s', type=int, default=60,
                    help='Seconds to wait after consumer warmup before '
                         'starting the broker roll. Lets the share '
                         'group establish a stable assignment and '
                         'gives a steady-state baseline.')
    ap.add_argument('--cycles', type=int, default=3,
                    help='Number of full broker rolls')
    ap.add_argument('--seed', type=int, default=None,
                    help='RNG seed for the broker stop order (default: '
                         'time-based, non-deterministic)')
    ap.add_argument('--unclean-stop', action='store_true',
                    help='[broker-roll mode] Kill brokers with '
                         'SIGKILL (no graceful shutdown). Default: '
                         'clean SIGTERM stop.')
    ap.add_argument('--leave-broker-down', type=int, default=None,
                    metavar='N',
                    help='[broker-roll mode] Stop broker index N '
                         'once before the roll begins and never '
                         'restart it. N is excluded from the roll '
                         'rotation. Honours --unclean-stop. Useful '
                         'for measuring consumer recovery time '
                         'against a permanently-dead broker '
                         '(bounded by '
                         'topic.metadata.refresh.interval.ms). '
                         'Default: disabled.')
    ap.add_argument('--rebalance-mid-roll', action='store_true',
                    help='[broker-roll mode] Pair a share-group '
                         'rebalance with the broker roll: '
                         'add_consumer at the start of cycle '
                         'floor(--cycles / 2), remove_consumer '
                         '(uniform random target) at the start of '
                         'the next cycle. Tests partition '
                         'reassignment under concurrent leader '
                         'migration. Needs --cycles >= 2 for the '
                         'remove leg to fire. Default: disabled.')
    ap.add_argument('--topic-chaos',
                    choices=['delete', 'recreate-immediate',
                             'recreate-delayed'],
                    default=None,
                    help='Fire a one-shot topic-level chaos action '
                         'at a random moment during the roll, '
                         'against a randomly-picked topic from the '
                         'set this run subscribed to:\n'
                         '  delete: kafka-topics.sh --delete and '
                         'leave deleted.\n'
                         '  recreate-immediate: delete + immediate '
                         're-create with the same partition + '
                         'replication; exercises the in-place '
                         'topic_id mutation path.\n'
                         '  recreate-delayed: delete, dwell for a '
                         'random window, re-create; exercises the '
                         'topic disappearing then reappearing path '
                         '(NOTEXISTS -> EXISTS transition).\n'
                         'Default: disabled. Note: records produced '
                         'against the deleted topic in flight are '
                         'lost; verify.txt may report '
                         'never-consumed / never-acked entries '
                         'around the chaos timestamp — expected.')
    ap.add_argument('--leader-change-mode',
                    choices=['broker-roll', 'change-leader',
                             'reassign-partitions'],
                    default='broker-roll',
                    help='How to trigger leader changes in auto '
                         'mode.\n'
                         '  broker-roll (default): SIGTERM/SIGKILL '
                         'brokers; tests restart + leader migration '
                         'combined.\n'
                         '  change-leader: per cycle, reorder each '
                         'broker\'s partitions so it\'s no longer '
                         'preferred leader. No data movement. '
                         'Brokers stay up.\n'
                         '  reassign-partitions: per cycle, build a '
                         'new replica set that excludes each '
                         'broker. Data actually moves between '
                         'brokers. Brokers stay up.')
    ap.add_argument('--leader-poll-s', type=int, default=2,
                    help='Background leader-watcher poll interval in '
                         'seconds. Prints every observed leader '
                         'transition to stdout (prefix [leader '
                         'HH:MM:SS]) and writes them to '
                         '<log-dir>/leader_history.log. Set to 0 to '
                         'disable.')
    ap.add_argument('--stop-s', type=int, default=5,
                    help='Dwell time with broker stopped')
    ap.add_argument('--up-s', type=int, default=5,
                    help='Dwell time after broker is back up')
    ap.add_argument('--cooldown-s', type=int, default=15,
                    help='Observation window after last roll, producer + '
                         'consumers still running')
    ap.add_argument('--drain-s', type=int, default=30,
                    help='After cooldown, stop the producer and let '
                         'consumers run for this long to drain remaining '
                         'records before tearing everything down')
    ap.add_argument('--consume-mode',
                    choices=['share-consumer-verify',
                             'py-share-consumer-verify',
                             'rdkafka-perf-share',
                             'rdkafka-perf-group'],
                    default='share-consumer-verify',
                    help='[--mode auto only] Which producer + '
                         'consumer + verification bundle to run.\n'
                         '  share-consumer-verify (default): bundled '
                         'share_consume_verify C binary; emits per-'
                         'record JSON events; verify.txt bookkeeping '
                         'report at the end.\n'
                         '  py-share-consumer-verify: confluent-kafka-python '
                         'share_consume_verify.py, run under the same '
                         'interpreter as chaos.py (sys.executable). '
                         'Emits the same per-record JSON events, so it '
                         'produces the same verify.txt bookkeeping report. '
                         'NOTE: the Python '
                         'Message has no topic_id accessor yet, so the '
                         'id field is the zero UUID; do not trust '
                         'per-generation bookkeeping under --topic-chaos '
                         'recreate-* with this mode.\n'
                         '  rdkafka-perf-share: rdkafka_performance '
                         '-S (KIP-932 share consumer); conservation + '
                         'partition-coverage reports.\n'
                         '  rdkafka-perf-group: rdkafka_performance '
                         '-G (regular group consumer); same reports '
                         'as above. Useful as a cluster sanity '
                         'check.')
    ap.add_argument('--consumer-debug',
                    default='broker,fetch,cgrp,protocol,topic',
                    help='librdkafka debug contexts for each consumer '
                         '(producer runs without debug). Applied to '
                         'share-consumer-verify via -d and to '
                         'rdkafka-perf-{share,group} via -d. Empty '
                         'string disables.')
    ap.add_argument('--consumer-conf', action='append', default=[],
                    metavar='KEY=VALUE',
                    help='Extra rdkafka conf property for the consumer '
                         '(repeatable). Forwarded as -X k=v to '
                         'share-consumer-verify and rdkafka_performance.')
    ap.add_argument('--producer-conf', action='append', default=[],
                    metavar='KEY=VALUE',
                    help='Extra rdkafka conf property for the producer '
                         '(repeatable). Forwarded as -X k=v to '
                         'rdkafka_performance. Use this to drive '
                         'compression, acks, idempotence, batch.size, '
                         'linger.ms, etc.')
    ap.add_argument('--log-budget-gb', type=float, default=1.0,
                    help='Total cap on consumer logs on disk, summed across '
                         'all consumers')
    ap.add_argument('--log-dir', default='/tmp/chaos-logs')
    ap.add_argument('--perf-binary', default=None,
                    help='Path to rdkafka_performance (auto-detect by '
                         'default)')
    ap.add_argument('--mode', choices=['auto', 'manual'],
                    default='auto',
                    help='Orchestrator mode. "auto" (default) runs the '
                         'full workflow: cluster + producer + N '
                         'consumers + auto-cycle roll + drain + '
                         'reports. "manual" brings up only the cluster '
                         'and topics, prints bootstrap, and drops into '
                         'a REPL for broker stop/start operations. You '
                         'run your own producers/consumers against the '
                         'cluster from a separate shell. No workload '
                         'and no reports are run by the orchestrator in '
                         'manual mode.')
    args = ap.parse_args()

    here = _HERE
    perf = args.perf_binary or os.path.abspath(
        os.path.join(here, '..', '..', 'examples', 'rdkafka_performance'))
    # read_scenario_conf() and LibrdkafkaTestCluster expect cwd to be
    # tests/ (scenario JSON, trivup root 'tmp/', etc. are all relative
    # paths).
    os.chdir(_TESTS_DIR)
    # perf binary is only needed in auto mode; manual mode doesn't
    # spawn any built-in workload.
    if args.mode == 'auto' and not os.access(perf, os.X_OK):
        print(f'[orch] FATAL: {perf} not executable. Build with `make` '
              f'from the librdkafka root.', file=sys.stderr)
        sys.exit(2)

    os.makedirs(args.log_dir, exist_ok=True)

    # Build cluster conf (shared across both modes).
    cluster_conf = build_cluster_conf(
        args.scenario, args.reauth_ms, args.extra_broker_prop)
    print(f'[orch] log dir: {args.log_dir}', flush=True)
    print(f'[orch] broker conf: {cluster_conf["conf"]}', flush=True)

    if args.mode == 'manual':
        _run_manual_mode(args, cluster_conf)
        return

    total_budget = int(args.log_budget_gb * (1 << 30))
    # Producer gets ~10% (just stats, no debug); consumers split the rest.
    producer_budget = total_budget // 10
    per_consumer_budget = (total_budget - producer_budget) // args.consumers

    print(f'[orch] log budget: {args.log_budget_gb} GB total '
          f'(producer {producer_budget // (1 << 20)} MB, '
          f'{per_consumer_budget // (1 << 20)} MB per consumer)', flush=True)

    cluster = LibrdkafkaTestCluster(
        version=args.version, conf=cluster_conf,
        num_brokers=args.brokers, scenario=args.scenario, kraft=True)
    cluster.deploy()
    cluster.start(timeout=30)

    # producer list is built once we know the topic list; the
    # consumer list is built dynamically by spawn_consumer (defined
    # once we know bootstrap + topics + env).
    producers = []
    consumers = []
    drain_end_ts = None
    partition_hwms = {}
    watcher_stop = None
    watcher_thread = None
    topic_chaos_stop = None
    topic_chaos_thread = None
    chaos_state = {}
    try:
        bootstrap = cluster.bootstrap_servers()
        print(f'[orch] bootstrap: {bootstrap}', flush=True)

        replication = args.replication_factor or min(args.brokers, 3)
        if replication > args.brokers:
            print(f'[orch] FATAL: --replication-factor={replication} > '
                  f'--brokers={args.brokers}', file=sys.stderr)
            sys.exit(2)
        if args.num_topics == 1:
            topics = [args.topic]
        else:
            topics = [f'{args.topic}_{i}'
                      for i in range(args.num_topics)]
        for t in topics:
            create_topic(cluster, t, args.partitions, replication)
        set_share_group_offset_reset(cluster, args.group, 'earliest')

        watcher_stop, watcher_thread = start_leader_watcher(
            cluster, topics, args.leader_poll_s, args.log_dir)

        # Build -t flags shared by consumers.
        topic_args = []
        for t in topics:
            topic_args += ['-t', t]

        # One producer per topic; each producer runs at --produce-rate
        # msgs/sec (per-topic rate, NOT shared). Total cluster rate is
        # num_topics * produce_rate.
        per_producer_budget = max(producer_budget // len(topics), 1 << 20)
        producers = [Producer(idx, args.log_dir, per_producer_budget)
                     for idx in range(len(topics))]
        print(f'[orch] producers: {len(producers)} (one per topic) '
              f'at {args.produce_rate} rec/s each '
              f'(aggregate {args.produce_rate * len(topics)} rec/s)',
              flush=True)

        env = os.environ.copy()

        # Producer: no librdkafka debug. stdout carries the summary line
        # ("% N messages produced ... D delivered ...") that we parse for
        # the conservation check at end-of-run.
        topic_to_producer = dict(zip(topics, producers))

        def producer_cmd_for_topic(t):
            cmd = [
                perf, '-P', '-b', bootstrap, '-t', t,
                '-s', str(args.msg_size),
                '-r', str(args.produce_rate),
            ]
            for kv in args.producer_conf:
                cmd.extend(['-X', kv])
            return cmd

        for prod, t in zip(producers, topics):
            prod.start(producer_cmd_for_topic(t), env)

        def restart_producer_for_topic(topic_name):
            """Stop + re-spawn the producer subprocess for a topic.
            Called by the topic-chaos thread after a recreate so the
            new topic_id gets traffic. The new process appends its
            own monotonically-increasing stats lines to the same
            stdout file; _scan_producer_cumulative handles the
            counter reset across the restart boundary."""
            prod = topic_to_producer.get(topic_name)
            if prod is None:
                return
            print(f'[orch] restarting producer for {topic_name} '
                  f'(post-chaos recreate)', flush=True)
            prod.stop()
            prod.start(producer_cmd_for_topic(topic_name), env)

        print(f'[orch] producer warmup {args.warmup_s}s', flush=True)
        time.sleep(args.warmup_s)

        # -X k=v passthrough for any consumer config the user wants
        # to layer on top. Shared between verify and perf paths so
        # both modes accept the same --consumer-conf entries.
        extra_conf_args = []
        for kv in args.consumer_conf:
            extra_conf_args += ['-X', kv]

        verify_bin = None
        py_verify_cmd = None
        if args.consume_mode == 'share-consumer-verify':
            verify_bin = os.path.join(_HERE, 'share_consume_verify')
            if not os.access(verify_bin, os.X_OK):
                print(f'[orch] FATAL: {verify_bin} not executable. '
                      f'Build with `make` at the librdkafka root '
                      f'(or `make -C tests/chaos`).',
                      file=sys.stderr)
                sys.exit(2)
        elif args.consume_mode == 'py-share-consumer-verify':
            # Run the Python workload under the same interpreter that runs
            # chaos.py (sys.executable).
            script = os.path.join(_HERE, 'share_consume_verify.py')
            if not os.path.isfile(script):
                print(f'[orch] FATAL: Python verify workload {script} not '
                      f'found (expected next to chaos.py).',
                      file=sys.stderr)
                sys.exit(2)
            # Fail fast with a clear message if ShareConsumer isn't
            # importable in this interpreter, rather than letting every
            # consumer subprocess crash-loop on the import.
            try:
                from confluent_kafka import (  # noqa: F401
                    ShareConsumer as _Probe_ShareConsumer,
                    AcknowledgeType as _Probe_AcknowledgeType,
                )
            except ImportError as e:
                print(f'[orch] FATAL: cannot import '
                      f'confluent_kafka.ShareConsumer in this interpreter '
                      f'({sys.executable}): {e}\n'
                      f'        Install a confluent-kafka-python build with '
                      f'the KIP-932 ShareConsumer into the Python running '
                      f'chaos.py.',
                      file=sys.stderr)
                sys.exit(2)
            py_verify_cmd = [sys.executable, script]

        def spawn_consumer(idx):
            """Construct + start a ShareConsumer for the configured
            consume_mode. Called for the baseline pool and by the
            add-consumer REPL command / --rebalance triggers."""
            c = ShareConsumer(idx, args.log_dir, per_consumer_budget)
            if args.consume_mode in ('share-consumer-verify',
                                     'py-share-consumer-verify'):
                # Both verify workloads parse the identical CLI
                # (-d <debug>, -X k=v..., bootstrap group topics...) and
                # emit the identical per-record JSON, so they share one
                # arg tail. They differ only in the launch prefix: the C
                # binary is invoked directly; the Python workload via
                # [interp, script].
                if args.consume_mode == 'share-consumer-verify':
                    cons_cmd = [verify_bin]
                else:
                    cons_cmd = list(py_verify_cmd)
                if args.consumer_debug:
                    cons_cmd += ['-d', args.consumer_debug]
                cons_cmd += extra_conf_args
                cons_cmd += [bootstrap, args.group, *topics]
            else:
                mode_flag = ('-S' if args.consume_mode ==
                             'rdkafka-perf-share' else '-G')
                cons_cmd = [
                    perf, mode_flag, args.group, '-b', bootstrap,
                    *topic_args,
                ]
                if args.consumer_debug:
                    cons_cmd += ['-d', args.consumer_debug]
                cons_cmd += extra_conf_args
                cons_cmd += [
                    '-T', '5000',
                    # Route stats JSON to a per-consumer file instead
                    # of mixing it with -T's textual stats on stdout.
                    '-Y', f'cat >> {c.stats_path}',
                ]
            c.start(cons_cmd, env)
            return c

        for i in range(args.consumers):
            consumers.append(spawn_consumer(i))
        print(f'[orch] consumer warmup {args.warmup_s}s', flush=True)
        time.sleep(args.warmup_s)

        leader_log_path = os.path.join(args.log_dir,
                                       'leader_changes.txt')
        if args.mode == 'manual':
            manual_roll(cluster, topics,
                        leader_log_path=leader_log_path)
        else:
            if args.pre_roll_s > 0:
                print(f'[orch] pre-roll settle {args.pre_roll_s}s '
                      '(steady-state baseline before triggering '
                      'leader changes)', flush=True)
                time.sleep(args.pre_roll_s)
            seed = (args.seed if args.seed is not None
                    else int(time.time()))
            print(f'[orch] roll RNG seed: {seed} '
                  f'leader-change-mode={args.leader_change_mode}',
                  flush=True)
            exclude = []
            if args.leave_broker_down is not None:
                if args.leader_change_mode != 'broker-roll':
                    print('[orch] WARNING: --leave-broker-down only '
                          'applies in broker-roll mode; ignoring',
                          file=sys.stderr, flush=True)
                else:
                    leave_broker_down(cluster, topics,
                                      args.leave_broker_down,
                                      args.unclean_stop, args.stop_s,
                                      leader_log_path)
                    exclude = [args.leave_broker_down]
            add_cycle = None
            remove_cycle = None
            if args.rebalance_mid_roll:
                if args.leader_change_mode != 'broker-roll':
                    print('[orch] WARNING: --rebalance-mid-roll only '
                          'applies in broker-roll mode; ignoring',
                          file=sys.stderr, flush=True)
                else:
                    add_cycle = args.cycles // 2
                    if args.cycles >= 2:
                        remove_cycle = add_cycle + 1
                    else:
                        print('[orch] NOTE: --rebalance-mid-roll '
                              'remove leg skipped (need --cycles '
                              '>= 2); add only', flush=True)
            if args.topic_chaos is not None:
                topic_chaos_stop, topic_chaos_thread = (
                    start_topic_chaos(
                        cluster, topics, args.partitions,
                        replication, args.topic_chaos,
                        random.Random(seed ^ 0xC0FFEE),
                        leader_log_path, chaos_state,
                        restart_producer_for_topic))
            if args.leader_change_mode == 'broker-roll':
                roll(cluster, args.cycles, args.stop_s, args.up_s,
                     random.Random(seed), topics, args.unclean_stop,
                     leader_log_path=leader_log_path,
                     exclude_indices=exclude,
                     rebalance_add_cycle=add_cycle,
                     rebalance_remove_cycle=remove_cycle,
                     consumers=consumers,
                     spawn_consumer_fn=spawn_consumer)
            else:
                reassign_roll(cluster, args.cycles, args.stop_s,
                              args.up_s, random.Random(seed),
                              topics, replication,
                              args.leader_change_mode,
                              leader_log_path=leader_log_path)

        # Mark the start of the observation window so the partition
        # coverage report can diff rxmsgs over (cooldown + drain).
        print('[orch] marking pre-observation stats snapshot', flush=True)
        for c in consumers:
            c.mark_pre_obs()

        print(f'[orch] cooldown {args.cooldown_s}s '
              '(producer + consumers running)', flush=True)
        time.sleep(args.cooldown_s)

        # Drain phase: stop all producers so no new records arrive,
        # and let consumers keep running to catch up.
        print(f'[orch] stopping {len(producers)} producer(s) '
              '(drain phase starts)...', flush=True)
        for prod in producers:
            prod.stop()
        print(f'[orch] drain {args.drain_s}s '
              '(consumers running, producers stopped)', flush=True)
        time.sleep(args.drain_s)
        drain_end_ts = time.time()

        # Snapshot per-partition high-water-marks *before* tearing
        # down the cluster, so the bookkeeping report can compare
        # broker-side produced vs consumer-side consumed per
        # partition.
        print('[orch] capturing per-partition broker HWMs...',
              flush=True)
        partition_hwms = get_partition_hwms(cluster, topics)

    finally:
        if topic_chaos_stop is not None:
            topic_chaos_stop.set()
            if topic_chaos_thread is not None:
                topic_chaos_thread.join(timeout=5)
        if watcher_stop is not None:
            watcher_stop.set()
            if watcher_thread is not None:
                watcher_thread.join(timeout=5)
        print('[orch] stopping consumers...', flush=True)
        for c in consumers:
            c.stop()
        # Producers may already be stopped from the drain phase;
        # stop() is a no-op if proc is None or already exited.
        print('[orch] stopping producers (idempotent)...', flush=True)
        for prod in producers:
            prod.stop()
        print('[orch] stopping cluster (graceful)...', flush=True)
        try:
            cluster.stop(force=False)
        except Exception as e:
            print(f'[orch] graceful stop failed ({e}); forcing',
                  flush=True)
            try:
                cluster.stop(force=True)
            except Exception as e2:
                print(f'[orch] forced stop also failed: {e2}',
                      flush=True)
        print('[orch] cluster cleanup (keep kafka tarball)...', flush=True)
        try:
            cluster.cleanup(keeptypes=['perm', 'log'])
        except Exception as e:
            print(f'[orch] cluster cleanup failed: {e}', flush=True)

    print('[orch] closing log files...', flush=True)
    for p in (*producers, *consumers):
        p.close_logs()

    summarize(consumers, os.path.join(args.log_dir, 'summary.txt'))
    metadata_trigger_report(
        consumers, os.path.join(args.log_dir, 'metadata_triggers.txt'))
    if args.consume_mode in ('share-consumer-verify',
                             'py-share-consumer-verify'):
        bookkeeping_report(
            producers, consumers, topics, args.partitions,
            partition_hwms, drain_end_ts,
            os.path.join(args.log_dir, 'verify.txt'),
            chaos_state=chaos_state)
    else:
        conservation_report(producers, consumers,
                            os.path.join(args.log_dir, 'conservation.txt'))
        partition_coverage_report(
            consumers, topics, args.partitions,
            os.path.join(args.log_dir, 'partition_coverage.txt'))


if __name__ == '__main__':
    main()
