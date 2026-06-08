# chaos — KIP-932 share-consumer chaos-test harness

A local diagnostic harness for librdkafka's KIP-932 share consumer
under broker rolling restarts and leader migrations. Brings up a
real Kafka KRaft cluster via
[trivup](https://github.com/edenhill/trivup), spawns one producer
per topic and N share-consumer instances, then drives chaos
(broker stop/start, leader reassignment) against the running
cluster.
Captures per-consumer logs and produces several reports so you can
attribute observed behavior to specific code paths.

Two modes:

- **`--mode auto`** (default) — full workflow: cluster + producer +
  consumers + automated broker roll + cooldown + drain + reports.
- **`--mode manual`** — cluster only, plus an interactive REPL for
  broker stop/start. You run your own producer/consumer/workload
  against the printed bootstrap servers from a separate shell.

## Non-Java client testing improvements

The harness covers three broker-movement scenarios against a real
KRaft Kafka cluster (via trivup):

- **Leader migration** — via `kafka-reassign-partitions.sh`
  (`--leader-change-mode change-leader|reassign-partitions`) or
  implicitly as a side effect of broker stop.
- **Broker up / down** — clean (SIGTERM) or unclean (SIGKILL).
- **Broker roll** — full cycle through every broker, repeated for
  N cycles.

Two distinct improvements:

1. **Mock-broker surface shrinkage (share consumer).** These
   scenarios have historically been covered for the share consumer
   via the **mock broker** — hand-crafted responses that simulate
   cluster behavior. That approach has limits:
   - the mock encodes our assumptions about timing and response
     ordering, so when the real broker diverges, a mock test passes
     and the real bug ships;
   - multi-broker interleavings (broker dies → leader migrates →
     consumer rejoins new leader → ack lands on stale broker)
     aren't reproducible in a single-process mock.

   Mock tests that primarily exercise broker-movement behavior can
   migrate here. Tests that need deterministic single-broker
   semantics (specific err-code injection, exact response ordering,
   partial-response shaping) stay in the mock suite.

2. **Leader-change + broker-down framework for any client type.**
   Independent of the share-consumer mock-migration angle, this is
   the canonical place to write any new **leader-change** or
   **broker-down** test in librdkafka. The harness is
   share-consumer-focused today, but the orchestrator
   (`chaos.py`) is workload-agnostic — the same
   cluster/roll/report machinery can be extended to cover regular
   consumer and producer scenarios by plugging in a different
   workload binary. New leader-change / broker-down tests for any
   client type should land here.

## Files in this folder

| File | Purpose |
|---|---|
| `chaos.py` | Orchestrator. Brings up the cluster, runs the workload (auto) or the REPL (manual), and emits reports. |
| `share_consume_verify.c` | Workload binary used by `--consume-mode share-consumer-verify` (the default). Subscribes as a share consumer in explicit-ack mode and emits one JSON event per consumed record and per acked record. |
| `Makefile` | Builds `share_consume_verify` against the in-tree librdkafka. |
| `chaos_until_fail.sh` | Looping harness around `chaos.py`. Runs iterations until one fails (VERDICT: FAIL or chaos.py exits non-zero) or until a fixed iteration count is reached. See [Looping with `chaos_until_fail.sh`](#looping-with-chaos_until_failsh). |
| `run-history.tsv` | One row per iteration appended by `chaos_until_fail.sh`. Survives across reboots and runner invocations. Gitignored. |
| `run-history.log` | Verbose stdout/stderr capture of every `chaos_until_fail.sh` invocation, banner-separated per invocation. Gitignored. |
| `runs/` | Per-runner-invocation report archives. One subdir per invocation (`<utc>-pid<P>/`), one sub-subdir per iteration (`iter-NNN-<verdict>/`). Holds the small report files only (`verify.txt`, `summary.txt`, `leader_*`); not the multi-GB debug logs. Gitignored. |
| `README.md` | This file. |

## Prerequisites

- A built librdkafka: from the repository root, run `make`. This
  produces `src/librdkafka.a`, `examples/rdkafka_performance`, and
  `tests/chaos/share_consume_verify` (this folder's workload binary,
  built automatically). To rebuild just this folder after editing
  `share_consume_verify.c`, run `make chaos` at the root or
  `make -C tests/chaos`.
- `trivup` Python package installed:
  `pip install -r tests/requirements.txt`.

## Quick start

```bash
# auto mode (default): full roll + reports
python3 tests/chaos/chaos.py

# manual mode: cluster + REPL, bring your own workload
python3 tests/chaos/chaos.py --mode manual
```

All outputs land under `--log-dir` (default `/tmp/chaos-logs`).

## Looping with `chaos_until_fail.sh`

`chaos.py` is one iteration; `chaos_until_fail.sh` is the loop
around it. Each iteration cleans up any stranded Kafka JVMs +
trivup instance dirs, runs `chaos.py` once, and stops at the
first VERDICT: FAIL (or after a fixed iteration count).

```bash
cd tests/chaos

# 100 iterations of default config (stops on first failure)
./chaos_until_fail.sh

# Override iteration count and forward extra args to chaos.py:
./chaos_until_fail.sh 100 --cycles 10 --stop-s 2 --up-s 2 \
                       --produce-rate 500 --drain-s 180
```

Anything after the iteration count is forwarded verbatim to
`chaos.py`. The runner pre-pends a sensible chaos default
(`--unclean-stop --warmup-s 5 --pre-roll-s 10 --cooldown-s 10
--drain-s 30`); user-supplied flags override these via argparse
"last wins".

### What gets captured

Three layers of persistence, designed to be cheap on disk while still
letting you reconstruct any iteration after the fact:

| Location | What | Across iterations? | Survives Ctrl-C? |
|---|---|---|---|
| `/tmp/chaos-logs/` | **Current iteration's full logs**: `verify.txt`, `summary.txt`, `consumer-0/{stdout*,stderr}`, `producer-0/{...}`, `leader_history.log`, `leader_changes.txt`, `metadata_triggers.txt`. ~100–200 MB. | rotated out at end of each iter (becomes `prev`) | yes — last-running iter's partial state |
| `/tmp/chaos-prev/` | **Previous iteration's full logs**, same shape as above. Overwritten at the end of each new iteration. | yes — last completed iter | yes |
| `tests/chaos/runs/<run-id>/iter-NNN-<verdict>/` | **Small report files only** (`verify.txt`, `summary.txt`, `leader_changes.txt`, `leader_history.log`, `metadata_triggers.txt`). ~15 KB per iter. | yes — one subdir per iter for the whole runner invocation | yes — only the in-progress iter is unarchived |
| `run-history.tsv` | TSV: `iter | start_utc | elapsed_s | seed | cycles | produce_rate | extra_args | verdict | summary`. | yes — appended | yes |
| `run-history.log` | Banner-separated stdout/stderr of every `chaos_until_fail.sh` invocation. | yes — appended | yes |

`run-id` is `<runner-start-utc>-pid<P>`, so multiple invocations
don't collide.

### Ctrl-C behaviour

`Ctrl-C` triggers the runner's `SIGINT` trap, which:

1. kills any stranded Kafka / trivup JVMs;
2. removes trivup per-broker instance dirs (`tests/tmp/LibrdkafkaTestCluster/<numeric>/`);
3. exits with code 130.

What is **not** touched:

- `/tmp/chaos-logs/` — current iter's partial logs stay put.
- `/tmp/chaos-prev/` — last completed iter's full logs stay put.
- `tests/chaos/runs/<run-id>/` — every completed iter's
  reports are still there.

So after Ctrl-C you can immediately inspect the last two full log
trees plus the report archive for every iteration that finished.

### Disk envelope

Two rolling full log dirs (~200 MB each = ~400 MB) + ~15 KB × N
iters of reports. 100 iters of reports is ~1.5 MB.

`/tmp/chaos-logs/` and `/tmp/chaos-prev/` are wiped on
reboot; `tests/chaos/runs/` survives reboot (gitignored).

### Stopping criteria

Iteration loop exits early on:
- `chaos.py` exits non-zero — recorded as `ERROR` in TSV
- `verify.txt` reports `VERDICT: FAIL` — recorded as `FAIL`
- the SIGINT trap fires (exit 130)

Iteration completes successfully with `VERDICT: OK`. Note that
`acked_with_err` (records with non-zero ack callback err but at
least one callback fired) is *not* loss and does **not** trigger
FAIL — see [Reading the reports → `verify.txt`](#verifytxt-share-consumer-verify-workload).

## What the harness produces

### Auto mode

After the run completes, `--log-dir` contains:

```
producer-0/               per-topic producer dir (one per topic)
producer-1/               (only present with --num-topics > 1)
consumer-0/               per-consumer dir, stdout + stderr (rotating)
consumer-1/
...
summary.txt               counts of known librdkafka gap signatures
metadata_triggers.txt     where each metadata refresh came from
verify.txt                per-record bookkeeping
                          (--consume-mode share-consumer-verify)
conservation.txt          rough produced-vs-consumed counts
                          (--consume-mode rdkafka-perf-share / -group)
partition_coverage.txt    per-partition coverage during observation
                          (--consume-mode rdkafka-perf-share / -group)
leader_changes.txt        per-bounce leader migration log
leader_history.log        live leader transitions captured by the
                          background watcher (--leader-poll-s)
```

### Manual mode

```
leader_changes.txt        REPL command history with leader diffs
leader_history.log        live leader transitions captured by the
                          background watcher (--leader-poll-s)
```

No consumer reports are produced because you're running your own
workload.

## Consume modes (auto mode only)

A consume mode selects which producer + consumer(s) + verification
bundle the orchestrator runs against the cluster.

| `--consume-mode` | Consumer binary | Output report |
|---|---|---|
| `share-consumer-verify` (default) | `share_consume_verify` (in this folder) | `verify.txt` — per-record bookkeeping with delivery-count distribution, pending/orphan tally, per-partition first/last consume time, broker-HWM comparison |
| `rdkafka-perf-share` | `examples/rdkafka_performance -S` | `conservation.txt`, `partition_coverage.txt` (rdkafka_performance summary lines) |
| `rdkafka-perf-group` | `examples/rdkafka_performance -G` | Same reports as `rdkafka-perf-share`; provided as a cluster sanity check (regular group consumer) |

## Manual-mode REPL commands

```
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
  show               full replica + ISR view (kafka-topics.sh --describe)
  leaders            shorthand: just current leader per partition
  status             broker up/down state for all brokers
  sleep <sec>        pause for N seconds (your workload keeps flowing)
  help               this help
  done | quit        exit manual mode and tear down the cluster
```

Every `stop` / `kill` / `start` / `change-leader` / `reassign` snapshots
leaders before and after, prints the diff live and persists it to
`leader_changes.txt`.

The two new partition-scope commands choose their target broker(s)
automatically:

- `change-leader <t> <p>`: current replicas `[L, b, c]` → new
  `[b, c, L]`. The next replica becomes leader. Replica set
  unchanged.
- `reassign <t> <p>`: current replicas `[L, b, c]`, cluster brokers
  `[L, b, c, d, e]` → pick the lowest-id broker not in current
  replicas (= `d`). New replicas `[d, b, c]`. If saturated (no
  broker outside current replicas), falls back to reorder and
  prints a notice.

## Leader-change modes

`--leader-change-mode` picks how the auto-mode roll triggers leader
changes. All three modes share the same `--cycles`, `--stop-s`,
`--up-s`, and seed semantics so timing is comparable across modes.

| Mode | Mechanism | Brokers restart? | Data moves? | Tests |
|---|---|:---:|:---:|---|
| `broker-roll` (default) | SIGTERM/SIGKILL the broker; wait; restart it | yes | no | broker restart **+** leader migration combined |
| `change-leader` | `kafka-reassign-partitions.sh` with replica reorder per broker drain | no | no | pure leader-migration handling, in isolation from broker restart |
| `reassign-partitions` | `kafka-reassign-partitions.sh` with a replica set that **excludes** the broker being drained | no | yes | leader migration **+** replica resync |

For `change-leader` and `reassign-partitions`, the orchestrator
iterates brokers per cycle just like `broker-roll`, but instead of
stopping process `i`, it submits a reassignment that moves
leadership (and, in the reassign case, the replica set) away from
`cluster.brokers[i].appid`. `--unclean-stop` only applies to
`broker-roll`.

When the cluster is **saturated** (`RF == num_brokers`), the
`reassign-partitions` path falls back to the same replica-reorder
the `change-leader` path uses. With the default `--brokers 5`
`--replication-factor 3`, every partition has 2 brokers available
to receive a new replica, so the reassign path is meaningfully
different from the reorder path.

### Internal verification

Every reassignment plan goes through two independent checks before
the call returns:

1. `kafka-reassign-partitions.sh --verify` is polled until the
   controller reports the plan completed at the metadata-log level.
2. The orchestrator then polls `kafka-topics.sh --describe` until
   every partition in the plan shows its **first listed replica**
   as the actual leader, or a 10 s deadline elapses.

Outcome is printed inline:

```
[verify] OK — all 6 partition(s) show expected leader after reassignment
```

or, on mismatch:

```
[verify] FAIL — 2 of 6 partition(s) did not get the expected leader within 10s:
           chaos_test[2] expected leader=broker 4, actual=broker 3
           chaos_test[5] expected leader=broker 4, actual=no-leader
```

This applies to every site that submits a reassignment plan:
auto-mode `change-leader` / `reassign-partitions` drains, manual
REPL `change-leader <t> <p>` / `reassign <t> <p>`. A failure here
indicates the plan ran but leadership didn't actually transfer —
diagnostic information about a broker-side issue, not a script bug.

## Key flags

### Cluster shape

| Flag | Default | Notes |
|---|---|---|
| `--version` | `4.2.0` | Kafka broker version (≥ 4.2.0 for KIP-932) |
| `--brokers` | `5` | Brokers in the trivup KRaft cluster |
| `--num-topics` | `1` | Use `--topic` as a prefix if `> 1` |
| `--partitions` | `6` | Per-topic partition count |
| `--replication-factor` | `min(--brokers, 3)` | Fails if > `--brokers` |
| `--topic` | `chaos_test` | Topic name or prefix |
| `--group` | `chaos_grp` | Share-group id |
| `--scenario` | `default` | trivup scenario name |
| `--reauth-ms` | `10000` | Broker-side `connections.max.reauth.ms` |
| `--extra-broker-prop K=V` | — | Repeatable; appended to broker conf |

### Auto-mode workflow

| Flag | Default | Notes |
|---|---|---|
| `--mode` | `auto` | `auto` or `manual` |
| `--consumers` | `1` | Number of share-consumer instances |
| `--produce-rate` | `100` | Producer msgs/sec **per topic** (`rdkafka_performance -r`). Aggregate cluster rate = `--produce-rate × --num-topics`. |
| `--msg-size` | `100` | Producer message size in bytes |
| `--warmup-s` | `10` | Settle after producer start; again after consumer start |
| `--pre-roll-s` | `60` | Wait after consumer warmup before bouncing brokers, so the share group reaches steady state |
| `--cycles` | `3` | Number of full broker rolls |
| `--stop-s` | `5` | Dwell with broker stopped |
| `--up-s` | `5` | Dwell after broker is back up |
| `--cooldown-s` | `15` | Observation window after last roll (producer + consumers still running) |
| `--drain-s` | `30` | After cooldown: producer stopped, consumers continue, then teardown |
| `--seed N` | random (time-based) | Reproducible broker stop order |
| `--unclean-stop` | off | [broker-roll only] SIGKILL brokers instead of SIGTERM |
| `--leave-broker-down N` | disabled | [broker-roll only] Stop broker index `N` once before the roll begins and never restart it. `N` is excluded from the roll rotation. Honours `--unclean-stop`. Useful for measuring consumer recovery time against a permanently-dead broker (bounded by `topic.metadata.refresh.interval.ms`). |
| `--leader-change-mode` | `broker-roll` | `broker-roll` / `change-leader` / `reassign-partitions`. Picks the mechanism the auto-mode roll uses to trigger leader changes. See the "Leader-change modes" section below. |
| `--leader-poll-s` | `2` | Background leader-watcher poll interval. Every observed transition is printed inline (`[leader HH:MM:SS] ...`) and appended to `leader_history.log`. Catches changes between/outside our explicit operations (preferred-leader timer, ISR shrinkage). Set to `0` to disable. |
| `--consume-mode` | `share-consumer-verify` | See table above |
| `--consumer-debug` | `broker,fetch,cgrp,protocol,topic` | librdkafka debug contexts. Forwarded as `-d` to whichever consumer binary the mode selects (`share_consume_verify` and `rdkafka_performance` both take `-d`). Pass empty (`--consumer-debug ''`) to disable. |
| `--consumer-conf KEY=VALUE` | `[]` | Extra rdkafka conf for the consumer, repeatable. Forwarded as `-X k=v` to both `share_consume_verify` and `rdkafka_performance`. Example: `--consumer-conf share.acknowledgement.mode=implicit`. |

### Output

| Flag | Default | Notes |
|---|---|---|
| `--log-dir` | `/tmp/chaos-logs` | Root for all artifacts |
| `--log-budget-gb` | `1.0` | Total disk cap (rotating); split 10% producer / 90% across consumers |
| `--perf-binary` | auto-detect | Override `rdkafka_performance` path |

Always-on broker properties (configured by the orchestrator):
`connections.max.reauth.ms=<--reauth-ms>`,
`transaction.state.log.replication.factor=1`,
`transaction.state.log.min.isr=1`,
`log.retention.bytes=1000000000`. Plus what
`tests/cluster_testing.py` auto-injects for Kafka ≥ 4.2.0:
`share.coordinator.state.topic.replication.factor=min(brokers,3)`,
`share.coordinator.state.topic.min.isr=min(brokers,2)`,
`group.share.min.record.lock.duration.ms=1000`.

After topic creation, the orchestrator also calls `kafka-configs.sh`
to set `share.auto.offset.reset=earliest` on the share group, so the
consumer reads every record produced (the broker-side default is
`latest`, which silently skips records produced before the
consumer's first fetch lands on each partition).

## Examples

```bash
# 1) Default auto run: 3 cycles × 5 brokers (RF=3),
#    share-consumer-verify workload, broker-roll, all reports.
python3 tests/chaos/chaos.py

# 2) Cluster sanity check with regular group consumer.
python3 tests/chaos/chaos.py \
    --consume-mode rdkafka-perf-group

# 2a) Same workload but trigger leader changes via pure replica
#     reorder (no broker restart, no data movement).
python3 tests/chaos/chaos.py \
    --leader-change-mode change-leader

# 2b) Same workload but trigger leader changes via full replica
#     reassignment (data moves between brokers).
python3 tests/chaos/chaos.py \
    --leader-change-mode reassign-partitions

# 3) Stress: 5 cycles, longer dwells, multiple topics, unclean stops.
python3 tests/chaos/chaos.py \
    --cycles 5 --stop-s 8 --up-s 3 \
    --num-topics 3 --partitions 4 \
    --unclean-stop

# 4) Manual mode: bring up cluster, run your own workload elsewhere.
python3 tests/chaos/chaos.py --mode manual
#  (then in another shell, use kafka-topics.sh / your-consumer.py
#   against the bootstrap that was printed)

# 5) Permanently-dead broker: kill broker 2 once before the roll and
#    never restart it; remaining 4 brokers roll normally on top.
#    Shortens metadata refresh so recovery completes inside the run.
python3 tests/chaos/chaos.py \
    --leave-broker-down 2 \
    --consumer-conf topic.metadata.refresh.interval.ms=30000
```

## share_consume_verify event format

The verify binary emits one JSON object per line on stdout. Format:

```
{"e":"consumed","t":"<topic>","p":<partition>,"o":<offset>,"dc":<broker_delivery_count>}
{"e":"acked","t":"<topic>","p":<partition>,"o":<offset>,"err":<resp_err_int>}
```

`err=0` means the broker accepted the ack for this partition. `dc`
is `rd_kafka_message_delivery_count(rkm)`: 1 means first-time
delivery, ≥2 means the broker re-served this record after the
acquisition lock timed out.

If you want to plug a custom workload into the same bookkeeping
report, have it emit JSON in this exact format on stdout — the
orchestrator's `bookkeeping_report` aggregates events across all
consumers regardless of which binary produced them.

### `share_consume_verify` CLI flags

The binary parses a couple of flags before its positional arguments
(`<broker> <group.id> <topic>…`), so you can configure it
identically to `rdkafka_performance`:

| Flag | Effect |
|---|---|
| `-d <debug>` | Sets librdkafka's `debug` conf (e.g. `broker,fetch,cgrp,protocol,topic`). Same idiom as `rdkafka_performance -d`. |
| `-X k=v` | Sets an arbitrary rdkafka conf property. Repeatable. The binary calls `rd_kafka_conf_set(k, v, ...)`. Default-set: `share.acknowledgement.mode=explicit`; override with `-X share.acknowledgement.mode=implicit`. |
| `-h` | Print usage. |

You don't usually invoke `share_consume_verify` directly — the
orchestrator does. It plumbs `--consumer-debug` through to `-d` and
each `--consumer-conf KEY=VALUE` through to `-X KEY=VALUE`.

```bash
# Implicit-ack mode end-to-end via the orchestrator:
python3 tests/chaos/chaos.py \
    --consumer-conf share.acknowledgement.mode=implicit
```

## Reading the reports

### `verify.txt` (share-consumer-verify workload)

```
producer produced           : 216211
producer delivered (broker) : 216211
distinct records consumed   : 203940
total consume events        : 207514
distinct records acked OK   : 203468   (>=1 ack callback with err=0)
ack-with-err only           : 472      (>=1 ack callback, all err!=0;
                                         ambiguous, NOT data loss)
never-acked (no callback)   : 0        <-- effective data loss (consumer-side)
orphan acks (ack w/o cons.) : 0
never-consumed              : 12271    <-- delivered by broker but no
                                          consume event seen

Delivery-count distribution (k = max broker dc, only records eventually
acked OK):
  dc[1] = 200000   records acked after 1 delivery
  dc[2] = 3468     records acked after a redelivery
  ...

Ack callback errors (per-code, ALL callbacks across redeliveries):
  err=-195  count=472    (= __TRANSPORT, broker connection dropped)
  ...

Per-record ack trail for N records with err in their ack history
(ambiguous, not loss):
  format: t[p] offset=O consumes=[dc1,dc2,...] acks=[err1,err2,...]
  chaos_test[5] offset=17133 consumes=[1] acks=[-195]
  ...

Per-(topic, partition) coverage:
  topic[p] consumed=N hwm=H missing=H-N first=… last=… idle@drain_end=Ns
```

Records are bucketed by the *latest* ack-callback err received per
(topic, partition, offset). A consume event always resets the
record's bucket to "no ack yet" so a redelivery starts a fresh
attempt:

| Bucket | Definition | Counts as loss? |
|---|---|---|
| `acked OK` | At least one ack callback fired with `err = 0`. Broker definitely processed the ack at some point. | No. |
| `ack-with-err only` | At least one ack callback fired, but every callback had `err != 0`. Broker may have processed the ack (response just lost in transit) OR will redeliver on lock expiry. | No — ambiguous, surfaced for forensics. |
| `never-acked (no callback)` | Consume event seen but no ack callback of any kind. Real consumer-pipeline loss. | **Yes** — triggers VERDICT: FAIL. |
| `never-consumed` | Broker has the record (counted in HWM) but the consumer never emitted a consume event for it. Real fetch-side issue. | **Yes** — triggers VERDICT: FAIL. |

Other interpretive cues:

- **`dc[2+] > 0`** → broker is redelivering. Expected during cluster
  roll when leaders move while acks are in flight; also normal after
  a `__TRANSPORT` ack failure (broker holds the record until the
  acquisition-lock timer expires, then redelivers).
- **Per-record ack trail** — printed only for records that received
  at least one `err != 0` callback. Useful to see whether a record
  recovered (`acks=[-195, 0]` = first ack failed, retry succeeded)
  or stayed ambiguous (`acks=[-195]` = only ack callback was the
  transport-failed one).
- **Per-partition `idle@drain_end`** large for some partition while
  others' are small → that partition stopped being fetched well
  before the drain ended, while siblings kept going. Investigate.

VERDICT rules:

- **OK** if `never-consumed == 0` and `never-acked == 0`. If
  `ack-with-err > 0` an `[ack-with-err: N ...]` note is appended;
  not a failure.
- **FAIL** if `never-consumed > 0` (producer-delivered records that
  never reached the consumer) — fetch-side loss.
- **FAIL** if `never-acked > 0` — consumer-pipeline loss (consume
  event fired, no ack callback ever followed).

### `summary.txt`

Counts of known librdkafka log-line signatures (e.g.
`SHAREFETCH per-partition leader-change err`, `S10 stale
PARTITION_LEAVE bail-out`, `_TRANSPORT broker disconnects`). One row
per consumer + grand totals. Reads consumer.stderr — both
`share_consume_verify` and `rdkafka_performance` honour
`--consumer-debug`, so by default the signatures populate in every
mode. Pass `--consumer-debug ''` to disable.

### `metadata_triggers.txt`

Groups every `triggering metadata refresh` log line by `(module,
reason)`. Useful for proving where leader-change recovery is being
driven from (e.g. share-fetch reply path vs broker-down path).

### `leader_changes.txt`

Append-only log of each broker bounce in auto mode (or each REPL
command in manual mode) with the `kafka-topics.sh --describe` diff
before vs after, so you can correlate "broker X went down at T" with
"partition Y stopped being consumed at T+δ".

## Caveats

- First run downloads the Kafka tarball into `tests/tmp-…/` (~100 MB);
  subsequent runs reuse it.
- `share_consume_verify` defaults to explicit-ack mode and
  commit-syncs every batch. To exercise implicit ack, pass
  `--consumer-conf share.acknowledgement.mode=implicit` (the binary
  forwards `-X k=v` to `rd_kafka_conf_set`).
- Manual-mode cluster persists only for the lifetime of the
  orchestrator process. Closing the REPL tears it down.

## Build

```bash
# at the librdkafka root: builds librdkafka.a +
# examples/rdkafka_performance + tests/chaos/share_consume_verify
make

# rebuild only this folder after editing share_consume_verify.c:
make chaos                             # from the root
# or
make -C tests/chaos                    # directly
```

## Cleanup

```bash
# Binaries
make -C tests/chaos clean              # remove share_consume_verify

# Per-iter logs (rolling, last two iters)
rm -rf /tmp/chaos-logs          # current iteration
rm -rf /tmp/chaos-prev          # previous iteration

# Per-runner-invocation report archives (cumulative across runs)
rm -rf tests/chaos/runs/        # report archive

# Runner history (cumulative across runs)
rm tests/chaos/run-history.tsv
rm tests/chaos/run-history.log

# Trivup per-cluster state (instance dirs; keep the KafkaBrokerApp/
# tarball cache to avoid re-downloading Kafka)
find tests/tmp/LibrdkafkaTestCluster -mindepth 1 -maxdepth 1 \
     -type d -regex '.*/[0-9]+$' -exec rm -rf {} +
```

The runner does the trivup-instance-dir cleanup automatically at
the start of every iteration, so you only need to do it manually
if you Ctrl-C'd the runner mid-iteration and have stranded JVMs to
chase.

## TODO

- **More chaos shapes.** Today the harness only churns broker
  liveness + (optionally) leader assignment. Add:

  *Consumer-side:*
  - **Dynamic consumer count.** Add/remove share-consumer instances
    mid-run to exercise share-group rebalancing across consumers.
  - **Slow / pausing consumer.** `share_consume_verify` sleeps
    between batches on a configurable schedule. Drives the
    acquisition-lock-timeout → broker redelivers → `dc[2+] > 0`
    path which we currently never reach (lock is 1 s in our config;
    a 2 s sleep per batch would trigger it reliably). High signal:
    the `dc[2+]` bucket stays empty in every run today.
  - **Mixed implicit / explicit ack consumers in the same group.**
    Run two consumers in the same share group with different
    `share.acknowledgement.mode`. Broker tracks ack mode per member,
    so this should work; look for inconsistency between members.
  - **Pause / resume partitions.** Triggers a `rktp_version` bump
    via the pause/resume op handler — exercises an alternative
    trigger into the same filter machinery that broker-movement
    paths use, plus broker-side lock-timeout under pause.
  - **Subscribe / unsubscribe / resubscribe mid-run.** Clean session
    teardown + re-establishment without restarting the consumer
    process.
  - **`acknowledge.type` mix per record.** Today `share_consume_verify`
    always sends ACCEPT. Mix in RELEASE (intentional retry) and
    REJECT (poison-pill skip) to test ack-state-machine corners.
  - **Multiple share groups consuming the same topic.** Exercises
    share-coordinator multiplexing across groups.

  *Producer-side:*
  - **Variable producer rate.** Burst 5000 rec/s for 10 s, idle for
    30 s, then 500 rec/s. Drives the consumer between "behind" and
    "caught up"; catches bugs that only surface during catch-up.
  - **Transactional producer with mixed commit / abort.** Today the
    producer is `rdkafka_performance -P` (non-transactional).
    Aborted batches exercise the share-consumer's `isolation_level`
    handling and aborted-transaction skipping.

  *Topic / metadata:*
  - **Topic deletion.** Delete a subscribed topic mid-run; verify
    the consumer drops the assignment cleanly and any subsequent
    ShareFetch surfaces the right err.
  - **Topic recreation (immediate).** Delete + recreate in the
    same metadata snapshot the consumer observes — exercises the
    topic-state-machine path where `rkt` is reused with a new
    `topic_id`.
  - **Topic recreation (delayed).** Delete in one metadata update,
    recreate in a later one — exercises the "topic disappeared,
    then came back" path, including the
    `RD_KAFKA_TOPIC_S_NOTEXISTS` → `S_EXISTS` transition.
  - **Partition count increase.** Add partitions to an existing
    topic mid-run; verify the consumer picks up the new partitions
    via heartbeat-driven reassignment (see
    [[gap-heartbeat-partition-count-increase]] for the known gap).

  *Broker-side (beyond just stopping):*
  - **Kill the active KRaft controller specifically.** Today
    `roll()` picks brokers at random — the controller is whichever
    one happens to be picked. A `--target-controller` mode forces
    a controller-election cycle on top of leader migration.
  - **ISR shrinkage without leader loss.** Kill follower replicas
    (not leader). Producer with `acks=all` sees `NOT_ENOUGH_REPLICAS`;
    consumer side should see nothing different — unless a bug
    surfaces.
  - **Bursty preferred-leader-election cycle.** Many partitions
    migrating in a single election (vs `change-leader` mode which
    drains brokers serially).

  *Network-level (heavier — needs root + `tc` / `iptables`):*
  - **TCP latency spikes.** `tc qdisc add dev lo root netem delay
    200ms 50ms` injected briefly. Stresses request-timeout handling,
    ack-RPC timeouts, lock-expiry overlap with in-flight acks.
  - **Asymmetric partition.** Broker receives but doesn't send
    (or vice versa) — TCP up, application protocol stalls. Tests
    `socket.timeout.ms` / `request.timeout.ms` paths that pure
    SIGKILL doesn't reach.

  *Config-level:*
  - **Topic config change mid-run.** `cleanup.policy`,
    `retention.ms`, `min.insync.replicas` via `kafka-configs.sh`
    while traffic is flowing.
  - **Share-group config change.** Adjust `share.delivery.count.limit`
    or `share.record.lock.duration.ms` mid-run. Test whether running
    consumers pick up changes vs only new members.
  - **ACL revoke + restore on topic mid-run.** Yanks read permission,
    restores after a few seconds. Tests `TOPIC_AUTHORIZATION_FAILED`
    propagation in the share-consumer error path (not exercised in
    any run yet).

  *Combinations worth designing intentionally:*
  - **Slow consumer + broker roll** — overlapping lock-expiry
    redelivery and leader-migration redelivery.
  - **Topic recreation + active consumer** — `S_NOTEXISTS` handling
    on a session-attached toppar.
  - **Controller kill + leader kill back-to-back** — two SIGKILLs
    where the second targets the new controller; stresses controller
    failover during leader migration.
  - **General:** interleave any of the above with broker rolls for
    maximum surface coverage.

### Top picks for highest signal vs effort

If picking up chaos work without a specific target in mind:

1. **Slow / pausing consumer** — drives `dc[2+]`, currently the
   only verify.txt bucket that never fires in practice. Small
   change to `share_consume_verify`.
2. **Multiple share groups** — share-coordinator multiplexing
   coverage. Orchestrator change only.
3. **Controller-targeted kill** — distinct broker-side failover
   code path from leader-of-data-partition kill.
4. **Variable producer rate** — catch-up phase coverage; bursts
   reveal back-pressure handling.
5. **`acknowledge.type` mix** — RELEASE / REJECT paths in the
   share-consumer ack-state machine.

Each is ~30–50 LoC in the orchestrator + `share_consume_verify`.
