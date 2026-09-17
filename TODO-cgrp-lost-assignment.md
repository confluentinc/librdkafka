# Follow-ups: lost-assignment commits (classic protocol)

Local reminder, not for the PR. Written 2026-09-17 on branch
`fix/cgrp-empty-member-id-commit-on-lost` (v2.15.1 + `95d6ecfb3` "Don't commit
offsets of a lost assignment" + `bc4170b1f` "Clear assignment lost flag when
unassign completes").

## Where things stand

- `tests/0192-cgrp_static_session_timeout_mock.c` has three passing subtests
  (static + cooperative + session timeout, eager + session timeout,
  cooperative + max.poll trip), each ending with a recovery check. The file is
  modified but not committed. `make style-fix` was not run locally (no
  clang-format-18); rely on CI.
- Two real gaps remain in the fix itself, both classic-protocol only.

## Open gaps

**#12, explicit-partition commit while lost.** An app that calls
`rd_kafka_commit(rk, revoked_partitions, ...)` from its rebalance callback on a
lost REVOKE bypasses the lost guard, because `rd_kafka_cgrp_offsets_commit`
(`src/rdkafka_cgrp.c:4598`) only checks the lost flag when
`rko_u.offset_commit.partitions == NULL`. The commit goes out with an empty
member id; a static member gets a fatal `FENCED_INSTANCE_ID`. Common pattern in
manual-commit apps.

**#13, max.poll trip during a pending REVOKE.** A vanilla app with
`enable.auto.commit=true` and no callback is affected. If a normal rebalance is
already in progress and the app then exceeds `max.poll.interval.ms`,
`rd_kafka_cgrp_max_poll_interval_check_tmr_cb` (`:5718`) resets the member id
to `""`, but `rd_kafka_cgrp_revoke_all_rejoin_maybe` (`:5556`) returns early
from its `RD_KAFKA_CGRP_REBALANCING` branch (`:5560`), so the lost flag is never
set. When the app polls again, librdkafka's own revoke-time auto-commit goes out
with the empty member id; a static member is fatally fenced. The same window
lets the auto-commit timer commit kept partitions with an empty member id
during the rejoin.

## Sized fix

**Core, ~10-15 lines, one function.** In `rd_kafka_cgrp_offsets_commit`, right
before the request is built, fail the commit locally with
`RD_KAFKA_RESP_ERR__ASSIGNMENT_LOST` when `rkcg->rkcg_subscription != NULL` and
`rkcg_member_id` is empty. That state only exists after a session-timeout or
max.poll reset, and the coordinator would reject the commit anyway
(`UNKNOWN_MEMBER_ID`, or fatal `FENCED_INSTANCE_ID` for static members). Every
commit path funnels through this function (auto-commit timer, revoke-time
commit, explicit commits with or without partitions, deferred commits on
reprocess), so it closes #12 and #13 together. `assign()` mode is untouched
(`rkcg_subscription` is NULL); KIP-848 is untouched (member id is never reset).

**Optional companion, ~5 lines.** In the `RD_KAFKA_CGRP_REBALANCING` branch of
`rd_kafka_cgrp_revoke_all_rejoin_maybe`, call
`rd_kafka_cgrp_assignment_set_lost` when `assignment_lost` is true so
`rd_kafka_assignment_lost()` is accurate inside the pending revoke callback.
Visibility only, not needed for correctness once the funnel guard is in.

**Tests, medium, ~150 lines.** Add a fourth 0192 subtest for #13: a second
consumer joins to force a normal revoke; the first consumer's rebalance
callback sleeps past `max.poll.interval.ms` before unassigning; assert zero
OffsetCommit requests and no fatal error. Add a #12 subtest that commits the
revoked partitions explicitly from the callback and expects
`_ASSIGNMENT_LOST` non-fatally, then a successful commit after rejoin.

**Risk.** Few lines, but on every commit path. Run 0106, 0113, 0116, 0147,
0148, 0192, plus the tests that commit in `assign()` mode, and one
broker-backed run. CHANGELOG entry: explicit commits in this state now return
`_ASSIGNMENT_LOST` locally instead of a broker error.

## Remaining proposed tests (from the 13-item plan)

Pins for the corrected fix (flag must clear when the unassign completes):

- [ ] 1. Deleted-topic partial revoke (`rd_kafka_mock_topic_delete`), under
  both `group.protocol=classic` and `consumer`; only path that exercises the
  KIP-848 clear site.
- [ ] 2. Rebalance-callback path: `assignment_lost()` true inside the lost
  REVOKE, false inside the following ASSIGN. This is what the first fix broke.
- [ ] 3. Auto-commit timer: zero timer commits while lost, timer commits
  resume after rejoin.
- [ ] 4. `rd_kafka_consumer_close()` while lost: no hang, no empty-member-id
  commit, no fatal.
- [ ] 5. Unsubscribe while lost: LeaveGroup sent, no commit.

Variants of the existing scenario:

- [ ] 6. Dynamic member (no `group.instance.id`).
- [ ] 7. Manual commit while lost with `enable.auto.commit=false`:
  `rd_kafka_commit(NULL)` returns `_ASSIGNMENT_LOST`, succeeds after rejoin.
- [ ] 8. Eager + max.poll (last missing cell of the trigger-by-assignor
  matrix).
- [ ] 9. Redelivery semantics: commit at X, consume to Y, lose, rejoin,
  consumption restarts at X.
- [ ] 10. Symptom-level: push `FENCED_INSTANCE_ID` onto OffsetCommit during the
  lost window, assert no fatal; clear pushed errors before the recovery commit.
- [ ] 11. Max.poll trip with short `session.timeout.ms` so the mock broker
  evicts the member first; same instance id rejoins cleanly.

Probes of the open gaps (fail on the current branch until the fix above lands):

- [ ] 12. Explicit-partition commit from the callback.
- [ ] 13. Max.poll trip during a pending REVOKE.

Suggested order: 1, 2, 10 first (regression that already happened once, the
KIP-848 clear site, the user-visible symptom).

## Test-writing notes

- `test_consumer_poll*` helpers TEST_FAIL on any non-EOF message error; use a
  raw `rd_kafka_consumer_poll` discard loop when `ERR__MAX_POLL_EXCEEDED` is
  expected.
- Discard loops can drain all seed messages; produce a fresh message after the
  rejoin-wait loop before any "consume one" recovery check.
- `test_conf_init(conf, tconf, 30)` is enforced at 30s * 2.7 locally.
- Run with `cd tests && TESTS=0192 make -j8`.
