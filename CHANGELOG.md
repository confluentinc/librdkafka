# confluent-kafka-javascript v1.0.0

v1.0.0 is a feature release. It is supported for all usage.

## Enhancements

1. Add support for an Admin API to fetch topic offsets (#156).
2. Add support for Node v23 pre-built binaries (#158).

## Fixes

1. Fixes an issue where `uv_async_init` was being called off the event loop thread,
   causing the node process to hang (#190).


# confluent-kafka-javascript v0.6.1

v0.6.1 is a limited availability maintenance release. It is supported for all usage.

## Fixes

### Schema Registry

1. Sync aws-sdk packages to the same versioning.

2. Minor enhancement to allow AWS profile to be specified.

3. Add ability to override disable flag and actions on a rule.

# confluent-kafka-javascript v0.6.0

v0.6.0 is a limited availability maintenance release. It is supported for all usage.

## Fixes

### Schema Registry

1. Add AWS AssumeRole support to AWS KMS. You can now specify a role arn, and optional
role session name and optional role external id.

2. Ensure different key ids use different client instances.

# confluent-kafka-javascript v0.5.2

v0.5.2 is a limited availability maintenance release. It is supported for all usage.

Note: v0.5.2 has no code changes from v0.5.1, version is bumped to sync with @confluentinc/schemaregistry.


# confluent-kafka-javascript v0.5.1

v0.5.1 is a limited availability maintenance release. It is supported for all usage.

## Fixes

1. Fix an issue where `sendOffsets` wasn't working correctly in the transactional
   producer (#172).


# confluent-kafka-javascript v0.5.0

v0.5.0 is a limited availability feature release. It is supported for all usage.

Note: v0.5.0 was not released because of the bug mentioned in v0.5.1 section. Instead,
      v0.5.1 was released with the fix directly.

## Enhancements

1. Add support for an Admin API to delete records.(#141).
2. Add support for an Admin API to describe topics.(#155).
3. Add support for dependent Admin client (#153).
4. References librdkafka v2.6.1. Refer to the [librdkafka v2.6.1 release notes](https://github.com/confluentinc/librdkafka/releases/tag/v2.6.1) for more information.

## Fixes

1. Fixes an issue with unresolved raced Promises leaking in the consumer (#151).
2. Removes schemaregistry dependencies from root package.json to prevent issues with Node 18 (#162).


# confluent-kafka-javascript v0.4.0

v0.4.0 is a limited availability feature release. It is supported for all usage.

## Enhancements

1. Fixes an issue where headers were not passed correctly to the `eachBatch` callback (#130).
2. Add support for an Admin API to list a consumer group's offsets (#49).
3. Reduce consumer poll timeout to nil and add wakeups for new messages. This improves
   the consumer efficiency, and resolves issues while running multiple consumers within
   the same node process (#135).
4. Add pre-built binaries for macOS (Intel).


# confluent-kafka-javascript v0.3.0

v0.3.0 is a limited availability feature release. It is supported for all usage.

## Enhancements

1. References librdkafka v2.6.0. Refer to the [librdkafka v2.6.0 release notes](https://github.com/confluentinc/librdkafka/releases/tag/v2.6.0) for more information.
1. Minor optimization to reduce schema ID lookups (#123).


# confluent-kafka-javascript v0.2.1

v0.2.1 is a limited availability release. It is supported for all usage.

## Features

1. Update README, docs, and examples for Confluent's Schema Registry client.


# confluent-kafka-javascript v0.2.0

v0.2.0 is a limited availability release. It is supported for all usage.

## Features

1. Switch to using `librdkafka` on the latest released tag `v2.5.3` instead of `master`.


# confluent-kafka-javascript v0.1.17-devel

v0.1.17-devel is a pre-production, early-access release.

## Features

1. Add a commitCb method to the callback-based API which allows committing asynchronously.
2. Pass assign/unassign functions to the rebalance callback in the promisified API, allowing
   the user to control the assignment of partitions, or pause just after a rebalance.
3. Remove store from promisified API and let the library handle all the stores.
4. Add JavaScript-level debug logging to the client for debugging issues within the binding.
5. Various fixes for performance and robustness of the consumer cache.
6. Remove `consumerGroupId` argument from the `sendOffsets` method of the transactional producer,
   and instead, only allow using a `consumer`.

## Fixes

1. Do not modify RegExps which don't start with a ^, instead, throw an error so
   that there is no unexpected behaviour for the user (Issue [#64](https://github.com/confluentinc/confluent-kafka-javascript/issues/64)).
2. Do not mutate arguments in run, pause and resume (Issue [#61](https://github.com/confluentinc/confluent-kafka-javascript/issues/61)).
3. Fix a segmentation fault in `listGroups` when passing `matchConsumerGroupStates` as undefined.


# confluent-kafka-javascript v0.1.16-devel

v0.1.16-devel is a pre-production, early-access release.

## Features

1. Add per-partition concurrency to consumer.
2. Add true `eachBatch` support to consumer.
3. Add a `leaderEpoch` field to the topic partitions where required (listing, committing, etc.).


# confluent-kafka-javascript v0.1.15-devel

v0.1.15-devel is a pre-production, early-access release.

## Features

1. Add Node v22 builds and bump librdkafka version on each version bump of this library.


# confluent-kafka-javascript v0.1.14-devel

v0.1.14-devel is a pre-production, early-access release.

## Features

1. Add metadata to offset commit and offset store (non-promisified API).
2. Add types for logger and loglevel to configuration.
3. Add Producer polling from background thread. This improves performance for cases when send is awaited on.
4. Enable consume optimization from v0.1.13-devel (Features #2) by default for the promisified API.

## Bug Fixes

1. Fix issues with the header conversions from promisified API to the non-promisified API to match
   the type signature and allow Buffers to be passed as header values in the C++ layer.


# confluent-kafka-javascript v0.1.13-devel

v0.1.13-devel is a pre-production, early-access release.

## Features

1. Add support for `storeOffsets` in the consumer API.
2. Add optimization while consuming, in cases where the size of messages pending in our subscription is less than the consumer cache size.

## Bug Fixes

1. Fix memory leak in incremental assign (@martijnimhoff, #35).
2. Fix various issues with typings, and reconcile typings, JavaScript code, and MIGRATION.md to be consistent.


# confluent-kafka-javascript v0.1.12-devel

v0.1.12-devel is a pre-production, early-access release.

## Features

1. Add support for `listTopics` in the Admin API.
2. Add support for OAUTHBEARER token refresh callback for both promisified and non promisified API.

## Bug Fixes

1. Fix aliasing bug between `NodeKafka::Conf` and `RdKafka::ConfImpl`.
2. Fix issue where `assign/unassign` were called instead of `incrementalAssign/incrementalUnassign` while using
   the Cooperative Sticky assigner, and setting the `rebalance_cb` as a boolean rather than as a function.
3. Fix memory leaks in Dispatcher and Conf (both leaked memory at client close).
4. Fix type definitions and make `KafkaJS` and `RdKafka` separate namespaces, while maintaining compatibility
   with node-rdkafka's type definitions.


# confluent-kafka-javascript v0.1.11-devel

v0.1.11-devel is a pre-production, early-access release.

## Features

1. Add support for `eachBatch` in the Consumer API (partial support for API compatibility).
2. Add support for `listGroups`, `describeGroups` and `deleteGroups` in the Admin API.


# confluent-kafka-javascript v0.1.10-devel

v0.1.10-devel is a pre-production, early-access release.

## Features

1. Pre-built binaries for Windows (x64) added on an experimental basis.


# confluent-kafka-javascript v0.1.9-devel

v0.1.9-devel is a pre-production, early-access release.

## Features

1. Pre-built binaries for Linux (both amd64 and arm64, both musl and glibc), for macOS (m1), for node versions 18, 20 and 21.
2. Promisified API for Consumer, Producer and Admin Client.
3. Allow passing topic configuration properties via the global configuration block.
4. Remove dependencies with security issues.
5. Support for the Cooperative Sticky assignor.
