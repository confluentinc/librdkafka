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
