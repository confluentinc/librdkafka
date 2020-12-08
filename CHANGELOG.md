# librdkafka v1.5.3

librdkafka v1.5.3 is a maintenance release.

## Upgrade considerations

 * CentOS 6 is now EOL and is no longer included in binary librdkafka packages,
   such as NuGet.

## Fixes

### General fixes

 * Fix a use-after-free crash when certain coordinator requests were retried.


### Consumer fixes

 * Consumer would not filter out messages for aborted transactions
   if the messages were compressed (#3020).
 * Consumer destroy without prior `close()` could hang in certain
   cgrp states (@gridaphobe, #3127).
 * Fix possible null dereference in `Message::errstr()` (#3140).
 * The `roundrobin` partition assignment strategy could get stuck in an
   endless loop or generate uneven assignments in case the group members
   had asymmetric subscriptions (e.g., c1 subscribes to t1,t2 while c2
   subscribes to t2,t3).  (#3159)


# librdkafka v1.5.2

librdkafka v1.5.2 is a maintenance release.


## Upgrade considerations

 * The default value for the producer configuration property `retries` has
   been increased from 2 to infinity, effectively limiting Produce retries to
   only `message.timeout.ms`.
   As the reasons for the automatic internal retries vary (various broker error
   codes as well as transport layer issues), it doesn't make much sense to limit
   the number of retries for retriable errors, but instead only limit the
   retries based on the allowed time to produce a message.
 * The default value for the producer configuration property
   `request.timeout.ms` has been increased from 5 to 30 seconds to match
   the Apache Kafka Java producer default.
   This change yields increased robustness for broker-side congestion.


## Enhancements

 * The generated `CONFIGURATION.md` (through `rd_kafka_conf_properties_show())`)
   now include all properties and values, regardless if they were included in
   the build, and setting a disabled property or value through
   `rd_kafka_conf_set()` now returns `RD_KAFKA_CONF_INVALID` and provides
   a more useful error string saying why the property can't be set.
 * Consumer configs on producers and vice versa will now be logged with
   warning messages on client instantiation.


## Fixes

### Security fixes

 * There was an incorrect call to zlib's `inflateGetHeader()` with
   unitialized memory pointers that could lead to the GZIP header of a fetched
   message batch to be copied to arbitrary memory.
   This function call has now been completely removed since the result was
   not used.
   Reported by Ilja van Sprundel.


### General fixes

 * `rd_kafka_topic_opaque()` (used by the C++ API) would cause object
   refcounting issues when used on light-weight (error-only) topic objects
   such as consumer errors (#2693).
 * Handle name resolution failures when formatting IP addresses in error logs,
   and increase printed hostname limit to ~256 bytes (was ~60).
 * Broker sockets would be closed twice (thus leading to potential race
   condition with fd-reuse in other threads) if a custom `socket_cb` would
   return error.

### Consumer fixes

 * The `roundrobin` `partition.assignment.strategy` could crash (assert)
   for certain combinations of members and partitions.
   This is a regression in v1.5.0. (#3024)
 * The C++ `KafkaConsumer` destructor did not destroy the underlying
   C `rd_kafka_t` instance, causing a leak if `close()` was not used.
 * Expose rich error strings for C++ Consumer `Message->errstr()`.
 * The consumer could get stuck if an outstanding commit failed during
   rebalancing (#2933).
 * Topic authorization errors during fetching are now reported only once (#3072).

### Producer fixes

 * Topic authorization errors are now properly propagated for produced messages,
   both through delivery reports and as `ERR_TOPIC_AUTHORIZATION_FAILED`
   return value from `produce*()` (#2215)
 * Treat cluster authentication failures as fatal in the transactional
   producer (#2994).
 * The transactional producer code did not properly reference-count partition
   objects which could in very rare circumstances lead to a use-after-free bug
   if a topic was deleted from the cluster when a transaction was using it.
 * `ERR_KAFKA_STORAGE_ERROR` is now correctly treated as a retriable
   produce error (#3026).
 * Messages that timed out locally would not fail the ongoing transaction.
   If the application did not take action on failed messages in its delivery
   report callback and went on to commit the transaction, the transaction would
   be successfully committed, simply omitting the failed messages.
 * EndTxnRequests (sent on commit/abort) are only retried in allowed
   states (#3041).
   Previously the transaction could hang on commit_transaction() if an abortable
   error was hit and the EndTxnRequest was to be retried.


*Note: there was no v1.5.1 librdkafka release*



# librdkafka v1.5.0

The v1.5.0 release brings usability improvements, enhancements and fixes to
librdkafka.

## Enhancements

 * Improved broker connection error reporting with more useful information and
   hints on the cause of the problem.
 * Consumer: Propagate errors when subscribing to unavailable topics (#1540)
 * Producer: Add `batch.size` producer configuration property (#638)
 * Add `topic.metadata.propagation.max.ms` to allow newly manually created
   topics to be propagated throughout the cluster before reporting them
   as non-existent. This fixes race issues where CreateTopics() is
   quickly followed by produce().
 * Prefer least idle connection for periodic metadata refreshes, et.al.,
   to allow truly idle connections to time out and to avoid load-balancer-killed
   idle connection errors (#2845)
 * Added `rd_kafka_event_debug_contexts()` to get the debug contexts for
   a debug log line (by @wolfchimneyrock).
 * Added Test scenarios which define the cluster configuration.
 * Added MinGW-w64 builds (@ed-alertedh, #2553)
 * `./configure --enable-XYZ` now requires the XYZ check to pass,
   and `--disable-XYZ` disables the feature altogether (@benesch)
 * Added `rd_kafka_produceva()` which takes an array of produce arguments
   for situations where the existing `rd_kafka_producev()` va-arg approach
   can't be used.
 * Added `rd_kafka_message_broker_id()` to see the broker that a message
   was produced or fetched from, or an error was associated with.
 * Added RTT/delay simulation to mock brokers.


## Upgrade considerations

 * Subscribing to non-existent and unauthorized topics will now propagate
   errors `RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART` and
   `RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED` to the application through
   the standard consumer error (the err field in the message object).
 * Consumer will no longer trigger auto creation of topics,
   `allow.auto.create.topics=true` may be used to re-enable the old deprecated
   functionality.
 * The default consumer pre-fetch queue threshold `queued.max.messages.kbytes`
   has been decreased from 1GB to 64MB to avoid excessive network usage for low
   and medium throughput consumer applications. High throughput consumer
   applications may need to manually set this property to a higher value.
 * The default consumer Fetch wait time has been increased from 100ms to 500ms
   to avoid excessive network usage for low throughput topics.
 * If OpenSSL is linked statically, or `ssl.ca.location=probe` is configured,
   librdkafka will probe known CA certificate paths and automatically use the
   first one found. This should alleviate the need to configure
   `ssl.ca.location` when the statically linked OpenSSL's OPENSSLDIR differs
   from the system's CA certificate path.
 * The heuristics for handling Apache Kafka < 0.10 brokers has been removed to
   improve connection error handling for modern Kafka versions.
   Users on Brokers 0.9.x or older should already be configuring
   `api.version.request=false` and `broker.version.fallback=...` so there
   should be no functional change.
 * The default producer batch accumulation time, `linger.ms`, has been changed
   from 0.5ms to 5ms to improve batch sizes and throughput while reducing
   the per-message protocol overhead.
   Applications that require lower produce latency than 5ms will need to
   manually set `linger.ms` to a lower value.
 * librdkafka's build tooling now requires Python 3.x (python3 interpreter).


## Fixes

### General fixes

 * The client could crash in rare circumstances on ApiVersion or
   SaslHandshake request timeouts (#2326)
 * `./configure --LDFLAGS='a=b, c=d'` with arguments containing = are now
   supported (by @sky92zwq).
 * `./configure` arguments now take precedence over cached `configure` variables
   from previous invocation.
 * Fix theoretical crash on coord request failure.
 * Unknown partition error could be triggered for existing partitions when
   additional partitions were added to a topic (@benesch, #2915)
 * Quickly refresh topic metadata for desired but non-existent partitions.
   This will speed up the initial discovery delay when new partitions are added
   to an existing topic (#2917).


### Consumer fixes

 * The roundrobin partition assignor could crash if subscriptions
   where asymmetrical (different sets from different members of the group).
   Thanks to @ankon and @wilmai for identifying the root cause (#2121).
 * The consumer assignors could ignore some topics if there were more subscribed
   topics than consumers in taking part in the assignment.
 * The consumer would connect to all partition leaders of a topic even
   for partitions that were not being consumed (#2826).
 * Initial consumer group joins should now be a couple of seconds quicker
   thanks expedited query intervals (@benesch).
 * Fix crash and/or inconsistent subscriptions when using multiple consumers
   (in the same process) with wildcard topics on Windows.
 * Don't propagate temporary offset lookup errors to application.
 * Immediately refresh topic metadata when partitions are reassigned to other
   brokers, avoiding a fetch stall of up to `topic.metadata.refresh.interval.ms`. (#2955)
 * Memory for batches containing control messages would not be freed when
   using the batch consume APIs (@pf-qiu, #2990).


### Producer fixes

 * Proper locking for transaction state in EndTxn handler.



# librdkafka v1.4.4

v1.4.4 is a maintenance release with the following fixes and enhancements:

 * Transactional producer could crash on request timeout due to dereferencing
   NULL pointer of non-existent response object.
 * Mark `rd_kafka_send_offsets_to_transaction()` CONCURRENT_TRANSACTION (et.al)
   errors as retriable.
 * Fix crash on transactional coordinator FindCoordinator request failure.
 * Minimize broker re-connect delay when broker's connection is needed to
   send requests.
 * Proper locking for transaction state in EndTxn handler.
 * `socket.timeout.ms` was ignored when `transactional.id` was set.
 * Added RTT/delay simulation to mock brokers.

*Note: there was no v1.4.3 librdkafka release*



# librdkafka v1.4.2

v1.4.2 is a maintenance release with the following fixes and enhancements:

 * Fix produce/consume hang after partition goes away and comes back,
   such as when a topic is deleted and re-created.
 * Consumer: Reset the stored offset when partitions are un-assign()ed (fixes #2782).
    This fixes the case where a manual offset-less commit() or the auto-committer
    would commit a stored offset from a previous assignment before
    a new message was consumed by the application.
 * Probe known CA cert paths and set default `ssl.ca.location` accordingly
   if OpenSSL is statically linked or `ssl.ca.location` is set to `probe`.
 * Per-partition OffsetCommit errors were unhandled (fixes #2791)
 * Seed the PRNG (random number generator) by default, allow application to
   override with `enable.random.seed=false` (#2795)
 * Fix stack overwrite (of 1 byte) when SaslHandshake MechCnt is zero
 * Align bundled c11 threads (tinycthreads) constants to glibc and musl (#2681)
 * Fix return value of rd_kafka_test_fatal_error() (by @ckb42)
 * Ensure CMake sets disabled defines to zero on Windows (@benesch)


*Note: there was no v1.4.1 librdkafka release*





# Older releases

See https://github.com/edenhill/librdkafka/releases
