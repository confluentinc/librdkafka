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
 * Added Test scenarios which define the cluster configuration
 * Add MinGW-w64 builds (@ed-alertedh, #2553)
 * `./configure --enable-XYZ` now requires the XYZ check to pass,
   and `--disable-XYZ` disables the feature altogether (@benesch)


## Upgrade considerations

 * Subscribing to non-existent and unauthorized topics will now propagate
   errors `RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART` and
   `RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED` to the application through
   the standard consumer error (the err field in the message object).
 * Consumer will no longer trigger auto creation of topics,
   `allow.auto.create.topics=true` may be used to re-enable the old deprecated
   functionality.
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
 * librdkafka's build tooling now requires Python 3.x (python3 interpreter).


## Fixes

### General fixes

 * The client could crash in rare circumstances on ApiVersion or
   SaslHandshake request timeouts (#2326)


### Consumer fixes

 * The roundrobin partition assignor could crash if subscriptions
   where asymmetrical (different sets from different members of the group).
   Thanks to @ankon and @wilmai for identifying the root cause (#2121).
 * Initial consumer group joins should now be a couple of seconds quicker
   thanks expedited query intervals (@benesch).
 * Don't propagate temporary offset lookup errors to application
 * Reset the stored offset when partitions are un-assign()ed (fixes #2782)

### Producer fixes




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
