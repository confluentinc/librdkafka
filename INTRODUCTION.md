# Introduction to librdkafka - the Apache Kafka C client library


librdkafka is a high performance C implementation of the Apache
Kafka client, providing a reliable and performant client for production use.

## Contents

The following chapters are available in this document

  * Performance
    * Performance numbers
    * High throughput
    * Low latency
    * Compression
  * Message reliability
  * Usage
    * Documentation
    * Initialization
    * Configuration
    * Threads and callbacks
    * Brokers
    * Producer API
    * Consumer API
  * Appendix
    * Test detailts
  



## Performance

librdkafka is a multi-threaded library designed for use on modern hardware and
it attempts to keep memory copying at a minimal. The payload of produced or
consumed messages may pass through without any copying
(if so desired by the application) putting no limit on message sizes.

> *"You can have high throughput or low latency, but you cant have both"*

librdkafka allows you to decide if high throughput is the name of the game,
or if a low latency service is required, all through the configuration
property interface.

The two most important configuration properties for performance tuning are:

  * batch.num.messages - the minimum number of messages to wait for to
	  accumulate in the local queue before sending off a message set.
  * queue.buffering.max.ms - how long to wait for batch.num.messages to
	  fill up in the local queue.


### Performance numbers

The following performance numbers stem from tests using the following setup:

  * Intel Quad Core i7 at 3.4GHz, 8GB of memory
  * Disk performance has been shortcut by setting the brokers' flush
	configuration properties as so:
	* `log.flush.interval.messages=10000000`
	* `log.flush.interval.ms=100000`
  * Two brokers running on the same machine as librdkafka.
  * One topic with two partitions.
  * Each broker is leader for one partition each.
  * Using `rdkafka_performance` program available in the `examples` subdir.



	

**Test results**

  * **Test1**: 2 brokers, 2 partitions, required.acks=2, 100 byte messages: 
	  **850000 messages/second**, **85 MB/second**

  * **Test2**: 1 broker, 1 partition, required.acks=0, 100 byte messages: 
	  **710000 messages/second**, **71 MB/second**
	  
  * **Test3**: 2 broker2, 2 partitions, required.acks=2, 100 byte messages,
	  snappy compression:
	  **300000 messages/second**, **30 MB/second**

  * **Test4**: 2 broker2, 2 partitions, required.acks=2, 100 byte messages,
	  gzip compression:
	  **230000 messages/second**, **23 MB/second**



**Note**: See the *Test details* chapter at the end of this document for
	information about the commands executed, etc.

	


### High throughput

The key to high throughput is message batching - waiting for a certain amount
of messages to accumulate in the local queue before sending them off in
one large message set or batch to the peer. This amortizes the messaging
overhead and eliminates the adverse effect of the round trip time (rtt).

The default settings, batch.num.messages=1000 and queue.buffering.max.ms=1000,
are suitable for high throughput. This allows librdkafka to wait up to
1000 ms for up to 1000 messages to accumulate in the local queue before
sending the accumulate messages to the broker.

These setting are set globally (`rd_kafka_conf_t`) but applies on a
per topic+partition basis.


### Low latency

When low latency messaging is required the "queue.buffering.max.ms" should be
tuned to the maximum permitted producer-side latency.
Setting queue.buffering.max.ms to 0 will make sure messages are sent as
soon as possible.


### Compression

Producer message compression is enabled through the "compression.codec"
configuration property.

Compression is performed on the batch of messages in the local queue, the
larger the batch the higher likelyhood of a higher compression ratio.
The local batch queue size is controlled through the "batch.num.messages" and
"queue.buffering.max.ms" configuration properties as described in the
**High throughput** chapter above.



## Message reliability

Message reliability is an important factor of librdkafka - an application
can rely fully on librdkafka to deliver a message according to the specified
configuration ("request.required.acks" and "message.send.max.retries", etc).

If the topic configuration property "request.required.acks" is set to wait
for message commit acknowledgements from brokers (any value but 0, see
[`rdkafka.h`](https://github.com/edenhill/librdkafka/blob/master/rdkafka.h)
for specifics) then librdkafka will hold on to the message until
all expected acks have been received, gracefully handling the following events:
     
  * Broker connection failure
  * Topic leader change
  * Produce errors signaled by the broker

This is handled automatically by librdkafka and the application does not need
to take any action at any of the above events.
The message will be resent up to "message.send.max.retries" times before
reporting a failure back to the application.

The delivery report callback is used by librdkafka to signal the status of
a message back to the application, it will be called once for each message
to report the status of message delivery:

  * If `error_code` is non-zero the message delivery failed and the error_code
    indicates the nature of the failure (`rd_kafka_resp_err_t` enum).
  * If `error_code` is zero the message has been successfully delivered.

See Producer API chapter for more details on delivery report callback usage.

The delivery report callback is optional.






## Usage

### Documentation

The librdkafka API is documented in the
[`rdkafka.h`](https://github.com/edenhill/librdkafka/blob/master/rdkafka.h)
header file, including the supported configuration properties.


### Initialization

The application needs to instantiate a top-level object `rd_kafka_t` which is
the base container, providing global configuration and shared state.
It is created by calling `rd_kafka_new()`.

It also needs to instantiate one or more topics (`rd_kafka_topic_t`) to be used
for producing to or consuming from. The topic object holds topic-specific
configuration and will be internally populated with a mapping of all available
partitions and their leader brokers.
It is created by calling `rd_kafka_topic_new()`.

Both `rd_kafka_t` and `rd_kafka_topic_t` comes with a configuration API which
is optional.
Not using the API will cause librdkafka to use its default values which are
documented *`CONFIGURATION.md`*.

**Note**: An application may create multiple `rd_kafka_t` objects and
	they share no state.

**Note**: An `rd_kafka_topic_t` object may only be used with the `rd_kafka_t`
	object it was created from.



### Configuration

To ease integration with the official Apache Kafka software and lower
the learning curve, librdkafka implements identical configuration
properties as found in the official clients of Apache Kafka.

Configuration is applied prior to object creation using the
`rd_kafka_conf_set()` and `rd_kafka_topic_conf_set()` APIs.

**Note**: The `rd_kafka.._conf_t` objects are not reusable after have been
	passed to `rd_kafka.._new()`.
	The application does not need to free any config resources after a
	`rd_kafka.._new()` call.

#### Example

    rd_kafka_conf_t *conf;
    char errstr[512];
    
    conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "compression.codec", "snappy", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "batch.num.messages", "100", errstr, sizeof(errstr));
    
    rd_kafka_new(RD_KAFKA_PRODUCER, conf);


### Threads and callbacks

librdkafka uses multiple threads internally to fully utilize modern hardware.
The API is completely thread-safe and the calling application may call any
of the API functions from any of its own threads at any time.

A poll-based API is used to provide signaling back to the application,
the application should call rd_kafka_poll() at regular intervals.
The poll API will call the following configured callbacks (optional):

  * message delivery report callback - signals that a message has been
    delivered or failed delivery, allowing the application to take action
    and to release any application resources used in the message.
  * error callback - signals an error. These errors are usually of an
    informational nature, i.e., failure to connect to a broker, and the
    application usually does not need to take any action.
    The type of error is passed as a rd_kafka_resp_err_t enum value,
    including both remote broker errors as well as local failures.


Optional callbacks not triggered by poll, these may be called from any thread:

  * Logging callback - allows the application to output log messages
	  generated by librdkafka.
  * partitioner callback - application provided message partitioner.
	  The partitioner may be called in any thread at any time, it may be
	  called multiple times for the same key.
	  Partitioner function contraints:
	  * MUST NOT call any rd_kafka_*() functions
      * MUST NOT block or execute for prolonged periods of time.
      * MUST return a value between 0 and partition_cnt-1, or the
          special RD_KAFKA_PARTITION_UA value if partitioning
              could not be performed.



### Brokers

librdkafka only needs an initial list of brokers (at least one), called the
bootstrap brokers.
It will connect to all the bootstrap brokers, specified by the
"metadata.broker.list" configuration property or by `rd_kafka_brokers_add()`,
and query each one for Metadata information which contains the full list of
brokers, topic, partitions and their leaders in the Kafka cluster.

Broker names are specified as "host[:port]" where the port is optional 
(default 9092) and the host is either a resolvable hostname or an IPv4 or IPv6
address.
If host resolves to multiple addresses librdkafka will round-robin the
addresses for each connection attempt.
A DNS recording containing all broker address can thus be used to provide a
reliable bootstrap broker.


### Producer API

After setting up the `rd_kafka_t` object and one or more `rd_kafka_topic_t`
objects librdkafka is ready for accepting messages to be produced and sent to
brokers.

The `rd_kafka_produce()` function takes the following arguments:

  * `rkt` - the topic to produce to, previously created with
	  `rd_kafka_topic_new()`
  * `partition` - partition to produce to. If this is set to
	  `RD_KAFKA_PARTITION_UA` (UnAssigned) then the configured partitioner
		  function will be used to select a target partition.
  * `payload`,`len` - the message payload
  * `msgflags` - 0, or one of:
	  * `RD_KAFKA_MSG_F_COPY` - librdkafka will immediately make a copy of
		  the payload. Use this when the payload is in non-persistent memory,
		  such as the stack.
	  * `RD_KAFKA_MSG_F_FREE` - let librdkafka free the payload using `free(3)`
		  when it is done with it.
	
	These two flags are mutually exclusive and neither need to be set in
	which case the payload is neither copied nor freed by librdkafka.
		
	If `RD_KAFKA_MSG_F_COPY` flag is not set no data copying will be
	performed and librdkafka will hold on the payload pointer until
	the message	has been delivered or fails.
	The delivery report callback will be called when librdkafka is done
	with the message to let the application regain ownership of the
	payload memory.
	The application must not free the payload in the delivery report
	callback if `RD_KAFKA_MSG_F_FREE is set`.
	
  * `key`,`keylen` - an optional message key which can be used for partitioning.
	  It will be passed to the topic partitioner callback, if any, and
	  will be attached to the message when sending to the broker.
  * `msg_opaque` - an optional application-provided per-message opaque pointer
	  that will be provided in the message delivery callback to let
	  the application reference a specific message.


`rd_kafka_produce()` is a non-blocking API, it will enqueue the message
on an internal queue and return immediately.
If the number of queued messages would exceed the "queue.buffering.max.messages"
configuration property then `rd_kafka_produce()` returns -1 and sets errno
to `ENOBUFS`, thus providing a backpressure mechanism.



### Consumer API

To be announced.








## Appendix

### Test details

#### Test1: Produce to two brokers, two partitions, required.acks=2, 100 byte messages

Each broker is leader for one of the two partitions.
The random partitioner is used (default) and each broker and partition is
assigned approximately 250000 messages each.

**Command:**

    # examples/rdkafka_performance -P -t test2 -s 100 -c 500000 -m "_____________Test1:TwoBrokers:500kmsgs:100bytes" -S 1 -a 2
	....
    % 500000 messages and 50000000 bytes sent in 587ms: 851531 msgs/s and 85.15 Mb/s, 0 messages failed, no compression

**Result:**

Message transfer rate is approximately **850000 messages per second**,
**85 megabytes per second**.



#### Test2: Produce to one broker, one partition, required.acks=0, 100 byte messages

**Command:**

    # examples/rdkafka_performance -P -t test2 -s 100 -c 500000 -m "_____________Test2:OneBrokers:500kmsgs:100bytes" -S 1 -a 0 -p 1
	....
	% 500000 messages and 50000000 bytes sent in 698ms: 715994 msgs/s and 71.60 Mb/s, 0 messages failed, no compression

**Result:**

Message transfer rate is approximately **710000 messages per second**,
**71 megabytes per second**.



#### Test3: Produce to two brokers, two partitions, required.acks=2, 100 byte messages, snappy compression

**Command:**

	# examples/rdkafka_performance -P -t test2 -s 100 -c 500000 -m "_____________Test3:TwoBrokers:500kmsgs:100bytes:snappy" -S 1 -a 2 -z snappy
	....
	% 500000 messages and 50000000 bytes sent in 1672ms: 298915 msgs/s and 29.89 Mb/s, 0 messages failed, snappy compression

**Result:**

Message transfer rate is approximately **300000 messages per second**,
**30 megabytes per second**.


#### Test4: Produce to two brokers, two partitions, required.acks=2, 100 byte messages, gzip compression

**Command:**

	# examples/rdkafka_performance -P -t test2 -s 100 -c 500000 -m "_____________Test3:TwoBrokers:500kmsgs:100bytes:gzip" -S 1 -a 2 -z gzip
	....
	% 500000 messages and 50000000 bytes sent in 2111ms: 236812 msgs/s and 23.68 Mb/s, 0 messages failed, gzip compression

**Result:**

Message transfer rate is approximately **230000 messages per second**,
**23 megabytes per second**.

