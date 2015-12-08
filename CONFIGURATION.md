//@file
## Global configuration properties

Property                                 | C/P |       Default | Description              
-----------------------------------------|-----|--------------:|--------------------------
builtin.features                         |  *  |               | Indicates the builtin features for this build of librdkafka. An application can either query this value or attempt to set it with its list of required features to check for library support.
client.id                                |  *  |       rdkafka | Client identifier.
metadata.broker.list                     |  *  |               | Initial list of brokers. The application may also use `rd_kafka_brokers_add()` to add brokers during runtime.
bootstrap.servers                        |  *  |               | Alias for `metadata.broker.list`
message.max.bytes                        |  *  |       1000000 | Maximum transmit message size.
receive.message.max.bytes                |  *  |     100000000 | Maximum receive message size. This is a safety precaution to avoid memory exhaustion in case of protocol hickups. The value should be at least fetch.message.max.bytes * number of partitions consumed from + messaging overhead (e.g. 200000 bytes).
metadata.request.timeout.ms              |  *  |         60000 | Non-topic request timeout in milliseconds. This is for metadata requests, etc.
topic.metadata.refresh.interval.ms       |  *  |        300000 | Topic metadata refresh interval in milliseconds. The metadata is automatically refreshed on error and connect. Use -1 to disable the intervalled refresh.
topic.metadata.refresh.fast.cnt          |  *  |            10 | When a topic looses its leader this number of metadata requests are sent with `topic.metadata.refresh.fast.interval.ms` interval disregarding the `topic.metadata.refresh.interval.ms` value. This is used to recover quickly from transitioning leader brokers.
topic.metadata.refresh.fast.interval.ms  |  *  |           250 | See `topic.metadata.refresh.fast.cnt` description
topic.metadata.refresh.sparse            |  *  |          true | Sparse metadata requests (consumes less network bandwidth)
topic.blacklist                          |  *  |               | Topic blacklist, a comma-separated list of regular expressions for matching topic names that should be ignored in broker metadata information as if the topics did not exist.
debug                                    |  *  |               | A comma-separated list of debug contexts to enable: all,generic,broker,topic,metadata,producer,queue,msg,protocol,cgrp,security,fetch
socket.timeout.ms                        |  *  |         60000 | Timeout for network requests.
socket.blocking.max.ms                   |  *  |           100 | Maximum time a broker socket operation may block. A lower value improves responsiveness at the expense of slightly higher CPU usage.
socket.send.buffer.bytes                 |  *  |             0 | Broker socket send buffer size. System default is used if 0.
socket.receive.buffer.bytes              |  *  |             0 | Broker socket receive buffer size. System default is used if 0.
socket.keepalive.enable                  |  *  |         false | Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets
socket.max.fails                         |  *  |             3 | Disconnect from broker when this number of send failures (e.g., timed out requests) is reached. Disable with 0. NOTE: The connection is automatically re-established.
broker.address.ttl                       |  *  |        300000 | How long to cache the broker address resolving results.
broker.address.family                    |  *  |           any | Allowed broker IP address families: any, v4, v6
statistics.interval.ms                   |  *  |             0 | librdkafka statistics emit interval. The application also needs to register a stats callback using `rd_kafka_conf_set_stats_cb()`. The granularity is 1000ms. A value of 0 disables statistics.
error_cb                                 |  *  |               | Error callback (set with rd_kafka_conf_set_error_cb())
throttle_cb                              |  *  |               | Throttle callback (set with rd_kafka_conf_set_throttle_cb())
stats_cb                                 |  *  |               | Statistics callback (set with rd_kafka_conf_set_stats_cb())
log_cb                                   |  *  |               | Log callback (set with rd_kafka_conf_set_log_cb())
log_level                                |  *  |             6 | Logging level (syslog(3) levels)
log.thread.name                          |  *  |         false | Print internal thread name in log messages (useful for debugging librdkafka internals)
socket_cb                                |  *  |               | Socket creation callback to provide race-free CLOEXEC
open_cb                                  |  *  |               | File open callback to provide race-free CLOEXEC
opaque                                   |  *  |               | Application opaque (set with rd_kafka_conf_set_opaque())
default_topic_conf                       |  *  |               | Default topic configuration for automatically subscribed topics
internal.termination.signal              |  *  |             0 | Signal that librdkafka will use to quickly terminate on rd_kafka_destroy(). If this signal is not set then there will be a delay before rd_kafka_wait_destroyed() returns true as internal threads are timing out their system calls. If this signal is set however the delay will be minimal. The application should mask this signal as an internal signal handler is installed.
quota.support.enable                     |  *  |         false | Enables application forwarding of broker's throttle time for Produce and Fetch (consume) requests. Whenever a Produce or Fetch request is returned with a non-zero throttle time (how long the broker throttled the request to enforce configured quota rates) a throttle_cb will be enqueued for the next call to `rd_kafka_poll()`. The same is also true for the first non-throttled request following a throttled request. Requires Kafka brokers >=0.9.0 with quotas enabled.
protocol.version                         |  *  |             0 | Broker protocol version. Since there is no way for a client to know what protocol version is used by the broker it can't know which API version to use for certain protocol requests. This property is used to hint the client of the broker version. Format is 0xMMmmrrpp where MM=Major, mm=minor, rr=revision, pp=patch, e.g., 0x00080200 for 0.8.2. A version of 0 means an optimistic approach where the client assumes the latest version of APIs are supported.
security.protocol                        |  *  |     plaintext | Protocol used to communicate with brokers.
ssl.cipher.suites                        |  *  |               | A cipher suite is a named combination of authentication, encryption, MAC and key exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL network protocol. See manual page for `ciphers(1)` and `SSL_CTX_set_cipher_list(3).
ssl.key.location                         |  *  |               | Path to client's private key (PEM) used for authentication.
ssl.key.password                         |  *  |               | Private key passphrase
ssl.certificate.location                 |  *  |               | Path to certificate file for verifying the broker's key.
ssl.ca.location                          |  *  |               | File or directory path to CA certificate(s) for verifying the broker's key.
sasl.mechanisms                          |  *  |        GSSAPI | Space separated list of eligible SASL mechanisms
sasl.kerberos.service.name               |  *  |         kafka | Kerberos principal name that Kafka runs as.
sasl.kerberos.principal                  |  *  |   kafkaclient | This client's Kerberos principal name.
sasl.kerberos.kinit.cmd                  |  *  |         kinit | Kerberos kinit command path.
sasl.kerberos.keytab                     |  *  |               | Path to Kerberos keytab file. Uses system default if not set.
sasl.kerberos.min.time.before.relogin    |  *  |         60000 | Minimum time in milliseconds between key refresh attempts.
group.id                                 |  *  |               | Client group id string. All clients sharing the same group.id belong to the same group.
partition.assignment.strategy            |  *  | range,roundrobin | Name of partition assignment strategy to use when elected group leader assigns partitions to group members.
session.timeout.ms                       |  *  |         30000 | Client group session and failure detection timeout.
heartbeat.interval.ms                    |  *  |          1000 | Group session keepalive heartbeat interval.
group.protocol.type                      |  *  |      consumer | Group protocol type
coordinator.query.interval.ms            |  *  |        600000 | How often to query for the current client group coordinator. If the currently assigned coordinator is down the configured query interval will be divided by ten to more quickly recover in case of coordinator reassignment.
enable.auto.commit                       |  C  |          true | Automatically and periodically commit offsets in the background.
auto.commit.interval.ms                  |  C  |          5000 | The frequency in milliseconds that the consumer offsets are commited (written) to offset storage. (0 = disable)
queued.min.messages                      |  C  |        100000 | Minimum number of messages per topic+partition in the local consumer queue.
queued.max.messages.kbytes               |  C  |       1000000 | Maximum number of kilobytes per topic+partition in the local consumer queue. This value may be overshot by fetch.message.max.bytes.
fetch.wait.max.ms                        |  C  |           100 | Maximum time the broker may wait to fill the response with fetch.min.bytes.
fetch.message.max.bytes                  |  C  |       1048576 | Maximum number of bytes per topic+partition to request when fetching messages from the broker.
max.partition.fetch.bytes                |  C  |               | Alias for `fetch.message.max.bytes`
fetch.min.bytes                          |  C  |             1 | Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting.
fetch.error.backoff.ms                   |  C  |           500 | How long to postpone the next fetch request for a topic+partition in case of a fetch error.
offset.store.method                      |  C  |        broker | Offset commit store method: 'file' - local file store (offset.store.path, et.al), 'broker' - broker commit store (requires Apache Kafka 0.8.2 or later on the broker).
consume_cb                               |  C  |               | Message consume callback (set with rd_kafka_conf_set_consume_cb())
rebalance_cb                             |  C  |               | Called after consumer group has been rebalanced (set with rd_kafka_conf_set_rebalance_cb())
offset_commit_cb                         |  C  |               | Offset commit result propagation callback. (set with rd_kafka_conf_set_offset_commit_cb())
queue.buffering.max.messages             |  P  |        100000 | Maximum number of messages allowed on the producer queue.
queue.buffering.max.ms                   |  P  |          1000 | Maximum time, in milliseconds, for buffering data on the producer queue.
message.send.max.retries                 |  P  |             2 | How many times to retry sending a failing MessageSet. **Note:** retrying may cause reordering.
retry.backoff.ms                         |  P  |           100 | The backoff time in milliseconds before retrying a message send.
compression.codec                        |  P  |          none | Compression codec to use for compressing message sets: none, gzip or snappy
batch.num.messages                       |  P  |          1000 | Maximum number of messages batched in one MessageSet.
delivery.report.only.error               |  P  |         false | Only provide delivery reports for failed messages.
dr_cb                                    |  P  |               | Delivery report callback (set with rd_kafka_conf_set_dr_cb())
dr_msg_cb                                |  P  |               | Delivery report callback (set with rd_kafka_conf_set_dr_msg_cb())


## Topic configuration properties

Property                                 | C/P |       Default | Description              
-----------------------------------------|-----|--------------:|--------------------------
request.required.acks                    |  P  |             1 | This field indicates how many acknowledgements the leader broker must receive from ISR brokers before responding to the request: *0*=broker does not send any response, *1*=broker will wait until the data is written to local log before sending a response, *-1*=broker will block until message is committed by all in sync replicas (ISRs) or broker's `in.sync.replicas` setting before sending response. *1*=Only the leader broker will need to ack the message. 
request.timeout.ms                       |  P  |          5000 | The ack timeout of the producer request in milliseconds. This value is only enforced by the broker and relies on `request.required.acks` being > 0.
message.timeout.ms                       |  P  |        300000 | Local message timeout. This value is only enforced locally and limits the time a produced message waits for successful delivery. A time of 0 is infinite.
produce.offset.report                    |  P  |         false | Report offset of produced message back to application. The application must be use the `dr_msg_cb` to retrieve the offset from `rd_kafka_message_t.offset`.
partitioner_cb                           |  P  |               | Partitioner callback (set with rd_kafka_topic_conf_set_partitioner_cb())
opaque                                   |  *  |               | Application opaque (set with rd_kafka_topic_conf_set_opaque())
compression.codec                        |  P  |       inherit | Compression codec to use for compressing message sets: none, gzip or snappy
auto.commit.enable                       |  C  |          true | If true, periodically commit offset of the last message handed to the application. This commited offset will be used when the process restarts to pick up where it left off. If false, the application will have to call `rd_kafka_offset_store()` to store an offset (optional). **NOTE:** There is currently no zookeeper integration, offsets will be written to broker or local file according to offset.store.method.
enable.auto.commit                       |  C  |               | Alias for `auto.commit.enable`
auto.commit.interval.ms                  |  C  |         60000 | The frequency in milliseconds that the consumer offsets are commited (written) to offset storage.
auto.offset.reset                        |  C  |       largest | Action to take when there is no initial offset in offset store or the desired offset is out of range: 'smallest','earliest' - automatically reset the offset to the smallest offset, 'largest','latest' - automatically reset the offset to the largest offset, 'error' - trigger an error which is retrieved by consuming messages and checking 'message->err'.
offset.store.path                        |  C  |             . | Path to local file for storing offsets. If the path is a directory a filename will be automatically generated in that directory based on the topic and partition.
offset.store.sync.interval.ms            |  C  |            -1 | fsync() interval for the offset file, in milliseconds. Use -1 to disable syncing, and 0 for immediate sync after each write.
offset.store.method                      |  C  |        broker | Offset commit store method: 'file' - local file store (offset.store.path, et.al), 'broker' - broker commit store (requires Apache Kafka 0.8.2 or later on the broker).
consume.callback.max.messages            |  C  |             0 | Maximum number of messages to dispatch in one `rd_kafka_consume_callback*()` call (0 = unlimited)

### C/P legend: C = Consumer, P = Producer, * = both
