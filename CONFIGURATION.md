## Global configuration properties

Property                                 | C/P |       Default | Description              
-----------------------------------------|-----|--------------:|--------------------------
client.id                                |  *  |       rdkafka | Client identifier.
metadata.broker.list                     |  *  |               | Initial list of brokers. The application may also use `rd_kafka_brokers_add()` to add brokers during runtime.
message.max.bytes                        |  *  |       4000000 | Maximum transmit message size.
receive.message.max.bytes                |  *  |     100000000 | Maximum receive message size. This is a safety precaution to avoid memory exhaustion in case of protocol hickups. The value should be at least fetch.message.max.bytes * number of partitions consumed from + messaging overhead (e.g. 200000 bytes).
metadata.request.timeout.ms              |  *  |         60000 | Non-topic request timeout in milliseconds. This is for metadata requests, etc.
topic.metadata.refresh.interval.ms       |  *  |         10000 | Topic metadata refresh interval in milliseconds. The metadata is automatically refreshed on error and connect. Use -1 to disable the intervalled refresh.
topic.metadata.refresh.fast.cnt          |  *  |            10 | When a topic looses its leader this number of metadata requests are sent with `topic.metadata.refresh.fast.interval.ms` interval disregarding the `topic.metadata.refresh.interval.ms` value. This is used to recover quickly from transitioning leader brokers.
topic.metadata.refresh.fast.interval.ms  |  *  |           250 | See `topic.metadata.refresh.fast.cnt` description
topic.metadata.refresh.sparse            |  *  |         false | Sparse metadata requests (consumes less network bandwidth)
debug                                    |  *  |               | A comma-separated list of debug contexts to enable: all,generic,broker,topic,metadata,producer,queue,msg,protocol
socket.timeout.ms                        |  *  |         60000 | Timeout for network requests.
socket.send.buffer.bytes                 |  *  |             0 | Broker socket send buffer size. System default is used if 0.
socket.receive.buffer.bytes              |  *  |             0 | Broker socket receive buffer size. System default is used if 0.
socket.keepalive.enable                  |  *  |         false | Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets
socket.max.fails                         |  *  |             3 | Disconnect from broker when this number of send failures (e.g., timed out requests) is reached. Disable with 0. NOTE: The connection is automatically re-established.
broker.address.ttl                       |  *  |        300000 | How long to cache the broker address resolving results.
broker.address.family                    |  *  |           any | Allowed broker IP address families: any, v4, v6
statistics.interval.ms                   |  *  |             0 | librdkafka statistics emit interval. The application also needs to register a stats callback using `rd_kafka_conf_set_stats_cb()`. The granularity is 1000ms. A value of 0 disables statistics.
error_cb                                 |  *  |               | Error callback (set with rd_kafka_conf_set_error_cb())
stats_cb                                 |  *  |               | Statistics callback (set with rd_kafka_conf_set_stats_cb())
log_cb                                   |  *  |               | Log callback (set with rd_kafka_conf_set_log_cb())
log_level                                |  *  |             6 | Logging level (syslog(3) levels)
socket_cb                                |  *  |               | Socket creation callback to provide race-free CLOEXEC
open_cb                                  |  *  |               | File open callback to provide race-free CLOEXEC
opaque                                   |  *  |               | Application opaque (set with rd_kafka_conf_set_opaque())
queued.min.messages                      |  C  |        100000 | Minimum number of messages per topic+partition in the local consumer queue.
queued.max.messages.kbytes               |  C  |       1000000 | Maximum number of kilobytes per topic+partition in the local consumer queue. This value may be overshot by fetch.message.max.bytes.
fetch.wait.max.ms                        |  C  |           100 | Maximum time the broker may wait to fill the response with fetch.min.bytes.
fetch.message.max.bytes                  |  C  |       1048576 | Maximum number of bytes per topic+partition to request when fetching messages from the broker.
fetch.min.bytes                          |  C  |             1 | Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting.
fetch.error.backoff.ms                   |  C  |           500 | How long to postpone the next fetch request for a topic+partition in case of a fetch error.
group.id                                 |  C  |               | Consumer group id string. All clients sharing the same group.id belong to the same consumer group.
queue.buffering.max.messages             |  P  |        100000 | Maximum number of messages allowed on the producer queue.
queue.buffering.max.ms                   |  P  |          1000 | Maximum time, in milliseconds, for buffering data on the producer queue.
message.send.max.retries                 |  P  |             2 | How many times to retry sending a failing MessageSet. **Note:** retrying may cause reordering.
retry.backoff.ms                         |  P  |           100 | The backoff time in milliseconds before retrying a message send.
compression.codec                        |  P  |          none | Compression codec to use for compressing message sets: none, gzip or snappy
batch.num.messages                       |  P  |          1000 | Maximum number of messages batched in one MessageSet.
dr_cb                                    |  P  |               | Delivery report callback (set with rd_kafka_conf_set_dr_cb())
dr_msg_cb                                |  P  |               | Delivery report callback (set with rd_kafka_conf_set_dr_msg_cb())


## Topic configuration properties

Property                                 | C/P |       Default | Description              
-----------------------------------------|-----|--------------:|--------------------------
request.required.acks                    |  P  |             1 | This field indicates how many acknowledgements the leader broker must receive from ISR brokers before responding to the request: *0*=broker does not send any response, *1*=broker will wait until the data is written to local log before sending a response, *-1*=broker will block until message is committed by all in sync replicas (ISRs) before sending response. *>1*=for any number > 1 the broker will block waiting for this number of acknowledgements to be received (but the broker will never wait for more acknowledgements than there are ISRs).
enforce.isr.cnt                          |  P  |             0 | Fail messages locally if the currently known ISR count for a partition is less than this value. **NOTE**: The ISR count is fetched from the broker at regular intervals (`topic.metadata.refresh.interval.ms`) and might thus be outdated.
request.timeout.ms                       |  P  |          5000 | The ack timeout of the producer request in milliseconds. This value is only enforced by the broker and relies on `request.required.acks` being > 0.
message.timeout.ms                       |  P  |        300000 | Local message timeout. This value is only enforced locally and limits the time a produced message waits for successful delivery. A time of 0 is infinite.
produce.offset.report                    |  P  |         false | Report offset of produced message back to application. The application must be use the `dr_msg_cb` to retrieve the offset from `rd_kafka_message_t.offset`.
partitioner                              |  P  |               | Partitioner callback (set with rd_kafka_topic_conf_set_partitioner_cb())
opaque                                   |  *  |               | Application opaque (set with rd_kafka_topic_conf_set_opaque())
auto.commit.enable                       |  C  |          true | If true, periodically commit offset of the last message handed to the application. This commited offset will be used when the process restarts to pick up where it left off. If false, the application will have to call `rd_kafka_offset_store()` to store an offset (optional). **NOTE:** There is currently no zookeeper integration, offsets will be written to local file according to offset.store.path.
auto.commit.interval.ms                  |  C  |         60000 | The frequency in milliseconds that the consumer offsets are commited (written) to offset storage.
auto.offset.reset                        |  C  |       largest | Action to take when there is no initial offset in offset store or the desired offset is out of range: 'smallest' - automatically reset the offset to the smallest offset, 'largest' - automatically reset the offset to the largest offset, 'error' - trigger an error which is retrieved by consuming messages and checking 'message->err'.
offset.store.path                        |  C  |             . | Path to local file for storing offsets. If the path is a directory a filename will be automatically generated in that directory based on the topic and partition.
offset.store.sync.interval.ms            |  C  |            -1 | fsync() interval for the offset file, in milliseconds. Use -1 to disable syncing, and 0 for immediate sync after each write.
offset.store.method                      |  C  |          file | Offset commit store method: 'file' - local file store (offset.store.path, et.al), 'broker' - broker commit store (requires Apache Kafka 0.8.1 or later on the broker).

### C/P legend: C = Consumer, P = Producer, * = both
