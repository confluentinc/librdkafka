## Global configuration properties

Property                                 |       Default | Description              
-----------------------------------------|--------------:|--------------------------
client.id                                |       rdkafka | Client identifier.
metadata.broker.list                     |               | Initial list of brokers. The application may also use `rd_kafka_brokers_add()` to add brokers during runtime.
message.max.bytes                        |       4000000 | Maximum receive message size. This is a safety precaution to avoid memory exhaustion in case of protocol hickups.
metadata.request.timeout.ms              |         60000 | Non-topic request timeout in milliseconds. This is for metadata requests, etc.
topic.metadata.refresh.interval.ms       |         10000 | Topic metadata refresh interval in milliseconds. The metadata is automatically refreshed on error and connect. Use -1 to disable the intervalled refresh.
topic.metadata.refresh.fast.cnt          |            10 | When a topic looses its leader this number of metadata requests are sent with `topic.metadata.refresh.fast.interval.ms` interval disregarding the `topic.metadata.refresh.interval.ms` value. This is used to recover quickly from transitioning leader brokers.
topic.metadata.refresh.fast.interval.ms  |           250 | See `topic.metadata.refresh.fast.cnt` description
debug                                    |               | A comma-separated list of debug contexts to enable: all,generic,broker,topic,metadata,producer,queue,msg
socket.timeout.ms                        |         60000 | Timeout for network requests.
broker.address.ttl                       |        300000 | How long to cache the broker address resolving results.
statistics.interval.ms                   |             0 | librdkafka statistics emit interval. The application also needs to register a stats callback using `rd_kafka_conf_set_stats_cb()`. The granularity is 1000ms.
queued.min.messages                      |        100000 | Minimum number of messages that should to be available for consumption by application.
fetch.wait.max.ms                        |           100 | Maximum time the broker may wait to fill the response with fetch.min.bytes.
fetch.min.bytes                          |             1 | Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting.
fetch.error.backoff.ms                   |           500 | How long to postpone the next fetch request for a topic+partition in case of a fetch error.
queue.buffering.max.messages             |        100000 | Maximum number of messages allowed on the producer queue.
queue.buffering.max.ms                   |          1000 | Maximum time, in milliseconds, for buffering data on the producer queue.
message.send.max.retries                 |             2 | How many times to retry sending a failing MessageSet. **Note:** retrying may cause reordering.
retry.backoff.ms                         |           100 | The backoff time in milliseconds before retrying a message send.
compression.codec                        |          none | Compression codec to use for compressing message sets.
batch.num.messages                       |          1000 | Maximum number of messages batched in one MessageSet.


## Topic configuration properties

Property                                 |       Default | Description              
-----------------------------------------|--------------:|--------------------------
request.required.acks                    |             1 | This field indicates how many acknowledgements the leader broker must receive from ISR brokers before responding to the request: *0*=broker does not send any response, *1*=broker will wait until the data is written to local log before sending a response, *-1*=broker will block until message is committed by all in sync replicas (ISRs) before sending response. *>1*=for any number > 1 the broker will block waiting for this number of acknowledgements to be received (but the broker will never wait for more acknowledgements than there are ISRs).
request.timeout.ms                       |          5000 | The ack timeout of the producer request in milliseconds. This value is only enforced by the broker and relies on `request.required.acks` being > 0.
message.timeout.ms                       |        300000 | Local message timeout. This value is only enforced locally and limits the time a produced message waits for successful delivery.

