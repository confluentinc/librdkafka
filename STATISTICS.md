# Statistics

librdkafka may be configured to emit internal metrics at a fixed interval
by setting the `statistics.interval.ms` configuration property to a value > 0
and registering a `stats_cb` (or similar, depending on language).

The stats are provided as a JSON object string.

**Note**: The metrics returned may not be completely consistent between
          brokers, toppars and totals, due to the internal asynchronous
          nature of librdkafka.
          E.g., the top level `tx` total may be less than the sum of
          the broker `tx` values which it represents.


## General structure

All fields that contain sizes are are in bytes unless otherwise noted.

```
{
 <Top-level fields>
 "brokers": {
    <brokers fields>,
    "toppars": { <toppars fields> }
 },
 "topics": {
   <topic fields>,
   "partitions": {
     <partitions fields>
   }
 }
[, "cgrp": { <cgrp fields> } ]
}
```

## Field type

Fields are represented as follows:
 * string - UTF8 string.
 * int - Integer counter (64 bits wide). Ever increasing.
 * int gauge - Integer gauge (64 bits wide). Will be reset to 0 on each stats emit.
 * object - Nested JSON object.
 * bool - `true` or `false`.


## Top-level

Field | Type | Example | Description
----- | ---- | ------- | -----------
name | string | `"rdkafka#producer-1"` | Handle instance name
client_id | string | `"rdkafka"` | The configured (or default) `client.id`
type | string | `"producer"` | Instance type (producer or consumer)
ts | int | 12345678912345 | librdkafka's internal monotonic clock (micro seconds)
time | int | | Wall clock time in seconds since the epoch
replyq | int gauge | | Number of ops waiting in queue for application to serve with rd_kafka_poll()
msg_cnt | int gauge | | Current number of messages in instance queues
msg_size | int gauge | | Current total size of messages in instance queues
msg_max | int | | Threshold: maximum number of messages allowed
msg_size_max | int | | Threshold: maximum total size of messages allowed
tx | int | | Total number of requests sent
txbytes | int | | Total number of bytes sent
rx | int | | Total number of responses received
rxbytes | int | | Total number of bytes received
txmsgs | int | | Total number of messages transmitted (produced)
txmsg_bytes | int | | Total number of bytes transmitted for txmsgs
rxmsgs | int | | Total number of messages consumed, not including ignored messages (due to offset, etc).
rxmsg_bytes | int | | Total number of bytes received for rxmsgs
simple_cnt | int gauge | | Internal tracking of legacy vs new consumer API state
metadata_cache_cnt | int gauge | | Number of topics in the metadata cache.
brokers | object | | Dict of brokers, key is broker name, value is object. See **brokers** below
topics | object | | Dict of topics, key is topic name, value is object. See **topics** below
cgrp | object | | Consumer group metrics. See **cgrp** below

## brokers

Field | Type | Example | Description
----- | ---- | ------- | -----------
name | string | `"example.com:9092/13"` | Broker hostname, port and broker id
nodeid | int | 13 | Broker id (-1 for bootstraps)
state | string | `"UP"` | Broker state (INIT, DOWN, CONNECT, AUTH, APIVERSION_QUERY, AUTH_HANDSHAKE, UP, UPDATE)
stateage | int gauge | | Time since last broker state change (microseconds)
outbuf_cnt | int gauge | | Number of requests awaiting transmission to broker
outbuf_msg_cnt | int gauge | | Number of messages in outbuf_cnt
waitresp_cnt | int gauge | | Number of requests in-flight to broker awaiting response
waitresp_msg_cnt | int gauge | | Number of messages in waitresp_cnt
tx | int | | Total number of requests sent
txbytes | int | | Total number of bytes sent
txerrs | int | | Total number of transmission errors
txretries | int | | Total number of request retries
req_timeouts | int | | Total number of requests timed out
rx | int | | Total number of responses received
rxbytes | int | | Total number of bytes received
rxerrs | int | | Total number of receive errors
rxcorriderrs | int | | Total number of unmatched correlation ids in response (typically for timed out requests)
rxpartial | int | | Total number of partial messagesets received
zbuf_grow | int | | Total number of decompression buffer size increases
buf_grow | int | | Total number of buffer size increases
wakeups | int | | Broker thread poll wakeups
int_latency | object | | Internal producer queue latency in microseconds. See *Window stats* below
rtt | object | | Broker latency / round-trip time in microseconds. See *Window stats* below
throttle | object | | Broker throttling time in milliseconds. See *Window stats* below
toppars | object | | Partitions handled by this broker handle. Key is "topic-partition". See *brokers.toppars* below


## Window stats

Rolling window statistics. The values are in microseconds unless otherwise stated.

Field | Type | Example | Description
----- | ---- | ------- | -----------
min | int gauge | | Smallest value
max | int gauge | | Largest value
avg | int gauge | | Average value
sum | int gauge | | Sum of values
cnt | int gauge | | Number of values sampled
stddev | int gauge | | Standard deviation (based on histogram)
mean | int gauge | | Mean value (based on histogram)
histoor | int gauge | | Values skipped due to out of histogram range
p50 | int gauge | | 50th percentile
p75 | int gauge | | 75th percentile
p90 | int gauge | | 90th percentile
p95 | int gauge | | 95th percentile
p99 | int gauge | | 99th percentile
p99_99 | int gauge | | 99.99th percentile


## brokers.toppars

Topic partition assigned to broker.

Field | Type | Example | Description
----- | ---- | ------- | -----------
topic | string | `"mytopic"` | Topic name
partition | int | 3 | Partition id

## topics

Field | Type | Example | Description
----- | ---- | ------- | -----------
topic | string | `"myatopic"` | Topic name
metadata_age | int gauge | | Age of metadata from broker for this topic (milliseconds)
batchsize | object | | Batch sizes in bytes. See *Window stats*·
partitions | object | | Partitions dict, key is partition id. See **partitions** below.


## partitions

Field | Type | Example | Description
----- | ---- | ------- | -----------
partition | int | 3 | Partition Id (-1 for internal UA/UnAssigned partition)
leader | int | | Current leader broker id
desired | bool | | Partition is explicitly desired by application
unknown | bool | | Partition not seen in topic metadata from broker
msgq_cnt | int gauge | | Number of messages waiting to be produced in first-level queue
msgq_bytes | int gauge | | Number of bytes in msgq_cnt
xmit_msgq_cnt | int gauge | | Number of messages ready to be produced in transmit queue
xmit_msgq_bytes | int gauge | | Number of bytes in xmit_msgq
fetchq_cnt | int gauge | | Number of pre-fetched messages in fetch queue
fetchq_size | int gauge | | Bytes in fetchq
fetch_state | string | `"active"` | Consumer fetch state for this partition (none, stopping, stopped, offset-query, offset-wait, active).
query_offset | int gauge | | Current/Last logical offset query
next_offset | int gauge | | Next offset to fetch
app_offset | int gauge | | Offset of last message passed to application
stored_offset | int gauge | | Offset to be committed
committed_offset | int gauge | | Last committed offset
eof_offset | int gauge | | Last PARTITION_EOF signaled offset
lo_offset | int gauge | | Partition's low watermark offset on broker
hi_offset | int gauge | | Partition's high watermark offset on broker
consumer_lag | int gauge | | Difference between hi_offset - app_offset
txmsgs | int | | Total number of messages transmitted (produced)
txbytes | int | | Total number of bytes transmitted for txmsgs
rxmsgs | int | | Total number of messages consumed, not including ignored messages (due to offset, etc).
rxbytes | int | | Total number of bytes received for rxmsgs
msgs | int | | Total number of messages received (consumer, same as rxmsgs), or total number of messages produced (possibly not yet transmitted) (producer).
rx_ver_drops | int | | Dropped outdated messages


## cgrp

Field | Type | Example | Description
----- | ---- | ------- | -----------
rebalance_age | int gauge | | Time elapsed since last rebalance (assign or revoke) (milliseconds)
rebalance_cnt | int | | Total number of rebalances (assign or revoke)
assignment_size | int gauge | | Current assignment's partition count


# Example output

This example output is from a short-lived high level consumer using the following command:
`rdkafka_performance -G myfinegroup -b 0 -t test -o beginning -T 2000 -Y 'cat > stats.json'`

```
{
  "name": "rdkafka#consumer-1",
  "type": "consumer",
  "ts": 895747604205,
  "time": 1479659343,
  "replyq": 0,
  "msg_cnt": 0,
  "msg_size": 0,
  "msg_max": 0,
  "msg_size_max": 0,
  "simple_cnt": 0,
  "brokers": {
    "0:9092/bootstrap": {
      "name": "0:9092/bootstrap",
      "nodeid": -1,
      "state": "UP",
      "stateage": 5989882,
      "outbuf_cnt": 0,
      "outbuf_msg_cnt": 0,
      "waitresp_cnt": 0,
      "waitresp_msg_cnt": 0,
      "tx": 2,
      "txbytes": 56,
      "txerrs": 0,
      "txretries": 0,
      "req_timeouts": 0,
      "rx": 2,
      "rxbytes": 31692,
      "rxerrs": 0,
      "rxcorriderrs": 0,
      "rxpartial": 0,
      "zbuf_grow": 0,
      "buf_grow": 0,
      "rtt": {
        "min": 0,
        "max": 0,
        "avg": 0,
        "sum": 0,
        "cnt": 0
      },
      "throttle": {
        "min": 0,
        "max": 0,
        "avg": 0,
        "sum": 0,
        "cnt": 0
      },
      "toppars": {}
    },
    "localhost:9092/2": {
      "name": "localhost:9092/2",
      "nodeid": 2,
      "state": "UP",
      "stateage": 5958663,
      "outbuf_cnt": 0,
      "outbuf_msg_cnt": 0,
      "waitresp_cnt": 1,
      "waitresp_msg_cnt": 0,
      "tx": 54,
      "txbytes": 3650,
      "txerrs": 0,
      "txretries": 0,
      "req_timeouts": 0,
      "rx": 53,
      "rxbytes": 89546,
      "rxerrs": 0,
      "rxcorriderrs": 0,
      "rxpartial": 0,
      "zbuf_grow": 0,
      "buf_grow": 0,
      "rtt": {
        "min": 721,
        "max": 106064,
        "avg": 87530,
        "sum": 1925664,
        "cnt": 22
      },
      "throttle": {
        "min": 0,
        "max": 0,
        "avg": 0,
        "sum": 0,
        "cnt": 19
      },
      "toppars": {
        "test-1": {
          "topic": "test",
          "partition": 1
        }
      }
    },
    "localhost:9094/4": {
      "name": "localhost:9094/4",
      "nodeid": 4,
      "state": "UP",
      "stateage": 5958663,
      "outbuf_cnt": 0,
      "outbuf_msg_cnt": 0,
      "waitresp_cnt": 1,
      "waitresp_msg_cnt": 0,
      "tx": 40,
      "txbytes": 3042,
      "txerrs": 0,
      "txretries": 0,
      "req_timeouts": 0,
      "rx": 39,
      "rxbytes": 87058,
      "rxerrs": 0,
      "rxcorriderrs": 0,
      "rxpartial": 0,
      "zbuf_grow": 0,
      "buf_grow": 0,
      "rtt": {
        "min": 100169,
        "max": 101198,
        "avg": 100730,
        "sum": 2014600,
        "cnt": 20
      },
      "throttle": {
        "min": 0,
        "max": 0,
        "avg": 0,
        "sum": 0,
        "cnt": 20
      },
      "toppars": {
        "test-3": {
          "topic": "test",
          "partition": 3
        },
        "test-0": {
          "topic": "test",
          "partition": 0
        }
      }
    },
    "localhost:9093/3": {
      "name": "localhost:9093/3",
      "nodeid": 3,
      "state": "UP",
      "stateage": 5958647,
      "outbuf_cnt": 0,
      "outbuf_msg_cnt": 0,
      "waitresp_cnt": 1,
      "waitresp_msg_cnt": 0,
      "tx": 44,
      "txbytes": 2688,
      "txerrs": 0,
      "txretries": 0,
      "req_timeouts": 0,
      "rx": 43,
      "rxbytes": 90161,
      "rxerrs": 0,
      "rxcorriderrs": 0,
      "rxpartial": 0,
      "zbuf_grow": 0,
      "buf_grow": 0,
      "rtt": {
        "min": 99647,
        "max": 101254,
        "avg": 100612,
        "sum": 2012247,
        "cnt": 20
      },
      "throttle": {
        "min": 0,
        "max": 0,
        "avg": 0,
        "sum": 0,
        "cnt": 20
      },
      "toppars": {
        "test-2": {
          "topic": "test",
          "partition": 2
        }
      }
    }
  },
  "topics": {
    "test": {
      "topic": "test",
      "metadata_age": 4957,
      "partitions": {
        "0": {
          "partition": 0,
          "leader": 4,
          "desired": true,
          "unknown": false,
          "msgq_cnt": 0,
          "msgq_bytes": 0,
          "xmit_msgq_cnt": 0,
          "xmit_msgq_bytes": 0,
          "fetchq_cnt": 0,
          "fetchq_size": 0,
          "fetch_state": "active",
          "query_offset": -2,
          "next_offset": 427,
          "app_offset": 427,
          "stored_offset": 427,
          "commited_offset": 427,
          "committed_offset": 427,
          "eof_offset": 427,
          "lo_offset": -1001,
          "hi_offset": 427,
          "consumer_lag": 0,
          "txmsgs": 0,
          "txbytes": 0,
          "msgs": 0,
          "rx_ver_drops": 0
        },
        "1": {
          "partition": 1,
          "leader": 2,
          "desired": true,
          "unknown": false,
          "msgq_cnt": 0,
          "msgq_bytes": 0,
          "xmit_msgq_cnt": 0,
          "xmit_msgq_bytes": 0,
          "fetchq_cnt": 0,
          "fetchq_size": 0,
          "fetch_state": "active",
          "query_offset": -2,
          "next_offset": 436,
          "app_offset": 436,
          "stored_offset": 436,
          "commited_offset": 436,
          "committed_offset": 436,
          "eof_offset": 436,
          "lo_offset": -1001,
          "hi_offset": 436,
          "consumer_lag": 0,
          "txmsgs": 0,
          "txbytes": 0,
          "msgs": 0,
          "rx_ver_drops": 0
        },
        "2": {
          "partition": 2,
          "leader": 3,
          "desired": true,
          "unknown": false,
          "msgq_cnt": 0,
          "msgq_bytes": 0,
          "xmit_msgq_cnt": 0,
          "xmit_msgq_bytes": 0,
          "fetchq_cnt": 0,
          "fetchq_size": 0,
          "fetch_state": "active",
          "query_offset": -2,
          "next_offset": 458,
          "app_offset": 458,
          "stored_offset": 458,
          "commited_offset": 458,
          "committed_offset": 458,
          "eof_offset": 458,
          "lo_offset": -1001,
          "hi_offset": 458,
          "consumer_lag": 0,
          "txmsgs": 0,
          "txbytes": 0,
          "msgs": 0,
          "rx_ver_drops": 0
        },
        "3": {
          "partition": 3,
          "leader": 4,
          "desired": true,
          "unknown": false,
          "msgq_cnt": 0,
          "msgq_bytes": 0,
          "xmit_msgq_cnt": 0,
          "xmit_msgq_bytes": 0,
          "fetchq_cnt": 0,
          "fetchq_size": 0,
          "fetch_state": "active",
          "query_offset": -2,
          "next_offset": 497,
          "app_offset": 497,
          "stored_offset": 497,
          "commited_offset": 497,
          "committed_offset": 497,
          "eof_offset": 497,
          "lo_offset": -1001,
          "hi_offset": 497,
          "consumer_lag": 0,
          "txmsgs": 0,
          "txbytes": 0,
          "msgs": 0,
          "rx_ver_drops": 0
        },
        "-1": {
          "partition": -1,
          "leader": -1,
          "desired": false,
          "unknown": false,
          "msgq_cnt": 0,
          "msgq_bytes": 0,
          "xmit_msgq_cnt": 0,
          "xmit_msgq_bytes": 0,
          "fetchq_cnt": 0,
          "fetchq_size": 0,
          "fetch_state": "none",
          "query_offset": 0,
          "next_offset": 0,
          "app_offset": -1001,
          "stored_offset": -1001,
          "commited_offset": -1001,
          "committed_offset": -1001,
          "eof_offset": -1001,
          "lo_offset": -1001,
          "hi_offset": -1001,
          "consumer_lag": -1,
          "txmsgs": 0,
          "txbytes": 0,
          "msgs": 0,
          "rx_ver_drops": 0
        }
      }
    }
  },
  "cgrp": {
    "rebalance_age": 5251,
    "rebalance_cnt": 2,
    "assignment_size": 4
  }
}
```
