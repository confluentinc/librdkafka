# Migration Guide

## KafkaJS


1. Change the import statement, and add a `kafkaJS` block around your configs.
    ```javascript
    const { Kafka } = require('kafkajs');
    const kafka = new Kafka({ brokers: ['kafka1:9092', 'kafka2:9092'], /* ... */ });
    const producer = kafka.producer({ /* ... */, });
    ```
    to
    ```javascript
    const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
    const kafka = new Kafka({ kafkaJS: { brokers: ['kafka1:9092', 'kafka2:9092'], /* ... */ } });
    const producer = kafka.producer({ kafkaJS: { /* ... */, } });
    ```

2. Try running your program. In case a migration is needed, an informative error will be thrown.
   If you're using Typescript, some of these changes will be caught at compile time.

3. The most common expected changes to the code are:
  - For the **producer**: `acks`, `compression` and `timeout` are not set on a per-send() basis.
    Rather, they must be configured in the top-level configuration while creating the producer.
  - For the **consumer**:
    - `fromBeginning` is not set on a per-subscribe() basis.
      Rather, it must be configured in the top-level configuration while creating the consumer.
    - `autoCommit` and `autoCommitInterval` are not set on a per-run() basis.
      Rather, they must be configured in the top-level configuration while creating the consumer.
    - `autoCommitThreshold` is not supported.
    - `eachBatch`'s batch size never exceeds 1.
  - For errors: Check the `error.code` rather than the error `name` or `type`.

4. A more exhaustive list of semantic and configuration differences is [presented below](#common).

5. An example migration:

```diff
-const { Kafka } = require('kafkajs');
+const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

const kafka = new Kafka({
+ kafkaJS: {
  clientId: 'my-app',
  brokers: ['kafka1:9092', 'kafka2:9092']
+ }
})

const producerRun = async () => {
- const producer = kafka.producer();
+ const producer = kafka.producer({ kafkaJS: { acks: 1 } });
  await producer.connect();
  await producer.send({
    topic: 'test-topic',
-   acks: 1,
    messages: [
      { value: 'Hello confluent-kafka-javascript user!' },
    ],
  });
};


const consumerRun = async () => {
  // Consuming
- const consumer = kafka.consumer({ groupId: 'test-group' });
+ const consumer = kafka.consumer({ kafkaJS: { groupId: 'test-group', fromBeginning: true } });
  await consumer.connect();
- await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });
+ await consumer.subscribe({ topic: 'test-topic' });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      })
    },
  });
};

producerRun().then(consumerRun).catch(console.error);
```

### Common

#### Configuration changes
  ```javascript
  const kafka = new Kafka({ kafkaJS: { /* common configuration changes */ } });
  ```
  Each allowed config property is discussed in the table below.
  If there is any change in semantics or the default values, the property and the change is **highlighted in bold**.

  | Property                      | Default Value                        | Comment                                                                                                                                                                                                                                                                                                                                                                                                  |
  |-------------------------------|--------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
  | **brokers**                   | -                                    | A list of strings, representing the bootstrap brokers. **A function is no longer allowed as an argument for this.**                                                                                                                                                                                                                                                                                      |
  | **ssl**                       | false                                | A boolean, set to true if ssl needs to be enabled. **Additional properties like CA, certificate, key, etc. need to be specified outside the kafkaJS block.**                                                                                                                                                                                                                                             |
  | **sasl**                      | -                                    | An optional object of the form  `{ mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512', username: string, password: string }` or `{ mechanism: 'oauthbearer', oauthBearerProvider: function }`. Note that for OAUTHBEARER based authentication, the provider function must return lifetime (in ms), and principal name along with token value. **Additional authentication types are not supported.** |
  | clientId                      | "rdkafka"                            | An optional string used to identify the client.                                                                                                                                                                                                                                                                                                                                                          |
  | **connectionTimeout**         | 1000                                 | This timeout is not enforced individually, but a sum of `connectionTimeout` and `authenticationTimeout` is enforced together.                                                                                                                                                                                                                                                                            |
  | **authenticationTimeout**     | 10000                                | This timeout is not enforced individually, but a sum of `connectionTimeout` and `authenticationTimeout` is enforced together.                                                                                                                                                                                                                                                                            |
  | **reauthenticationThreshold** | **80% of connections.max.reauth.ms** | **No longer checked, the default is always used.**                                                                                                                                                                                                                                                                                                                                                       |
  | requestTimeout                | 30000                                | number of milliseconds for a network request to timeout.                                                                                                                                                                                                                                                                                                                                                 |
  | **enforceRequestTimeout**     | true                                 | When set to false, `requestTimeout` is set to 5 minutes. **This cannot be completely disabled.**                                                                                                                                                                                                                                                                                                         |
  | retry                         | object                               | Properties individually discussed below.                                                                                                                                                                                                                                                                                                                                                                 |
  | retry.maxRetryTime            | 30000                                | maximum time to backoff a retry, in milliseconds.                                                                                                                                                                                                                                                                                                                                                        |
  | retry.initialRetryTime        | 300                                  | minimum time to backoff a retry, in milliseconds                                                                                                                                                                                                                                                                                                                                                         |
  | **retry.retries**             | 5                                    | Total cap on the number of retries. **Applicable only to Produce requests.**                                                                                                                                                                                                                                                                                                                             |
  | **retry.factor**              | 0.2                                  | Randomization factor (jitter) for backoff. **Cannot be changed**.                                                                                                                                                                                                                                                                                                                                        |
  | **retry.multiplier**          | 2                                    | Multiplier for exponential factor of backoff. **Cannot be changed.**                                                                                                                                                                                                                                                                                                                                     |
  | **retry.restartOnFailure**    | true                                 | Consumer only. **Cannot be changed**. Consumer will always make an attempt to restart.                                                                                                                                                                                                                                                                                                                   |
  | logLevel                      | `logLevel.INFO`                      | Decides the severity level of the logger created by the underlying library. A logger created with the `INFO` level will not be able to log `DEBUG` messages later.                                                                                                                                                                                                                                       |
  | **socketFactory**             | null                                 | **No longer supported.**                                                                                                                                                                                                                                                                                                                                                                                 |
  | outer config                  | {}                                   | The configuration outside the kafkaJS block can contain any of the keys present in the [librdkafka CONFIGURATION table](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).                                                                                                                                                                                                        |


### Producer

#### Producer Configuration Changes

  ```javascript
  const producer = kafka.producer({ kafkaJS: { /* producer-specific configuration changes. */ } });
  ```

  Each allowed config property is discussed in the table below.
  If there is any change in semantics or the default values, the property and the change is **highlighted in bold**.

  | Property                | Default Value                                              | Comment                                                                                                                                                                                                                                              |
  |-------------------------|------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
  | **createPartitioner**   | DefaultPartioner (murmur2_random) - Java client compatible | Custom partitioner support is not yet provided. The default partitioner's behaviour is retained, and a number of partitioners are provided via the `partitioner` property, which is specified outside the `kafkaJS` block.                           |
  | **retry**               | object                                                     | Identical to `retry` in the common configuration. This takes precedence over the common config retry.                                                                                                                                                |
  | metadataMaxAge          | 5 minutes                                                  | Time in milliseconds after which to refresh metadata for known topics                                                                                                                                                                                |
  | allowAutoTopicCreation  | true                                                       | Determines if a topic should be created if it doesn't exist while producing.                                                                                                                                                                         |
  | transactionTimeout      | 60000                                                      | The maximum amount of time in milliseconds that the transaction coordinator will wait for a transaction status update from the producer before proactively aborting the ongoing transaction.  Only applicable when `transactionalId` is set to true. |
  | idempotent              | false                                                      | If set to true, ensures that messages are delivered exactly once and in order. If true, certain constraints must be respected for other properties, `maxInFlightRequests <= 5`, `retry.retries >= 0`                                                 |
  | **maxInFlightRequests** | null                                                       | Maximum number of in-flight requests **per broker connection**. If not set, it is practically unbounded (same as KafkaJS).                                                                                                                           |
  | transactionalId         | null                                                       | If set, turns this into a transactional producer with this identifier. This also automatically sets `idempotent` to true.                                                                                                                            |
  | **acks**                | -1                                                         | The number of required acks before a Produce succeeds. **This is set on a per-producer level, not on a per `send` level**. -1 denotes it will wait for all brokers in the in-sync replica set.                                                       |
  | **compression**         | CompressionTypes.NONE                                      | Compression codec for Produce messages. **This is set on a per-producer level, not on a per `send` level**. It must be a key of CompressionType, namely GZIP, SNAPPY, LZ4, ZSTD or NONE.                                                             |
  | **timeout**             | 30000                                                      | The ack timeout of the producer request in milliseconds. This value is only enforced by the broker. **This is set on a per-producer level, not on a per `send` level**.                                                                              |
  | outer config            | {}                                                         | The configuration outside the kafkaJS block can contain any of the keys present in the [librdkafka CONFIGURATION table](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).                                                    |



#### Semantic and Per-Method Changes

* `send`: and `sendBatch`:`
  - While sending multiple messages, even if one of the messages fails, the method throws an error.
  - While `sendBatch` is available, it acts as a wrapper around send, and the actual batching is handled by librdkafka.
  - `acks`, `compression` and `timeout` are not set on a per-send basis. Rather, they must be configured in the top-level configuration. See [configuration changes](#producer-configuration-changes).
    Additionally, there are several more compression types available by default besides GZIP.
    Before:
    ```javascript
    const kafka = new Kafka({/* ... */});
    const producer = kafka.producer();
    await producer.connect();

    await producer.send({
      topic: 'test',
      messages: [ /* ... */ ],
      acks: 1,
      compression: CompressionTypes.GZIP,
      timeout: 30000,
    });
    ```

    After:
    ```javascript
    const kafka = new Kafka({ kafkaJS: { /* ... */ }});
    const producer = kafka.producer({
      kafkaJS: {
        acks: 1,
        compression: CompressionTypes.GZIP|CompressionTypes.SNAPPY|CompressionTypes.LZ4|CompressionTypes.ZSTD|CompressionTypes.NONE,
        timeout: 30000,
      }
    });
    await producer.connect();

    await producer.send({
      topic: 'test',
      messages: [ /* ... */ ],
    });
    ```
  - It's recommended to send a number of messages without awaiting them, and then calling `flush` to ensure all messages are sent, rather than awaiting each message. This is more efficient.
    Example:
    ```javascript
    const kafka = new Kafka({ kafkaJS: { /* ... */ }});
    const producer = kafka.producer();
    await producer.connect();
    for (/*...*/) producer.send({ /* ... */});
    await producer.flush({timeout: 5000});
    ```

    However, in case it is desired to await every message, `linger.ms` should be set to 0, to ensure that the default batching behaviour does not cause a delay in awaiting messages.
    Example:
    ```javascript
    const kafka = new Kafka({ kafkaJS: { /* ... */ }});
    const producer = kafka.producer({ 'linger.ms': 0 });
    ```

* A transactional producer (with a `transactionId`) set, **cannot** send messages without initiating a transaction using `producer.transaction()`.
* While using `sendOffsets` from a transactional producer, the `consumerGroupId` argument must be omitted, and rather, the consumer object itself must be passed instead.

### Consumer

#### Consumer Configuration Changes

  ```javascript
  const consumer = kafka.consumer({ kafkaJS: { /* producer-specific configuration changes. */ } });
  ```
  Each allowed config property is discussed in the table below.
  If there is any change in semantics or the default values, the property and the change is **highlighted in bold**.

  | Property                 | Default Value                     | Comment                                                                                                                                                                                                                                     |
  |--------------------------|-----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
  | groupId                  | -                                 | A mandatory string denoting consumer group name that this consumer is a part of.                                                                                                                                                            |
  | **partitionAssigners**   | `[PartitionAssigners.roundRobin]` | Support for range, roundRobin, and cooperativeSticky assignors is provided. Custom assignors are not supported.                                                                                                                             |
  | **partitionAssignors**   | `[PartitionAssignors.roundRobin]` | Alias for `partitionAssigners`                                                                                                                                                                                                              |
  | **rebalanceTimeout**     | **300000**                        | The maximum allowed time for each member to join the group once a rebalance has begun. Note, that setting this value *also* changes the max poll interval. Message processing in `eachMessage/eachBatch` must not take more than this time. |
  | heartbeatInterval        | 3000                              | The expected time in milliseconds between heartbeats to the consumer coordinator.                                                                                                                                                           |
  | metadataMaxAge           | 5 minutes                         | Time in milliseconds after which to refresh metadata for known topics                                                                                                                                                                       |
  | allowAutoTopicCreation   | true                              | Determines if a topic should be created if it doesn't exist while consuming.                                                                                                                                                                |
  | **maxBytesPerPartition** | 1048576 (1MB)                     | determines how many bytes can be fetched in one request from a single partition. There is a change in semantics, this size grows dynamically if a single message larger than this is encountered, and the client does not get stuck.        |
  | minBytes                 | 1                                 | Minimum number of bytes the broker responds with (or wait until `maxWaitTimeInMs`)                                                                                                                                                          |
  | **maxWaitTimeInMs**      | 500                               | Maximum time the broker may wait to fill the Fetch response with minBytes of messages. Its default value has been changed to match librdkafka's configuration.
  | maxBytes                 | 10485760 (10MB)                   | Maximum number of bytes the broker responds with.                                                                                                                                                                                           |
  | **retry**                | object                            | Identical to `retry` in the common configuration. This takes precedence over the common config retry.                                                                                                                                       |
  | readUncommitted          | false                             | If true, consumer will read transactional messages which have not been committed.                                                                                                                                                           |
  | **maxInFlightRequests**  | null                              | Maximum number of in-flight requests **per broker connection**. If not set, it is practically unbounded (same as KafkaJS).                                                                                                                  |
  | rackId                   | null                              | Can be set to an arbitrary string which will be used for fetch-from-follower if set up on the cluster.                                                                                                                                      |
  | **fromBeginning**        | false                             | If there is initial offset in offset store or the desired offset is out of range, and this is true, we consume the earliest possible offset.  **This is set on a per-consumer level, not on a per `subscribe` level**.                      |
  | **autoCommit**           | true                              | Whether to periodically auto-commit offsets to the broker while consuming.  **This is set on a per-consumer level, not on a per `run` level**.                                                                                              |
  | **autoCommitInterval**   | 5000                              | Offsets are committed periodically at this interval, if autoCommit is true. **This is set on a per-consumer level, not on a per `run` level. The default value is changed to 5 seconds.**.                                                  |
  | outer config             | {}                                | The configuration outside the kafkaJS block can contain any of the keys present in the [librdkafka CONFIGURATION table](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).                                           |


#### Semantic and Per-Method Changes


* `subscribe`:
  - Regex flags are ignored while passing a topic subscription (like 'i' or 'g'). Regexes must start with '^', otherwise, an error is thrown.
  - Subscribe must be called only after `connect`.
  - An optional parameter, `replace` is provided.
    If set to true, the current subscription is replaced with the new one. If set to false, the new subscription is added to the current one, for example,
    `consumer.subscribe({ topics: ['topic1'], replace: true});`.
    The default value is false to retain existing behaviour.
  - While passing a list of topics to `subscribe`, the `fromBeginning` is not set on a per-subscribe basis. Rather, it must be configured in the top-level configuration.

    Before:
      ```javascript
        const consumer = kafka.consumer({
          groupId: 'test-group',
        });
        await consumer.connect();
        await consumer.subscribe({ topics: ["topic"], fromBeginning: true});
      ```
    After:
      ```javascript
        const consumer = kafka.consumer({
          kafkaJS: {
            groupId: 'test-group',
            fromBeginning: true,
          }
        });
        await consumer.connect();
        await consumer.subscribe({ topics: ["topic"] });
      ```

* `run` :
  - For auto-committing using a consumer, the properties `autoCommit` and `autoCommitInterval` on `run` are not set on a per-subscribe basis.
    Rather, they must be configured in the top-level configuration.
    `autoCommitThreshold` is not supported.
    If `autoCommit` is set to true, messages are *not* committed per-message, but rather periodically at the interval specified by `autoCommitInterval` (default 5 seconds).

    Before:
    ```javascript
      const kafka = new Kafka({ /* ... */ });
      const consumer = kafka.consumer({ /* ... */ });
      await consumer.connect();
      await consumer.subscribe({ topics: ["topic"] });
      consumer.run({
        eachMessage: someFunc,
        autoCommit: true,
        autoCommitInterval: 5000,
      });
    ```
    After:
    ```javascript
      const kafka = new Kafka({ kafkaJS: { /* ... */ } });
      const consumer = kafka.consumer({
        kafkaJS: {
          /* ... */,
          autoCommit: true,
          autoCommitInterval: 5000,
        },
      });
      await consumer.connect();
      await consumer.subscribe({ topics: ["topic"] });
      consumer.run({
        eachMessage: someFunc,
      });
    ```
  - The `heartbeat()` no longer needs to be called by the user in the `eachMessage/eachBatch` callback.
    Heartbeats are automatically managed by librdkafka.
  - The `partitionsConsumedConcurrently` is supported by both `eachMessage` and `eachBatch`.
  - An API compatible version of `eachBatch` is available, but the batch size calculation is not
    as per configured parameters, rather, a constant maximum size is configured internally. This is subject
    to change.
    The property `eachBatchAutoResolve` is supported.
    Within the `eachBatch` callback, use of `uncommittedOffsets` is unsupported,
    and within the returned batch, `offsetLag` and `offsetLagLow` are unsupported.
* `commitOffsets`:
  - Does not yet support sending metadata for topic partitions being committed.
  - If called with no arguments, it commits all offsets passed to the user (or the stored offsets, if manually handling offset storage using `consumer.storeOffsets`).
* `seek`:
  - The restriction to call seek only after `run` is removed. It can be called any time.
* `pause` and `resume`:
  - These methods MUST be called after the consumer group is joined.
    In practice, this means it can be called whenever `consumer.assignment()` has a non-zero size, or within the `eachMessage/eachBatch` callback.
* `stop` is not yet supported, and the user must disconnect the consumer.

### Admin Client

The admin-client only has support for a limited subset of methods, with more to be added.

  * The `createTopics` method does not yet support the `validateOnly` or `waitForLeaders` properties, and the per-topic configuration
    does not support `replicaAssignment`.
  * The `deleteTopics` method is fully supported.
  * The `listTopics` method is supported with an additional `timeout` option.
  * The `listGroups` method is supported with additional `timeout` and `matchConsumerGroupStates` options.
    A number of additional properties have been added to the returned groups, and a list of errors within the returned object.
  * The `describeGroups` method is supported with additional `timeout` and `includeAuthorizedOperations` options.
    A number of additional properties have been added to the returned groups.
  * The `deleteGroups` method is supported with an additional `timeout` option.
  * The `fetchOffsets` method is supported with additional `timeout` and
  `requireStableOffsets` options but `resolveOffsets` option is not yet supported.
  * The `deleteTopicRecords` method is supported with additional `timeout`
  and `operationTimeout` options.
  * The `fetchTopicMetadata` method is supported with additional `timeout`
  and `includeAuthorizedOperations` options. Fetching for all topics is not advisable.
  * The `fetchTopicOffsets` method is supported with additional `timeout`
  and `isolationLevel` options.
  * The `fetchTopicOffsetsByTimestamp` method is supported with additional `timeout`
  and `isolationLevel` options.

### Using the Schema Registry

In case you are using the Schema Registry client at `kafkajs/confluent-schema-registry`, you will not need to make any changes to the usage.
An example is made available [here](./examples/kafkajs/sr.js).

### Error Handling

  Convert any checks based on `instanceof` and `error.name` or to error checks based on `error.code` or `error.type`.

  **Example**:
  ```javascript
  try {
    await producer.send(/* args */);
  } catch (error) {
    if (!Kafka.isKafkaJSError(error)) { /* unrelated err handling */ }
    else if (error.fatal) { /* fatal error, abandon producer */ }
    else if (error.code === Kafka.ErrorCode.ERR__QUEUE_FULL) { /*...*/ }
    else if (error.type === 'ERR_MSG_SIZE_TOO_LARGE') { /*...*/ }
    /* and so on for specific errors */
  }
  ```

  **Error Type Changes**:

   Some possible subtypes of `KafkaJSError` have been removed,
   and additional information has been added into `KafkaJSError`.
   Fields have been added denoting if the error is fatal, retriable, or abortable (the latter two only relevant for a transactional producer).
   Some error-specific fields have also been removed.

   An exhaustive list of changes is at the bottom of this section.

   For compatibility, as many error types as possible have been retained, but it is
   better to switch to checking the `error.code`.

   Note that `KafkaJSAggregateError` remains as before. Check the `.errors` array
   for the individual errors when checking the error code.

   Exhaustive list of error types and error fields removed:

   | Error                                     | Change                                                                                                                                                                                                                                                                                                                                             |
   |-------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
   | `KafkaJSNonRetriableError`                | Removed. Retriable errors are automatically retried by librdkafka, so there's no need for this type. Note that `error.retriable` still exists, but it's applicable only for transactional producer, where users are expected to retry an action themselves. All error types using this as a superclass now use `KafkaJSError` as their superclass. |
   | `KafkaJSOffsetOutOfRange`                 | `topic` and `partition` are removed from this object.                                                                                                                                                                                                                                                                                              |
   | `KafkaJSMemberIdRequired`                 | Removed. Automatically handled by librdkafka.                                                                                                                                                                                                                                                                                                      |
   | `KafkaJSNumberOfRetriesExceeded`          | Removed. Retries are handled by librdkafka.                                                                                                                                                                                                                                                                                                        |
   | `KafkaJSNumberOfRetriesExceeded`          | `broker, correlationId, createdAt, sentAt` and `pendingDuration` are removed from this object.                                                                                                                                                                                                                                                     |
   | `KafkaJSMetadataNotLoaded`                | Removed. Metadata is automatically reloaded by librdkafka.                                                                                                                                                                                                                                                                                         |
   | `KafkaJSTopicMetadataNotLoaded`           | Removed. Topic metadata is automatically reloaded by librdkafka.                                                                                                                                                                                                                                                                                   |
   | `KafkaJSStaleTopicMetadataAssignment`     | removed as it's automatically refreshed by librdkafka.                                                                                                                                                                                                                                                                                             |
   | `KafkaJSServerDoesNotSupportApiKey`       | Removed, as this error isn't generally exposed to user in librdkafka. If raised, it is subsumed into `KafkaJSError` where `error.code === Kafka.ErrorCode.ERR_UNSUPPORTED_VERSION`.                                                                                                                                                                |
   | `KafkaJSBrokerNotFound`                   | Removed. This error isn't exposed directly to the user in librdkafka.                                                                                                                                                                                                                                                                              |
   | `KafkaJSLockTimeout`                      | Removed. This error is not applicable while using librdkafka.                                                                                                                                                                                                                                                                                      |
   | `KafkaJSUnsupportedMagicByteInMessageSet` | Removed. It is subsumed into `KafkaJSError` where `error.code === Kafka.ErrorCode.ERR_UNSUPPORTED_VERSION`.                                                                                                                                                                                                                                        |
   | `KafkaJSInvariantViolation`               | Removed, as it's not applicable to librdkafka. Errors in internal state are subsumed into `KafkaJSError` where `error.code === Kafka.ErrorCode.ERR__STATE`.                                                                                                                                                                                        |
   | `KafkaJSInvalidVarIntError`               | Removed. This error isn't exposed directly to the user in librdkafka.                                                                                                                                                                                                                                                                              |
   | `KafkaJSInvalidLongError`                 | Removed. This error isn't exposed directly to the user in librdkafka.                                                                                                                                                                                                                                                                              |
   | `KafkaJSAlterPartitionReassignmentsError` | removed, as the RPC is not used in librdkafka.                                                                                                                                                                                                                                                                                                     |
   | `KafkaJSFetcherRebalanceError`            | Removed. This error isn't exposed directly to the user in librdkafka.                                                                                                                                                                                                                                                                              |
   | `KafkaJSConnectionError`                  | `broker` is removed from this object.                                                                                                                                                                                                                                                                                                              |
   | `KafkaJSConnectionClosedError`            | Removed. Subsumed into `KafkaJSConnectionError` as librdkafka treats them equivalently.                                                                                                                                                                                                                                                            |

## node-rdkafka

Migration from v2.18.0 and below should only require changing the import statement, from

  ```javascript
  const Kafka = require('node-rdkafka');
  ```

  to

  ```javascript
  const Kafka = require('@confluentinc/kafka-javascript');
  ```

The rest of the functionality should work as usual.

For releases > v2.18.0, the node-rdkafka API diverges from this library. If you encounter any issues migrating, refer to the [INTRODUCTION.md](./INTRODUCTION.md) for a guide to using this library.
