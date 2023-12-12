# Migration Guide

## KafkaJS

### Common

#### Configuration changes
  ```javascript
  const kafka = new Kafka({/* common configuration changes */});
  ```
  There are several changes in the common configuration. Each config property is discussed.
  If there needs to be any change, the property is highlighted.

  * An `rdKafka` block can be added to the config. It allows directly setting librdkafka properties.
    If you are starting to make the configuration anew, it is best to specify properties using
    the `rdKafka` block. [Complete list of properties here](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).

    Example:
    ```javascript
    const kafka = new Kafka({
      rdKafka: {
        globalConfig: { /* properties mentioned within the 'global config' section of the list */ }
        topicConfig: { /* properties mentioned within the 'topic config' section of the list */ }
      },
      /* ... */
    });
    ```
  * **`brokers`** list of strings, representing the bootstrap brokers.
               a function is no longer allowed as an argument for this.
  * **`ssl`**: boolean, set true if ssl needs to be enabled.
              In case additional properties, like CA, Certificate, Key etc. need to be added, use the `rdKafka` block.
  * **`sasl`**: omit if the brokers need no authentication, otherwise, an object of the following forms:
    - For SASL PLAIN or SASL SCRAM : `{ mechanism: 'plain'|'scram-sha-256'|'scram-sha-512', username: string, password: string }`
    - For SASL OAUTHBEARER: not supported yet.
    - For AWS IAM or custom mechanisms: not supported with no planned support.
    - For GSSAPI/Kerberos: use the `rdKafka` configuration.
  * `clientId`: string for identifying this client.
  * **`connectionTimeout`** and **`authenticationTimeout`**:
          These timeouts (specified in milliseconds) are not enforced individually. Instead, the sum of these values is
          enforced. The default value of the sum is 30000. It corresponds to librdkafka's `socket.connection.setup.timeout.ms`.
  * **`reauthenticationThreshold`**: no longer checked, librdkafka handles reauthentication on its own.
  * **`requestTimeout`**: number of milliseconds for a network request to timeout. The default value has been changed to 60000. It now corresponds to librdkafka's `socket.timeout.ms`.
  * **`enforceRequestTimeout`**: if this is set to false, `requestTimeout` is set to 5 minutes. The timeout cannot be disabled completely.
  * **`retry`** is partially supported. It must be an object, with the following (optional) properties
    - `maxRetryTime`: maximum time to backoff a retry, in milliseconds. Corresponds to librdkafka's `retry.backoff.max.ms`. The default is 1000.
    - `initialRetryTime`: minimum time to backoff a retry, in milliseconds. Corresponds to librdkafka's `retry.backoff.ms`. The default is 100.
    - `retries`: maximum number of retries, *only* applicable to Produce messages. However, it's recommended to keep this unset.
                 Librdkafka handles the number of retries, and rather than capping the number of retries, caps the total time spent
                 while sending the message, controlled by `message.timeout.ms`.
    - `factor` and `multiplier` cannot be changed from their defaults of 0.2 and 2.
  * **`restartOnFailure`**: this cannot be changed, and will always be true (the consumer recovers from errors on its own).
  * `logLevel` is mapped to the syslog(3) levels supported by librdkafka. `LOG_NOTHING` is not YET supported, as some panic situations are still logged.
  * **`socketFactory`** is no longer supported.

#### Error Handling

   Some possible subtypes of `KafkaJSError` have been removed,
   and additional information has been added into `KafkaJSError`.
   Fields have been added denoting if the error is fatal, retriable, or abortable (the latter two only relevant for a transactional producer).
   Some error-specific fields have also been removed.

   An exhaustive list of changes is at the bottom of this section.

   For compatibility, as many error types as possible have been retained, but it is
   better to switch to checking the `error.code`.

   **Action**: Convert any checks based on `instanceof` and `error.name` or to error
               checks based on `error.code` or `error.type`.

   **Example:**
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

   Exhaustive list of error types and error fields removed:
   1. `KafkaJSNonRetriableError`: retriable errors are automatically retried by librdkafka, so there's no need for this type.
                                  Note that `error.retriable` still exists, but it's applicable only for transactional producer,
                                  where users are expected to retry an action themselves.
                                  All error types using this as a superclass now use `KafkaJSError` as their superclass.
   2. `topic` and `partition` are removed from `KafkaJSOffsetOutOfRange`.
   3. `KafkaJSMemberIdRequired`: removed as automatically handled by librdkafka.
   4. `KafkaJSNumberOfRetriesExceeded`: removed as retries are handled by librdkafka.
   5. `broker, correlationId, createdAt, sentAt` and `pendingDuration` are removed from `KafkaJSNumberOfRetriesExceeded`.
   6. `KafkaJSMetadataNotLoaded`: removed as metadata is automatically reloaded by librdkafka.
   7. `KafkaJSTopicMetadataNotLoaded`: removed as topic metadata is automatically reloaded by librdkafka.
   8. `KafkaJSStaleTopicMetadataAssignment`: removed as it's automatically refreshed by librdkafka.
   9. `KafkaJSDeleteGroupsError`: removed, as the Admin Client doesn't have this yet. May be added back again, or changed.
   10. `KafkaJSServerDoesNotSupportApiKey`: removed, as this error isn't generally exposed to user in librdkafka. If raised,
                                            it is subsumed into `KafkaJSError` where `error.code === Kafka.ErrorCode.ERR_UNSUPPORTED_VERSION`.
   11. `KafkaJSBrokerNotFound`: removed, as this error isn't exposed directly to the user in librdkafka.
   12. `KafkaJSLockTimeout`: removed, as such an error is not applicable while using librdkafka.
   13. `KafkaJSUnsupportedMagicByteInMessageSet`: removed. It is subsumed into `KafkaJSError` where `error.code === Kafka.ErrorCode.ERR_UNSUPPORTED_VERSION`.
   14. `KafkaJSDeleteTopicRecordsError`: removed, as the Admin Client doesn't have this yet. May be added back again, or changed.
   15. `KafkaJSInvariantViolation`: removed, as it's not applicable to librdkafka. Errors in internal state are subsumed into `KafkaJSError` where `error.code === Kafka.ErrorCode.ERR__STATE`.
   16. `KafkaJSInvalidVarIntError`: removed, as it's not exposed to the user in librdkafka.
   17. `KafkaJSInvalidLongError`: removed, as it's not exposed to the user in librdkafka.
   18. `KafkaJSCreateTopicError`: removed, as the Admin Client doesn't have this yet. May be added back again, or changed.
   19. `KafkaJSAlterPartitionReassignmentsError`: removed, as the RPC is not used in librdkafka.
   20. `KafkaJSFetcherRebalanceError`: removed, it's not exposed to the user in librdkafka.
   21. `broker` is removed from `KafkaJSConnectionError`.
   22. `KafkaJSConnectionClosedError`: removed, and subsumed into `KafkaJSConnectionError` as librdkafka treats them equivalently.

### Producer

#### Configuration changes

  ```javascript
  const producer = kafka.producer({ /* producer-specific configuration changes. */});
  ```

  There are several changes in the common configuration. Each config property is discussed.
  If there needs to be any change, the property is highlighted.

  * **`createPartitioner`**: this is not supported (YET). For behaviour identical to the Java client (the DefaultPartitioner),
                             use the `rdKafka` block, and set the property `partitioner` to `murmur2_random`. This is critical
                             when planning to produce to topics where messages with certain keys have been produced already.
  * **`retry`**: See the section for retry above. The producer config `retry` takes precedence over the common config `retry`.
  * `metadataMaxAge`: Time in milliseconds after which to refresh metadata for known topics. The default value remains 5min. This
                      corresponds to the librdkafka property `topic.metadata.refresh.interval.ms` (and not `metadata.max.age.ms`).
  * `allowAutoTopicCreation`: determines if a topic should be created if it doesn't exist while producing. True by default.
  * `transactionTimeout`:  The maximum amount of time in milliseconds that the transaction coordinator will wait for a transaction
                           status update from the producer before proactively aborting the ongoing transaction. The default value remains 60000.
                           Only applicable when `transactionalId` is set to true.
  * `idempotent`: if set to true, ensures that messages are delivered exactly once and in order. False by default.
                  In case this is set to true, certain constraints must be respected for other properties, `maxInFlightRequests <= 5`, `retry.retries >= 0`.
  * **`maxInFlightRequests`**: Maximum number of in-flight requests *per broker connection*. If not set, a very high limit is used.
  * `transactionalId`: if set, turns this into a transactional producer with this identifier. This also automatically sets `idempotent` to true.
  * An `rdKafka` block can be added to the config. It allows directly setting librdkafka properties.
    If you are starting to make the configuration anew, it is best to specify properties using
    the `rdKafka` block. [Complete list of properties here](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).

#### Semantic and Per-Method Changes

* Changes to `send`:
  * `acks`, `compression` and `timeout` are not set on a per-send basis. Rather, they must be configured in the configuration.
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
    const kafka = new Kafka({/* ... */});
    const producer = kafka.producer({
      rdKafka: {
        topicConfig: {
          "acks": "1",
          "compression.codec": "gzip",
          "message.timeout.ms": "30000",
        },
      }
    });
    await producer.connect();

    await producer.send({
      topic: 'test',
      messages: [ /* ... */ ],
    });
    ```

  * Error-handling for a failed `send` is stricter. While sending multiple messages, even if one of the messages fails, the method throws an error.
* `sendBatch` is supported. However, the actual batching semantics are handled by librdkafka, and it just acts as a wrapper around `send` (See `send` for changes).
* A transactional producer (with a `transactionId`) set, can only send messages after calling `producer.transaction()`.

### Consumer

#### Configuration changes

  ```javascript
  const consumer = kafka.consumer({ /* producer-specific configuration changes. */});
  ```
  There are several changes in the common configuration. Each config property is discussed.
  If there needs to be any change, the property is highlighted. The change could be a change in
  the default values, some added/missing features, or a change in semantics.

  * **`partitionAssigners`**: The **default value** of this is changed to `[PartitionAssigners.range,PartitionAssigners.roundRobin]`. Support for range, roundRobin and cooperativeSticky
                              partition assignors is provided. The cooperative assignor cannot be used along with the other two, and there
                              is no support for custom assignors. An alias for these properties is also made available, `partitionAssignors` and `PartitionAssignors` to maintain
                              parlance with the Java client's terminology.
  * **`sessionTimeout`**: If no heartbeats are received by the broker for a group member within the session timeout, the broker will remove the consumer from
                         the group and trigger a rebalance. The **default value** is changed to 45000.
  * **`rebalanceTimeout`**: The maximum allowed time for each member to join the group once a rebalance has begun. The **default value** is changed to 300000.
                            Note, before changing: setting this value *also* changes the max poll interval. Message processing in `eachMessage` must not take more than this time.
  * `heartbeatInterval`: The expected time in milliseconds between heartbeats to the consumer coordinator. The default value remains 3000.
  * `metadataMaxAge`: Time in milliseconds after which to refresh metadata for known topics. The default value remains 5min. This
                      corresponds to the librdkafka property `topic.metadata.refresh.interval.ms` (and not `metadata.max.age.ms`).
  * **`allowAutoTopicCreation`**: determines if a topic should be created if it doesn't exist while producing. The **default value** is changed to false.
  * **`maxBytesPerPartition`**: determines how many bytes can be fetched in one request from a single partition. The default value remains 1048576.
                                There is a slight change in semantics, this size grows dynamically if a single message larger than this is encountered,
                                and the client does not get stuck.
  * `minBytes`: Minimum number of bytes the broker responds with (or wait until `maxWaitTimeInMs`). The default remains 1.
  * **`maxBytes`**: Maximum number of bytes the broker responds with. The **default value** is changed to 52428800 (50MB).
  * **`maxWaitTimeInMs`**: Maximum time in milliseconds the broker waits for the `minBytes` to be fulfilled. The **default value** is changed to 500.
  * **`retry`**: See the section for retry above. The consumer config `retry` takes precedence over the common config `retry`.
  * `readUncommitted`: if true, consumer will read transactional messages which have not been committed. The default value remains false.
  * **`maxInFlightRequests`**: Maximum number of in-flight requests *per broker connection*. If not set, a very high limit is used.
  * `rackId`: Can be set to an arbitrary string which will be used for fetch-from-follower if set up on the cluster.
  * An `rdKafka` block can be added to the config. It allows directly setting librdkafka properties.
    If you are starting to make the configuration anew, it is best to specify properties using
    the `rdKafka` block. [Complete list of properties here](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).

#### Semantic and Per-Method Changes


 * Changes to subscribe:
  * Regex flags are ignored while passing a topic subscription (like 'i' or 'g').
  * Subscribe must be called after `connect`.
  * While passing a list of topics to `subscribe`, the `fromBeginning` property is not supported. Instead, the property `auto.offset.reset` needs to be used.
   Before:
    ```javascript
      const kafka = new Kafka({ /* ... */ });
      const consumer = kafka.consumer({
        groupId: 'test-group',
      });
      await consumer.connect();
      await consumer.subscribe({ topics: ["topic"], fromBeginning: true});
    ```
   After:
    ```javascript
      const kafka = new Kafka({ /* ... */ });
      const consumer = kafka.consumer({
        groupId: 'test-group',
        rdKafka: {
          topicConfig: {
            'auto.offset.reset': 'earliest',
          },
        }
      });
      await consumer.connect();
      await consumer.subscribe({ topics: ["topic"] });
    ```

 * For auto-committing using a consumer, the properties on `run` are no longer used. Instead, corresponding rdKafka properties must be set.
    * `autoCommit` corresponds to `enable.auto.commit`.
    * `autoCommitInterval` corresponds to `auto.commit.interval.ms`.
    * `autoCommitThreshold` is no longer supported.

    Before:
    ```javascript
      const kafka = new Kafka({ /* ... */ });
      const consumer = kafka.consumer({ /* ... */ });
      await consumer.connect();
      await consumer.subscribe({ topics: ["topic"] });
      consumer.run({
        eachMessage: someFunc,
        autoCommit: true,
        autoCommitThreshold: 5000,
      });
    ```

    After:
    ```javascript
      const kafka = new Kafka({ /* ... */ });
      const consumer = kafka.consumer({
        /* ... */,
        rdKafka: {
          globalConfig: {
            "enable.auto.commit": "true",
            "auto.commit.interval.ms": "5000",
          }
        },
      });
      await consumer.connect();
      await consumer.subscribe({ topics: ["topic"] });
      consumer.run({
        eachMessage: someFunc,
      });
    ```

  * For the `eachMessage` method while running the consumer:
    * The `heartbeat()` no longer needs to be called. Heartbeats are automatically managed by librdkafka.
    * The `partitionsConsumedConcurrently` property is not supported (YET).
  * The `eachBatch` method is not supported.
  * `commitOffsets` does not (YET) support sending metadata for topic partitions being committed.
  * `paused()` is supported without any changes.
  * Custom partition assignors are not supported.
  * Changes to `seek`:
    * The restriction to call seek only after `run` is removed. It can be called any time.
    * Rather than the `autoCommit` property of `run` deciding if the offset is committed, the librdkafka property `enable.auto.commit` of the consumer config is used.
  * `pause` and `resume` MUST be called after the consumer group is joined. In practice, this means it can be called whenever `consumer.assignment()` has a non-zero size, or within the `eachMessage`
    callback.

### Admin Client

  * The admin-client is currently experimental, and only has support for a limited subset of methods. The API is subject to change.
    The methods supported are:
    * The `createTopics` method does not yet support the `validateOnly` or `waitForLeaders` properties, and the per-topic configuration
      does not support `replicaAssignment`.
    * The `deleteTopics` method is fully supported.

## node-rdkafka
