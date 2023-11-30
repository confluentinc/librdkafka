# Migration Guide

## KafkaJS

### Common

* Configuration changes

* Error Handling: Some possible subtypes of `KafkaJSError` have been removed,
   and additional information has been added into `KafkaJSError`.
   Internally, fields have been added denoting if the error is fatal, retriable, or abortable (the latter two only relevant for a
   transactional producer).
   Some error-specific fields have also been removed. An exhaustive list is at the bottom of this section.

   For compability, as many error types as possible have been retained, but it is
   better to switch to checking the `error.code`.

   **Action**: Convert any checks based on `instanceof` and `error.name` or to error
               checks based on `error.code` or `error.type`.

   **Example:**:
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

* `sendBatch` is currently unsupported - but will be supported. TODO. However, the actual batching semantics are handled by librdkafka.
* Changes to `send`:
  1. `acks`, `compression` and `timeout` are not set on a per-send basis. Rather, they must be configured in the configuration.
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

### Consumer

## node-rdkafka
