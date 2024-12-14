/*
 * confluent-kafka-javascript - Node.js wrapper  for RdKafka C/C++ library
 *
 * Copyright (c) 2016-2023 Blizzard Entertainment
 *           (c) 2023 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */
'use strict';

module.exports = KafkaConsumer;

var Client = require('./client');
var util = require('util');
var Kafka = require('../librdkafka');
var KafkaConsumerStream = require('./kafka-consumer-stream');
var LibrdKafkaError = require('./error');
var TopicPartition = require('./topic-partition');
var shallowCopy = require('./util').shallowCopy;
var DEFAULT_CONSUME_LOOP_TIMEOUT_DELAY = 500;
var DEFAULT_CONSUME_TIME_OUT = 1000;
const DEFAULT_IS_TIMEOUT_ONLY_FOR_FIRST_MESSAGE = false;
util.inherits(KafkaConsumer, Client);

/**
 * KafkaConsumer class for reading messages from Kafka.
 *
 * This is the main entry point for reading data from Kafka. You
 * configure this like you do any other client, with a global
 * configuration and default topic configuration.
 *
 * Data will not be read until you tell the consumer what topics
 * you want to read from.
 *
 * Methods on a KafkaConsumer will throw a [LibrdKafkaError]{@link RdKafka.LibrdKafkaError}
 * on failure.
 *
 * @param {object} conf - Key value pairs to configure the consumer.
 * @param {object?} topicConf - Key value pairs to create a default topic configuration.
 * @extends RdKafka.Client
 * @memberof RdKafka
 * @constructor
 * @see [Consumer example]{@link https://github.com/confluentinc/confluent-kafka-javascript/blob/master/examples/node-rdkafka/consumer-flow.md}
 */
function KafkaConsumer(conf, topicConf) {
  if (!(this instanceof KafkaConsumer)) {
    return new KafkaConsumer(conf, topicConf);
  }

  conf = shallowCopy(conf);
  topicConf = shallowCopy(topicConf);

  var onRebalance = conf.rebalance_cb;

  var self = this;

  // If rebalance is undefined we don't want any part of this
  if (onRebalance && typeof onRebalance === 'boolean') {
    conf.rebalance_cb = function(err, assignment) {
      // Create the librdkafka error
      err = LibrdKafkaError.create(err);
      // Emit the event
      self.emit('rebalance', err, assignment);

      // That's it
      try {
        if (err.code === -175 /*ERR__ASSIGN_PARTITIONS*/) {
          if (self.rebalanceProtocol() === 'COOPERATIVE') {
            self.incrementalAssign(assignment);
          } else {
            self.assign(assignment);
          }
        } else if (err.code === -174 /*ERR__REVOKE_PARTITIONS*/) {
          if (self.rebalanceProtocol() === 'COOPERATIVE') {
            self.incrementalUnassign(assignment);
          } else {
            self.unassign();
          }
        }
      } catch (e) {
        // Ignore exceptions if we are not connected
        if (self.isConnected()) {
          self.emit('rebalance.error', e);
        }
      }
    };
  } else if (onRebalance && typeof onRebalance === 'function') {
    /*
     * Once this is opted in to, that's it. It's going to manually rebalance
     * forever. There is no way to unset config values in librdkafka, just
     * a way to override them.
     */

     conf.rebalance_cb = function(err, assignment) {
       // Create the librdkafka error
       err = err ? LibrdKafkaError.create(err) : undefined;

       self.emit('rebalance', err, assignment);
       onRebalance.call(self, err, assignment);
     };
  }

  // Same treatment for offset_commit_cb
  var onOffsetCommit = conf.offset_commit_cb;

  if (onOffsetCommit && typeof onOffsetCommit === 'boolean') {
    conf.offset_commit_cb = function(err, offsets) {
      if (err) {
        err = LibrdKafkaError.create(err);
      }
      // Emit the event
      self.emit('offset.commit', err, offsets);
    };
  } else if (onOffsetCommit && typeof onOffsetCommit === 'function') {
    conf.offset_commit_cb = function(err, offsets) {
      if (err) {
        err = LibrdKafkaError.create(err);
      }
      // Emit the event
      self.emit('offset.commit', err, offsets);
      onOffsetCommit.call(self, err, offsets);
    };
  }

  // Note: This configuration is for internal use for now, and hence is not documented, or
  // exposed via types.
  const queue_non_empty_cb = conf.queue_non_empty_cb || null;
  delete conf.queue_non_empty_cb;

  Client.call(this, conf, Kafka.KafkaConsumer, topicConf);

  this.globalConfig = conf;
  this.topicConfig = topicConf;

  this._consumeTimeout = DEFAULT_CONSUME_TIME_OUT;
  this._consumeLoopTimeoutDelay = DEFAULT_CONSUME_LOOP_TIMEOUT_DELAY;
  this._consumeIsTimeoutOnlyForFirstMessage = DEFAULT_IS_TIMEOUT_ONLY_FOR_FIRST_MESSAGE;

  if (queue_non_empty_cb) {
    this._cb_configs.event.queue_non_empty_cb = queue_non_empty_cb;
  }
}

/**
 * Set the default consume timeout provided to C++land.
 *
 * @param {number} timeoutMs - number of milliseconds to wait for a message to be fetched.
 */
KafkaConsumer.prototype.setDefaultConsumeTimeout = function(timeoutMs) {
  this._consumeTimeout = timeoutMs;
};

/**
 * Set the default sleep delay for the next consume loop after the previous one has timed out.
 *
 * @param {number} intervalMs - number of milliseconds to sleep after a message fetch has timed out.
 */
KafkaConsumer.prototype.setDefaultConsumeLoopTimeoutDelay = function(intervalMs) {
  this._consumeLoopTimeoutDelay = intervalMs;
};

/**
 * If true:
 *  In consume(number, cb), we will wait for `timeoutMs` for the first message to be fetched.
 *  Subsequent messages will not be waited for and will be fetched (upto `number`) if already ready.
 *
 * If false:
 *  In consume(number, cb), we will wait for upto `timeoutMs` for each message to be fetched.
 *
 * @param {boolean} isTimeoutOnlyForFirstMessage
 * @private
 */
KafkaConsumer.prototype.setDefaultIsTimeoutOnlyForFirstMessage = function(isTimeoutOnlyForFirstMessage) {
  this._consumeIsTimeoutOnlyForFirstMessage = isTimeoutOnlyForFirstMessage;
};

/**
 * Get a stream representation of this KafkaConsumer.
 *
 * @example
 * var consumerStream = Kafka.KafkaConsumer.createReadStream({
 * 	'metadata.broker.list': 'localhost:9092',
 * 	'group.id': 'librd-test',
 * 	'socket.keepalive.enable': true,
 * 	'enable.auto.commit': false
 * }, {}, { topics: [ 'test' ] });
 *
 * @param {object} conf - Key value pairs to configure the consumer.
 * @param {object?} topicConf - Key value pairs to create a default topic configuration. May be null.
 * @param {object} streamOptions - Stream options.
 * @param {array} streamOptions.topics - Array of topics to subscribe to.
 * @return {RdKafka.KafkaConsumerStream} - Readable stream that receives messages when new ones become available.
 */
KafkaConsumer.createReadStream = function(conf, topicConf, streamOptions) {
  var consumer = new KafkaConsumer(conf, topicConf);
  return new KafkaConsumerStream(consumer, streamOptions);
};

/**
 * Get a current list of the committed offsets per topic partition.
 *
 * Calls back with array of objects in the form of a topic partition list.
 *
 * @param {TopicPartition[]?} toppars - Topic partition list to query committed
 * offsets for. Defaults to the current assignment.
 * @param  {number} timeout - Number of ms to block before calling back
 * and erroring.
 * @param  {function} cb - Callback method to execute when finished or timed out.
 * @return {RdKafka.KafkaConsumer} - Returns itself.
 */
KafkaConsumer.prototype.committed = function(toppars, timeout, cb) {
  // We want to be backwards compatible here, and the previous version of
  // this function took two arguments

  // If CB is not set, shift to backwards compatible version
  if (!cb) {
    cb = arguments[1];
    timeout = arguments[0];
    toppars = this.assignments();
  } else {
    toppars = toppars || this.assignments();
  }

  this._client.committed(toppars, timeout, function(err, topicPartitions) {
    if (err) {
      cb(LibrdKafkaError.create(err));
      return;
    }

    cb(null, topicPartitions);
  });
  return this;
};

/**
 * Seek consumer for topic+partition to offset which is either an absolute or
 * logical offset.
 *
 * The consumer must have previously been assigned to topics and partitions that
 * are being seeked to in the call.
 *
 * @example
 * consumer.seek({ topic: 'topic', partition: 0, offset: 1000 }, 2000, function(err) {
 *   if (err) {
 *
 *   }
 * });
 *
 * @param {TopicPartition} toppar - Topic partition to seek, including offset.
 * @param  {number} timeout - Number of ms to try seeking before erroring out.
 * @param  {function} cb - Callback method to execute when finished or timed
 *                         out. If the seek timed out, the internal state of the
 *                         consumer is unknown.
 * @return {RdKafka.KafkaConsumer} - Returns itself.
 */
KafkaConsumer.prototype.seek = function(toppar, timeout, cb) {
  this._client.seek(TopicPartition.create(toppar), timeout, function(err) {
    if (err) {
      cb(LibrdKafkaError.create(err));
      return;
    }

    cb();
  });
  return this;
};

/**
 * Assign the consumer specific partitions and topics. Used for
 * eager (non-cooperative) rebalancing from within the rebalance callback.
 *
 * @param {array} assignments - Assignments array. Should contain
 * objects with topic and partition set.
 * @return {RdKafka.KafkaConsumer} - Returns itself.
 * @see RdKafka.KafkaConsumer#incrementalAssign
 */
KafkaConsumer.prototype.assign = function(assignments) {
  this._client.assign(TopicPartition.map(assignments));
  return this;
};

/**
 * Unassign the consumer from its assigned partitions and topics. Used for
 * eager (non-cooperative) rebalancing from within the rebalance callback.
 *
 * @return {RdKafka.KafkaConsumer} - Returns itself.
 * @see RdKafka.KafkaConsumer#incrementalUnassign
 */

KafkaConsumer.prototype.unassign = function() {
  this._client.unassign();
  return this;
};

/**
 * Assign the consumer specific partitions and topics. Used for
 * cooperative rebalancing from within the rebalance callback.
 *
 * @param {array} assignments - Assignments array. Should contain
 * objects with topic and partition set. Assignments are additive.
 * @return {RdKafka.KafkaConsumer} - Returns itself.
 * @see RdKafka.KafkaConsumer#assign
 */
KafkaConsumer.prototype.incrementalAssign = function(assignments) {
  this._client.incrementalAssign(TopicPartition.map(assignments));
  return this;
};

/**
 * Unassign the consumer specific partitions and topics. Used for
 * cooperative rebalancing from within the rebalance callback.
 *
 * @param {array} assignments - Assignments array. Should contain
 * objects with topic and partition set. Assignments are subtractive.
 * @return {RdKafka.KafkaConsumer} - Returns itself.
 * @see RdKafka.KafkaConsumer#unassign
 */
KafkaConsumer.prototype.incrementalUnassign = function(assignments) {
  this._client.incrementalUnassign(TopicPartition.map(assignments));
  return this;
};

/**
 * Get the assignments for the consumer.
 *
 * @return {array} assignments - Array of topic partitions.
 */
KafkaConsumer.prototype.assignments = function() {
  return this._errorWrap(this._client.assignments(), true);
};

/**
 * Is current assignment in rebalance callback lost?
 *
 * @note This method should only be called from within the rebalance callback
 * when partitions are revoked.
 *
 * @return {boolean} true if assignment was lost.
 */

KafkaConsumer.prototype.assignmentLost = function() {
  return this._client.assignmentLost();
};

/**
 * Get the type of rebalance protocol used in the consumer group.
 *
 * @returns {string} "NONE" (if not in a group yet), "COOPERATIVE" or "EAGER".
 */
KafkaConsumer.prototype.rebalanceProtocol = function() {
  return this._client.rebalanceProtocol();
};

/**
 * Subscribe to an array of topics (synchronously).
 *
 * This operation is pretty fast because it just sets
 * an assignment in librdkafka. This is the recommended
 * way to deal with subscriptions in a situation where you
 * will be reading across multiple files or as part of
 * your configure-time initialization.
 *
 * This is also a good way to do it for streams.
 *
 * This does not actually cause any reassignment of topic partitions until
 * the consume loop is running or the consume method is called.
 *
 * @param  {array} topics - An array of topics to listen to.
 * @fires RdKafka.KafkaConsumer#subscribed
 * @return {KafkaConsumer} - Returns itself.
 */
KafkaConsumer.prototype.subscribe = function(topics) {
  // Will throw if it is a bad error.
  this._errorWrap(this._client.subscribe(topics));
  /**
   * Subscribe event. Called efter changing subscription.
   *
   * @event RdKafka.KafkaConsumer#subscribed
   * @type {Array<string>}
   */
  this.emit('subscribed', topics);
  return this;
};

/**
 * Get the current subscription of the KafkaConsumer.
 *
 * Get a list of subscribed topics. Should generally match what you
 * passed on via subscribe.
 *
 * @see RdKafka.KafkaConsumer#subscribe
 * @return {array} - Array of topic string to show the current subscription.
 */
KafkaConsumer.prototype.subscription = function() {
  return this._errorWrap(this._client.subscription(), true);
};

/**
 * Get the current offset position of the KafkaConsumer.
 *
 * Returns a list of TopicPartitions on success, or throws
 * an error on failure.
 *
 * @param {TopicPartition[]} toppars - List of topic partitions to query
 * position for. Defaults to the current assignment.
 * @return {array} - TopicPartition array. Each item is an object with
 * an offset, topic, and partition.
 */
KafkaConsumer.prototype.position = function(toppars) {
  if (!toppars) {
    toppars = this.assignments();
  }
  return this._errorWrap(this._client.position(toppars), true);
};

/**
 * Unsubscribe from all currently subscribed topics.
 *
 * Before you subscribe to new topics you need to unsubscribe
 * from the old ones, if there is an active subscription.
 * Otherwise, you will get an error because there is an
 * existing subscription.
 *
 * @return {RdKafka.KafkaConsumer} - Returns itself.
 */
KafkaConsumer.prototype.unsubscribe = function() {
  this._errorWrap(this._client.unsubscribe());
  this.emit('unsubscribed', []);
  // Backwards compatible change
  this.emit('unsubscribe', []);
  return this;
};

/**
 * Consume messages from the subscribed topics.
 *
 * This method can be used in two ways.
 *
 * 1. To read messages as fast as possible.
 *    A background thread is created to fetch the messages as quickly as it can,
 *    sleeping only in between EOF and broker timeouts.
 *    If called in this way, the method returns immediately. Messages will be
 *    sent via an optional callback that can be provided to the method, and via
 *    the [data]{@link RdKafka.KafkaConsumer#data} event.
 *
 * 2. To read a certain number of messages.
 *    If the first argument provided is a number, this method reads multiple
 *    messages, the count of which doesn't exceed the number. No additional thread
 *    is created.
 *
 * Configuring timeouts for consume:
 *
 * 1. [setDefaultConsumeTimeout]{@link RdKafka.KafkaConsumer#setDefaultConsumeTimeout} -
 *    This is the time we wait for a message to be fetched from the underlying library.
 *    If this is exceeded, we either backoff for a while (Way 1), or return with however
 *    many messages we've already fetched by that point (Way 2).
 *
 * 2. [setDefaultConsumeLoopTimeoutDelay]{@link RdKafka.KafkaConsumer#setDefaultConsumeLoopTimeoutDelay} -
 *    This is the time we wait before attempting another fetch after a message fetch has timed out.
 *    This is only used in Way 1.
 *
 * @example
 * // Way 1:
 * consumer.consume(); // No callback is okay.
 * consumer.consume(processMessage); // Messages will be send to the callback.
 *
 * // Way 2:
 * consumer.consume(10); // First parameter must be number of messages.
 * consumer.consume(10, processMessage); // Callback can be given as the second parameter.
 *
 * @param {number|function|null} sizeOrCb - Either the number of messages to
 *                                          read (if using in second way, or the
 *                                          callback) if using the first way.
 * @param {RdKafka.KafkaConsumer~readCallback?} cb - Callback to return when work is done, if using second way.
 */
KafkaConsumer.prototype.consume = function(number, cb) {
  var timeoutMs = this._consumeTimeout !== undefined ? this._consumeTimeout : DEFAULT_CONSUME_TIME_OUT;

  if ((number && typeof number === 'number') || (number && cb)) {

    if (cb === undefined) {
      cb = function() {};
    } else if (typeof cb !== 'function') {
      throw new TypeError('Callback must be a function');
    }

    this._consumeNum(timeoutMs, number, cb);
  } else {

    // See https://github.com/confluentinc/confluent-kafka-javascript/issues/220
    // Docs specify just a callback can be provided but really we needed
    // a fallback to the number argument
    // @deprecated
    if (cb === undefined) {
      if (typeof number === 'function') {
        cb = number;
      } else {
        cb = function() {};
      }
    }

    this._consumeLoop(timeoutMs, cb);
  }
};

/**
 * Open a background thread and keep getting messages as fast
 * as we can. Should not be called directly, and instead should
 * be called using consume.
 *
 * @private
 * @see consume
 */
KafkaConsumer.prototype._consumeLoop = function(timeoutMs, cb) {
  var self = this;
  var retryReadInterval = this._consumeLoopTimeoutDelay;
  self._client.consumeLoop(timeoutMs, retryReadInterval, function readCallback(err, message, eofEvent, warning) {

    if (err) {
      // A few different types of errors here
      // but the two we do NOT care about are
      // time outs at least now
      // Broker no more messages will also not come here
      cb(LibrdKafkaError.create(err));
    } else if (eofEvent) {
      self.emit('partition.eof', eofEvent);
    } else if (warning) {
      self.emit('warning', LibrdKafkaError.create(warning));
    } else {
      /**
       * Data event. called whenever a message is received.
       *
       * @event RdKafka.KafkaConsumer#data
       * @type {RdKafka.KafkaConsumer~Message}
       */
      self.emit('data', message);
      cb(err, message);
    }
  });

};

/**
 * Consume a number of messages and wrap in a try catch with
 * proper error reporting. Should not be called directly,
 * and instead should be called using consume.
 *
 * @private
 * @see consume
 */
KafkaConsumer.prototype._consumeNum = function(timeoutMs, numMessages, cb) {
  var self = this;

  this._client.consume(timeoutMs, numMessages, this._consumeIsTimeoutOnlyForFirstMessage, function(err, messages, eofEvents) {
    if (err) {
      err = LibrdKafkaError.create(err);
      if (cb) {
        cb(err);
      }
      return;
    }

    var currentEofEventsIndex = 0;

    function emitEofEventsFor(messageIndex) {
      while (currentEofEventsIndex < eofEvents.length && eofEvents[currentEofEventsIndex].messageIndex === messageIndex) {
        delete eofEvents[currentEofEventsIndex].messageIndex;
        self.emit('partition.eof', eofEvents[currentEofEventsIndex]);
        ++currentEofEventsIndex;
      }
    }

    emitEofEventsFor(-1);

    for (var i = 0; i < messages.length; i++) {
      self.emit('data', messages[i]);
      emitEofEventsFor(i);
    }

    emitEofEventsFor(messages.length);

    if (cb) {
      cb(null, messages);
    }

  });

};

/**
 * This callback returns the message read from Kafka.
 *
 * @callback KafkaConsumer~readCallback
 * @param {LibrdKafkaError} err - An error, if one occurred while reading
 * the data.
 * @param {KafkaConsumer~Message} message
 */

/**
 * Commit specific topic partitions, or all the topic partitions that have been read.
 *
 * This is asynchronous, and when this method returns, it's not guaranteed that the
 * offsets have been committed. If you need that, use
 * [commitSync]{@link RdKafka.KafkaConsumer#commitSync} or
 * [commitCb]{@link RdKafka.KafkaConsumer#commitCb}.
 *
 * If you provide a topic partition, or an array of topic parrtitins, it will commit those.
 * Otherwise, it will commit all read offsets for all topic partitions.
 *
 * @param {object|array|null} - Topic partition object to commit, list of topic
 * partitions, or null if you want to commit all read offsets.
 * @return {RdKafka.KafkaConsumer} - returns itself.
 */
KafkaConsumer.prototype.commit = function(topicPartition) {
  this._errorWrap(this._client.commit(topicPartition), true);
  return this;
};

/**
 * Commit a message.
 *
 * This is a convenience method to map commit properly. The offset of the message + 1 is committed
 * for the topic partition where the message comes from.
 *
 * @param {object} - Message object to commit.
 *
 * @return {RdKafka.KafkaConsumer} - returns itself.
 */
KafkaConsumer.prototype.commitMessage = function(msg) {
  var topicPartition = {
    topic: msg.topic,
    partition: msg.partition,
    offset: msg.offset + 1,
    leaderEpoch: msg.leaderEpoch
  };

  this._errorWrap(this._client.commit(topicPartition), true);
  return this;
};

/**
 * Commit a topic partition (or all topic partitions) synchronously.
 *
 * @param {object|array|null} - Topic partition object to commit, list of topic
 * partitions, or null if you want to commit all read offsets.
 * @return {RdKafka.KafkaConsumer} - returns itself.
 */
KafkaConsumer.prototype.commitSync = function(topicPartition) {
  this._errorWrap(this._client.commitSync(topicPartition), true);
  return this;
};

/**
 * Commit a message synchronously.
 *
 * @see RdKafka.KafkaConsumer#commitMessageSync
 * @param  {object} msg - A message object to commit.
 * @return {RdKafka.KafkaConsumer} - returns itself.
 */
KafkaConsumer.prototype.commitMessageSync = function(msg) {
  var topicPartition = {
    topic: msg.topic,
    partition: msg.partition,
    offset: msg.offset + 1,
    leaderEpoch: msg.leaderEpoch,
  };

  this._errorWrap(this._client.commitSync(topicPartition), true);
  return this;
};

/**
 * Commits a list of offsets per topic partition, using provided callback.
 *
 * @param {TopicPartition[]} toppars - Topic partition list to commit
 * offsets for. Defaults to the current assignment.
 * @param  {function} cb - Callback method to execute when finished.
 * @return {RdKafka.KafkaConsumer} - returns itself.
 */
KafkaConsumer.prototype.commitCb = function(toppars, cb) {
  this._client.commitCb(toppars, function(err) {
    if (err) {
      cb(LibrdKafkaError.create(err));
      return;
    }

    cb(null);
  });
  return this;
};

/**
 * Get last known offsets from the client.
 *
 * The low offset is updated periodically (if statistics.interval.ms is set)
 * while the high offset is updated on each fetched message set from the
 * broker.
 *
 * If there is no cached offset (either low or high, or both), then this will
 * throw an error.
 *
 * @param {string} topic - Topic to recieve offsets from.
 * @param {number} partition - Partition of the provided topic to recieve offsets from.
 * @return {RdKafka.Client~watermarkOffsets} - Returns an object with a high and low property, specifying
 * the high and low offsets for the topic partition.
 */
KafkaConsumer.prototype.getWatermarkOffsets = function(topic, partition) {
  if (!this.isConnected()) {
    throw new Error('Client is disconnected');
  }

  return this._errorWrap(this._client.getWatermarkOffsets(topic, partition), true);
};

/**
 * Store offset for topic partition.
 *
 * The offset will be committed (written) to the offset store according to the auto commit interval,
 * if auto commit is on, or next manual offset if not.
 *
 * enable.auto.offset.store must be set to false to use this API.
 *
 * @see {@link https://github.com/confluentinc/librdkafka/blob/261371dc0edef4cea9e58a076c8e8aa7dc50d452/src-cpp/rdkafkacpp.h#L1702}
 *
 * @param {Array.<TopicPartition>} topicPartitions - Topic partitions with offsets to store offsets for.
 */
KafkaConsumer.prototype.offsetsStore = function(topicPartitions) {
  if (!this.isConnected()) {
    throw new Error('Client is disconnected');
  }

  return this._errorWrap(this._client.offsetsStore(topicPartitions), true);
};

/**
 * Store offset for a single topic partition. Do not use this method.
 * This method is meant for internal use, and the API is not guaranteed to be stable.
 * Use offsetsStore instead.
 *
 * @param {string} topic - Topic to store offset for.
 * @param {number} partition - Partition of the provided topic to store offset for.
 * @param {number} offset - Offset to store.
 * @param {number} leaderEpoch - Leader epoch of the provided offset.
 * @private
 */
KafkaConsumer.prototype._offsetsStoreSingle = function(topic, partition, offset, leaderEpoch) {
  if (!this.isConnected()) {
    throw new Error('Client is disconnected');
  }

  return this._errorWrap(
    this._client.offsetsStoreSingle(topic, partition, offset, leaderEpoch), true);
};

/**
 * Resume consumption for the provided list of partitions.
 *
 * @param {Array.<TopicPartition>} topicPartitions - List of topic partitions to resume consumption on.
 */
KafkaConsumer.prototype.resume = function(topicPartitions) {
  if (!this.isConnected()) {
    throw new Error('Client is disconnected');
  }

  return this._errorWrap(this._client.resume(topicPartitions), true);
};

/**
 * Pause consumption for the provided list of partitions.
 *
 * @param {Array.<TopicPartition>} topicPartitions - List of topics to pause consumption on.
 */
KafkaConsumer.prototype.pause = function(topicPartitions) {
  if (!this.isConnected()) {
    throw new Error('Client is disconnected');
  }

  return this._errorWrap(this._client.pause(topicPartitions), true);
};

/**
 * @typedef {object} RdKafka.KafkaConsumer~MessageHeader
 * The key of this object denotes the header key, and the value of the key is the header value.
 */

/**
 * @typedef {object} RdKafka.KafkaConsumer~Message
 * @property {string} topic - The topic the message was read from.
 * @property {number} partition - The partition the message was read from.
 * @property {number} offset - The offset of the message.
 * @property {Buffer?} key - The key of the message.
 * @property {Buffer?} value - The value of the message.
 * @property {number} size - The size of the message.
 * @property {number} timestamp - The timestamp of the message (in milliseconds since the epoch in UTC).
 * @property {number?} leaderEpoch - The leader epoch of the message if available.
 * @property {RdKafka.KafkaConsumer~MessageHeader[]?} headers - The headers of the message.
 */