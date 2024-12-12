/*
 * confluent-kafka-javascript - Node.js wrapper  for RdKafka C/C++ library
 *
 * Copyright (c) 2016-2023 Blizzard Entertainment
 *           (c) 2024 Confluent, Inc
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */
'use strict';

/* TODO: Think of a way to fetch these from within librdkafka instead of this
 * hardcoded list.
 * New additions won't be automatically added to this list.
 */
const ConsumerGroupStates = Object.seal({
  UNKNOWN: 0,
  PREPARING_REBALANCE: 1,
  COMPLETING_REBALANCE: 2,
  STABLE: 3,
  DEAD: 4,
  EMPTY: 5,
});

const AclOperationTypes = Object.seal({
  UNKNOWN: 0,
  ANY: 1,
  ALL: 2,
  READ: 3,
  WRITE: 4,
  CREATE: 5,
  DELETE: 6,
  ALTER: 7,
  DESCRIBE: 8,
  CLUSTER_ACTION: 9,
  DESCRIBE_CONFIGS: 10,
  ALTER_CONFIGS: 11,
  IDEMPOTENT_WRITE: 12,
});

/**
 * A list of isolation levels.
 * @enum {number}
 * @readonly
 * @memberof RdKafka
 */
const IsolationLevel = {
  READ_UNCOMMITTED: 0,
  READ_COMMITTED: 1,
};

/**
 * Define an OffsetSpec to list offsets at.
 * Either a timestamp can be used, or else, one of the special, pre-defined values
 * (EARLIEST, LATEST, MAX_TIMESTAMP) can be used while passing an OffsetSpec to listOffsets.
 * @param {number} timestamp - The timestamp to list offsets at.
 * @constructor
 */
function OffsetSpec(timestamp) {
  this.timestamp = timestamp;
}

/**
 * Specific OffsetSpec value used to retrieve the offset with the largest timestamp of a partition
 * as message timestamps can be specified client side this may not match
 * the log end offset returned by OffsetSpec.LATEST.
 */
OffsetSpec.MAX_TIMESTAMP = new OffsetSpec(-3);

/**
 * Special OffsetSpec value denoting the earliest offset for a topic partition.
 */
OffsetSpec.EARLIEST = new OffsetSpec(-2);

/**
 * Special OffsetSpec value denoting the latest offset for a topic partition.
 */
OffsetSpec.LATEST = new OffsetSpec(-1);

module.exports = {
  create: createAdminClient,
  createFrom: createAdminClientFrom,
  ConsumerGroupStates,
  AclOperationTypes,
  IsolationLevel: Object.freeze(IsolationLevel),
  OffsetSpec,
};

var Client = require('./client');
var util = require('util');
var Kafka = require('../librdkafka');
var LibrdKafkaError = require('./error');
var { shallowCopy } = require('./util');

util.inherits(AdminClient, Client);

/**
 * Create a new AdminClient for making topics, partitions, and more.
 *
 * This is a factory method because it immediately starts an
 * active handle with the brokers.
 *
 * @param {object} conf - Key value pairs to configure the admin client
 * @param {object} eventHandlers - optional key value pairs of event handlers to attach to the client
 *
 */
function createAdminClient(conf, eventHandlers) {
  var client = new AdminClient(conf);

  if (eventHandlers && typeof eventHandlers === 'object') {
    for (const key in eventHandlers) {
      client.on(key, eventHandlers[key]);
    }
  }

  // Wrap the error so we throw if it failed with some context
  LibrdKafkaError.wrap(client.connect(), true);

  // Return the client if we succeeded
  return client;
}

/**
 * Create a new AdminClient from an existing producer or consumer.
 *
 * This is a factory method because it immediately starts an
 * active handle with the brokers.
 *
 * The producer or consumer being used must be connected.
 * The client can only be used while the producer or consumer is connected.
 * Logging and other events from this client will be emitted on the producer or consumer.
 * @param {import('../types/rdkafka').Producer | import('../types/rdkafka').KafkaConsumer} existingClient a producer or consumer to create the admin client from
 * @param {object} eventHandlers optional key value pairs of event handlers to attach to the client
 */
function createAdminClientFrom(existingClient, eventHandlers) {
  var client = new AdminClient(null, existingClient);

  if (eventHandlers && typeof eventHandlers === 'object') {
    for (const key in eventHandlers) {
      client.on(key, eventHandlers[key]);
    }
  }

  LibrdKafkaError.wrap(client.connect(), true);

  // Return the client if we succeeded
  return client;
}

/**
 * AdminClient class for administering Kafka
 *
 * This client is the way you can interface with the Kafka Admin APIs.
 * This class should not be made using the constructor, but instead
 * should be made using the factory method.
 *
 * <code>
 * var client = AdminClient.create({ ... }); // From configuration
 * var client = AdminClient.createFrom(existingClient); // From existing producer or consumer
 * </code>
 *
 * Once you instantiate this object, it will have a handle to the kafka broker.
 * Unlike the other confluent-kafka-javascript classes, this class does not ensure that
 * it is connected to the upstream broker. Instead, making an action will
 * validate that.
 *
 * @param {object} conf - Key value pairs to configure the admin client
 * topic configuration
 * @param {import('../types/rdkafka').Producer | import('../types/rdkafka').KafkaConsumer | null} existingClient
 * @constructor
 */
function AdminClient(conf, existingClient) {
  if (!(this instanceof AdminClient)) {
    return new AdminClient(conf);
  }

  if (conf) {
    conf = shallowCopy(conf);
  }

  Client.call(this, conf, Kafka.AdminClient, null, existingClient);

  if (existingClient) {
    this._isConnected = true;
    this._hasUnderlyingClient = true;
  } else {
    this._isConnected = false;
    this._hasUnderlyingClient = false;
  }
  this.globalConfig = conf;
}

/**
 * Connect using the admin client.
 *
 * Should be run using the factory method, so should never
 * need to be called outside.
 *
 * Unlike the other connect methods, this one is synchronous.
 */
AdminClient.prototype.connect = function () {
  if (!this._hasUnderlyingClient) {
    this._client.configureCallbacks(true, this._cb_configs);
    LibrdKafkaError.wrap(this._client.connect(), true);
  }
  // While this could be a no-op for an existing client, we still emit the event
  // to have a consistent API.
  this._isConnected = true;
  this.emit('ready', { name: this._client.name() });
};

/**
 * Disconnect the admin client.
 *
 * This is a synchronous method, but all it does is clean up
 * some memory and shut some threads down
 */
AdminClient.prototype.disconnect = function () {
  if (this._hasUnderlyingClient) {
    // no-op if we're from an existing client, we're just reusing the handle.
    return;
  }

  LibrdKafkaError.wrap(this._client.disconnect(), true);
  // The AdminClient doesn't provide a callback. So we can't
  // wait for completion.
  this._client.configureCallbacks(false, this._cb_configs);
  this._isConnected = false;
};

/**
 * Create a topic with a given config.
 *
 * @param {NewTopic} topic - Topic to create.
 * @param {number} timeout - Number of milliseconds to wait while trying to create the topic.
 * @param {function} cb - The callback to be executed when finished
 */
AdminClient.prototype.createTopic = function (topic, timeout, cb) {
  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if (typeof timeout === 'function') {
    cb = timeout;
    timeout = 5000;
  }

  if (!timeout) {
    timeout = 5000;
  }

  this._client.createTopic(topic, timeout, function (err) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      cb();
    }
  });
};

/**
 * Delete a topic.
 *
 * @param {string} topic - The topic to delete, by name.
 * @param {number} timeout - Number of milliseconds to wait while trying to delete the topic.
 * @param {function} cb - The callback to be executed when finished
 */
AdminClient.prototype.deleteTopic = function (topic, timeout, cb) {
  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if (typeof timeout === 'function') {
    cb = timeout;
    timeout = 5000;
  }

  if (!timeout) {
    timeout = 5000;
  }

  this._client.deleteTopic(topic, timeout, function (err) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      cb();
    }
  });
};

/**
 * Create new partitions for a topic.
 *
 * @param {string} topic - The topic to add partitions to, by name.
 * @param {number} totalPartitions - The total number of partitions the topic should have
 *                                   after the request
 * @param {number} timeout - Number of milliseconds to wait while trying to create the partitions.
 * @param {function} cb - The callback to be executed when finished
 */
AdminClient.prototype.createPartitions = function (topic, totalPartitions, timeout, cb) {
  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if (typeof timeout === 'function') {
    cb = timeout;
    timeout = 5000;
  }

  if (!timeout) {
    timeout = 5000;
  }

  this._client.createPartitions(topic, totalPartitions, timeout, function (err) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      cb();
    }
  });
};

/**
 * List consumer groups.
 * @param {any} options
 * @param {number?} options.timeout - The request timeout in milliseconds.
 *                                    May be unset (default: 5000)
 * @param {import("../").ConsumerGroupStates[]?} options.matchConsumerGroupStates -
 *        A list of consumer group states to match. May be unset, fetches all states (default: unset).
 * @param {function} cb - The callback to be executed when finished.
 *
 * Valid ways to call this function:
 * listGroups(cb)
 * listGroups(options, cb)
 */
AdminClient.prototype.listGroups = function (options, cb) {
  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  if (!options) {
    options = {};
  }

  this._client.listGroups(options, function (err, groups) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      cb(null, groups);
    }
  });
};

/**
 * Describe consumer groups.
 * @param {string[]} groups - The names of the groups to describe.
 * @param {any?} options
 * @param {number?} options.timeout - The request timeout in milliseconds.
 *                                    May be unset (default: 5000)
 * @param {boolean?} options.includeAuthorizedOperations - If true, include operations allowed on the group by the calling client (default: false).
 * @param {function} cb - The callback to be executed when finished.
 *
 * Valid ways to call this function:
 * describeGroups(groups, cb)
 * describeGroups(groups, options, cb)
 */
AdminClient.prototype.describeGroups = function (groups, options, cb) {
  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  if (!options) {
    options = {};
  }

  this._client.describeGroups(groups, options, function (err, descriptions) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      cb(null, descriptions);
    }
  });
};

/**
 * Delete consumer groups.
 * @param {string[]} groups - The names of the groups to delete.
 * @param {any?} options
 * @param {number?} options.timeout - The request timeout in milliseconds.
 *                                    May be unset (default: 5000)
 * @param {function} cb - The callback to be executed when finished.
 *
 * Valid ways to call this function:
 * deleteGroups(groups, cb)
 * deleteGroups(groups, options, cb)
 */
AdminClient.prototype.deleteGroups = function (groups, options, cb) {
  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  if (!options) {
    options = {};
  }

  this._client.deleteGroups(groups, options, function (err, reports) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      cb(null, reports);
    }
  });
};

/**
 * List topics.
 *
 * @param {any?} options
 * @param {number?} options.timeout - The request timeout in milliseconds.
 *                                    May be unset (default: 5000)
 * @param {function} cb - The callback to be executed when finished.
 *
 * Valid ways to call this function:
 * listTopics(cb)
 * listTopics(options, cb)
 */
AdminClient.prototype.listTopics = function (options, cb) {
  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  if (!options) {
    options = {};
  }

  // Always set allTopics to true, since we need a list.
  options.allTopics = true;
  if (!Object.hasOwn(options, 'timeout')) {
    options.timeout = 5000;
  }

  // This definitely isn't the fastest way to list topics as
  // this makes a pretty large metadata request. But for the sake
  // of AdminAPI, this is okay.
  this._client.getMetadata(options, function (err, metadata) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    const topics = [];
    if (metadata.topics) {
      for (const topic of metadata.topics) {
        topics.push(topic.name);
      }
    }

    if (cb) {
      cb(null, topics);
    }
  });
};

/**
 * List offsets for topic partition(s) for consumer group(s).
 *
 * @param {import("../../types/rdkafka").ListGroupOffsets} listGroupOffsets - The list of groupId, partitions to fetch offsets for.
 *                                                                            If partitions is null, list offsets for all partitions
 *                                                                            in the group.
 * @param {number?} options.timeout - The request timeout in milliseconds.
 *                                    May be unset (default: 5000)
 * @param {boolean?} options.requireStableOffsets - Whether broker should return stable offsets
 *                                                  (transaction-committed). (default: false)
 *
 * @param {function} cb - The callback to be executed when finished.
 */
AdminClient.prototype.listConsumerGroupOffsets = function (listGroupOffsets, options, cb) {

  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if (!listGroupOffsets[0].groupId) {
    throw new Error('groupId must be provided');
  }

  if(!options) {
    options = {};
  }

  if (!Object.hasOwn(options, 'timeout')) {
    options.timeout = 5000;
  }

  if (!Object.hasOwn(options, 'requireStableOffsets')) {
    options.requireStableOffsets = false;
  }

  this._client.listConsumerGroupOffsets(listGroupOffsets, options, function (err, offsets) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      cb(null, offsets);
    }
  });
};

/**
 * Deletes records (messages) in topic partitions older than the offsets provided.
 * Provide Topic.OFFSET_END or -1 as offset to delete all records.
 *
 * @param {import("../../types/rdkafka").TopicPartitionOffset[]} delRecords - The list of topic partitions and
 *                                                                            offsets to delete records up to.
 * @param {number?} options.operationTimeout - The operation timeout in milliseconds.
 *                                             May be unset (default: 60000)
 * @param {number?} options.timeout - The request timeout in milliseconds.
 *                                    May be unset (default: 5000)
 *
 * @param {function} cb - The callback to be executed when finished.
 */
AdminClient.prototype.deleteRecords = function (delRecords, options, cb) {
  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if(!options) {
    options = {};
  }

  if (!Object.hasOwn(options, 'timeout')) {
    options.timeout = 5000;
  }

  if (!Object.hasOwn(options, 'operationTimeout')) {
    options.operationTimeout = 60000;
  }

  if (!Array.isArray(delRecords) || delRecords.length === 0) {
    throw new Error('delRecords must be a non-empty array');
  }

  this._client.deleteRecords(delRecords, options, function (err, results) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      cb(null, results);
    }
  });
};

/**
 * Describe Topics.
 *
 * @param {string[]} topics - The names of the topics to describe.
 * @param {number?} options.timeout - The request timeout in milliseconds.
 *                                    May be unset (default: 5000)
 * @param {boolean?} options.includeAuthorizedOperations - If true, include operations allowed on the topic by the calling client.
 *                                                         (default: false)
 * @param {function} cb - The callback to be executed when finished.
 */
AdminClient.prototype.describeTopics = function (topics, options, cb) {
  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if(!options) {
    options = {};
  }

  if(!Object.hasOwn(options, 'timeout')) {
    options.timeout = 5000;
  }

  if(!Object.hasOwn(options, 'includeAuthorizedOperations')) {
    options.includeAuthorizedOperations = false;
  }

  this._client.describeTopics(topics, options, function (err, descriptions) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      cb(null, descriptions);
    }
  });
};

/**
 * List offsets for topic partition(s).
 *
 * @param {Array<{topic: string, partition: number, offset: OffsetSpec}>} partitions - The list of partitions to fetch offsets for.
 * @param {any?} options
 * @param {number?} options.timeout - The request timeout in milliseconds.
 *                                    May be unset (default: 5000)
 * @param {RdKafka.IsolationLevel?} options.isolationLevel - The isolation level for reading the offsets.
 *                                                           (default: READ_UNCOMMITTED)
 * @param {function} cb - The callback to be executed when finished.
 */
AdminClient.prototype.listOffsets = function (partitions, options, cb) {
  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if(!options) {
    options = {};
  }

  if (!Object.hasOwn(options, 'timeout')) {
    options.timeout = 5000;
  }

  if(!Object.hasOwn(options, 'isolationLevel')) {
    options.isolationLevel = IsolationLevel.READ_UNCOMMITTED;
  }

  this._client.listOffsets(partitions, options, function (err, offsets) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      cb(null, offsets);
    }
  });
};
