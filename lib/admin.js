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

module.exports = {
  create: createAdminClient,
  ConsumerGroupStates,
  AclOperationTypes,
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
 * AdminClient class for administering Kafka
 *
 * This client is the way you can interface with the Kafka Admin APIs.
 * This class should not be made using the constructor, but instead
 * should be made using the factory method.
 *
 * <code>
 * var client = AdminClient.create({ ... });
 * </code>
 *
 * Once you instantiate this object, it will have a handle to the kafka broker.
 * Unlike the other confluent-kafka-javascript classes, this class does not ensure that
 * it is connected to the upstream broker. Instead, making an action will
 * validate that.
 *
 * @param {object} conf - Key value pairs to configure the admin client
 * topic configuration
 * @constructor
 */
function AdminClient(conf) {
  if (!(this instanceof AdminClient)) {
    return new AdminClient(conf);
  }

  conf = shallowCopy(conf);

  /**
   * NewTopic model.
   *
   * This is the representation of a new message that is requested to be made
   * using the Admin client.
   *
   * @typedef {object} AdminClient~NewTopic
   * @property {string} topic - the topic name to create
   * @property {number} num_partitions - the number of partitions to give the topic
   * @property {number} replication_factor - the replication factor of the topic
   * @property {object} config - a list of key values to be passed as configuration
   * for the topic.
   */

  Client.call(this, conf, Kafka.AdminClient);
  this._isConnected = false;
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
  this._client.configureCallbacks(true, this._cb_configs);
  LibrdKafkaError.wrap(this._client.connect(), true);
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
  LibrdKafkaError.wrap(this._client.disconnect(), true);
  this._isConnected = false;
  // The AdminClient doesn't provide a callback. So we can't
  // wait for completion.
  this._client.configureCallbacks(false, this._cb_configs);
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
