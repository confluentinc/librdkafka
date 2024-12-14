/*
 * confluent-kafka-javascript - Node.js wrapper  for RdKafka C/C++ library
 *
 * Copyright (c) 2016-2023 Blizzard Entertainment
 *           (c) 2023 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

module.exports = Client;

var Emitter = require('events').EventEmitter;
var util = require('util');
var Kafka = require('../librdkafka.js');

const { bindingVersion, dictToStringList } = require('./util');

var LibrdKafkaError = require('./error');

util.inherits(Client, Emitter);

/**
 * Base class for Consumer and Producer.
 *
 * This should not be created independently, but rather is
 * the base class on which both producer and consumer
 * get their common functionality.
 *
 * @param {object} globalConf - Global configuration in key value pairs.
 * @param {function} SubClientType - The function representing the subclient
 * type. In C++ land this needs to be a class that inherits from Connection.
 * @param {object} topicConf - Topic configuration in key value pairs
 * @param {object?} existingClient - a producer or a consumer to derive this client from.
 *                                   Only used by the AdminClient. Must be connected.
 * @constructor
 * @extends Emitter
 * @memberof RdKafka
 * @package
 */
function Client(globalConf, SubClientType, topicConf, existingClient) {
  if (!(this instanceof Client)) {
    return new Client(globalConf, SubClientType, topicConf, existingClient);
  }

  // Throw an error early - this allows us to set confs to {} and avoid all
  // the null-checking in case of existingClient being set.
  if (!existingClient && !globalConf) {
    throw new Error('Global configuration data must be specified');
  }

  if (existingClient && globalConf) {
    throw new Error('Global configuration data must not be specified when creating a client from an existing client');
  }

  if (existingClient && topicConf) {
    throw new Error('Topic configuration data must not be specified when creating a client from an existing client');
  }

  if (existingClient && !(existingClient._client instanceof Kafka.Producer) && !(existingClient._client instanceof Kafka.KafkaConsumer)) {
    throw new Error('Existing client must be a Producer or Consumer instance');
  }

  if (existingClient && !existingClient._isConnected) {
    throw new Error('Existing client must be connected before creating a new client from it');
  }

  let existingInternalClient;
  if (existingClient) {
    globalConf = {};
    topicConf = {};
    existingInternalClient = existingClient.getClient();
  }

  Emitter.call(this);

  // This superclass must be initialized with the Kafka.{Producer,Consumer}
  // @example var client = new Client({}, Kafka.Producer);
  // remember this is a superclass so this will get taken care of in
  // the producer and consumer main wrappers

  var no_event_cb = globalConf.event_cb === false;

  // delete this because librdkafka will complain since this particular
  // key is a real conf value
  delete globalConf.event_cb;

  // These properties are not meant to be user-set.
  // Clients derived from this might want to change them, but for
  // now we override them.
  globalConf['client.software.name'] = 'confluent-kafka-javascript';
  globalConf['client.software.version'] = `${bindingVersion}-librdkafka-${Kafka.librdkafkaVersion}`;

  this._client = new SubClientType(globalConf, topicConf, existingInternalClient);

  // We should not modify the globalConf object. We have cloned it already.
  delete globalConf['client.software.name'];
  delete globalConf['client.software.version'];

  var extractFunctions = function(obj) {
    obj = obj || {};
    var obj2 = {};
    for (var p in obj) {
      if (typeof obj[p] === "function") {
        obj2[p] = obj[p];
      }
    }
    return obj2;
  };
  this._cb_configs = {
    global: extractFunctions(globalConf),
    topic: extractFunctions(topicConf),
    event: {},
  };

  if (!existingClient && !no_event_cb) {
    this._cb_configs.event.event_cb = function(eventType, eventData) {
      switch (eventType) {
        case 'error':
          this.emit('event.error', LibrdKafkaError.create(eventData));
          break;
        case 'stats':
          this.emit('event.stats', eventData);
          break;
        case 'log':
          this.emit('event.log', eventData);
          break;
        default:
          this.emit('event.event', eventData);
          this.emit('event.' + eventType, eventData);
      }
    }.bind(this);
  }

  if (Object.hasOwn(this._cb_configs.global, 'oauthbearer_token_refresh_cb')) {
    const savedCallback = this._cb_configs.global.oauthbearer_token_refresh_cb;
    this._cb_configs.global.oauthbearer_token_refresh_cb = (oauthbearer_config) => {
      if (this._isDisconnecting) {
        // Don't call the callback if we're in the middle of disconnecting.
        // This is especially important when the credentials are wrong, and
        // we might want to disconnect without ever completing connection.
        return;
      }

      // This sets the token or error within librdkafka, and emits any
      // errors on the emitter.
      const postProcessTokenRefresh = (err, token) => {
        try {
          if (err) {
            throw err;
          }
          let { tokenValue, lifetime, principal, extensions } = token;

          // If the principal isn't there, set an empty principal.
          if (!principal) {
            principal = '';
          }

          // Convert extensions from a Map/object to a list that librdkafka expects.
          extensions = dictToStringList(extensions);

          this._client.setOAuthBearerToken(tokenValue, lifetime, principal, extensions);
        } catch (e) {
          e.message = "oauthbearer_token_refresh_cb: " + e.message;
          this._client.setOAuthBearerTokenFailure(e.message);
          this.emit('error', e);
        }
      };
      const returnPromise = savedCallback(oauthbearer_config, postProcessTokenRefresh);

      // If it looks like a promise, and quacks like a promise, it is a promise
      // (or an async function). We expect the callback NOT to have been called
      // in such a case.
      if (returnPromise && (typeof returnPromise.then === 'function')) {
        returnPromise.then((token) => {
          postProcessTokenRefresh(null, token);
        }).catch(err => {
          postProcessTokenRefresh(err);
        });
      }
    };
  }

  this.metrics = {};
  this._isConnected = false;
  this.errorCounter = 0;

  /**
   * Metadata object. Starts out empty but will be filled with information after
   * the initial connect.
   *
   * @type {RdKafka.Client~Metadata}
   * @private
   */
  this._metadata = {};

  var self = this;

  this.on('ready', function(info) {
    self.metrics.connectionOpened = Date.now();
    self.name = info.name;
  })
  .on('disconnected', function() {
    // reset metrics
    self.metrics = {};
    self._isConnected = false;
    // keep the metadata. it still may be useful
  })
  .on('event.error', function(err) {
    self.lastError = err;
    ++self.errorCounter;
  });

}

/**
 * Connect to the broker and receive its metadata.
 *
 * Connects to a broker by establishing the client and fetches its metadata.
 *
 * @param {object?} metadataOptions - Options to be sent to the metadata.
 * @param {string} metadataOptions.topic - Topic to fetch metadata for. Empty string is treated as empty.
 * @param {boolean} metadataOptions.allTopics - Fetch metadata for all topics, not just the ones we know about.
 * @param {int} metadataOptions.timeout - The timeout, in ms, to allow for fetching metadata. Defaults to 30000ms
 * @param  {RdKafka.Client~connectionCallback?} cb - Callback that indicates we are
 * done connecting.
 * @fires RdKafka.Client#ready
 * @return {RdKafka.Client} - Returns itself.
 */
Client.prototype.connect = function(metadataOptions, cb) {
  if (this._hasUnderlyingClient) {
    // This is a derived client. We don't want to connect it, it's already connected.
    // No one should be reaching this method in the first place if they use the
    // API correctly, but it is possible to do so accidentally.
    throw new Error('Cannot connect an existing client');
  }

  var self = this;

  var next = function(err, data) {
    self._isConnecting = false;
    if (cb) {
      cb(err, data);
    }
  };

  if (this._isConnected) {
    setImmediate(next);
    return self;
  }

  if (this._isConnecting) {
    this.once('ready', function() {
      next(null, this._metadata);
    });
    return self;
  }

  this._isConnecting = true;

  var fail = function(err) {
    var callbackCalled = false;
    var t;

    if (self._isConnected) {
      self._isConnected = false;
      self._client.disconnect(function() {
        if (callbackCalled) {
          return;
        }
        clearTimeout(t);
        callbackCalled = true;

        next(err); return;
      });

      // don't take too long. this is a failure, after all
      t = setTimeout(function() {
        if (callbackCalled) {
          return;
        }
        callbackCalled = true;

        next(err); return;
      }, 10000).unref();

      self.emit('connection.failure', err, self.metrics);
    } else {

      next(err);
    }
  };

  this._client.configureCallbacks(true, this._cb_configs);

  this._client.connect(function(err, info) {
    if (err) {
      fail(LibrdKafkaError.create(err)); return;
    }

    self._isConnected = true;

    // Otherwise we are successful
    self.getMetadata(metadataOptions || {}, function(err, metadata) {
      if (err) {
        // We are connected so we need to disconnect
        fail(LibrdKafkaError.create(err)); return;
      }

      self._isConnecting = false;
      // We got the metadata otherwise. It is set according to above param
      // Set it here as well so subsequent ready callbacks
      // can check it
      self._isConnected = true;

      /**
       * Ready event. Called when the Client connects successfully
       *
       * @event RdKafka.Client#ready
       * @type {object}
       * @property {string} name - the name of the client.
       */
      self.emit('ready', info, metadata);
      next(null, metadata); return;

    });

  });

  return self;

};

/**
 * Get the native Kafka client.
 *
 * You probably shouldn't use this, but if you want to execute methods directly
 * on the C++ wrapper you can do it here.
 *
 * @see connection.cc
 * @return {Connection} - The native Kafka client.
 */
Client.prototype.getClient = function() {
  return this._client;
};

/**
 * Find out how long we have been connected to Kafka.
 *
 * @return {number} - Milliseconds since the connection has been established.
 */
Client.prototype.connectedTime = function() {
  if (!this.isConnected()) {
    return 0;
  }
  return Date.now() - this.metrics.connectionOpened;
};

/**
 * Whether or not we are connected to Kafka.
 *
 * @return {boolean} - Whether we are connected.
 */
Client.prototype.isConnected = function() {
  return !!(this._isConnected && this._client);
};

/**
 * Get the last error emitted if it exists.
 *
 * @return {LibrdKafkaError?} - Returns the LibrdKafkaError or null if
 * one hasn't been thrown.
 */
Client.prototype.getLastError = function() {
  return this.lastError || null;
};

/**
 * Disconnect from the Kafka client.
 *
 * This method will disconnect us from Kafka unless we are already in a
 * disconnecting state. Use this when you're done reading or producing messages
 * on a given client.
 *
 * It will also emit the disconnected event.
 *
 * @fires RdKafka.Client#disconnected
 * @param {function} cb - Callback to call when disconnection is complete.
 */
Client.prototype.disconnect = function(cb) {
  if (this._hasUnderlyingClient) {
    // This is a derived client.
    // We don't want to disconnect it as it's controlled by the underlying client.
    // No one should be reaching this method in the first place if they use the
    // API correctly, but it is possible to do so accidentally.
    throw new Error('Cannot disconnect an existing client');
  }

  var self = this;

  if (!this._isDisconnecting && this._client) {
    this._isDisconnecting = true;
    this._client.disconnect(function() {
      // this take 5000 milliseconds. Librdkafka needs to make sure the memory
      // has been cleaned up before we delete things. @see RdKafka::wait_destroyed
      self._client.configureCallbacks(false, self._cb_configs);

      // Broadcast metrics. Gives people one last chance to do something with them
      self._isDisconnecting = false;
      var metricsCopy = Object.assign({}, self.metrics);
      /**
       * Disconnect event. Called after disconnection is finished.
       *
       * @event RdKafka.Client#disconnected
       * @type {object}
       * @property {date} connectionOpened - when the connection was opened.
       * @memberof RdKafka
       */
      self.emit('disconnected', metricsCopy);
      if (cb) {
        cb(null, metricsCopy);
      }

    });

  }

  return self;
};

/**
 * Get client metadata.
 *
 * Use {@link RdKafka.AdminClient#describeTopics} instead for fetching metadata for specific
 * topics.
 *
 * Note: using a `metadataOptions.topic` parameter has a potential side-effect.
 * A Topic object will be created, if it did not exist yet, with default options
 * and it will be cached by librdkafka.
 *
 * A subsequent call to create the topic object with specific options (e.g. `acks`) will return
 * the previous instance and the specific options will be silently ignored.
 *
 * To avoid this side effect, the topic object can be created with the expected options before requesting metadata,
 * or the metadata request can be performed for all topics (by omitting `metadataOptions.topic`).
 *
 * @param {object} metadataOptions - Metadata options to pass to the client.
 * @param {string} metadataOptions.topic - Topic string for which to fetch
 * metadata
 * @param {number} metadataOptions.timeout - Max time, in ms, to try to fetch
 * metadata before timing out. Defaults to 3000.
 * @param {function} cb - Callback to fire with the metadata.
 */
Client.prototype.getMetadata = function(metadataOptions, cb) {
  if (!this.isConnected()) {
    return cb(new Error('Client is disconnected'));
  }

  var self = this;

  this._client.getMetadata(metadataOptions || {}, function(err, metadata) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    // No error otherwise
    self._metadata = metadata;

    if (cb) {
      cb(null, metadata);
    }

  });

};

/**
 * Query offsets from the broker.
 *
 * This function makes a call to the broker to get the current low (oldest/beginning)
 * and high (newest/end) offsets for a topic partition.
 *
 * @param {string} topic - Topic to recieve offsets from.
 * @param {number} partition - Partition of the provided topic to recieve offsets from
 * @param {number} timeout - Number of ms to wait to recieve a response.
 * @param {RdKafka.ClientClient~watermarkOffsetsCallback} cb - Callback to fire with the offsets.
 */
Client.prototype.queryWatermarkOffsets = function(topic, partition, timeout, cb) {
  if (!this.isConnected()) {
    if (cb) {
      return cb(new Error('Client is disconnected'));
    } else {
      return;
    }
  }

  if (typeof timeout === 'function') {
    cb = timeout;
    timeout = 1000;
  }

  if (!timeout) {
    timeout = 1000;
  }

  this._client.queryWatermarkOffsets(topic, partition, timeout, function(err, offsets) {
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
 * Query offsets for times from the broker.
 *
 * This function makes a call to the broker to get the offsets for times specified.
 *
 * @param {TopicPartition[]} toppars - Array of topic partitions. The offset in these
 *                                     should instead refer to a timestamp you want
 *                                     offsets for
 * @param {number} timeout - Number of ms to wait to recieve a response.
 * @param {RdKafka.Client~offsetsForTimesCallback} cb - Callback to fire with the filled in offsets.
 */
Client.prototype.offsetsForTimes = function(toppars, timeout, cb) {
  if (!this.isConnected()) {
    if (cb) {
      return cb(new Error('Client is disconnected'));
    } else {
      return;
    }
  }

  if (typeof timeout === 'function') {
    cb = timeout;
    timeout = 1000;
  }

  if (!timeout) {
    timeout = 1000;
  }

  this._client.offsetsForTimes(toppars, timeout, function(err, toppars) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      cb(null, toppars);
    }

  });
};

/**
 * Change SASL credentials to be sent on the next authentication attempt.
*
 * Only applicable if SASL authentication is being used.
 * @param {string} username
 * @param {string} password
 */
Client.prototype.setSaslCredentials = function(username, password) {
  if (!this.isConnected()) {
      return;
  }

  this._client.setSaslCredentials(username, password);
};

/**
 * Wrap a potential RdKafka error.
 *
 * This internal method is meant to take a return value
 * from a function that returns an RdKafka error code, throw if it
 * is an error (Making it a proper librdkafka error object), or
 * return the appropriate value otherwise.
 *
 * It is intended to be used in a return statement,
 *
 * @private
 * @param {number} errorCode - Error code returned from a native method
 * @param {bool} intIsError - If specified true, any non-number return type will be classified as a success
 * @return {boolean} - Returns true or the method return value unless it throws.
 */
Client.prototype._errorWrap = function(errorCode, intIsError) {
  var returnValue = true;
  if (intIsError) {
    returnValue = errorCode;
    errorCode = typeof errorCode === 'number' ? errorCode : 0;
  }

  if (errorCode !== LibrdKafkaError.codes.ERR_NO_ERROR) {
    var e = LibrdKafkaError.create(errorCode);
    throw e;
  }

  return returnValue;
};

/**
 * This callback is used to pass metadata or an error after a successful
 * connection
 *
 * @callback RdKafka.Client~connectionCallback
 * @param {Error} err - An error, if one occurred while connecting.
 * @param {RdKafka.Client~Metadata} metadata - Metadata object.
 */

 /**
  * This callback is used to pass offsets or an error after a successful
  * query
  *
  * @callback RdKafka.Client~watermarkOffsetsCallback
  * @param {Error} err - An error, if one occurred while connecting.
  * @param {RdKafka.Client~watermarkOffsets} offsets - Watermark offsets
  */

  /**
   * This callback is used to pass toppars or an error after a successful
   * times query
   *
   * @callback RdKafka.Client~offsetsForTimesCallback
   * @param {Error} err - An error, if one occurred while connecting.
   * @param {TopicPartition[]} toppars - Topic partitions with offsets filled in
   */

/**
 * @typedef {object} RdKafka.Client~watermarkOffsets
 * @property {number} high - High (newest/end) offset
 * @property {number} low - Low (oldest/beginning) offset
 */

/**
 * @typedef {object} RdKafka.Client~MetadataBroker
 * @property {number} id - Broker ID
 * @property {string} host - Broker host
 * @property {number} port - Broker port.
 */

/**
 * @typedef {object} RdKafka.Client~MetadataTopic
 * @property {string} name - Topic name
 * @property {RdKafka.Client~MetadataPartition[]} partitions - Array of partitions
 */

/**
 * @typedef {object} RdKafka.Client~MetadataPartition
 * @property {number} id - Partition id
 * @property {number} leader - Broker ID for the partition leader
 * @property {number[]} replicas - Array of replica IDs
 * @property {number[]} isrs - Array of ISRS ids
*/

/**
 * Metadata object.
 *
 * This is the representation of Kafka metadata in JavaScript.
 *
 * @typedef {object} RdKafka.Client~Metadata
 * @property {number} orig_broker_id - The broker ID of the original bootstrap
 * broker.
 * @property {string} orig_broker_name - The name of the original bootstrap
 * broker.
 * @property {RdKafka.Client~MetadataBroker[]} brokers - An array of broker objects
 * @property {RdKafka.Client~MetadataTopic[]} topics - An array of topics.
 */
