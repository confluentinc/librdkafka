/*
 * confluent-kafka-javascript - Node.js wrapper  for RdKafka C/C++ library
 *
 * Copyright (c) 2024 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

var Kafka = require('../');
var t = require('assert');

var eventListener = require('./listener');

var kafkaBrokerList = process.env.KAFKA_HOST || 'localhost:9092';

const oauthbearer_config = 'key=value';
let oauthbearer_cb_called = 0;

let oauthbearer_cb_callback = function (config, cb) {
  console.log("Called oauthbearer_cb with given config: " + config);
  t.equal(config, oauthbearer_config);
  oauthbearer_cb_called++;

  // The broker is not expected to be configured for oauthbearer authentication.
  // We just want to make sure that token refresh callback is triggered.
  cb(new Error('oauthbearer_cb error'), null);
};

let oauthbearer_cb_async = async function (config) {
  console.log("Called oauthbearer_cb with given config: " + config);
  t.equal(config, oauthbearer_config);
  oauthbearer_cb_called++;

  // The broker is not expected to be configured for oauthbearer authentication.
  // We just want to make sure that token refresh callback is triggered.
  throw new Error('oauthbearer_cb error');
};

for (const oauthbearer_cb of [oauthbearer_cb_async, oauthbearer_cb_callback]) {
  describe('Client with ' + (oauthbearer_cb.name), function () {

    const commonConfig = {
      'metadata.broker.list': kafkaBrokerList,
      'debug': 'all',
      'security.protocol': 'SASL_PLAINTEXT',
      'sasl.mechanisms': 'OAUTHBEARER',
      'oauthbearer_token_refresh_cb': oauthbearer_cb,
      'sasl.oauthbearer.config': oauthbearer_config,
    }

    const checkClient = function (client, done, useCb) {
      eventListener(client);

      client.on('error', function (e) {
        t.match(e.message, /oauthbearer_cb error/);
      });

      // The default timeout for the connect is 30s, so even if we
      // call disconnect() midway, the test ends up being at least 30s.
      client.connect({timeout: 2000});

      // We don't actually expect the connection to succeed, but we want to
      // make sure that the oauthbearer_cb is called so give it a couple seconds.
      setTimeout(() => {
        t.equal(oauthbearer_cb_called >= 1, true);
        client.disconnect(() => {
          done();
        });
        client = null;
        if (!useCb) // for admin client, where disconnect is sync.
          done();
      }, 2000);
    }

    beforeEach(function (done) {
      oauthbearer_cb_called = 0;
      done();
    });

    it('as producer', function (done) {
      let producer = new Kafka.Producer(commonConfig);
      checkClient(producer, done, true);
      producer = null;
    }).timeout(5000);

    it('as consumer', function (done) {
      const config = Object.assign({ 'group.id': 'gid' }, commonConfig);
      let consumer = new Kafka.KafkaConsumer(config);
      checkClient(consumer, done, true);
      consumer = null;
    }).timeout(5000);

    it('as admin', function (done) {
      let admin = new Kafka.AdminClient.create(commonConfig);
      checkClient(admin, done, false);
      admin = null;
    }).timeout(5000);

  });
}
