/*
 * confluent-kafka-javascript - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2024 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

var Kafka = require('../');
var t = require('assert');

var kafkaBrokerList = process.env.KAFKA_HOST || 'localhost:9092';
var time = Date.now();

describe('Dependent Admin', function () {
  describe('from Producer', function () {
    let producer;

    this.beforeEach(function (done) {
      producer = new Kafka.Producer({
        'metadata.broker.list': kafkaBrokerList,
      });
      done();
    });

    it('should be created and useable from connected producer', function (done) {
      producer.on('ready', function () {
        let admin = Kafka.AdminClient.createFrom(producer);
        admin.listTopics(null, function (err, res) {
          t.ifError(err);
          t.ok(res);
          producer.disconnect(done);
          admin = null;
        });
        t.ok(admin);
      });
      producer.connect();
    });

    it('should fail to be created from unconnected producer', function (done) {
      t.throws(function () {
        Kafka.AdminClient.createFrom(producer);
      }, /Existing client must be connected before creating a new client from it/);
      done();
    });

  });

  describe('from Consumer', function () {
    let consumer;

    this.beforeEach(function (done) {
      consumer = new Kafka.KafkaConsumer({
        'metadata.broker.list': kafkaBrokerList,
        'group.id': 'kafka-mocha-grp-' + time,
      });
      done();
    });

    it('should be created and useable from connected consumer', function (done) {
      consumer.on('ready', function () {
        let admin = Kafka.AdminClient.createFrom(consumer);
        admin.listTopics(null, function (err, res) {
          t.ifError(err);
          t.ok(res);
          consumer.disconnect(done);
          admin = null;
        });
        t.ok(admin);
      });
      consumer.connect();
    });

    it('should fail to be created from unconnected consumer', function (done) {
      t.throws(function () {
        Kafka.AdminClient.createFrom(consumer);
      }, /Existing client must be connected before creating a new client from it/);
      done();
    });

  });
});
