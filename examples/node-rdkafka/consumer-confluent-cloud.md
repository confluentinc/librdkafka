Connecting to a Kafka Consumer is easy. Let's try to connect to one using
the Stream implementation

```js
/*
 * confluent-kafka-javascript - Node.js wrapper  for RdKafka C/C++ library
 *
 * Copyright (c) 2023 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

const Transform = require('stream').Transform;

const Kafka = require('../');

const CLUSTER_BOOTSTRAP_URL = 'your_cluster_url_here';
const CLUSTER_API_KEY = 'your_cluster_api_key_here';
const CLUSTER_API_SECRET = 'your_cluster_api_secret_here';

const stream = Kafka.KafkaConsumer.createReadStream({
  'bootstrap.servers': `${CLUSTER_BOOTSTRAP_URL}`,
  'group.id': 'test-group',
  'socket.keepalive.enable': true,
  'enable.auto.commit': false,
  'security.protocol': 'SASL_SSL',
  'sasl.mechanisms': 'PLAIN',
  'sasl.username': `${CLUSTER_API_KEY}`,
  'sasl.password': `${CLUSTER_API_SECRET}`,
}, {}, {
  topics: 'test',
  waitInterval: 0,
  objectMode: false
});

stream.on('error', function(err) {
  if (err) console.log(err);
  process.exit(1);
});

stream
  .pipe(process.stdout);

stream.on('error', function(err) {
  console.log(err);
  process.exit(1);
});

stream.consumer.on('event.error', function(err) {
  console.log(err);
})
```
