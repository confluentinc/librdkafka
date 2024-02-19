```js
const Kafka = require('../');

const CLUSTER_BOOTSTRAP_URL = 'your_cluster_url_here';
const CLUSTER_API_KEY = 'your_cluster_api_key_here';
const CLUSTER_API_SECRET = 'your_cluster_api_secret_here';

const producer = new Kafka.HighLevelProducer({
  'bootstrap.servers': `${CLUSTER_BOOTSTRAP_URL}`,
  'security.protocol': 'SASL_SSL',
  'sasl.mechanisms': 'PLAIN',
  'sasl.username': `${CLUSTER_API_KEY}`,
  'sasl.password': `${CLUSTER_API_SECRET}`,
});

// Throw away the keys
producer.setKeySerializer(function(v) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve(null);
    }, 20);
  });
});

// Take the message field
producer.setValueSerializer(function(v) {
  return Buffer.from(v.message);
});

producer.connect(null, function() {
  producer.produce('test', null, {
    message: 'alliance4ever',
  }, null, Date.now(), function(err, offset) {
    // The offset if our acknowledgement level allows us to receive delivery offsets
    setImmediate(function() {
      producer.disconnect();
    });
  });
});
```
