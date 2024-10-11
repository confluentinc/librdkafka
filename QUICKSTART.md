# Basic Producer Example

```javascript
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

async function producerStart() {
    const producer = new Kafka().producer({
        'bootstrap.servers': '<fill>',
    });

    await producer.connect();

    const deliveryReports = await producer.send({
            topic: 'topic2',
            messages: [
                { value: 'v222', partition: 0 },
                { value: 'v11', partition: 0, key: 'x' },
            ]
        });

    await producer.disconnect();
}

producerStart();
```

# Basic Consumer Example

```javascript
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

async function consumerStart() {
  let consumer;
  let stopped = false;

  // Initialization
  consumer = new Kafka().consumer({
    'bootstrap.servers': '<fill>',
    'group.id': 'test',
    'auto.offset.reset': 'earliest',
  });

  await consumer.connect();
  await consumer.subscribe({ topics: ["topic"] });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString(),
        value: message.value.toString(),
      });
    }
  });

  // Update stopped whenever we're done consuming.
  // The update can be in another async function or scheduled with setTimeout etc.
  while(!stopped) {
    await new Promise(resolve => setTimeout(resolve, 1000));
  }

  await consumer.disconnect();
}

consumerStart();
```

See the examples in the [examples](examples) directory for more in-depth examples.