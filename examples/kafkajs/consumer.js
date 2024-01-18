// require('kafkajs') is replaced with require('confluent-kafka-js').KafkaJS.
// Since this example is within the package itself, we use '../..', but code
// will typically use 'confluent-kafka-js'.
const { Kafka } = require('../..').KafkaJS;

async function consumerStart() {
  let consumer;
  var stopped = false;

  const kafka = new Kafka({
    kafkaJS: {
      brokers: ['localhost:9092'],
      ssl: true,
      connectionTimeout: 5000,
      sasl: {
        mechanism: 'plain',
        username: '<fill>',
        password: '<fill>',
      },
    }
  });

  consumer = kafka.consumer({
    kafkaJS: {
      groupId: 'test-group',
      autoCommit: false,
      rebalanceListener: {
        onPartitionsAssigned: async (assignment) => {
          console.log(`Assigned partitions ${JSON.stringify(assignment)}`);
        },
        onPartitionsRevoked: async (assignment) => {
          console.log(`Revoked partitions ${JSON.stringify(assignment)}`);
          if (!stopped) {
            await consumer.commitOffsets().catch((e) => {
              console.error(`Failed to commit ${e}`);
            })
          }
        }
      },
    },

    /* Properties from librdkafka can also be used */
    'auto.commit.interval.ms': 6000,
  });

  await consumer.connect();
  console.log("Connected successfully");

  await consumer.subscribe({
    topics: [
      "topic2"
    ]
  })

  // Batch consumer, commit and seek example
  var batch = 0;
  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString(),
        value: message.value.toString(),
      })

      if (++batch % 100 == 0) {
        await consumer.seek({
          topic,
          partition,
          offset: -2
        });
        await consumer.commitOffsets();
        batch = 0;
      }
    },
  });

  // Pause/Resume example
  const pauseResumeLoop = async () => {
    let paused = false;
    let ticks = 0;
    while (!stopped) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      if (stopped)
        break;

      ticks++;
      if (ticks == 200) {
        ticks = 0;
        const assignment = consumer.assignment();
        if (paused) {
          console.log(`Resuming partitions ${JSON.stringify(assignment)}`)
          consumer.resume(assignment);
        } else {
          console.log(`Pausing partitions ${JSON.stringify(assignment)}`);
          consumer.pause(assignment);
        }
        paused = !paused;
      }
    }
  }

  if (consumer.assignment()) {
    // KafkaJS doesn't have assignment()
    pauseResumeLoop()
  }

  // Disconnect example
  const disconnect = () => {
    process.off('SIGINT', disconnect);
    process.off('SIGTERM', disconnect);
    stopped = true;
    consumer.commitOffsets()
      .finally(() =>
        consumer.disconnect()
      )
      .finally(() =>
        console.log("Disconnected successfully")
      );
  }
  process.on('SIGINT', disconnect);
  process.on('SIGTERM', disconnect);
}

consumerStart()
