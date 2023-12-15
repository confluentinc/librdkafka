const { Kafka, logLevel } = require('../..').KafkaJS;
//const { Kafka } = require('kafkajs')

async function consumerStart() {
    let consumer;
    var stopped = false;

    const kafka = new Kafka({
        brokers: ['localhost:9092'],
    });

    consumer = kafka.consumer({
      groupId: 'test-group22' + Math.random(),
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
      rdKafka: {
        globalConfig: {
        'debug': 'conf',
        'enable.auto.commit': false,
        'auto.offset.reset': 'error'
        },
        topicConfig: {
        },
      }
    });

    await consumer.connect();
    console.log("Connected successfully");

    await consumer.subscribe({
      topics: [
        "test-topic"
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

    if (consumer.assignment) {
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
