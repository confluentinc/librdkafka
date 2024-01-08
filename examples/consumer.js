const { Kafka, ErrorCodes } = require('../').KafkaJS

async function consumerStart() {
  let stopped = false;

  // Initialization
  const consumer = new Kafka().consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-group2',
    'auto.offset.reset': 'earliest',
    'rebalance_cb': (err, assignment) => {
        switch (err.code) {
            case ErrorCodes.ERR__ASSIGN_PARTITIONS:
                console.log(`Assigned partitions ${JSON.stringify(assignment)}`);
                pauseResumeLoop();
                break;
            case ErrorCodes.ERR__REVOKE_PARTITIONS:
                console.log(`Revoked partitions ${JSON.stringify(assignment)}`);
                break;
            default:
                console.error(err);
        }
    },
  });

  await consumer.connect();
  console.log("Connected successfully");

  await consumer.subscribe({ topics: [ "topic2" ] });

  // Consume example with seek.
  let batch = 0;
  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString(),
        value: message.value.toString(),
      })

      // Seek example: seek to beginning every 100 messages
      if (++batch % 100 == 0) {
        await consumer.seek({
          topic,
          partition,
          offset: -2 /* Offset beginning is -2 */
        });
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

consumerStart();
