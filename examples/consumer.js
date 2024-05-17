const { Kafka, ErrorCodes } = require('@confluentinc/kafka-javascript').KafkaJS;

async function consumerStart() {
  let consumer;
  let stopped = false;

  // Pause/Resume example, pause and resume alternately every 2 seconds.
  let pauseResumeLoopStarted = false;
  const pauseResumeLoop = async () => {
    let paused = false;
    pauseResumeLoopStarted = true;
    while (!stopped) {
      await new Promise((resolve) => setTimeout(resolve, 2000));
      if (stopped)
        break;

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
  };

  // Set up signals for a graceful shutdown.
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



  // Initialization
  consumer = new Kafka().consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest',
    'enable.partition.eof': 'true',
    'rebalance_cb': (err, assignment) => {
      switch (err.code) {
        case ErrorCodes.ERR__ASSIGN_PARTITIONS:
          console.log(`Assigned partitions ${JSON.stringify(assignment)}`);
          if (!pauseResumeLoopStarted) // Start the pause/resume loop for the example.
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
  await consumer.subscribe({ topics: ["test-topic"] });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        headers: message.headers,
        offset: message.offset,
        key: message.key?.toString(),
        value: message.value.toString(),
      });
    }
  });
}

consumerStart();
