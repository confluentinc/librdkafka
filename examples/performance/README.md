# Performance Benchmarking

The library can be benchmarked by running the following command:

```bash
node performance-consolidated.js [--producer] [--consumer] [--ctp] [--all]
```

The `--producer` flag will run the producer benchmark, the `--consumer` flag
will run the consumer benchmark, and the `--ctp` flag will run the
consume-transform-produce benchmark.

The `--create-topics` flag will create the topics before running the benchmarks
(and delete any existing topics of the same name). It's recommended to use this
unless the number of partitions or replication factor needs to be changed.

If no flags are provided, no benchmarks will be run. If the `--all` flag is
provided, all benchmarks will be run ignoring any other flags.

The benchmarks assume topics are already created (unless usig `--create-topics`).
The consumer benchmark assumes that the topic already has at least `MESSAGE_COUNT` messages within,
which can generally be done by running the producer benchmark along with it.

The following environment variables can be set to configure the benchmark, with
default values given in parentheses.

| Variable | Description | Default |
|----------|-------------|---------|
| KAFKA_BROKERS | Kafka brokers to connect to | localhost:9092 |
| KAFKA_TOPIC | Kafka topic to produce to/consume from | test-topic |
| KAFKA_TOPIC2 | Kafka topic to produce to after consumption in consume-transform-produce | test-topic2 |
| MESSAGE_COUNT | Number of messages to produce/consume | 1000000 |
| MESSAGE_SIZE | Size of each message in bytes | 256 |
| BATCH_SIZE | Number of messages to produce in a single batch | 100 |
| COMPRESSION | Compression codec to use (None, GZIP, Snappy, LZ4, ZSTD) | None |
| WARMUP_MESSAGES | Number of messages to produce before starting the produce benchmark | BATCH_SIZE * 10 |
| MESSAGE_PROCESS_TIME_MS | Time to sleep after consuming each message in the consume-transform-produce benchmark. Simulates "transform". May be 0. | 5 |
| CONSUME_TRANSFORM_PRODUCE_CONCURRENCY | partitionsConsumedConcurrently for the consume-transform-produce benchmark | 1 |
| MODE | Mode to run the benchmarks in (confluent, kafkajs). Can be used for comparison with KafkaJS | confluent |
