/**
 * @function kafkaJSToRdKafkaConfig()
 * @param {object} config
 * @returns {{globalConfig: import("../../types/config").ConsumerGlobalConfig|import("../../types/config").ProducerTopicConfig, topicConfig: import("../../types/config").ConsumerTopicConfig|import("../../types/config").ProducerTopicConfig}}
 */
async function kafkaJSToRdKafkaConfig(config) {
  const globalConfig = {
    "allow.auto.create.topics": "false",
  };
  const topicConfig = {};
  globalConfig["bootstrap.servers"] = config["brokers"].join(",");

  let withSASL = false;

  if (config.sasl) {
    const sasl = config.sasl;
    if (
      sasl.mechanism === "plain" &&
      typeof sasl.username === "string" &&
      typeof sasl.password === "string"
    ) {
      globalConfig["sasl.mechanism"] = "PLAIN";
      globalConfig["sasl.username"] = sasl.username;
      globalConfig["sasl.password"] = sasl.password;
      withSASL = true;
    }
  }

  if (config.ssl === true && withSASL) {
    globalConfig["security.protocol"] = "sasl_ssl";
  } else if (withSASL) {
    globalConfig["security.protocol"] = "sasl_plaintext";
  }

  if (config.rdKafka) {
    if (config.rdKafka.constructor === Function) {
      await config.rdKafka(globalConfig, topicConfig);
    } else {
      Object.assign(globalConfig, config.rdKafka.globalConfig);
      Object.assign(topicConfig, config.rdKafka.topicConfig);
    }
  }

  return { globalConfig, topicConfig };
}

function topicPartitionOffsetToRdKafka(tpo) {
  return {
    topic: tpo.topic,
    partition: tpo.partition,
    offset: Number(tpo.offset),
  };
}

module.exports = { kafkaJSToRdKafkaConfig, topicPartitionOffsetToRdKafka };
