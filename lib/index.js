var KafkaJS = require('./kafkajs');
var RdKafka = require('./rdkafka');

module.exports = {
  ...RdKafka,
  RdKafka,
  KafkaJS
};
