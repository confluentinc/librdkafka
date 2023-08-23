async function kafkaJSToRdKafkaConfig(config) {
  const ret = {
    'allow.auto.create.topics': 'false'
  }
  ret['bootstrap.servers'] = config['brokers'].join(',');

  let withSASL = false;

  if (config.sasl) {
    const sasl  = config.sasl;
    if (sasl.mechanism === 'plain' &&
        typeof sasl.username === 'string' &&
        typeof sasl.password === 'string') {
        ret['sasl.mechanism'] = 'PLAIN';
        ret['sasl.username'] = sasl.username;
        ret['sasl.password'] = sasl.password;
        withSASL = true;
    }
  }

  if (config.ssl === true && withSASL) {
    ret['security.protocol'] = 'sasl_ssl';
  } else if (withSASL) {
    ret['security.protocol'] = 'sasl_plaintext';
  }

  if (config.rdKafka) {
    if (config.rdKafka.constructor === Function) {
      await config.rdKafka(ret);
    } else {
      Object.assign(ret, config.rdKafka);
    }
  }

  return ret;
}

module.exports = { kafkaJSToRdKafkaConfig }
