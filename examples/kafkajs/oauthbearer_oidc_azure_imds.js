const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

async function run() {
    const bootstrapServers = "";
    const azureIMDSQueryParams = "api-version=&resource=&client_id=";
    const kafka = new Kafka({
        'bootstrap.servers': bootstrapServers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'OAUTHBEARER',
        'sasl.oauthbearer.method': 'oidc',
        'sasl.oauthbearer.metadata.authentication.type': 'azure_imds',
        'sasl.oauthbearer.config': `query=${azureIMDSQueryParams}`
    });
    const producer = kafka.producer();

    await producer.connect();
    console.log("Producer connected");

    const deliveryReport = await producer.send({
        topic: 'topic',
        messages: [
            { value: 'Hello world!' },
        ],
    });
    console.log("Producer sent message", deliveryReport);

    await producer.disconnect();
}

run().catch(console.error);