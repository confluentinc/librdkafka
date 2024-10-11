const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
var jwt = require('jsonwebtoken');

// This example uses the Producer for demonstration purposes.
// It is the same whether you use a Consumer/AdminClient.

async function token_refresh(oauthbearer_config /* string - passed from config */) {
    console.log("Called token_refresh with given config: " + oauthbearer_config);
    // At this point, we can use the information in the token, make
    // some API calls, fetch something from a file...
    // For the illustration, everything is hard-coded.
    const principal = 'admin';
    // In seconds - needed by jsonwebtoken library
    const exp_seconds = Math.floor(Date.now() / 1000) + (60 * 60);
    // In milliseconds - needed by kafka-javascript.
    const exp_ms = exp_seconds * 1000;

    // For illustration, we're not signing our JWT (algorithm: none).
    // For production uses-cases, it should be signed.
    const value = jwt.sign(
        { 'sub': principal, exp: exp_seconds, 'scope': 'requiredScope' }, '', { algorithm: 'none' });

    // SASL extensions can be passed as Map or key/value pairs in an object.
    const extensions = {
        traceId: '123'
    };

    // The callback is called with the new token, its lifetime, and the principal.
    // The extensions are optional and may be omitted.
    console.log("Finished token_refresh, triggering callback: with value: " +
        value.slice(0, 10) + "..., lifetime: " + exp_ms +
        ", principal: " + principal + ", extensions: " + JSON.stringify(extensions));

    // If no token could be fetched or an error occurred, an Error can be thrown instead.
    return { value, lifetime: exp_ms, principal, extensions };
}

async function run() {
    const kafka = new Kafka({});
    const producer = kafka.producer({
        kafkaJS: {
            brokers: ['localhost:46611'],
            sasl: {
                mechanism: 'oauthbearer',
                oauthBearerProvider: token_refresh,
            },
        },
        'sasl.oauthbearer.config': 'someConfigPropertiesKey=value',
    });

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