const Kafka = require('@confluentinc/kafka-javascript');
var jwt = require('jsonwebtoken');

// This example uses the Producer for demonstration purposes.
// It is the same whether you use a Consumer/AdminClient.

function token_refresh(oauthbearer_config /* string - passed from config */, cb) {
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
    const tokenValue = jwt.sign(
        { 'sub': principal, exp: exp_seconds, 'scope': 'requiredScope' }, '', { algorithm: 'none' });

    // SASL extensions can be passed as Map or key/value pairs in an object.
    const extensions = {
        traceId: '123'
    };

    // The callback is called with the new token, its lifetime, and the principal.
    // The extensions are optional and may be omitted.
    console.log("Finished token_refresh, triggering callback: with tokenValue: " +
        tokenValue.slice(0, 10) + "..., lifetime: " + exp_ms +
        ", principal: " + principal + ", extensions: " + JSON.stringify(extensions));
    cb(
        // If no token could be fetched or an error occurred, a new Error can be
        // and passed as the first parameter and the second parameter omitted.
        null,
        { tokenValue, lifetime: exp_ms, principal, extensions });
}

function run() {
    const producer = new Kafka.Producer({
        'metadata.broker.list': 'localhost:60125',
        'dr_cb': true,
        // 'debug': 'all'

        // Config important for OAUTHBEARER:
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanisms': 'OAUTHBEARER',
        'sasl.oauthbearer.config': 'someConfigPropertiesKey=value',
        'oauthbearer_token_refresh_cb': token_refresh,
    });

    producer.connect();

    producer.on('event.log', (event) => {
        console.log(event);
    });

    producer.on('ready', () => {
        console.log('Producer is ready!');
        producer.setPollInterval(1000);
        console.log("Producing message.");
        producer.produce(
            'topic',
            null, // partition - let partitioner choose
            Buffer.from('messageValue'),
            'messageKey',
        );
    });

    producer.on('error', (err) => {
        console.error("Encountered error in producer: " + err.message);
    });

    producer.on('delivery-report', function (err, report) {
        console.log('delivery-report: ' + JSON.stringify(report));
        // since we just want to produce one message, close shop.
        producer.disconnect();
    });
}

run();
