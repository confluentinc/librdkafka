const RdKafka = require('../rdkafka');

const ProducerState = Object.freeze({
    INIT:   0,
    CONNECTING:  1,
    CONNECTED: 2,
    DISCONNECTING: 3,
    DISCONNECTED: 4,
});

class Producer {
    #config = {}
    #internalClient = null;
    #connectPromiseFunc = {};
    #state = ProducerState.INIT;

    constructor(config) {
        this.#config = config;
    }

    #readyCb(arg) {
        //console.log('Connected and ready.');
        if (this.#state !== ProducerState.CONNECTING) {
            // I really don't know how to handle this now.
            return;
        }
        this.#state = ProducerState.CONNECTED;

        // Start a loop to poll.
        let pollInterval = setInterval(() => {
            if (this.#state >= ProducerState.DISCONNECTING) {
                clearInterval(pollInterval);
                return;
            }
            this.#internalClient.poll();
        }, 500);

        this.#internalClient.on('delivery-report', function(err, report) {
            //console.log('got delivery report', report, err);
            const opaque = report.opaque;
            if (!opaque) {
                // not sure how to handle this.
                return;
            }
            if (err) {
                opaque.reject('err out');
                return;
            }
            //console.log('delivery-report: ' + JSON.stringify(report));
            delete report['opaque'];
            opaque.resolve(report);
        });

        // Resolve the promise.
        this.#connectPromiseFunc["resolve"]();
    }

    #errorCb(args) {
        console.log('error', args);
        if (this.#state === ProducerState.CONNECTING) {
            this.#connectPromiseFunc["reject"](args);
        } else {
            // do nothing for now.
        }
    }

    connect() {
        if (this.#state !== ProducerState.INIT) {
            return Promise.reject("Connect has already been called elsewhere.");
        }

        this.#state = ProducerState.CONNECTING;
        this.#internalClient = new RdKafka.Producer(this.#config);
        this.#internalClient.on('ready', this.#readyCb.bind(this));
        this.#internalClient.on('event.error', this.#errorCb.bind(this));
        this.#internalClient.on('event.log', console.log);

        this.#internalClient.on('disconnected', (arg) => {
            this.#state = ProducerState.DISCONNECTED;
            console.log('producer disconnected. ' + JSON.stringify(arg));
        });

        return new Promise((resolve, reject) => {
            this.#connectPromiseFunc = {resolve, reject};
            console.log("Connecting....");
            this.#internalClient.connect();
            console.log("connect() called");
        });
    }

    disconnect() {
      if (this.#state >= ProducerState.DISCONNECTING) {
        return;
      }
      this.#state = ProducerState.DISCONNECTING;
      this.#internalClient.disconnect();
    }

    // producer.send({
    //     topic: <String>,
    //     messages: <Message[]>,
    //     acks: <Number>,
    //     timeout: <Number>,
    //     compression: <CompressionTypes>,
    // })

    send(sendOptions) {
        if (this.#state !== ProducerState.CONNECTED) {
            return Promise.reject("Cannot send message without awaiting connect()");
        }

        if (sendOptions === null || !(sendOptions instanceof Object)) {
            return Promise.reject("sendOptions must be set correctly");
        }

        // Ignore all properties except topic and messages.
        // TODO: log a warning instead of ignoring.
        if (!sendOptions.hasOwnProperty("topic") || !sendOptions.hasOwnProperty("messages") || !Array.isArray(sendOptions["messages"])) {
            // TODO: add further validations.
            return Promise.reject("sendOptions must be of the form {topic: string, messages: Message[]}");
        }

        const msgPromises = [];
        for (let i = 0; i < sendOptions.messages.length; i++) {
            const msg = sendOptions.messages[i];

            if (!msg.hasOwnProperty("partition") || msg.partition === null) {
                msg.partition = -1;
            }

            if (typeof msg.value === 'string') {
                msg.value = Buffer.from(msg.value);
            }

            msgPromises.push(new Promise((resolve, reject) => {
                const opaque = {resolve, reject};
                this.#internalClient.produce(sendOptions.topic, msg.partition, msg.value, msg.key, msg.timestamp ?? Date.now(), opaque, msg.headers);
            }));

        }
        return Promise.all(msgPromises);
    }
}

class Kafka {
    #commonClientConfig = {};

    constructor(config) {
        this.#commonClientConfig = config;
    }

    producer(config) {
        if (config === null || !(config instanceof Object)) {
            config = {};
        }

        config = Object.assign(config, this.#commonClientConfig);
        return new Producer(config);
    }
}

module.exports = { Kafka }
