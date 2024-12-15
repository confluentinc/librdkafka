const LibrdKafkaError = require('../error');

/**
 * KafkaJSError represents an error when using the promisified interface.
 * @memberof KafkaJS
 */
class KafkaJSError extends Error {
    /**
     * This constructor is meant to be used by the library. Please see the members for more information on
     * what can be accessed from an error object.
     *
     * @param {Error | string} error an Error or a string describing the error.
     * @param {object} properties a set of optional error properties.
     * @param {boolean} [properties.retriable=false] whether the error is retriable. Applies only to the transactional producer
     * @param {boolean} [properties.fatal=false] whether the error is fatal. Applies only to the transactional producer.
     * @param {boolean} [properties.abortable=false] whether the error is abortable. Applies only to the transactional producer.
     * @param {string} [properties.stack] the stack trace of the error.
     * @param {number} [properties.code=LibrdKafkaError.codes.ERR_UNKNOWN] the error code.
     */
    constructor(e, { retriable = false, fatal = false, abortable = false, stack = null, code = LibrdKafkaError.codes.ERR_UNKNOWN } = {}) {
        super(e, {});
        /**
         * Name of the error.
         * @type {string}
         */
        this.name = 'KafkaJSError';
        /**
         * Message detailing the error.
         * @type {string}
         */
        this.message = e.message || e;
        /**
         * Whether the error is retriable (for transactional producer).
         * @type {boolean}
         */
        this.retriable = retriable;
        /**
         * Whether the error is fatal (for transactional producer).
         * @type {boolean}
         */
        this.fatal = fatal;
        /**
         * Whether the error is abortable (for transactional producer).
         * @type {boolean}
         */
        this.abortable = abortable;
        /**
         * The error code from Librdkafka.
         *
         * This field should be checked (as opposed to the type of the error) to determine what sort of an error this is.
         * @see {@link RdKafka.LibrdKafkaError.codes} For a list of error codes that can be returned.
         * @type {number}
         */
        this.code = code;

        if (stack) {
            /**
             * The stack trace of the error.
             */
            this.stack = stack;
        } else {
            Error.captureStackTrace(this, this.constructor);
        }

        const errTypes = Object
            .keys(LibrdKafkaError.codes)
            .filter(k => LibrdKafkaError.codes[k] === this.code);

        if (errTypes.length !== 1) {
            this.type = LibrdKafkaError.codes.ERR_UNKNOWN;
        } else {
            this.type = errTypes[0];
        }
    }
}

/**
 * KafkaJSProtocolError represents an error that is caused when a Kafka Protocol RPC has an embedded error.
 * @extends KafkaJS.KafkaJSError
 * @memberof KafkaJS
 */
class KafkaJSProtocolError extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSProtocolError';
    }
}

/**
 * KafkaJSOffsetOutOfRange represents the error raised when fetching from an offset out of range.
 * @extends KafkaJS.KafkaJSProtocolError
 * @memberof KafkaJS
 */
class KafkaJSOffsetOutOfRange extends KafkaJSProtocolError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSOffsetOutOfRange';
    }
}

/**
 * KafkaJSConnectionError represents the error raised when a connection to a broker cannot be established or is broken unexpectedly.
 * @extends KafkaJS.KafkaJSError
 * @memberof KafkaJS
 */
class KafkaJSConnectionError extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSConnectionError';
    }
}

/**
 * KafkaJSRequestTimeoutError represents the error raised on a timeout for one request.
 * @extends KafkaJS.KafkaJSError
 * @memberof KafkaJS
 */
class KafkaJSRequestTimeoutError extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSRequestTimeoutError';
    }
}

/**
 * KafkaJSPartialMessageError represents the error raised when a response does not contain all expected information.
 * @extends KafkaJS.KafkaJSError
 * @memberof KafkaJS
 */
class KafkaJSPartialMessageError extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSPartialMessageError';
    }
}

/**
 * KafkaJSSASLAuthenticationError represents an error raised when authentication fails.
 * @extends KafkaJS.KafkaJSError
 * @memberof KafkaJS
 */
class KafkaJSSASLAuthenticationError extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSSASLAuthenticationError';
    }
}

/**
 * KafkaJSGroupCoordinatorNotFound represents an error raised when the group coordinator is not found.
 * @extends KafkaJS.KafkaJSError
 * @memberof KafkaJS
 */
class KafkaJSGroupCoordinatorNotFound extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSGroupCoordinatorNotFound';
    }
}

/**
 * KafkaJSNotImplemented represents an error raised when a feature is not implemented for this particular client.
 * @extends KafkaJS.KafkaJSError
 * @memberof KafkaJS
 */
class KafkaJSNotImplemented extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSNotImplemented';
    }
}

/**
 * KafkaJSTimeout represents an error raised when a timeout for an operation occurs (including retries).
 * @extends KafkaJS.KafkaJSError
 * @memberof KafkaJS
 */
class KafkaJSTimeout extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSTimeout';
    }
}
class KafkaJSLockTimeout extends KafkaJSTimeout {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSLockTimeout';
    }
}

class KafkaJSCreateTopicError extends KafkaJSProtocolError {
    constructor(e, topicName, properties) {
      super(e, properties);
      this.topic = topicName;
      this.name = 'KafkaJSCreateTopicError';
    }
}

class KafkaJSDeleteGroupsError extends KafkaJSError {
    constructor(e, groups) {
      super(e);
      this.groups = groups || [];
      this.name = 'KafkaJSDeleteGroupsError';
    }
}

class KafkaJSDeleteTopicRecordsError extends KafkaJSError {
    constructor({ partitions }) {
      /*
       * This error is retriable if all the errors were retriable
       */
      const retriable = partitions
        .filter(({ error }) => error !== null)
        .every(({ error }) => error.retriable === true);

      super('Error while deleting records', { retriable });
      this.name = 'KafkaJSDeleteTopicRecordsError';
      this.partitions = partitions;
    }
}

/**
 * KafkaJSAggregateError represents an error raised when multiple errors occur at once.
 * @extends Error
 * @memberof KafkaJS
 */
class KafkaJSAggregateError extends Error {
    constructor(message, errors) {
        super(message);
        /**
         * A list of errors that are part of the aggregate error.
         * @type {Array<KafkaJS.KafkaJSError>}
         */
        this.errors = errors;
        this.name = 'KafkaJSAggregateError';
    }
}

/**
 * KafkaJSNoBrokerAvailableError represents an error raised when no broker is available for the operation.
 * @extends KafkaJS.KafkaJSError
 * @memberof KafkaJS
 */
class KafkaJSNoBrokerAvailableError extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSNoBrokerAvailableError';
    }
}

/**
 * @function isRebalancing
 * @param {KafkaJS.KafkaJSError} e
 * @returns {boolean} Returns whether the error is a rebalancing error.
 * @memberof KafkaJS
 */
const isRebalancing = e =>
    e.type === 'REBALANCE_IN_PROGRESS' ||
    e.type === 'NOT_COORDINATOR_FOR_GROUP' ||
    e.type === 'ILLEGAL_GENERATION';

/**
 * @function isKafkaJSError
 * @param {any} e
 * @returns {boolean} Returns whether the error is a KafkaJSError.
 * @memberof KafkaJS
 */
const isKafkaJSError = e => e instanceof KafkaJSError;

module.exports = {
    KafkaJSError,
    KafkaJSPartialMessageError,
    KafkaJSProtocolError,
    KafkaJSConnectionError,
    KafkaJSRequestTimeoutError,
    KafkaJSSASLAuthenticationError,
    KafkaJSOffsetOutOfRange,
    KafkaJSGroupCoordinatorNotFound,
    KafkaJSNotImplemented,
    KafkaJSTimeout,
    KafkaJSLockTimeout,
    KafkaJSCreateTopicError,
    KafkaJSDeleteGroupsError,
    KafkaJSDeleteTopicRecordsError,
    KafkaJSAggregateError,
    KafkaJSNoBrokerAvailableError,
    isRebalancing,
    isKafkaJSError,
    ErrorCodes: LibrdKafkaError.codes,
};
