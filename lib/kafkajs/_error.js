const LibrdKafkaError = require('../error');

/**
 * @typedef {Object} KafkaJSError represents an error when using the promisified interface.
 */
class KafkaJSError extends Error {
    /**
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
        this.name = 'KafkaJSError';
        this.message = e.message || e;
        this.retriable = retriable;
        this.fatal = fatal;
        this.abortable = abortable;
        this.code = code;

        if (stack) {
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
 * @typedef {Object} KafkaJSProtocolError represents an error that is caused when a Kafka Protocol RPC has an embedded error.
 */
class KafkaJSProtocolError extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSProtocolError';
    }
}

/**
 * @typedef {Object} KafkaJSOffsetOutOfRange represents the error raised when fetching from an offset out of range.
 */
class KafkaJSOffsetOutOfRange extends KafkaJSProtocolError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSOffsetOutOfRange';
    }
}

/**
 * @typedef {Object} KafkaJSConnectionError represents the error raised when a connection to a broker cannot be established or is broken unexpectedly.
 */
class KafkaJSConnectionError extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSConnectionError';
    }
}

/**
 * @typedef {Object} KafkaJSRequestTimeoutError represents the error raised on a timeout for one request.
 */
class KafkaJSRequestTimeoutError extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSRequestTimeoutError';
    }
}

/**
 * @typedef {Object} KafkaJSPartialMessageError represents the error raised when a response does not contain all expected information.
 */
class KafkaJSPartialMessageError extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSPartialMessageError';
    }
}

/**
 * @typedef {Object} KafkaJSSASLAuthenticationError represents an error raised when authentication fails.
 */
class KafkaJSSASLAuthenticationError extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSSASLAuthenticationError';
    }
}

/**
 * @typedef {Object} KafkaJSGroupCoordinatorNotFound represents an error raised when the group coordinator is not found.
 */
class KafkaJSGroupCoordinatorNotFound extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSGroupCoordinatorNotFound';
    }
}

/**
 * @typedef {Object} KafkaJSNotImplemented represents an error raised when a feature is not implemented for this particular client.
 */
class KafkaJSNotImplemented extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSNotImplemented';
    }
}

/**
 * @typedef {Object} KafkaJSTimeout represents an error raised when a timeout for an operation occurs (including retries).
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

/**
 * @typedef {Object} KafkaJSAggregateError represents an error raised when multiple errors occur at once.
 */
class KafkaJSAggregateError extends Error {
    constructor(message, errors) {
        super(message);
        this.errors = errors;
        this.name = 'KafkaJSAggregateError';
    }
}

/**
 * @typedef {Object} KafkaJSNoBrokerAvailableError represents an error raised when no broker is available for the operation.
 */
class KafkaJSNoBrokerAvailableError extends KafkaJSError {
    constructor() {
        super(...arguments);
        this.name = 'KafkaJSNoBrokerAvailableError';
    }
}

/**
 * @function isRebalancing
 * @param {KafkaJSError} e
 * @returns boolean representing whether the error is a rebalancing error.
 */
const isRebalancing = e =>
    e.type === 'REBALANCE_IN_PROGRESS' ||
    e.type === 'NOT_COORDINATOR_FOR_GROUP' ||
    e.type === 'ILLEGAL_GENERATION';

/**
 * @function isKafkaJSError
 * @param {any} e
 * @returns boolean representing whether the error is a KafkaJSError.
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
    KafkaJSAggregateError,
    KafkaJSNoBrokerAvailableError,
    isRebalancing,
    isKafkaJSError,
    ErrorCodes: LibrdKafkaError.codes,
};
