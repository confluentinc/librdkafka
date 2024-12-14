const {
    partitionKey,
} = require('./_common');
const { LinkedList } = require('./_linked-list');

/**
 * A PerPartitionMessageCache is a cache for messages for a single partition.
 * @private
 */
class PerPartitionMessageCache {
    /* The cache is a list of messages. */
    #cache = new LinkedList();
    /* The key for the partition. */
    #key = null;
    /* Whether the cache is assigned to a consumer. */
    _assigned = false;

    constructor(key) {
        this.#key = key;
    }

    /**
     * Returns the number of total elements in the cache.
     */
    size() {
        return this.#cache.length;
    }

    /**
     * Adds a message to the cache.
     */
    _add(message) {
        this.#cache.addLast(message);
    }

    get key() {
        return this.#key;
    }

    /**
     * @returns The next element in the cache or null if none exists.
     */
    _next() {
        return this.#cache.removeFirst();
    }

    /**
     * @returns Upto `n` next elements in the cache or an empty array if none exists.
     */
    _nextN(n) {
        const len = this.#cache.length;
        n = (n < 0 || len < n) ? len : n;

        const ret = new Array(n);
        for (let i = 0; i < n; i++) {
            ret[i] = this.#cache.removeFirst();
        }
        return ret;
    }
}


/**
 * MessageCache defines a dynamically sized cache for messages.
 * Internally, it uses PerPartitionMessageCache to store messages for each partition.
 * @private
 */
class MessageCache {
    #size;
    /* Map of topic+partition to PerPartitionMessageCache. */
    #tpToPpc;
    /* LinkedList of available partitions. */
    #availablePartitions;
    /* LinkedList of assigned partitions. */
    #assignedPartitions;


    constructor(logger) {
        this.logger = logger ?? console;
        this.#reinit();
    }

    /**
     * Reinitializes the cache.
     */
    #reinit() {
      this.#tpToPpc = new Map();
      this.#availablePartitions = new LinkedList();
      this.#assignedPartitions = new LinkedList();
      this.#size = 0;
    }

    /**
     * Assign a new partition to the consumer, if available.
     *
     * @returns {PerPartitionMessageCache} - the partition assigned to the consumer, or null if none available.
     */
    #assignNewPartition() {
        let ppc = this.#availablePartitions.removeFirst();
        if (!ppc)
            return null;

        ppc._node = this.#assignedPartitions.addLast(ppc);
        ppc._assigned = true;
        return ppc;
    }

    /**
     * Remove an empty partition from the cache.
     *
     * @param {PerPartitionMessageCache} ppc The partition to remove from the cache.
     */
    #removeEmptyPartition(ppc) {
        this.#assignedPartitions.remove(ppc._node);
        ppc._assigned = false;
        ppc._node = null;
        this.#tpToPpc.delete(ppc.key);
    }

    /**
     * Add a single message to a PPC.
     * In case the PPC does not exist, it is created.
     *
     * @param {Object} message - the message to add to the cache.
     */
    #add(message) {
        const key = partitionKey(message);
        let cache = this.#tpToPpc.get(key);
        if (!cache) {
            cache = new PerPartitionMessageCache(key);
            this.#tpToPpc.set(key, cache);
            cache._node = this.#availablePartitions.addLast(cache);
        }
        cache._add(message);
    }

    get availableSize() {
        return this.#availablePartitions.length;
    }

    get assignedSize() {
        return this.#assignedPartitions.length;
    }

    get size() {
        return this.#size;
    }

    /**
     * Mark a set of topic partitions 'stale'.
     *
     * Post-conditions: PPCs are removed from their currently assigned list
     * and deleted from the PPC map. Cache size is decremented accordingly.
     * PPCs are marked as not assigned.
     */
    markStale(topicPartitions) {
        for (const topicPartition of topicPartitions) {
            const key = partitionKey(topicPartition);
            const ppc = this.#tpToPpc.get(key);
            if (!ppc)
                continue;

            this.#size -= ppc.size();
            if (ppc._assigned) {
                this.#assignedPartitions.remove(ppc._node);
            } else {
                this.#availablePartitions.remove(ppc._node);
            }
            this.#tpToPpc.delete(key);
            ppc._assigned = false;
        }
    }

    /**
     * Adds many messages into the cache, partitioning them as per their toppar.
     * Increases cache size by the number of messages added.
     *
     * @param {Array} messages - the messages to add to the cache.
     */
    addMessages(messages) {
        for (const message of messages)
            this.#add(message);
        this.#size += messages.length;
    }

    /**
     * Allows returning the PPC without asking for another message.
     *
     * @param {PerPartitionMessageCache} ppc - the partition to return.
     *
     * @note this is a no-op if the PPC is not assigned.
     */
    return(ppc) {
        if (!ppc._assigned)
            return;
        if (ppc._node) {
            this.#assignedPartitions.remove(ppc._node);
            ppc._node = this.#availablePartitions.addLast(ppc);
            ppc._assigned = false;
        }
    }

    /**
     * Returns the next element in the cache, or null if none exists.
     *
     * If the current PPC is exhausted, it moves to the next PPC.
     * If all PPCs are exhausted, it returns null.
     *
     * @param {PerPartitionMessageCache} ppc - after a consumer has consumed a message, it must return the PPC back to us via this parameter.
     *                       otherwise, no messages from that topic partition will be consumed.
     * @returns {Array} - the next message in the cache, or null if none exists, and the corresponding PPC.
     * @note Whenever making changes to this function, ensure that you benchmark perf.
     */
    next(ppc = null) {
        if (!ppc|| !ppc._assigned)
            ppc = this.#assignNewPartition();
        if (!ppc)
            return null;

        let next = ppc._next();

        if (!next) {
            this.#removeEmptyPartition(ppc);
            return this.next();
        }

        this.#size--;
        return [next, ppc];
    }

    /**
     * Returns the next `size` elements in the cache as an array, or null if none exists.
     *
     * @sa next, the behaviour is similar in other aspects.
     */
    nextN(ppc = null, size = -1) {
        if (!ppc || !ppc._assigned)
            ppc = this.#assignNewPartition();
        if (!ppc)
            return null;

        let nextN = ppc._nextN(size);

        if (size === -1 || nextN.length < size) {
            this.#removeEmptyPartition(ppc);
        }
        if (!nextN.length)
            return this.nextN(null, size);

        this.#size -= nextN.length;
        return [nextN, ppc];
    }

    /**
     * Clears the cache completely.
     * This resets it to a base state.
     */
    clear() {
        for (const ppc of this.#tpToPpc.values()) {
            ppc._assigned = false;
        }
        this.#reinit();
    }
}

module.exports = MessageCache;
