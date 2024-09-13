/**
 * Node class for linked list, after being removed
 * it cannot be used again.
 */
class LinkedListNode {
    // Value contained by the node.
    #value;
    // Node was removed from the list.
    _removed = false;
    // Next node in the list.
    _prev = null;
    // Previous node in the list.
    _next = null;

    constructor(value) {
        this.#value = value;
    }

    get value() {
        return this.#value;
    }

    get prev() {
        return this._prev;
    }

    get next() {
        return this._next;
    }
}

class LinkedList {
    _head = null;
    _tail = null;
    #count = 0;

    *#iterator() {
        let node = this._head;
        while (node) {
            yield node.value;
            node = node._next;
        }
    }

    #insertInBetween(node, prev, next) {
        node._next = next;
        node._prev = prev;
        if (prev)
            prev._next = node;
        else
            this._head = node;

        if (next)
            next._prev = node;
        else
            this._tail = node;

        this.#count++;
        return node;
    }

    /**
     * Removes given node from the list,
     * if it is not already removed.
     * 
     * @param {LinkedListNode} node 
     */
    remove(node) {
        if (node._removed) {
            return;
        }

        if (node._prev)
            node._prev._next = node._next;
        else
            this._head = node._next;

        if (node._next)
            node._next._prev = node._prev;
        else
            this._tail = node._prev;

        node._next = null;
        node._prev = null;
        node._removed = true;
        this.#count--;
    }

    /**
     * Removes the first node from the list and returns it,
     * or null if the list is empty.
     * 
     * @returns {any} The value of the first node in the list or null.
     */
    removeFirst() {
        if (this._head === null) {
            return null;
        }

        const node = this._head;
        this.remove(node);
        return node.value;
    }

    /**
     * Removes the last node from the list and returns its value,
     * or null if the list is empty.
     * 
     * @returns {any} The value of the last node in the list or null.
     */
    removeLast() {
        if (this._tail === null) {
            return null;
        }

        const node = this._tail;
        this.remove(node);
        return node.value;
    }

    /**
     * Add a new node to the beginning of the list and returns it.
     * 
     * @param {any} value 
     * @returns {LinkedListNode} The new node.
     */
    addFirst(value) {    
        const node = new LinkedListNode(value);
        return this.#insertInBetween(node, null,
            this._head);
    }

    /**
     * Add a new node to the end of the list and returns it.
     * 
     * @param {any} value Node value.
     * @returns {LinkedListNode} The new node.
     */
    addLast(value) {    
        const node = new LinkedListNode(value);
        return this.#insertInBetween(node, this._tail, null);
    }

    /**
     * Add a new node before the given node and returns it.
     * Given node must not be removed.
     * 
     * @param {LinkedListNode} node Reference node.
     * @param {any} value New node value.
     * @returns {LinkedListNode} The new node.
     */
    addBefore(node, value) {
        if (node._removed)
            throw new Error('Node was removed');
        const newNode = new LinkedListNode(value);
        return this.#insertInBetween(newNode, node._prev, node);
    }

    /**
     * Add a new node after the given node and returns it.
     * Given node must not be removed.
     * 
     * @param {LinkedListNode} node Reference node.
     * @param {any} value New node value.
     * @returns {LinkedListNode} The new node.
     */
    addAfter(node, value) {
        if (node._removed)
            throw new Error('Node was removed');
        const newNode = new LinkedListNode(value);
        return this.#insertInBetween(newNode, node, node._next);
    }

    /**
     * Concatenates the given list to the end of this list.
     * 
     * @param {LinkedList} list List to concatenate.
     */
    concat(list) {
        if (list.length === 0) {
            return;
        }

        if (this._tail) {
            this._tail._next = list._head;
        }

        if (list._head) {
            list._head._prev = this._tail;
        }

        this._tail = list._tail;
        this.#count += list.length;
        list.#count = 0;
        list._head = null;
        list._tail = null;
    }

    get first() {
        return this._head;
    }

    get last() {
        return this._tail;
    }

    get length() {
        return this.#count;
    }

    [Symbol.iterator]() {
        return this.#iterator();
    }
}

module.exports = {
    LinkedList,
    LinkedListNode
};
