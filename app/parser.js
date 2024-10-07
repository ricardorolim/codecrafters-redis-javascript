class PeekableIterator {
    constructor(iterator) {
        this.iterator = iterator;
        this.buffered = undefined;
        this.initialized = false;
    }

    async next() {
        if (this.buffered !== undefined) {
            let token = this.buffered;
            this.buffered = undefined;
            return token;
        }

        return this.iterator.next();
    }

    async peek() {
        this.buffered = this.iterator.next();
        return this.buffered;
    }

    [Symbol.asyncIterator]() {
        return this;
    }
}

async function* streamToIterator(stream) {
    for await (const chunk of stream) {
        yield* chunk;
    }
}

module.exports = {
    PeekableIterator,
    streamToIterator,
};
