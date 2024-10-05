const parser = require("./parser.js");

class RespParser {
    constructor(bytes) {
        this.bytes = new parser.PeekableIterator(bytes);
    }

    async *parseCommand() {
        for (;;) {
            let v = await this.bytes.peek();
            if (v.done) {
                return;
            }

            yield this.parseArray();
        }
    }

    async parseArray() {
        await this.expect("*");

        let length = await this.parseLength();

        let array = [];
        for (let i = 0; i < length; i++) {
            array.push(await this.parseElement());
        }

        return array;
    }

    async parseElement() {
        const v = await this.bytes.next();
        const header = v.value;

        switch (header) {
            case "*":
                return this.parseArray();
            case "$":
                return this.parseBulkString();
            default:
                return new Error(`invalid command header: ${header}`);
        }
    }

    async expect(byte) {
        const v = await this.bytes.next();
        let curr = v.value;

        if (curr != byte) {
            throw new Error(`unexpected value: '${curr}' != '${byte}'`);
        }
    }

    async parseBulkString() {
        let length = await this.parseLength();
        return this.parseString(length);
    }

    async parseLength() {
        let length = [];

        for (;;) {
            let byte = await this.bytes.next();
            if (byte.done) {
                break;
            }

            if (byte.value == "\r") {
                await this.expect("\n");
                break;
            }

            length.push(byte.value);
        }

        return parseInt(length.join(""));
    }

    async parseString(length) {
        let data = [];
        for (let i = 0; i < length; i++) {
            const v = await this.bytes.next();
            data.push(v.value);
        }

        await this.expect("\r");
        await this.expect("\n");
        return data.join("");
    }
}

module.exports = { RespParser };
