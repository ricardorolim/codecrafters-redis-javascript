const parser = require("./parser.js");

class RedisDB {
    constructor(db = {}) {
        this.redis = db;
    }

    get(key) {
        if (!(key in this.redis)) {
            return;
        }

        let [value, expiration] = this.redis[key];
        if (expiration === Infinity || expiration > Date.now()) {
            return value;
        }
    }

    set(key, value, expiration = Infinity) {
        if (expiration !== Infinity) {
            expiration = Date.now() + expiration;
        }
        this.redis[key] = [value, expiration];
    }

    in(key) {
        return key in this.redis;
    }

    keys() {
        return Object.keys(this.redis);
    }

    toString() {
        return this.redis.toString();
    }
}

class EntryId {
    constructor(timestamp, seqno) {
        this.timestamp = timestamp;
        this.seqno = seqno;
    }

    toString() {
        return `${this.timestamp}-${this.seqno}`;
    }
}

class RedisStream {
    constructor() {
        this.entries = new Map();
        this.lastEntryId = new EntryId(0, 0);
    }

    add(entryIdStr, key, value) {
        this.validate(entryIdStr);

        let entryId =
            entryIdStr.slice(-1) == "*"
                ? this.autogenerateId(entryIdStr)
                : this.parseEntryId(entryIdStr);

        if (!(entryId in this.entries)) {
            this.entries.set(entryId, new Map());
        }

        this.entries.get(entryId).set(key, value);
        this.lastEntryId = entryId;

        return entryId.toString();
    }

    search(startIdStr, stopIdStr) {
        let startId = this.parseEntryId(startIdStr);
        let stopId = this.parseEntryId(stopIdStr);

        return [...this.entries.entries()]
            .filter(([e, _]) => {
                if (
                    startId.timestamp < e.timestamp &&
                    e.timestamp < stopId.timestamp
                ) {
                    return true;
                }
                if (
                    startId.timestamp == e.timestamp &&
                    e.seqno < startId.seqno
                ) {
                    return false;
                }
                if (stopId.timestamp == e.timestamp && e.seqno > stopId.seqno) {
                    return false;
                }

                return true;
            })
            .map(([e, kvpairs]) => [
                e.toString(),
                [...kvpairs.entries()].flat(),
            ]);
    }

    validate(entryIdStr) {
        let [currTimestamp, currSeqno] = entryIdStr.split("-");
        if (entryIdStr == "*" || currSeqno == "*") {
            return;
        }

        if (currTimestamp == 0 && currSeqno == 0) {
            throw new Error(
                "ERR The ID specified in XADD must be greater than 0-0",
            );
        }

        let err = new Error(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        );

        if (
            currTimestamp == this.lastEntryId.timestamp &&
            currSeqno == this.lastEntryId.seqno
        ) {
            throw err;
        }

        if (
            currTimestamp < this.lastEntryId.timestamp ||
            (currTimestamp == this.lastEntryId.timestamp &&
                currSeqno <= this.lastEntryId.seqno)
        ) {
            throw err;
        }
    }

    autogenerateId(entryIdStr) {
        let timestamp =
            entryIdStr == "*" ? Date.now() : entryIdStr.split("-")[0];
        let seqno =
            timestamp == this.lastEntryId.timestamp
                ? this.lastEntryId.seqno + 1
                : 0;

        return new EntryId(timestamp, seqno);
    }

    parseEntryId(string) {
        let parts = string.split("-");
        return new EntryId(parseInt(parts[0]), parseInt(parts[1]));
    }
}

class RDBParser {
    constructor(stream) {
        this.index = 0;
        let it = parser.streamToIterator(stream);
        this.it = new parser.PeekableIterator(it);
    }

    async parse() {
        await this.validateHeader();
        await this.skipMetadata();

        let db = await this.parseDatabase();
        await this.skipEOF();
        return db;
    }

    async validateHeader() {
        let magic = await this.read(5);
        if (magic.toString() !== "REDIS") {
            throw new Error("invalid RDB file");
        }

        // skip version number
        for (let i = 0; i < 4; i++) {
            await this.next();
        }
    }

    async skipMetadata() {
        for (;;) {
            let byte = (await this.it.peek()).value;
            if (byte != 0xfa) {
                break;
            }
            this.it.next();

            // skip name and value
            await this.parseString();
            await this.parseString();
        }
    }

    async skipEOF() {
        await this.expect(0xff);

        // skip CRC
        for (let i = 0; i < 8; i++) {
            await this.next();
        }
    }

    async parseDatabase() {
        await this.expect(0xfe); // start of database section
        await this.expect(0x00); // database index (assuming there's only one database)

        await this.expect(0xfb); // hashtable size information
        let s1 = await this.next(); // hashtable entries without expiration
        let s2 = await this.next(); // hashtable entries with expiration

        let db = {};
        for (let i = 0; i < Math.max(s1, s2); i++) {
            let entry = await this.parseHashTableEntry();
            db = { ...entry, ...db };
        }

        return db;
    }

    async expect(byte) {
        let curr = await this.next();
        if (curr !== byte) {
            throw new Error(
                `unexpected byte: ${curr.toString(16)} != ${byte.toString(16)}`,
            );
        }
    }

    async parseHashTableEntry() {
        let key = null;
        let value = null;

        switch (await this.next()) {
            case 0x00: //string without expiration
                key = await this.parseString();
                value = await this.parseString();
                return { [key]: [value, Infinity] };
            case 0xfc: //expiration in msec
                let expMillis = await this.read(8);
                expMillis = expMillis.readBigUInt64LE();
                await this.validateEntryTypeString();
                key = await this.parseString();
                value = await this.parseString();
                return { [key]: [value, Number(expMillis)] };
            case 0xfd: //expiration is sec
                let expSec = await this.read(4);
                expSec = expSec.readUInt32LE();
                await this.validateEntryTypeString();
                key = await this.parseString();
                value = await this.parseString();
                return { [key]: [value, expSec] };
        }
    }

    async validateEntryTypeString() {
        let type = await this.next();
        if (type !== 0) {
            throw new Error("value type is not string");
        }
    }

    async read(size) {
        let array = [];
        for (let i = 0; i < size; i++) {
            array.push(await this.next());
        }

        return Buffer.from(array);
    }

    async next() {
        let p = await this.it.next();
        return p.value;
    }

    async parseString() {
        let size = await this.next();
        let strlen = 0;
        let strbuf = null;

        switch (size >> 6) {
            case 0:
                strlen = size & 0b00111111;
                strbuf = await this.read(strlen);
                return strbuf.toString();
            case 1:
                strlen = (size & 0b00111111) << 8;
                strlen += await this.next();
                strbuf = await this.read(strlen);
                return strbuf.toString();
            case 2:
                // read 4 bytes big-endian
                strlen = await this.read(4);
                strbuf = await this.read(strlen.readUInt32BE());
                return strbuf.toString();
            case 3:
                let num = 0;

                switch (size) {
                    case 0xc0:
                        num = await this.next();
                        return num.toString();
                    case 0xc1:
                        num = await this.read(2);
                        return num.readUInt16LE().toString();
                    case 0xc2:
                        num = await this.read(4);
                        return num.readUInt32LE().toString();
                    case 0xc3:
                        throw new Error(
                            "lzf-compressed parsing not implemented",
                        );
                }
        }
    }
}

function encodeEmptyRDB() {
    return Buffer.from(
        "524544495330303131fa0972656469732d76657205372e322e30fa" +
            "0a72656469732d62697473c040fa056374696d65c26d08bc65fa08" +
            "757365642d6d656dc2b0c41000fa08616f662d62617365c000fff0" +
            "6e3bfec0ff5aa2",
        "hex",
    );
}

module.exports = { RedisDB, RDBParser, RedisStream, encodeEmptyRDB };
