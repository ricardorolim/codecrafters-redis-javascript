const enc = require("./encoder.js");

const fs = require("fs");
const net = require("net");
const path = require("path");
const emitter = require("events");
const parser = require("./parser.js");
const resp_parser = require("./resp_parser.js");
const rdb = require("./rdb.js");

class Config {
    DEFAULT_PORT = 6379;

    constructor() {
        this.dir = "";
        this.dbfilename = "";
        this.port = this.DEFAULT_PORT;
        this.master_host = "";
        this.master_port = "";
    }

    dbpath() {
        return path.join(this.dir, this.dbfilename);
    }

    rdbFileExists() {
        if (this.dir === "" && this.dbfilename === "") {
            return false;
        }

        return fs.existsSync(this.dbpath());
    }

    masterSet() {
        return this.master_host !== "" && this.master_port !== "";
    }
}

const Role = Object.freeze({
    MASTER: "master",
    SLAVE: "slave",
});

class Info {
    role = Role.MASTER;
    master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    master_repl_offset = 0;

    toString() {
        let str = "";
        for (let key of Object.keys(this)) {
            str += `${key}:${this[key]}\n`;
        }

        return str;
    }
}

class Server {
    constructor(config) {
        this.config = config;
        this.redis = new Redis(config);
    }

    async listen() {
        let db = {};

        if (this.config.rdbFileExists()) {
            let rdbFile = fs.createReadStream(this.config.dbpath());
            let parser = new rdb.RDBParser(rdbFile);
            db = await parser.parse();
        }

        this.redis.setDB(new rdb.RedisDB(db));

        if (this.config.masterSet()) {
            this.redis.setRole(Role.SLAVE);
            const master = net.createConnection(
                {
                    host: this.config.master_host,
                    port: this.config.master_port,
                },
                () => this.redis.masterHandshake(master),
            );
        }

        const server = net.createServer(async (socket) => {
            let it = parser.streamToIterator(socket);
            const cmdParser = new resp_parser.RespParser(it);

            try {
                for await (const command of cmdParser.parseCommand()) {
                    this.redis.process(command, socket);
                }
            } catch (err) {
                console.error(err);
            }
        });

        server.listen(this.config.port, "127.0.0.1");
    }
}

class Redis {
    constructor(config) {
        this.config = config;
        this.db = new rdb.RedisDB();
        this.info = new Info();
        this.slaves = [];
        this.slaveReplOffset = 0;
        this.masterReplOffset = 0;
        this.ackEmitter = new emitter();
        this.xaddEmitter = new emitter();
    }

    setDB(db) {
        this.db = db;
    }

    setRole(role) {
        this.info.role = role;
    }

    isMaster() {
        return this.info.role == Role.MASTER;
    }

    async masterHandshake(master) {
        let offset = 0;

        let it = (async function* () {
            let it = parser.streamToIterator(master);
            for await (const byte of it) {
                offset += 1;
                yield byte;
            }
        })();

        // step 1
        let ping = new enc.RedisBulkString("PING");
        let req = new enc.RedisArray([ping]);
        master.write(req.encode());

        let exp = new enc.RedisSimpleString("PONG");
        if (await this.expect(exp.encode(), it)) {
            throw new Error("handshake error: expected PONG");
        }

        // step 2
        req = enc.splitToRedisArray(
            `REPLCONF listening-port ${this.config.port}`,
        );
        master.write(req.encode());

        exp = new enc.RedisSimpleString("OK");
        if (await this.expect(exp.encode(), it)) {
            throw new Error("handshake error: expected OK");
        }

        // step 3
        req = new enc.splitToRedisArray("REPLCONF capa psync2");
        master.write(req.encode());

        if (await this.expect(exp.encode(), it)) {
            throw new Error("handshake error: expected OK");
        }

        // step 4
        req = enc.splitToRedisArray("PSYNC ? -1");
        master.write(req.encode());

        const cmdParser = new resp_parser.RespParser(it);
        await cmdParser.parseFullResync();

        this.db = await this.receiveDB(it, cmdParser);

        offset = 0;

        try {
            for await (const command of cmdParser.parseCommand()) {
                this.process(command, master);
                this.slaveReplOffset = offset;
            }
        } catch (err) {
            console.error(err);
        }
    }

    async expect(string, iterator) {
        for (const c of string) {
            let v = await iterator.next();
            if (c != String.fromCharCode(v.value)) {
                return true;
            }
        }

        return false;
    }

    async receiveDB(iterator, cmdParser) {
        await this.expect("$", iterator);

        let length = await cmdParser.parseLength();
        let db = [];

        for (let i = 0; i < length; i++) {
            let byte = await iterator.next();
            db.push(byte.value);
        }

        return new rdb.RedisDB(db);
    }

    sendToSlaves(command) {
        let strings = [];
        for (let str of command) {
            strings.push(new enc.RedisBulkString(str));
        }

        const array = new enc.RedisArray(strings);
        const req = array.encode();
        this.masterReplOffset += req.length;

        for (const socket of this.slaves) {
            socket.write(req);
        }
    }

    async process(command, socket) {
        switch (command[0].toUpperCase()) {
            case "ECHO":
                this.process_echo(command, socket);
                break;
            case "SET":
                this.process_set(command, socket);
                break;
            case "GET":
                this.process_get(command, socket);
                break;
            case "PING":
                this.process_ping(socket);
                break;
            case "CONFIG":
                this.process_config(command, socket);
                break;
            case "KEYS":
                this.process_keys(command, socket);
                break;
            case "INFO":
                this.process_info(command, socket);
                break;
            case "REPLCONF":
                this.process_replconf(command, socket);
                break;
            case "PSYNC":
                this.process_psync(socket, socket);
                break;
            case "WAIT":
                this.process_wait(command, socket);
                break;
            case "TYPE":
                this.process_type(command, socket);
                break;
            case "XADD":
                this.process_xadd(command, socket);
                break;
            case "XRANGE":
                this.process_xrange(command, socket);
                break;
            case "XREAD":
                this.process_xread(command, socket);
                break;
            case "INCR":
                this.process_incr(command, socket);
                break;
            default:
                console.error("unknown command:", command[0]);
        }
    }

    process_echo(command, socket) {
        let resp = new enc.RedisBulkString(command[1]);
        socket.write(resp.encode());
    }

    process_set(command, socket) {
        let key = command[1];
        let value = command[2];
        let expiration = Infinity;

        if (command[3]?.toLowerCase() == "px" && command.length > 4) {
            expiration = parseInt(command[4]);
        }

        this.db.set(key, value, expiration);

        if (this.isMaster()) {
            let resp = new enc.RedisSimpleString("OK");
            socket.write(resp.encode());

            this.sendToSlaves(command);
        }
    }

    process_get(command, socket) {
        let key = command[1];
        let value = this.db.get(key);
        let resp =
            value !== undefined
                ? new enc.RedisBulkString(value)
                : new enc.RedisNullBulkString();
        socket.write(resp.encode());
    }

    process_ping(socket) {
        let resp = new enc.RedisSimpleString("PONG");
        if (this.isMaster()) {
            socket.write(resp.encode());
        }
    }

    process_config(command, socket) {
        if (command[1] == "GET") {
            let key = command[2];
            let keyBulkString = new enc.RedisBulkString(key);
            let valueBulkString = new enc.RedisBulkString(this.config[key]);
            let resp = new enc.RedisArray([keyBulkString, valueBulkString]);
            socket.write(resp.encode());
        }
    }

    process_keys(command, socket) {
        if (command[1] == "*") {
            let keys = this.db
                .keys()
                .map((key) => new enc.RedisBulkString(key));
            let resp = new enc.RedisArray(keys);
            socket.write(resp.encode());
        }
    }

    process_info(command, socket) {
        if (command[1].toLowerCase() == "replication") {
            let role = this.info.toString();
            let resp = new enc.RedisBulkString(role);
            socket.write(resp.encode());
        }
    }

    process_replconf(command, socket) {
        if (command[1] == "GETACK") {
            // request from master
            let offset = this.slaveReplOffset;
            let resp = new enc.RedisArray([
                new enc.RedisBulkString("REPLCONF"),
                new enc.RedisBulkString("ACK"),
                new enc.RedisBulkString(offset.toString()),
            ]);
            socket.write(resp.encode());
        } else if (command[1] == "ACK") {
            // response from slave
            let offset = parseInt(command[2]);
            this.ackEmitter.emit("slaveAck", offset);
        } else {
            let resp = new enc.RedisSimpleString("OK");
            socket.write(resp.encode());
        }
    }

    process_psync(_, socket) {
        let resp = new enc.RedisSimpleString(
            `FULLRESYNC ${this.info.master_replid} 0`,
        );
        socket.write(resp.encode());

        // the length has to be sent separately because of a bug in how the CodeCrafter
        // tester is implemented
        let db = rdb.encodeEmptyRDB();
        socket.write(`$${db.length}\r\n`);
        socket.write(db);

        this.slaves.push(socket);
    }

    async process_wait(command, socket) {
        let nreplicas = command[1];
        let waitMs = parseInt(command[2]);
        let count = 0;

        if (this.masterReplOffset == 0) {
            count = this.slaves.length;
        } else {
            let masterOffset = this.masterReplOffset;
            this.sendToSlaves("REPLCONF GETACK *".split(" "));

            let updated = new Promise((resolve) => {
                // whatever happens first fullfills the promise
                let timeout = setTimeout(resolve, waitMs);

                this.ackEmitter.on("slaveAck", (slaveOffset) => {
                    if (slaveOffset == masterOffset) {
                        count++;
                    }

                    if (count == nreplicas) {
                        clearTimeout(timeout);
                        resolve();
                    }
                });
            });

            await updated;
            this.ackEmitter.removeAllListeners();
        }

        let resp = new enc.RedisInteger(count);
        socket.write(resp.encode());
    }

    process_type(command, socket) {
        let key = command[1];

        let type = "none";
        if (this.db.has(key)) {
            type =
                this.db.get(key) instanceof rdb.RedisStream
                    ? "stream"
                    : "string";
        }

        let resp = new enc.RedisSimpleString(type);
        socket.write(resp.encode());
    }

    process_xadd(command, socket) {
        let key = command[1];
        let entryId = command[2];
        let entryKey = command[3];
        let entryValue = command[4];

        let stream = new rdb.RedisStream();
        if (this.db.has(key)) {
            stream = this.db.get(key);
        }

        let resp = null;

        try {
            entryId = stream.add(entryId, entryKey, entryValue);
            this.db.set(key, stream);
            resp = new enc.RedisBulkString(entryId);
            this.xaddEmitter.emit("xadd");
        } catch (err) {
            resp = new enc.RedisSimpleError(err.message);
        }

        socket.write(resp.encode());
    }

    process_xrange(command, socket) {
        let [key, start, end] = command.slice(1);

        if (!this.db.has(key)) {
            let resp = new enc.RedisArray([]);
            socket.write(resp);
            return;
        }

        let stream = this.db.get(key);
        const entries = stream.search(start, end);

        let array = entries.map(
            ([entryId, kvpairs]) =>
                new enc.RedisArray([
                    new enc.RedisBulkString(entryId),
                    new enc.RedisArray(
                        kvpairs.map((s) => new enc.RedisBulkString(s)),
                    ),
                ]),
        );

        let resp = new enc.RedisArray(array);
        socket.write(resp.encode());
    }

    process_xread(command, socket) {
        if (command[1].toUpperCase() == "BLOCK") {
            this.replace$WithCurrEntryId(command);

            let timeout = command[2];
            let timedOut = false;
            let timeoutId = null;

            if (timeout != 0) {
                timeoutId = setTimeout(() => {
                    timedOut = true;
                    let resp = new enc.RedisNullBulkString();
                    socket.write(resp.encode());
                }, timeout);
            }

            this.xaddEmitter.on("xadd", () => {
                if (!timedOut) {
                    if (timeoutId) {
                        clearTimeout(timeout);
                    }
                    // slice removes 2 additional arguments: block N
                    this.xread(command.slice(2), socket);
                }
            });
        } else {
            this.xread(command, socket);
        }
    }

    replace$WithCurrEntryId(command) {
        let firstKey = 4;
        let keys = (command.length - firstKey) / 2;

        for (let i = firstKey; i < firstKey + keys; i++) {
            let key = command[i];
            let startIdx = i + keys;

            if (command[startIdx] === "$" && this.db.has(key)) {
                let stream = this.db.get(key);
                command[startIdx] = stream.getLastEntryId();
            }
        }
    }

    xread(command, socket) {
        let streamArray = [];

        // xread streams key1 key2 ... start1 start2 ...
        command.splice(0, 2);

        for (let i = 0; i < command.length / 2; i++) {
            let key = command[i];
            let start = command[command.length / 2 + i];

            if (!this.db.has(key)) {
                continue;
            }

            let stream = this.db.get(key);
            const entries = stream.after(start);

            let entriesArray = entries.map(
                ([entryId, kvpairs]) =>
                    new enc.RedisArray([
                        new enc.RedisBulkString(entryId),
                        new enc.RedisArray(
                            kvpairs.map((s) => new enc.RedisBulkString(s)),
                        ),
                    ]),
            );

            streamArray.push(
                new enc.RedisArray([
                    new enc.RedisBulkString(key),
                    new enc.RedisArray(entriesArray),
                ]),
            );
        }

        let resp = new enc.RedisArray(streamArray);
        socket.write(resp.encode());
    }

    process_incr(command, socket) {
        let key = command[1];

        let val = this.db.has(key) ? this.db.get(key) : "0";
        if (isNaN(val)) {
            let resp = new enc.RedisSimpleError(
                "ERR value is not an integer or out of range",
            );
            socket.write(resp.encode());
            return;
        }

        val = (parseInt(val) + 1).toString();
        this.db.set(key, val);

        let resp = new enc.RedisInteger(val);
        socket.write(resp.encode());
    }
}

module.exports = { process, Config, Server };
