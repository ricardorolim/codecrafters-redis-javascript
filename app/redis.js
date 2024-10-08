const enc = require("./encoder.js");

const fs = require("fs");
const net = require("net");
const path = require("path");
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

class Redis {
    constructor(config) {
        this.config = config;
        this.db = null;
        this.info = new Info();
        this.slaves = [];
    }

    async listen() {
        let db = {};

        if (this.config.rdbFileExists()) {
            let rdbFile = fs.createReadStream(this.config.dbpath());
            let parser = new rdb.RDBParser(rdbFile);
            db = await parser.parse();
        }

        this.db = new rdb.RedisDB(db);

        if (this.config.masterSet()) {
            this.info.role = Role.SLAVE;
            this.masterHandshake();
        }

        const server = net.createServer(async (socket) => {
            let it = parser.streamToIterator(socket);
            const cmdParser = new resp_parser.RespParser(it);

            try {
                for await (const command of cmdParser.parseCommand()) {
                    this.process(command, socket);
                }
            } catch (err) {
                console.error(err);
            }
        });

        server.listen(this.config.port, "127.0.0.1");
    }

    isMaster() {
        return this.info.role == Role.MASTER;
    }

    masterHandshake() {
        const master = net.createConnection(
            { host: this.config.master_host, port: this.config.master_port },
            async () => {
                let it = parser.streamToIterator(master);

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

                try {
                    for await (const command of cmdParser.parseCommand()) {
                        this.process(command, master);
                    }
                } catch (err) {
                    console.error(err);
                }
            },
        );
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
        for (const socket of this.slaves) {
            let strings = [];

            for (let str of command) {
                strings.push(new enc.RedisBulkString(str));
            }

            const array = new enc.RedisArray(strings);
            socket.write(array.encode());
        }
    }

    process(command, socket) {
        let key = null;
        let value = null;
        let resp = null;

        switch (command[0]) {
            case "ECHO":
                resp = new enc.RedisBulkString(command[1]);
                socket.write(resp.encode());
                break;
            case "SET":
                key = command[1];
                value = command[2];
                let expiration = Infinity;

                if (command[3]?.toLowerCase() == "px" && command.length > 4) {
                    expiration = parseInt(command[4]);
                }

                this.db.set(key, value, expiration);

                if (this.isMaster()) {
                    resp = new enc.RedisSimpleString("OK");
                    socket.write(resp.encode());

                    this.sendToSlaves(command);
                }

                break;
            case "GET":
                key = command[1];
                value = this.db.get(key);
                resp =
                    value !== undefined
                        ? new enc.RedisBulkString(value)
                        : new enc.RedisNullBulkString();
                socket.write(resp.encode());
                break;
            case "PING":
                resp = new enc.RedisSimpleString("PONG");
                socket.write(resp.encode());
                break;
            case "CONFIG":
                if (command[1] == "GET") {
                    let key = command[2];
                    let keyBulkString = new enc.RedisBulkString(key);
                    let valueBulkString = new enc.RedisBulkString(
                        this.config[key],
                    );
                    resp = new enc.RedisArray([keyBulkString, valueBulkString]);
                    socket.write(resp.encode());
                }
                break;
            case "KEYS":
                if (command[1] == "*") {
                    let keys = this.db
                        .keys()
                        .map((key) => new enc.RedisBulkString(key));
                    resp = new enc.RedisArray(keys);
                    socket.write(resp.encode());
                }
                break;
            case "INFO":
                if (command[1].toLowerCase() == "replication") {
                    let role = this.info.toString();
                    resp = new enc.RedisBulkString(role);
                    socket.write(resp.encode());
                }
                break;
            case "REPLCONF":
                resp = new enc.RedisSimpleString("OK");
                socket.write(resp.encode());
                break;
            case "PSYNC":
                resp = new enc.RedisSimpleString(
                    `FULLRESYNC ${this.info.master_replid} 0`,
                );
                socket.write(resp.encode());

                // the length has to be sent separately because of a bug in how the CodeCrafter
                // tester is implemented
                let db = rdb.encodeEmptyRDB();
                socket.write(`$${db.length}\r\n`);
                socket.write(db);

                this.slaves.push(socket);
                break;
            default:
                console.error("unknown command:", command[0]);
        }
    }
}

module.exports = { process, Config, Redis };
