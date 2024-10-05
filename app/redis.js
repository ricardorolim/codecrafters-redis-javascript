const enc = require("./encoder.js");

const fs = require("fs");
const net = require("net");
const path = require("path");
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

async function* asyncStreamByteIterator(stream) {
    for await (const chunk of stream) {
        for (const byte of chunk) {
            yield String.fromCharCode(byte);
        }
    }
}

class Redis {
    constructor(config) {
        this.config = config;
        this.db = null;
        this.info = new Info();
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
            let it = asyncStreamByteIterator(socket);
            const cmdParser = new resp_parser.RespParser(it);

            try {
                for await (const command of cmdParser.parseCommand()) {
                    let response = this.process(command);
                    socket.write(response);
                }
            } catch (err) {
                console.error(err);
            }
        });

        server.listen(this.config.port, "127.0.0.1");
    }

    masterHandshake() {
        console.log({
            host: this.config.master_host,
            port: this.config.master_port,
        });
        const client = net.createConnection(
            { host: this.config.master_host, port: this.config.master_port },
            async () => {
                let ping = new enc.RedisBulkString("PING");
                let resp = new enc.RedisArray([ping]);
                client.write(resp.encode());
            },
        );
    }

    process(command) {
        let key = null;
        let value = null;
        let resp = null;

        switch (command[0]) {
            case "ECHO":
                resp = new enc.RedisBulkString(command[1]);
                return resp.encode();
            case "SET":
                key = command[1];
                value = command[2];
                let expiration = Infinity;

                if (command[3]?.toLowerCase() == "px" && command.length > 4) {
                    expiration = parseInt(command[4]);
                }

                this.db.set(key, value, expiration);
                resp = new enc.RedisSimpleString("OK");
                return resp.encode();
            case "GET":
                key = command[1];
                value = this.db.get(key);
                resp =
                    value !== undefined
                        ? new enc.RedisBulkString(value)
                        : new enc.RedisNullBulkString();
                return resp.encode();
            case "PING":
                resp = new enc.RedisSimpleString("PONG");
                return resp.encode();
            case "CONFIG":
                if (command[1] == "GET") {
                    let key = command[2];
                    let keyBulkString = new enc.RedisBulkString(key);
                    let valueBulkString = new enc.RedisBulkString(
                        this.config[key],
                    );
                    let array = new enc.RedisArray([
                        keyBulkString,
                        valueBulkString,
                    ]);
                    return array.encode();
                }
            case "KEYS":
                if (command[1] == "*") {
                    let keys = this.db
                        .keys()
                        .map((key) => new enc.RedisBulkString(key));
                    let array = new enc.RedisArray(keys);
                    return array.encode();
                }
                break;
            case "INFO":
                if (command[1].toLowerCase() == "replication") {
                    let role = this.info.toString();
                    let info = new enc.RedisBulkString(role);
                    return info.encode();
                }
                break;
            default:
                console.error("unknown command:", command[0]);
        }
    }
}

module.exports = { process, Config, Redis };
