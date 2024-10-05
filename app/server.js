const fs = require("fs");
const net = require("net");
const path = require("path");
const resp_parser = require("./resp_parser.js");
const redis = require("./redis.js");
const rdb = require("./rdb.js");

class Config {
    constructor() {
        this.dir = "";
        this.dbfilename = "";
    }

    dbpath() {
        return path.join(this.dir, this.dbfilename);
    }

    isValid() {
        if (this.dir === "" && this.dbfilename === "") {
            return false;
        }

        return fs.existsSync(this.dbpath());
    }
}

async function* asyncStreamByteIterator(stream) {
    for await (const chunk of stream) {
        for (const byte of chunk) {
            yield String.fromCharCode(byte);
        }
    }
}

async function listen(cfg) {
    let db = {};

    if (cfg.isValid()) {
        let rdbFile = fs.createReadStream(cfg.dbpath());
        let parser = new rdb.RDBParser(rdbFile);
        db = await parser.parse();
    }

    let redisDB = new rdb.RedisDB(db);

    const server = net.createServer(async (socket) => {
        let it = asyncStreamByteIterator(socket);
        const cmdParser = new resp_parser.RespParser(it);
        for await (const command of cmdParser.parseCommand()) {
            let response = redis.process(command, redisDB, cfg);
            socket.write(response);
        }
    });

    server.listen(6379, "127.0.0.1");
}

module.exports = { Config, listen };
