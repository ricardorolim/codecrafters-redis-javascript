const net = require("net");
const parser = require("./parser.js");
const redis = require("./redis.js");

async function* asyncStreamByteIterator(stream) {
    for await (const chunk of stream) {
        for (const byte of chunk) {
            yield String.fromCharCode(byte);
        }
    }
}

let redisDB = new redis.RedisDB();

function listen(config) {
    const server = net.createServer(async (socket) => {
        let it = asyncStreamByteIterator(socket);
        const cmdParser = new parser.Parser(it);
        for await (const command of cmdParser.parseCommand()) {
            let response = redis.process(command, redisDB, config);
            socket.write(response);
        }
    });

    server.listen(6379, "127.0.0.1");
}

module.exports = { listen };
