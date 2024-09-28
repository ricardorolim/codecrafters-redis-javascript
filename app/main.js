const net = require("net");
const redis = require("./redis");

async function* asyncStreamByteIterator(stream) {
    for await (const chunk of stream) {
        for (const byte of chunk) {
            yield String.fromCharCode(byte);
        }
    }
}

const server = net.createServer(async (socket) => {
    let it = asyncStreamByteIterator(socket);
    const parser = new redis.Parser(it);
    for await (const command of parser.parseCommand()) {
        let response = redis.process(command);
        socket.write(response);
    }
});

server.listen(6379, "127.0.0.1");
