const net = require("net");

const server = net.createServer((connection) => {
    connection.on("data", () => {
        let response = "+PONG\r\n";
        connection.write(response);
    });
});

server.listen(6379, "127.0.0.1");
