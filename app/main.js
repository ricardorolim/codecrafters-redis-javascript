const net = require("net");

const server = net.createServer((connection) => {
    console.log("connected");
    let response = "+PONG\r\n";
    connection.write(response);
    connection.end();
});

server.listen(6379, "127.0.0.1");
