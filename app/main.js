const server = require("./server.js");

function parseArgs() {
    let config = new server.Config();
    let dirIndex = process.argv.indexOf("--dir");

    if (dirIndex > -1) {
        config.dir = process.argv[dirIndex + 1];
    }

    let dbfilenameIndex = process.argv.indexOf("--dbfilename");
    if (dbfilenameIndex > -1) {
        config.dbfilename = process.argv[dbfilenameIndex + 1];
    }

    let portIndex = process.argv.indexOf("--port");
    if (portIndex > -1) {
        config.port = parseInt(process.argv[portIndex + 1]);
    }

    return config;
}

let config = parseArgs();
server.listen(config);
