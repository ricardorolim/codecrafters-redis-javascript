const server = require("./server.js");

function parseArgs() {
    let config = {};
    let dirIndex = process.argv.indexOf("--dir");

    if (dirIndex > -1) {
        config.dir = process.argv[dirIndex + 1];
    }

    let dbfilenameIndex = process.argv.indexOf("--dbfilename");
    if (dbfilenameIndex > -1) {
        config.dbfilename = process.argv[dbfilenameIndex + 1];
    }

    return config;
}

let config = parseArgs();
server.listen(config);
