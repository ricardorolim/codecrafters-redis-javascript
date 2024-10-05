const server = require("./redis.js");

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

    let replicaofIndex = process.argv.indexOf("--replicaof");
    if (replicaofIndex > -1) {
        let master = process.argv[replicaofIndex + 1];
        [config.master_host, config.master_port] = master
            .split(" ")
            .filter((i) => i);
    }

    return config;
}

let config = parseArgs();
const redis = new server.Redis(config);
redis.listen(config);
