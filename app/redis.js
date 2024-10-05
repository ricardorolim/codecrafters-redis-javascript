const encoder = require("./encoder.js");

function process(command, db, config) {
    let key = null;
    let value = null;
    let resp = null;

    switch (command[0]) {
        case "ECHO":
            resp = new encoder.RedisBulkString(command[1]);
            return resp.encode();
        case "SET":
            key = command[1];
            value = command[2];
            let expiration = Infinity;

            if (command[3]?.toLowerCase() == "px" && command.length > 4) {
                expiration = parseInt(command[4]);
            }

            db.set(key, value, expiration);
            resp = new encoder.RedisSimpleString("OK");
            return resp.encode();
        case "GET":
            key = command[1];
            value = db.get(key);
            resp =
                value !== undefined
                    ? new encoder.RedisBulkString(value)
                    : new encoder.RedisNullBulkString();
            return resp.encode();
        case "PING":
            resp = new encoder.RedisSimpleString("PONG");
            return resp.encode();
        case "CONFIG":
            if (command[1] == "GET") {
                let key = command[2];
                let keyBulkString = new encoder.RedisBulkString(key);
                let valueBulkString = new encoder.RedisBulkString(config[key]);
                let array = new encoder.RedisArray([
                    keyBulkString,
                    valueBulkString,
                ]);
                return array.encode();
            }
        case "KEYS":
            if (command[1] == "*") {
                let keys = db
                    .keys()
                    .map((key) => new encoder.RedisBulkString(key));
                let array = new encoder.RedisArray(keys);
                return array.encode();
            }
            break;
    }
}

module.exports = { process };
