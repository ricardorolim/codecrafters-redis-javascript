class RedisDB {
    constructor() {
        this.redis = {};
    }

    get(key) {
        if (!(key in this.redis)) {
            return;
        }

        let [value, expiration] = this.redis[key];
        if (expiration === Infinity || expiration > Date.now()) {
            return value;
        }
    }

    set(key, value, expiration = Infinity) {
        if (expiration !== Infinity) {
            expiration = Date.now() + expiration;
        }
        this.redis[key] = [value, expiration];
    }
}

function encodeBulkString(string) {
    let res = [];
    res.push("$");
    res.push(string.length.toString());
    res.push("\r\n");
    res.push(string);
    res.push("\r\n");
    return res.join("");
}

function encodeSimpleString(string) {
    return `+${string}\r\n`;
}

function encodeNullBulkString() {
    return "$-1\r\n";
}

function process(command, db) {
    let key = null;
    let value = null;

    switch (command[0]) {
        case "ECHO":
            return encodeBulkString(command[1]);
        case "SET":
            key = command[1];
            value = command[2];
            let expiration = Infinity;

            if (command[3]?.toLowerCase() == "px" && command.length > 4) {
                expiration = parseInt(command[4]);
            }

            db.set(key, value, expiration);
            return encodeSimpleString("OK");
        case "GET":
            key = command[1];
            value = db.get(key);
            return value !== undefined
                ? encodeBulkString(value)
                : encodeNullBulkString();
        case "PING":
            let r = encodeSimpleString("PONG");
            return r;
    }
}

module.exports = { RedisDB, process };
