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

    switch (command[0]) {
        case "ECHO":
            return encodeBulkString(command[1]);
        case "SET":
            key = command[1];
            let value = command[2];
            db[key] = value;
            return encodeSimpleString("OK");
        case "GET":
            key = command[1];
            return key in db
                ? encodeBulkString(db[key])
                : encodeNullBulkString();
        case "PING":
            let r = encodeSimpleString("PONG");
            return r;
    }
}

module.exports = { process };
