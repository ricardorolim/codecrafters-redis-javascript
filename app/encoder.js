class RedisArray {
    constructor(items) {
        this.items = items;
    }

    encode() {
        let res = "*";
        res += this.items.length;
        res += "\r\n";
        for (const item of this.items) {
            res += item.encode();
        }

        return res;
    }
}

function splitToRedisArray(string) {
    let array = [];
    string.split(" ").forEach((s) => {
        array.push(new RedisBulkString(s));
    });

    return new RedisArray(array);
}

class RedisBulkString {
    constructor(string) {
        this.string = string;
    }

    encode() {
        let res = "";
        res += "$";
        res += this.string.length;
        res += "\r\n";
        res += this.string;
        res += "\r\n";
        return res;
    }
}

class RedisSimpleString {
    constructor(string) {
        this.string = string;
    }

    encode() {
        return `+${this.string}\r\n`;
    }
}

class RedisNullBulkString {
    encode() {
        return "$-1\r\n";
    }
}

class RedisInteger {
    constructor(int) {
        this.int = int;
    }

    encode() {
        return `:${this.int}\r\n`;
    }
}

class RedisSimpleError {
    constructor(string) {
        this.string = string;
    }

    encode() {
        return `-${this.string}\r\n`;
    }
}

module.exports = {
    RedisArray,
    RedisBulkString,
    RedisNullBulkString,
    RedisSimpleString,
    RedisInteger,
    RedisSimpleError,
    splitToRedisArray,
};
