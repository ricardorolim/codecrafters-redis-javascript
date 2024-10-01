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

module.exports = {
    RedisArray,
    RedisBulkString,
    RedisNullBulkString,
    RedisSimpleString,
};
