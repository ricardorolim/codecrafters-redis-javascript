const redis = require("./redis.js");

function toIter(string) {
    return string[Symbol.iterator]();
}

test("parse length", () => {
    let it = toIter("1234");
    const parser = new redis.Parser(it);
    expect(parser.parseLength()).resolves.toBe(1234);
});

test("parse length until delimiter", () => {
    let it = toIter("12\r\n34");
    const parser = new redis.Parser(it);
    expect(parser.parseLength()).resolves.toBe(12);
});

test("parse length rejects invalid format", () => {
    let it = toIter("12\r34");
    const parser = new redis.Parser(it);
    expect(parser.parseLength()).rejects.toThrow();
});

test("parse bulkString", () => {
    let it = toIter("5\r\nhello\r\n");
    const parser = new redis.Parser(it);
    expect(parser.parseBulkString()).resolves.toBe("hello");
});

test("parse bulkString rejects invalid format", () => {
    let it = toIter("5\r\nhello\r3");
    const parser = new redis.Parser(it);
    expect(parser.parseBulkString()).rejects.toThrow();
});

test("parse command", async () => {
    let it = toIter("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
    const parser = new redis.Parser(it);
    let command = await parser.parseCommand().next();
    expect(command.value).toStrictEqual(["ECHO", "hey"]);
});
