const rdb = require("./rdb.js");
const { Readable } = require("stream");

test("decode rdb string that starts with 0b00", async () => {
    let buffer = Buffer.from("0D48656C6C6F2C20576F726C6421", "hex");
    let parser = new rdb.RDBParser(Readable.from(buffer));
    let string = await parser.parseString();
    expect(string).toBe("Hello, World!");
});

test("decode rdb string that starts with 0b01", async () => {
    let buffer = Buffer.from("42BC" + "41".repeat(700), "hex");
    let parser = new rdb.RDBParser(Readable.from(buffer));
    let string = await parser.parseString();
    expect(string).toBe("A".repeat(700));
});

test("decode rdb string that starts with 0b10", async () => {
    let buffer = Buffer.from("8000004268" + "41".repeat(17000), "hex");
    let parser = new rdb.RDBParser(Readable.from(buffer));
    let string = await parser.parseString();
    expect(string).toBe("A".repeat(17000));
});

test("decode 8-bit integer", async () => {
    let buffer = Buffer.from("C07B", "hex");
    let parser = new rdb.RDBParser(Readable.from(buffer));
    let string = await parser.parseString();
    expect(string).toBe("123");
});

test("decode 16-bit integer", async () => {
    let buffer = Buffer.from("C13930", "hex");
    let parser = new rdb.RDBParser(Readable.from(buffer));
    let string = await parser.parseString();
    expect(string).toBe("12345");
});

test("decode 32-bit integer", async () => {
    let buffer = Buffer.from("C287D61200", "hex");
    let parser = new rdb.RDBParser(Readable.from(buffer));
    let string = await parser.parseString();
    expect(string).toBe("1234567");
});

test("decode hashtable entry", async () => {
    let buffer = Buffer.from("0006666F6F6261720662617A717578", "hex");
    let parser = new rdb.RDBParser(Readable.from(buffer));
    let string = await parser.parseHashTableEntry();
    expect(string).toStrictEqual({ foobar: ["bazqux", Infinity] });
});

test("decode hashtable entry", async () => {
    let buffer = Buffer.from("FC1572E7078F0100000003666F6F03626172", "hex");
    let parser = new rdb.RDBParser(Readable.from(buffer));
    let string = await parser.parseHashTableEntry();
    expect(string).toStrictEqual({ foo: ["bar", 1713824559637] });
});

test("decode hashtable entry", async () => {
    let buffer = Buffer.from("FD52ED2A66000362617A03717578", "hex");
    let parser = new rdb.RDBParser(Readable.from(buffer));
    let string = await parser.parseHashTableEntry();
    expect(string).toStrictEqual({ baz: ["qux", 1714089298] });
});

test("decode database section", async () => {
    let buffer = Buffer.from(
        "FE00FB03020006666F6F6261720662617A717578FC1572E7078F0100000003666F6F03626172FD52ED2A66000362617A03717578",
        "hex",
    );
    let parser = new rdb.RDBParser(Readable.from(buffer));
    let string = await parser.parseDatabase();
    expect(string).toStrictEqual({
        foobar: ["bazqux", Infinity],
        foo: ["bar", 1713824559637],
        baz: ["qux", 1714089298],
    });
});

test("decode rdb", async () => {
    let buffer = Buffer.from(
        (
            "52 45 44 49 53 30 30 31 31" + // header
            "FA" + // metadata section
            "09 72 65 64 69 73 2D 76 65 72" +
            "06 36 2E 30 2E 31 36" +
            "FE00FB03020006666F6F6261720662617A717578FC1572E7078F0100000003666F6F03626172FD52ED2A66000362617A03717578" +
            "FF" + // EOF section with CRC
            "89 3b b7 4e f8 0f 77 19"
        ).replace(/ /g, ""),
        "hex",
    );
    let parser = new rdb.RDBParser(Readable.from(buffer));
    let string = await parser.parse();
    expect(string).toStrictEqual({
        foobar: ["bazqux", Infinity],
        foo: ["bar", 1713824559637],
        baz: ["qux", 1714089298],
    });
});

test("stream search", async () => {
    let stream = new rdb.RedisStream();
    for (let i = 0; i < 3; i++) {
        for (let j = 0; j < 3; j++) {
            if (i == 0 && j == 0) continue;
            stream.add(`${i}-${j}`, `k${i}${j}`, `v${i}${j}`);
        }
    }

    let entries = stream.search("0-2", "2-0");

    expect(entries).toStrictEqual([
        ["0-2", ["k02", "v02"]],
        ["1-0", ["k10", "v10"]],
        ["1-1", ["k11", "v11"]],
        ["1-2", ["k12", "v12"]],
        ["2-0", ["k20", "v20"]],
    ]);
});
