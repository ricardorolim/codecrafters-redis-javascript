const redis = require("./redis.js");

test("set value without expiration", () => {
    const db = new redis.RedisDB();
    db.set("foo", "bar");
    expect(db.get("foo")).toBe("bar");
});

test("set value with expiration", () => {
    const db = new redis.RedisDB();
    db.set("foo", "bar", 100);
    expect(db.get("foo")).toBe("bar");
});

test("get expired value", () => {
    const db = new redis.RedisDB();
    db.set("foo", "bar", -1);
    expect(db.get("foo")).toBe(undefined);
});
