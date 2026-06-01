"use strict";
const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { cleanupKeys } = require("./helpers/cleanup");
const { directRedis, redisConfigNode } = require("./helpers/deployment");

helper.init(require.resolve("node-red"));

const CONFIG = redisConfigNode("config1", "Local");

function direct() {
    return directRedis();
}

function makeOutFlow(command, topic, obj) {
    return [
        CONFIG,
        {
            id: "out",
            type: "redis-out",
            server: "config1",
            command,
            topic,
            obj,
            wires: [],
        },
    ];
}

describe("redis-out node", function () {
    this.timeout(8000);

    beforeEach(function (done) { helper.startServer(done); });

    afterEach((done) => {
        helper.unload().then(() =>
            helper.stopServer(() => cleanupKeys("test:out:*", done))
        );
    });

    // ── rpush ──────────────────────────────────────────────────────────────

    it("rpush — appends string payload to the right of the list", function (done) {
        helper.load(redisNode, makeOutFlow("rpush", "test:out:rpush", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ payload: "item1" });

            setTimeout(() => {
                c.lrange("test:out:rpush", 0, -1)
                    .then((items) => {
                        c.disconnect();
                        try {
                            items.should.eql(["item1"]);
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 200);
        });
    });

    it("rpush — serializes object to JSON string when obj is true", function (done) {
        helper.load(redisNode, makeOutFlow("rpush", "test:out:rpush:json", true), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ payload: { a: 1 } });

            setTimeout(() => {
                c.lrange("test:out:rpush:json", 0, -1)
                    .then((items) => {
                        c.disconnect();
                        try {
                            items.length.should.equal(1);
                            JSON.parse(items[0]).should.deepEqual({ a: 1 });
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 200);
        });
    });

    it("rpush — msg.topic overrides the node topic", function (done) {
        helper.load(redisNode, makeOutFlow("rpush", "test:out:rpush:default", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ topic: "test:out:rpush:override", payload: "overridden" });

            setTimeout(() => {
                Promise.all([
                    c.lrange("test:out:rpush:override", 0, -1),
                    c.lrange("test:out:rpush:default", 0, -1),
                ])
                    .then(([ovr, def]) => {
                        c.disconnect();
                        try {
                            ovr.should.eql(["overridden"]);
                            def.should.eql([]);
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 200);
        });
    });

    it("rpush — appends multiple sequential messages in order", function (done) {
        helper.load(redisNode, makeOutFlow("rpush", "test:out:rpush:seq", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ payload: "first" });
            out.receive({ payload: "second" });
            out.receive({ payload: "third" });

            setTimeout(() => {
                c.lrange("test:out:rpush:seq", 0, -1)
                    .then((items) => {
                        c.disconnect();
                        try {
                            items.should.eql(["first", "second", "third"]);
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 300);
        });
    });

    // ── lpush ──────────────────────────────────────────────────────────────

    it("lpush — prepends string payload to the left of the list", function (done) {
        helper.load(redisNode, makeOutFlow("lpush", "test:out:lpush", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ payload: "first" });
            setTimeout(() => out.receive({ payload: "second" }), 50);

            setTimeout(() => {
                c.lrange("test:out:lpush", 0, -1)
                    .then((items) => {
                        c.disconnect();
                        try {
                            // lpush prepends; second push goes to head
                            items[0].should.equal("second");
                            items[1].should.equal("first");
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 300);
        });
    });

    it("lpush — serializes object to JSON when obj is true", function (done) {
        helper.load(redisNode, makeOutFlow("lpush", "test:out:lpush:json", true), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ payload: { flag: true } });

            setTimeout(() => {
                c.lrange("test:out:lpush:json", 0, -1)
                    .then((items) => {
                        c.disconnect();
                        try {
                            JSON.parse(items[0]).should.deepEqual({ flag: true });
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 200);
        });
    });

    // ── rpushx ─────────────────────────────────────────────────────────────

    it("rpushx — appends to existing list", function (done) {
        helper.load(redisNode, makeOutFlow("rpushx", "test:out:rpushx:exists", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            c.rpush("test:out:rpushx:exists", "seed")
                .then(() => {
                    out.receive({ payload: "appended" });
                    return new Promise((res) => setTimeout(res, 200));
                })
                .then(() => c.lrange("test:out:rpushx:exists", 0, -1))
                .then((items) => {
                    c.disconnect();
                    try {
                        items.should.eql(["seed", "appended"]);
                        done();
                    } catch (e) { done(e); }
                })
                .catch((e) => { c.disconnect(); done(e); });
        });
    });

    it("rpushx — does not create key when it does not exist", function (done) {
        helper.load(redisNode, makeOutFlow("rpushx", "test:out:rpushx:none", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ payload: "ignored" });

            setTimeout(() => {
                c.exists("test:out:rpushx:none")
                    .then((n) => {
                        c.disconnect();
                        try {
                            n.should.equal(0);
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 200);
        });
    });

    // ── lpushx ─────────────────────────────────────────────────────────────

    it("lpushx — prepends to existing list", function (done) {
        helper.load(redisNode, makeOutFlow("lpushx", "test:out:lpushx:exists", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            c.rpush("test:out:lpushx:exists", "seed")
                .then(() => {
                    out.receive({ payload: "prepended" });
                    return new Promise((res) => setTimeout(res, 200));
                })
                .then(() => c.lrange("test:out:lpushx:exists", 0, -1))
                .then((items) => {
                    c.disconnect();
                    try {
                        items[0].should.equal("prepended");
                        items[1].should.equal("seed");
                        done();
                    } catch (e) { done(e); }
                })
                .catch((e) => { c.disconnect(); done(e); });
        });
    });

    it("lpushx — does not create key when it does not exist", function (done) {
        helper.load(redisNode, makeOutFlow("lpushx", "test:out:lpushx:none", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ payload: "ignored" });

            setTimeout(() => {
                c.exists("test:out:lpushx:none")
                    .then((n) => {
                        c.disconnect();
                        try {
                            n.should.equal(0);
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 200);
        });
    });

    // ── publish ────────────────────────────────────────────────────────────

    it("publish — delivers plain string to subscriber", function (done) {
        helper.load(redisNode, makeOutFlow("publish", "test:out:publish:ch", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            c.subscribe("test:out:publish:ch");
            c.on("message", function (channel, message) {
                c.disconnect();
                try {
                    channel.should.equal("test:out:publish:ch");
                    message.should.equal("ping");
                    done();
                } catch (e) { done(e); }
            });

            setTimeout(() => out.receive({ payload: "ping" }), 300);
        });
    });

    it("publish — serializes object to JSON when obj is true", function (done) {
        helper.load(redisNode, makeOutFlow("publish", "test:out:publish:json", true), function () {
            const out = helper.getNode("out");
            const c = direct();

            c.subscribe("test:out:publish:json");
            c.on("message", function (channel, message) {
                c.disconnect();
                try {
                    JSON.parse(message).should.deepEqual({ hello: "world" });
                    done();
                } catch (e) { done(e); }
            });

            setTimeout(() => out.receive({ payload: { hello: "world" } }), 300);
        });
    });

    it("publish — msg.topic overrides the node topic", function (done) {
        helper.load(redisNode, makeOutFlow("publish", "test:out:publish:default", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            c.subscribe("test:out:publish:override");
            c.on("message", function (channel, message) {
                c.disconnect();
                try {
                    channel.should.equal("test:out:publish:override");
                    message.should.equal("routed");
                    done();
                } catch (e) { done(e); }
            });

            setTimeout(() => out.receive({ topic: "test:out:publish:override", payload: "routed" }), 300);
        });
    });

    // ── xadd ───────────────────────────────────────────────────────────────

    it("xadd — appends object payload as stream field-value pairs", function (done) {
        helper.load(redisNode, makeOutFlow("xadd", "test:out:xadd:obj", true), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ payload: { temperature: "22", unit: "C" } });

            setTimeout(() => {
                c.xrange("test:out:xadd:obj", "-", "+")
                    .then((entries) => {
                        c.disconnect();
                        try {
                            entries.length.should.equal(1);
                            const fields = entries[0][1]; // flat [field, val, ...]
                            const ti = fields.indexOf("temperature");
                            fields[ti + 1].should.equal("22");
                            const ui = fields.indexOf("unit");
                            fields[ui + 1].should.equal("C");
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 300);
        });
    });

    it("xadd — wraps primitive payload in a 'value' field", function (done) {
        helper.load(redisNode, makeOutFlow("xadd", "test:out:xadd:str", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ payload: "sensor-reading" });

            setTimeout(() => {
                c.xrange("test:out:xadd:str", "-", "+")
                    .then((entries) => {
                        c.disconnect();
                        try {
                            entries.length.should.equal(1);
                            const fields = entries[0][1];
                            fields[0].should.equal("value");
                            fields[1].should.equal("sensor-reading");
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 300);
        });
    });

    it("xadd — uses flat array payload as field-value args", function (done) {
        helper.load(redisNode, makeOutFlow("xadd", "test:out:xadd:arr", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ payload: ["city", "Auckland", "country", "NZ"] });

            setTimeout(() => {
                c.xrange("test:out:xadd:arr", "-", "+")
                    .then((entries) => {
                        c.disconnect();
                        try {
                            entries.length.should.equal(1);
                            const fields = entries[0][1];
                            const ci = fields.indexOf("city");
                            fields[ci + 1].should.equal("Auckland");
                            const nzi = fields.indexOf("country");
                            fields[nzi + 1].should.equal("NZ");
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 300);
        });
    });

    it("xadd — multiple messages each become separate stream entries", function (done) {
        helper.load(redisNode, makeOutFlow("xadd", "test:out:xadd:multi", true), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ payload: { seq: "1" } });
            out.receive({ payload: { seq: "2" } });
            out.receive({ payload: { seq: "3" } });

            setTimeout(() => {
                c.xlen("test:out:xadd:multi")
                    .then((len) => {
                        c.disconnect();
                        try {
                            len.should.equal(3);
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 300);
        });
    });

    // ── zadd ───────────────────────────────────────────────────────────────

    it("zadd — adds member with score from {score, member} object", function (done) {
        helper.load(redisNode, makeOutFlow("zadd", "test:out:zadd:obj", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ payload: { score: 42, member: "job-a" } });

            setTimeout(() => {
                c.zscore("test:out:zadd:obj", "job-a")
                    .then((score) => {
                        c.disconnect();
                        try {
                            parseFloat(score).should.equal(42);
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 200);
        });
    });

    it("zadd — adds multiple members from flat [score, member, ...] array", function (done) {
        helper.load(redisNode, makeOutFlow("zadd", "test:out:zadd:arr", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ payload: [10, "alpha", 20, "beta"] });

            setTimeout(() => {
                Promise.all([
                    c.zscore("test:out:zadd:arr", "alpha"),
                    c.zscore("test:out:zadd:arr", "beta"),
                ])
                    .then(([s1, s2]) => {
                        c.disconnect();
                        try {
                            parseFloat(s1).should.equal(10);
                            parseFloat(s2).should.equal(20);
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 200);
        });
    });

    it("zadd — serializes object member to JSON string when obj is true", function (done) {
        helper.load(redisNode, makeOutFlow("zadd", "test:out:zadd:json", true), function () {
            const out = helper.getNode("out");
            const c = direct();
            const member = { id: 99, name: "task" };

            out.receive({ payload: { score: 5, member } });

            setTimeout(() => {
                c.zrange("test:out:zadd:json", 0, -1)
                    .then((members) => {
                        c.disconnect();
                        try {
                            members.length.should.equal(1);
                            JSON.parse(members[0]).should.deepEqual(member);
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 200);
        });
    });

    it("zadd — updates score when same member is added twice", function (done) {
        helper.load(redisNode, makeOutFlow("zadd", "test:out:zadd:update", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            out.receive({ payload: { score: 1, member: "item" } });
            setTimeout(() => out.receive({ payload: { score: 99, member: "item" } }), 50);

            setTimeout(() => {
                c.zscore("test:out:zadd:update", "item")
                    .then((score) => {
                        c.disconnect();
                        try {
                            parseFloat(score).should.equal(99);
                            done();
                        } catch (e) { done(e); }
                    })
                    .catch((e) => { c.disconnect(); done(e); });
            }, 300);
        });
    });

    // ── error handling ─────────────────────────────────────────────────────

    it("calls node.error when both msg.topic and node topic are empty", function (done) {
        helper.load(redisNode, makeOutFlow("rpush", "", false), function () {
            const out = helper.getNode("out");

            out.receive({ topic: "", payload: "should-fail" });

            setTimeout(() => {
                try {
                    out.error.callCount.should.be.above(0);
                    done();
                } catch (e) { done(e); }
            }, 200);
        });
    });

    it("zadd — calls node.error when payload is a plain string", function (done) {
        helper.load(redisNode, makeOutFlow("zadd", "test:out:zadd:err", false), function () {
            const out = helper.getNode("out");

            out.receive({ payload: "invalid-payload" });

            setTimeout(() => {
                try {
                    out.error.callCount.should.be.above(0);
                    done();
                } catch (e) { done(e); }
            }, 200);
        });
    });
});
