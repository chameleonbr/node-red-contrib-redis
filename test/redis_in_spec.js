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

function makeInFlow(command, topic, obj, extra = {}) {
    return [
        CONFIG,
        {
            id: "in",
            type: "redis-in",
            server: "config1",
            command,
            topic,
            obj,
            timeout: extra.timeout !== undefined ? extra.timeout : 1,
            groupname: extra.groupname || "",
            consumername: extra.consumername || "",
            wires: [["h"]],
        },
        { id: "h", type: "helper" },
    ];
}

describe("redis-in node", function () {
    this.timeout(8000);

    beforeEach(function (done) { helper.startServer(done); });

    afterEach((done) => {
        helper.unload().then(() =>
            helper.stopServer(() =>
                cleanupKeys("test:in:*", () =>
                    cleanupKeys("testinxrg*", done)
                )
            )
        );
    });

    // ── blpop ──────────────────────────────────────────────────────────────

    it("blpop — emits raw string payload with key as topic", function (done) {
        helper.load(redisNode, makeInFlow("blpop", "test:in:blpop", false), function () {
            const h = helper.getNode("h");
            const c = direct();

            h.on("input", function (msg) {
                c.disconnect();
                try {
                    msg.topic.should.equal("test:in:blpop");
                    msg.payload.should.equal("hello");
                    done();
                } catch (e) { done(e); }
            });

            setTimeout(() => c.rpush("test:in:blpop", "hello"), 150);
        });
    });

    it("blpop — parses JSON payload when obj is true", function (done) {
        helper.load(redisNode, makeInFlow("blpop", "test:in:blpop:json", true), function () {
            const h = helper.getNode("h");
            const c = direct();

            h.on("input", function (msg) {
                c.disconnect();
                try {
                    msg.payload.should.be.an.Object();
                    msg.payload.k.should.equal("v");
                    done();
                } catch (e) { done(e); }
            });

            setTimeout(() => c.rpush("test:in:blpop:json", JSON.stringify({ k: "v" })), 150);
        });
    });

    it("blpop — falls back to raw string for invalid JSON when obj is true", function (done) {
        helper.load(redisNode, makeInFlow("blpop", "test:in:blpop:fallback", true), function () {
            const h = helper.getNode("h");
            const c = direct();

            h.on("input", function (msg) {
                c.disconnect();
                try {
                    msg.payload.should.equal("not-json");
                    done();
                } catch (e) { done(e); }
            });

            setTimeout(() => c.rpush("test:in:blpop:fallback", "not-json"), 150);
        });
    });

    it("blpop — topic in output reflects actual Redis key, not msg.topic", function (done) {
        helper.load(redisNode, makeInFlow("blpop", "test:in:blpop:key", false), function () {
            const h = helper.getNode("h");
            const c = direct();

            h.on("input", function (msg) {
                c.disconnect();
                try {
                    msg.topic.should.equal("test:in:blpop:key");
                    done();
                } catch (e) { done(e); }
            });

            setTimeout(() => c.rpush("test:in:blpop:key", "data"), 150);
        });
    });

    // ── brpop ──────────────────────────────────────────────────────────────

    it("brpop — emits raw string payload popped from right of list", function (done) {
        helper.load(redisNode, makeInFlow("brpop", "test:in:brpop", false), function () {
            const h = helper.getNode("h");
            const c = direct();

            h.on("input", function (msg) {
                c.disconnect();
                try {
                    msg.topic.should.equal("test:in:brpop");
                    msg.payload.should.equal("world");
                    done();
                } catch (e) { done(e); }
            });

            // lpush so the element is at right (brpop pops from right)
            setTimeout(() => c.lpush("test:in:brpop", "world"), 150);
        });
    });

    it("brpop — parses JSON payload when obj is true", function (done) {
        helper.load(redisNode, makeInFlow("brpop", "test:in:brpop:json", true), function () {
            const h = helper.getNode("h");
            const c = direct();

            h.on("input", function (msg) {
                c.disconnect();
                try {
                    msg.payload.n.should.equal(42);
                    done();
                } catch (e) { done(e); }
            });

            setTimeout(() => c.lpush("test:in:brpop:json", JSON.stringify({ n: 42 })), 150);
        });
    });

    it("brpop — multiple messages received in order", function (done) {
        helper.load(redisNode, makeInFlow("brpop", "test:in:brpop:multi", false), function () {
            const h = helper.getNode("h");
            const c = direct();
            const received = [];

            h.on("input", function (msg) {
                received.push(msg.payload);
                if (received.length === 2) {
                    c.disconnect();
                    try {
                        // brpop pops from right, lpush pushes to left so rightmost is "a"
                        received[0].should.equal("a");
                        received[1].should.equal("b");
                        done();
                    } catch (e) { done(e); }
                }
            });

            // Pre-populate so brpop can immediately fire twice
            setTimeout(() => {
                c.rpush("test:in:brpop:multi", "a");
                setTimeout(() => c.rpush("test:in:brpop:multi", "b"), 50);
            }, 150);
        });
    });

    // ── subscribe ──────────────────────────────────────────────────────────

    it("subscribe — emits message published to the channel", function (done) {
        helper.load(
            redisNode,
            makeInFlow("subscribe", "test:in:subscribe:ch", false, { timeout: 0 }),
            function () {
                const h = helper.getNode("h");
                const c = direct();

                h.on("input", function (msg) {
                    c.disconnect();
                    try {
                        msg.topic.should.equal("test:in:subscribe:ch");
                        msg.payload.should.equal("hello sub");
                        done();
                    } catch (e) { done(e); }
                });

                setTimeout(() => c.publish("test:in:subscribe:ch", "hello sub"), 300);
            }
        );
    });

    it("subscribe — parses JSON payload when obj is true", function (done) {
        helper.load(
            redisNode,
            makeInFlow("subscribe", "test:in:subscribe:json", true, { timeout: 0 }),
            function () {
                const h = helper.getNode("h");
                const c = direct();

                h.on("input", function (msg) {
                    c.disconnect();
                    try {
                        msg.payload.should.be.an.Object();
                        msg.payload.x.should.equal(1);
                        done();
                    } catch (e) { done(e); }
                });

                setTimeout(() => c.publish("test:in:subscribe:json", JSON.stringify({ x: 1 })), 300);
            }
        );
    });

    it("subscribe — falls back to raw string for invalid JSON when obj is true", function (done) {
        helper.load(
            redisNode,
            makeInFlow("subscribe", "test:in:subscribe:fallback", true, { timeout: 0 }),
            function () {
                const h = helper.getNode("h");
                const c = direct();

                h.on("input", function (msg) {
                    c.disconnect();
                    try {
                        msg.payload.should.equal("plain-text");
                        done();
                    } catch (e) { done(e); }
                });

                setTimeout(() => c.publish("test:in:subscribe:fallback", "plain-text"), 300);
            }
        );
    });

    it("subscribe — receives multiple messages on same channel", function (done) {
        helper.load(
            redisNode,
            makeInFlow("subscribe", "test:in:subscribe:multi", false, { timeout: 0 }),
            function () {
                const h = helper.getNode("h");
                const c = direct();
                const received = [];

                h.on("input", function (msg) {
                    received.push(msg.payload);
                    if (received.length === 3) {
                        c.disconnect();
                        try {
                            received.should.eql(["msg1", "msg2", "msg3"]);
                            done();
                        } catch (e) { done(e); }
                    }
                });

                setTimeout(() => {
                    c.publish("test:in:subscribe:multi", "msg1");
                    c.publish("test:in:subscribe:multi", "msg2");
                    c.publish("test:in:subscribe:multi", "msg3");
                }, 300);
            }
        );
    });

    // ── psubscribe ─────────────────────────────────────────────────────────

    it("psubscribe — emits msg.pattern and msg.topic for matching channels", function (done) {
        helper.load(
            redisNode,
            makeInFlow("psubscribe", "test:in:ps:*", false, { timeout: 0 }),
            function () {
                const h = helper.getNode("h");
                const c = direct();

                h.on("input", function (msg) {
                    c.disconnect();
                    try {
                        msg.pattern.should.equal("test:in:ps:*");
                        msg.topic.should.equal("test:in:ps:news");
                        msg.payload.should.equal("event");
                        done();
                    } catch (e) { done(e); }
                });

                setTimeout(() => c.publish("test:in:ps:news", "event"), 300);
            }
        );
    });

    it("psubscribe — receives messages from multiple matching channels", function (done) {
        helper.load(
            redisNode,
            makeInFlow("psubscribe", "test:in:psmulti:*", false, { timeout: 0 }),
            function () {
                const h = helper.getNode("h");
                const c = direct();
                const topics = [];

                h.on("input", function (msg) {
                    topics.push(msg.topic);
                    if (topics.length === 2) {
                        c.disconnect();
                        try {
                            topics.sort().should.eql([
                                "test:in:psmulti:alpha",
                                "test:in:psmulti:beta",
                            ]);
                            done();
                        } catch (e) { done(e); }
                    }
                });

                setTimeout(() => {
                    c.publish("test:in:psmulti:alpha", "1");
                    c.publish("test:in:psmulti:beta", "2");
                }, 300);
            }
        );
    });

    it("psubscribe — parses JSON payload when obj is true", function (done) {
        helper.load(
            redisNode,
            makeInFlow("psubscribe", "test:in:psjson:*", true, { timeout: 0 }),
            function () {
                const h = helper.getNode("h");
                const c = direct();

                h.on("input", function (msg) {
                    c.disconnect();
                    try {
                        msg.payload.should.be.an.Object();
                        msg.payload.type.should.equal("psubscribe");
                        done();
                    } catch (e) { done(e); }
                });

                setTimeout(
                    () => c.publish("test:in:psjson:ch1", JSON.stringify({ type: "psubscribe" })),
                    300
                );
            }
        );
    });

    // ── xreadgroup ─────────────────────────────────────────────────────────

    it("xreadgroup — receives stream message as field object when obj is true", function (done) {
        const STREAM = "testinxrg";
        const GROUP = "grp";
        const c = direct();

        c.xgroup("CREATE", STREAM, GROUP, "0", "MKSTREAM")
            .then(() => c.xadd(STREAM, "*", "field1", "val1", "field2", "val2"))
            .then(() => {
                helper.load(
                    redisNode,
                    makeInFlow("xreadgroup", `${STREAM}:>`, true, {
                        timeout: 0,
                        groupname: GROUP,
                        consumername: "consumer-1",
                    }),
                    function () {
                        const h = helper.getNode("h");

                        h.on("input", function (msg) {
                            c.disconnect();
                            try {
                                msg.stream.should.equal(STREAM);
                                msg.messageId.should.be.a.String();
                                msg.payload.should.be.an.Object();
                                msg.payload.field1.should.equal("val1");
                                msg.payload.field2.should.equal("val2");
                                done();
                            } catch (e) { done(e); }
                        });
                    }
                );
            })
            .catch(done);
    });

    it("xreadgroup — emits raw flat key-value array when obj is false", function (done) {
        const STREAM = "testinxrgraw";
        const GROUP = "grpraw";
        const c = direct();

        c.xgroup("CREATE", STREAM, GROUP, "0", "MKSTREAM")
            .then(() => c.xadd(STREAM, "*", "k", "v"))
            .then(() => {
                helper.load(
                    redisNode,
                    makeInFlow("xreadgroup", `${STREAM}:>`, false, {
                        timeout: 0,
                        groupname: GROUP,
                        consumername: "consumer-1",
                    }),
                    function () {
                        const h = helper.getNode("h");

                        h.on("input", function (msg) {
                            c.disconnect();
                            try {
                                msg.payload.should.be.an.Array();
                                msg.payload[0].should.equal("k");
                                msg.payload[1].should.equal("v");
                                done();
                            } catch (e) { done(e); }
                        });
                    }
                );
            })
            .catch(done);
    });

    // ── bzpopmin ───────────────────────────────────────────────────────────

    it("bzpopmin — emits {member, score} popping the lowest-score element first", function (done) {
        const c = direct();

        // Pre-populate both members so bzpopmin sees them together and picks the minimum
        Promise.all([
            c.zadd("test:in:bzpopmin", 5, "task-high"),
            c.zadd("test:in:bzpopmin", 1, "task-low"),
        ]).then(() => {
            helper.load(redisNode, makeInFlow("bzpopmin", "test:in:bzpopmin", false), function () {
                const h = helper.getNode("h");

                h.on("input", function (msg) {
                    c.disconnect();
                    try {
                        msg.topic.should.equal("test:in:bzpopmin");
                        msg.payload.member.should.equal("task-low");
                        msg.payload.score.should.equal(1);
                        done();
                    } catch (e) { done(e); }
                });
            });
        }).catch(done);
    });

    it("bzpopmin — parses JSON member when obj is true", function (done) {
        helper.load(redisNode, makeInFlow("bzpopmin", "test:in:bzpopmin:json", true), function () {
            const h = helper.getNode("h");
            const c = direct();

            h.on("input", function (msg) {
                c.disconnect();
                try {
                    msg.payload.member.should.be.an.Object();
                    msg.payload.member.name.should.equal("job1");
                    msg.payload.score.should.equal(3);
                    done();
                } catch (e) { done(e); }
            });

            setTimeout(
                () => c.zadd("test:in:bzpopmin:json", 3, JSON.stringify({ name: "job1" })),
                150
            );
        });
    });

    // ── bzpopmax ───────────────────────────────────────────────────────────

    it("bzpopmax — emits {member, score} popping the highest-score element first", function (done) {
        const c = direct();

        // Pre-populate both members so bzpopmax sees them together and picks the maximum
        Promise.all([
            c.zadd("test:in:bzpopmax", 10, "task-high"),
            c.zadd("test:in:bzpopmax", 1, "task-low"),
        ]).then(() => {
            helper.load(redisNode, makeInFlow("bzpopmax", "test:in:bzpopmax", false), function () {
                const h = helper.getNode("h");

                h.on("input", function (msg) {
                    c.disconnect();
                    try {
                        msg.topic.should.equal("test:in:bzpopmax");
                        msg.payload.member.should.equal("task-high");
                        msg.payload.score.should.equal(10);
                        done();
                    } catch (e) { done(e); }
                });
            });
        }).catch(done);
    });

    it("bzpopmax — score is returned as a float", function (done) {
        helper.load(redisNode, makeInFlow("bzpopmax", "test:in:bzpopmax:float", false), function () {
            const h = helper.getNode("h");
            const c = direct();

            h.on("input", function (msg) {
                c.disconnect();
                try {
                    msg.payload.score.should.equal(3.14);
                    done();
                } catch (e) { done(e); }
            });

            setTimeout(() => c.zadd("test:in:bzpopmax:float", 3.14, "pi-task"), 150);
        });
    });
});
