"use strict";
const assert = require("assert");
const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const Redis = require("ioredis");
const {
    badRedisConfigNode,
    redisConfigNode,
} = require("./helpers/deployment");

helper.init(require.resolve("node-red"));

// ── shared config that points at a real local Redis ────────────────────────
const GOOD_CONFIG = redisConfigNode("cfg-good", "GoodConn");

// Bad config: port where nothing listens, so the connection is refused immediately.
const BAD_CONFIG = badRedisConfigNode("cfg-bad", "BadConn");

// ── helpers ────────────────────────────────────────────────────────────────

// Waits for node.status() to be called with a matching fill/text, then calls done().
// Resolves the race where status may fire before OR after we attach the listener.
function onStatus(node, predicate, done) {
    let finished = false;
    const listener = function (call) {
        const s = call.args[0];
        if (!finished && s && predicate(s)) {
            finished = true;
            node.removeListener("call:status", listener);
            done();
        }
    };
    node.on("call:status", listener);
}

function isGreen(s) { return s.fill === "green"; }
function isRed(s)   { return s.fill === "red"; }

describe("node connection status", function () {
    this.timeout(5000);

    beforeEach(function (done) { helper.startServer(done); });
    afterEach(function (done) {
        helper.unload().then(() => helper.stopServer(done));
    });

    describe("redis-config test connection endpoint", function () {
        it("connects, pings, and gracefully disconnects with current JSON options", async function () {
            let quitCalled = false;
            const originalQuit = Redis.prototype.quit;
            Redis.prototype.quit = async function () {
                quitCalled = true;
                return originalQuit.call(this);
            };
            try {
                await helper.load(redisNode, [GOOD_CONFIG]);
                const res = await helper
                    .request()
                    .post("/redis-config/test")
                    .send({
                        id: "cfg-good",
                        cluster: false,
                        optionsType: "json",
                        options: GOOD_CONFIG.options,
                    })
                    .expect(200);

                assert.strictEqual(res.body.success, true);
                assert.strictEqual(res.body.response, "PONG");
                assert.match(res.body.message, /PING -> PONG/);
                assert.ok(Array.isArray(res.body.log), "verbose log should be returned");
                assert.ok(quitCalled, "temporary test client should disconnect with QUIT");
            } finally {
                Redis.prototype.quit = originalQuit;
            }
        });

        it("returns verbose errors and calls node.error when the test connection fails", async function () {
            let consoleErrorCalled = false;
            const originalConsoleError = console.error;
            console.error = function () {
                consoleErrorCalled = true;
            };
            await helper.load(redisNode, [BAD_CONFIG]);
            const config = helper.getNode("cfg-bad");
            let errorCall;
            config.on("call:error", function (call) {
                errorCall = call;
            });

            try {
                const res = await helper
                    .request()
                    .post("/redis-config/test")
                    .send({
                        id: "cfg-bad",
                        cluster: false,
                        optionsType: "json",
                        options: BAD_CONFIG.options,
                    })
                    .expect(500);

                await new Promise((resolve) => setImmediate(resolve));

                assert.strictEqual(res.body.success, false);
                assert.match(res.body.message, /Connection test failed/);
                assert.ok(res.body.error && res.body.error.message, "error details should be returned");
                assert.ok(Array.isArray(res.body.log), "verbose log should be returned");
                assert.ok(consoleErrorCalled, "full error should be logged to console");
                assert.ok(errorCall, "config node should call node.error");
                assert.match(String(errorCall.args[0]), /redis-config test connection failed/);
            } finally {
                console.error = originalConsoleError;
            }
        });
    });

    // ── redis-in ────────────────────────────────────────────────────────────

    describe("redis-in", function () {
        it("blpop — shows green connected when Redis is reachable", function (done) {
            const flow = [GOOD_CONFIG, {
                id: "n1", type: "redis-in", server: "cfg-good",
                command: "blpop", topic: "status:blpop", obj: false, timeout: 1,
                groupname: "", consumername: "", wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isGreen, done);
            });
        });

        it("subscribe — shows green connected when Redis is reachable", function (done) {
            const flow = [GOOD_CONFIG, {
                id: "n1", type: "redis-in", server: "cfg-good",
                command: "subscribe", topic: "status:sub", obj: false, timeout: 0,
                groupname: "", consumername: "", wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isGreen, done);
            });
        });

        it("psubscribe — shows green connected when Redis is reachable", function (done) {
            const flow = [GOOD_CONFIG, {
                id: "n1", type: "redis-in", server: "cfg-good",
                command: "psubscribe", topic: "status:psub:*", obj: false, timeout: 0,
                groupname: "", consumername: "", wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isGreen, done);
            });
        });

        it("blpop — shows red/error status when Redis is unreachable", function (done) {
            const flow = [BAD_CONFIG, {
                id: "n1", type: "redis-in", server: "cfg-bad",
                command: "blpop", topic: "status:bad:blpop", obj: false, timeout: 1,
                groupname: "", consumername: "", wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isRed, done);
            });
        });
    });

    // ── redis-out ───────────────────────────────────────────────────────────

    describe("redis-out", function () {
        it("shows green connected when Redis is reachable", function (done) {
            const flow = [GOOD_CONFIG, {
                id: "n1", type: "redis-out", server: "cfg-good",
                command: "rpush", topic: "status:out", obj: false, wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isGreen, done);
            });
        });

        it("shows red/error status when Redis is unreachable", function (done) {
            const flow = [BAD_CONFIG, {
                id: "n1", type: "redis-out", server: "cfg-bad",
                command: "rpush", topic: "status:out:bad", obj: false, wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isRed, done);
            });
        });
    });

    // ── redis-command ───────────────────────────────────────────────────────

    describe("redis-command", function () {
        it("shows green connected when Redis is reachable", function (done) {
            const flow = [GOOD_CONFIG, {
                id: "n1", type: "redis-command", server: "cfg-good",
                command: "GET", topic: "", params: "[]", block: false, wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isGreen, done);
            });
        });

        it("shows red/error status when Redis is unreachable", function (done) {
            const flow = [BAD_CONFIG, {
                id: "n1", type: "redis-command", server: "cfg-bad",
                command: "GET", topic: "", params: "[]", block: false, wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isRed, done);
            });
        });
    });

    // ── redis-lua-script ────────────────────────────────────────────────────

    describe("redis-lua-script", function () {
        it("(non-stored) shows green connected when Redis is reachable", function (done) {
            const flow = [GOOD_CONFIG, {
                id: "n1", type: "redis-lua-script", server: "cfg-good",
                func: "return 1", keyval: 0, stored: false, block: false, wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isGreen, done);
            });
        });

        it("(stored) shows green 'script loaded' when Redis is reachable", function (done) {
            const flow = [GOOD_CONFIG, {
                id: "n1", type: "redis-lua-script", server: "cfg-good",
                func: "return 1", keyval: 0, stored: true, block: false, wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), function (s) {
                    return s.fill === "green" && s.text === "script loaded";
                }, done);
            });
        });

        it("(non-stored) shows red/error status when Redis is unreachable", function (done) {
            const flow = [BAD_CONFIG, {
                id: "n1", type: "redis-lua-script", server: "cfg-bad",
                func: "return 1", keyval: 0, stored: false, block: false, wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isRed, done);
            });
        });
    });

    // ── redis-instance ──────────────────────────────────────────────────────

    describe("redis-instance", function () {
        it("shows green connected when Redis is reachable", function (done) {
            const flow = [GOOD_CONFIG, {
                id: "n1", type: "redis-instance", server: "cfg-good",
                topic: "myClient", location: "flow", wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isGreen, done);
            });
        });

        it("shows red/error status when Redis is unreachable", function (done) {
            const flow = [BAD_CONFIG, {
                id: "n1", type: "redis-instance", server: "cfg-bad",
                topic: "myClient", location: "flow", wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isRed, done);
            });
        });
    });

    // ── close clears status ─────────────────────────────────────────────────

    describe("on node close", function () {
        it("redis-in clears status to empty on close", function (done) {
            const flow = [GOOD_CONFIG, {
                id: "n1", type: "redis-in", server: "cfg-good",
                command: "blpop", topic: "status:close", obj: false, timeout: 1,
                groupname: "", consumername: "", wires: [],
            }];
            helper.load(redisNode, flow, function () {
                const n = helper.getNode("n1");
                onStatus(n, isGreen, function () {
                    let sawEmpty = false;
                    // helper.unload() calls sandbox.restore() before stopFlows(),
                    // which removes the call:status proxy. Override the instance
                    // method directly so we still intercept the close-handler call.
                    const orig = n.status.bind(n);
                    n.status = function (s) {
                        if (s && Object.keys(s).length === 0) { sawEmpty = true; }
                        return orig(s);
                    };
                    helper.unload().then(function () {
                        if (sawEmpty) {
                            done();
                        } else {
                            done(new Error("status({}) was not called on close"));
                        }
                    });
                });
            });
        });
    });

    // ── graceful shutdown ───────────────────────────────────────────────────

    describe("graceful shutdown", function () {
        // Spy on Redis.prototype.quit by wrapping it. Since ioredis is cached by
        // Node.js module system, the same prototype is used by redis.js — so this
        // spy is visible inside the node without any module re-loading tricks.
        let originalQuit;
        let quitCalled;

        beforeEach(function () {
            quitCalled = false;
            originalQuit = Redis.prototype.quit;
            Redis.prototype.quit = async function () {
                quitCalled = true;
                return originalQuit.call(this);
            };
        });

        afterEach(function () {
            Redis.prototype.quit = originalQuit;
        });

        it("redis-out calls quit() on shutdown", function (done) {
            const flow = [GOOD_CONFIG, {
                id: "n1", type: "redis-out", server: "cfg-good",
                command: "rpush", topic: "shutdown:out", obj: false, wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isGreen, function () {
                    helper.unload().then(function () {
                        if (quitCalled) {
                            done();
                        } else {
                            done(new Error("quit() was not called on shutdown"));
                        }
                    });
                });
            });
        });

        it("redis-command calls quit() on shutdown", function (done) {
            const flow = [GOOD_CONFIG, {
                id: "n1", type: "redis-command", server: "cfg-good",
                command: "GET", topic: "", params: "[]", block: false, wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isGreen, function () {
                    helper.unload().then(function () {
                        if (quitCalled) {
                            done();
                        } else {
                            done(new Error("quit() was not called on shutdown"));
                        }
                    });
                });
            });
        });

        it("redis-in (blpop) skips quit() and disconnects immediately on shutdown", function (done) {
            // BLPOP/XREADGROUP BLOCK 0 queue QUIT behind the in-flight blocking
            // command — QUIT would never be sent, so we force-disconnect instead.
            const flow = [GOOD_CONFIG, {
                id: "n1", type: "redis-in", server: "cfg-good",
                command: "blpop", topic: "shutdown:in", obj: false, timeout: 1,
                groupname: "", consumername: "", wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isGreen, function () {
                    helper.unload().then(function () {
                        if (!quitCalled) {
                            done();
                        } else {
                            done(new Error("quit() should NOT be called for blocking redis-in nodes"));
                        }
                    });
                });
            });
        });

        it("redis-lua-script calls quit() on shutdown", function (done) {
            const flow = [GOOD_CONFIG, {
                id: "n1", type: "redis-lua-script", server: "cfg-good",
                func: "return 1", keyval: 0, stored: false, block: false, wires: [],
            }];
            helper.load(redisNode, flow, function () {
                onStatus(helper.getNode("n1"), isGreen, function () {
                    helper.unload().then(function () {
                        if (quitCalled) {
                            done();
                        } else {
                            done(new Error("quit() was not called on shutdown"));
                        }
                    });
                });
            });
        });
    });
});
