"use strict";
const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const Redis = require("ioredis");

helper.init(require.resolve("node-red"));

// ── shared config that points at a real local Redis ────────────────────────
const GOOD_CONFIG = {
    id: "cfg-good",
    type: "redis-config",
    name: "GoodConn",
    options: '{"host":"127.0.0.1","port":6379}',
    optionsType: "json",
    cluster: false,
};

// Bad config: port where nothing listens, so the connection is refused immediately.
const BAD_CONFIG = {
    id: "cfg-bad",
    type: "redis-config",
    name: "BadConn",
    options: '{"host":"127.0.0.1","port":6399}',
    optionsType: "json",
    cluster: false,
};

// ── helpers ────────────────────────────────────────────────────────────────

// Waits for node.status() to be called with a matching fill/text, then calls done().
// Resolves the race where status may fire before OR after we attach the listener.
function onStatus(node, predicate, done) {
    node.on("call:status", function (call) {
        const s = call.args[0];
        if (s && predicate(s)) {
            done();
        }
    });
}

function isGreen(s) { return s.fill === "green"; }
function isRed(s)   { return s.fill === "red"; }

describe("node connection status", function () {
    this.timeout(5000);

    beforeEach(function (done) { helper.startServer(done); });
    afterEach(function (done) {
        helper.unload().then(() => helper.stopServer(done));
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

        it("redis-in (blpop) calls quit() on shutdown", function (done) {
            const flow = [GOOD_CONFIG, {
                id: "n1", type: "redis-in", server: "cfg-good",
                command: "blpop", topic: "shutdown:in", obj: false, timeout: 1,
                groupname: "", consumername: "", wires: [],
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
