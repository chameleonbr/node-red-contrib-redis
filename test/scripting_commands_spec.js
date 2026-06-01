const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { cleanupKeys } = require("./helpers/cleanup");
const { directRedis, redisConfigNode } = require("./helpers/deployment");

helper.init(require.resolve("node-red"));

// Polls until the redis-lua-script node has loaded its stored script and
// captured the SHA1, then invokes cb. Avoids racing the async SCRIPT LOAD
// that fires on the connection "ready" event.
function waitForSha1(node, cb) {
  const start = Date.now();
  const tick = () => {
    if (node.sha1 && node.sha1.length === 40) {
      cb();
    } else if (Date.now() - start > 4000) {
      cb(new Error("script was never loaded (sha1 not set)"));
    } else {
      setTimeout(tick, 25);
    }
  };
  tick();
}

describe("Scripting commands", function () {
  this.timeout(5000);

  const configNode = redisConfigNode("config1", "Local");

  beforeEach((done) => {
    helper.startServer(done);
  });

  afterEach((done) => {
    helper.unload().then(() => {
      helper.stopServer(() => {
        cleanupKeys("test:script:*", done);
      });
    });
  });

  it("should EVAL execute a Lua script and return result", function (done) {
    const flow = [
      configNode,
      {
        id: "eval-node",
        type: "redis-command",
        server: "config1",
        command: "EVAL",
        name: "EVAL",
        topic: "",
        params: "[]",
        wires: [["eval-helper"]],
      },
      { id: "eval-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const evalNode = helper.getNode("eval-node");
      const evalHelper = helper.getNode("eval-helper");

      evalHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("hello");
          done();
        } catch (err) {
          done(err);
        }
      });

      evalNode.receive({ payload: ["return 'hello'", "0"] });
    });
  });

  it("should EVAL_RO execute a read-only Lua script", function (done) {
    const flow = [
      configNode,
      {
        id: "set-node",
        type: "redis-command",
        server: "config1",
        command: "SET",
        name: "SET",
        topic: "",
        params: "[]",
        wires: [["set-helper"]],
      },
      { id: "set-helper", type: "helper" },
      {
        id: "evalro-node",
        type: "redis-command",
        server: "config1",
        command: "EVAL_RO",
        name: "EVAL_RO",
        topic: "",
        params: "[]",
        wires: [["evalro-helper"]],
      },
      { id: "evalro-helper", type: "helper" },
      {
        id: "del-node",
        type: "redis-command",
        server: "config1",
        command: "DEL",
        name: "DEL",
        topic: "",
        params: "[]",
        wires: [["del-helper"]],
      },
      { id: "del-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const setNode = helper.getNode("set-node");
      const setHelper = helper.getNode("set-helper");
      const evalroNode = helper.getNode("evalro-node");
      const evalroHelper = helper.getNode("evalro-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      evalroHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("world");
          delNode.receive({ topic: "test:script:evalro" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        evalroNode.receive({
          payload: ["return redis.call('GET',KEYS[1])", "1", "test:script:evalro"],
        });
      });

      setNode.receive({ topic: "test:script:evalro", payload: "world" });
    });
  });

  it("should SCRIPT LOAD return SHA1 and EVALSHA execute it", function (done) {
    const flow = [
      configNode,
      {
        id: "scriptload-node",
        type: "redis-command",
        server: "config1",
        command: "SCRIPT",
        name: "SCRIPT",
        topic: "",
        params: "[]",
        wires: [["scriptload-helper"]],
      },
      { id: "scriptload-helper", type: "helper" },
      {
        id: "evalsha-node",
        type: "redis-command",
        server: "config1",
        command: "EVALSHA",
        name: "EVALSHA",
        topic: "",
        params: "[]",
        wires: [["evalsha-helper"]],
      },
      { id: "evalsha-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const scriptloadNode = helper.getNode("scriptload-node");
      const scriptloadHelper = helper.getNode("scriptload-helper");
      const evalshaNode = helper.getNode("evalsha-node");
      const evalshaHelper = helper.getNode("evalsha-helper");

      evalshaHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("scripted");
          done();
        } catch (err) {
          done(err);
        }
      });

      scriptloadHelper.on("input", (msg) => {
        try {
          const sha1 = msg.payload;
          sha1.should.be.a.String();
          sha1.length.should.equal(40);
          evalshaNode.receive({ topic: sha1, payload: "0" });
        } catch (err) {
          done(err);
        }
      });

      scriptloadNode.receive({ payload: ["LOAD", "return 'scripted'"] });
    });
  });

  it("should SCRIPT EXISTS return 1 for a loaded script", function (done) {
    const flow = [
      configNode,
      {
        id: "scriptload-node",
        type: "redis-command",
        server: "config1",
        command: "SCRIPT",
        name: "SCRIPT_LOAD",
        topic: "",
        params: "[]",
        wires: [["scriptload-helper"]],
      },
      { id: "scriptload-helper", type: "helper" },
      {
        id: "scriptexists-node",
        type: "redis-command",
        server: "config1",
        command: "SCRIPT",
        name: "SCRIPT_EXISTS",
        topic: "",
        params: "[]",
        wires: [["scriptexists-helper"]],
      },
      { id: "scriptexists-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const scriptloadNode = helper.getNode("scriptload-node");
      const scriptloadHelper = helper.getNode("scriptload-helper");
      const scriptexistsNode = helper.getNode("scriptexists-node");
      const scriptexistsHelper = helper.getNode("scriptexists-helper");

      scriptexistsHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal(1);
          done();
        } catch (err) {
          done(err);
        }
      });

      scriptloadHelper.on("input", (msg) => {
        try {
          const sha = msg.payload;
          scriptexistsNode.receive({ payload: ["EXISTS", sha] });
        } catch (err) {
          done(err);
        }
      });

      scriptloadNode.receive({ payload: ["LOAD", "return 1"] });
    });
  });

  // ── redis-lua-script node: stored scripts (EVALSHA) and NOSCRIPT fallback ──

  it("redis-lua-script (stored) executes via EVALSHA and returns result", function (done) {
    const flow = [
      configNode,
      {
        id: "lua-node",
        type: "redis-lua-script",
        server: "config1",
        name: "LUA",
        func: "return 'stored-ok'",
        keyval: 0,
        stored: true,
        block: false,
        wires: [["lua-helper"]],
      },
      { id: "lua-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const luaNode = helper.getNode("lua-node");
      const luaHelper = helper.getNode("lua-helper");

      luaHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("stored-ok");
          // EVALSHA is the command used when the SHA1 is cached.
          luaNode.command.should.equal("evalsha");
          done();
        } catch (err) {
          done(err);
        }
      });

      waitForSha1(luaNode, (err) => {
        if (err) return done(err);
        luaNode.receive({ payload: [] });
      });
    });
  });

  it("redis-lua-script (stored) falls back to EVAL on NOSCRIPT", function (done) {
    const flow = [
      configNode,
      {
        id: "lua-node",
        type: "redis-lua-script",
        server: "config1",
        name: "LUA",
        func: "return 'fallback-ok'",
        keyval: 0,
        stored: true,
        block: false,
        wires: [["lua-helper"]],
      },
      { id: "lua-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const luaNode = helper.getNode("lua-node");
      const luaHelper = helper.getNode("lua-helper");

      luaHelper.on("input", (msg) => {
        try {
          // Even though EVALSHA failed with NOSCRIPT, the script still ran
          // because the node re-sent the body via EVAL.
          msg.payload.should.equal("fallback-ok");
          luaNode.command.should.equal("eval");
          done();
        } catch (err) {
          done(err);
        }
      });

      waitForSha1(luaNode, (err) => {
        if (err) return done(err);
        // Evict every cached script from Redis so the node's SHA1 is no longer
        // known — the next EVALSHA must return a NOSCRIPT error.
        const flushClient = directRedis();
        flushClient
          .script("flush")
          .then(() => {
            flushClient.disconnect();
            luaNode.receive({ payload: [] });
          })
          .catch((e) => {
            flushClient.disconnect();
            done(e);
          });
      });
    });
  });
});
