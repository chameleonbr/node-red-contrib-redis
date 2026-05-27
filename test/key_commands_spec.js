const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { cleanupKeys } = require("./helpers/cleanup");

helper.init(require.resolve("node-red"));

describe("Key commands", function () {
  this.timeout(5000);

  const configNode = {
    id: "config1",
    type: "redis-config",
    name: "Local",
    options: '{"host":"127.0.0.1","port":6379}',
    optionsType: "json",
    cluster: false,
  };

  beforeEach((done) => {
    helper.startServer(done);
  });

  afterEach((done) => {
    helper.unload().then(() => {
      helper.stopServer(() => {
        cleanupKeys("test:key:*", done);
      });
    });
  });

  it("should EXISTS return 1 for existing key and 0 for missing", function (done) {
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
        id: "exists-node",
        type: "redis-command",
        server: "config1",
        command: "EXISTS",
        name: "EXISTS",
        topic: "",
        params: "[]",
        wires: [["exists-helper"]],
      },
      { id: "exists-helper", type: "helper" },
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
      const existsNode = helper.getNode("exists-node");
      const existsHelper = helper.getNode("exists-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      existsHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          delNode.receive({ topic: "test:key:exists" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        existsNode.receive({ topic: "test:key:exists" });
      });

      setNode.receive({ topic: "test:key:exists", payload: "hello" });
    });
  });

  it("should TYPE return the type of a key", function (done) {
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
        id: "type-node",
        type: "redis-command",
        server: "config1",
        command: "TYPE",
        name: "TYPE",
        topic: "",
        params: "[]",
        wires: [["type-helper"]],
      },
      { id: "type-helper", type: "helper" },
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
      const typeNode = helper.getNode("type-node");
      const typeHelper = helper.getNode("type-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      typeHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("string");
          delNode.receive({ topic: "test:key:type" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        typeNode.receive({ topic: "test:key:type" });
      });

      setNode.receive({ topic: "test:key:type", payload: "hello" });
    });
  });

  it("should RENAME a key to a new name", function (done) {
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
        id: "rename-node",
        type: "redis-command",
        server: "config1",
        command: "RENAME",
        name: "RENAME",
        topic: "",
        params: "[]",
        wires: [["rename-helper"]],
      },
      { id: "rename-helper", type: "helper" },
      {
        id: "get-node",
        type: "redis-command",
        server: "config1",
        command: "GET",
        name: "GET",
        topic: "",
        params: "[]",
        wires: [["get-helper"]],
      },
      { id: "get-helper", type: "helper" },
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
      const renameNode = helper.getNode("rename-node");
      const renameHelper = helper.getNode("rename-helper");
      const getNode = helper.getNode("get-node");
      const getHelper = helper.getNode("get-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      getHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("hello");
          delNode.receive({ topic: "test:key:renamed" });
        } catch (err) {
          done(err);
        }
      });

      renameHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("OK");
          getNode.receive({ topic: "test:key:renamed" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        renameNode.receive({
          topic: "test:key:rename",
          payload: "test:key:renamed",
        });
      });

      setNode.receive({ topic: "test:key:rename", payload: "hello" });
    });
  });

  it("should RENAMENX rename only if new name does not exist", function (done) {
    const flow = [
      configNode,
      {
        id: "set1-node",
        type: "redis-command",
        server: "config1",
        command: "SET",
        name: "SET1",
        topic: "",
        params: "[]",
        wires: [["set1-helper"]],
      },
      { id: "set1-helper", type: "helper" },
      {
        id: "set2-node",
        type: "redis-command",
        server: "config1",
        command: "SET",
        name: "SET2",
        topic: "",
        params: "[]",
        wires: [["set2-helper"]],
      },
      { id: "set2-helper", type: "helper" },
      {
        id: "renamenx-node",
        type: "redis-command",
        server: "config1",
        command: "RENAMENX",
        name: "RENAMENX",
        topic: "",
        params: "[]",
        wires: [["renamenx-helper"]],
      },
      { id: "renamenx-helper", type: "helper" },
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
      const set1Node = helper.getNode("set1-node");
      const set1Helper = helper.getNode("set1-helper");
      const set2Node = helper.getNode("set2-node");
      const set2Helper = helper.getNode("set2-helper");
      const renamenxNode = helper.getNode("renamenx-node");
      const renamenxHelper = helper.getNode("renamenx-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      renamenxHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(0);
          delNode.receive({
            payload: ["test:key:rnxsrc", "test:key:rnxdst"],
          });
        } catch (err) {
          done(err);
        }
      });

      set2Helper.on("input", () => {
        renamenxNode.receive({
          topic: "test:key:rnxsrc",
          payload: "test:key:rnxdst",
        });
      });

      set1Helper.on("input", () => {
        set2Node.receive({ topic: "test:key:rnxdst", payload: "existing" });
      });

      set1Node.receive({ topic: "test:key:rnxsrc", payload: "hello" });
    });
  });

  it("should EXPIRE set TTL and TTL return remaining seconds", function (done) {
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
        id: "expire-node",
        type: "redis-command",
        server: "config1",
        command: "EXPIRE",
        name: "EXPIRE",
        topic: "",
        params: "[]",
        wires: [["expire-helper"]],
      },
      { id: "expire-helper", type: "helper" },
      {
        id: "ttl-node",
        type: "redis-command",
        server: "config1",
        command: "TTL",
        name: "TTL",
        topic: "",
        params: "[]",
        wires: [["ttl-helper"]],
      },
      { id: "ttl-helper", type: "helper" },
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
      const expireNode = helper.getNode("expire-node");
      const expireHelper = helper.getNode("expire-helper");
      const ttlNode = helper.getNode("ttl-node");
      const ttlHelper = helper.getNode("ttl-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      ttlHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.above(0);
          delNode.receive({ topic: "test:key:expire" });
        } catch (err) {
          done(err);
        }
      });

      expireHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          ttlNode.receive({ topic: "test:key:expire" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        expireNode.receive({ topic: "test:key:expire", payload: "100" });
      });

      setNode.receive({ topic: "test:key:expire", payload: "hello" });
    });
  });

  it("should EXPIREAT set TTL by Unix timestamp", function (done) {
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
        id: "expireat-node",
        type: "redis-command",
        server: "config1",
        command: "EXPIREAT",
        name: "EXPIREAT",
        topic: "",
        params: "[]",
        wires: [["expireat-helper"]],
      },
      { id: "expireat-helper", type: "helper" },
      {
        id: "ttl-node",
        type: "redis-command",
        server: "config1",
        command: "TTL",
        name: "TTL",
        topic: "",
        params: "[]",
        wires: [["ttl-helper"]],
      },
      { id: "ttl-helper", type: "helper" },
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
      const expireatNode = helper.getNode("expireat-node");
      const expireatHelper = helper.getNode("expireat-helper");
      const ttlNode = helper.getNode("ttl-node");
      const ttlHelper = helper.getNode("ttl-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      const futureTimestamp = Math.floor(Date.now() / 1000) + 3600;

      delHelper.on("input", () => {
        done();
      });

      ttlHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.above(0);
          delNode.receive({ topic: "test:key:expireat" });
        } catch (err) {
          done(err);
        }
      });

      expireatHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          ttlNode.receive({ topic: "test:key:expireat" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        expireatNode.receive({
          topic: "test:key:expireat",
          payload: String(futureTimestamp),
        });
      });

      setNode.receive({ topic: "test:key:expireat", payload: "hello" });
    });
  });

  it("should PEXPIRE set millisecond TTL and PTTL return remaining ms", function (done) {
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
        id: "pexpire-node",
        type: "redis-command",
        server: "config1",
        command: "PEXPIRE",
        name: "PEXPIRE",
        topic: "",
        params: "[]",
        wires: [["pexpire-helper"]],
      },
      { id: "pexpire-helper", type: "helper" },
      {
        id: "pttl-node",
        type: "redis-command",
        server: "config1",
        command: "PTTL",
        name: "PTTL",
        topic: "",
        params: "[]",
        wires: [["pttl-helper"]],
      },
      { id: "pttl-helper", type: "helper" },
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
      const pexpireNode = helper.getNode("pexpire-node");
      const pexpireHelper = helper.getNode("pexpire-helper");
      const pttlNode = helper.getNode("pttl-node");
      const pttlHelper = helper.getNode("pttl-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      pttlHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.above(0);
          delNode.receive({ topic: "test:key:pexpire" });
        } catch (err) {
          done(err);
        }
      });

      pexpireHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          pttlNode.receive({ topic: "test:key:pexpire" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        pexpireNode.receive({ topic: "test:key:pexpire", payload: "100000" });
      });

      setNode.receive({ topic: "test:key:pexpire", payload: "hello" });
    });
  });

  it("should PEXPIREAT set millisecond TTL by Unix timestamp", function (done) {
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
        id: "pexpireat-node",
        type: "redis-command",
        server: "config1",
        command: "PEXPIREAT",
        name: "PEXPIREAT",
        topic: "",
        params: "[]",
        wires: [["pexpireat-helper"]],
      },
      { id: "pexpireat-helper", type: "helper" },
      {
        id: "pttl-node",
        type: "redis-command",
        server: "config1",
        command: "PTTL",
        name: "PTTL",
        topic: "",
        params: "[]",
        wires: [["pttl-helper"]],
      },
      { id: "pttl-helper", type: "helper" },
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
      const pexpireatNode = helper.getNode("pexpireat-node");
      const pexpireatHelper = helper.getNode("pexpireat-helper");
      const pttlNode = helper.getNode("pttl-node");
      const pttlHelper = helper.getNode("pttl-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      const futureMs = Date.now() + 3600000;

      delHelper.on("input", () => {
        done();
      });

      pttlHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.above(0);
          delNode.receive({ topic: "test:key:pexpireat" });
        } catch (err) {
          done(err);
        }
      });

      pexpireatHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          pttlNode.receive({ topic: "test:key:pexpireat" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        pexpireatNode.receive({
          topic: "test:key:pexpireat",
          payload: String(futureMs),
        });
      });

      setNode.receive({ topic: "test:key:pexpireat", payload: "hello" });
    });
  });

  it("should PERSIST remove TTL and make key permanent", function (done) {
    const flow = [
      configNode,
      {
        id: "setex-node",
        type: "redis-command",
        server: "config1",
        command: "SETEX",
        name: "SETEX",
        topic: "",
        params: "[]",
        wires: [["setex-helper"]],
      },
      { id: "setex-helper", type: "helper" },
      {
        id: "persist-node",
        type: "redis-command",
        server: "config1",
        command: "PERSIST",
        name: "PERSIST",
        topic: "",
        params: "[]",
        wires: [["persist-helper"]],
      },
      { id: "persist-helper", type: "helper" },
      {
        id: "ttl-node",
        type: "redis-command",
        server: "config1",
        command: "TTL",
        name: "TTL",
        topic: "",
        params: "[]",
        wires: [["ttl-helper"]],
      },
      { id: "ttl-helper", type: "helper" },
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
      const setexNode = helper.getNode("setex-node");
      const setexHelper = helper.getNode("setex-helper");
      const persistNode = helper.getNode("persist-node");
      const persistHelper = helper.getNode("persist-helper");
      const ttlNode = helper.getNode("ttl-node");
      const ttlHelper = helper.getNode("ttl-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      ttlHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(-1);
          delNode.receive({ topic: "test:key:persist" });
        } catch (err) {
          done(err);
        }
      });

      persistHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          ttlNode.receive({ topic: "test:key:persist" });
        } catch (err) {
          done(err);
        }
      });

      setexHelper.on("input", () => {
        persistNode.receive({ topic: "test:key:persist" });
      });

      setexNode.receive({
        topic: "test:key:persist",
        payload: ["100", "hello"],
      });
    });
  });

  it("should TOUCH update access time and return number of touched keys", function (done) {
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
        id: "touch-node",
        type: "redis-command",
        server: "config1",
        command: "TOUCH",
        name: "TOUCH",
        topic: "",
        params: "[]",
        wires: [["touch-helper"]],
      },
      { id: "touch-helper", type: "helper" },
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
      const touchNode = helper.getNode("touch-node");
      const touchHelper = helper.getNode("touch-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      touchHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          delNode.receive({ topic: "test:key:touch" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        touchNode.receive({ topic: "test:key:touch" });
      });

      setNode.receive({ topic: "test:key:touch", payload: "hello" });
    });
  });

  it("should UNLINK asynchronously delete a key", function (done) {
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
        id: "unlink-node",
        type: "redis-command",
        server: "config1",
        command: "UNLINK",
        name: "UNLINK",
        topic: "",
        params: "[]",
        wires: [["unlink-helper"]],
      },
      { id: "unlink-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const setNode = helper.getNode("set-node");
      const setHelper = helper.getNode("set-helper");
      const unlinkNode = helper.getNode("unlink-node");
      const unlinkHelper = helper.getNode("unlink-helper");

      unlinkHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          done();
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        unlinkNode.receive({ topic: "test:key:unlink" });
      });

      setNode.receive({ topic: "test:key:unlink", payload: "hello" });
    });
  });

  it("should KEYS return matching key names", function (done) {
    const flow = [
      configNode,
      {
        id: "mset-node",
        type: "redis-command",
        server: "config1",
        command: "MSET",
        name: "MSET",
        topic: "",
        params: "[]",
        wires: [["mset-helper"]],
      },
      { id: "mset-helper", type: "helper" },
      {
        id: "keys-node",
        type: "redis-command",
        server: "config1",
        command: "KEYS",
        name: "KEYS",
        topic: "",
        params: "[]",
        wires: [["keys-helper"]],
      },
      { id: "keys-helper", type: "helper" },
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
      const msetNode = helper.getNode("mset-node");
      const msetHelper = helper.getNode("mset-helper");
      const keysNode = helper.getNode("keys-node");
      const keysHelper = helper.getNode("keys-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      keysHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.equal(2);
          delNode.receive({
            payload: ["test:key:keys:a", "test:key:keys:b"],
          });
        } catch (err) {
          done(err);
        }
      });

      msetHelper.on("input", () => {
        keysNode.receive({ payload: "test:key:keys:*" });
      });

      msetNode.receive({
        payload: ["test:key:keys:a", "1", "test:key:keys:b", "2"],
      });
    });
  });

  it("should SCAN iterate over keys with a cursor", function (done) {
    const flow = [
      configNode,
      {
        id: "scan-node",
        type: "redis-command",
        server: "config1",
        command: "SCAN",
        name: "SCAN",
        topic: "",
        params: "[]",
        wires: [["scan-helper"]],
      },
      { id: "scan-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const scanNode = helper.getNode("scan-node");
      const scanHelper = helper.getNode("scan-helper");

      scanHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.equal(2);
          msg.payload[1].should.be.an.Array();
          done();
        } catch (err) {
          done(err);
        }
      });

      scanNode.receive({ payload: "0" });
    });
  });

  it("should COPY duplicate a key to a new destination", function (done) {
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
        id: "copy-node",
        type: "redis-command",
        server: "config1",
        command: "COPY",
        name: "COPY",
        topic: "",
        params: "[]",
        wires: [["copy-helper"]],
      },
      { id: "copy-helper", type: "helper" },
      {
        id: "get-node",
        type: "redis-command",
        server: "config1",
        command: "GET",
        name: "GET",
        topic: "",
        params: "[]",
        wires: [["get-helper"]],
      },
      { id: "get-helper", type: "helper" },
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
      const copyNode = helper.getNode("copy-node");
      const copyHelper = helper.getNode("copy-helper");
      const getNode = helper.getNode("get-node");
      const getHelper = helper.getNode("get-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      getHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("hello");
          delNode.receive({
            payload: ["test:key:copysrc", "test:key:copydst"],
          });
        } catch (err) {
          done(err);
        }
      });

      copyHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          getNode.receive({ topic: "test:key:copydst" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        copyNode.receive({
          topic: "test:key:copysrc",
          payload: "test:key:copydst",
        });
      });

      setNode.receive({ topic: "test:key:copysrc", payload: "hello" });
    });
  });

  it("should SORT a list and return sorted elements", function (done) {
    const flow = [
      configNode,
      {
        id: "rpush-node",
        type: "redis-command",
        server: "config1",
        command: "RPUSH",
        name: "RPUSH",
        topic: "",
        params: "[]",
        wires: [["rpush-helper"]],
      },
      { id: "rpush-helper", type: "helper" },
      {
        id: "sort-node",
        type: "redis-command",
        server: "config1",
        command: "SORT",
        name: "SORT",
        topic: "",
        params: "[]",
        wires: [["sort-helper"]],
      },
      { id: "sort-helper", type: "helper" },
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
      const rpushNode = helper.getNode("rpush-node");
      const rpushHelper = helper.getNode("rpush-helper");
      const sortNode = helper.getNode("sort-node");
      const sortHelper = helper.getNode("sort-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      sortHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.eql(["1", "2", "3"]);
          delNode.receive({ topic: "test:key:sort" });
        } catch (err) {
          done(err);
        }
      });

      rpushHelper.on("input", () => {
        sortNode.receive({ topic: "test:key:sort" });
      });

      rpushNode.receive({
        topic: "test:key:sort",
        payload: ["3", "1", "2"],
      });
    });
  });

  it("should EXPIRETIME return the expiry as unix timestamp", function (done) {
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
        id: "expireat-node",
        type: "redis-command",
        server: "config1",
        command: "EXPIREAT",
        name: "EXPIREAT",
        topic: "",
        params: "[]",
        wires: [["expireat-helper"]],
      },
      { id: "expireat-helper", type: "helper" },
      {
        id: "expiretime-node",
        type: "redis-command",
        server: "config1",
        command: "EXPIRETIME",
        name: "EXPIRETIME",
        topic: "",
        params: "[]",
        wires: [["expiretime-helper"]],
      },
      { id: "expiretime-helper", type: "helper" },
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
      const expireatNode = helper.getNode("expireat-node");
      const expireatHelper = helper.getNode("expireat-helper");
      const expiretimeNode = helper.getNode("expiretime-node");
      const expiretimeHelper = helper.getNode("expiretime-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      const futureTs = Math.floor(Date.now() / 1000) + 315360000; // ~10 years

      delHelper.on("input", () => {
        done();
      });

      expiretimeHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.above(0);
          delNode.receive({ topic: "test:key:expiretime" });
        } catch (err) {
          done(err);
        }
      });

      expireatHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          expiretimeNode.receive({ topic: "test:key:expiretime" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        expireatNode.receive({
          topic: "test:key:expiretime",
          payload: String(futureTs),
        });
      });

      setNode.receive({ topic: "test:key:expiretime", payload: "v" });
    });
  });

  it("should PEXPIRETIME return the expiry as millisecond timestamp", function (done) {
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
        id: "pexpire-node",
        type: "redis-command",
        server: "config1",
        command: "PEXPIRE",
        name: "PEXPIRE",
        topic: "",
        params: "[]",
        wires: [["pexpire-helper"]],
      },
      { id: "pexpire-helper", type: "helper" },
      {
        id: "pexpiretime-node",
        type: "redis-command",
        server: "config1",
        command: "PEXPIRETIME",
        name: "PEXPIRETIME",
        topic: "",
        params: "[]",
        wires: [["pexpiretime-helper"]],
      },
      { id: "pexpiretime-helper", type: "helper" },
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
      const pexpireNode = helper.getNode("pexpire-node");
      const pexpireHelper = helper.getNode("pexpire-helper");
      const pexpiretimeNode = helper.getNode("pexpiretime-node");
      const pexpiretimeHelper = helper.getNode("pexpiretime-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      pexpiretimeHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.above(0);
          delNode.receive({ topic: "test:key:pexpiretime" });
        } catch (err) {
          done(err);
        }
      });

      pexpireHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          pexpiretimeNode.receive({ topic: "test:key:pexpiretime" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        pexpireNode.receive({
          topic: "test:key:pexpiretime",
          payload: "10000",
        });
      });

      setNode.receive({ topic: "test:key:pexpiretime", payload: "v" });
    });
  });

  it("should DUMP serialize a key and RESTORE deserialize it", function (done) {
    const Redis = require("ioredis");
    const flow = [
      configNode,
      {
        id: "restore-node",
        type: "redis-command",
        server: "config1",
        command: "RESTORE",
        name: "RESTORE",
        topic: "",
        params: "[]",
        wires: [["restore-helper"]],
      },
      { id: "restore-helper", type: "helper" },
      {
        id: "get-node",
        type: "redis-command",
        server: "config1",
        command: "GET",
        name: "GET",
        topic: "",
        params: "[]",
        wires: [["get-helper"]],
      },
      { id: "get-helper", type: "helper" },
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
      const restoreNode = helper.getNode("restore-node");
      const restoreHelper = helper.getNode("restore-helper");
      const getNode = helper.getNode("get-node");
      const getHelper = helper.getNode("get-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      getHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("hello");
          delNode.receive({
            payload: ["test:key:dump", "test:key:restored"],
          });
        } catch (err) {
          done(err);
        }
      });

      restoreHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("OK");
          getNode.receive({ topic: "test:key:restored" });
        } catch (err) {
          done(err);
        }
      });

      // Use a direct ioredis connection with buffer support to DUMP the key
      const directClient = new Redis({ host: "127.0.0.1", port: 6379 });
      directClient.set("test:key:dump", "hello").then(() => {
        return directClient.callBuffer("DUMP", "test:key:dump");
      }).then((dumpBuf) => {
        directClient.disconnect();
        restoreNode.receive({
          topic: "test:key:restored",
          payload: ["0", dumpBuf],
        });
      }).catch((err) => {
        directClient.disconnect();
        done(err);
      });
    });
  });

  it("should SORT_RO return sorted elements without modifying the list", function (done) {
    const flow = [
      configNode,
      {
        id: "rpush-node",
        type: "redis-command",
        server: "config1",
        command: "RPUSH",
        name: "RPUSH",
        topic: "",
        params: "[]",
        wires: [["rpush-helper"]],
      },
      { id: "rpush-helper", type: "helper" },
      {
        id: "sortro-node",
        type: "redis-command",
        server: "config1",
        command: "SORT_RO",
        name: "SORT_RO",
        topic: "",
        params: "[]",
        wires: [["sortro-helper"]],
      },
      { id: "sortro-helper", type: "helper" },
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
      const rpushNode = helper.getNode("rpush-node");
      const rpushHelper = helper.getNode("rpush-helper");
      const sortroNode = helper.getNode("sortro-node");
      const sortroHelper = helper.getNode("sortro-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      sortroHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.eql(["1", "2", "3"]);
          delNode.receive({ topic: "test:key:sortro" });
        } catch (err) {
          done(err);
        }
      });

      rpushHelper.on("input", () => {
        sortroNode.receive({ topic: "test:key:sortro" });
      });

      rpushNode.receive({
        topic: "test:key:sortro",
        payload: ["3", "1", "2"],
      });
    });
  });

  it("should RANDOMKEY return a random existing key", function (done) {
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
        id: "randomkey-node",
        type: "redis-command",
        server: "config1",
        command: "RANDOMKEY",
        name: "RANDOMKEY",
        topic: "",
        params: "[]",
        wires: [["randomkey-helper"]],
      },
      { id: "randomkey-helper", type: "helper" },
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
      const randomkeyNode = helper.getNode("randomkey-node");
      const randomkeyHelper = helper.getNode("randomkey-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      randomkeyHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.String();
          delNode.receive({ topic: "test:key:randomkey" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        randomkeyNode.receive({});
      });

      setNode.receive({ topic: "test:key:randomkey", payload: "hello" });
    });
  });
});
