const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { cleanupKeys } = require("./helpers/cleanup");

helper.init(require.resolve("node-red"));

describe("Hash commands", function () {
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
        cleanupKeys("test:hash:*", done);
      });
    });
  });

  it("should HSET multiple fields and HGET a single field", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hget-node",
        type: "redis-command",
        server: "config1",
        command: "HGET",
        name: "HGET",
        topic: "",
        params: "[]",
        wires: [["hget-helper"]],
      },
      { id: "hget-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hgetNode = helper.getNode("hget-node");
      const hgetHelper = helper.getNode("hget-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hgetHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("val1");
          delNode.receive({ topic: "test:hash:hset" });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          hgetNode.receive({ topic: "test:hash:hset", payload: "field1" });
        } catch (err) {
          done(err);
        }
      });

      hsetNode.receive({
        topic: "test:hash:hset",
        payload: ["field1", "val1", "field2", "val2"],
      });
    });
  });

  it("should HDEL remove fields and HEXISTS check presence", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hdel-node",
        type: "redis-command",
        server: "config1",
        command: "HDEL",
        name: "HDEL",
        topic: "",
        params: "[]",
        wires: [["hdel-helper"]],
      },
      { id: "hdel-helper", type: "helper" },
      {
        id: "hexists-node",
        type: "redis-command",
        server: "config1",
        command: "HEXISTS",
        name: "HEXISTS",
        topic: "",
        params: "[]",
        wires: [["hexists-helper"]],
      },
      { id: "hexists-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hdelNode = helper.getNode("hdel-node");
      const hdelHelper = helper.getNode("hdel-helper");
      const hexistsNode = helper.getNode("hexists-node");
      const hexistsHelper = helper.getNode("hexists-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hexistsHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(0);
          delNode.receive({ topic: "test:hash:hdel" });
        } catch (err) {
          done(err);
        }
      });

      hdelHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          hexistsNode.receive({ topic: "test:hash:hdel", payload: "field1" });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hdelNode.receive({ topic: "test:hash:hdel", payload: "field1" });
      });

      hsetNode.receive({
        topic: "test:hash:hdel",
        payload: ["field1", "val1"],
      });
    });
  });

  it("should HGETALL return all fields and values as an object", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hgetall-node",
        type: "redis-command",
        server: "config1",
        command: "HGETALL",
        name: "HGETALL",
        topic: "",
        params: "[]",
        wires: [["hgetall-helper"]],
      },
      { id: "hgetall-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hgetallNode = helper.getNode("hgetall-node");
      const hgetallHelper = helper.getNode("hgetall-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hgetallHelper.on("input", (msg) => {
        try {
          // client.call("HGETALL",...) bypasses ioredis reply transformer (case-sensitive
          // lookup), so the result is a flat array rather than an object
          msg.payload.should.be.an.Array();
          msg.payload.should.containEql("f1");
          msg.payload.should.containEql("v1");
          msg.payload.should.containEql("f2");
          msg.payload.should.containEql("v2");
          delNode.receive({ topic: "test:hash:hgetall" });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hgetallNode.receive({ topic: "test:hash:hgetall" });
      });

      hsetNode.receive({
        topic: "test:hash:hgetall",
        payload: ["f1", "v1", "f2", "v2"],
      });
    });
  });

  it("should HKEYS and HVALS return field names and values", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hkeys-node",
        type: "redis-command",
        server: "config1",
        command: "HKEYS",
        name: "HKEYS",
        topic: "",
        params: "[]",
        wires: [["hkeys-helper"]],
      },
      { id: "hkeys-helper", type: "helper" },
      {
        id: "hvals-node",
        type: "redis-command",
        server: "config1",
        command: "HVALS",
        name: "HVALS",
        topic: "",
        params: "[]",
        wires: [["hvals-helper"]],
      },
      { id: "hvals-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hkeysNode = helper.getNode("hkeys-node");
      const hkeysHelper = helper.getNode("hkeys-helper");
      const hvalsNode = helper.getNode("hvals-node");
      const hvalsHelper = helper.getNode("hvals-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hvalsHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.containEql("v1");
          msg.payload.should.containEql("v2");
          delNode.receive({ topic: "test:hash:hkv" });
        } catch (err) {
          done(err);
        }
      });

      hkeysHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.containEql("f1");
          msg.payload.should.containEql("f2");
          hvalsNode.receive({ topic: "test:hash:hkv" });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hkeysNode.receive({ topic: "test:hash:hkv" });
      });

      hsetNode.receive({
        topic: "test:hash:hkv",
        payload: ["f1", "v1", "f2", "v2"],
      });
    });
  });

  it("should HMGET return values for specified fields", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hmget-node",
        type: "redis-command",
        server: "config1",
        command: "HMGET",
        name: "HMGET",
        topic: "",
        params: "[]",
        wires: [["hmget-helper"]],
      },
      { id: "hmget-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hmgetNode = helper.getNode("hmget-node");
      const hmgetHelper = helper.getNode("hmget-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hmgetHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.eql(["v1", "v2"]);
          delNode.receive({ topic: "test:hash:hmget" });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hmgetNode.receive({
          payload: ["test:hash:hmget", "f1", "f2"],
        });
      });

      hsetNode.receive({
        topic: "test:hash:hmget",
        payload: ["f1", "v1", "f2", "v2"],
      });
    });
  });

  it("should HLEN return number of fields in a hash", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hlen-node",
        type: "redis-command",
        server: "config1",
        command: "HLEN",
        name: "HLEN",
        topic: "",
        params: "[]",
        wires: [["hlen-helper"]],
      },
      { id: "hlen-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hlenNode = helper.getNode("hlen-node");
      const hlenHelper = helper.getNode("hlen-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hlenHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(3);
          delNode.receive({ topic: "test:hash:hlen" });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hlenNode.receive({ topic: "test:hash:hlen" });
      });

      hsetNode.receive({
        topic: "test:hash:hlen",
        payload: ["f1", "v1", "f2", "v2", "f3", "v3"],
      });
    });
  });

  it("should HINCRBY increment a hash field by integer", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hincrby-node",
        type: "redis-command",
        server: "config1",
        command: "HINCRBY",
        name: "HINCRBY",
        topic: "",
        params: "[]",
        wires: [["hincrby-helper"]],
      },
      { id: "hincrby-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hincrbyNode = helper.getNode("hincrby-node");
      const hincrbyHelper = helper.getNode("hincrby-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hincrbyHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(15);
          delNode.receive({ topic: "test:hash:hincrby" });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hincrbyNode.receive({
          topic: "test:hash:hincrby",
          payload: ["counter", "5"],
        });
      });

      hsetNode.receive({
        topic: "test:hash:hincrby",
        payload: ["counter", "10"],
      });
    });
  });

  it("should HINCRBYFLOAT increment a hash field by float", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hincrbyfloat-node",
        type: "redis-command",
        server: "config1",
        command: "HINCRBYFLOAT",
        name: "HINCRBYFLOAT",
        topic: "",
        params: "[]",
        wires: [["hincrbyfloat-helper"]],
      },
      { id: "hincrbyfloat-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hincrbyfloatNode = helper.getNode("hincrbyfloat-node");
      const hincrbyfloatHelper = helper.getNode("hincrbyfloat-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hincrbyfloatHelper.on("input", (msg) => {
        try {
          parseFloat(msg.payload).should.be.approximately(3.8, 0.001);
          delNode.receive({ topic: "test:hash:hincrbyfloat" });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hincrbyfloatNode.receive({
          topic: "test:hash:hincrbyfloat",
          payload: ["score", "2.3"],
        });
      });

      hsetNode.receive({
        topic: "test:hash:hincrbyfloat",
        payload: ["score", "1.5"],
      });
    });
  });

  it("should HSETNX set field only when it does not exist", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hsetnx-node",
        type: "redis-command",
        server: "config1",
        command: "HSETNX",
        name: "HSETNX",
        topic: "",
        params: "[]",
        wires: [["hsetnx-helper"]],
      },
      { id: "hsetnx-helper", type: "helper" },
      {
        id: "hsetnx2-node",
        type: "redis-command",
        server: "config1",
        command: "HSETNX",
        name: "HSETNX2",
        topic: "",
        params: "[]",
        wires: [["hsetnx2-helper"]],
      },
      { id: "hsetnx2-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hsetnxNode = helper.getNode("hsetnx-node");
      const hsetnxHelper = helper.getNode("hsetnx-helper");
      const hsetnx2Node = helper.getNode("hsetnx2-node");
      const hsetnx2Helper = helper.getNode("hsetnx2-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hsetnx2Helper.on("input", (msg) => {
        try {
          msg.payload.should.equal(0);
          delNode.receive({ topic: "test:hash:hsetnx" });
        } catch (err) {
          done(err);
        }
      });

      hsetnxHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          hsetnx2Node.receive({
            topic: "test:hash:hsetnx",
            payload: ["newfield", "newval"],
          });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hsetnxNode.receive({
          topic: "test:hash:hsetnx",
          payload: ["newfield", "newval"],
        });
      });

      hsetNode.receive({
        topic: "test:hash:hsetnx",
        payload: ["existing", "value"],
      });
    });
  });

  it("should HRANDFIELD return one or more random fields", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hrandfield-node",
        type: "redis-command",
        server: "config1",
        command: "HRANDFIELD",
        name: "HRANDFIELD",
        topic: "",
        params: "[]",
        wires: [["hrandfield-helper"]],
      },
      { id: "hrandfield-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hrandfieldNode = helper.getNode("hrandfield-node");
      const hrandfieldHelper = helper.getNode("hrandfield-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hrandfieldHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.String();
          delNode.receive({ topic: "test:hash:hrandfield" });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hrandfieldNode.receive({ topic: "test:hash:hrandfield" });
      });

      hsetNode.receive({
        topic: "test:hash:hrandfield",
        payload: ["f1", "v1", "f2", "v2", "f3", "v3"],
      });
    });
  });

  it("should HSTRLEN return the string length of a hash field value", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hstrlen-node",
        type: "redis-command",
        server: "config1",
        command: "HSTRLEN",
        name: "HSTRLEN",
        topic: "",
        params: "[]",
        wires: [["hstrlen-helper"]],
      },
      { id: "hstrlen-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hstrlenNode = helper.getNode("hstrlen-node");
      const hstrlenHelper = helper.getNode("hstrlen-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hstrlenHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(5);
          delNode.receive({ topic: "test:hash:hstrlen" });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hstrlenNode.receive({ topic: "test:hash:hstrlen", payload: "f1" });
      });

      hsetNode.receive({
        topic: "test:hash:hstrlen",
        payload: ["f1", "hello"],
      });
    });
  });

  it("should HEXPIRE set field expiry and HTTL return remaining seconds", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hexpire-node",
        type: "redis-command",
        server: "config1",
        command: "HEXPIRE",
        name: "HEXPIRE",
        topic: "",
        params: "[]",
        wires: [["hexpire-helper"]],
      },
      { id: "hexpire-helper", type: "helper" },
      {
        id: "httl-node",
        type: "redis-command",
        server: "config1",
        command: "HTTL",
        name: "HTTL",
        topic: "",
        params: "[]",
        wires: [["httl-helper"]],
      },
      { id: "httl-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hexpireNode = helper.getNode("hexpire-node");
      const hexpireHelper = helper.getNode("hexpire-helper");
      const httlNode = helper.getNode("httl-node");
      const httlHelper = helper.getNode("httl-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      httlHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.be.above(0);
          delNode.receive({ topic: "test:hash:hexpire" });
        } catch (err) {
          done(err);
        }
      });

      hexpireHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal(1);
          httlNode.receive({
            topic: "test:hash:hexpire",
            payload: ["FIELDS", "1", "f1"],
          });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hexpireNode.receive({
          topic: "test:hash:hexpire",
          payload: ["100", "FIELDS", "1", "f1"],
        });
      });

      hsetNode.receive({
        topic: "test:hash:hexpire",
        payload: ["f1", "v1"],
      });
    });
  });

  it("should HPEXPIRE set field ms expiry and HPTTL return remaining ms", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hpexpire-node",
        type: "redis-command",
        server: "config1",
        command: "HPEXPIRE",
        name: "HPEXPIRE",
        topic: "",
        params: "[]",
        wires: [["hpexpire-helper"]],
      },
      { id: "hpexpire-helper", type: "helper" },
      {
        id: "hpttl-node",
        type: "redis-command",
        server: "config1",
        command: "HPTTL",
        name: "HPTTL",
        topic: "",
        params: "[]",
        wires: [["hpttl-helper"]],
      },
      { id: "hpttl-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hpexpireNode = helper.getNode("hpexpire-node");
      const hpexpireHelper = helper.getNode("hpexpire-helper");
      const hpttlNode = helper.getNode("hpttl-node");
      const hpttlHelper = helper.getNode("hpttl-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hpttlHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.be.above(0);
          delNode.receive({ topic: "test:hash:hpexpire" });
        } catch (err) {
          done(err);
        }
      });

      hpexpireHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal(1);
          hpttlNode.receive({
            topic: "test:hash:hpexpire",
            payload: ["FIELDS", "1", "f1"],
          });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hpexpireNode.receive({
          topic: "test:hash:hpexpire",
          payload: ["100000", "FIELDS", "1", "f1"],
        });
      });

      hsetNode.receive({
        topic: "test:hash:hpexpire",
        payload: ["f1", "v1"],
      });
    });
  });

  it("should HPERSIST remove field expiry and HTTL return -1", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hexpire-node",
        type: "redis-command",
        server: "config1",
        command: "HEXPIRE",
        name: "HEXPIRE",
        topic: "",
        params: "[]",
        wires: [["hexpire-helper"]],
      },
      { id: "hexpire-helper", type: "helper" },
      {
        id: "hpersist-node",
        type: "redis-command",
        server: "config1",
        command: "HPERSIST",
        name: "HPERSIST",
        topic: "",
        params: "[]",
        wires: [["hpersist-helper"]],
      },
      { id: "hpersist-helper", type: "helper" },
      {
        id: "httl-node",
        type: "redis-command",
        server: "config1",
        command: "HTTL",
        name: "HTTL",
        topic: "",
        params: "[]",
        wires: [["httl-helper"]],
      },
      { id: "httl-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hexpireNode = helper.getNode("hexpire-node");
      const hexpireHelper = helper.getNode("hexpire-helper");
      const hpersistNode = helper.getNode("hpersist-node");
      const hpersistHelper = helper.getNode("hpersist-helper");
      const httlNode = helper.getNode("httl-node");
      const httlHelper = helper.getNode("httl-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      httlHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal(-1);
          delNode.receive({ topic: "test:hash:hpersist" });
        } catch (err) {
          done(err);
        }
      });

      hpersistHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal(1);
          httlNode.receive({
            topic: "test:hash:hpersist",
            payload: ["FIELDS", "1", "f1"],
          });
        } catch (err) {
          done(err);
        }
      });

      hexpireHelper.on("input", () => {
        hpersistNode.receive({
          topic: "test:hash:hpersist",
          payload: ["FIELDS", "1", "f1"],
        });
      });

      hsetHelper.on("input", () => {
        hexpireNode.receive({
          topic: "test:hash:hpersist",
          payload: ["100", "FIELDS", "1", "f1"],
        });
      });

      hsetNode.receive({
        topic: "test:hash:hpersist",
        payload: ["f1", "v1"],
      });
    });
  });

  it("should HGETDEL return field values and delete them", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hgetdel-node",
        type: "redis-command",
        server: "config1",
        command: "HGETDEL",
        name: "HGETDEL",
        topic: "",
        params: "[]",
        wires: [["hgetdel-helper"]],
      },
      { id: "hgetdel-helper", type: "helper" },
      {
        id: "hlen-node",
        type: "redis-command",
        server: "config1",
        command: "HLEN",
        name: "HLEN",
        topic: "",
        params: "[]",
        wires: [["hlen-helper"]],
      },
      { id: "hlen-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hgetdelNode = helper.getNode("hgetdel-node");
      const hgetdelHelper = helper.getNode("hgetdel-helper");
      const hlenNode = helper.getNode("hlen-node");
      const hlenHelper = helper.getNode("hlen-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hlenHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(0);
          delNode.receive({ topic: "test:hash:hgetdel" });
        } catch (err) {
          done(err);
        }
      });

      hgetdelHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.eql(["v1", "v2"]);
          hlenNode.receive({ topic: "test:hash:hgetdel" });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hgetdelNode.receive({
          topic: "test:hash:hgetdel",
          payload: ["FIELDS", "2", "f1", "f2"],
        });
      });

      hsetNode.receive({
        topic: "test:hash:hgetdel",
        payload: ["f1", "v1", "f2", "v2"],
      });
    });
  });

  it("should HGETEX return field values with PERSIST option", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hgetex-node",
        type: "redis-command",
        server: "config1",
        command: "HGETEX",
        name: "HGETEX",
        topic: "",
        params: "[]",
        wires: [["hgetex-helper"]],
      },
      { id: "hgetex-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hgetexNode = helper.getNode("hgetex-node");
      const hgetexHelper = helper.getNode("hgetex-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hgetexHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.eql(["v1"]);
          delNode.receive({ topic: "test:hash:hgetex" });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hgetexNode.receive({
          topic: "test:hash:hgetex",
          payload: ["PERSIST", "FIELDS", "1", "f1"],
        });
      });

      hsetNode.receive({
        topic: "test:hash:hgetex",
        payload: ["f1", "v1"],
      });
    });
  });

  it("should HSETEX set fields with expiry and HGET retrieve them", function (done) {
    const flow = [
      configNode,
      {
        id: "hsetex-node",
        type: "redis-command",
        server: "config1",
        command: "HSETEX",
        name: "HSETEX",
        topic: "",
        params: "[]",
        wires: [["hsetex-helper"]],
      },
      { id: "hsetex-helper", type: "helper" },
      {
        id: "hget-node",
        type: "redis-command",
        server: "config1",
        command: "HGET",
        name: "HGET",
        topic: "",
        params: "[]",
        wires: [["hget-helper"]],
      },
      { id: "hget-helper", type: "helper" },
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
      const hsetexNode = helper.getNode("hsetex-node");
      const hsetexHelper = helper.getNode("hsetex-helper");
      const hgetNode = helper.getNode("hget-node");
      const hgetHelper = helper.getNode("hget-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hgetHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("v1");
          delNode.receive({ topic: "test:hash:hsetex" });
        } catch (err) {
          done(err);
        }
      });

      hsetexHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          hgetNode.receive({ topic: "test:hash:hsetex", payload: "f1" });
        } catch (err) {
          done(err);
        }
      });

      hsetexNode.receive({
        topic: "test:hash:hsetex",
        payload: ["EX", "10", "FIELDS", "1", "f1", "v1"],
      });
    });
  });

  it("should HSCAN iterate over hash fields and values", function (done) {
    const flow = [
      configNode,
      {
        id: "hset-node",
        type: "redis-command",
        server: "config1",
        command: "HSET",
        name: "HSET",
        topic: "",
        params: "[]",
        wires: [["hset-helper"]],
      },
      { id: "hset-helper", type: "helper" },
      {
        id: "hscan-node",
        type: "redis-command",
        server: "config1",
        command: "HSCAN",
        name: "HSCAN",
        topic: "",
        params: "[]",
        wires: [["hscan-helper"]],
      },
      { id: "hscan-helper", type: "helper" },
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
      const hsetNode = helper.getNode("hset-node");
      const hsetHelper = helper.getNode("hset-helper");
      const hscanNode = helper.getNode("hscan-node");
      const hscanHelper = helper.getNode("hscan-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      hscanHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.equal(2);
          msg.payload[1].should.be.an.Array();
          delNode.receive({ topic: "test:hash:hscan" });
        } catch (err) {
          done(err);
        }
      });

      hsetHelper.on("input", () => {
        hscanNode.receive({ topic: "test:hash:hscan", payload: "0" });
      });

      hsetNode.receive({
        topic: "test:hash:hscan",
        payload: ["f1", "v1", "f2", "v2"],
      });
    });
  });
});
