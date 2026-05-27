const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { cleanupKeys } = require("./helpers/cleanup");

helper.init(require.resolve("node-red"));

describe("List commands", function () {
  this.timeout(8000);

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
        cleanupKeys("test:list:*", done);
      });
    });
  });

  it("should LPUSH elements and LRANGE retrieve all elements", function (done) {
    const flow = [
      configNode,
      {
        id: "lpush-node",
        type: "redis-command",
        server: "config1",
        command: "LPUSH",
        name: "LPUSH",
        topic: "",
        params: "[]",
        wires: [["lpush-helper"]],
      },
      { id: "lpush-helper", type: "helper" },
      {
        id: "lrange-node",
        type: "redis-command",
        server: "config1",
        command: "LRANGE",
        name: "LRANGE",
        topic: "",
        params: "[]",
        wires: [["lrange-helper"]],
      },
      { id: "lrange-helper", type: "helper" },
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
      const lpushNode = helper.getNode("lpush-node");
      const lpushHelper = helper.getNode("lpush-helper");
      const lrangeNode = helper.getNode("lrange-node");
      const lrangeHelper = helper.getNode("lrange-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      lrangeHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.eql(["c", "b", "a"]);
          delNode.receive({ topic: "test:list:lpush" });
        } catch (err) {
          done(err);
        }
      });

      lpushHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(3);
          lrangeNode.receive({ topic: "test:list:lpush", payload: ["0", "-1"] });
        } catch (err) {
          done(err);
        }
      });

      lpushNode.receive({
        topic: "test:list:lpush",
        payload: ["a", "b", "c"],
      });
    });
  });

  it("should RPUSH elements and RPOP remove from right", function (done) {
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
        id: "rpop-node",
        type: "redis-command",
        server: "config1",
        command: "RPOP",
        name: "RPOP",
        topic: "",
        params: "[]",
        wires: [["rpop-helper"]],
      },
      { id: "rpop-helper", type: "helper" },
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
      const rpopNode = helper.getNode("rpop-node");
      const rpopHelper = helper.getNode("rpop-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      rpopHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("c");
          delNode.receive({ topic: "test:list:rpush" });
        } catch (err) {
          done(err);
        }
      });

      rpushHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(3);
          rpopNode.receive({ topic: "test:list:rpush" });
        } catch (err) {
          done(err);
        }
      });

      rpushNode.receive({
        topic: "test:list:rpush",
        payload: ["a", "b", "c"],
      });
    });
  });

  it("should LPUSHX and RPUSHX push only to existing lists", function (done) {
    const flow = [
      configNode,
      {
        id: "lpush-node",
        type: "redis-command",
        server: "config1",
        command: "LPUSH",
        name: "LPUSH",
        topic: "",
        params: "[]",
        wires: [["lpush-helper"]],
      },
      { id: "lpush-helper", type: "helper" },
      {
        id: "lpushx-node",
        type: "redis-command",
        server: "config1",
        command: "LPUSHX",
        name: "LPUSHX",
        topic: "",
        params: "[]",
        wires: [["lpushx-helper"]],
      },
      { id: "lpushx-helper", type: "helper" },
      {
        id: "rpushx-node",
        type: "redis-command",
        server: "config1",
        command: "RPUSHX",
        name: "RPUSHX",
        topic: "",
        params: "[]",
        wires: [["rpushx-helper"]],
      },
      { id: "rpushx-helper", type: "helper" },
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
      const lpushNode = helper.getNode("lpush-node");
      const lpushHelper = helper.getNode("lpush-helper");
      const lpushxNode = helper.getNode("lpushx-node");
      const lpushxHelper = helper.getNode("lpushx-helper");
      const rpushxNode = helper.getNode("rpushx-node");
      const rpushxHelper = helper.getNode("rpushx-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      rpushxHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(0);
          delNode.receive({ topic: "test:list:lpushx" });
        } catch (err) {
          done(err);
        }
      });

      lpushxHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.above(0);
          rpushxNode.receive({
            topic: "test:list:nonexistent:lpushx",
            payload: "val",
          });
        } catch (err) {
          done(err);
        }
      });

      lpushHelper.on("input", () => {
        lpushxNode.receive({
          topic: "test:list:lpushx",
          payload: "prepended",
        });
      });

      lpushNode.receive({ topic: "test:list:lpushx", payload: "a" });
    });
  });

  it("should LPOP remove and return head element", function (done) {
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
        id: "lpop-node",
        type: "redis-command",
        server: "config1",
        command: "LPOP",
        name: "LPOP",
        topic: "",
        params: "[]",
        wires: [["lpop-helper"]],
      },
      { id: "lpop-helper", type: "helper" },
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
      const lpopNode = helper.getNode("lpop-node");
      const lpopHelper = helper.getNode("lpop-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      lpopHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("a");
          delNode.receive({ topic: "test:list:lpop" });
        } catch (err) {
          done(err);
        }
      });

      rpushHelper.on("input", () => {
        lpopNode.receive({ topic: "test:list:lpop" });
      });

      rpushNode.receive({
        topic: "test:list:lpop",
        payload: ["a", "b", "c"],
      });
    });
  });

  it("should LLEN return the number of elements in a list", function (done) {
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
        id: "llen-node",
        type: "redis-command",
        server: "config1",
        command: "LLEN",
        name: "LLEN",
        topic: "",
        params: "[]",
        wires: [["llen-helper"]],
      },
      { id: "llen-helper", type: "helper" },
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
      const llenNode = helper.getNode("llen-node");
      const llenHelper = helper.getNode("llen-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      llenHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(3);
          delNode.receive({ topic: "test:list:llen" });
        } catch (err) {
          done(err);
        }
      });

      rpushHelper.on("input", () => {
        llenNode.receive({ topic: "test:list:llen" });
      });

      rpushNode.receive({
        topic: "test:list:llen",
        payload: ["a", "b", "c"],
      });
    });
  });

  it("should LINDEX return element at a given position", function (done) {
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
        id: "lindex-node",
        type: "redis-command",
        server: "config1",
        command: "LINDEX",
        name: "LINDEX",
        topic: "",
        params: "[]",
        wires: [["lindex-helper"]],
      },
      { id: "lindex-helper", type: "helper" },
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
      const lindexNode = helper.getNode("lindex-node");
      const lindexHelper = helper.getNode("lindex-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      lindexHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("b");
          delNode.receive({ topic: "test:list:lindex" });
        } catch (err) {
          done(err);
        }
      });

      rpushHelper.on("input", () => {
        lindexNode.receive({ topic: "test:list:lindex", payload: "1" });
      });

      rpushNode.receive({
        topic: "test:list:lindex",
        payload: ["a", "b", "c"],
      });
    });
  });

  it("should LSET overwrite an element at a specific index", function (done) {
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
        id: "lset-node",
        type: "redis-command",
        server: "config1",
        command: "LSET",
        name: "LSET",
        topic: "",
        params: "[]",
        wires: [["lset-helper"]],
      },
      { id: "lset-helper", type: "helper" },
      {
        id: "lindex-node",
        type: "redis-command",
        server: "config1",
        command: "LINDEX",
        name: "LINDEX",
        topic: "",
        params: "[]",
        wires: [["lindex-helper"]],
      },
      { id: "lindex-helper", type: "helper" },
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
      const lsetNode = helper.getNode("lset-node");
      const lsetHelper = helper.getNode("lset-helper");
      const lindexNode = helper.getNode("lindex-node");
      const lindexHelper = helper.getNode("lindex-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      lindexHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("X");
          delNode.receive({ topic: "test:list:lset" });
        } catch (err) {
          done(err);
        }
      });

      lsetHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("OK");
          lindexNode.receive({ topic: "test:list:lset", payload: "1" });
        } catch (err) {
          done(err);
        }
      });

      rpushHelper.on("input", () => {
        lsetNode.receive({ topic: "test:list:lset", payload: ["1", "X"] });
      });

      rpushNode.receive({
        topic: "test:list:lset",
        payload: ["a", "b", "c"],
      });
    });
  });

  it("should LINSERT add element before a pivot", function (done) {
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
        id: "linsert-node",
        type: "redis-command",
        server: "config1",
        command: "LINSERT",
        name: "LINSERT",
        topic: "",
        params: "[]",
        wires: [["linsert-helper"]],
      },
      { id: "linsert-helper", type: "helper" },
      {
        id: "lrange-node",
        type: "redis-command",
        server: "config1",
        command: "LRANGE",
        name: "LRANGE",
        topic: "",
        params: "[]",
        wires: [["lrange-helper"]],
      },
      { id: "lrange-helper", type: "helper" },
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
      const linsertNode = helper.getNode("linsert-node");
      const linsertHelper = helper.getNode("linsert-helper");
      const lrangeNode = helper.getNode("lrange-node");
      const lrangeHelper = helper.getNode("lrange-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      lrangeHelper.on("input", (msg) => {
        try {
          msg.payload.should.eql(["a", "X", "b"]);
          delNode.receive({ topic: "test:list:linsert" });
        } catch (err) {
          done(err);
        }
      });

      linsertHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(3);
          lrangeNode.receive({
            topic: "test:list:linsert",
            payload: ["0", "-1"],
          });
        } catch (err) {
          done(err);
        }
      });

      rpushHelper.on("input", () => {
        linsertNode.receive({
          topic: "test:list:linsert",
          payload: ["BEFORE", "b", "X"],
        });
      });

      rpushNode.receive({ topic: "test:list:linsert", payload: ["a", "b"] });
    });
  });

  it("should LREM remove occurrences of an element", function (done) {
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
        id: "lrem-node",
        type: "redis-command",
        server: "config1",
        command: "LREM",
        name: "LREM",
        topic: "",
        params: "[]",
        wires: [["lrem-helper"]],
      },
      { id: "lrem-helper", type: "helper" },
      {
        id: "llen-node",
        type: "redis-command",
        server: "config1",
        command: "LLEN",
        name: "LLEN",
        topic: "",
        params: "[]",
        wires: [["llen-helper"]],
      },
      { id: "llen-helper", type: "helper" },
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
      const lremNode = helper.getNode("lrem-node");
      const lremHelper = helper.getNode("lrem-helper");
      const llenNode = helper.getNode("llen-node");
      const llenHelper = helper.getNode("llen-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      llenHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          delNode.receive({ topic: "test:list:lrem" });
        } catch (err) {
          done(err);
        }
      });

      lremHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          llenNode.receive({ topic: "test:list:lrem" });
        } catch (err) {
          done(err);
        }
      });

      rpushHelper.on("input", () => {
        lremNode.receive({
          topic: "test:list:lrem",
          payload: ["2", "a"],
        });
      });

      rpushNode.receive({
        topic: "test:list:lrem",
        payload: ["a", "b", "a", "c"],
      });
    });
  });

  it("should LTRIM trim list to specified range", function (done) {
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
        id: "ltrim-node",
        type: "redis-command",
        server: "config1",
        command: "LTRIM",
        name: "LTRIM",
        topic: "",
        params: "[]",
        wires: [["ltrim-helper"]],
      },
      { id: "ltrim-helper", type: "helper" },
      {
        id: "lrange-node",
        type: "redis-command",
        server: "config1",
        command: "LRANGE",
        name: "LRANGE",
        topic: "",
        params: "[]",
        wires: [["lrange-helper"]],
      },
      { id: "lrange-helper", type: "helper" },
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
      const ltrimNode = helper.getNode("ltrim-node");
      const ltrimHelper = helper.getNode("ltrim-helper");
      const lrangeNode = helper.getNode("lrange-node");
      const lrangeHelper = helper.getNode("lrange-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      lrangeHelper.on("input", (msg) => {
        try {
          msg.payload.should.eql(["a", "b"]);
          delNode.receive({ topic: "test:list:ltrim" });
        } catch (err) {
          done(err);
        }
      });

      ltrimHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("OK");
          lrangeNode.receive({
            topic: "test:list:ltrim",
            payload: ["0", "-1"],
          });
        } catch (err) {
          done(err);
        }
      });

      rpushHelper.on("input", () => {
        ltrimNode.receive({ topic: "test:list:ltrim", payload: ["0", "1"] });
      });

      rpushNode.receive({
        topic: "test:list:ltrim",
        payload: ["a", "b", "c", "d"],
      });
    });
  });

  it("should LPOS return index of a matching element", function (done) {
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
        id: "lpos-node",
        type: "redis-command",
        server: "config1",
        command: "LPOS",
        name: "LPOS",
        topic: "",
        params: "[]",
        wires: [["lpos-helper"]],
      },
      { id: "lpos-helper", type: "helper" },
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
      const lposNode = helper.getNode("lpos-node");
      const lposHelper = helper.getNode("lpos-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      lposHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          delNode.receive({ topic: "test:list:lpos" });
        } catch (err) {
          done(err);
        }
      });

      rpushHelper.on("input", () => {
        lposNode.receive({ topic: "test:list:lpos", payload: "c" });
      });

      rpushNode.receive({
        topic: "test:list:lpos",
        payload: ["a", "b", "c", "d"],
      });
    });
  });

  it("should LMOVE move an element between two lists", function (done) {
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
        id: "lmove-node",
        type: "redis-command",
        server: "config1",
        command: "LMOVE",
        name: "LMOVE",
        topic: "",
        params: "[]",
        wires: [["lmove-helper"]],
      },
      { id: "lmove-helper", type: "helper" },
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
      const lmoveNode = helper.getNode("lmove-node");
      const lmoveHelper = helper.getNode("lmove-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      lmoveHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("a");
          delNode.receive({
            payload: ["test:list:lmovesrc", "test:list:lmovedst"],
          });
        } catch (err) {
          done(err);
        }
      });

      rpushHelper.on("input", () => {
        lmoveNode.receive({
          topic: "test:list:lmovesrc",
          payload: ["test:list:lmovedst", "LEFT", "RIGHT"],
        });
      });

      rpushNode.receive({
        topic: "test:list:lmovesrc",
        payload: ["a", "b"],
      });
    });
  });

  it("should RPOPLPUSH move tail of source to head of dest (deprecated)", function (done) {
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
        id: "rpoplpush-node",
        type: "redis-command",
        server: "config1",
        command: "RPOPLPUSH",
        name: "RPOPLPUSH",
        topic: "",
        params: "[]",
        wires: [["rpoplpush-helper"]],
      },
      { id: "rpoplpush-helper", type: "helper" },
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
      const rpoplpushNode = helper.getNode("rpoplpush-node");
      const rpoplpushHelper = helper.getNode("rpoplpush-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      rpoplpushHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("b");
          delNode.receive({
            payload: ["test:list:rpoplpushsrc", "test:list:rpoplpushdst"],
          });
        } catch (err) {
          done(err);
        }
      });

      rpushHelper.on("input", () => {
        rpoplpushNode.receive({
          topic: "test:list:rpoplpushsrc",
          payload: "test:list:rpoplpushdst",
        });
      });

      rpushNode.receive({
        topic: "test:list:rpoplpushsrc",
        payload: ["a", "b"],
      });
    });
  });

  it("should BLPOP return immediately when list has data", function (done) {
    const flow = [
      configNode,
      {
        id: "lpush-node",
        type: "redis-command",
        server: "config1",
        command: "LPUSH",
        name: "LPUSH",
        topic: "",
        params: "[]",
        wires: [["lpush-helper"]],
      },
      { id: "lpush-helper", type: "helper" },
      {
        id: "blpop-node",
        type: "redis-command",
        server: "config1",
        command: "BLPOP",
        name: "BLPOP",
        block: true,
        topic: "",
        params: "[]",
        wires: [["blpop-helper"]],
      },
      { id: "blpop-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const lpushNode = helper.getNode("lpush-node");
      const lpushHelper = helper.getNode("lpush-helper");
      const blpopNode = helper.getNode("blpop-node");
      const blpopHelper = helper.getNode("blpop-helper");

      blpopHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal("test:list:blpop");
          msg.payload[1].should.equal("myvalue");
          done();
        } catch (err) {
          done(err);
        }
      });

      lpushHelper.on("input", () => {
        blpopNode.receive({ payload: ["test:list:blpop", "1"] });
      });

      lpushNode.receive({ topic: "test:list:blpop", payload: "myvalue" });
    });
  });

  it("should BRPOP return immediately when list has data", function (done) {
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
        id: "brpop-node",
        type: "redis-command",
        server: "config1",
        command: "BRPOP",
        name: "BRPOP",
        block: true,
        topic: "",
        params: "[]",
        wires: [["brpop-helper"]],
      },
      { id: "brpop-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const rpushNode = helper.getNode("rpush-node");
      const rpushHelper = helper.getNode("rpush-helper");
      const brpopNode = helper.getNode("brpop-node");
      const brpopHelper = helper.getNode("brpop-helper");

      brpopHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal("test:list:brpop");
          msg.payload[1].should.equal("myvalue");
          done();
        } catch (err) {
          done(err);
        }
      });

      rpushHelper.on("input", () => {
        brpopNode.receive({ payload: ["test:list:brpop", "1"] });
      });

      rpushNode.receive({ topic: "test:list:brpop", payload: "myvalue" });
    });
  });
});
