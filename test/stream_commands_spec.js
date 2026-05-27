const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { cleanupKeys } = require("./helpers/cleanup");

helper.init(require.resolve("node-red"));

describe("Stream commands", function () {
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
        cleanupKeys("test:stream:*", done);
      });
    });
  });

  it("should XADD entries and XLEN return entry count", function (done) {
    const flow = [
      configNode,
      {
        id: "xadd-node",
        type: "redis-command",
        server: "config1",
        command: "XADD",
        name: "XADD",
        topic: "",
        params: "[]",
        wires: [["xadd-helper"]],
      },
      { id: "xadd-helper", type: "helper" },
      {
        id: "xlen-node",
        type: "redis-command",
        server: "config1",
        command: "XLEN",
        name: "XLEN",
        topic: "",
        params: "[]",
        wires: [["xlen-helper"]],
      },
      { id: "xlen-helper", type: "helper" },
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
      const xaddNode = helper.getNode("xadd-node");
      const xaddHelper = helper.getNode("xadd-helper");
      const xlenNode = helper.getNode("xlen-node");
      const xlenHelper = helper.getNode("xlen-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      xlenHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          delNode.receive({ topic: "test:stream:xlen" });
        } catch (err) {
          done(err);
        }
      });

      xaddHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.String();
          msg.payload.should.match(/^\d+-\d+$/);
          xlenNode.receive({ payload: "test:stream:xlen" });
        } catch (err) {
          done(err);
        }
      });

      xaddNode.receive({
        topic: "test:stream:xlen",
        payload: ["*", "field1", "value1"],
      });
    });
  });

  it("should XRANGE and XREVRANGE return entries in order", function (done) {
    const flow = [
      configNode,
      {
        id: "xadd-node",
        type: "redis-command",
        server: "config1",
        command: "XADD",
        name: "XADD",
        topic: "",
        params: "[]",
        wires: [["xadd-helper"]],
      },
      { id: "xadd-helper", type: "helper" },
      {
        id: "xrange-node",
        type: "redis-command",
        server: "config1",
        command: "XRANGE",
        name: "XRANGE",
        topic: "",
        params: "[]",
        wires: [["xrange-helper"]],
      },
      { id: "xrange-helper", type: "helper" },
      {
        id: "xrevrange-node",
        type: "redis-command",
        server: "config1",
        command: "XREVRANGE",
        name: "XREVRANGE",
        topic: "",
        params: "[]",
        wires: [["xrevrange-helper"]],
      },
      { id: "xrevrange-helper", type: "helper" },
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
      const xaddNode = helper.getNode("xadd-node");
      const xaddHelper = helper.getNode("xadd-helper");
      const xrangeNode = helper.getNode("xrange-node");
      const xrangeHelper = helper.getNode("xrange-helper");
      const xrevrangeNode = helper.getNode("xrevrange-node");
      const xrevrangeHelper = helper.getNode("xrevrange-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      xrevrangeHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.equal(1);
          delNode.receive({ topic: "test:stream:xrange" });
        } catch (err) {
          done(err);
        }
      });

      xrangeHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.equal(1);
          msg.payload[0].should.be.an.Array();
          xrevrangeNode.receive({
            topic: "test:stream:xrange",
            payload: ["+", "-"],
          });
        } catch (err) {
          done(err);
        }
      });

      xaddHelper.on("input", () => {
        xrangeNode.receive({
          topic: "test:stream:xrange",
          payload: ["-", "+"],
        });
      });

      xaddNode.receive({
        topic: "test:stream:xrange",
        payload: ["*", "f1", "v1"],
      });
    });
  });

  it("should XREAD return entries from a stream", function (done) {
    const flow = [
      configNode,
      {
        id: "xadd-node",
        type: "redis-command",
        server: "config1",
        command: "XADD",
        name: "XADD",
        topic: "",
        params: "[]",
        wires: [["xadd-helper"]],
      },
      { id: "xadd-helper", type: "helper" },
      {
        id: "xread-node",
        type: "redis-command",
        server: "config1",
        command: "XREAD",
        name: "XREAD",
        topic: "",
        params: "[]",
        wires: [["xread-helper"]],
      },
      { id: "xread-helper", type: "helper" },
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
      const xaddNode = helper.getNode("xadd-node");
      const xaddHelper = helper.getNode("xadd-helper");
      const xreadNode = helper.getNode("xread-node");
      const xreadHelper = helper.getNode("xread-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      xreadHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.equal(1);
          delNode.receive({ topic: "test:stream:xread" });
        } catch (err) {
          done(err);
        }
      });

      xaddHelper.on("input", () => {
        xreadNode.receive({
          payload: ["COUNT", "10", "STREAMS", "test:stream:xread", "0"],
        });
      });

      xaddNode.receive({
        topic: "test:stream:xread",
        payload: ["*", "f1", "v1"],
      });
    });
  });

  it("should XTRIM limit stream length", function (done) {
    const flow = [
      configNode,
      {
        id: "xadd1-node",
        type: "redis-command",
        server: "config1",
        command: "XADD",
        name: "XADD1",
        topic: "",
        params: "[]",
        wires: [["xadd1-helper"]],
      },
      { id: "xadd1-helper", type: "helper" },
      {
        id: "xadd2-node",
        type: "redis-command",
        server: "config1",
        command: "XADD",
        name: "XADD2",
        topic: "",
        params: "[]",
        wires: [["xadd2-helper"]],
      },
      { id: "xadd2-helper", type: "helper" },
      {
        id: "xtrim-node",
        type: "redis-command",
        server: "config1",
        command: "XTRIM",
        name: "XTRIM",
        topic: "",
        params: "[]",
        wires: [["xtrim-helper"]],
      },
      { id: "xtrim-helper", type: "helper" },
      {
        id: "xlen-node",
        type: "redis-command",
        server: "config1",
        command: "XLEN",
        name: "XLEN",
        topic: "",
        params: "[]",
        wires: [["xlen-helper"]],
      },
      { id: "xlen-helper", type: "helper" },
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
      const xadd1Node = helper.getNode("xadd1-node");
      const xadd1Helper = helper.getNode("xadd1-helper");
      const xadd2Node = helper.getNode("xadd2-node");
      const xadd2Helper = helper.getNode("xadd2-helper");
      const xtrimNode = helper.getNode("xtrim-node");
      const xtrimHelper = helper.getNode("xtrim-helper");
      const xlenNode = helper.getNode("xlen-node");
      const xlenHelper = helper.getNode("xlen-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      xlenHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          delNode.receive({ topic: "test:stream:xtrim" });
        } catch (err) {
          done(err);
        }
      });

      xtrimHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          xlenNode.receive({ payload: "test:stream:xtrim" });
        } catch (err) {
          done(err);
        }
      });

      xadd2Helper.on("input", () => {
        xtrimNode.receive({
          topic: "test:stream:xtrim",
          payload: ["MAXLEN", "1"],
        });
      });

      xadd1Helper.on("input", () => {
        xadd2Node.receive({
          topic: "test:stream:xtrim",
          payload: ["*", "f2", "v2"],
        });
      });

      xadd1Node.receive({
        topic: "test:stream:xtrim",
        payload: ["*", "f1", "v1"],
      });
    });
  });

  it("should XDEL remove a specific entry by ID", function (done) {
    const flow = [
      configNode,
      {
        id: "xadd-node",
        type: "redis-command",
        server: "config1",
        command: "XADD",
        name: "XADD",
        topic: "",
        params: "[]",
        wires: [["xadd-helper"]],
      },
      { id: "xadd-helper", type: "helper" },
      {
        id: "xdel-node",
        type: "redis-command",
        server: "config1",
        command: "XDEL",
        name: "XDEL",
        topic: "",
        params: "[]",
        wires: [["xdel-helper"]],
      },
      { id: "xdel-helper", type: "helper" },
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
      const xaddNode = helper.getNode("xadd-node");
      const xaddHelper = helper.getNode("xadd-helper");
      const xdelNode = helper.getNode("xdel-node");
      const xdelHelper = helper.getNode("xdel-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      xdelHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          delNode.receive({ topic: "test:stream:xdel" });
        } catch (err) {
          done(err);
        }
      });

      xaddHelper.on("input", (msg) => {
        const entryId = msg.payload;
        xdelNode.receive({ topic: "test:stream:xdel", payload: entryId });
      });

      xaddNode.receive({
        topic: "test:stream:xdel",
        payload: ["*", "f1", "v1"],
      });
    });
  });

  it("should XGROUP CREATE and XINFO GROUPS show group details", function (done) {
    const flow = [
      configNode,
      {
        id: "xadd-node",
        type: "redis-command",
        server: "config1",
        command: "XADD",
        name: "XADD",
        topic: "",
        params: "[]",
        wires: [["xadd-helper"]],
      },
      { id: "xadd-helper", type: "helper" },
      {
        id: "xgroup-node",
        type: "redis-command",
        server: "config1",
        command: "XGROUP",
        name: "XGROUP",
        topic: "",
        params: "[]",
        wires: [["xgroup-helper"]],
      },
      { id: "xgroup-helper", type: "helper" },
      {
        id: "xinfo-node",
        type: "redis-command",
        server: "config1",
        command: "XINFO",
        name: "XINFO",
        topic: "",
        params: "[]",
        wires: [["xinfo-helper"]],
      },
      { id: "xinfo-helper", type: "helper" },
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
      const xaddNode = helper.getNode("xadd-node");
      const xaddHelper = helper.getNode("xadd-helper");
      const xgroupNode = helper.getNode("xgroup-node");
      const xgroupHelper = helper.getNode("xgroup-helper");
      const xinfoNode = helper.getNode("xinfo-node");
      const xinfoHelper = helper.getNode("xinfo-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      xinfoHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.be.above(0);
          delNode.receive({ topic: "test:stream:xgroup" });
        } catch (err) {
          done(err);
        }
      });

      xgroupHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("OK");
          xinfoNode.receive({
            topic: "GROUPS",
            payload: "test:stream:xgroup",
          });
        } catch (err) {
          done(err);
        }
      });

      xaddHelper.on("input", () => {
        xgroupNode.receive({
          payload: [
            "CREATE",
            "test:stream:xgroup",
            "mygroup",
            "0",
          ],
        });
      });

      xaddNode.receive({
        topic: "test:stream:xgroup",
        payload: ["*", "f1", "v1"],
      });
    });
  });

  it("should XREADGROUP read messages and XACK acknowledge them", function (done) {
    const flow = [
      configNode,
      {
        id: "xadd-node",
        type: "redis-command",
        server: "config1",
        command: "XADD",
        name: "XADD",
        topic: "",
        params: "[]",
        wires: [["xadd-helper"]],
      },
      { id: "xadd-helper", type: "helper" },
      {
        id: "xgroup-node",
        type: "redis-command",
        server: "config1",
        command: "XGROUP",
        name: "XGROUP",
        topic: "",
        params: "[]",
        wires: [["xgroup-helper"]],
      },
      { id: "xgroup-helper", type: "helper" },
      {
        id: "xreadgroup-node",
        type: "redis-command",
        server: "config1",
        command: "XREADGROUP",
        name: "XREADGROUP",
        topic: "",
        params: "[]",
        wires: [["xreadgroup-helper"]],
      },
      { id: "xreadgroup-helper", type: "helper" },
      {
        id: "xack-node",
        type: "redis-command",
        server: "config1",
        command: "XACK",
        name: "XACK",
        topic: "",
        params: "[]",
        wires: [["xack-helper"]],
      },
      { id: "xack-helper", type: "helper" },
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
      const xaddNode = helper.getNode("xadd-node");
      const xaddHelper = helper.getNode("xadd-helper");
      const xgroupNode = helper.getNode("xgroup-node");
      const xgroupHelper = helper.getNode("xgroup-helper");
      const xreadgroupNode = helper.getNode("xreadgroup-node");
      const xreadgroupHelper = helper.getNode("xreadgroup-helper");
      const xackNode = helper.getNode("xack-node");
      const xackHelper = helper.getNode("xack-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      xackHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          delNode.receive({ topic: "test:stream:xreadgroup" });
        } catch (err) {
          done(err);
        }
      });

      xreadgroupHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.equal(1);
          const streamEntries = msg.payload[0][1];
          const entryId = streamEntries[0][0];
          xackNode.receive({
            topic: "test:stream:xreadgroup",
            payload: ["mygrp", entryId],
          });
        } catch (err) {
          done(err);
        }
      });

      xgroupHelper.on("input", () => {
        xreadgroupNode.receive({
          payload: [
            "GROUP",
            "mygrp",
            "consumer1",
            "COUNT",
            "10",
            "STREAMS",
            "test:stream:xreadgroup",
            ">",
          ],
        });
      });

      xaddHelper.on("input", () => {
        xgroupNode.receive({
          payload: ["CREATE", "test:stream:xreadgroup", "mygrp", "0"],
        });
      });

      xaddNode.receive({
        topic: "test:stream:xreadgroup",
        payload: ["*", "f1", "v1"],
      });
    });
  });

  it("should XPENDING show pending message summary", function (done) {
    const flow = [
      configNode,
      {
        id: "xadd-node",
        type: "redis-command",
        server: "config1",
        command: "XADD",
        name: "XADD",
        topic: "",
        params: "[]",
        wires: [["xadd-helper"]],
      },
      { id: "xadd-helper", type: "helper" },
      {
        id: "xgroup-node",
        type: "redis-command",
        server: "config1",
        command: "XGROUP",
        name: "XGROUP",
        topic: "",
        params: "[]",
        wires: [["xgroup-helper"]],
      },
      { id: "xgroup-helper", type: "helper" },
      {
        id: "xreadgroup-node",
        type: "redis-command",
        server: "config1",
        command: "XREADGROUP",
        name: "XREADGROUP",
        topic: "",
        params: "[]",
        wires: [["xreadgroup-helper"]],
      },
      { id: "xreadgroup-helper", type: "helper" },
      {
        id: "xpending-node",
        type: "redis-command",
        server: "config1",
        command: "XPENDING",
        name: "XPENDING",
        topic: "",
        params: "[]",
        wires: [["xpending-helper"]],
      },
      { id: "xpending-helper", type: "helper" },
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
      const xaddNode = helper.getNode("xadd-node");
      const xaddHelper = helper.getNode("xadd-helper");
      const xgroupNode = helper.getNode("xgroup-node");
      const xgroupHelper = helper.getNode("xgroup-helper");
      const xreadgroupNode = helper.getNode("xreadgroup-node");
      const xreadgroupHelper = helper.getNode("xreadgroup-helper");
      const xpendingNode = helper.getNode("xpending-node");
      const xpendingHelper = helper.getNode("xpending-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      xpendingHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          delNode.receive({ topic: "test:stream:xpending" });
        } catch (err) {
          done(err);
        }
      });

      xreadgroupHelper.on("input", () => {
        xpendingNode.receive({
          topic: "test:stream:xpending",
          payload: ["pendgrp", "-", "+", "10"],
        });
      });

      xgroupHelper.on("input", () => {
        xreadgroupNode.receive({
          payload: [
            "GROUP",
            "pendgrp",
            "consumer1",
            "COUNT",
            "10",
            "STREAMS",
            "test:stream:xpending",
            ">",
          ],
        });
      });

      xaddHelper.on("input", () => {
        xgroupNode.receive({
          payload: ["CREATE", "test:stream:xpending", "pendgrp", "0"],
        });
      });

      xaddNode.receive({
        topic: "test:stream:xpending",
        payload: ["*", "f1", "v1"],
      });
    });
  });

  it("should XINFO STREAM return stream metadata", function (done) {
    const flow = [
      configNode,
      {
        id: "xadd-node",
        type: "redis-command",
        server: "config1",
        command: "XADD",
        name: "XADD",
        topic: "",
        params: "[]",
        wires: [["xadd-helper"]],
      },
      { id: "xadd-helper", type: "helper" },
      {
        id: "xinfo-node",
        type: "redis-command",
        server: "config1",
        command: "XINFO",
        name: "XINFO",
        topic: "",
        params: "[]",
        wires: [["xinfo-helper"]],
      },
      { id: "xinfo-helper", type: "helper" },
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
      const xaddNode = helper.getNode("xadd-node");
      const xaddHelper = helper.getNode("xadd-helper");
      const xinfoNode = helper.getNode("xinfo-node");
      const xinfoHelper = helper.getNode("xinfo-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      xinfoHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.be.above(0);
          delNode.receive({ topic: "test:stream:xinfostream" });
        } catch (err) {
          done(err);
        }
      });

      xaddHelper.on("input", () => {
        xinfoNode.receive({
          topic: "STREAM",
          payload: "test:stream:xinfostream",
        });
      });

      xaddNode.receive({
        topic: "test:stream:xinfostream",
        payload: ["*", "f1", "v1"],
      });
    });
  });
});
