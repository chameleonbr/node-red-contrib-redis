const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { cleanupKeys } = require("./helpers/cleanup");
const { redisConfigNode } = require("./helpers/deployment");

helper.init(require.resolve("node-red"));

describe("Set commands", function () {
  this.timeout(5000);

  const configNode = redisConfigNode("config1", "Local");

  beforeEach((done) => {
    helper.startServer(done);
  });

  afterEach((done) => {
    helper.unload().then(() => {
      helper.stopServer(() => {
        cleanupKeys("test:set:*", done);
      });
    });
  });

  it("should SADD members and SMEMBERS return them all", function (done) {
    const flow = [
      configNode,
      {
        id: "sadd-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD",
        topic: "",
        params: "[]",
        wires: [["sadd-helper"]],
      },
      { id: "sadd-helper", type: "helper" },
      {
        id: "smembers-node",
        type: "redis-command",
        server: "config1",
        command: "SMEMBERS",
        name: "SMEMBERS",
        topic: "",
        params: "[]",
        wires: [["smembers-helper"]],
      },
      { id: "smembers-helper", type: "helper" },
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
      const saddNode = helper.getNode("sadd-node");
      const saddHelper = helper.getNode("sadd-helper");
      const smembersNode = helper.getNode("smembers-node");
      const smembersHelper = helper.getNode("smembers-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      smembersHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.containEql("a");
          msg.payload.should.containEql("b");
          msg.payload.should.containEql("c");
          delNode.receive({ topic: "test:set:sadd" });
        } catch (err) {
          done(err);
        }
      });

      saddHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(3);
          smembersNode.receive({ topic: "test:set:sadd" });
        } catch (err) {
          done(err);
        }
      });

      saddNode.receive({ topic: "test:set:sadd", payload: ["a", "b", "c"] });
    });
  });

  it("should SREM remove members and SCARD return count", function (done) {
    const flow = [
      configNode,
      {
        id: "sadd-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD",
        topic: "",
        params: "[]",
        wires: [["sadd-helper"]],
      },
      { id: "sadd-helper", type: "helper" },
      {
        id: "srem-node",
        type: "redis-command",
        server: "config1",
        command: "SREM",
        name: "SREM",
        topic: "",
        params: "[]",
        wires: [["srem-helper"]],
      },
      { id: "srem-helper", type: "helper" },
      {
        id: "scard-node",
        type: "redis-command",
        server: "config1",
        command: "SCARD",
        name: "SCARD",
        topic: "",
        params: "[]",
        wires: [["scard-helper"]],
      },
      { id: "scard-helper", type: "helper" },
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
      const saddNode = helper.getNode("sadd-node");
      const saddHelper = helper.getNode("sadd-helper");
      const sremNode = helper.getNode("srem-node");
      const sremHelper = helper.getNode("srem-helper");
      const scardNode = helper.getNode("scard-node");
      const scardHelper = helper.getNode("scard-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      scardHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          delNode.receive({ topic: "test:set:srem" });
        } catch (err) {
          done(err);
        }
      });

      sremHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          scardNode.receive({ topic: "test:set:srem" });
        } catch (err) {
          done(err);
        }
      });

      saddHelper.on("input", () => {
        sremNode.receive({ topic: "test:set:srem", payload: "a" });
      });

      saddNode.receive({ topic: "test:set:srem", payload: ["a", "b", "c"] });
    });
  });

  it("should SISMEMBER and SMISMEMBER check membership", function (done) {
    const flow = [
      configNode,
      {
        id: "sadd-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD",
        topic: "",
        params: "[]",
        wires: [["sadd-helper"]],
      },
      { id: "sadd-helper", type: "helper" },
      {
        id: "sismember-node",
        type: "redis-command",
        server: "config1",
        command: "SISMEMBER",
        name: "SISMEMBER",
        topic: "",
        params: "[]",
        wires: [["sismember-helper"]],
      },
      { id: "sismember-helper", type: "helper" },
      {
        id: "smismember-node",
        type: "redis-command",
        server: "config1",
        command: "SMISMEMBER",
        name: "SMISMEMBER",
        topic: "",
        params: "[]",
        wires: [["smismember-helper"]],
      },
      { id: "smismember-helper", type: "helper" },
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
      const saddNode = helper.getNode("sadd-node");
      const saddHelper = helper.getNode("sadd-helper");
      const sismemberNode = helper.getNode("sismember-node");
      const sismemberHelper = helper.getNode("sismember-helper");
      const smismemberNode = helper.getNode("smismember-node");
      const smismemberHelper = helper.getNode("smismember-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      smismemberHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.eql([1, 0]);
          delNode.receive({ topic: "test:set:ismember" });
        } catch (err) {
          done(err);
        }
      });

      sismemberHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          smismemberNode.receive({
            topic: "test:set:ismember",
            payload: ["a", "z"],
          });
        } catch (err) {
          done(err);
        }
      });

      saddHelper.on("input", () => {
        sismemberNode.receive({ topic: "test:set:ismember", payload: "a" });
      });

      saddNode.receive({
        topic: "test:set:ismember",
        payload: ["a", "b", "c"],
      });
    });
  });

  it("should SUNION return union of multiple sets", function (done) {
    const flow = [
      configNode,
      {
        id: "sadd1-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD1",
        topic: "",
        params: "[]",
        wires: [["sadd1-helper"]],
      },
      { id: "sadd1-helper", type: "helper" },
      {
        id: "sadd2-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD2",
        topic: "",
        params: "[]",
        wires: [["sadd2-helper"]],
      },
      { id: "sadd2-helper", type: "helper" },
      {
        id: "sunion-node",
        type: "redis-command",
        server: "config1",
        command: "SUNION",
        name: "SUNION",
        topic: "",
        params: "[]",
        wires: [["sunion-helper"]],
      },
      { id: "sunion-helper", type: "helper" },
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
      const sadd1Node = helper.getNode("sadd1-node");
      const sadd1Helper = helper.getNode("sadd1-helper");
      const sadd2Node = helper.getNode("sadd2-node");
      const sadd2Helper = helper.getNode("sadd2-helper");
      const sunionNode = helper.getNode("sunion-node");
      const sunionHelper = helper.getNode("sunion-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      sunionHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.equal(4);
          delNode.receive({
            payload: ["test:set:sunion1", "test:set:sunion2"],
          });
        } catch (err) {
          done(err);
        }
      });

      sadd2Helper.on("input", () => {
        sunionNode.receive({
          payload: ["test:set:sunion1", "test:set:sunion2"],
        });
      });

      sadd1Helper.on("input", () => {
        sadd2Node.receive({
          topic: "test:set:sunion2",
          payload: ["c", "d"],
        });
      });

      sadd1Node.receive({
        topic: "test:set:sunion1",
        payload: ["a", "b"],
      });
    });
  });

  it("should SUNIONSTORE store union result into destination", function (done) {
    const flow = [
      configNode,
      {
        id: "sadd1-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD1",
        topic: "",
        params: "[]",
        wires: [["sadd1-helper"]],
      },
      { id: "sadd1-helper", type: "helper" },
      {
        id: "sadd2-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD2",
        topic: "",
        params: "[]",
        wires: [["sadd2-helper"]],
      },
      { id: "sadd2-helper", type: "helper" },
      {
        id: "sunionstore-node",
        type: "redis-command",
        server: "config1",
        command: "SUNIONSTORE",
        name: "SUNIONSTORE",
        topic: "",
        params: "[]",
        wires: [["sunionstore-helper"]],
      },
      { id: "sunionstore-helper", type: "helper" },
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
      const sadd1Node = helper.getNode("sadd1-node");
      const sadd1Helper = helper.getNode("sadd1-helper");
      const sadd2Node = helper.getNode("sadd2-node");
      const sadd2Helper = helper.getNode("sadd2-helper");
      const sunionStoreNode = helper.getNode("sunionstore-node");
      const sunionStoreHelper = helper.getNode("sunionstore-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      sunionStoreHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(4);
          delNode.receive({
            payload: [
              "test:set:sus1",
              "test:set:sus2",
              "test:set:sudst",
            ],
          });
        } catch (err) {
          done(err);
        }
      });

      sadd2Helper.on("input", () => {
        sunionStoreNode.receive({
          topic: "test:set:sudst",
          payload: ["test:set:sus1", "test:set:sus2"],
        });
      });

      sadd1Helper.on("input", () => {
        sadd2Node.receive({ topic: "test:set:sus2", payload: ["c", "d"] });
      });

      sadd1Node.receive({ topic: "test:set:sus1", payload: ["a", "b"] });
    });
  });

  it("should SINTER return intersection of multiple sets", function (done) {
    const flow = [
      configNode,
      {
        id: "sadd1-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD1",
        topic: "",
        params: "[]",
        wires: [["sadd1-helper"]],
      },
      { id: "sadd1-helper", type: "helper" },
      {
        id: "sadd2-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD2",
        topic: "",
        params: "[]",
        wires: [["sadd2-helper"]],
      },
      { id: "sadd2-helper", type: "helper" },
      {
        id: "sinter-node",
        type: "redis-command",
        server: "config1",
        command: "SINTER",
        name: "SINTER",
        topic: "",
        params: "[]",
        wires: [["sinter-helper"]],
      },
      { id: "sinter-helper", type: "helper" },
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
      const sadd1Node = helper.getNode("sadd1-node");
      const sadd1Helper = helper.getNode("sadd1-helper");
      const sadd2Node = helper.getNode("sadd2-node");
      const sadd2Helper = helper.getNode("sadd2-helper");
      const sinterNode = helper.getNode("sinter-node");
      const sinterHelper = helper.getNode("sinter-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      sinterHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.containEql("b");
          msg.payload.should.containEql("c");
          msg.payload.length.should.equal(2);
          delNode.receive({
            payload: ["test:set:sinter1", "test:set:sinter2"],
          });
        } catch (err) {
          done(err);
        }
      });

      sadd2Helper.on("input", () => {
        sinterNode.receive({
          payload: ["test:set:sinter1", "test:set:sinter2"],
        });
      });

      sadd1Helper.on("input", () => {
        sadd2Node.receive({
          topic: "test:set:sinter2",
          payload: ["b", "c", "d"],
        });
      });

      sadd1Node.receive({
        topic: "test:set:sinter1",
        payload: ["a", "b", "c"],
      });
    });
  });

  it("should SINTERSTORE store intersection into destination", function (done) {
    const flow = [
      configNode,
      {
        id: "sadd1-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD1",
        topic: "",
        params: "[]",
        wires: [["sadd1-helper"]],
      },
      { id: "sadd1-helper", type: "helper" },
      {
        id: "sadd2-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD2",
        topic: "",
        params: "[]",
        wires: [["sadd2-helper"]],
      },
      { id: "sadd2-helper", type: "helper" },
      {
        id: "sinterstore-node",
        type: "redis-command",
        server: "config1",
        command: "SINTERSTORE",
        name: "SINTERSTORE",
        topic: "",
        params: "[]",
        wires: [["sinterstore-helper"]],
      },
      { id: "sinterstore-helper", type: "helper" },
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
      const sadd1Node = helper.getNode("sadd1-node");
      const sadd1Helper = helper.getNode("sadd1-helper");
      const sadd2Node = helper.getNode("sadd2-node");
      const sadd2Helper = helper.getNode("sadd2-helper");
      const sinterstoreNode = helper.getNode("sinterstore-node");
      const sinterstoreHelper = helper.getNode("sinterstore-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      sinterstoreHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          delNode.receive({
            payload: ["test:set:sis1", "test:set:sis2", "test:set:sisdst"],
          });
        } catch (err) {
          done(err);
        }
      });

      sadd2Helper.on("input", () => {
        sinterstoreNode.receive({
          topic: "test:set:sisdst",
          payload: ["test:set:sis1", "test:set:sis2"],
        });
      });

      sadd1Helper.on("input", () => {
        sadd2Node.receive({
          topic: "test:set:sis2",
          payload: ["b", "c", "d"],
        });
      });

      sadd1Node.receive({
        topic: "test:set:sis1",
        payload: ["a", "b", "c"],
      });
    });
  });

  it("should SINTERCARD return count of intersection members", function (done) {
    const flow = [
      configNode,
      {
        id: "sadd1-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD1",
        topic: "",
        params: "[]",
        wires: [["sadd1-helper"]],
      },
      { id: "sadd1-helper", type: "helper" },
      {
        id: "sadd2-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD2",
        topic: "",
        params: "[]",
        wires: [["sadd2-helper"]],
      },
      { id: "sadd2-helper", type: "helper" },
      {
        id: "sintercard-node",
        type: "redis-command",
        server: "config1",
        command: "SINTERCARD",
        name: "SINTERCARD",
        topic: "",
        params: "[]",
        wires: [["sintercard-helper"]],
      },
      { id: "sintercard-helper", type: "helper" },
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
      const sadd1Node = helper.getNode("sadd1-node");
      const sadd1Helper = helper.getNode("sadd1-helper");
      const sadd2Node = helper.getNode("sadd2-node");
      const sadd2Helper = helper.getNode("sadd2-helper");
      const sintercardNode = helper.getNode("sintercard-node");
      const sintercardHelper = helper.getNode("sintercard-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      sintercardHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          delNode.receive({
            payload: ["test:set:sic1", "test:set:sic2"],
          });
        } catch (err) {
          done(err);
        }
      });

      sadd2Helper.on("input", () => {
        sintercardNode.receive({
          payload: ["2", "test:set:sic1", "test:set:sic2"],
        });
      });

      sadd1Helper.on("input", () => {
        sadd2Node.receive({
          topic: "test:set:sic2",
          payload: ["b", "c", "d"],
        });
      });

      sadd1Node.receive({
        topic: "test:set:sic1",
        payload: ["a", "b", "c"],
      });
    });
  });

  it("should SDIFF return difference between sets", function (done) {
    const flow = [
      configNode,
      {
        id: "sadd1-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD1",
        topic: "",
        params: "[]",
        wires: [["sadd1-helper"]],
      },
      { id: "sadd1-helper", type: "helper" },
      {
        id: "sadd2-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD2",
        topic: "",
        params: "[]",
        wires: [["sadd2-helper"]],
      },
      { id: "sadd2-helper", type: "helper" },
      {
        id: "sdiff-node",
        type: "redis-command",
        server: "config1",
        command: "SDIFF",
        name: "SDIFF",
        topic: "",
        params: "[]",
        wires: [["sdiff-helper"]],
      },
      { id: "sdiff-helper", type: "helper" },
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
      const sadd1Node = helper.getNode("sadd1-node");
      const sadd1Helper = helper.getNode("sadd1-helper");
      const sadd2Node = helper.getNode("sadd2-node");
      const sadd2Helper = helper.getNode("sadd2-helper");
      const sdiffNode = helper.getNode("sdiff-node");
      const sdiffHelper = helper.getNode("sdiff-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      sdiffHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.containEql("a");
          msg.payload.length.should.equal(1);
          delNode.receive({
            payload: ["test:set:sdiff1", "test:set:sdiff2"],
          });
        } catch (err) {
          done(err);
        }
      });

      sadd2Helper.on("input", () => {
        sdiffNode.receive({
          payload: ["test:set:sdiff1", "test:set:sdiff2"],
        });
      });

      sadd1Helper.on("input", () => {
        sadd2Node.receive({
          topic: "test:set:sdiff2",
          payload: ["b", "c"],
        });
      });

      sadd1Node.receive({
        topic: "test:set:sdiff1",
        payload: ["a", "b", "c"],
      });
    });
  });

  it("should SDIFFSTORE store difference into destination", function (done) {
    const flow = [
      configNode,
      {
        id: "sadd1-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD1",
        topic: "",
        params: "[]",
        wires: [["sadd1-helper"]],
      },
      { id: "sadd1-helper", type: "helper" },
      {
        id: "sadd2-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD2",
        topic: "",
        params: "[]",
        wires: [["sadd2-helper"]],
      },
      { id: "sadd2-helper", type: "helper" },
      {
        id: "sdiffstore-node",
        type: "redis-command",
        server: "config1",
        command: "SDIFFSTORE",
        name: "SDIFFSTORE",
        topic: "",
        params: "[]",
        wires: [["sdiffstore-helper"]],
      },
      { id: "sdiffstore-helper", type: "helper" },
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
      const sadd1Node = helper.getNode("sadd1-node");
      const sadd1Helper = helper.getNode("sadd1-helper");
      const sadd2Node = helper.getNode("sadd2-node");
      const sadd2Helper = helper.getNode("sadd2-helper");
      const sdiffstoreNode = helper.getNode("sdiffstore-node");
      const sdiffstoreHelper = helper.getNode("sdiffstore-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      sdiffstoreHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          delNode.receive({
            payload: ["test:set:sds1", "test:set:sds2", "test:set:sdsdst"],
          });
        } catch (err) {
          done(err);
        }
      });

      sadd2Helper.on("input", () => {
        sdiffstoreNode.receive({
          topic: "test:set:sdsdst",
          payload: ["test:set:sds1", "test:set:sds2"],
        });
      });

      sadd1Helper.on("input", () => {
        sadd2Node.receive({
          topic: "test:set:sds2",
          payload: ["b", "c"],
        });
      });

      sadd1Node.receive({
        topic: "test:set:sds1",
        payload: ["a", "b", "c"],
      });
    });
  });

  it("should SPOP remove and return a random member", function (done) {
    const flow = [
      configNode,
      {
        id: "sadd-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD",
        topic: "",
        params: "[]",
        wires: [["sadd-helper"]],
      },
      { id: "sadd-helper", type: "helper" },
      {
        id: "spop-node",
        type: "redis-command",
        server: "config1",
        command: "SPOP",
        name: "SPOP",
        topic: "",
        params: "[]",
        wires: [["spop-helper"]],
      },
      { id: "spop-helper", type: "helper" },
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
      const saddNode = helper.getNode("sadd-node");
      const saddHelper = helper.getNode("sadd-helper");
      const spopNode = helper.getNode("spop-node");
      const spopHelper = helper.getNode("spop-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      spopHelper.on("input", (msg) => {
        try {
          ["a", "b", "c"].should.containEql(msg.payload);
          delNode.receive({ topic: "test:set:spop" });
        } catch (err) {
          done(err);
        }
      });

      saddHelper.on("input", () => {
        spopNode.receive({ topic: "test:set:spop" });
      });

      saddNode.receive({ topic: "test:set:spop", payload: ["a", "b", "c"] });
    });
  });

  it("should SRANDMEMBER return random member without removing", function (done) {
    const flow = [
      configNode,
      {
        id: "sadd-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD",
        topic: "",
        params: "[]",
        wires: [["sadd-helper"]],
      },
      { id: "sadd-helper", type: "helper" },
      {
        id: "srandmember-node",
        type: "redis-command",
        server: "config1",
        command: "SRANDMEMBER",
        name: "SRANDMEMBER",
        topic: "",
        params: "[]",
        wires: [["srandmember-helper"]],
      },
      { id: "srandmember-helper", type: "helper" },
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
      const saddNode = helper.getNode("sadd-node");
      const saddHelper = helper.getNode("sadd-helper");
      const srandmemberNode = helper.getNode("srandmember-node");
      const srandmemberHelper = helper.getNode("srandmember-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      srandmemberHelper.on("input", (msg) => {
        try {
          ["a", "b", "c"].should.containEql(msg.payload);
          delNode.receive({ topic: "test:set:srandmember" });
        } catch (err) {
          done(err);
        }
      });

      saddHelper.on("input", () => {
        srandmemberNode.receive({ topic: "test:set:srandmember" });
      });

      saddNode.receive({
        topic: "test:set:srandmember",
        payload: ["a", "b", "c"],
      });
    });
  });

  it("should SMOVE move a member from one set to another", function (done) {
    const flow = [
      configNode,
      {
        id: "sadd1-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD1",
        topic: "",
        params: "[]",
        wires: [["sadd1-helper"]],
      },
      { id: "sadd1-helper", type: "helper" },
      {
        id: "smove-node",
        type: "redis-command",
        server: "config1",
        command: "SMOVE",
        name: "SMOVE",
        topic: "",
        params: "[]",
        wires: [["smove-helper"]],
      },
      { id: "smove-helper", type: "helper" },
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
      const sadd1Node = helper.getNode("sadd1-node");
      const sadd1Helper = helper.getNode("sadd1-helper");
      const smoveNode = helper.getNode("smove-node");
      const smoveHelper = helper.getNode("smove-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      smoveHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          delNode.receive({
            payload: ["test:set:smovesrc", "test:set:smovedst"],
          });
        } catch (err) {
          done(err);
        }
      });

      sadd1Helper.on("input", () => {
        smoveNode.receive({
          topic: "test:set:smovesrc",
          payload: ["test:set:smovedst", "a"],
        });
      });

      sadd1Node.receive({
        topic: "test:set:smovesrc",
        payload: ["a", "b"],
      });
    });
  });

  it("should SSCAN iterate over set members", function (done) {
    const flow = [
      configNode,
      {
        id: "sadd-node",
        type: "redis-command",
        server: "config1",
        command: "SADD",
        name: "SADD",
        topic: "",
        params: "[]",
        wires: [["sadd-helper"]],
      },
      { id: "sadd-helper", type: "helper" },
      {
        id: "sscan-node",
        type: "redis-command",
        server: "config1",
        command: "SSCAN",
        name: "SSCAN",
        topic: "",
        params: "[]",
        wires: [["sscan-helper"]],
      },
      { id: "sscan-helper", type: "helper" },
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
      const saddNode = helper.getNode("sadd-node");
      const saddHelper = helper.getNode("sadd-helper");
      const sscanNode = helper.getNode("sscan-node");
      const sscanHelper = helper.getNode("sscan-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      sscanHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.equal(2);
          msg.payload[1].should.be.an.Array();
          delNode.receive({ topic: "test:set:sscan" });
        } catch (err) {
          done(err);
        }
      });

      saddHelper.on("input", () => {
        sscanNode.receive({ topic: "test:set:sscan", payload: "0" });
      });

      saddNode.receive({
        topic: "test:set:sscan",
        payload: ["a", "b", "c"],
      });
    });
  });
});
