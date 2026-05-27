const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { cleanupKeys } = require("./helpers/cleanup");

helper.init(require.resolve("node-red"));

describe("HyperLogLog commands", function () {
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
        cleanupKeys("test:hll:*", done);
      });
    });
  });

  it("should PFADD elements and PFCOUNT approximate cardinality", function (done) {
    const flow = [
      configNode,
      {
        id: "pfadd-node",
        type: "redis-command",
        server: "config1",
        command: "PFADD",
        name: "PFADD",
        topic: "",
        params: "[]",
        wires: [["pfadd-helper"]],
      },
      { id: "pfadd-helper", type: "helper" },
      {
        id: "pfcount-node",
        type: "redis-command",
        server: "config1",
        command: "PFCOUNT",
        name: "PFCOUNT",
        topic: "",
        params: "[]",
        wires: [["pfcount-helper"]],
      },
      { id: "pfcount-helper", type: "helper" },
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
      const pfaddNode = helper.getNode("pfadd-node");
      const pfaddHelper = helper.getNode("pfadd-helper");
      const pfcountNode = helper.getNode("pfcount-node");
      const pfcountHelper = helper.getNode("pfcount-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      pfcountHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.Number();
          msg.payload.should.be.aboveOrEqual(4);
          delNode.receive({ topic: "test:hll:hll1" });
        } catch (err) {
          done(err);
        }
      });

      pfaddHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          pfcountNode.receive({ payload: "test:hll:hll1" });
        } catch (err) {
          done(err);
        }
      });

      pfaddNode.receive({
        topic: "test:hll:hll1",
        payload: ["a", "b", "c", "d", "e"],
      });
    });
  });

  it("should PFMERGE two HyperLogLogs into a destination", function (done) {
    const flow = [
      configNode,
      {
        id: "pfadd1-node",
        type: "redis-command",
        server: "config1",
        command: "PFADD",
        name: "PFADD1",
        topic: "",
        params: "[]",
        wires: [["pfadd1-helper"]],
      },
      { id: "pfadd1-helper", type: "helper" },
      {
        id: "pfadd2-node",
        type: "redis-command",
        server: "config1",
        command: "PFADD",
        name: "PFADD2",
        topic: "",
        params: "[]",
        wires: [["pfadd2-helper"]],
      },
      { id: "pfadd2-helper", type: "helper" },
      {
        id: "pfmerge-node",
        type: "redis-command",
        server: "config1",
        command: "PFMERGE",
        name: "PFMERGE",
        topic: "",
        params: "[]",
        wires: [["pfmerge-helper"]],
      },
      { id: "pfmerge-helper", type: "helper" },
      {
        id: "pfcount-node",
        type: "redis-command",
        server: "config1",
        command: "PFCOUNT",
        name: "PFCOUNT",
        topic: "",
        params: "[]",
        wires: [["pfcount-helper"]],
      },
      { id: "pfcount-helper", type: "helper" },
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
      const pfadd1Node = helper.getNode("pfadd1-node");
      const pfadd1Helper = helper.getNode("pfadd1-helper");
      const pfadd2Node = helper.getNode("pfadd2-node");
      const pfadd2Helper = helper.getNode("pfadd2-helper");
      const pfmergeNode = helper.getNode("pfmerge-node");
      const pfmergeHelper = helper.getNode("pfmerge-helper");
      const pfcountNode = helper.getNode("pfcount-node");
      const pfcountHelper = helper.getNode("pfcount-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      pfcountHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.Number();
          msg.payload.should.be.aboveOrEqual(4);
          delNode.receive({
            payload: ["test:hll:src1", "test:hll:src2", "test:hll:dest"],
          });
        } catch (err) {
          done(err);
        }
      });

      pfmergeHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("OK");
          pfcountNode.receive({ payload: "test:hll:dest" });
        } catch (err) {
          done(err);
        }
      });

      pfadd2Helper.on("input", () => {
        pfmergeNode.receive({
          topic: "test:hll:dest",
          payload: ["test:hll:src1", "test:hll:src2"],
        });
      });

      pfadd1Helper.on("input", () => {
        pfadd2Node.receive({
          topic: "test:hll:src2",
          payload: ["c", "d", "e", "f"],
        });
      });

      pfadd1Node.receive({
        topic: "test:hll:src1",
        payload: ["a", "b", "c", "d"],
      });
    });
  });
});
