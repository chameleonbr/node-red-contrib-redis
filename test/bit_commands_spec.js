const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { cleanupKeys } = require("./helpers/cleanup");
const { directRedis, redisConfigNode } = require("./helpers/deployment");

helper.init(require.resolve("node-red"));

describe("Bit commands", function () {
  this.timeout(5000);

  const configNode = redisConfigNode("config1", "Local");

  beforeEach((done) => {
    helper.startServer(done);
  });

  afterEach((done) => {
    helper.unload().then(() => {
      helper.stopServer(() => {
        cleanupKeys("test:bit:*", done);
      });
    });
  });

  it("should SETBIT set a bit and GETBIT retrieve it", function (done) {
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
        id: "setbit-node",
        type: "redis-command",
        server: "config1",
        command: "SETBIT",
        name: "SETBIT",
        topic: "",
        params: "[]",
        wires: [["setbit-helper"]],
      },
      { id: "setbit-helper", type: "helper" },
      {
        id: "getbit-node",
        type: "redis-command",
        server: "config1",
        command: "GETBIT",
        name: "GETBIT",
        topic: "",
        params: "[]",
        wires: [["getbit-helper"]],
      },
      { id: "getbit-helper", type: "helper" },
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
      const setbitNode = helper.getNode("setbit-node");
      const setbitHelper = helper.getNode("setbit-helper");
      const getbitNode = helper.getNode("getbit-node");
      const getbitHelper = helper.getNode("getbit-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      getbitHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          delNode.receive({ topic: "test:bit:setget" });
        } catch (err) {
          done(err);
        }
      });

      setbitHelper.on("input", () => {
        getbitNode.receive({ topic: "test:bit:setget", payload: "7" });
      });

      setHelper.on("input", () => {
        setbitNode.receive({
          topic: "test:bit:setget",
          payload: ["7", "1"],
        });
      });

      setNode.receive({ topic: "test:bit:setget", payload: "hello" });
    });
  });

  it("should BITCOUNT return number of set bits", function (done) {
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
        id: "bitcount-node",
        type: "redis-command",
        server: "config1",
        command: "BITCOUNT",
        name: "BITCOUNT",
        topic: "",
        params: "[]",
        wires: [["bitcount-helper"]],
      },
      { id: "bitcount-helper", type: "helper" },
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
      const bitcountNode = helper.getNode("bitcount-node");
      const bitcountHelper = helper.getNode("bitcount-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      bitcountHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(26);
          delNode.receive({ topic: "test:bit:count" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        bitcountNode.receive({ topic: "test:bit:count" });
      });

      setNode.receive({ topic: "test:bit:count", payload: "foobar" });
    });
  });

  it("should BITPOS find first 0 bit position", function (done) {
    const flow = [
      configNode,
      {
        id: "bitpos-node",
        type: "redis-command",
        server: "config1",
        command: "BITPOS",
        name: "BITPOS",
        topic: "",
        params: "[]",
        wires: [["bitpos-helper"]],
      },
      { id: "bitpos-helper", type: "helper" },
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
      const bitposNode = helper.getNode("bitpos-node");
      const bitposHelper = helper.getNode("bitpos-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      bitposHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(12);
          delNode.receive({ topic: "test:bit:pos" });
        } catch (err) {
          done(err);
        }
      });

      // Use direct ioredis to SET the binary value correctly
      // Buffer [0xff, 0xf0, 0x00] — first 12 bits are 1, bit 12 is 0
      const directClient = directRedis();
      directClient
        .set("test:bit:pos", Buffer.from([0xff, 0xf0, 0x00]))
        .then(() => {
          directClient.disconnect();
          // find first 0 bit
          bitposNode.receive({ topic: "test:bit:pos", payload: "0" });
        })
        .catch((err) => {
          directClient.disconnect();
          done(err);
        });
    });
  });

  it("should BITOP perform bitwise AND on two keys", function (done) {
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
        id: "bitop-node",
        type: "redis-command",
        server: "config1",
        command: "BITOP",
        name: "BITOP",
        topic: "",
        params: "[]",
        wires: [["bitop-helper"]],
      },
      { id: "bitop-helper", type: "helper" },
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
      const bitopNode = helper.getNode("bitop-node");
      const bitopHelper = helper.getNode("bitop-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      bitopHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(3);
          delNode.receive({
            payload: ["test:bit:op1", "test:bit:op2", "test:bit:result"],
          });
        } catch (err) {
          done(err);
        }
      });

      set2Helper.on("input", () => {
        bitopNode.receive({
          payload: ["AND", "test:bit:result", "test:bit:op1", "test:bit:op2"],
        });
      });

      set1Helper.on("input", () => {
        set2Node.receive({ topic: "test:bit:op2", payload: "abc" });
      });

      set1Node.receive({ topic: "test:bit:op1", payload: "abc" });
    });
  });

  it("should BITFIELD SET and GET an unsigned 8-bit integer", function (done) {
    const flow = [
      configNode,
      {
        id: "bitfield-set-node",
        type: "redis-command",
        server: "config1",
        command: "BITFIELD",
        name: "BITFIELD_SET",
        topic: "",
        params: "[]",
        wires: [["bitfield-set-helper"]],
      },
      { id: "bitfield-set-helper", type: "helper" },
      {
        id: "bitfield-get-node",
        type: "redis-command",
        server: "config1",
        command: "BITFIELD",
        name: "BITFIELD_GET",
        topic: "",
        params: "[]",
        wires: [["bitfield-get-helper"]],
      },
      { id: "bitfield-get-helper", type: "helper" },
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
      const bitfieldSetNode = helper.getNode("bitfield-set-node");
      const bitfieldSetHelper = helper.getNode("bitfield-set-helper");
      const bitfieldGetNode = helper.getNode("bitfield-get-node");
      const bitfieldGetHelper = helper.getNode("bitfield-get-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      bitfieldGetHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal(200);
          delNode.receive({ topic: "test:bit:field" });
        } catch (err) {
          done(err);
        }
      });

      bitfieldSetHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal(0);
          bitfieldGetNode.receive({
            topic: "test:bit:field",
            payload: ["GET", "u8", "0"],
          });
        } catch (err) {
          done(err);
        }
      });

      bitfieldSetNode.receive({
        topic: "test:bit:field",
        payload: ["SET", "u8", "0", "200"],
      });
    });
  });

  it("should BITFIELD_RO read a bitfield value read-only", function (done) {
    const flow = [
      configNode,
      {
        id: "bitfield-node",
        type: "redis-command",
        server: "config1",
        command: "BITFIELD",
        name: "BITFIELD",
        topic: "",
        params: "[]",
        wires: [["bitfield-helper"]],
      },
      { id: "bitfield-helper", type: "helper" },
      {
        id: "bitfieldro-node",
        type: "redis-command",
        server: "config1",
        command: "BITFIELD_RO",
        name: "BITFIELD_RO",
        topic: "",
        params: "[]",
        wires: [["bitfieldro-helper"]],
      },
      { id: "bitfieldro-helper", type: "helper" },
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
      const bitfieldNode = helper.getNode("bitfield-node");
      const bitfieldHelper = helper.getNode("bitfield-helper");
      const bitfieldroNode = helper.getNode("bitfieldro-node");
      const bitfieldroHelper = helper.getNode("bitfieldro-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      bitfieldroHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal(200);
          delNode.receive({ topic: "test:bit:fro" });
        } catch (err) {
          done(err);
        }
      });

      bitfieldHelper.on("input", () => {
        bitfieldroNode.receive({
          topic: "test:bit:fro",
          payload: ["GET", "u8", "0"],
        });
      });

      bitfieldNode.receive({
        topic: "test:bit:fro",
        payload: ["SET", "u8", "0", "200"],
      });
    });
  });
});
