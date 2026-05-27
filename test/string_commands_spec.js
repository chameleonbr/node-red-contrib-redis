const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { cleanupKeys } = require("./helpers/cleanup");

helper.init(require.resolve("node-red"));

describe("String commands", function () {
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
        cleanupKeys("test:str:*", done);
      });
    });
  });

  it("should GETDEL return value and delete key atomically", function (done) {
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
        id: "getdel-node",
        type: "redis-command",
        server: "config1",
        command: "GETDEL",
        name: "GETDEL",
        topic: "",
        params: "[]",
        wires: [["getdel-helper"]],
      },
      { id: "getdel-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const setNode = helper.getNode("set-node");
      const setHelper = helper.getNode("set-helper");
      const getdelNode = helper.getNode("getdel-node");
      const getdelHelper = helper.getNode("getdel-helper");

      getdelHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("world");
          done();
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        getdelNode.receive({ topic: "test:str:getdel" });
      });

      setNode.receive({ topic: "test:str:getdel", payload: "world" });
    });
  });

  it("should GETSET return old value and store new value (deprecated)", function (done) {
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
        id: "getset-node",
        type: "redis-command",
        server: "config1",
        command: "GETSET",
        name: "GETSET",
        topic: "",
        params: "[]",
        wires: [["getset-helper"]],
      },
      { id: "getset-helper", type: "helper" },
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
      const getsetNode = helper.getNode("getset-node");
      const getsetHelper = helper.getNode("getset-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      getsetHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("world");
          delNode.receive({ topic: "test:str:getset" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        getsetNode.receive({ topic: "test:str:getset", payload: "newworld" });
      });

      setNode.receive({ topic: "test:str:getset", payload: "world" });
    });
  });

  it("should GETEX return value and set expiry", function (done) {
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
        id: "getex-node",
        type: "redis-command",
        server: "config1",
        command: "GETEX",
        name: "GETEX",
        topic: "",
        params: "[]",
        wires: [["getex-helper"]],
      },
      { id: "getex-helper", type: "helper" },
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
      const getexNode = helper.getNode("getex-node");
      const getexHelper = helper.getNode("getex-helper");
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
          delNode.receive({ topic: "test:str:getex" });
        } catch (err) {
          done(err);
        }
      });

      getexHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("world");
          ttlNode.receive({ topic: "test:str:getex" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        getexNode.receive({
          topic: "test:str:getex",
          payload: ["EX", "100"],
        });
      });

      setNode.receive({ topic: "test:str:getex", payload: "world" });
    });
  });

  it("should GETRANGE return substring of stored string", function (done) {
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
        id: "getrange-node",
        type: "redis-command",
        server: "config1",
        command: "GETRANGE",
        name: "GETRANGE",
        topic: "",
        params: "[]",
        wires: [["getrange-helper"]],
      },
      { id: "getrange-helper", type: "helper" },
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
      const getrangeNode = helper.getNode("getrange-node");
      const getrangeHelper = helper.getNode("getrange-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      getrangeHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("hello");
          delNode.receive({ topic: "test:str:getrange" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        getrangeNode.receive({
          topic: "test:str:getrange",
          payload: ["0", "4"],
        });
      });

      setNode.receive({ topic: "test:str:getrange", payload: "helloworld" });
    });
  });

  it("should SETRANGE overwrite part of a string and return new length", function (done) {
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
        id: "setrange-node",
        type: "redis-command",
        server: "config1",
        command: "SETRANGE",
        name: "SETRANGE",
        topic: "",
        params: "[]",
        wires: [["setrange-helper"]],
      },
      { id: "setrange-helper", type: "helper" },
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
      const setrangeNode = helper.getNode("setrange-node");
      const setrangeHelper = helper.getNode("setrange-helper");
      const getNode = helper.getNode("get-node");
      const getHelper = helper.getNode("get-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      getHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("helloWORLD");
          delNode.receive({ topic: "test:str:setrange" });
        } catch (err) {
          done(err);
        }
      });

      setrangeHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(10);
          getNode.receive({ topic: "test:str:setrange" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        setrangeNode.receive({
          topic: "test:str:setrange",
          payload: ["5", "WORLD"],
        });
      });

      setNode.receive({ topic: "test:str:setrange", payload: "helloworld" });
    });
  });

  it("should SETNX set only if key does not exist", function (done) {
    const flow = [
      configNode,
      {
        id: "setnx-node",
        type: "redis-command",
        server: "config1",
        command: "SETNX",
        name: "SETNX",
        topic: "",
        params: "[]",
        wires: [["setnx-helper"]],
      },
      { id: "setnx-helper", type: "helper" },
      {
        id: "setnx2-node",
        type: "redis-command",
        server: "config1",
        command: "SETNX",
        name: "SETNX2",
        topic: "",
        params: "[]",
        wires: [["setnx2-helper"]],
      },
      { id: "setnx2-helper", type: "helper" },
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
      const setnxNode = helper.getNode("setnx-node");
      const setnxHelper = helper.getNode("setnx-helper");
      const setnx2Node = helper.getNode("setnx2-node");
      const setnx2Helper = helper.getNode("setnx2-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      setnx2Helper.on("input", (msg) => {
        try {
          msg.payload.should.equal(0);
          delNode.receive({ topic: "test:str:setnx" });
        } catch (err) {
          done(err);
        }
      });

      setnxHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          setnx2Node.receive({ topic: "test:str:setnx", payload: "other" });
        } catch (err) {
          done(err);
        }
      });

      setnxNode.receive({ topic: "test:str:setnx", payload: "world" });
    });
  });

  it("should SETEX set key with TTL and verify with TTL command", function (done) {
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
          delNode.receive({ topic: "test:str:setex" });
        } catch (err) {
          done(err);
        }
      });

      setexHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("OK");
          ttlNode.receive({ topic: "test:str:setex" });
        } catch (err) {
          done(err);
        }
      });

      setexNode.receive({ topic: "test:str:setex", payload: ["100", "world"] });
    });
  });

  it("should PSETEX set key with millisecond TTL", function (done) {
    const flow = [
      configNode,
      {
        id: "psetex-node",
        type: "redis-command",
        server: "config1",
        command: "PSETEX",
        name: "PSETEX",
        topic: "",
        params: "[]",
        wires: [["psetex-helper"]],
      },
      { id: "psetex-helper", type: "helper" },
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
      const psetexNode = helper.getNode("psetex-node");
      const psetexHelper = helper.getNode("psetex-helper");
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
          delNode.receive({ topic: "test:str:psetex" });
        } catch (err) {
          done(err);
        }
      });

      psetexHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("OK");
          pttlNode.receive({ topic: "test:str:psetex" });
        } catch (err) {
          done(err);
        }
      });

      psetexNode.receive({
        topic: "test:str:psetex",
        payload: ["100000", "world"],
      });
    });
  });

  it("should STRLEN return the length of a string value", function (done) {
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
        id: "strlen-node",
        type: "redis-command",
        server: "config1",
        command: "STRLEN",
        name: "STRLEN",
        topic: "",
        params: "[]",
        wires: [["strlen-helper"]],
      },
      { id: "strlen-helper", type: "helper" },
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
      const strlenNode = helper.getNode("strlen-node");
      const strlenHelper = helper.getNode("strlen-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      strlenHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(5);
          delNode.receive({ topic: "test:str:strlen" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        strlenNode.receive({ topic: "test:str:strlen" });
      });

      setNode.receive({ topic: "test:str:strlen", payload: "hello" });
    });
  });

  it("should APPEND concatenate to an existing string value", function (done) {
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
        id: "append-node",
        type: "redis-command",
        server: "config1",
        command: "APPEND",
        name: "APPEND",
        topic: "",
        params: "[]",
        wires: [["append-helper"]],
      },
      { id: "append-helper", type: "helper" },
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
      const appendNode = helper.getNode("append-node");
      const appendHelper = helper.getNode("append-helper");
      const getNode = helper.getNode("get-node");
      const getHelper = helper.getNode("get-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      getHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("hello world");
          delNode.receive({ topic: "test:str:append" });
        } catch (err) {
          done(err);
        }
      });

      appendHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(11);
          getNode.receive({ topic: "test:str:append" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        appendNode.receive({ topic: "test:str:append", payload: " world" });
      });

      setNode.receive({ topic: "test:str:append", payload: "hello" });
    });
  });

  it("should INCR increment integer value by one", function (done) {
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
        id: "incr-node",
        type: "redis-command",
        server: "config1",
        command: "INCR",
        name: "INCR",
        topic: "",
        params: "[]",
        wires: [["incr-helper"]],
      },
      { id: "incr-helper", type: "helper" },
      {
        id: "decr-node",
        type: "redis-command",
        server: "config1",
        command: "DECR",
        name: "DECR",
        topic: "",
        params: "[]",
        wires: [["decr-helper"]],
      },
      { id: "decr-helper", type: "helper" },
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
      const incrNode = helper.getNode("incr-node");
      const incrHelper = helper.getNode("incr-helper");
      const decrNode = helper.getNode("decr-node");
      const decrHelper = helper.getNode("decr-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      decrHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(5);
          delNode.receive({ topic: "test:str:counter" });
        } catch (err) {
          done(err);
        }
      });

      incrHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(6);
          decrNode.receive({ topic: "test:str:counter" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        incrNode.receive({ topic: "test:str:counter" });
      });

      setNode.receive({ topic: "test:str:counter", payload: "5" });
    });
  });

  it("should INCRBY and DECRBY change value by given amount", function (done) {
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
        id: "incrby-node",
        type: "redis-command",
        server: "config1",
        command: "INCRBY",
        name: "INCRBY",
        topic: "",
        params: "[]",
        wires: [["incrby-helper"]],
      },
      { id: "incrby-helper", type: "helper" },
      {
        id: "decrby-node",
        type: "redis-command",
        server: "config1",
        command: "DECRBY",
        name: "DECRBY",
        topic: "",
        params: "[]",
        wires: [["decrby-helper"]],
      },
      { id: "decrby-helper", type: "helper" },
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
      const incrbyNode = helper.getNode("incrby-node");
      const incrbyHelper = helper.getNode("incrby-helper");
      const decrbyNode = helper.getNode("decrby-node");
      const decrbyHelper = helper.getNode("decrby-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      decrbyHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(5);
          delNode.receive({ topic: "test:str:incrby" });
        } catch (err) {
          done(err);
        }
      });

      incrbyHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(8);
          decrbyNode.receive({ topic: "test:str:incrby", payload: "3" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        incrbyNode.receive({ topic: "test:str:incrby", payload: "3" });
      });

      setNode.receive({ topic: "test:str:incrby", payload: "5" });
    });
  });

  it("should INCRBYFLOAT increment by a floating point value", function (done) {
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
        id: "incrbyfloat-node",
        type: "redis-command",
        server: "config1",
        command: "INCRBYFLOAT",
        name: "INCRBYFLOAT",
        topic: "",
        params: "[]",
        wires: [["incrbyfloat-helper"]],
      },
      { id: "incrbyfloat-helper", type: "helper" },
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
      const incrbyfloatNode = helper.getNode("incrbyfloat-node");
      const incrbyfloatHelper = helper.getNode("incrbyfloat-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      incrbyfloatHelper.on("input", (msg) => {
        try {
          parseFloat(msg.payload).should.be.approximately(3.8, 0.001);
          delNode.receive({ topic: "test:str:float" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        incrbyfloatNode.receive({ topic: "test:str:float", payload: "2.3" });
      });

      setNode.receive({ topic: "test:str:float", payload: "1.5" });
    });
  });

  it("should MSET store multiple keys and MGET retrieve them", function (done) {
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
        id: "mget-node",
        type: "redis-command",
        server: "config1",
        command: "MGET",
        name: "MGET",
        topic: "",
        params: "[]",
        wires: [["mget-helper"]],
      },
      { id: "mget-helper", type: "helper" },
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
      const mgetNode = helper.getNode("mget-node");
      const mgetHelper = helper.getNode("mget-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      mgetHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.eql(["v1", "v2"]);
          delNode.receive({
            payload: ["test:str:mset:k1", "test:str:mset:k2"],
          });
        } catch (err) {
          done(err);
        }
      });

      msetHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("OK");
          mgetNode.receive({
            payload: ["test:str:mset:k1", "test:str:mset:k2"],
          });
        } catch (err) {
          done(err);
        }
      });

      msetNode.receive({
        payload: ["test:str:mset:k1", "v1", "test:str:mset:k2", "v2"],
      });
    });
  });

  it("should MSETNX set multiple keys only when none exist", function (done) {
    const flow = [
      configNode,
      {
        id: "msetnx-node",
        type: "redis-command",
        server: "config1",
        command: "MSETNX",
        name: "MSETNX",
        topic: "",
        params: "[]",
        wires: [["msetnx-helper"]],
      },
      { id: "msetnx-helper", type: "helper" },
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
      const msetnxNode = helper.getNode("msetnx-node");
      const msetnxHelper = helper.getNode("msetnx-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      msetnxHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          delNode.receive({
            payload: ["test:str:msetnx:k1", "test:str:msetnx:k2"],
          });
        } catch (err) {
          done(err);
        }
      });

      msetnxNode.receive({
        payload: ["test:str:msetnx:k1", "v1", "test:str:msetnx:k2", "v2"],
      });
    });
  });

  it("should SUBSTR (alias for GETRANGE) return a substring", function (done) {
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
        id: "substr-node",
        type: "redis-command",
        server: "config1",
        command: "SUBSTR",
        name: "SUBSTR",
        topic: "",
        params: "[]",
        wires: [["substr-helper"]],
      },
      { id: "substr-helper", type: "helper" },
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
      const substrNode = helper.getNode("substr-node");
      const substrHelper = helper.getNode("substr-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      substrHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("world");
          delNode.receive({ topic: "test:str:substr" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        substrNode.receive({
          topic: "test:str:substr",
          payload: ["6", "10"],
        });
      });

      setNode.receive({ topic: "test:str:substr", payload: "hello world" });
    });
  });
});
