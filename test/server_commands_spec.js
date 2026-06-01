const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { cleanupKeys } = require("./helpers/cleanup");
const { redisConfigNode } = require("./helpers/deployment");

helper.init(require.resolve("node-red"));

describe("Server commands", function () {
  this.timeout(10000);

  const configNode = redisConfigNode("config1", "Local");

  beforeEach((done) => {
    helper.startServer(done);
  });

  afterEach((done) => {
    helper.unload().then(() => {
      helper.stopServer(() => {
        cleanupKeys("test:srv:*", done);
      });
    });
  });

  it("should PING return PONG", function (done) {
    const flow = [
      configNode,
      {
        id: "ping-node",
        type: "redis-command",
        server: "config1",
        command: "PING",
        name: "PING",
        topic: "",
        params: "[]",
        wires: [["ping-helper"]],
      },
      { id: "ping-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const pingNode = helper.getNode("ping-node");
      const pingHelper = helper.getNode("ping-helper");

      pingHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("PONG");
          done();
        } catch (err) {
          done(err);
        }
      });

      pingNode.receive({});
    });
  });

  it("should PING echo back a custom message", function (done) {
    const flow = [
      configNode,
      {
        id: "ping-node",
        type: "redis-command",
        server: "config1",
        command: "PING",
        name: "PING",
        topic: "",
        params: "[]",
        wires: [["ping-helper"]],
      },
      { id: "ping-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const pingNode = helper.getNode("ping-node");
      const pingHelper = helper.getNode("ping-helper");

      pingHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("hello");
          done();
        } catch (err) {
          done(err);
        }
      });

      pingNode.receive({ payload: "hello" });
    });
  });

  it("should DBSIZE return a non-negative integer", function (done) {
    const flow = [
      configNode,
      {
        id: "dbsize-node",
        type: "redis-command",
        server: "config1",
        command: "DBSIZE",
        name: "DBSIZE",
        topic: "",
        params: "[]",
        wires: [["dbsize-helper"]],
      },
      { id: "dbsize-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const dbsizeNode = helper.getNode("dbsize-node");
      const dbsizeHelper = helper.getNode("dbsize-helper");

      dbsizeHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.Number();
          msg.payload.should.be.aboveOrEqual(0);
          done();
        } catch (err) {
          done(err);
        }
      });

      dbsizeNode.receive({});
    });
  });

  it("should TIME return unix timestamp and microseconds", function (done) {
    const flow = [
      configNode,
      {
        id: "time-node",
        type: "redis-command",
        server: "config1",
        command: "TIME",
        name: "TIME",
        topic: "",
        params: "[]",
        wires: [["time-helper"]],
      },
      { id: "time-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const timeNode = helper.getNode("time-node");
      const timeHelper = helper.getNode("time-helper");

      timeHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.equal(2);
          parseInt(msg.payload[0]).should.be.above(0);
          done();
        } catch (err) {
          done(err);
        }
      });

      timeNode.receive({});
    });
  });

  it("should INFO return server information string", function (done) {
    const flow = [
      configNode,
      {
        id: "info-node",
        type: "redis-command",
        server: "config1",
        command: "INFO",
        name: "INFO",
        topic: "",
        params: "[]",
        wires: [["info-helper"]],
      },
      { id: "info-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const infoNode = helper.getNode("info-node");
      const infoHelper = helper.getNode("info-helper");

      infoHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.String();
          msg.payload.should.containEql("redis_version");
          done();
        } catch (err) {
          done(err);
        }
      });

      infoNode.receive({ payload: "server" });
    });
  });

  it("should COMMAND COUNT return total number of Redis commands", function (done) {
    const flow = [
      configNode,
      {
        id: "command-node",
        type: "redis-command",
        server: "config1",
        command: "COMMAND",
        name: "COMMAND",
        topic: "",
        params: "[]",
        wires: [["command-helper"]],
      },
      { id: "command-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const commandNode = helper.getNode("command-node");
      const commandHelper = helper.getNode("command-helper");

      commandHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.Number();
          msg.payload.should.be.above(0);
          done();
        } catch (err) {
          done(err);
        }
      });

      commandNode.receive({ payload: "COUNT" });
    });
  });

  it("should CLIENT ID return the current connection ID", function (done) {
    const flow = [
      configNode,
      {
        id: "client-node",
        type: "redis-command",
        server: "config1",
        command: "CLIENT",
        name: "CLIENT",
        topic: "",
        params: "[]",
        wires: [["client-helper"]],
      },
      { id: "client-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const clientNode = helper.getNode("client-node");
      const clientHelper = helper.getNode("client-helper");

      clientHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.Number();
          msg.payload.should.be.above(0);
          done();
        } catch (err) {
          done(err);
        }
      });

      clientNode.receive({ payload: "ID" });
    });
  });

  it("should CONFIG GET return configuration value", function (done) {
    const flow = [
      configNode,
      {
        id: "config-node",
        type: "redis-command",
        server: "config1",
        command: "CONFIG",
        name: "CONFIG",
        topic: "",
        params: "[]",
        wires: [["config-helper"]],
      },
      { id: "config-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const configCmdNode = helper.getNode("config-node");
      const configHelper = helper.getNode("config-helper");

      configHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.be.above(0);
          done();
        } catch (err) {
          done(err);
        }
      });

      configCmdNode.receive({ topic: "GET", payload: "maxmemory" });
    });
  });

  it("should LASTSAVE return the unix timestamp of last save", function (done) {
    const flow = [
      configNode,
      {
        id: "lastsave-node",
        type: "redis-command",
        server: "config1",
        command: "LASTSAVE",
        name: "LASTSAVE",
        topic: "",
        params: "[]",
        wires: [["lastsave-helper"]],
      },
      { id: "lastsave-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const lastsaveNode = helper.getNode("lastsave-node");
      const lastsaveHelper = helper.getNode("lastsave-helper");

      lastsaveHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.Number();
          msg.payload.should.be.above(0);
          done();
        } catch (err) {
          done(err);
        }
      });

      lastsaveNode.receive({});
    });
  });

  it("should MEMORY USAGE return memory bytes used by a key", function (done) {
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
        id: "memory-node",
        type: "redis-command",
        server: "config1",
        command: "MEMORY",
        name: "MEMORY",
        topic: "",
        params: "[]",
        wires: [["memory-helper"]],
      },
      { id: "memory-helper", type: "helper" },
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
      const memoryNode = helper.getNode("memory-node");
      const memoryHelper = helper.getNode("memory-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      memoryHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.Number();
          msg.payload.should.be.above(0);
          delNode.receive({ topic: "test:srv:memkey" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        memoryNode.receive({ topic: "USAGE", payload: "test:srv:memkey" });
      });

      setNode.receive({ topic: "test:srv:memkey", payload: "helloworld" });
    });
  });

  it("should OBJECT ENCODING return the encoding of a key", function (done) {
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
        id: "object-node",
        type: "redis-command",
        server: "config1",
        command: "OBJECT",
        name: "OBJECT",
        topic: "",
        params: "[]",
        wires: [["object-helper"]],
      },
      { id: "object-helper", type: "helper" },
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
      const objectNode = helper.getNode("object-node");
      const objectHelper = helper.getNode("object-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      objectHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.String();
          delNode.receive({ topic: "test:srv:enckey" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        objectNode.receive({
          topic: "ENCODING",
          payload: "test:srv:enckey",
        });
      });

      setNode.receive({ topic: "test:srv:enckey", payload: "helloworld" });
    });
  });

  it("should OBJECT IDLETIME return idle seconds since last access", function (done) {
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
        id: "object-node",
        type: "redis-command",
        server: "config1",
        command: "OBJECT",
        name: "OBJECT",
        topic: "",
        params: "[]",
        wires: [["object-helper"]],
      },
      { id: "object-helper", type: "helper" },
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
      const objectNode = helper.getNode("object-node");
      const objectHelper = helper.getNode("object-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      objectHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.Number();
          msg.payload.should.be.aboveOrEqual(0);
          delNode.receive({ topic: "test:srv:idlekey" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        objectNode.receive({
          topic: "IDLETIME",
          payload: "test:srv:idlekey",
        });
      });

      setNode.receive({ topic: "test:srv:idlekey", payload: "helloworld" });
    });
  });

  it("should OBJECT REFCOUNT return reference count of a key", function (done) {
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
        id: "object-node",
        type: "redis-command",
        server: "config1",
        command: "OBJECT",
        name: "OBJECT",
        topic: "",
        params: "[]",
        wires: [["object-helper"]],
      },
      { id: "object-helper", type: "helper" },
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
      const objectNode = helper.getNode("object-node");
      const objectHelper = helper.getNode("object-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      objectHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.Number();
          msg.payload.should.be.aboveOrEqual(1);
          delNode.receive({ topic: "test:srv:refkey" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        objectNode.receive({
          topic: "REFCOUNT",
          payload: "test:srv:refkey",
        });
      });

      setNode.receive({ topic: "test:srv:refkey", payload: "helloworld" });
    });
  });

  it("should ACL WHOAMI return the current username", function (done) {
    const flow = [
      configNode,
      {
        id: "acl-node",
        type: "redis-command",
        server: "config1",
        command: "ACL",
        name: "ACL",
        topic: "",
        params: "[]",
        wires: [["acl-helper"]],
      },
      { id: "acl-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const aclNode = helper.getNode("acl-node");
      const aclHelper = helper.getNode("acl-helper");

      aclHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.String();
          done();
        } catch (err) {
          done(err);
        }
      });

      aclNode.receive({ payload: "WHOAMI" });
    });
  });

  it("should SLOWLOG GET return the slow log entries array", function (done) {
    const flow = [
      configNode,
      {
        id: "slowlog-node",
        type: "redis-command",
        server: "config1",
        command: "SLOWLOG",
        name: "SLOWLOG",
        topic: "",
        params: "[]",
        wires: [["slowlog-helper"]],
      },
      { id: "slowlog-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const slowlogNode = helper.getNode("slowlog-node");
      const slowlogHelper = helper.getNode("slowlog-helper");

      slowlogHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          done();
        } catch (err) {
          done(err);
        }
      });

      slowlogNode.receive({ payload: "GET" });
    });
  });

  it("should LOLWUT return the Redis art string", function (done) {
    const flow = [
      configNode,
      {
        id: "lolwut-node",
        type: "redis-command",
        server: "config1",
        command: "LOLWUT",
        name: "LOLWUT",
        topic: "",
        params: "[]",
        wires: [["lolwut-helper"]],
      },
      { id: "lolwut-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const lolwutNode = helper.getNode("lolwut-node");
      const lolwutHelper = helper.getNode("lolwut-helper");

      lolwutHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.String();
          msg.payload.length.should.be.above(0);
          done();
        } catch (err) {
          done(err);
        }
      });

      lolwutNode.receive({});
    });
  });

  it("should ECHO return the same string that was sent", function (done) {
    const flow = [
      configNode,
      {
        id: "echo-node",
        type: "redis-command",
        server: "config1",
        command: "ECHO",
        name: "ECHO",
        topic: "",
        params: "[]",
        wires: [["echo-helper"]],
      },
      { id: "echo-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const echoNode = helper.getNode("echo-node");
      const echoHelper = helper.getNode("echo-helper");

      echoHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("hello");
          done();
        } catch (err) {
          done(err);
        }
      });

      echoNode.receive({ payload: "hello" });
    });
  });

  it("should PUBLISH send a message to a channel and return subscriber count", function (done) {
    const flow = [
      configNode,
      {
        id: "publish-node",
        type: "redis-command",
        server: "config1",
        command: "PUBLISH",
        name: "PUBLISH",
        topic: "",
        params: "[]",
        wires: [["publish-helper"]],
      },
      { id: "publish-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const publishNode = helper.getNode("publish-node");
      const publishHelper = helper.getNode("publish-helper");

      publishHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.Number();
          done();
        } catch (err) {
          done(err);
        }
      });

      publishNode.receive({
        topic: "test:srv:channel",
        payload: "hello",
      });
    });
  });

  it("should PUBSUB CHANNELS return an array of active channel names", function (done) {
    const flow = [
      configNode,
      {
        id: "pubsub-node",
        type: "redis-command",
        server: "config1",
        command: "PUBSUB",
        name: "PUBSUB",
        topic: "",
        params: "[]",
        wires: [["pubsub-helper"]],
      },
      { id: "pubsub-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const pubsubNode = helper.getNode("pubsub-node");
      const pubsubHelper = helper.getNode("pubsub-helper");

      pubsubHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          done();
        } catch (err) {
          done(err);
        }
      });

      pubsubNode.receive({ payload: ["CHANNELS", "*"] });
    });
  });

  it("should BGREWRITEAOF trigger AOF rewrite and return a string response", function (done) {
    const flow = [
      configNode,
      {
        id: "bgrewriteaof-node",
        type: "redis-command",
        server: "config1",
        command: "BGREWRITEAOF",
        name: "BGREWRITEAOF",
        topic: "",
        params: "[]",
        wires: [["bgrewriteaof-helper"]],
      },
      { id: "bgrewriteaof-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const bgrewriteaofNode = helper.getNode("bgrewriteaof-node");
      const bgrewriteaofHelper = helper.getNode("bgrewriteaof-helper");

      bgrewriteaofHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.String();
          done();
        } catch (err) {
          done(err);
        }
      });

      bgrewriteaofNode.receive({});
    });
  });

  it("should BGSAVE or SAVE perform a background or synchronous save", function (done) {
    const flow = [
      configNode,
      {
        id: "save-node",
        type: "redis-command",
        server: "config1",
        command: "SAVE",
        name: "SAVE",
        topic: "",
        params: "[]",
        wires: [["save-helper"]],
      },
      { id: "save-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const saveNode = helper.getNode("save-node");
      const saveHelper = helper.getNode("save-helper");

      saveHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("OK");
          done();
        } catch (err) {
          done(err);
        }
      });

      saveNode.receive({});
    });
  });

  it("should WAIT return number of replicas that acknowledged", function (done) {
    const flow = [
      configNode,
      {
        id: "wait-node",
        type: "redis-command",
        server: "config1",
        command: "WAIT",
        name: "WAIT",
        topic: "",
        params: "[]",
        wires: [["wait-helper"]],
      },
      { id: "wait-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const waitNode = helper.getNode("wait-node");
      const waitHelper = helper.getNode("wait-helper");

      waitHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.Number();
          done();
        } catch (err) {
          done(err);
        }
      });

      waitNode.receive({ payload: ["0", "0"] });
    });
  });
});
