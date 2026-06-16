var helper = require("node-red-node-test-helper");
var redisNode = require("../redis.js");
var deployment = require("./helpers/deployment");

helper.init(require.resolve("node-red"));

describe("redis-command node", function () {
  this.timeout(5000);

  const ENV_OPTIONS_NAME = "NODE_RED_REDIS_RUNTIME_OPTIONS";
  let originalEnvOptions;

  beforeEach(function (done) {
    originalEnvOptions = process.env[ENV_OPTIONS_NAME];
    process.env[ENV_OPTIONS_NAME] = JSON.stringify(deployment.redisOptions());
    helper.startServer(done);
  });
  afterEach(function (done) {
    helper.unload().then(function () {
      if (originalEnvOptions === undefined) {
        delete process.env[ENV_OPTIONS_NAME];
      } else {
        process.env[ENV_OPTIONS_NAME] = originalEnvOptions;
      }
      helper.stopServer(done);
    });
  });

  it("should SET hello=world and GET hello returning world", function (done) {
    var flow = [
      deployment.redisConfigNode("config1", "Local"),
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

    helper.load(redisNode, flow, function () {
      var setNode = helper.getNode("set-node");
      var setHelper = helper.getNode("set-helper");
      var getNode = helper.getNode("get-node");
      var getHelper = helper.getNode("get-helper");
      var delNode = helper.getNode("del-node");
      var delHelper = helper.getNode("del-helper");

      delHelper.on("input", function () {
        done();
      });

      getHelper.on("input", function (msg) {
        try {
          msg.payload.should.equal("world");
          delNode.receive({ topic: "hello" });
        } catch (err) {
          done(err);
        }
      });

      // After SET completes (payload = "OK"), trigger the GET flow
      setHelper.on("input", function () {
        getNode.receive({ topic: "hello" });
      });

      // Flow 1: inject { topic: "hello", payload: "world" } → SET
      setNode.receive({ topic: "hello", payload: "world" });
    });
  });

  it("should read redis-config options from an environment variable", function (done) {
    var flow = [
      {
        id: "config-env",
        type: "redis-config",
        name: "EnvConn",
        options: ENV_OPTIONS_NAME,
        optionsType: "env",
        cluster: false,
      },
      {
        id: "set-node",
        type: "redis-command",
        server: "config-env",
        command: "SET",
        name: "SET",
        topic: "",
        params: "[]",
        wires: [["set-helper"]],
      },
      { id: "set-helper", type: "helper" },
      {
        id: "get-node",
        type: "redis-command",
        server: "config-env",
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
        server: "config-env",
        command: "DEL",
        name: "DEL",
        topic: "",
        params: "[]",
        wires: [["del-helper"]],
      },
      { id: "del-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, function () {
      var setNode = helper.getNode("set-node");
      var setHelper = helper.getNode("set-helper");
      var getNode = helper.getNode("get-node");
      var getHelper = helper.getNode("get-helper");
      var delNode = helper.getNode("del-node");
      var delHelper = helper.getNode("del-helper");
      var key = "test:env-options:hello";

      delHelper.on("input", function () {
        done();
      });

      getHelper.on("input", function (msg) {
        try {
          msg.payload.should.equal("env-world");
          delNode.receive({ topic: key });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", function () {
        getNode.receive({ topic: key });
      });

      setNode.receive({ topic: key, payload: "env-world" });
    });
  });
});
