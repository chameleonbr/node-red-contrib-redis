var helper = require("node-red-node-test-helper");
var redisNode = require("../redis.js");

helper.init(require.resolve("node-red"));

describe("redis-command node", function () {
  this.timeout(5000);

  beforeEach(function (done) {
    helper.startServer(done);
  });
  afterEach(function (done) {
    helper.unload().then(function () {
      helper.stopServer(done);
    });
  });

  it("should SET hello=world and GET hello returning world", function (done) {
    var flow = [
      {
        id: "config1",
        type: "redis-config",
        name: "Local",
        options: '{"host":"127.0.0.1","port":6379}',
        optionsType: "json",
        cluster: false,
      },
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
});
