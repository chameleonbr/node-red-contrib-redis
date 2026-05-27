const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { cleanupKeys } = require("./helpers/cleanup");

helper.init(require.resolve("node-red"));

describe("Scripting commands", function () {
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
        cleanupKeys("test:script:*", done);
      });
    });
  });

  it("should EVAL execute a Lua script and return result", function (done) {
    const flow = [
      configNode,
      {
        id: "eval-node",
        type: "redis-command",
        server: "config1",
        command: "EVAL",
        name: "EVAL",
        topic: "",
        params: "[]",
        wires: [["eval-helper"]],
      },
      { id: "eval-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const evalNode = helper.getNode("eval-node");
      const evalHelper = helper.getNode("eval-helper");

      evalHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("hello");
          done();
        } catch (err) {
          done(err);
        }
      });

      evalNode.receive({ payload: ["return 'hello'", "0"] });
    });
  });

  it("should EVAL_RO execute a read-only Lua script", function (done) {
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
        id: "evalro-node",
        type: "redis-command",
        server: "config1",
        command: "EVAL_RO",
        name: "EVAL_RO",
        topic: "",
        params: "[]",
        wires: [["evalro-helper"]],
      },
      { id: "evalro-helper", type: "helper" },
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
      const evalroNode = helper.getNode("evalro-node");
      const evalroHelper = helper.getNode("evalro-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      evalroHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("world");
          delNode.receive({ topic: "test:script:evalro" });
        } catch (err) {
          done(err);
        }
      });

      setHelper.on("input", () => {
        evalroNode.receive({
          payload: [
            "return redis.call('GET',KEYS[1])",
            "1",
            "test:script:evalro",
          ],
        });
      });

      setNode.receive({ topic: "test:script:evalro", payload: "world" });
    });
  });

  it("should SCRIPT LOAD return SHA1 and EVALSHA execute it", function (done) {
    const flow = [
      configNode,
      {
        id: "scriptload-node",
        type: "redis-command",
        server: "config1",
        command: "SCRIPT",
        name: "SCRIPT",
        topic: "",
        params: "[]",
        wires: [["scriptload-helper"]],
      },
      { id: "scriptload-helper", type: "helper" },
      {
        id: "evalsha-node",
        type: "redis-command",
        server: "config1",
        command: "EVALSHA",
        name: "EVALSHA",
        topic: "",
        params: "[]",
        wires: [["evalsha-helper"]],
      },
      { id: "evalsha-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const scriptloadNode = helper.getNode("scriptload-node");
      const scriptloadHelper = helper.getNode("scriptload-helper");
      const evalshaNode = helper.getNode("evalsha-node");
      const evalshaHelper = helper.getNode("evalsha-helper");

      evalshaHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("scripted");
          done();
        } catch (err) {
          done(err);
        }
      });

      scriptloadHelper.on("input", (msg) => {
        try {
          const sha1 = msg.payload;
          sha1.should.be.a.String();
          sha1.length.should.equal(40);
          evalshaNode.receive({ topic: sha1, payload: "0" });
        } catch (err) {
          done(err);
        }
      });

      scriptloadNode.receive({ payload: ["LOAD", "return 'scripted'"] });
    });
  });

  it("should SCRIPT EXISTS return 1 for a loaded script", function (done) {
    const flow = [
      configNode,
      {
        id: "scriptload-node",
        type: "redis-command",
        server: "config1",
        command: "SCRIPT",
        name: "SCRIPT_LOAD",
        topic: "",
        params: "[]",
        wires: [["scriptload-helper"]],
      },
      { id: "scriptload-helper", type: "helper" },
      {
        id: "scriptexists-node",
        type: "redis-command",
        server: "config1",
        command: "SCRIPT",
        name: "SCRIPT_EXISTS",
        topic: "",
        params: "[]",
        wires: [["scriptexists-helper"]],
      },
      { id: "scriptexists-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const scriptloadNode = helper.getNode("scriptload-node");
      const scriptloadHelper = helper.getNode("scriptload-helper");
      const scriptexistsNode = helper.getNode("scriptexists-node");
      const scriptexistsHelper = helper.getNode("scriptexists-helper");

      scriptexistsHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal(1);
          done();
        } catch (err) {
          done(err);
        }
      });

      scriptloadHelper.on("input", (msg) => {
        try {
          const sha = msg.payload;
          scriptexistsNode.receive({ payload: ["EXISTS", sha] });
        } catch (err) {
          done(err);
        }
      });

      scriptloadNode.receive({ payload: ["LOAD", "return 1"] });
    });
  });
});
