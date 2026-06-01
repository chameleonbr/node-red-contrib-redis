const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { cleanupKeys } = require("./helpers/cleanup");
const { redisConfigNode } = require("./helpers/deployment");

helper.init(require.resolve("node-red"));

describe("Sorted Set commands", function () {
  this.timeout(8000);

  const configNode = redisConfigNode("config1", "Local");

  beforeEach((done) => {
    helper.startServer(done);
  });

  afterEach((done) => {
    helper.unload().then(() => {
      helper.stopServer(() => {
        cleanupKeys("test:zset:*", done);
      });
    });
  });

  it("should ZADD members and ZSCORE return the score", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zscore-node",
        type: "redis-command",
        server: "config1",
        command: "ZSCORE",
        name: "ZSCORE",
        topic: "",
        params: "[]",
        wires: [["zscore-helper"]],
      },
      { id: "zscore-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zscoreNode = helper.getNode("zscore-node");
      const zscoreHelper = helper.getNode("zscore-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zscoreHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("2");
          delNode.receive({ topic: "test:zset:zadd" });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(3);
          zscoreNode.receive({ topic: "test:zset:zadd", payload: "b" });
        } catch (err) {
          done(err);
        }
      });

      zaddNode.receive({
        topic: "test:zset:zadd",
        payload: ["1", "a", "2", "b", "3", "c"],
      });
    });
  });

  it("should ZREM remove members and ZCARD return count", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zrem-node",
        type: "redis-command",
        server: "config1",
        command: "ZREM",
        name: "ZREM",
        topic: "",
        params: "[]",
        wires: [["zrem-helper"]],
      },
      { id: "zrem-helper", type: "helper" },
      {
        id: "zcard-node",
        type: "redis-command",
        server: "config1",
        command: "ZCARD",
        name: "ZCARD",
        topic: "",
        params: "[]",
        wires: [["zcard-helper"]],
      },
      { id: "zcard-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zremNode = helper.getNode("zrem-node");
      const zremHelper = helper.getNode("zrem-helper");
      const zcardNode = helper.getNode("zcard-node");
      const zcardHelper = helper.getNode("zcard-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zcardHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          delNode.receive({ topic: "test:zset:zrem" });
        } catch (err) {
          done(err);
        }
      });

      zremHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          zcardNode.receive({ topic: "test:zset:zrem" });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zremNode.receive({ topic: "test:zset:zrem", payload: "a" });
      });

      zaddNode.receive({
        topic: "test:zset:zrem",
        payload: ["1", "a", "2", "b", "3", "c"],
      });
    });
  });

  it("should ZRANK and ZREVRANK return position in ascending and descending order", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zrank-node",
        type: "redis-command",
        server: "config1",
        command: "ZRANK",
        name: "ZRANK",
        topic: "",
        params: "[]",
        wires: [["zrank-helper"]],
      },
      { id: "zrank-helper", type: "helper" },
      {
        id: "zrevrank-node",
        type: "redis-command",
        server: "config1",
        command: "ZREVRANK",
        name: "ZREVRANK",
        topic: "",
        params: "[]",
        wires: [["zrevrank-helper"]],
      },
      { id: "zrevrank-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zrankNode = helper.getNode("zrank-node");
      const zrankHelper = helper.getNode("zrank-helper");
      const zrevrankNode = helper.getNode("zrevrank-node");
      const zrevrankHelper = helper.getNode("zrevrank-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zrevrankHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          delNode.receive({ topic: "test:zset:rank" });
        } catch (err) {
          done(err);
        }
      });

      zrankHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(0);
          zrevrankNode.receive({ topic: "test:zset:rank", payload: "a" });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zrankNode.receive({ topic: "test:zset:rank", payload: "a" });
      });

      zaddNode.receive({
        topic: "test:zset:rank",
        payload: ["1", "a", "2", "b", "3", "c"],
      });
    });
  });

  it("should ZRANGE and ZREVRANGE return elements in order", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zrange-node",
        type: "redis-command",
        server: "config1",
        command: "ZRANGE",
        name: "ZRANGE",
        topic: "",
        params: "[]",
        wires: [["zrange-helper"]],
      },
      { id: "zrange-helper", type: "helper" },
      {
        id: "zrevrange-node",
        type: "redis-command",
        server: "config1",
        command: "ZREVRANGE",
        name: "ZREVRANGE",
        topic: "",
        params: "[]",
        wires: [["zrevrange-helper"]],
      },
      { id: "zrevrange-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zrangeNode = helper.getNode("zrange-node");
      const zrangeHelper = helper.getNode("zrange-helper");
      const zrevrangeNode = helper.getNode("zrevrange-node");
      const zrevrangeHelper = helper.getNode("zrevrange-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zrevrangeHelper.on("input", (msg) => {
        try {
          msg.payload.should.eql(["c", "b", "a"]);
          delNode.receive({ topic: "test:zset:range" });
        } catch (err) {
          done(err);
        }
      });

      zrangeHelper.on("input", (msg) => {
        try {
          msg.payload.should.eql(["a", "b", "c"]);
          zrevrangeNode.receive({
            topic: "test:zset:range",
            payload: ["0", "-1"],
          });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zrangeNode.receive({
          topic: "test:zset:range",
          payload: ["0", "-1"],
        });
      });

      zaddNode.receive({
        topic: "test:zset:range",
        payload: ["1", "a", "2", "b", "3", "c"],
      });
    });
  });

  it("should ZINCRBY increment a member score", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zincrby-node",
        type: "redis-command",
        server: "config1",
        command: "ZINCRBY",
        name: "ZINCRBY",
        topic: "",
        params: "[]",
        wires: [["zincrby-helper"]],
      },
      { id: "zincrby-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zincrbyNode = helper.getNode("zincrby-node");
      const zincrbyHelper = helper.getNode("zincrby-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zincrbyHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal("6");
          delNode.receive({ topic: "test:zset:zincrby" });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zincrbyNode.receive({
          topic: "test:zset:zincrby",
          payload: ["5", "a"],
        });
      });

      zaddNode.receive({ topic: "test:zset:zincrby", payload: ["1", "a"] });
    });
  });

  it("should ZCOUNT return number of members within score range", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zcount-node",
        type: "redis-command",
        server: "config1",
        command: "ZCOUNT",
        name: "ZCOUNT",
        topic: "",
        params: "[]",
        wires: [["zcount-helper"]],
      },
      { id: "zcount-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zcountNode = helper.getNode("zcount-node");
      const zcountHelper = helper.getNode("zcount-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zcountHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          delNode.receive({ topic: "test:zset:zcount" });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zcountNode.receive({
          topic: "test:zset:zcount",
          payload: ["1", "2"],
        });
      });

      zaddNode.receive({
        topic: "test:zset:zcount",
        payload: ["1", "a", "2", "b", "3", "c"],
      });
    });
  });

  it("should ZLEXCOUNT count members between lex range", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zlexcount-node",
        type: "redis-command",
        server: "config1",
        command: "ZLEXCOUNT",
        name: "ZLEXCOUNT",
        topic: "",
        params: "[]",
        wires: [["zlexcount-helper"]],
      },
      { id: "zlexcount-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zlexcountNode = helper.getNode("zlexcount-node");
      const zlexcountHelper = helper.getNode("zlexcount-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zlexcountHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(3);
          delNode.receive({ topic: "test:zset:zlexcount" });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zlexcountNode.receive({
          topic: "test:zset:zlexcount",
          payload: ["-", "+"],
        });
      });

      zaddNode.receive({
        topic: "test:zset:zlexcount",
        payload: ["0", "a", "0", "b", "0", "c"],
      });
    });
  });

  it("should ZRANGEBYSCORE and ZREVRANGEBYSCORE return members in score range", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zrangebyscore-node",
        type: "redis-command",
        server: "config1",
        command: "ZRANGEBYSCORE",
        name: "ZRANGEBYSCORE",
        topic: "",
        params: "[]",
        wires: [["zrangebyscore-helper"]],
      },
      { id: "zrangebyscore-helper", type: "helper" },
      {
        id: "zrevrangebyscore-node",
        type: "redis-command",
        server: "config1",
        command: "ZREVRANGEBYSCORE",
        name: "ZREVRANGEBYSCORE",
        topic: "",
        params: "[]",
        wires: [["zrevrangebyscore-helper"]],
      },
      { id: "zrevrangebyscore-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zrangebyscoreNode = helper.getNode("zrangebyscore-node");
      const zrangebyscoreHelper = helper.getNode("zrangebyscore-helper");
      const zrevrangebyscoreNode = helper.getNode("zrevrangebyscore-node");
      const zrevrangebyscoreHelper = helper.getNode("zrevrangebyscore-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zrevrangebyscoreHelper.on("input", (msg) => {
        try {
          msg.payload.should.eql(["b", "a"]);
          delNode.receive({ topic: "test:zset:byscore" });
        } catch (err) {
          done(err);
        }
      });

      zrangebyscoreHelper.on("input", (msg) => {
        try {
          msg.payload.should.eql(["a", "b"]);
          zrevrangebyscoreNode.receive({
            topic: "test:zset:byscore",
            payload: ["2", "1"],
          });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zrangebyscoreNode.receive({
          topic: "test:zset:byscore",
          payload: ["1", "2"],
        });
      });

      zaddNode.receive({
        topic: "test:zset:byscore",
        payload: ["1", "a", "2", "b", "3", "c"],
      });
    });
  });

  it("should ZRANGEBYLEX and ZREVRANGEBYLEX return members in lex range", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zrangebylex-node",
        type: "redis-command",
        server: "config1",
        command: "ZRANGEBYLEX",
        name: "ZRANGEBYLEX",
        topic: "",
        params: "[]",
        wires: [["zrangebylex-helper"]],
      },
      { id: "zrangebylex-helper", type: "helper" },
      {
        id: "zrevrangebylex-node",
        type: "redis-command",
        server: "config1",
        command: "ZREVRANGEBYLEX",
        name: "ZREVRANGEBYLEX",
        topic: "",
        params: "[]",
        wires: [["zrevrangebylex-helper"]],
      },
      { id: "zrevrangebylex-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zrangebylexNode = helper.getNode("zrangebylex-node");
      const zrangebylexHelper = helper.getNode("zrangebylex-helper");
      const zrevrangebylexNode = helper.getNode("zrevrangebylex-node");
      const zrevrangebylexHelper = helper.getNode("zrevrangebylex-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zrevrangebylexHelper.on("input", (msg) => {
        try {
          msg.payload.should.eql(["b", "a"]);
          delNode.receive({ topic: "test:zset:bylex" });
        } catch (err) {
          done(err);
        }
      });

      zrangebylexHelper.on("input", (msg) => {
        try {
          msg.payload.should.eql(["a", "b"]);
          zrevrangebylexNode.receive({
            topic: "test:zset:bylex",
            payload: ["[b", "[a"],
          });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zrangebylexNode.receive({
          topic: "test:zset:bylex",
          payload: ["[a", "[b"],
        });
      });

      zaddNode.receive({
        topic: "test:zset:bylex",
        payload: ["0", "a", "0", "b", "0", "c"],
      });
    });
  });

  it("should ZRANGESTORE copy a range into a new key", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zrangestore-node",
        type: "redis-command",
        server: "config1",
        command: "ZRANGESTORE",
        name: "ZRANGESTORE",
        topic: "",
        params: "[]",
        wires: [["zrangestore-helper"]],
      },
      { id: "zrangestore-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zrangestoreNode = helper.getNode("zrangestore-node");
      const zrangestoreHelper = helper.getNode("zrangestore-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zrangestoreHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          delNode.receive({
            payload: ["test:zset:zrssrc", "test:zset:zrsdst"],
          });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zrangestoreNode.receive({
          topic: "test:zset:zrsdst",
          payload: ["test:zset:zrssrc", "0", "1"],
        });
      });

      zaddNode.receive({
        topic: "test:zset:zrssrc",
        payload: ["1", "a", "2", "b", "3", "c"],
      });
    });
  });

  it("should ZPOPMIN and ZPOPMAX pop lowest and highest scored members", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zpopmin-node",
        type: "redis-command",
        server: "config1",
        command: "ZPOPMIN",
        name: "ZPOPMIN",
        topic: "",
        params: "[]",
        wires: [["zpopmin-helper"]],
      },
      { id: "zpopmin-helper", type: "helper" },
      {
        id: "zpopmax-node",
        type: "redis-command",
        server: "config1",
        command: "ZPOPMAX",
        name: "ZPOPMAX",
        topic: "",
        params: "[]",
        wires: [["zpopmax-helper"]],
      },
      { id: "zpopmax-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zpopminNode = helper.getNode("zpopmin-node");
      const zpopminHelper = helper.getNode("zpopmin-helper");
      const zpopmaxNode = helper.getNode("zpopmax-node");
      const zpopmaxHelper = helper.getNode("zpopmax-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zpopmaxHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal("c");
          delNode.receive({ topic: "test:zset:zpop" });
        } catch (err) {
          done(err);
        }
      });

      zpopminHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal("a");
          zpopmaxNode.receive({ topic: "test:zset:zpop" });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zpopminNode.receive({ topic: "test:zset:zpop" });
      });

      zaddNode.receive({
        topic: "test:zset:zpop",
        payload: ["1", "a", "2", "b", "3", "c"],
      });
    });
  });

  it("should ZRANDMEMBER return a random member", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zrandmember-node",
        type: "redis-command",
        server: "config1",
        command: "ZRANDMEMBER",
        name: "ZRANDMEMBER",
        topic: "",
        params: "[]",
        wires: [["zrandmember-helper"]],
      },
      { id: "zrandmember-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zrandmemberNode = helper.getNode("zrandmember-node");
      const zrandmemberHelper = helper.getNode("zrandmember-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zrandmemberHelper.on("input", (msg) => {
        try {
          ["a", "b", "c"].should.containEql(msg.payload);
          delNode.receive({ topic: "test:zset:zrandmember" });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zrandmemberNode.receive({ topic: "test:zset:zrandmember" });
      });

      zaddNode.receive({
        topic: "test:zset:zrandmember",
        payload: ["1", "a", "2", "b", "3", "c"],
      });
    });
  });

  it("should ZMSCORE return scores for multiple members", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zmscore-node",
        type: "redis-command",
        server: "config1",
        command: "ZMSCORE",
        name: "ZMSCORE",
        topic: "",
        params: "[]",
        wires: [["zmscore-helper"]],
      },
      { id: "zmscore-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zmscoreNode = helper.getNode("zmscore-node");
      const zmscoreHelper = helper.getNode("zmscore-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zmscoreHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.eql(["1", "3"]);
          delNode.receive({ topic: "test:zset:zmscore" });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zmscoreNode.receive({
          topic: "test:zset:zmscore",
          payload: ["a", "c"],
        });
      });

      zaddNode.receive({
        topic: "test:zset:zmscore",
        payload: ["1", "a", "2", "b", "3", "c"],
      });
    });
  });

  it("should ZREMRANGEBYSCORE remove members in score range", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zremrangebyscore-node",
        type: "redis-command",
        server: "config1",
        command: "ZREMRANGEBYSCORE",
        name: "ZREMRANGEBYSCORE",
        topic: "",
        params: "[]",
        wires: [["zremrangebyscore-helper"]],
      },
      { id: "zremrangebyscore-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zremrangebyscoreNode = helper.getNode("zremrangebyscore-node");
      const zremrangebyscoreHelper = helper.getNode("zremrangebyscore-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zremrangebyscoreHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          delNode.receive({ topic: "test:zset:zrrbs" });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zremrangebyscoreNode.receive({
          topic: "test:zset:zrrbs",
          payload: ["1", "2"],
        });
      });

      zaddNode.receive({
        topic: "test:zset:zrrbs",
        payload: ["1", "a", "2", "b", "3", "c"],
      });
    });
  });

  it("should ZREMRANGEBYRANK remove members by rank range", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zremrangebyrank-node",
        type: "redis-command",
        server: "config1",
        command: "ZREMRANGEBYRANK",
        name: "ZREMRANGEBYRANK",
        topic: "",
        params: "[]",
        wires: [["zremrangebyrank-helper"]],
      },
      { id: "zremrangebyrank-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zremrangebyrankNode = helper.getNode("zremrangebyrank-node");
      const zremrangebyrankHelper = helper.getNode("zremrangebyrank-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zremrangebyrankHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          delNode.receive({ topic: "test:zset:zrrbr" });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zremrangebyrankNode.receive({
          topic: "test:zset:zrrbr",
          payload: ["0", "1"],
        });
      });

      zaddNode.receive({
        topic: "test:zset:zrrbr",
        payload: ["1", "a", "2", "b", "3", "c"],
      });
    });
  });

  it("should ZREMRANGEBYLEX remove members in lex range", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zremrangebylex-node",
        type: "redis-command",
        server: "config1",
        command: "ZREMRANGEBYLEX",
        name: "ZREMRANGEBYLEX",
        topic: "",
        params: "[]",
        wires: [["zremrangebylex-helper"]],
      },
      { id: "zremrangebylex-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zremrangebylexNode = helper.getNode("zremrangebylex-node");
      const zremrangebylexHelper = helper.getNode("zremrangebylex-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zremrangebylexHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          delNode.receive({ topic: "test:zset:zrrbl" });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zremrangebylexNode.receive({
          topic: "test:zset:zrrbl",
          payload: ["[a", "[b"],
        });
      });

      zaddNode.receive({
        topic: "test:zset:zrrbl",
        payload: ["0", "a", "0", "b", "0", "c"],
      });
    });
  });

  it("should ZUNIONSTORE and ZUNION combine sorted sets", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd1-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD1",
        topic: "",
        params: "[]",
        wires: [["zadd1-helper"]],
      },
      { id: "zadd1-helper", type: "helper" },
      {
        id: "zadd2-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD2",
        topic: "",
        params: "[]",
        wires: [["zadd2-helper"]],
      },
      { id: "zadd2-helper", type: "helper" },
      {
        id: "zunionstore-node",
        type: "redis-command",
        server: "config1",
        command: "ZUNIONSTORE",
        name: "ZUNIONSTORE",
        topic: "",
        params: "[]",
        wires: [["zunionstore-helper"]],
      },
      { id: "zunionstore-helper", type: "helper" },
      {
        id: "zunion-node",
        type: "redis-command",
        server: "config1",
        command: "ZUNION",
        name: "ZUNION",
        topic: "",
        params: "[]",
        wires: [["zunion-helper"]],
      },
      { id: "zunion-helper", type: "helper" },
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
      const zadd1Node = helper.getNode("zadd1-node");
      const zadd1Helper = helper.getNode("zadd1-helper");
      const zadd2Node = helper.getNode("zadd2-node");
      const zadd2Helper = helper.getNode("zadd2-helper");
      const zunionstoreNode = helper.getNode("zunionstore-node");
      const zunionstoreHelper = helper.getNode("zunionstore-helper");
      const zunionNode = helper.getNode("zunion-node");
      const zunionHelper = helper.getNode("zunion-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zunionHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.equal(4);
          delNode.receive({
            payload: [
              "test:zset:zus1",
              "test:zset:zus2",
              "test:zset:zusdst",
            ],
          });
        } catch (err) {
          done(err);
        }
      });

      zunionstoreHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(4);
          zunionNode.receive({
            payload: ["2", "test:zset:zus1", "test:zset:zus2"],
          });
        } catch (err) {
          done(err);
        }
      });

      zadd2Helper.on("input", () => {
        zunionstoreNode.receive({
          topic: "test:zset:zusdst",
          payload: ["2", "test:zset:zus1", "test:zset:zus2"],
        });
      });

      zadd1Helper.on("input", () => {
        zadd2Node.receive({
          topic: "test:zset:zus2",
          payload: ["3", "c", "4", "d"],
        });
      });

      zadd1Node.receive({
        topic: "test:zset:zus1",
        payload: ["1", "a", "2", "b"],
      });
    });
  });

  it("should ZINTERSTORE and ZINTER intersect sorted sets", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd1-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD1",
        topic: "",
        params: "[]",
        wires: [["zadd1-helper"]],
      },
      { id: "zadd1-helper", type: "helper" },
      {
        id: "zadd2-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD2",
        topic: "",
        params: "[]",
        wires: [["zadd2-helper"]],
      },
      { id: "zadd2-helper", type: "helper" },
      {
        id: "zinterstore-node",
        type: "redis-command",
        server: "config1",
        command: "ZINTERSTORE",
        name: "ZINTERSTORE",
        topic: "",
        params: "[]",
        wires: [["zinterstore-helper"]],
      },
      { id: "zinterstore-helper", type: "helper" },
      {
        id: "zinter-node",
        type: "redis-command",
        server: "config1",
        command: "ZINTER",
        name: "ZINTER",
        topic: "",
        params: "[]",
        wires: [["zinter-helper"]],
      },
      { id: "zinter-helper", type: "helper" },
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
      const zadd1Node = helper.getNode("zadd1-node");
      const zadd1Helper = helper.getNode("zadd1-helper");
      const zadd2Node = helper.getNode("zadd2-node");
      const zadd2Helper = helper.getNode("zadd2-helper");
      const zinterstoreNode = helper.getNode("zinterstore-node");
      const zinterstoreHelper = helper.getNode("zinterstore-helper");
      const zinterNode = helper.getNode("zinter-node");
      const zinterHelper = helper.getNode("zinter-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zinterHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.containEql("b");
          delNode.receive({
            payload: ["test:zset:zis1", "test:zset:zis2", "test:zset:zisdst"],
          });
        } catch (err) {
          done(err);
        }
      });

      zinterstoreHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          zinterNode.receive({
            payload: ["2", "test:zset:zis1", "test:zset:zis2"],
          });
        } catch (err) {
          done(err);
        }
      });

      zadd2Helper.on("input", () => {
        zinterstoreNode.receive({
          topic: "test:zset:zisdst",
          payload: ["2", "test:zset:zis1", "test:zset:zis2"],
        });
      });

      zadd1Helper.on("input", () => {
        zadd2Node.receive({
          topic: "test:zset:zis2",
          payload: ["2", "b", "3", "c"],
        });
      });

      zadd1Node.receive({
        topic: "test:zset:zis1",
        payload: ["1", "a", "2", "b"],
      });
    });
  });

  it("should ZDIFFSTORE and ZDIFF compute difference of sorted sets", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd1-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD1",
        topic: "",
        params: "[]",
        wires: [["zadd1-helper"]],
      },
      { id: "zadd1-helper", type: "helper" },
      {
        id: "zadd2-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD2",
        topic: "",
        params: "[]",
        wires: [["zadd2-helper"]],
      },
      { id: "zadd2-helper", type: "helper" },
      {
        id: "zdiffstore-node",
        type: "redis-command",
        server: "config1",
        command: "ZDIFFSTORE",
        name: "ZDIFFSTORE",
        topic: "",
        params: "[]",
        wires: [["zdiffstore-helper"]],
      },
      { id: "zdiffstore-helper", type: "helper" },
      {
        id: "zdiff-node",
        type: "redis-command",
        server: "config1",
        command: "ZDIFF",
        name: "ZDIFF",
        topic: "",
        params: "[]",
        wires: [["zdiff-helper"]],
      },
      { id: "zdiff-helper", type: "helper" },
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
      const zadd1Node = helper.getNode("zadd1-node");
      const zadd1Helper = helper.getNode("zadd1-helper");
      const zadd2Node = helper.getNode("zadd2-node");
      const zadd2Helper = helper.getNode("zadd2-helper");
      const zdiffstoreNode = helper.getNode("zdiffstore-node");
      const zdiffstoreHelper = helper.getNode("zdiffstore-helper");
      const zdiffNode = helper.getNode("zdiff-node");
      const zdiffHelper = helper.getNode("zdiff-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zdiffHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.containEql("a");
          delNode.receive({
            payload: [
              "test:zset:zds1",
              "test:zset:zds2",
              "test:zset:zdsdst",
            ],
          });
        } catch (err) {
          done(err);
        }
      });

      zdiffstoreHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          zdiffNode.receive({
            payload: ["2", "test:zset:zds1", "test:zset:zds2"],
          });
        } catch (err) {
          done(err);
        }
      });

      zadd2Helper.on("input", () => {
        zdiffstoreNode.receive({
          topic: "test:zset:zdsdst",
          payload: ["2", "test:zset:zds1", "test:zset:zds2"],
        });
      });

      zadd1Helper.on("input", () => {
        zadd2Node.receive({
          topic: "test:zset:zds2",
          payload: ["2", "b"],
        });
      });

      zadd1Node.receive({
        topic: "test:zset:zds1",
        payload: ["1", "a", "2", "b"],
      });
    });
  });

  it("should ZSCAN iterate over sorted set members", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zscan-node",
        type: "redis-command",
        server: "config1",
        command: "ZSCAN",
        name: "ZSCAN",
        topic: "",
        params: "[]",
        wires: [["zscan-helper"]],
      },
      { id: "zscan-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zscanNode = helper.getNode("zscan-node");
      const zscanHelper = helper.getNode("zscan-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zscanHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.equal(2);
          msg.payload[1].should.be.an.Array();
          delNode.receive({ topic: "test:zset:zscan" });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zscanNode.receive({ topic: "test:zset:zscan", payload: "0" });
      });

      zaddNode.receive({
        topic: "test:zset:zscan",
        payload: ["1", "a", "2", "b"],
      });
    });
  });

  it("should ZINTERCARD return the count of intersection members", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd1-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD1",
        topic: "",
        params: "[]",
        wires: [["zadd1-helper"]],
      },
      { id: "zadd1-helper", type: "helper" },
      {
        id: "zadd2-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD2",
        topic: "",
        params: "[]",
        wires: [["zadd2-helper"]],
      },
      { id: "zadd2-helper", type: "helper" },
      {
        id: "zintercard-node",
        type: "redis-command",
        server: "config1",
        command: "ZINTERCARD",
        name: "ZINTERCARD",
        topic: "",
        params: "[]",
        wires: [["zintercard-helper"]],
      },
      { id: "zintercard-helper", type: "helper" },
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
      const zadd1Node = helper.getNode("zadd1-node");
      const zadd1Helper = helper.getNode("zadd1-helper");
      const zadd2Node = helper.getNode("zadd2-node");
      const zadd2Helper = helper.getNode("zadd2-helper");
      const zintercardNode = helper.getNode("zintercard-node");
      const zintercardHelper = helper.getNode("zintercard-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zintercardHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(1);
          delNode.receive({
            payload: ["test:zset:zic1", "test:zset:zic2"],
          });
        } catch (err) {
          done(err);
        }
      });

      zadd2Helper.on("input", () => {
        zintercardNode.receive({
          payload: ["2", "test:zset:zic1", "test:zset:zic2"],
        });
      });

      zadd1Helper.on("input", () => {
        zadd2Node.receive({
          topic: "test:zset:zic2",
          payload: ["1", "b", "2", "c"],
        });
      });

      zadd1Node.receive({
        topic: "test:zset:zic1",
        payload: ["1", "a", "2", "b"],
      });
    });
  });

  it("should ZMPOP pop the minimum element from a sorted set", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "zmpop-node",
        type: "redis-command",
        server: "config1",
        command: "ZMPOP",
        name: "ZMPOP",
        topic: "",
        params: "[]",
        wires: [["zmpop-helper"]],
      },
      { id: "zmpop-helper", type: "helper" },
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
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const zmpopNode = helper.getNode("zmpop-node");
      const zmpopHelper = helper.getNode("zmpop-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      zmpopHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal("test:zset:zmpop");
          msg.payload[1].should.be.an.Array();
          msg.payload[1][0][0].should.equal("a");
          delNode.receive({ topic: "test:zset:zmpop" });
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        zmpopNode.receive({
          payload: ["1", "test:zset:zmpop", "MIN"],
        });
      });

      zaddNode.receive({
        topic: "test:zset:zmpop",
        payload: ["1", "a", "2", "b"],
      });
    });
  });

  it("should BZMPOP return immediately when sorted set has data", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "bzmpop-node",
        type: "redis-command",
        server: "config1",
        command: "BZMPOP",
        name: "BZMPOP",
        block: true,
        topic: "",
        params: "[]",
        wires: [["bzmpop-helper"]],
      },
      { id: "bzmpop-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const bzmpopNode = helper.getNode("bzmpop-node");
      const bzmpopHelper = helper.getNode("bzmpop-helper");

      bzmpopHelper.on("input", (msg) => {
        try {
          msg.payload.should.not.be.null();
          msg.payload.should.be.an.Array();
          done();
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        bzmpopNode.receive({
          payload: ["1", "1", "test:zset:bzmpop", "MIN"],
        });
      });

      zaddNode.receive({
        topic: "test:zset:bzmpop",
        payload: ["1", "a"],
      });
    });
  });

  it("should BZPOPMIN return immediately when sorted set has data", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "bzpopmin-node",
        type: "redis-command",
        server: "config1",
        command: "BZPOPMIN",
        name: "BZPOPMIN",
        block: true,
        topic: "",
        params: "[]",
        wires: [["bzpopmin-helper"]],
      },
      { id: "bzpopmin-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const bzpopminNode = helper.getNode("bzpopmin-node");
      const bzpopminHelper = helper.getNode("bzpopmin-helper");

      bzpopminHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal("test:zset:bzpopmin");
          msg.payload[1].should.equal("a");
          done();
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        bzpopminNode.receive({
          payload: ["test:zset:bzpopmin", "1"],
        });
      });

      zaddNode.receive({
        topic: "test:zset:bzpopmin",
        payload: ["1", "a", "2", "b"],
      });
    });
  });

  it("should BZPOPMAX return immediately when sorted set has data", function (done) {
    const flow = [
      configNode,
      {
        id: "zadd-node",
        type: "redis-command",
        server: "config1",
        command: "ZADD",
        name: "ZADD",
        topic: "",
        params: "[]",
        wires: [["zadd-helper"]],
      },
      { id: "zadd-helper", type: "helper" },
      {
        id: "bzpopmax-node",
        type: "redis-command",
        server: "config1",
        command: "BZPOPMAX",
        name: "BZPOPMAX",
        block: true,
        topic: "",
        params: "[]",
        wires: [["bzpopmax-helper"]],
      },
      { id: "bzpopmax-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const zaddNode = helper.getNode("zadd-node");
      const zaddHelper = helper.getNode("zadd-helper");
      const bzpopmaxNode = helper.getNode("bzpopmax-node");
      const bzpopmaxHelper = helper.getNode("bzpopmax-helper");

      bzpopmaxHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload[0].should.equal("test:zset:bzpopmax");
          msg.payload[1].should.equal("b");
          done();
        } catch (err) {
          done(err);
        }
      });

      zaddHelper.on("input", () => {
        bzpopmaxNode.receive({
          payload: ["test:zset:bzpopmax", "1"],
        });
      });

      zaddNode.receive({
        topic: "test:zset:bzpopmax",
        payload: ["1", "a", "2", "b"],
      });
    });
  });
});
