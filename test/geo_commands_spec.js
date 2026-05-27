const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { cleanupKeys } = require("./helpers/cleanup");

helper.init(require.resolve("node-red"));

describe("Geo commands", function () {
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
        cleanupKeys("test:geo:*", done);
      });
    });
  });

  it("should GEOADD members and GEODIST between them", function (done) {
    const flow = [
      configNode,
      {
        id: "geoadd-node",
        type: "redis-command",
        server: "config1",
        command: "GEOADD",
        name: "GEOADD",
        topic: "",
        params: "[]",
        wires: [["geoadd-helper"]],
      },
      { id: "geoadd-helper", type: "helper" },
      {
        id: "geodist-node",
        type: "redis-command",
        server: "config1",
        command: "GEODIST",
        name: "GEODIST",
        topic: "",
        params: "[]",
        wires: [["geodist-helper"]],
      },
      { id: "geodist-helper", type: "helper" },
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
      const geoaddNode = helper.getNode("geoadd-node");
      const geoaddHelper = helper.getNode("geoadd-helper");
      const geodistNode = helper.getNode("geodist-node");
      const geodistHelper = helper.getNode("geodist-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      geodistHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.String();
          parseFloat(msg.payload).should.be.above(100);
          delNode.receive({ topic: "test:geo:geokey" });
        } catch (err) {
          done(err);
        }
      });

      geoaddHelper.on("input", (msg) => {
        try {
          msg.payload.should.equal(2);
          geodistNode.receive({
            topic: "test:geo:geokey",
            payload: ["Palermo", "Catania", "km"],
          });
        } catch (err) {
          done(err);
        }
      });

      geoaddNode.receive({
        topic: "test:geo:geokey",
        payload: [
          "13.361389",
          "38.115556",
          "Palermo",
          "15.087269",
          "37.502669",
          "Catania",
        ],
      });
    });
  });

  it("should GEOHASH return base32 encoded hashes", function (done) {
    const flow = [
      configNode,
      {
        id: "geoadd-node",
        type: "redis-command",
        server: "config1",
        command: "GEOADD",
        name: "GEOADD",
        topic: "",
        params: "[]",
        wires: [["geoadd-helper"]],
      },
      { id: "geoadd-helper", type: "helper" },
      {
        id: "geohash-node",
        type: "redis-command",
        server: "config1",
        command: "GEOHASH",
        name: "GEOHASH",
        topic: "",
        params: "[]",
        wires: [["geohash-helper"]],
      },
      { id: "geohash-helper", type: "helper" },
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
      const geoaddNode = helper.getNode("geoadd-node");
      const geoaddHelper = helper.getNode("geoadd-helper");
      const geohashNode = helper.getNode("geohash-node");
      const geohashHelper = helper.getNode("geohash-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      geohashHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.equal(1);
          msg.payload[0].should.be.a.String();
          msg.payload[0].length.should.equal(11);
          delNode.receive({ topic: "test:geo:hashkey" });
        } catch (err) {
          done(err);
        }
      });

      geoaddHelper.on("input", () => {
        geohashNode.receive({
          topic: "test:geo:hashkey",
          payload: "Palermo",
        });
      });

      geoaddNode.receive({
        topic: "test:geo:hashkey",
        payload: ["13.361389", "38.115556", "Palermo"],
      });
    });
  });

  it("should GEOPOS return longitude and latitude", function (done) {
    const flow = [
      configNode,
      {
        id: "geoadd-node",
        type: "redis-command",
        server: "config1",
        command: "GEOADD",
        name: "GEOADD",
        topic: "",
        params: "[]",
        wires: [["geoadd-helper"]],
      },
      { id: "geoadd-helper", type: "helper" },
      {
        id: "geopos-node",
        type: "redis-command",
        server: "config1",
        command: "GEOPOS",
        name: "GEOPOS",
        topic: "",
        params: "[]",
        wires: [["geopos-helper"]],
      },
      { id: "geopos-helper", type: "helper" },
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
      const geoaddNode = helper.getNode("geoadd-node");
      const geoaddHelper = helper.getNode("geoadd-helper");
      const geoposNode = helper.getNode("geopos-node");
      const geoposHelper = helper.getNode("geopos-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      geoposHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.length.should.equal(1);
          msg.payload[0].should.be.an.Array();
          msg.payload[0].length.should.equal(2);
          delNode.receive({ topic: "test:geo:poskey" });
        } catch (err) {
          done(err);
        }
      });

      geoaddHelper.on("input", () => {
        geoposNode.receive({
          topic: "test:geo:poskey",
          payload: "Palermo",
        });
      });

      geoaddNode.receive({
        topic: "test:geo:poskey",
        payload: ["13.361389", "38.115556", "Palermo"],
      });
    });
  });

  it("should GEOSEARCH members within radius", function (done) {
    const flow = [
      configNode,
      {
        id: "geoadd-node",
        type: "redis-command",
        server: "config1",
        command: "GEOADD",
        name: "GEOADD",
        topic: "",
        params: "[]",
        wires: [["geoadd-helper"]],
      },
      { id: "geoadd-helper", type: "helper" },
      {
        id: "geosearch-node",
        type: "redis-command",
        server: "config1",
        command: "GEOSEARCH",
        name: "GEOSEARCH",
        topic: "",
        params: "[]",
        wires: [["geosearch-helper"]],
      },
      { id: "geosearch-helper", type: "helper" },
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
      const geoaddNode = helper.getNode("geoadd-node");
      const geoaddHelper = helper.getNode("geoadd-helper");
      const geosearchNode = helper.getNode("geosearch-node");
      const geosearchHelper = helper.getNode("geosearch-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      geosearchHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.containEql("Palermo");
          msg.payload.should.containEql("Catania");
          delNode.receive({ topic: "test:geo:searchkey" });
        } catch (err) {
          done(err);
        }
      });

      geoaddHelper.on("input", () => {
        geosearchNode.receive({
          topic: "test:geo:searchkey",
          payload: [
            "FROMMEMBER",
            "Palermo",
            "BYRADIUS",
            "200",
            "km",
            "ASC",
          ],
        });
      });

      geoaddNode.receive({
        topic: "test:geo:searchkey",
        payload: [
          "13.361389",
          "38.115556",
          "Palermo",
          "15.087269",
          "37.502669",
          "Catania",
        ],
      });
    });
  });

  it("should GEOSEARCHSTORE results into a sorted set", function (done) {
    const flow = [
      configNode,
      {
        id: "geoadd-node",
        type: "redis-command",
        server: "config1",
        command: "GEOADD",
        name: "GEOADD",
        topic: "",
        params: "[]",
        wires: [["geoadd-helper"]],
      },
      { id: "geoadd-helper", type: "helper" },
      {
        id: "geosearchstore-node",
        type: "redis-command",
        server: "config1",
        command: "GEOSEARCHSTORE",
        name: "GEOSEARCHSTORE",
        topic: "",
        params: "[]",
        wires: [["geosearchstore-helper"]],
      },
      { id: "geosearchstore-helper", type: "helper" },
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
      const geoaddNode = helper.getNode("geoadd-node");
      const geoaddHelper = helper.getNode("geoadd-helper");
      const geosearchstoreNode = helper.getNode("geosearchstore-node");
      const geosearchstoreHelper = helper.getNode("geosearchstore-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      geosearchstoreHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.a.Number();
          msg.payload.should.be.aboveOrEqual(1);
          delNode.receive({
            payload: ["test:geo:storesrc", "test:geo:storedst"],
          });
        } catch (err) {
          done(err);
        }
      });

      geoaddHelper.on("input", () => {
        geosearchstoreNode.receive({
          topic: "test:geo:storedst",
          payload: [
            "test:geo:storesrc",
            "FROMMEMBER",
            "Palermo",
            "BYRADIUS",
            "200",
            "km",
            "ASC",
          ],
        });
      });

      geoaddNode.receive({
        topic: "test:geo:storesrc",
        payload: [
          "13.361389",
          "38.115556",
          "Palermo",
          "15.087269",
          "37.502669",
          "Catania",
        ],
      });
    });
  });

  it("should GEORADIUS return members within radius (deprecated command)", function (done) {
    const flow = [
      configNode,
      {
        id: "geoadd-node",
        type: "redis-command",
        server: "config1",
        command: "GEOADD",
        name: "GEOADD",
        topic: "",
        params: "[]",
        wires: [["geoadd-helper"]],
      },
      { id: "geoadd-helper", type: "helper" },
      {
        id: "georadius-node",
        type: "redis-command",
        server: "config1",
        command: "GEORADIUS",
        name: "GEORADIUS",
        topic: "",
        params: "[]",
        wires: [["georadius-helper"]],
      },
      { id: "georadius-helper", type: "helper" },
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
      const geoaddNode = helper.getNode("geoadd-node");
      const geoaddHelper = helper.getNode("geoadd-helper");
      const georadiusNode = helper.getNode("georadius-node");
      const georadiusHelper = helper.getNode("georadius-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      georadiusHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          delNode.receive({ topic: "test:geo:radiuskey" });
        } catch (err) {
          done(err);
        }
      });

      geoaddHelper.on("input", () => {
        georadiusNode.receive({
          topic: "test:geo:radiuskey",
          payload: ["15.0", "37.0", "200", "km"],
        });
      });

      geoaddNode.receive({
        topic: "test:geo:radiuskey",
        payload: [
          "13.361389",
          "38.115556",
          "Palermo",
          "15.087269",
          "37.502669",
          "Catania",
        ],
      });
    });
  });

  it("should GEORADIUSBYMEMBER return members within radius (deprecated command)", function (done) {
    const flow = [
      configNode,
      {
        id: "geoadd-node",
        type: "redis-command",
        server: "config1",
        command: "GEOADD",
        name: "GEOADD",
        topic: "",
        params: "[]",
        wires: [["geoadd-helper"]],
      },
      { id: "geoadd-helper", type: "helper" },
      {
        id: "georadiusbymember-node",
        type: "redis-command",
        server: "config1",
        command: "GEORADIUSBYMEMBER",
        name: "GEORADIUSBYMEMBER",
        topic: "",
        params: "[]",
        wires: [["georadiusbymember-helper"]],
      },
      { id: "georadiusbymember-helper", type: "helper" },
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
      const geoaddNode = helper.getNode("geoadd-node");
      const geoaddHelper = helper.getNode("geoadd-helper");
      const georadiusbymemberNode = helper.getNode("georadiusbymember-node");
      const georadiusbymemberHelper = helper.getNode("georadiusbymember-helper");
      const delNode = helper.getNode("del-node");
      const delHelper = helper.getNode("del-helper");

      delHelper.on("input", () => {
        done();
      });

      georadiusbymemberHelper.on("input", (msg) => {
        try {
          msg.payload.should.be.an.Array();
          msg.payload.should.containEql("Catania");
          delNode.receive({ topic: "test:geo:membkey" });
        } catch (err) {
          done(err);
        }
      });

      geoaddHelper.on("input", () => {
        georadiusbymemberNode.receive({
          topic: "test:geo:membkey",
          payload: ["Palermo", "200", "km"],
        });
      });

      geoaddNode.receive({
        topic: "test:geo:membkey",
        payload: [
          "13.361389",
          "38.115556",
          "Palermo",
          "15.087269",
          "37.502669",
          "Catania",
        ],
      });
    });
  });
});
