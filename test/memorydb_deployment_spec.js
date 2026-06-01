"use strict";

const helper = require("node-red-node-test-helper");
const Redis = require("ioredis");
const redisNode = require("../redis.js");
const { commandNode, expectError, helperNode, invoke, load } = require("./helpers/topology");
const {
  clusterProneFlow,
  clusterProneKeys,
  runClusterProneCrossSlotFailures,
  runClusterProneSuccessCases,
} = require("./helpers/cluster-prone");

helper.init(require.resolve("node-red"));

const describeMemoryDb =
  process.env.REDIS_DEPLOYMENT === "memorydb" && process.env.MEMORYDB_ENABLED === "1"
    ? describe
    : describe.skip;

function memoryDbNodeOptions() {
  return [
    {
      dnsLookupStrategy: "identity",
      host: process.env.MEMORYDB_ENDPOINT,
      port: Number(process.env.MEMORYDB_PORT || 6379),
      username: process.env.MEMORYDB_USERNAME,
      password: process.env.MEMORYDB_PASSWORD,
    },
  ];
}

function memoryDbConfigNode() {
  return {
    id: "config1",
    type: "redis-config",
    name: "AWS MemoryDB",
    options: JSON.stringify(memoryDbNodeOptions()),
    optionsType: "json",
    cluster: true,
  };
}

function directMemoryDb() {
  return new Redis.Cluster(memoryDbNodeOptions(), {
    dnsLookup: (address, callback) => callback(null, address),
    redisOptions: {
      tls: {},
      username: process.env.MEMORYDB_USERNAME,
      password: process.env.MEMORYDB_PASSWORD,
      connectTimeout: 3000,
      maxRetriesPerRequest: 1,
    },
    slotsRefreshTimeout: 5000,
  });
}

async function cleanupMemoryDbKeys() {
  const client = directMemoryDb();
  const keys = [
    "test:memorydb:{basic}:one",
    "test:memorydb:{basic}:two",
    "test:memorydb:{lua}:counter",
  ].concat(clusterProneKeys("test:memorydb"));
  try {
    await Promise.all(keys.map((key) => client.del(key).catch(() => null)));
  } finally {
    client.disconnect();
  }
}

function commandFlow(defs) {
  const flow = [memoryDbConfigNode()];
  defs.forEach((def) => {
    flow.push(commandNode(def.id, def.command));
    flow.push(helperNode(def.id));
  });
  return flow;
}

async function loadScriptOnMemoryDb(script) {
  const client = directMemoryDb();
  try {
    await client.ping();
    const shas = await Promise.all(client.nodes("master").map((node) => node.script("load", script)));
    return shas[0];
  } finally {
    client.disconnect();
  }
}

async function runDirectMemoryDbTransaction() {
  const client = directMemoryDb();
  const txA = "test:memorydb:{prone}:tx-a";
  const txB = "test:memorydb:{prone}:tx-b";
  try {
    await client.watch(txA, txB);
    const res = await client.multi().set(txA, "direct-tx").get(txA).exec();
    res.should.be.an.Array();
    res[0][1].should.equal("OK");
    res[1][1].should.equal("direct-tx");
  } finally {
    client.disconnect();
  }
}

describeMemoryDb("AWS MemoryDB deployment", function () {
  this.timeout(45000);

  beforeEach(function (done) {
    helper.startServer(done);
  });

  afterEach(function (done) {
    helper
      .unload()
      .then(cleanupMemoryDbKeys)
      .then(() => helper.stopServer(done))
      .catch(done);
  });

  it("runs authenticated cluster command smoke coverage", async function () {
    await load(
      helper,
      redisNode,
      commandFlow([
        { id: "ping", command: "PING" },
        { id: "acl", command: "ACL" },
        { id: "set", command: "SET" },
        { id: "get", command: "GET" },
        { id: "del", command: "DEL" },
        { id: "mset", command: "MSET" },
        { id: "mget", command: "MGET" },
        { id: "crossslot", command: "MSET" },
      ])
    );

    (await invoke(helper, "ping")).should.equal("PONG");
    (await invoke(helper, "acl", { payload: ["WHOAMI"] })).should.equal(
      process.env.MEMORYDB_USERNAME
    );
    (
      await invoke(helper, "set", {
        topic: "test:memorydb:{basic}:one",
        payload: "value",
      })
    ).should.equal("OK");
    (
      await invoke(helper, "get", {
        topic: "test:memorydb:{basic}:one",
      })
    ).should.equal("value");
    (
      await invoke(helper, "mset", {
        payload: ["test:memorydb:{basic}:one", "1", "test:memorydb:{basic}:two", "2"],
      })
    ).should.equal("OK");
    (
      await invoke(helper, "mget", {
        payload: ["test:memorydb:{basic}:one", "test:memorydb:{basic}:two"],
      })
    ).should.eql(["1", "2"]);

    const err = await expectError(helper, "crossslot", {
      payload: ["test:memorydb:{slot-a}:one", "1", "test:memorydb:{slot-b}:two", "2"],
    });
    err.message.should.match(/CROSSSLOT|same slot/i);

    (
      await invoke(helper, "del", {
        payload: ["test:memorydb:{basic}:one", "test:memorydb:{basic}:two"],
      })
    ).should.be.a.Number();
  });

  it("runs same-slot cluster Lua through redis-lua-script", async function () {
    const flow = [
      memoryDbConfigNode(),
      {
        id: "lua-node",
        type: "redis-lua-script",
        server: "config1",
        name: "memorydb-lua",
        keyval: 1,
        func: "return redis.call('INCRBY', KEYS[1], ARGV[1])",
        stored: false,
        block: false,
        wires: [["lua-helper"]],
      },
      helperNode("lua"),
    ];
    await load(helper, redisNode, flow);

    (
      await invoke(helper, "lua", {
        payload: ["test:memorydb:{lua}:counter", "3"],
      })
    ).should.equal(3);
  });

  it("runs Redis 7.2 cluster-prone commands with same-slot keys", async function () {
    const script =
      "redis.call('SET', KEYS[1], ARGV[1]); redis.call('SET', KEYS[2], ARGV[1]); return {redis.call('GET', KEYS[1]), redis.call('GET', KEYS[2])}";
    const scriptSha = await loadScriptOnMemoryDb(script);
    await load(helper, redisNode, clusterProneFlow(memoryDbConfigNode()));

    await runClusterProneSuccessCases(helper, {
      prefix: "test:memorydb",
      scriptSha,
      nodeTransaction: false,
      selectSupported: false,
    });
    await runDirectMemoryDbTransaction();
  });

  it("rejects Redis 7.2 cluster-prone commands with cross-slot keys", async function () {
    const script = "return {KEYS[1], KEYS[2]}";
    const scriptSha = await loadScriptOnMemoryDb(script);
    await load(helper, redisNode, clusterProneFlow(memoryDbConfigNode()));

    await runClusterProneCrossSlotFailures(helper, {
      prefix: "test:memorydb",
      scriptSha,
    });
  });
});
