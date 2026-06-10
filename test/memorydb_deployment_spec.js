"use strict";

const helper = require("node-red-node-test-helper");
const Redis = require("ioredis");
const redisNode = require("../redis.js");
const { commandNode, expectError, helperNode, invoke, load } = require("./helpers/topology");
const { waitForNodeProp } = require("./helpers/wait");
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

function memoryDbEnvConfigNode(envName) {
  return {
    id: "config1",
    type: "redis-config",
    name: "AWS MemoryDB (env)",
    options: envName,
    optionsType: "env",
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
    "test:memorydb:{lua}:ro",
    "test:memorydb:{lua}:fn",
    "test:memorydb:{lua}:blk",
    "test:memorydb:{lua}:blkfn",
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
    const shas = await Promise.all(
      client.nodes("master").map((node) => node.script("load", script))
    );
    return shas[0];
  } finally {
    client.disconnect();
  }
}

async function memoryDbSupportsFunctions() {
  const client = directMemoryDb();
  try {
    await client.function("list");
    return true;
  } catch (err) {
    return false;
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

  it("runs read-only EVAL and (when supported) FCALL", async function () {
    const supportsFunctions = await memoryDbSupportsFunctions();

    const flow = [
      memoryDbConfigNode(),
      {
        id: "ro-node",
        type: "redis-lua-script",
        server: "config1",
        name: "memorydb-ro",
        mode: "script",
        readonly: true,
        stored: false,
        keyval: 1,
        func: "return redis.call('GET', KEYS[1])",
        block: false,
        wires: [["ro-helper"]],
      },
      helperNode("ro"),
    ];
    // The read-only EVAL coverage always runs; only the FCALL portion is
    // gated on engine support for Redis Functions.
    if (supportsFunctions) {
      flow.push({
        id: "fn-node",
        type: "redis-lua-script",
        server: "config1",
        name: "memorydb-fn",
        mode: "function",
        readonly: false,
        keyval: 1,
        func: "#!lua name=memorydblib\nredis.register_function('memorydbfn', function(keys, args) redis.call('SET', keys[1], args[1]); return redis.call('GET', keys[1]) end)",
        fname: "memorydbfn",
        block: false,
        wires: [["fn-helper"]],
      });
      flow.push(helperNode("fn"));
    }

    await load(helper, redisNode, flow);

    const client = directMemoryDb();
    try {
      await client.set("test:memorydb:{lua}:ro", "ro-value");
    } finally {
      client.disconnect();
    }
    (await invoke(helper, "ro", { payload: ["test:memorydb:{lua}:ro"] })).should.equal("ro-value");

    if (supportsFunctions) {
      const fnNode = helper.getNode("fn-node");
      await waitForNodeProp(fnNode, "libname");
      (
        await invoke(helper, "fn", { payload: ["test:memorydb:{lua}:fn", "fn-value"] })
      ).should.equal("fn-value");
    }
  });

  it("runs block-mode (dedicated connection) Script and (when supported) Function", async function () {
    // Execution-level coverage only: the server-side dedicated-connection proof
    // (CLIENT LIST counting by connectionName) lives in scripting_commands_spec
    // and the sentinel spec — the cluster-style config path cannot carry an
    // ioredis connectionName, and counting clients on shared AWS infrastructure
    // would be unreliable anyway.
    const supportsFunctions = await memoryDbSupportsFunctions();
    const flow = [
      memoryDbConfigNode(),
      {
        id: "blk-script-node",
        type: "redis-lua-script",
        server: "config1",
        name: "memorydb-block-script",
        mode: "script",
        readonly: false,
        stored: false,
        keyval: 1,
        func: "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])",
        block: true,
        wires: [["blk-script-helper"]],
      },
      helperNode("blk-script"),
    ];
    if (supportsFunctions) {
      flow.push({
        id: "blk-fn-node",
        type: "redis-lua-script",
        server: "config1",
        name: "memorydb-block-fn",
        mode: "function",
        readonly: false,
        keyval: 1,
        func: "#!lua name=blockmemorydblib\nredis.register_function('blockmemorydbfn', function(keys, args) redis.call('SET', keys[1], args[1]); return redis.call('GET', keys[1]) end)",
        fname: "blockmemorydbfn",
        block: true,
        wires: [["blk-fn-helper"]],
      });
      flow.push(helperNode("blk-fn"));
    }

    await load(helper, redisNode, flow);

    (
      await invoke(helper, "blk-script", { payload: ["test:memorydb:{lua}:blk", "s-value"] })
    ).should.equal("s-value");

    if (supportsFunctions) {
      const fnNode = helper.getNode("blk-fn-node");
      await waitForNodeProp(fnNode, "libname");
      (
        await invoke(helper, "blk-fn", { payload: ["test:memorydb:{lua}:blkfn", "f-value"] })
      ).should.equal("f-value");
    }
  });

  it("authenticates with env-var optionsType (cluster options JSON read from an env var)", async function () {
    const ENV_NAME = "AWS_MEMORYDB_OPTIONS_JSON";
    const original = process.env[ENV_NAME];
    process.env[ENV_NAME] = JSON.stringify(memoryDbNodeOptions());
    try {
      await load(helper, redisNode, [
        memoryDbEnvConfigNode(ENV_NAME),
        commandNode("ping-env", "PING"),
        helperNode("ping-env"),
        commandNode("acl-env", "ACL"),
        helperNode("acl-env"),
        commandNode("set-env", "SET"),
        helperNode("set-env"),
        commandNode("get-env", "GET"),
        helperNode("get-env"),
        commandNode("del-env", "DEL"),
        helperNode("del-env"),
      ]);

      (await invoke(helper, "ping-env")).should.equal("PONG");
      (await invoke(helper, "acl-env", { payload: ["WHOAMI"] })).should.equal(
        process.env.MEMORYDB_USERNAME
      );
      (
        await invoke(helper, "set-env", {
          topic: "test:memorydb:{basic}:one",
          payload: "env-value",
        })
      ).should.equal("OK");
      (
        await invoke(helper, "get-env", {
          topic: "test:memorydb:{basic}:one",
        })
      ).should.equal("env-value");
      (
        await invoke(helper, "del-env", {
          payload: ["test:memorydb:{basic}:one"],
        })
      ).should.be.a.Number();
    } finally {
      if (original === undefined) {
        delete process.env[ENV_NAME];
      } else {
        process.env[ENV_NAME] = original;
      }
    }
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
