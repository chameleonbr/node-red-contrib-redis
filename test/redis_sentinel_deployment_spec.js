"use strict";

const helper = require("node-red-node-test-helper");
const Redis = require("ioredis");
const redisNode = require("../redis.js");
const { commandNode, helperNode, invoke, load } = require("./helpers/topology");
const { waitForNodeProp } = require("./helpers/wait");
const { clusterProneFlow, runClusterProneSuccessCases } = require("./helpers/cluster-prone");

helper.init(require.resolve("node-red"));

const describeSentinel =
  process.env.REDIS_DEPLOYMENT === "sentinel-auth" ? describe : describe.skip;

function auth() {
  return {
    username: process.env.REDIS_USERNAME,
    password: process.env.REDIS_PASSWORD,
  };
}

function sentinelEndpoints() {
  return (process.env.REDIS_SENTINELS || "127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381")
    .split(",")
    .map((item) => {
      const [host, port] = item.split(":");
      return { host, port: Number(port) };
    });
}

function sentinelOptions() {
  return Object.assign(
    {
      sentinels: sentinelEndpoints(),
      name: process.env.REDIS_SENTINEL_MASTER_NAME || "mymaster",
      connectTimeout: 1000,
      maxRetriesPerRequest: 1,
    },
    auth()
  );
}

function sentinelConfigNode() {
  return {
    id: "config1",
    type: "redis-config",
    name: "SentinelAuth",
    options: JSON.stringify(sentinelOptions()),
    optionsType: "json",
    cluster: false,
  };
}

function directRedis() {
  return new Redis(sentinelOptions());
}

function directSentinel(endpoint = sentinelEndpoints()[0]) {
  return new Redis({
    host: endpoint.host,
    port: endpoint.port,
    connectTimeout: 1000,
    maxRetriesPerRequest: 1,
    retryStrategy: null,
  });
}

async function currentMaster() {
  let lastError;
  for (const endpoint of sentinelEndpoints()) {
    const sentinel = directSentinel(endpoint);
    try {
      const res = await sentinel.call(
        "SENTINEL",
        "get-master-addr-by-name",
        process.env.REDIS_SENTINEL_MASTER_NAME || "mymaster"
      );
      sentinel.disconnect();
      return { host: res[0], port: Number(res[1]) };
    } catch (err) {
      lastError = err;
      sentinel.disconnect();
    }
  }
  throw lastError || new Error("No sentinel returned a master");
}

async function waitForMasterChange(oldPort) {
  const deadline = Date.now() + 30000;
  let lastMaster;
  while (Date.now() < deadline) {
    lastMaster = await currentMaster();
    if (lastMaster.port !== oldPort) {
      const client = new Redis(
        Object.assign(
          {
            host: lastMaster.host,
            port: lastMaster.port,
            connectTimeout: 1000,
            maxRetriesPerRequest: 1,
            retryStrategy: null,
          },
          auth()
        )
      );
      try {
        const role = await client.role();
        if (role[0] === "master") {
          client.disconnect();
          return lastMaster;
        }
      } finally {
        client.disconnect();
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(
    `Sentinel did not fail over from port ${oldPort}; current=${JSON.stringify(lastMaster)}`
  );
}

function sentinelRowToObject(row) {
  const out = {};
  for (let i = 0; i < row.length; i += 2) {
    out[row[i]] = row[i + 1];
  }
  return out;
}

async function sentinelReplicas() {
  let lastError;
  for (const endpoint of sentinelEndpoints()) {
    const sentinel = directSentinel(endpoint);
    try {
      const rows = await sentinel.call(
        "SENTINEL",
        "slaves",
        process.env.REDIS_SENTINEL_MASTER_NAME || "mymaster"
      );
      sentinel.disconnect();
      return rows.map(sentinelRowToObject);
    } catch (err) {
      lastError = err;
      sentinel.disconnect();
    }
  }
  throw lastError || new Error("No sentinel returned replicas");
}

async function waitForPromotableReplica() {
  const deadline = Date.now() + 20000;
  let replicas = [];
  while (Date.now() < deadline) {
    replicas = await sentinelReplicas();
    if (
      replicas.some((replica) => {
        const flags = String(replica.flags || "");
        return (
          !flags.includes("s_down") &&
          !flags.includes("o_down") &&
          !flags.includes("disconnected") &&
          replica["master-link-status"] === "ok" &&
          replica["slave-priority"] !== "0"
        );
      })
    ) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`Sentinel has no promotable replica: ${JSON.stringify(replicas)}`);
}

async function requestFailover() {
  const deadline = Date.now() + 25000;
  let lastError;
  while (Date.now() < deadline) {
    await waitForPromotableReplica();
    const sentinel = directSentinel();
    try {
      await sentinel.call(
        "SENTINEL",
        "failover",
        process.env.REDIS_SENTINEL_MASTER_NAME || "mymaster"
      );
      sentinel.disconnect();
      return;
    } catch (err) {
      lastError = err;
      sentinel.disconnect();
      if (!err.message || !/NOGOODSLAVE/i.test(err.message)) {
        throw err;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw lastError || new Error("Sentinel failover did not start");
}

async function cleanupSentinelKeys() {
  const client = directRedis();
  try {
    const keys = await client.keys("test:sentinel:*");
    if (keys.length > 0) {
      await client.del(keys);
    }
  } finally {
    client.disconnect();
  }
}

function commandFlow(defs) {
  const flow = [sentinelConfigNode()];
  defs.forEach((def) => {
    flow.push(commandNode(def.id, def.command));
    flow.push(helperNode(def.id));
  });
  return flow;
}

async function loadScriptOnSentinel(script) {
  const client = directRedis();
  try {
    return await client.script("load", script);
  } finally {
    client.disconnect();
  }
}

describeSentinel("Redis Sentinel auth deployment", function () {
  this.timeout(40000);

  beforeEach(function (done) {
    helper.startServer(done);
  });

  afterEach(function (done) {
    helper
      .unload()
      .then(cleanupSentinelKeys)
      .then(() => helper.stopServer(done))
      .catch(done);
  });

  it("runs command and simple Lua coverage through Sentinel discovery", async function () {
    const flow = commandFlow([
      { id: "ping", command: "PING" },
      { id: "acl", command: "ACL" },
      { id: "set", command: "SET" },
      { id: "get", command: "GET" },
      { id: "del", command: "DEL" },
    ]);
    flow.push({
      id: "lua-node",
      type: "redis-lua-script",
      server: "config1",
      name: "sentinel-lua",
      keyval: 1,
      func: "redis.call('SET', KEYS[1], ARGV[1])\nreturn redis.call('GET', KEYS[1])",
      stored: false,
      block: false,
      wires: [["lua-helper"]],
    });
    flow.push(helperNode("lua"));

    await load(helper, redisNode, flow);

    (await invoke(helper, "ping")).should.equal("PONG");
    (await invoke(helper, "acl", { payload: ["WHOAMI"] })).should.equal(process.env.REDIS_USERNAME);
    (
      await invoke(helper, "set", {
        topic: "test:sentinel:basic",
        payload: "value",
      })
    ).should.equal("OK");
    (await invoke(helper, "get", { topic: "test:sentinel:basic" })).should.equal("value");
    (
      await invoke(helper, "lua", {
        payload: ["test:sentinel:lua", "script-value"],
      })
    ).should.equal("script-value");
    (
      await invoke(helper, "del", {
        payload: ["test:sentinel:basic", "test:sentinel:lua"],
      })
    ).should.be.a.Number();
  });

  it("runs FCALL and read-only EVAL through Sentinel discovery", async function () {
    const lib = [
      "#!lua name=sentinellib",
      "redis.register_function('sentinelfn', function(keys, args) redis.call('SET', keys[1], args[1]); return redis.call('GET', keys[1]) end)",
    ].join("\n");
    const flow = [
      sentinelConfigNode(),
      {
        id: "fn-node",
        type: "redis-lua-script",
        server: "config1",
        name: "sentinel-fn",
        mode: "function",
        readonly: false,
        keyval: 1,
        func: lib,
        fname: "sentinelfn",
        block: false,
        wires: [["fn-helper"]],
      },
      helperNode("fn"),
      {
        id: "ro-node",
        type: "redis-lua-script",
        server: "config1",
        name: "sentinel-ro",
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

    await load(helper, redisNode, flow);

    const fnNode = helper.getNode("fn-node");
    await waitForNodeProp(fnNode, "libname");
    (await invoke(helper, "fn", { payload: ["test:sentinel:fn", "fn-value"] })).should.equal(
      "fn-value"
    );
    // Seed via the function, then read it back read-only.
    (await invoke(helper, "ro", { payload: ["test:sentinel:fn"] })).should.equal("fn-value");
  });

  it("runs block-mode (dedicated connection) Script and Function through Sentinel", async function () {
    // The config carries an ioredis connectionName so the master's CLIENT LIST
    // proves, server-side, that each block node opened its own connection
    // while the non-block node uses the shared pooled one.
    const CONN_NAME = "lua-block-sentinel";
    const lib = [
      "#!lua name=blocksentinellib",
      "redis.register_function('blocksentinelfn', function(keys, args) redis.call('SET', keys[1], args[1]); return redis.call('GET', keys[1]) end)",
    ].join("\n");
    const namedConfig = {
      id: "config1",
      type: "redis-config",
      name: "SentinelAuthNamed",
      options: JSON.stringify(Object.assign(sentinelOptions(), { connectionName: CONN_NAME })),
      optionsType: "json",
      cluster: false,
    };
    const flow = [
      namedConfig,
      {
        id: "shared-script-node",
        type: "redis-lua-script",
        server: "config1",
        name: "sentinel-shared-script",
        mode: "script",
        readonly: false,
        stored: false,
        keyval: 0,
        func: "return 1",
        block: false,
        wires: [["shared-script-helper"]],
      },
      helperNode("shared-script"),
      {
        id: "blk-script-node",
        type: "redis-lua-script",
        server: "config1",
        name: "sentinel-block-script",
        mode: "script",
        readonly: false,
        stored: false,
        keyval: 1,
        func: "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])",
        block: true,
        wires: [["blk-script-helper"]],
      },
      helperNode("blk-script"),
      {
        id: "blk-fn-node",
        type: "redis-lua-script",
        server: "config1",
        name: "sentinel-block-fn",
        mode: "function",
        readonly: false,
        keyval: 1,
        func: lib,
        fname: "blocksentinelfn",
        block: true,
        wires: [["blk-fn-helper"]],
      },
      helperNode("blk-fn"),
    ];

    await load(helper, redisNode, flow);

    (await invoke(helper, "shared-script", { payload: [] })).should.equal(1);
    (
      await invoke(helper, "blk-script", { payload: ["test:sentinel:blk", "s-value"] })
    ).should.equal("s-value");

    const fnNode = helper.getNode("blk-fn-node");
    await waitForNodeProp(fnNode, "libname");
    (await invoke(helper, "blk-fn", { payload: ["test:sentinel:blkfn", "f-value"] })).should.equal(
      "f-value"
    );

    // Server-side dedication proof on the discovered master.
    const admin = directRedis();
    try {
      const list = await admin.client("list");
      const named = list.split("\n").filter((line) => line.includes(` name=${CONN_NAME} `));
      named.length.should.equal(
        3,
        `expected exactly 3 master connections named ${CONN_NAME} ` +
          `(1 shared pool + 1 per block node), got ${named.length}:\n${named.join("\n")}`
      );
    } finally {
      admin.disconnect();
    }
  });

  it("supports pub/sub and blocking list input nodes", async function () {
    const channel = "test:sentinel:pubsub";
    const listKey = "test:sentinel:blocking:list";
    const flow = [
      sentinelConfigNode(),
      {
        id: "sub",
        type: "redis-in",
        server: "config1",
        command: "subscribe",
        topic: channel,
        obj: false,
        timeout: 0,
        groupname: "",
        consumername: "",
        wires: [["sub-helper"]],
      },
      { id: "sub-helper", type: "helper" },
      {
        id: "pop",
        type: "redis-in",
        server: "config1",
        command: "blpop",
        topic: listKey,
        obj: false,
        timeout: 2,
        groupname: "",
        consumername: "",
        wires: [["pop-helper"]],
      },
      { id: "pop-helper", type: "helper" },
      {
        id: "pub-out",
        type: "redis-out",
        server: "config1",
        command: "publish",
        topic: channel,
        obj: false,
        wires: [],
      },
    ];

    await load(helper, redisNode, flow);

    const subMessage = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("pub/sub timed out")), 7000);
      helper.getNode("sub-helper").once("input", (msg) => {
        clearTimeout(timer);
        resolve(msg);
      });
    });
    setTimeout(() => helper.getNode("pub-out").receive({ payload: "hello" }), 300);
    const received = await subMessage;
    received.topic.should.equal(channel);
    received.payload.should.equal("hello");

    const popMessage = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("blpop timed out")), 7000);
      helper.getNode("pop-helper").once("input", (msg) => {
        clearTimeout(timer);
        resolve(msg);
      });
    });
    const client = directRedis();
    setTimeout(() => {
      client.rpush(listKey, "queued").finally(() => client.disconnect());
    }, 300);
    const popped = await popMessage;
    popped.topic.should.equal(listKey);
    popped.payload.should.equal("queued");
  });

  it("runs Redis 7.2 cluster-prone commands through Sentinel", async function () {
    const script =
      "redis.call('SET', KEYS[1], ARGV[1]); redis.call('SET', KEYS[2], ARGV[1]); return {redis.call('GET', KEYS[1]), redis.call('GET', KEYS[2])}";
    const scriptSha = await loadScriptOnSentinel(script);
    await load(helper, redisNode, clusterProneFlow(sentinelConfigNode()));

    await runClusterProneSuccessCases(helper, {
      prefix: "test:sentinel",
      scriptSha,
      selectSupported: true,
    });
  });

  it("reconnects through Sentinel after a failover", async function () {
    const flow = commandFlow([
      { id: "set", command: "SET" },
      { id: "get", command: "GET" },
    ]);
    await load(helper, redisNode, flow);

    (
      await invoke(helper, "set", {
        topic: "test:sentinel:failover",
        payload: "before",
      })
    ).should.equal("OK");
    (await invoke(helper, "get", { topic: "test:sentinel:failover" })).should.equal("before");

    const before = await currentMaster();
    await requestFailover();
    await waitForMasterChange(before.port);

    (
      await invoke(
        helper,
        "set",
        {
          topic: "test:sentinel:failover",
          payload: "after",
        },
        15000
      )
    ).should.equal("OK");
    (await invoke(helper, "get", { topic: "test:sentinel:failover" }, 15000)).should.equal("after");
  });
});
