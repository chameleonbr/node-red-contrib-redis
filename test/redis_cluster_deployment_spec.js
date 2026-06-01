"use strict";

const helper = require("node-red-node-test-helper");
const Redis = require("ioredis");
const redisNode = require("../redis.js");
const { commandNode, expectError, helperNode, invoke, load } = require("./helpers/topology");
const {
  clusterProneFlow,
  runClusterProneCrossSlotFailures,
  runClusterProneSuccessCases,
} = require("./helpers/cluster-prone");

helper.init(require.resolve("node-red"));

const describeCluster = process.env.REDIS_DEPLOYMENT === "cluster-auth" ? describe : describe.skip;

function auth() {
  return {
    username: process.env.REDIS_USERNAME,
    password: process.env.REDIS_PASSWORD,
  };
}

function clusterNodes() {
  return (process.env.REDIS_CLUSTER_NODES || "127.0.0.1:7000,127.0.0.1:7001")
    .split(",")
    .map((item) => {
      const [host, port] = item.split(":");
      return Object.assign({ host, port: Number(port) }, auth());
    });
}

function clusterConfigNode() {
  return {
    id: "config1",
    type: "redis-config",
    name: "ClusterAuth",
    options: JSON.stringify(clusterNodes()),
    optionsType: "json",
    cluster: true,
  };
}

function directCluster() {
  return new Redis.Cluster(clusterNodes(), {
    redisOptions: Object.assign(
      {
        connectTimeout: 1000,
        maxRetriesPerRequest: 1,
      },
      auth()
    ),
    slotsRefreshTimeout: 2000,
  });
}

async function cleanupClusterKeys() {
  const client = directCluster();
  try {
    await client.ping();
    const masters = client.nodes("master");
    for (const node of masters) {
      const keys = await node.keys("test:cluster:*");
      if (keys.length > 0) {
        await Promise.all(keys.map((key) => node.del(key)));
      }
    }
  } finally {
    client.disconnect();
  }
}

function commandFlow(defs) {
  const flow = [clusterConfigNode()];
  defs.forEach((def) => {
    flow.push(commandNode(def.id, def.command));
    flow.push(helperNode(def.id));
  });
  return flow;
}

function waitForLuaSha(node) {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const tick = () => {
      if (node.sha1 && node.sha1.length === 40) {
        resolve();
      } else if (Date.now() - started > 5000) {
        reject(new Error("stored Lua script was not loaded"));
      } else {
        setTimeout(tick, 25);
      }
    };
    tick();
  });
}

async function loadScriptOnCluster(script) {
  const client = directCluster();
  try {
    await client.ping();
    const shas = await Promise.all(client.nodes("master").map((node) => node.script("load", script)));
    return shas[0];
  } finally {
    client.disconnect();
  }
}

async function runDirectClusterTransaction() {
  const client = directCluster();
  const txA = "test:cluster:{prone}:tx-a";
  const txB = "test:cluster:{prone}:tx-b";
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

describeCluster("Redis Cluster auth deployment", function () {
  this.timeout(20000);

  beforeEach(function (done) {
    helper.startServer(done);
  });

  afterEach(function (done) {
    helper
      .unload()
      .then(cleanupClusterKeys)
      .then(() => helper.stopServer(done))
      .catch(done);
  });

  it("runs Redis 7.2-compatible command coverage through redis-command", async function () {
    await load(
      helper,
      redisNode,
      commandFlow([
        { id: "ping", command: "PING" },
        { id: "acl", command: "ACL" },
        { id: "set", command: "SET" },
        { id: "get", command: "GET" },
        { id: "del", command: "DEL" },
        { id: "exists", command: "EXISTS" },
        { id: "hset", command: "HSET" },
        { id: "hgetall", command: "HGETALL" },
        { id: "xadd", command: "XADD" },
        { id: "xread", command: "XREAD" },
        { id: "mset", command: "MSET" },
        { id: "mget", command: "MGET" },
      ])
    );

    const stringKey = "test:cluster:{basic}:string";
    const otherKey = "test:cluster:{basic}:other";
    const hashKey = "test:cluster:{basic}:hash";
    const streamKey = "test:cluster:{basic}:stream";

    (await invoke(helper, "ping")).should.equal("PONG");
    (await invoke(helper, "acl", { payload: ["WHOAMI"] })).should.equal(process.env.REDIS_USERNAME);
    (await invoke(helper, "set", { topic: stringKey, payload: "value" })).should.equal("OK");
    (await invoke(helper, "get", { topic: stringKey })).should.equal("value");
    (await invoke(helper, "exists", { payload: [stringKey, otherKey] })).should.equal(1);

    (
      await invoke(helper, "hset", {
        topic: hashKey,
        payload: ["field1", "value1", "field2", "value2"],
      })
    ).should.equal(2);
    const hash = await invoke(helper, "hgetall", { topic: hashKey });
    hash.should.containEql("field1");
    hash.should.containEql("value1");

    const streamId = await invoke(helper, "xadd", {
      topic: streamKey,
      payload: ["*", "field", "value"],
    });
    streamId.should.be.a.String();
    const streamRead = await invoke(helper, "xread", {
      payload: ["COUNT", "1", "STREAMS", streamKey, "0-0"],
    });
    streamRead.should.be.an.Array();
    streamRead.length.should.equal(1);

    (
      await invoke(helper, "mset", {
        payload: ["test:cluster:{multi}:one", "1", "test:cluster:{multi}:two", "2"],
      })
    ).should.equal("OK");
    (
      await invoke(helper, "mget", {
        payload: ["test:cluster:{multi}:one", "test:cluster:{multi}:two"],
      })
    ).should.eql(["1", "2"]);

    (
      await invoke(helper, "del", {
        payload: [stringKey, otherKey, hashKey, streamKey],
      })
    ).should.be.a.Number();
  });

  it("fails cross-slot multi-key commands deliberately", async function () {
    await load(helper, redisNode, commandFlow([{ id: "mset", command: "MSET" }]));

    const err = await expectError(helper, "mset", {
      payload: ["test:cluster:{slot-a}:one", "1", "test:cluster:{slot-b}:two", "2"],
    });
    err.message.should.match(/CROSSSLOT|same slot/i);
  });

  it("runs Redis 7.2 cluster-prone commands with same-slot keys", async function () {
    const script =
      "redis.call('SET', KEYS[1], ARGV[1]); redis.call('SET', KEYS[2], ARGV[1]); return {redis.call('GET', KEYS[1]), redis.call('GET', KEYS[2])}";
    const scriptSha = await loadScriptOnCluster(script);
    await load(helper, redisNode, clusterProneFlow(clusterConfigNode()));

    await runClusterProneSuccessCases(helper, {
      prefix: "test:cluster",
      scriptSha,
      nodeTransaction: false,
      selectSupported: false,
    });
    await runDirectClusterTransaction();
  });

  it("rejects Redis 7.2 cluster-prone commands with cross-slot keys", async function () {
    const script = "return {KEYS[1], KEYS[2]}";
    const scriptSha = await loadScriptOnCluster(script);
    await load(helper, redisNode, clusterProneFlow(clusterConfigNode()));

    await runClusterProneCrossSlotFailures(helper, {
      prefix: "test:cluster",
      scriptSha,
    });
  });

  it("supports pub/sub and blocking list input nodes", async function () {
    const channel = "test:cluster:pubsub";
    const listKey = "test:cluster:{blocking}:list";
    const flow = [
      clusterConfigNode(),
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
    const cluster = directCluster();
    setTimeout(() => {
      cluster.rpush(listKey, "queued").finally(() => cluster.disconnect());
    }, 300);
    const popped = await popMessage;
    popped.topic.should.equal(listKey);
    popped.payload.should.equal("queued");
  });

  it("runs same-slot Lua scripts and falls back after SCRIPT FLUSH", async function () {
    const unstoredFunc = [
      "redis.call('SET', KEYS[1], ARGV[1])",
      "redis.call('INCRBY', KEYS[2], ARGV[2])",
      "redis.call('ZADD', KEYS[3], ARGV[3], ARGV[4])",
      "return {redis.call('GET', KEYS[1]), redis.call('GET', KEYS[2]), redis.call('ZRANGE', KEYS[3], 0, -1)[1]}",
    ].join("\n");
    const storedFunc = "return redis.call('INCR', KEYS[1])";
    const flow = [
      clusterConfigNode(),
      {
        id: "lua-node",
        type: "redis-lua-script",
        server: "config1",
        name: "cluster-lua",
        keyval: 3,
        func: unstoredFunc,
        stored: false,
        block: false,
        wires: [["lua-helper"]],
      },
      helperNode("lua"),
      {
        id: "stored-node",
        type: "redis-lua-script",
        server: "config1",
        name: "cluster-stored-lua",
        keyval: 1,
        func: storedFunc,
        stored: true,
        block: false,
        wires: [["stored-helper"]],
      },
      helperNode("stored"),
    ];

    await load(helper, redisNode, flow);

    const result = await invoke(helper, "lua", {
      payload: [
        "test:cluster:{lua}:value",
        "test:cluster:{lua}:counter",
        "test:cluster:{lua}:zset",
        "payload",
        "2",
        "10",
        "member",
      ],
    });
    result.should.eql(["payload", "2", "member"]);

    const storedNode = helper.getNode("stored-node");
    await waitForLuaSha(storedNode);
    (
      await invoke(helper, "stored", {
        payload: ["test:cluster:{lua}:stored-counter"],
      })
    ).should.equal(1);

    const cluster = directCluster();
    try {
      await Promise.all(cluster.nodes("master").map((node) => node.script("flush")));
    } finally {
      cluster.disconnect();
    }

    (
      await invoke(helper, "stored", {
        payload: ["test:cluster:{lua}:stored-counter"],
      })
    ).should.equal(2);
  });
});
