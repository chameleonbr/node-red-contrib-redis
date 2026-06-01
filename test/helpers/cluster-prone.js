"use strict";

const { commandNode, expectError, helperNode, invoke } = require("./topology");

const COMMANDS = [
  ["set", "SET"],
  ["del", "DEL"],
  ["unlink", "UNLINK"],
  ["touch", "TOUCH"],
  ["mset", "MSET"],
  ["mget", "MGET"],
  ["exists", "EXISTS"],
  ["rename", "RENAME"],
  ["renamenx", "RENAMENX"],
  ["copy", "COPY"],
  ["sadd", "SADD"],
  ["sdiff", "SDIFF"],
  ["sinter", "SINTER"],
  ["sintercard", "SINTERCARD"],
  ["sunion", "SUNION"],
  ["sunionstore", "SUNIONSTORE"],
  ["sinterstore", "SINTERSTORE"],
  ["sdiffstore", "SDIFFSTORE"],
  ["zadd", "ZADD"],
  ["zdiff", "ZDIFF"],
  ["zinter", "ZINTER"],
  ["zintercard", "ZINTERCARD"],
  ["zunion", "ZUNION"],
  ["zinterstore", "ZINTERSTORE"],
  ["zunionstore", "ZUNIONSTORE"],
  ["pfadd", "PFADD"],
  ["pfcount", "PFCOUNT"],
  ["pfmerge", "PFMERGE"],
  ["xadd", "XADD"],
  ["xgroup", "XGROUP"],
  ["xread", "XREAD"],
  ["xreadgroup", "XREADGROUP"],
  ["lpush", "LPUSH"],
  ["rpush", "RPUSH"],
  ["blpop", "BLPOP"],
  ["brpop", "BRPOP"],
  ["bzpopmin", "BZPOPMIN"],
  ["bzpopmax", "BZPOPMAX"],
  ["lmpop", "LMPOP"],
  ["blmpop", "BLMPOP"],
  ["zmpop", "ZMPOP"],
  ["bzmpop", "BZMPOP"],
  ["watch", "WATCH"],
  ["multi", "MULTI"],
  ["exec", "EXEC"],
  ["eval", "EVAL"],
  ["evalsha", "EVALSHA"],
  ["keys", "KEYS"],
  ["scan", "SCAN"],
  ["dbsize", "DBSIZE"],
  ["select", "SELECT"],
];

function clusterProneFlow(configNode) {
  const flow = [configNode];
  COMMANDS.forEach(([id, command]) => {
    flow.push(commandNode(id, command));
    flow.push(helperNode(id));
  });
  return flow;
}

function key(prefix, tag, name) {
  return `${prefix}:{${tag}}:${name}`;
}

function clusterProneKeys(prefix) {
  const tags = ["prone", "slot-a", "slot-b"];
  const names = [
    "one",
    "two",
    "three",
    "tmp-a",
    "tmp-b",
    "rename-src",
    "rename-dst",
    "renamenx-src",
    "renamenx-dst",
    "copy-src",
    "copy-dst",
    "set-a",
    "set-b",
    "set-dst",
    "zset-a",
    "zset-b",
    "zset-dst",
    "pf-a",
    "pf-b",
    "pf-dst",
    "stream-a",
    "stream-b",
    "list-a",
    "list-b",
    "zpop-a",
    "zpop-b",
    "tx-a",
    "tx-b",
    "lua-a",
    "lua-b",
  ];
  return tags.flatMap((tag) => names.map((name) => key(prefix, tag, name)));
}

function sortValues(values) {
  return values.slice().sort();
}

function assertMembers(actual, expected) {
  sortValues(actual).should.eql(sortValues(expected));
}

async function step(label, fn) {
  try {
    return await fn();
  } catch (err) {
    err.message = `${label}: ${err.message}`;
    throw err;
  }
}

async function runClusterProneSuccessCases(helper, options) {
  const prefix = options.prefix;
  const scriptSha = options.scriptSha;
  const selectSupported = options.selectSupported;
  const tag = "prone";
  const k1 = key(prefix, tag, "one");
  const k2 = key(prefix, tag, "two");
  const k3 = key(prefix, tag, "three");
  const tmpA = key(prefix, tag, "tmp-a");
  const tmpB = key(prefix, tag, "tmp-b");
  const renameSrc = key(prefix, tag, "rename-src");
  const renameDst = key(prefix, tag, "rename-dst");
  const renamenxSrc = key(prefix, tag, "renamenx-src");
  const renamenxDst = key(prefix, tag, "renamenx-dst");
  const copySrc = key(prefix, tag, "copy-src");
  const copyDst = key(prefix, tag, "copy-dst");
  const setA = key(prefix, tag, "set-a");
  const setB = key(prefix, tag, "set-b");
  const setDst = key(prefix, tag, "set-dst");
  const zsetA = key(prefix, tag, "zset-a");
  const zsetB = key(prefix, tag, "zset-b");
  const zsetDst = key(prefix, tag, "zset-dst");
  const pfA = key(prefix, tag, "pf-a");
  const pfB = key(prefix, tag, "pf-b");
  const pfDst = key(prefix, tag, "pf-dst");
  const streamA = key(prefix, tag, "stream-a");
  const streamB = key(prefix, tag, "stream-b");
  const listA = key(prefix, tag, "list-a");
  const listB = key(prefix, tag, "list-b");
  const zpopA = key(prefix, tag, "zpop-a");
  const zpopB = key(prefix, tag, "zpop-b");
  const txA = key(prefix, tag, "tx-a");
  const txB = key(prefix, tag, "tx-b");
  const luaA = key(prefix, tag, "lua-a");
  const luaB = key(prefix, tag, "lua-b");

  (await step("MSET", () => invoke(helper, "mset", { payload: [k1, "1", k2, "2", k3, "3"] }))).should.equal("OK");
  (await step("MGET", () => invoke(helper, "mget", { payload: [k1, k2, k3] }))).should.eql([
    "1",
    "2",
    "3",
  ]);
  (
    await step("EXISTS", () =>
      invoke(helper, "exists", { payload: [k1, k2, key(prefix, tag, "missing")] })
    )
  ).should.equal(2);
  (await invoke(helper, "touch", { payload: [k1, k2] })).should.equal(2);
  await invoke(helper, "set", { topic: tmpA, payload: "a" });
  await invoke(helper, "set", { topic: tmpB, payload: "b" });
  (await invoke(helper, "unlink", { payload: [tmpA, tmpB] })).should.equal(2);
  await invoke(helper, "set", { topic: tmpA, payload: "a" });
  await invoke(helper, "set", { topic: tmpB, payload: "b" });
  (await invoke(helper, "del", { payload: [tmpA, tmpB] })).should.equal(2);

  await invoke(helper, "del", { payload: [renameDst, renamenxDst, copyDst] });
  await invoke(helper, "set", { topic: renameSrc, payload: "rename" });
  (await invoke(helper, "rename", { payload: [renameSrc, renameDst] })).should.equal("OK");
  await invoke(helper, "set", { topic: renamenxSrc, payload: "renamenx" });
  (await invoke(helper, "renamenx", { payload: [renamenxSrc, renamenxDst] })).should.equal(1);
  await invoke(helper, "set", { topic: copySrc, payload: "copy" });
  (await invoke(helper, "copy", { payload: [copySrc, copyDst, "REPLACE"] })).should.equal(1);

  await invoke(helper, "sadd", { topic: setA, payload: ["a", "b"] });
  await invoke(helper, "sadd", { topic: setB, payload: ["b", "c"] });
  assertMembers(await invoke(helper, "sdiff", { payload: [setA, setB] }), ["a"]);
  assertMembers(await invoke(helper, "sinter", { payload: [setA, setB] }), ["b"]);
  (await invoke(helper, "sintercard", { payload: ["2", setA, setB] })).should.equal(1);
  assertMembers(await invoke(helper, "sunion", { payload: [setA, setB] }), ["a", "b", "c"]);
  (await invoke(helper, "sunionstore", { payload: [setDst, setA, setB] })).should.equal(3);
  (await invoke(helper, "sinterstore", { payload: [setDst, setA, setB] })).should.equal(1);
  (await invoke(helper, "sdiffstore", { payload: [setDst, setA, setB] })).should.equal(1);

  await invoke(helper, "zadd", { topic: zsetA, payload: ["1", "a", "2", "b"] });
  await invoke(helper, "zadd", { topic: zsetB, payload: ["2", "b", "3", "c"] });
  assertMembers(await invoke(helper, "zdiff", { payload: ["2", zsetA, zsetB] }), ["a"]);
  assertMembers(await invoke(helper, "zinter", { payload: ["2", zsetA, zsetB] }), ["b"]);
  (await invoke(helper, "zintercard", { payload: ["2", zsetA, zsetB] })).should.equal(1);
  assertMembers(await invoke(helper, "zunion", { payload: ["2", zsetA, zsetB] }), ["a", "b", "c"]);
  (await invoke(helper, "zinterstore", { payload: [zsetDst, "2", zsetA, zsetB] })).should.equal(1);
  (await invoke(helper, "zunionstore", { payload: [zsetDst, "2", zsetA, zsetB] })).should.equal(3);

  (await invoke(helper, "pfadd", { topic: pfA, payload: ["a", "b"] })).should.equal(1);
  (await invoke(helper, "pfadd", { topic: pfB, payload: ["b", "c"] })).should.equal(1);
  (await invoke(helper, "pfcount", { payload: [pfA, pfB] })).should.be.aboveOrEqual(2);
  (await invoke(helper, "pfmerge", { payload: [pfDst, pfA, pfB] })).should.equal("OK");

  await invoke(helper, "xadd", { topic: streamA, payload: ["*", "field", "a"] });
  await invoke(helper, "xadd", { topic: streamB, payload: ["*", "field", "b"] });
  const streams = await invoke(helper, "xread", {
    payload: ["COUNT", "2", "STREAMS", streamA, streamB, "0-0", "0-0"],
  });
  streams.should.be.an.Array();
  streams.length.should.equal(2);
  await invoke(helper, "xgroup", { payload: ["CREATE", streamA, "cg", "0"] });
  await invoke(helper, "xgroup", { payload: ["CREATE", streamB, "cg", "0"] });
  const groupStreams = await invoke(helper, "xreadgroup", {
    payload: ["GROUP", "cg", "consumer", "COUNT", "2", "STREAMS", streamA, streamB, ">", ">"],
  });
  groupStreams.should.be.an.Array();
  groupStreams.length.should.equal(2);

  await invoke(helper, "lpush", { topic: listA, payload: "a-left" });
  await invoke(helper, "lpush", { topic: listB, payload: "b-left" });
  (await invoke(helper, "blpop", { payload: [listA, listB, "1"] })).should.eql([listA, "a-left"]);
  (await invoke(helper, "brpop", { payload: [listA, listB, "1"] })).should.eql([listB, "b-left"]);
  await invoke(helper, "lpush", { topic: listA, payload: "a-lmpop" });
  const lmpop = await invoke(helper, "lmpop", { payload: ["2", listA, listB, "LEFT", "COUNT", "1"] });
  lmpop.should.be.an.Array();
  lmpop[0].should.equal(listA);
  await invoke(helper, "lpush", { topic: listA, payload: "a-blmpop" });
  const blmpop = await invoke(helper, "blmpop", {
    payload: ["1", "2", listA, listB, "LEFT", "COUNT", "1"],
  });
  blmpop.should.be.an.Array();
  blmpop[0].should.equal(listA);

  await invoke(helper, "zadd", { topic: zpopA, payload: ["1", "za"] });
  await invoke(helper, "zadd", { topic: zpopB, payload: ["1", "zb"] });
  (await invoke(helper, "bzpopmin", { payload: [zpopA, zpopB, "1"] })).should.eql([
    zpopA,
    "za",
    "1",
  ]);
  (await invoke(helper, "bzpopmax", { payload: [zpopA, zpopB, "1"] })).should.eql([
    zpopB,
    "zb",
    "1",
  ]);
  await invoke(helper, "zadd", { topic: zpopA, payload: ["1", "za2"] });
  const zmpop = await invoke(helper, "zmpop", { payload: ["2", zpopA, zpopB, "MIN", "COUNT", "1"] });
  zmpop.should.be.an.Array();
  zmpop[0].should.equal(zpopA);
  await invoke(helper, "zadd", { topic: zpopA, payload: ["1", "za3"] });
  const bzmpop = await invoke(helper, "bzmpop", {
    payload: ["1", "2", zpopA, zpopB, "MIN", "COUNT", "1"],
  });
  bzmpop.should.be.an.Array();
  bzmpop[0].should.equal(zpopA);

  if (options.nodeTransaction !== false) {
    (await invoke(helper, "watch", { payload: [txA, txB] })).should.equal("OK");
    (await invoke(helper, "multi")).should.equal("OK");
    (await invoke(helper, "set", { topic: txA, payload: "tx-value" })).should.equal("QUEUED");
    (await invoke(helper, "set", { topic: txB, payload: "tx-other" })).should.equal("QUEUED");
    (await invoke(helper, "exec")).should.be.an.Array();
  }

  const script =
    "redis.call('SET', KEYS[1], ARGV[1]); redis.call('SET', KEYS[2], ARGV[1]); return {redis.call('GET', KEYS[1]), redis.call('GET', KEYS[2])}";
  (await invoke(helper, "eval", { payload: [script, "2", luaA, luaB, "eval-value"] })).should.eql([
    "eval-value",
    "eval-value",
  ]);
  if (scriptSha) {
    (
      await invoke(helper, "evalsha", {
        payload: [scriptSha, "2", luaA, luaB, "evalsha-value"],
      })
    ).should.eql(["evalsha-value", "evalsha-value"]);
  }

  (await invoke(helper, "keys", { payload: `${prefix}:*` })).should.be.an.Array();
  const scan = await invoke(helper, "scan", { payload: ["0", "MATCH", `${prefix}:*`, "COUNT", "10"] });
  scan.should.be.an.Array();
  scan.length.should.equal(2);
  (await invoke(helper, "dbsize")).should.be.a.Number();
  if (selectSupported) {
    (await invoke(helper, "select", { payload: "0" })).should.equal("OK");
  } else {
    const err = await expectError(helper, "select", { payload: "1" });
    err.message.should.match(/SELECT|cluster|not allowed/i);
  }
}

async function runClusterProneCrossSlotFailures(helper, options) {
  const prefix = options.prefix;
  const scriptSha = options.scriptSha;
  const a = "slot-a";
  const b = "slot-b";
  const one = key(prefix, a, "one");
  const two = key(prefix, b, "two");
  const setA = key(prefix, a, "set-a");
  const setB = key(prefix, b, "set-b");
  const setDst = key(prefix, a, "set-dst");
  const zsetA = key(prefix, a, "zset-a");
  const zsetB = key(prefix, b, "zset-b");
  const zsetDst = key(prefix, a, "zset-dst");
  const pfA = key(prefix, a, "pf-a");
  const pfB = key(prefix, b, "pf-b");
  const pfDst = key(prefix, a, "pf-dst");
  const streamA = key(prefix, a, "stream-a");
  const streamB = key(prefix, b, "stream-b");
  const listA = key(prefix, a, "list-a");
  const listB = key(prefix, b, "list-b");
  const zpopA = key(prefix, a, "zpop-a");
  const zpopB = key(prefix, b, "zpop-b");
  const luaA = key(prefix, a, "lua-a");
  const luaB = key(prefix, b, "lua-b");
  const script = "return {KEYS[1], KEYS[2]}";

  await invoke(helper, "set", { topic: one, payload: "1" });
  await invoke(helper, "set", { topic: two, payload: "2" });
  await invoke(helper, "sadd", { topic: setA, payload: ["a", "b"] });
  await invoke(helper, "sadd", { topic: setB, payload: ["b", "c"] });
  await invoke(helper, "zadd", { topic: zsetA, payload: ["1", "a", "2", "b"] });
  await invoke(helper, "zadd", { topic: zsetB, payload: ["2", "b", "3", "c"] });
  await invoke(helper, "pfadd", { topic: pfA, payload: ["a", "b"] });
  await invoke(helper, "pfadd", { topic: pfB, payload: ["b", "c"] });
  await invoke(helper, "xadd", { topic: streamA, payload: ["*", "field", "a"] });
  await invoke(helper, "xadd", { topic: streamB, payload: ["*", "field", "b"] });
  await invoke(helper, "lpush", { topic: listA, payload: "a" });
  await invoke(helper, "lpush", { topic: listB, payload: "b" });
  await invoke(helper, "zadd", { topic: zpopA, payload: ["1", "za"] });
  await invoke(helper, "zadd", { topic: zpopB, payload: ["1", "zb"] });

  const failures = [
    ["mget", { payload: [one, two] }],
    ["mset", { payload: [one, "1", two, "2"] }],
    ["exists", { payload: [one, two] }],
    ["del", { payload: [one, two] }],
    ["unlink", { payload: [one, two] }],
    ["touch", { payload: [one, two] }],
    ["rename", { payload: [one, two] }],
    ["renamenx", { payload: [one, two] }],
    ["copy", { payload: [one, two, "REPLACE"] }],
    ["sdiff", { payload: [setA, setB] }],
    ["sinter", { payload: [setA, setB] }],
    ["sintercard", { payload: ["2", setA, setB] }],
    ["sunion", { payload: [setA, setB] }],
    ["sunionstore", { payload: [setDst, setA, setB] }],
    ["sinterstore", { payload: [setDst, setA, setB] }],
    ["sdiffstore", { payload: [setDst, setA, setB] }],
    ["zdiff", { payload: ["2", zsetA, zsetB] }],
    ["zinter", { payload: ["2", zsetA, zsetB] }],
    ["zintercard", { payload: ["2", zsetA, zsetB] }],
    ["zunion", { payload: ["2", zsetA, zsetB] }],
    ["zinterstore", { payload: [zsetDst, "2", zsetA, zsetB] }],
    ["zunionstore", { payload: [zsetDst, "2", zsetA, zsetB] }],
    ["pfcount", { payload: [pfA, pfB] }],
    ["pfmerge", { payload: [pfDst, pfA, pfB] }],
    ["xread", { payload: ["STREAMS", streamA, streamB, "0-0", "0-0"] }],
    ["xreadgroup", { payload: ["GROUP", "cg", "consumer", "STREAMS", streamA, streamB, ">", ">"] }],
    ["blpop", { payload: [listA, listB, "1"] }],
    ["brpop", { payload: [listA, listB, "1"] }],
    ["bzpopmin", { payload: [zpopA, zpopB, "1"] }],
    ["bzpopmax", { payload: [zpopA, zpopB, "1"] }],
    ["lmpop", { payload: ["2", listA, listB, "LEFT"] }],
    ["blmpop", { payload: ["1", "2", listA, listB, "LEFT"] }],
    ["zmpop", { payload: ["2", zpopA, zpopB, "MIN"] }],
    ["bzmpop", { payload: ["1", "2", zpopA, zpopB, "MIN"] }],
    ["watch", { payload: [one, two] }],
    ["eval", { payload: [script, "2", luaA, luaB] }],
  ];

  if (scriptSha) {
    failures.push(["evalsha", { payload: [scriptSha, "2", luaA, luaB] }]);
  }

  for (const [id, msg] of failures) {
    const err = await expectError(helper, id, msg);
    err.message.should.match(/CROSSSLOT|same slot/i);
  }
}

module.exports = {
  clusterProneFlow,
  clusterProneKeys,
  runClusterProneCrossSlotFailures,
  runClusterProneSuccessCases,
};
