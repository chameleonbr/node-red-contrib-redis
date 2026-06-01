"use strict";

// Reproduction test for the redis-lua-script connection-id bug.
//
// RedisLua (redis.js) builds its pool key from `n.server.name`, but `n.server` is the
// config-node *id string*, not the resolved config node — so `n.server.name` is undefined.
// Every non-blocking redis-lua-script node therefore shares the single pool entry keyed
// `undefined`, regardless of which redis-config it points at.
//
// Setup: two config nodes on different DBs (db 0 and db 1) and one non-blocking lua node
// each. The db-0 node is constructed first, so it seeds connections[undefined] with a db-0
// client; the db-1 node reuses that same client. A SET driven through the db-1 node thus
// lands in db 0 instead of db 1.
//
// This spec asserts the CORRECT behavior (the key lands in db 1). It is expected to FAIL
// against the current code and PASS once the id uses the resolved `this.server.name`.

var helper = require("node-red-node-test-helper");
var redisNode = require("../redis.js");
var deployment = require("./helpers/deployment");

helper.init(require.resolve("node-red"));

var TEST_KEY = "lua:conntest:key";

function delKeyInBothDbs() {
  var c0 = deployment.directRedis({ db: 0 });
  var c1 = deployment.directRedis({ db: 1 });
  return Promise.all([c0.del(TEST_KEY), c1.del(TEST_KEY)]).then(function () {
    c0.disconnect();
    c1.disconnect();
  });
}

describe("redis-lua-script connection isolation", function () {
  this.timeout(8000);

  beforeEach(function (done) {
    helper.startServer(function () {
      delKeyInBothDbs().then(function () {
        done();
      });
    });
  });

  afterEach(function (done) {
    helper
      .unload()
      .then(function () {
        return delKeyInBothDbs();
      })
      .then(function () {
        helper.stopServer(done);
      });
  });

  it("routes a non-blocking lua node to its own config's DB (not a shared pooled client)", function (done) {
    var flow = [
      deployment.redisConfigNode("cfg-db0", "ConfigDb0", { db: 0 }),
      deployment.redisConfigNode("cfg-db1", "ConfigDb1", { db: 1 }),
      // Constructed first: seeds the (buggy) connections[undefined] slot with a db-0 client.
      {
        id: "lua-db0",
        type: "redis-lua-script",
        server: "cfg-db0",
        name: "luaDb0",
        keyval: 1,
        func: "redis.call('SET', KEYS[1], ARGV[1])\nreturn 'OK'",
        stored: false,
        block: false,
        wires: [["sink0"]],
      },
      { id: "sink0", type: "helper" },
      // Points at db 1, but with the bug reuses the db-0 client above.
      {
        id: "lua-db1",
        type: "redis-lua-script",
        server: "cfg-db1",
        name: "luaDb1",
        keyval: 1,
        func: "redis.call('SET', KEYS[1], ARGV[1])\nreturn 'OK'",
        stored: false,
        block: false,
        wires: [["sink1"]],
      },
      { id: "sink1", type: "helper" },
    ];

    helper.load(redisNode, flow, function () {
      var luaDb1 = helper.getNode("lua-db1");
      var sink1 = helper.getNode("sink1");

      sink1.on("input", function () {
        // The script ran. Now check which DB actually received the write.
        var probe1 = deployment.directRedis({ db: 1 });
        var probe0 = deployment.directRedis({ db: 0 });
        Promise.all([probe1.get(TEST_KEY), probe0.get(TEST_KEY)])
          .then(function (res) {
            var inDb1 = res[0];
            var inDb0 = res[1];
            probe0.disconnect();
            probe1.disconnect();
            try {
              assert_equal(
                inDb1,
                "valueB",
                "lua node pointed at db 1 should write to db 1 (got db1=" +
                  JSON.stringify(inDb1) +
                  ", db0=" +
                  JSON.stringify(inDb0) +
                  "). If the value landed in db 0, the lua nodes are sharing one pooled " +
                  "connection keyed `undefined`."
              );
              done();
            } catch (err) {
              done(err);
            }
          })
          .catch(done);
      });

      luaDb1.receive({ payload: [TEST_KEY, "valueB"] });
    });
  });
});

function assert_equal(actual, expected, msg) {
  if (actual !== expected) {
    throw new Error(msg);
  }
}
