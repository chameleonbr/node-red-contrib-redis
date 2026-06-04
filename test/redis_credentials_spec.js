"use strict";
const helper = require("node-red-node-test-helper");
const redisNode = require("../redis.js");
const { commandNode, helperNode, invoke } = require("./helpers/topology");

helper.init(require.resolve("node-red"));

function cfg(options, optionsType, cluster) {
  return {
    id: "cfg",
    type: "redis-config",
    name: "cred",
    options: typeof options === "string" ? options : JSON.stringify(options),
    optionsType: optionsType || "json",
    cluster: !!cluster,
  };
}

function loadWithCreds(flow, creds) {
  return new Promise((resolve, reject) => {
    helper.load(redisNode, flow, creds, (err) => (err ? reject(err) : resolve()));
  });
}

describe("redis-config credential secret merge", function () {
  this.timeout(8000);

  beforeEach(function (done) { helper.startServer(done); });
  afterEach(function (done) { helper.unload().then(() => helper.stopServer(done)); });

  it("merges the single-mode password from the secrets credential into options", async function () {
    await loadWithCreds([cfg({ host: "127.0.0.1", port: 6379 }, "json", false)], {
      cfg: { secrets: JSON.stringify({ password: "s3cret" }) },
    });
    helper.getNode("cfg").options.password.should.equal("s3cret");
  });

  it("merges cluster per-node passwords by index", async function () {
    await loadWithCreds([cfg([{ host: "h1", port: 7000 }, { host: "h2", port: 7001 }], "json", true)], {
      cfg: { secrets: JSON.stringify({ nodes: ["a", "b"] }) },
    });
    const opts = helper.getNode("cfg").options;
    opts[0].password.should.equal("a");
    opts[1].password.should.equal("b");
  });

  it("merges sentinel password and sentinelPassword", async function () {
    await loadWithCreds(
      [cfg({ sentinels: [{ host: "s1", port: 26379 }], name: "mymaster" }, "json", false)],
      { cfg: { secrets: JSON.stringify({ password: "dp", sentinelPassword: "sp" }) } }
    );
    const opts = helper.getNode("cfg").options;
    opts.password.should.equal("dp");
    opts.sentinelPassword.should.equal("sp");
  });

  it("keeps a legacy password embedded in options when no credential is set", async function () {
    await loadWithCreds([cfg({ host: "127.0.0.1", port: 6379, password: "legacy" }, "json", false)], {});
    helper.getNode("cfg").options.password.should.equal("legacy");
  });

  it("does not merge secrets in env mode", async function () {
    process.env.RC_TEST_OPTS = JSON.stringify({ host: "127.0.0.1", port: 6379 });
    try {
      await loadWithCreds([cfg("RC_TEST_OPTS", "env", false)], {
        cfg: { secrets: JSON.stringify({ password: "ignored" }) },
      });
      (helper.getNode("cfg").options.password === undefined).should.be.true();
    } finally {
      delete process.env.RC_TEST_OPTS;
    }
  });

  it("authenticates using the password supplied via the secrets credential", async function () {
    if (!process.env.REDIS_PASSWORD) { this.skip(); return; }
    const opts = { host: process.env.REDIS_HOST || "127.0.0.1", port: Number(process.env.REDIS_PORT || 6379) };
    if (process.env.REDIS_USERNAME) { opts.username = process.env.REDIS_USERNAME; }
    await loadWithCreds(
      [cfg(opts, "json", false), commandNode("getcred", "GET", "cfg"), helperNode("getcred")],
      { cfg: { secrets: JSON.stringify({ password: process.env.REDIS_PASSWORD }) } }
    );
    const out = await invoke(helper, "getcred", { topic: "test:cred:probe" });
    // Resolves only if the connection authenticated; a missing key returns null.
    (out === null || typeof out === "string").should.be.true();
  });
});
