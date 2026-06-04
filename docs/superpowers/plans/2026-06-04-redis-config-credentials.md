# redis-config Credential/Secret Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store `redis-config` passwords in Node-RED's encrypted credential store (a `text`-type `secrets` map) instead of plaintext in `flows.json`, for all modes, backward-compatibly.

**Architecture:** Two small helpers (`extractSecrets`/`mergeSecrets`) are mirrored in `redis.js` (runtime) and the `redis-config` editor script. Secrets are diverted out of the options blob at save (`oneditsave` → `extractSecrets`) and merged back at load (`oneditprepare`) and runtime (`RedisConfig`). `buildX*Options` and the live form↔editor sync are untouched. Legacy password-in-options still works (merge is a no-op without a credential).

**Tech Stack:** Node-RED 4.1.x config node + credentials, ioredis 5.x, Mocha + `node-red-node-test-helper`, Playwright editor harness, Docker-managed Redis (`redis:8.8-alpine`).

**Spec:** `docs/superpowers/specs/2026-06-04-redis-config-credentials-design.md`

**Branch:** work only on `claude-review`. Do **not** stage `.gitignore` (owned by the user).

---

## File Structure

- `redis.js` — add `extractSecrets`/`mergeSecrets`/`parseSecrets` (module-level, just above `RedisConfig`); merge in the `RedisConfig` constructor (non-env only); add `credentials: { secrets: { type: "text" } }` to the `registerType` call.
- `redis.html` (`redis-config` script) — mirror `extractSecrets`/`mergeSecrets`/`parseSecrets`; add `credentials` to the definition; add hidden `#node-config-input-secrets`; merge-on-load in `oneditprepare`; extract-on-save in `oneditsave`; one help paragraph.
- `test/redis_credentials_spec.js` — NEW. Constructor-merge tests (no Redis) + one guarded end-to-end auth test.
- `docs/NODE_GUIDE.md`, `docs/ARCHITECTURE.md`, `docs/REFERENCE_MAP.md`, `docs/TESTING.md`, `.claude/skills/node-red-contrib-redis-maintainer/SKILL.md` — secret-storage notes + spec-list entry.

`README.md` needs no change (its only secret mention is about MemoryDB test env vars, not flow storage).

---

## Task 0: Start a dev Redis (auth) for the red/green loop

- [ ] **Step 1: Start a password-protected standalone Redis**

Run:
```bash
sudo -n docker run -d --rm --name redis-dev -p 127.0.0.1:6379:6379 \
  redis:8.8-alpine redis-server --requirepass devpass --save "" --appendonly no
```
Expected: prints a container id.

- [ ] **Step 2: Confirm auth works**

Run:
```bash
sudo -n docker exec redis-dev redis-cli -a devpass ping
```
Expected: `PONG` (a warning about using a password on the CLI is fine).

> Auth lets the local end-to-end test run. Stop this Redis (Task 6) before any hook-running commit.

---

## Task 1: RED — create the credentials spec

**Files:**
- Create: `test/redis_credentials_spec.js`

- [ ] **Step 1: Write the spec**

Create `test/redis_credentials_spec.js`:

```javascript
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
```

- [ ] **Step 2: Run and verify the merge tests FAIL**

Run:
```bash
REDIS_PASSWORD=devpass npm run test:mocha -- test/redis_credentials_spec.js
```
Expected: **2 passing, 4 failing**. The three merge tests (single/cluster/sentinel) fail with `TypeError: Cannot read properties of undefined (reading 'should')` because the password is never injected, and the end-to-end auth test fails because, without the runtime merge, the credential password is not applied so the connection cannot authenticate against the auth dev Redis. The "legacy" (password already in options) and "env" (merge skipped) tests pass.

- [ ] **Step 3: Commit the failing spec (skip the hook — the matrix would fail on the intentional RED)**

```bash
git add test/redis_credentials_spec.js
git commit --no-verify -m "Add failing tests: redis-config does not merge secrets from credentials

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: GREEN (runtime) — merge secrets in redis.js

**Files:**
- Modify: `redis.js` (add helpers just above `function RedisConfig(n)`; merge in the constructor; add credentials to `registerType`)

- [ ] **Step 1: Add the helpers above `RedisConfig`**

Find:
```javascript
function RedisConfig(n) {
    RED.nodes.createNode(this, n);
```
Replace with:
```javascript
  // Secret extract/merge for redis-config. MUST stay in sync with the copy in
  // redis.html (editor): same paths — single/sentinel `password`, sentinel
  // `sentinelPassword`, cluster per-node `nodes[i]`.
  function extractSecrets(options) {
    var secrets = {};
    if (Array.isArray(options)) {
      var pws = options.map(function (node) {
        return node && node.password ? node.password : "";
      });
      if (pws.some(function (p) { return p; })) {
        secrets.nodes = pws;
      }
      var strippedNodes = options.map(function (node) {
        var copy = Object.assign({}, node);
        delete copy.password;
        return copy;
      });
      return { stripped: strippedNodes, secrets: secrets };
    }
    if (options && typeof options === "object") {
      var stripped = Object.assign({}, options);
      if (Array.isArray(options.sentinels)) {
        if (options.password) { secrets.password = options.password; }
        if (options.sentinelPassword) { secrets.sentinelPassword = options.sentinelPassword; }
        delete stripped.password;
        delete stripped.sentinelPassword;
      } else {
        if (options.password) { secrets.password = options.password; }
        delete stripped.password;
      }
      return { stripped: stripped, secrets: secrets };
    }
    return { stripped: options, secrets: secrets };
  }

  function mergeSecrets(options, secrets) {
    if (!secrets || typeof secrets !== "object") { return options; }
    if (Array.isArray(options)) {
      if (Array.isArray(secrets.nodes)) {
        secrets.nodes.forEach(function (pw, i) {
          if (pw && options[i]) { options[i].password = pw; }
        });
      }
      return options;
    }
    if (options && typeof options === "object") {
      if (Array.isArray(options.sentinels)) {
        if (secrets.password) { options.password = secrets.password; }
        if (secrets.sentinelPassword) { options.sentinelPassword = secrets.sentinelPassword; }
      } else if (secrets.password) {
        options.password = secrets.password;
      }
    }
    return options;
  }

  function parseSecrets(value) {
    if (!value) { return {}; }
    try { return JSON.parse(value) || {}; } catch (e) { return {}; }
  }

function RedisConfig(n) {
    RED.nodes.createNode(this, n);
```

- [ ] **Step 2: Merge in the constructor and register the credential**

Find:
```javascript
      this.options = evaluateConnectionOptions(n.options, this.optionsType, this);
      this.cluster = isClusterConnection(this.options, this.cluster);
    } catch (err) {
      this.options = undefined;
      this.error(err.message, null);
    }
  }
  RED.nodes.registerType("redis-config", RedisConfig);
```
Replace with:
```javascript
      this.options = evaluateConnectionOptions(n.options, this.optionsType, this);
      if (this.optionsType !== "env") {
        this.options = mergeSecrets(
          this.options,
          parseSecrets(this.credentials && this.credentials.secrets)
        );
      }
      this.cluster = isClusterConnection(this.options, this.cluster);
    } catch (err) {
      this.options = undefined;
      this.error(err.message, null);
    }
  }
  RED.nodes.registerType("redis-config", RedisConfig, {
    credentials: {
      secrets: { type: "text" },
    },
  });
```

- [ ] **Step 3: Run the spec; merge + auth tests pass**

Run:
```bash
REDIS_PASSWORD=devpass npm run test:mocha -- test/redis_credentials_spec.js
```
Expected: `6 passing` (the three merge tests now pass; legacy/env still pass; end-to-end auth connects to the dev Redis using the credential password and resolves).

---

## Task 3: GREEN (editor) — divert secrets in redis.html

**Files:**
- Modify: `redis.html` (`redis-config` `<script>`: helpers, credentials, hidden input, `oneditprepare`, `oneditsave`)

- [ ] **Step 1: Mirror the helpers at the top of the redis-config script**

Find:
```javascript
    "use strict";
    /*global RED*/
    RED.nodes.registerType('redis-config', {
```
Replace with:
```javascript
    "use strict";
    /*global RED*/
    // Secret extract/merge for redis-config. MUST stay in sync with the copy in
    // redis.js (runtime): same paths — single/sentinel `password`, sentinel
    // `sentinelPassword`, cluster per-node `nodes[i]`.
    function extractSecrets(options) {
        var secrets = {};
        if (Array.isArray(options)) {
            var pws = options.map(function (node) {
                return node && node.password ? node.password : "";
            });
            if (pws.some(function (p) { return p; })) { secrets.nodes = pws; }
            var strippedNodes = options.map(function (node) {
                var copy = Object.assign({}, node);
                delete copy.password;
                return copy;
            });
            return { stripped: strippedNodes, secrets: secrets };
        }
        if (options && typeof options === "object") {
            var stripped = Object.assign({}, options);
            if (Array.isArray(options.sentinels)) {
                if (options.password) { secrets.password = options.password; }
                if (options.sentinelPassword) { secrets.sentinelPassword = options.sentinelPassword; }
                delete stripped.password;
                delete stripped.sentinelPassword;
            } else {
                if (options.password) { secrets.password = options.password; }
                delete stripped.password;
            }
            return { stripped: stripped, secrets: secrets };
        }
        return { stripped: options, secrets: secrets };
    }
    function mergeSecrets(options, secrets) {
        if (!secrets || typeof secrets !== "object") { return options; }
        if (Array.isArray(options)) {
            if (Array.isArray(secrets.nodes)) {
                secrets.nodes.forEach(function (pw, i) {
                    if (pw && options[i]) { options[i].password = pw; }
                });
            }
            return options;
        }
        if (options && typeof options === "object") {
            if (Array.isArray(options.sentinels)) {
                if (secrets.password) { options.password = secrets.password; }
                if (secrets.sentinelPassword) { options.sentinelPassword = secrets.sentinelPassword; }
            } else if (secrets.password) {
                options.password = secrets.password;
            }
        }
        return options;
    }
    function parseSecrets(value) {
        if (!value) { return {}; }
        try { return JSON.parse(value) || {}; } catch (e) { return {}; }
    }
    RED.nodes.registerType('redis-config', {
```

- [ ] **Step 2: Declare the credential**

Find:
```javascript
            optionsType: { value: "json" }
        },
```
Replace with:
```javascript
            optionsType: { value: "json" }
        },
        credentials: {
            secrets: { type: "text" }
        },
```

- [ ] **Step 3: Add the hidden credential input to the template**

Find:
```html
    <input type="hidden" id="node-config-input-options">
    <input type="hidden" id="node-config-input-optionsType">
```
Replace with:
```html
    <input type="hidden" id="node-config-input-options">
    <input type="hidden" id="node-config-input-optionsType">
    <input type="hidden" id="node-config-input-secrets">
```

- [ ] **Step 4: Merge the credential back on load**

Find:
```javascript
            var initialOptions = parseJsonOptions();
            populateFormFromOptions(initialOptions || {});
```
Replace with:
```javascript
            var initialOptions = mergeSecrets(parseJsonOptions() || {}, parseSecrets($("#node-config-input-secrets").val()));
            populateFormFromOptions(initialOptions);
```

- [ ] **Step 5: Extract secrets on save**

Find:
```javascript
            if (type === "json" && this.redisConfigOptionsEditor) {
                var editorVal = this.redisConfigOptionsEditor.getValue();
                try {
                    JSON.parse(editorVal);
                    $("#node-config-input-options").val(editorVal);
                } catch (e) {
                    // Non-JSON editor content (e.g. AWS dnsLookup appended) -
                    // keep the hidden field value set by syncConnectionToEditor
                }
            } else if (type !== "json") {
                $("#node-config-input-options").val($("#redis-config-options-raw").val());
            }
```
Replace with:
```javascript
            if (type === "json" && this.redisConfigOptionsEditor) {
                var editorVal = this.redisConfigOptionsEditor.getValue();
                var parsed = null;
                try {
                    parsed = JSON.parse(editorVal);
                } catch (e) {
                    // Non-JSON editor content (e.g. AWS dnsLookup appended) -
                    // fall back to the hidden field kept in sync by syncConnectionToEditor
                    try { parsed = JSON.parse($("#node-config-input-options").val()); } catch (e2) { parsed = null; }
                }
                if (parsed !== null && parsed !== undefined) {
                    var result = extractSecrets(parsed);
                    $("#node-config-input-options").val(JSON.stringify(result.stripped));
                    $("#node-config-input-secrets").val(
                        Object.keys(result.secrets).length ? JSON.stringify(result.secrets) : ""
                    );
                }
            } else if (type !== "json") {
                $("#node-config-input-options").val($("#redis-config-options-raw").val());
                $("#node-config-input-secrets").val("");
            }
```

- [ ] **Step 6: Add a help paragraph**

Find:
```html
<p>The connection can target a standalone Redis server, Redis Cluster, AWS
MemoryDB/ElastiCache cluster endpoint, or Redis Sentinel.</p>
```
Replace with:
```html
<p>The connection can target a standalone Redis server, Redis Cluster, AWS
MemoryDB/ElastiCache cluster endpoint, or Redis Sentinel.</p>
<p><b>Secret storage:</b> in JSON mode, passwords are saved in Node-RED&rsquo;s encrypted
credentials (not in the exported flow) and merged into the connection at runtime. With the
<b>Environment variable</b> type, the secret stays in the environment and is never written to
the flow.</p>
```

---

## Task 4: Editor verification (Playwright)

**Files:**
- Modify: `test/playwright/redis-editor.spec.js` (add one test inside the `test.describe("Node-RED Redis editor", ...)` block)

- [ ] **Step 1: Add a credential round-trip test**

Add this test inside the `test.describe` block (after the existing `redis-config edits ...` test):

```javascript
  test("redis-config stores the password as a credential, not in the flow", async ({ page }) => {
    await openRedisConfig(page);
    await page.locator("#red-ui-tab-redis-config-tab-connection").click();
    await setSelectValue(page, "#redis-config-mode", "single");
    await setInputValue(page, "#redis-config-single-host", "127.0.0.1");
    await setInputValue(page, "#redis-config-single-port", "6379");
    await setInputValue(page, "#redis-config-single-username", "default");
    await setInputValue(page, "#redis-config-single-password", "super-secret-pw");
    await saveConfigDialog(page);
    await deploy(page);

    // The persisted flow must not contain the password anywhere.
    const flows = await (await page.request.get("flows")).text();
    expect(flows).not.toContain("super-secret-pw");

    // The credential exists for the config node.
    const flowsJson = JSON.parse(flows);
    const configNode = flowsJson.flows
      ? flowsJson.flows.find((n) => n.type === "redis-config")
      : flowsJson.find((n) => n.type === "redis-config");
    expect(configNode).toBeTruthy();
    const cred = await (await page.request.get(`credentials/redis-config/${configNode.id}`)).json();
    expect(JSON.stringify(cred)).toContain("super-secret-pw");

    // Reopen and confirm the password round-trips into the form.
    await openRedisConfig(page);
    await page.locator("#red-ui-tab-redis-config-tab-connection").click();
    await expect(page.locator("#redis-config-single-password")).toHaveValue("super-secret-pw");
    await saveConfigDialog(page);
  });
```

> Note: confirm `openRedisConfig`/`deploy` are exported by `test/playwright/helpers/node-red-editor.js` and imported at the top of the spec (the existing tests already use `saveConfigDialog`, `setSelectValue`, `setInputValue`); add `openRedisConfig` and `deploy` to the import if missing. The `/flows` and `/credentials` admin endpoints are unauthenticated in the test harness (`writeSettings` sets no `adminAuth`).

- [ ] **Step 2: Run the Playwright editor suite**

Run:
```bash
npm run test:playwright
```
Expected: all editor tests pass, including the new credential test. (This starts its own Docker deployment; it does not use the dev Redis from Task 0.)

---

## Task 5: Docs

**Files:**
- Modify: `docs/NODE_GUIDE.md`, `docs/ARCHITECTURE.md`, `docs/REFERENCE_MAP.md`, `docs/TESTING.md`, `.claude/skills/node-red-contrib-redis-maintainer/SKILL.md`

- [ ] **Step 1: `docs/NODE_GUIDE.md` — secret storage note**

Find:
```markdown
- never commit cloud Redis endpoints or credentials in tests, examples, or docs
```
Replace with:
```markdown
- never commit cloud Redis endpoints or credentials in tests, examples, or docs

Secret storage:

- in JSON mode, passwords are extracted into a `text`-type `secrets` credential (encrypted,
  out of `flows.json`) and merged back into `options` at runtime (`mergeSecrets`) and on editor
  load; `extractSecrets` strips them on save. The two helper copies in `redis.js` and `redis.html`
  must stay in sync
- a legacy password still embedded in `options` keeps working (merge is a no-op without a
  credential) and migrates to the credential when the config is reopened and saved
- `env` mode is untouched — the secret stays in the environment
```

- [ ] **Step 2: `docs/ARCHITECTURE.md` — redis-config summary**

Find:
```markdown
Stores Redis connection options and cluster mode.
Options can come from typedInput values and are evaluated in runtime code.
```
Replace with:
```markdown
Stores Redis connection options and cluster mode.
Options can come from typedInput values and are evaluated in runtime code.

In JSON mode, passwords are kept in a `text`-type `secrets` credential (encrypted, never in
`flows.json`) and merged into `options` at runtime; legacy password-in-options still works.
`extractSecrets`/`mergeSecrets` are mirrored in `redis.js` and `redis.html`.
```

- [ ] **Step 3: `docs/REFERENCE_MAP.md` — spec list**

Find:
```markdown
- `../test/redis_lua_ui_spec.js` — Lua editor/library UI (static HTML parse, no Redis needed)
```
Replace with:
```markdown
- `../test/redis_lua_ui_spec.js` — Lua editor/library UI (static HTML parse, no Redis needed)
- `../test/redis_credentials_spec.js` — `redis-config` secret merge from the `secrets` credential (single/cluster/sentinel/legacy/env) plus guarded end-to-end auth
```

- [ ] **Step 4: `docs/TESTING.md` — spec list**

Find:
```markdown
- `redis_lua_ui_spec.js` — Lua editor/library UI; static HTML parse, needs no Redis
```
Replace with:
```markdown
- `redis_lua_ui_spec.js` — Lua editor/library UI; static HTML parse, needs no Redis
- `redis_credentials_spec.js` — `redis-config` secret merge from the `secrets` credential; constructor-only (no Redis) plus a guarded end-to-end auth case in the auth stage
```

- [ ] **Step 5: maintainer skill — spec count + list**

In `.claude/skills/node-red-contrib-redis-maintainer/SKILL.md`, find:
```markdown
Mocha tests (21 spec files — `ls test/*_spec.js` for the live list):
```
Replace with:
```markdown
Mocha tests (22 spec files — `ls test/*_spec.js` for the live list):
```
Then find:
```markdown
  `test/redis_lua_conn_spec.js`, `test/redis_lua_ui_spec.js` (static HTML parse, no Redis needed)
```
Replace with:
```markdown
  `test/redis_lua_conn_spec.js`, `test/redis_lua_ui_spec.js` (static HTML parse, no Redis needed),
  `test/redis_credentials_spec.js` (secret merge from the `secrets` credential)
```

---

## Task 6: Verify the full matrix and commit

- [ ] **Step 1: Stop the dev Redis**

```bash
sudo -n docker stop redis-dev
```
Expected: prints `redis-dev`.

- [ ] **Step 2: Stage the runtime, editor, test, and docs (NOT .gitignore) and commit**

```bash
git add redis.js redis.html test/redis_credentials_spec.js test/playwright/redis-editor.spec.js \
  docs/NODE_GUIDE.md docs/ARCHITECTURE.md docs/REFERENCE_MAP.md docs/TESTING.md \
  .claude/skills/node-red-contrib-redis-maintainer/SKILL.md
git commit -m "Store redis-config passwords in encrypted credentials, not flows.json

Add a text-type 'secrets' credential holding all passwords (single/cluster/
sentinel). extractSecrets strips them from options on save; mergeSecrets
re-injects at editor load and at runtime (JSON mode only; env untouched).
Backward compatible: a legacy password in options still works and migrates on
resave. Helpers mirrored in redis.js and redis.html. Adds redis_credentials_spec.js
and a Playwright credential round-trip test; docs updated.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```
Expected: the pre-commit hook runs the full `npm test` matrix; standalone stages now include `redis_credentials_spec.js` (constructor merge tests pass everywhere; the end-to-end auth test runs in single-auth). All stages green; commit completes.

- [ ] **Step 3 (optional): MemoryDB + Playwright**

MemoryDB: `MEMORYDB_ENABLED=1 MEMORYDB_* npm test` (do not commit credentials). Playwright was run in Task 4.

---

## Done criteria

- The three merge tests fail before the runtime change and pass after.
- Full `npm test` matrix green; Playwright editor suite green.
- Saving a redis-config in JSON mode writes no password into the flow; the password is in the
  encrypted credential and round-trips into the form on reopen.
- Legacy password-in-options flows still connect; `env` mode unchanged.
- Help, NODE_GUIDE, ARCHITECTURE, REFERENCE_MAP, TESTING, and the skill are updated.
- Package version stays `2.0.0`.
