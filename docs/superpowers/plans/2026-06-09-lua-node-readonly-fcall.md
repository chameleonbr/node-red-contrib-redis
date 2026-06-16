# Read-only execution and Redis Functions in `redis-lua-script` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the `redis-lua-script` node with read-only execution variants (`EVAL_RO`/`EVALSHA_RO`) and a Function mode (`FUNCTION LOAD REPLACE` + `FCALL`/`FCALL_RO`), keeping `FUNCTION *`/`SCRIPT *` management in `redis-command`.

**Architecture:** A new `mode` (`script`|`function`) field and a `readonly` flag drive a runtime-derived command in `RedisLua` (`redis.js`). Script mode keeps today's `EVAL`/`EVALSHA` + `NOSCRIPT` recovery, gaining `_RO` variants. Function mode loads the editor's library source with `FUNCTION LOAD REPLACE` on every connection `ready` (on all masters when clustered) and invokes `node.fname` via `FCALL`/`FCALL_RO`, recovering once on a missing-function error. The editor (`redis.html`) gains a mode select, a read-only checkbox, and a function-name field, all round-tripped through `RED.library.create`.

**Tech Stack:** Node-RED v4.1 custom node (CommonJS), ioredis v5.11, Mocha + `node-red-node-test-helper`, Playwright, Docker-managed Redis deployments.

**Spec:** `docs/superpowers/specs/2026-06-09-lua-node-readonly-fcall-design.md`

**Conventions (from `CLAUDE.md`):** semicolons on, double quotes, trailing commas `es5`, print width 100, 2-space indent. CommonJS, `function` for Node-RED constructors. **Write all async code with `async`/`await` + `try`/`catch` — never `.then()/.catch()` chains and never new callback-style ioredis calls** (ioredis methods return a promise when called without a callback; `await client.eval(args)`). This converts the previously callback-based `RedisLua` to async/await. TDD: write the failing test, see it fail, implement, see it pass, commit. (The runtime/test code blocks below were originally drafted in callback/Promise-chain form and have been superseded by the async/await implementation in `redis.js` and `test/scripting_commands_spec.js`.)

**Running tests:**
- UI/static spec (no Redis): `npm run test:mocha -- test/redis_lua_ui_spec.js`
- Runtime specs (need a Redis you started yourself on `127.0.0.1:6379`): `npm run test:mocha -- test/scripting_commands_spec.js`
- Full Docker matrix (all deployments): `npm test`
- Browser editor: `npm run test:playwright`

> ioredis exposes `eval_ro`, `evalsha_ro`, `fcall`, `fcall_ro`, `function`, and `script` as client methods (verified against ioredis v5.11.0). They accept the same `(argsArray, callback)` form the node already uses for `client.eval(args, cb)` — ioredis flattens the array into positional arguments.

---

## File Structure

- **Modify** `redis.js` (`RedisLua`, lines ~827-921) — read `mode`/`readonly`/`fname`; cluster-aware library loader; command resolution for script vs function and read-only.
- **Modify** `redis.html`:
  - `redis-lua-script` `registerType` defaults + `oneditprepare` + template (lines ~2239-2364)
  - `redis-lua-script` help (lines ~2366-2432)
  - `redis-command` help (lines ~2150-2234)
- **Modify** `test/redis_lua_ui_spec.js` — static assertions for the new fields.
- **Modify** `test/scripting_commands_spec.js` — runtime tests driving the Lua node directly.
- **Modify** `test/redis_cluster_deployment_spec.js`, `test/redis_sentinel_deployment_spec.js`, `test/memorydb_deployment_spec.js` — per-deployment FCALL / read-only coverage.
- **Modify** `test/playwright/redis-editor.spec.js` — library Save metadata + Open round-trip.
- **Create** `examples/redis-fcall.json` — Function-mode example flow.
- **Modify** docs: `docs/NODE_GUIDE.md`, `README.md`, and (if coverage notes change) `docs/TESTING.md`, `docs/REFERENCE_MAP.md`, `.claude/skills/node-red-contrib-redis-maintainer/SKILL.md`.

---

## Task 1: Editor — failing UI assertions for the new fields

**Files:**
- Test: `test/redis_lua_ui_spec.js`

- [ ] **Step 1: Write the failing tests**

Add this `describe` block to `test/redis_lua_ui_spec.js`, immediately after the existing `describe("redis-lua-script UI template", ...)` block (before `describe("redis-config UI template", ...)`). It reuses the file's existing `html`, `extractFieldsBody`, `hasStringField`, and `hasObjectField` helpers.

```javascript
describe("redis-lua-script mode/readonly/fname fields", function () {
    function luaDefaultsBody() {
        const m = html.match(
            /registerType\('redis-lua-script',[\s\S]*?defaults:\s*\{([\s\S]*?)\},\s*\n\s*label:/
        );
        return m ? m[1] : null;
    }

    it("registers mode, readonly, and fname in defaults", function () {
        const defaults = luaDefaultsBody();
        assert.ok(defaults !== null, "redis-lua-script defaults block should be present");
        assert.match(defaults, /\bmode\s*:/, "defaults should include 'mode'");
        assert.match(defaults, /\breadonly\s*:/, "defaults should include 'readonly'");
        assert.match(defaults, /\bfname\s*:/, "defaults should include 'fname'");
    });

    it("template has a mode select, a read-only checkbox, and a function-name input", function () {
        assert.match(html, /id="node-input-mode"/, "template should include #node-input-mode");
        assert.match(html, /id="node-input-readonly"/, "template should include #node-input-readonly");
        assert.match(html, /id="node-input-fname"/, "template should include #node-input-fname");
    });

    it("library.create persists mode (object), readonly (object), and fname (string)", function () {
        const fieldsBody = extractFieldsBody(html);
        assert.ok(fieldsBody !== null, "fields array should be present");
        assert.ok(hasStringField(fieldsBody, "fname"), "fields should include 'fname' as a string field");
        assert.ok(
            hasObjectField(fieldsBody, "mode"),
            "'mode' must be an object field with get/set so Open Library re-applies field visibility"
        );
        assert.ok(
            hasObjectField(fieldsBody, "readonly"),
            "'readonly' must be an object field with get/set returning the string \"true\"/\"false\""
        );
    });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm run test:mocha -- test/redis_lua_ui_spec.js`
Expected: the three new tests FAIL (e.g. "defaults should include 'mode'"), existing tests PASS.

- [ ] **Step 3: Commit the failing tests**

```bash
git add test/redis_lua_ui_spec.js
git commit -m "test(lua): assert mode/readonly/fname editor + library fields"
```

---

## Task 2: Editor — implement `redis.html` fields, visibility, and library round-trip

**Files:**
- Modify: `redis.html` (`redis-lua-script` `registerType` defaults, `oneditprepare`, and template, ~2246-2364)

- [ ] **Step 1: Add the new `defaults`**

In the `RED.nodes.registerType('redis-lua-script', { ... defaults: { ... } })` block, the `block` default is the last entry:

```javascript
            block: {
                value: false
            }
```

Replace it with `block` plus the three new fields:

```javascript
            block: {
                value: false
            },
            mode: {
                value: "script"
            },
            readonly: {
                value: false
            },
            fname: {
                value: ""
            }
```

- [ ] **Step 2: Update `oneditprepare` (visibility + library fields)**

Replace the entire current `oneditprepare` function:

```javascript
        oneditprepare: function () {
            var that = this;
            $("#node-input-keyval").spinner({
                min: 0
            });
            this.editor = RED.editor.createEditor({
                id: 'node-input-func-editor',
                mode: 'ace/mode/lua',
                value: $("#node-input-func").val()
            });
            RED.library.create({
                url: "functions",
                type: "lua",
                editor: this.editor,
                mode: "ace/mode/lua",
                ext: "lua",
                fields: [
                    'name',
                    'keyval',
                    {
                        name: 'stored',
                        get: function() { return $("#node-input-stored").is(":checked") ? "true" : "false"; },
                        set: function(val) { $("#node-input-stored").prop("checked", val === "true"); }
                    },
                    {
                        name: 'block',
                        get: function() { return $("#node-input-block").is(":checked") ? "true" : "false"; },
                        set: function(val) { $("#node-input-block").prop("checked", val === "true"); }
                    }
                ]
            });
            this.editor.focus();
        },
```

with this version (adds `updateLuaModeVisibility`, the `mode`/`readonly`/`fname` fields, and a `mode` `set` that refreshes visibility — required because Node-RED's library load sets values without firing change events):

```javascript
        oneditprepare: function () {
            var that = this;
            $("#node-input-keyval").spinner({
                min: 0
            });

            // Script mode shows the Stored checkbox; Function mode shows the
            // Function name field and relabels the editor as the library source.
            function updateLuaModeVisibility() {
                var isFunction = $("#node-input-mode").val() === "function";
                $("#redis-lua-fname-row").toggle(isFunction);
                $("#redis-lua-stored-row").toggle(!isFunction);
                $("#redis-lua-editor-label").text(isFunction ? "Lua Library" : "Lua Script");
            }
            $("#node-input-mode").on("change", updateLuaModeVisibility);
            updateLuaModeVisibility();

            this.editor = RED.editor.createEditor({
                id: 'node-input-func-editor',
                mode: 'ace/mode/lua',
                value: $("#node-input-func").val()
            });
            RED.library.create({
                url: "functions",
                type: "lua",
                editor: this.editor,
                mode: "ace/mode/lua",
                ext: "lua",
                fields: [
                    'name',
                    'keyval',
                    'fname',
                    {
                        name: 'mode',
                        get: function() { return $("#node-input-mode").val(); },
                        set: function(val) {
                            $("#node-input-mode").val(val || "script");
                            updateLuaModeVisibility();
                        }
                    },
                    {
                        name: 'stored',
                        get: function() { return $("#node-input-stored").is(":checked") ? "true" : "false"; },
                        set: function(val) { $("#node-input-stored").prop("checked", val === "true"); }
                    },
                    {
                        name: 'readonly',
                        get: function() { return $("#node-input-readonly").is(":checked") ? "true" : "false"; },
                        set: function(val) { $("#node-input-readonly").prop("checked", val === "true"); }
                    },
                    {
                        name: 'block',
                        get: function() { return $("#node-input-block").is(":checked") ? "true" : "false"; },
                        set: function(val) { $("#node-input-block").prop("checked", val === "true"); }
                    }
                ]
            });
            this.editor.focus();
        },
```

- [ ] **Step 3: Update the template markup**

In the `<script type="text/x-red" data-template-name="redis-lua-script">` block:

(a) Add a Mode row right after the Name `form-row` (the one containing `node-input-name`), before the Keys row:

```html
    <div class="form-row">
        <label for="node-input-mode"><i class="fa fa-cogs"></i> Mode</label>
        <select id="node-input-mode" style="width:70%;">
            <option value="script">Script (EVAL / EVALSHA)</option>
            <option value="function">Function (FCALL)</option>
        </select>
    </div>
```

(b) Add a Function-name row right after the Keys `form-row` (the one containing `node-input-keyval`):

```html
    <div class="form-row" id="redis-lua-fname-row">
        <label for="node-input-fname"><i class="fa fa-superscript"></i> Function</label>
        <input type="text" id="node-input-fname" placeholder="registered function name">
    </div>
```

(c) Add `id="redis-lua-stored-row"` to the existing Stored checkbox `form-row`, and add a Read-only row after it. Replace this existing block:

```html
    <div class="form-row">
        <label>&nbsp;</label>
        <input type="checkbox" id="node-input-stored" style="display:inline-block; width:15px; vertical-align:baseline;">
        <label for="node-input-stored" style="width:auto; margin-left:4px;">Stored Script (EVALSHA)</label>
    </div>
```

with:

```html
    <div class="form-row" id="redis-lua-stored-row">
        <label>&nbsp;</label>
        <input type="checkbox" id="node-input-stored" style="display:inline-block; width:15px; vertical-align:baseline;">
        <label for="node-input-stored" style="width:auto; margin-left:4px;">Stored Script (EVALSHA)</label>
    </div>
    <div class="form-row">
        <label>&nbsp;</label>
        <input type="checkbox" id="node-input-readonly" style="display:inline-block; width:15px; vertical-align:baseline;">
        <label for="node-input-readonly" style="width:auto; margin-left:4px;">Read-only (EVAL_RO / EVALSHA_RO / FCALL_RO)</label>
    </div>
```

(d) Give the editor label a span id so the mode toggle can relabel it. Replace:

```html
        <label for="node-input-func"><i class="fa fa-code"></i> Lua Script</label>
```

with:

```html
        <label for="node-input-func"><i class="fa fa-code"></i> <span id="redis-lua-editor-label">Lua Script</span></label>
```

- [ ] **Step 4: Run the UI spec to verify it passes**

Run: `npm run test:mocha -- test/redis_lua_ui_spec.js`
Expected: all tests PASS (the three new ones plus the existing checkbox/type/ext/fields tests).

- [ ] **Step 5: Commit**

```bash
git add redis.html
git commit -m "feat(lua): editor mode select, read-only flag, function name + library round-trip"
```

---

## Task 3: Runtime — read-only + function execution in `RedisLua`

**Files:**
- Test: `test/scripting_commands_spec.js`
- Modify: `redis.js` (`RedisLua`, ~827-921)

- [ ] **Step 1: Add test helpers and failing tests**

In `test/scripting_commands_spec.js`, add a `waitForLib` helper next to the existing `waitForSha1` (top of file):

```javascript
// Polls until the function-mode node has loaded its library (libname set).
function waitForLib(node, cb) {
  const start = Date.now();
  const tick = () => {
    if (node.libname) {
      cb();
    } else if (Date.now() - start > 4000) {
      cb(new Error("library was never loaded (libname not set)"));
    } else {
      setTimeout(tick, 25);
    }
  };
  tick();
}
```

Then add these tests inside the existing `describe("Scripting commands", function () { ... })` block (after the last existing `it(...)`). They drive the `redis-lua-script` node directly and namespace keys under `test:script:*` (the suite's `afterEach` cleanup pattern).

```javascript
  it("redis-lua-script runs EVAL_RO when read-only is set", function (done) {
    const seed = directRedis();
    const flow = [
      configNode,
      {
        id: "luaro-node",
        type: "redis-lua-script",
        server: "config1",
        name: "luaro",
        mode: "script",
        readonly: true,
        stored: false,
        keyval: 1,
        func: "return redis.call('GET', KEYS[1])",
        block: false,
        wires: [["luaro-helper"]],
      },
      { id: "luaro-helper", type: "helper" },
    ];

    seed
      .set("test:script:ro", "ro-value")
      .then(() => {
        helper.load(redisNode, flow, () => {
          const node = helper.getNode("luaro-node");
          const sink = helper.getNode("luaro-helper");
          sink.on("input", (msg) => {
            try {
              msg.payload.should.equal("ro-value");
              node.command.should.equal("eval_ro");
              seed.disconnect();
              done();
            } catch (err) {
              seed.disconnect();
              done(err);
            }
          });
          node.receive({ payload: ["test:script:ro"] });
        });
      })
      .catch((err) => {
        seed.disconnect();
        done(err);
      });
  });

  it("redis-lua-script runs EVALSHA_RO and falls back to EVAL_RO after SCRIPT FLUSH", function (done) {
    const seed = directRedis();
    const flow = [
      configNode,
      {
        id: "sharo-node",
        type: "redis-lua-script",
        server: "config1",
        name: "sharo",
        mode: "script",
        readonly: true,
        stored: true,
        keyval: 1,
        func: "return redis.call('GET', KEYS[1])",
        block: false,
        wires: [["sharo-helper"]],
      },
      { id: "sharo-helper", type: "helper" },
    ];

    seed
      .set("test:script:sharo", "sha-value")
      .then(() => {
        helper.load(redisNode, flow, () => {
          const node = helper.getNode("sharo-node");
          const sink = helper.getNode("sharo-helper");
          let first = true;
          waitForSha1(node, (err) => {
            if (err) {
              seed.disconnect();
              return done(err);
            }
            sink.on("input", (msg) => {
              try {
                msg.payload.should.equal("sha-value");
                if (first) {
                  first = false;
                  node.command.should.equal("evalsha_ro");
                  // Evict the cached script, then re-fire to force NOSCRIPT -> EVAL_RO.
                  seed
                    .script("flush")
                    .then(() => node.receive({ payload: ["test:script:sharo"] }));
                } else {
                  node.command.should.equal("eval_ro");
                  seed.disconnect();
                  done();
                }
              } catch (e) {
                seed.disconnect();
                done(e);
              }
            });
            node.receive({ payload: ["test:script:sharo"] });
          });
        });
      })
      .catch((e) => {
        seed.disconnect();
        done(e);
      });
  });

  it("redis-lua-script loads a library and runs FCALL", function (done) {
    const lib = [
      "#!lua name=testlib",
      "redis.register_function('testfn', function(keys, args) return redis.call('GET', keys[1]) end)",
    ].join("\n");
    const seed = directRedis();
    const flow = [
      configNode,
      {
        id: "fcall-node",
        type: "redis-lua-script",
        server: "config1",
        name: "fcall",
        mode: "function",
        readonly: false,
        keyval: 1,
        func: lib,
        fname: "testfn",
        block: false,
        wires: [["fcall-helper"]],
      },
      { id: "fcall-helper", type: "helper" },
    ];

    Promise.all([seed.function("flush"), seed.set("test:script:fcall", "fn-value")])
      .then(() => {
        helper.load(redisNode, flow, () => {
          const node = helper.getNode("fcall-node");
          const sink = helper.getNode("fcall-helper");
          waitForLib(node, (err) => {
            if (err) {
              seed.disconnect();
              return done(err);
            }
            sink.on("input", (msg) => {
              try {
                msg.payload.should.equal("fn-value");
                node.command.should.equal("fcall");
                seed.disconnect();
                done();
              } catch (e) {
                seed.disconnect();
                done(e);
              }
            });
            node.receive({ payload: ["test:script:fcall"] });
          });
        });
      })
      .catch((e) => {
        seed.disconnect();
        done(e);
      });
  });

  it("redis-lua-script runs FCALL_RO for a no-writes function", function (done) {
    const lib = [
      "#!lua name=testlibro",
      "redis.register_function{function_name='testfnro', callback=function(keys, args) return redis.call('GET', keys[1]) end, flags={'no-writes'}}",
    ].join("\n");
    const seed = directRedis();
    const flow = [
      configNode,
      {
        id: "fcallro-node",
        type: "redis-lua-script",
        server: "config1",
        name: "fcallro",
        mode: "function",
        readonly: true,
        keyval: 1,
        func: lib,
        fname: "testfnro",
        block: false,
        wires: [["fcallro-helper"]],
      },
      { id: "fcallro-helper", type: "helper" },
    ];

    Promise.all([seed.function("flush"), seed.set("test:script:fcallro", "ro-fn-value")])
      .then(() => {
        helper.load(redisNode, flow, () => {
          const node = helper.getNode("fcallro-node");
          const sink = helper.getNode("fcallro-helper");
          waitForLib(node, (err) => {
            if (err) {
              seed.disconnect();
              return done(err);
            }
            sink.on("input", (msg) => {
              try {
                msg.payload.should.equal("ro-fn-value");
                node.command.should.equal("fcall_ro");
                seed.disconnect();
                done();
              } catch (e) {
                seed.disconnect();
                done(e);
              }
            });
            node.receive({ payload: ["test:script:fcallro"] });
          });
        });
      })
      .catch((e) => {
        seed.disconnect();
        done(e);
      });
  });

  it("redis-lua-script reloads its library and retries FCALL after FUNCTION FLUSH", function (done) {
    const lib = [
      "#!lua name=testlibrecover",
      "redis.register_function('recoverfn', function(keys, args) return redis.call('GET', keys[1]) end)",
    ].join("\n");
    const seed = directRedis();
    const flow = [
      configNode,
      {
        id: "rec-node",
        type: "redis-lua-script",
        server: "config1",
        name: "rec",
        mode: "function",
        readonly: false,
        keyval: 1,
        func: lib,
        fname: "recoverfn",
        block: false,
        wires: [["rec-helper"]],
      },
      { id: "rec-helper", type: "helper" },
    ];

    Promise.all([seed.function("flush"), seed.set("test:script:rec", "rec-value")])
      .then(() => {
        helper.load(redisNode, flow, () => {
          const node = helper.getNode("rec-node");
          const sink = helper.getNode("rec-helper");
          let first = true;
          waitForLib(node, (err) => {
            if (err) {
              seed.disconnect();
              return done(err);
            }
            sink.on("input", (msg) => {
              try {
                msg.payload.should.equal("rec-value");
                if (first) {
                  first = false;
                  // Drop the library out-of-band, then re-fire: FCALL should
                  // hit "function not found", reload, and retry successfully.
                  seed
                    .function("flush")
                    .then(() => node.receive({ payload: ["test:script:rec"] }));
                } else {
                  seed.disconnect();
                  done();
                }
              } catch (e) {
                seed.disconnect();
                done(e);
              }
            });
            node.receive({ payload: ["test:script:rec"] });
          });
        });
      })
      .catch((e) => {
        seed.disconnect();
        done(e);
      });
  });

  it("redis-lua-script errors in function mode when the library source is empty", function (done) {
    const flow = [
      configNode,
      {
        id: "badfn-node",
        type: "redis-lua-script",
        server: "config1",
        name: "badfn",
        mode: "function",
        readonly: false,
        keyval: 0,
        func: "",
        fname: "whatever",
        block: false,
        wires: [["badfn-helper"]],
      },
      { id: "badfn-helper", type: "helper" },
    ];

    helper.load(redisNode, flow, () => {
      const node = helper.getNode("badfn-node");
      node.once("call:error", (call) => {
        try {
          String(call.args[0]).should.match(/library source/i);
          done();
        } catch (e) {
          done(e);
        }
      });
      node.receive({ payload: [] });
    });
  });
```

- [ ] **Step 2: Run the runtime tests to verify they fail**

Start a local Redis 7.2+ on `127.0.0.1:6379` (e.g. `docker run --rm -p 6379:6379 redis:8`), then:

Run: `npm run test:mocha -- test/scripting_commands_spec.js`
Expected: the six new tests FAIL (read-only ones return via `EVAL`/`EVALSHA` so `node.command` mismatches; function-mode ones never load a library so `waitForLib` times out / FCALL errors). Existing tests PASS.

- [ ] **Step 3: Implement the runtime changes**

Replace the entire `RedisLua` function in `redis.js` (currently lines ~827-921, from `function RedisLua(n) {` through the line `RED.nodes.registerType("redis-lua-script", RedisLua);`) with:

```javascript
  function RedisLua(n) {
    RED.nodes.createNode(this, n);
    this.server = RED.nodes.getNode(n.server);
    this.func = n.func;
    this.name = n.name;
    this.keyval = n.keyval;
    this.stored = n.stored;
    this.mode = n.mode || "script";
    this.readonly = n.readonly || false;
    this.fname = n.fname || "";
    this.sha1 = "";
    this.libname = "";
    this.command = "eval";
    var node = this;
    this.block = n.block || false;
    let id = this.block ? n.id : this.server.name;

    let client = getConn(this.server, id);

    // FUNCTION LOAD/SCRIPT LOAD are node-local. In cluster mode the load must
    // reach every master so an FCALL/EVALSHA routed to any shard can resolve;
    // standalone/sentinel has a single target. Runs the loads in parallel.
    var loadLibraryOn = function (cb) {
      var targets = typeof client.nodes === "function" ? client.nodes("master") : [client];
      var pending = targets.length;
      if (pending === 0) {
        cb(new Error("no Redis master available"));
        return;
      }
      var failed = null;
      var name = null;
      targets.forEach(function (c) {
        c.function("load", "replace", node.func, function (err, res) {
          if (err) {
            failed = err;
          } else {
            name = res;
          }
          pending -= 1;
          if (pending === 0) {
            cb(failed, name);
          }
        });
      });
    };

    // On every "ready" (including reconnects) reload, since Redis is volatile
    // and a reconnected/replaced server may have lost the script/library.
    var loadLibrary = function () {
      if (!node.func || node.func.trim() === "") {
        node.status({ fill: "red", shape: "dot", text: "no library source" });
        return;
      }
      loadLibraryOn(function (err, name) {
        if (err) {
          node.status({ fill: "red", shape: "dot", text: "library not loaded" });
        } else {
          node.status({ fill: "green", shape: "dot", text: "library loaded" });
          node.libname = name;
        }
      });
    };

    var loadScript = function () {
      client.script("load", node.func, function (err, res) {
        if (err) {
          node.status({ fill: "red", shape: "dot", text: "script not loaded" });
        } else {
          node.status({ fill: "green", shape: "dot", text: "script loaded" });
          node.sha1 = res;
        }
      });
    };

    let removeListeners;
    if (node.mode === "function") {
      removeListeners = attachStatusListeners(node, client, loadLibrary);
    } else if (node.stored) {
      removeListeners = attachStatusListeners(node, client, loadScript);
    } else {
      removeListeners = attachStatusListeners(node, client);
    }

    node.on("close", async function (done) {
      removeListeners();
      node.status({});
      await disconnect(id);
      client = null;
      done();
    });

    node.on("input", function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments) };
      done = done || function (err) { if (err) node.error(err, msg); };

      if (node.mode === "function" && (!node.func || node.func.trim() === "")) {
        node.status({ fill: "red", shape: "dot", text: "no library source" });
        done(Error("Function mode requires a library source"));
        return;
      }
      if (node.keyval > 0 && !Array.isArray(msg.payload)) {
        throw Error("Payload is not Array");
      }

      // Sends the result downstream and releases the execution slot.
      var handleResult = function (res) {
        msg.payload = res;
        send(msg);
        done();
      };

      // [leadingArg, numkeys, ...keysAndArgs]; ioredis flattens the array.
      var argsWith = function (head) {
        return [head, node.keyval].concat(msg.payload);
      };

      if (node.mode === "function") {
        // FCALL/FCALL_RO invoke a registered function by name. If the library
        // was FUNCTION FLUSH/DELETE'd out of band, reload once and retry,
        // mirroring the stored-script NOSCRIPT recovery.
        var fcallCmd = node.readonly ? "fcall_ro" : "fcall";
        node.command = fcallCmd;
        var runFcall = function (reloaded) {
          client[fcallCmd](argsWith(node.fname), function (err, res) {
            if (err) {
              if (
                !reloaded &&
                err.message &&
                err.message.toLowerCase().indexOf("function not found") !== -1
              ) {
                loadLibraryOn(function (loadErr) {
                  if (loadErr) {
                    done(loadErr);
                  } else {
                    runFcall(true);
                  }
                });
              } else {
                done(err);
              }
            } else {
              handleResult(res);
            }
          });
        };
        runFcall(false);
        return;
      }

      // Script mode: ship the full body with EVAL/EVAL_RO so Redis (re)caches
      // it under its SHA1. Used directly for unstored scripts and as the
      // NOSCRIPT fallback for stored ones.
      var runWithEval = function () {
        var evalCmd = node.readonly ? "eval_ro" : "eval";
        node.command = evalCmd;
        client[evalCmd](argsWith(node.func), function (err, res) {
          if (err) {
            done(err);
          } else {
            handleResult(res);
          }
        });
      };

      if (node.stored) {
        // Stored scripts prefer EVALSHA(_RO) to avoid resending the body. A
        // NOSCRIPT means the SHA1 is no longer cached — fall back to the
        // matching EVAL variant (EVAL_RO stays read-only) which re-caches it.
        var evalshaCmd = node.readonly ? "evalsha_ro" : "evalsha";
        node.command = evalshaCmd;
        client[evalshaCmd](argsWith(node.sha1), function (err, res) {
          if (err) {
            if (err.message && err.message.indexOf("NOSCRIPT") !== -1) {
              runWithEval();
            } else {
              done(err);
            }
          } else {
            handleResult(res);
          }
        });
      } else {
        runWithEval();
      }
    });
  }
  RED.nodes.registerType("redis-lua-script", RedisLua);
```

- [ ] **Step 4: Run the runtime tests to verify they pass**

Run: `npm run test:mocha -- test/scripting_commands_spec.js`
Expected: all tests PASS (existing + six new).

- [ ] **Step 5: Run the Lua connection spec to confirm no regression**

Run: `npm run test:mocha -- test/redis_lua_conn_spec.js`
Expected: PASS (connection-id behavior unchanged).

- [ ] **Step 6: Commit**

```bash
git add redis.js test/scripting_commands_spec.js
git commit -m "feat(lua): read-only EVAL/EVALSHA variants and FCALL function mode with reload recovery"
```

---

## Task 4: Help text — both nodes state the execution-vs-management split

**Files:**
- Modify: `redis.html` (`redis-lua-script` help ~2366-2432, `redis-command` help ~2150-2234)

- [ ] **Step 1: Update the `redis-lua-script` intro and Properties**

In `<script type="text/x-red" data-help-name="redis-lua-script">`, replace the opening paragraph:

```html
<p>Executes a Lua script atomically inside Redis using <code>EVAL</code> or
<code>EVALSHA</code>. Because Redis runs the script in a single step, it is
ideal for read-modify-write operations that must not be interrupted by other clients.</p>
```

with:

```html
<p>Executes server-side Lua atomically. In <strong>Script</strong> mode it runs a Lua
script body with <code>EVAL</code>/<code>EVALSHA</code> (or their read-only
<code>_RO</code> variants); in <strong>Function</strong> mode it loads a Redis Functions
library and calls a registered function with <code>FCALL</code>/<code>FCALL_RO</code>.</p>
```

Then, inside the `<h3>Properties</h3>` `<dl>`, add these entries immediately after the `<dt>Server</dt> ... </dd>` pair:

```html
    <dt>Mode</dt>
    <dd><strong>Script</strong> runs the Lua body in the editor. <strong>Function</strong>
        treats the editor as a Redis Functions <em>library</em> source (must start with the
        <code>#!lua name=&lt;lib&gt;</code> shebang and call <code>redis.register_function</code>);
        the node runs <code>FUNCTION LOAD REPLACE</code> on deploy/reconnect and calls the
        function named in <strong>Function</strong> with <code>FCALL</code>.</dd>
    <dt>Read-only</dt>
    <dd>Uses the read-only command variant: <code>EVAL_RO</code>/<code>EVALSHA_RO</code> in
        Script mode, <code>FCALL_RO</code> in Function mode. Read-only functions must be
        registered with the <code>no-writes</code> flag.</dd>
    <dt>Function</dt>
    <dd>(Function mode) The registered function name to invoke. The library source is required;
        an empty library in Function mode is a configuration error.</dd>
```

- [ ] **Step 2: Add a "Managing scripts and functions" note to the Lua help**

In the same help block, immediately before the `<h3>Examples</h3>` heading, insert:

```html
<h3>Managing scripts and functions</h3>
<p>This node only <em>executes</em> scripts and functions. Run management/admin
subcommands through the <em>redis-command</em> node: select <code>SCRIPT</code> or
<code>FUNCTION</code> and pass the subcommand and arguments in <code>msg.payload</code>,
e.g. <code>["LOAD", "&lt;library source&gt;"]</code>, <code>["LIST"]</code>,
<code>["FLUSH"]</code>. Covered there:
<code>SCRIPT DEBUG/EXISTS/FLUSH/KILL/LOAD</code> and
<code>FUNCTION DELETE/DUMP/FLUSH/KILL/LIST/LOAD/RESTORE/STATS</code>.</p>
```

- [ ] **Step 3: Add the matching note to the `redis-command` help**

In `<script type="text/x-red" data-help-name="redis-command">`, immediately before the final `<p>Full Redis command reference: ...</p>`, insert:

```html
<h3>Managing scripts and functions</h3>
<p>This node is the home for Lua script-cache and Redis Functions management. Pick
<code>SCRIPT</code> or <code>FUNCTION</code> as the command and put the subcommand and
its arguments in <code>msg.payload</code>:</p>
<ul>
    <li><strong>SCRIPT LOAD:</strong> Command <code>SCRIPT</code>,
        payload <code>["LOAD", "return 1"]</code> &rarr; returns the SHA1.</li>
    <li><strong>FUNCTION LOAD:</strong> Command <code>FUNCTION</code>,
        payload <code>["LOAD", "REPLACE", "#!lua name=mylib\nredis.register_function('f', function() return 1 end)"]</code></li>
    <li><strong>FUNCTION LIST / STATS / FLUSH:</strong> Command <code>FUNCTION</code>,
        payload <code>["LIST"]</code> / <code>["STATS"]</code> / <code>["FLUSH"]</code>.</li>
</ul>
<p>The <em>redis-lua-script</em> node handles execution (<code>EVAL</code>/<code>EVALSHA</code>/<code>FCALL</code>
and their read-only variants).</p>
```

- [ ] **Step 4: Verify the UI spec still parses cleanly**

Run: `npm run test:mocha -- test/redis_lua_ui_spec.js`
Expected: PASS (help-text edits do not affect the parsed `registerType`/`library.create` regions).

- [ ] **Step 5: Commit**

```bash
git add redis.html
git commit -m "docs(help): document Lua modes/read-only and the execution-vs-management split"
```

---

## Task 5: `docs/NODE_GUIDE.md` — both node sections

**Files:**
- Modify: `docs/NODE_GUIDE.md` (`redis-command` ~130-152, `redis-lua-script` ~154-181)

- [ ] **Step 1: Expand the `redis-lua-script` section**

Replace the current `Behavior:` list under `## \`redis-lua-script\``:

```markdown
Behavior:

- unstored scripts use `EVAL`
- stored scripts load with `SCRIPT LOAD`
- stored scripts run via `EVALSHA`
- `NOSCRIPT` falls back to `EVAL`
```

with:

```markdown
Behavior (command resolved from `mode` + `stored` + `readonly`):

| mode | stored | readonly | on ready | on input | recovery |
|------|--------|----------|----------|----------|----------|
| script | no | no | — | `EVAL` | — |
| script | no | yes | — | `EVAL_RO` | — |
| script | yes | no | `SCRIPT LOAD` | `EVALSHA` | `NOSCRIPT` → `EVAL` |
| script | yes | yes | `SCRIPT LOAD` | `EVALSHA_RO` | `NOSCRIPT` → `EVAL_RO` |
| function | n/a | no | `FUNCTION LOAD REPLACE` | `FCALL` | "function not found" → reload → retry once |
| function | n/a | yes | `FUNCTION LOAD REPLACE` | `FCALL_RO` | "function not found" → reload → retry once |

- Function mode treats the editor as a Redis Functions library source (`#!lua name=…`);
  the node `FUNCTION LOAD REPLACE`s it on every connection `ready`, on all masters in
  cluster mode, so an `FCALL` routed to any shard can resolve. An empty library source in
  Function mode is a configuration error.
- This node is execution-only. `FUNCTION *` and `SCRIPT *` management subcommands stay in
  `redis-command`.
```

- [ ] **Step 2: Cross-reference from the `redis-command` section**

Under `## \`redis-command\``, in the `Use this node for:` list, add a bullet:

```markdown
- `FUNCTION` and `SCRIPT` management subcommands (LOAD, LIST, FLUSH, EXISTS, KILL, …);
  the `redis-lua-script` node only executes
```

- [ ] **Step 3: Commit**

```bash
git add docs/NODE_GUIDE.md
git commit -m "docs(node-guide): lua command-resolution table and management cross-reference"
```

---

## Task 6: `README.md`

**Files:**
- Modify: `README.md` (Examples ~59, Nodes table ~68-69, Message Patterns ~166)

- [ ] **Step 1: Update the Nodes table**

Replace the `redis-lua-script` row:

```markdown
| `redis-lua-script` | Atomic server-side logic         | `EVAL` or stored `SCRIPT LOAD` plus `EVALSHA`, `NOSCRIPT` recovery, Lua editor, library metadata, and optional dedicated connection.                       |
```

with:

```markdown
| `redis-lua-script` | Atomic server-side logic         | Script mode (`EVAL`/`EVAL_RO`, stored `SCRIPT LOAD`+`EVALSHA`/`EVALSHA_RO`, `NOSCRIPT` recovery) and Function mode (`FUNCTION LOAD REPLACE` + `FCALL`/`FCALL_RO`); Lua editor, library metadata, dedicated-connection option. |
```

And replace the `redis-command` row:

```markdown
| `redis-command`    | General Redis commands           | Runs configured commands through ioredis, supports JSON params, message overrides, and a dedicated connection option for blocking work.                    |
```

with:

```markdown
| `redis-command`    | General Redis commands           | Runs configured commands through ioredis, supports JSON params, message overrides, and a dedicated connection option for blocking work. Home for `FUNCTION`/`SCRIPT` management subcommands. |
```

- [ ] **Step 2: Add the example to the Examples list**

After the line:

```markdown
- [`redis-lua-script.json`](examples/redis-lua-script.json) - Lua scripting patterns.
```

add:

```markdown
- [`redis-fcall.json`](examples/redis-fcall.json) - Redis Functions (`FCALL`) pattern.
```

- [ ] **Step 3: Extend the Message Patterns guidance**

Replace:

```markdown
For advanced commands or Redis modules, prefer `redis-command` before writing a Function
node. For custom client code that really needs ioredis directly, use `redis-instance`.
```

with:

```markdown
For advanced commands or Redis modules, prefer `redis-command` before writing a Function
node. `redis-lua-script` *executes* Lua scripts and Redis Functions, while `FUNCTION *` and
`SCRIPT *` *management* subcommands run through `redis-command`. For custom client code that
really needs ioredis directly, use `redis-instance`.
```

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs(readme): note Lua read-only/function modes and management split"
```

---

## Task 7: Example flow `examples/redis-fcall.json`

**Files:**
- Create: `examples/redis-fcall.json`

- [ ] **Step 1: Create the example flow**

```json
[
    {
        "id": "fcall-comment",
        "type": "comment",
        "z": "fcall-flow",
        "name": "Redis Functions: FUNCTION LOAD + FCALL",
        "info": "The redis lua node in Function mode loads the library source (FUNCTION LOAD REPLACE) on deploy, then calls the registered function `getval` with FCALL. Inject sends [key] as KEYS[1]; the debug node shows the returned value. Manage libraries (LIST/FLUSH/DELETE) with the redis-command node.",
        "x": 230,
        "y": 80,
        "wires": []
    },
    {
        "id": "fcall-inject",
        "type": "inject",
        "z": "fcall-flow",
        "name": "key: demo:fcall",
        "props": [
            { "p": "payload" }
        ],
        "repeat": "",
        "once": false,
        "topic": "",
        "payload": "[\"demo:fcall\"]",
        "payloadType": "json",
        "x": 180,
        "y": 160,
        "wires": [["fcall-node"]]
    },
    {
        "id": "fcall-node",
        "type": "redis-lua-script",
        "z": "fcall-flow",
        "server": "fcall-config",
        "name": "FCALL getval",
        "mode": "function",
        "readonly": false,
        "keyval": 1,
        "func": "#!lua name=demolib\nredis.register_function('getval', function(keys, args) return redis.call('GET', keys[1]) end)",
        "fname": "getval",
        "stored": false,
        "block": false,
        "x": 430,
        "y": 160,
        "wires": [["fcall-debug"]]
    },
    {
        "id": "fcall-debug",
        "type": "debug",
        "z": "fcall-flow",
        "name": "result",
        "active": true,
        "tosidebar": true,
        "complete": "payload",
        "targetType": "msg",
        "x": 650,
        "y": 160,
        "wires": []
    },
    {
        "id": "fcall-config",
        "type": "redis-config",
        "name": "localhost",
        "options": "{\"host\":\"127.0.0.1\",\"port\":6379}",
        "optionsType": "json",
        "cluster": false
    }
]
```

- [ ] **Step 2: Validate the JSON parses**

Run: `node -e "require('./examples/redis-fcall.json'); console.log('ok')"`
Expected: prints `ok`.

- [ ] **Step 3: Commit**

```bash
git add examples/redis-fcall.json
git commit -m "docs(examples): add Redis Functions FCALL example flow"
```

---

## Task 8: Playwright — library Save metadata + Open round-trip

**Files:**
- Modify: `test/playwright/redis-editor.spec.js`

> These tests need the Playwright deployment: `npm run test:playwright`. The existing test `"redis-lua-script library save uses real lookup menu and checkbox metadata"` is the template for the Save side; reuse its helpers (`startNodeRed`, `noauthOptions`, `openEditor`, `setInputValue`, `setCodeEditorValue`, `waitForLibrarySaveFolder`, and the `fs`/`path` imports already at the top of the file).

- [ ] **Step 1: Extend the existing Save test to cover Function-mode metadata**

In the test `"redis-lua-script library save uses real lookup menu and checkbox metadata"`, after the existing line that sets the code editor value:

```javascript
    await setCodeEditorValue(page, "node-input-func-editor", "return redis.call('PING')");
```

add the Function-mode field changes:

```javascript
    await page.selectOption("#node-input-mode", "function");
    await expect(page.locator("#redis-lua-fname-row")).toBeVisible();
    await expect(page.locator("#redis-lua-stored-row")).toBeHidden();
    await setInputValue(page, "#node-input-fname", "demofn");
    await page.locator("#node-input-readonly").setChecked(true);
```

Then, in the same test, after the existing saved-file assertions:

```javascript
    expect(saved).toContain("// stored: true");
    expect(saved).toContain("// block: true");
    expect(saved).toContain("return redis.call('PING')");
```

add:

```javascript
    expect(saved).toContain("// mode: function");
    expect(saved).toContain("// readonly: true");
    expect(saved).toContain("// fname: demofn");
```

- [ ] **Step 2: Add the Open-Library round-trip test**

Add this test next to the save test. It pre-writes a library file with metadata, then loads it through the real **Open Library** menu and asserts the fields and visibility restore.

```javascript
  test("redis-lua-script Open Library restores function-mode fields and visibility", async ({
    page,
  }) => {
    nodeRed = await startNodeRed(noauthOptions());

    // Pre-seed a saved library entry with function-mode metadata.
    const libDir = path.join(nodeRed.userDir, "lib", "functions");
    fs.mkdirSync(libDir, { recursive: true });
    const body =
      "#!lua name=openrtlib\nredis.register_function('openrtfn', function(keys, args) return redis.call('GET', keys[1]) end)";
    fs.writeFileSync(
      path.join(libDir, "open-rt.lua"),
      [
        "// name: open rt",
        "// keyval: 1",
        "// mode: function",
        "// readonly: false",
        "// fname: openrtfn",
        "// stored: false",
        "// block: false",
        "//",
        body,
        "",
      ].join("\n")
    );

    await openEditor(page, nodeRed.url);
    await page.evaluate(() => {
      RED.nodes.import(
        [
          {
            id: "lua-open",
            type: "redis-lua-script",
            z: "flow1",
            server: "redis-config-1",
            name: "",
            keyval: 0,
            func: "\nreturn nil",
            stored: false,
            block: false,
            mode: "script",
            readonly: false,
            fname: "",
            wires: [[]],
            x: 250,
            y: 200,
          },
        ],
        { markChanged: true }
      );
      RED.view.redraw(true);
      RED.editor.edit(RED.nodes.node("lua-open"));
    });

    await page.waitForSelector("#node-input-func-editor", { state: "visible" });
    // Open Library menu -> select the pre-seeded entry -> load it.
    await page.locator("#node-input-lua-lookup").click();
    await page.locator("#node-input-lua-menu-open-library").click();
    await expect(page.locator("#red-ui-library-dialog")).toBeVisible();
    await page.locator(".red-ui-treeList-label:has-text('open rt')").first().click();
    await page.locator("#red-ui-library-dialog").getByText("Load", { exact: true }).click();

    // Round-trip assertions.
    await expect(page.locator("#node-input-mode")).toHaveValue("function");
    await expect(page.locator("#node-input-fname")).toHaveValue("openrtfn");
    await expect(page.locator("#node-input-keyval")).toHaveValue("1");
    await expect(page.locator("#redis-lua-fname-row")).toBeVisible();
    await expect(page.locator("#redis-lua-stored-row")).toBeHidden();
  });
```

- [ ] **Step 3: Run the Playwright suite and adjust selectors if needed**

Run: `npm run test:playwright`
Expected: both Lua tests PASS. If the library dialog tree/Load-button selectors differ in this Node-RED build, adjust the `#node-input-lua-menu-open-library`, `.red-ui-treeList-label`, and Load-button locators (inspect the live dialog), then re-run. The post-load assertions (`#node-input-mode` value, visibility) are the contract and must not change.

- [ ] **Step 4: Commit**

```bash
git add test/playwright/redis-editor.spec.js
git commit -m "test(playwright): function-mode library save metadata and Open round-trip"
```

---

## Task 9: Deployment specs — FCALL + read-only across Cluster, Sentinel, MemoryDB

**Files:**
- Modify: `test/redis_cluster_deployment_spec.js`, `test/redis_sentinel_deployment_spec.js`, `test/memorydb_deployment_spec.js`

> These run only inside their own deployment stage of `npm test`. They use the file-local helpers (`clusterConfigNode`/`directCluster`, `sentinelConfigNode`, `memoryDbConfigNode`/`directMemoryDb`) and the shared `commandNode`/`helperNode`/`invoke`/`load` from `test/helpers/topology.js`. Keys must be hash-tagged (`{tag}`) so same-slot routing works on sharded deployments.

- [ ] **Step 1: Cluster — add a function-mode + read-only test**

In `test/redis_cluster_deployment_spec.js`, add a `waitForLibName` helper next to the existing `waitForLuaSha`:

```javascript
function waitForLibName(node) {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const tick = () => {
      if (node.libname) {
        resolve();
      } else if (Date.now() - started > 5000) {
        reject(new Error("function library was not loaded"));
      } else {
        setTimeout(tick, 25);
      }
    };
    tick();
  });
}
```

Then add this `it` inside the `describeCluster(...)` block, after the existing `"runs same-slot Lua scripts and falls back after SCRIPT FLUSH"` test:

```javascript
  it("runs same-slot FCALL and read-only EVAL through the function/RO models", async function () {
    const lib = [
      "#!lua name=clusterlib",
      "redis.register_function('clusterfn', function(keys, args) redis.call('SET', keys[1], args[1]); return redis.call('GET', keys[1]) end)",
    ].join("\n");
    const flow = [
      clusterConfigNode(),
      {
        id: "fn-node",
        type: "redis-lua-script",
        server: "config1",
        name: "cluster-fn",
        mode: "function",
        readonly: false,
        keyval: 1,
        func: lib,
        fname: "clusterfn",
        block: false,
        wires: [["fn-helper"]],
      },
      helperNode("fn"),
      {
        id: "ro-node",
        type: "redis-lua-script",
        server: "config1",
        name: "cluster-ro",
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
    await waitForLibName(fnNode);
    (
      await invoke(helper, "fn", { payload: ["test:cluster:{lua}:fn", "fn-value"] })
    ).should.equal("fn-value");

    const cluster = directCluster();
    try {
      await cluster.set("test:cluster:{lua}:ro", "ro-value");
    } finally {
      cluster.disconnect();
    }
    (
      await invoke(helper, "ro", { payload: ["test:cluster:{lua}:ro"] })
    ).should.equal("ro-value");
  });
```

- [ ] **Step 2: Sentinel — add the same coverage against the discovered primary**

In `test/redis_sentinel_deployment_spec.js`, add the same `waitForLibName` helper (copy the block from Step 1), then add this `it` inside the sentinel `describe`, after the existing `"runs command and simple Lua coverage through Sentinel discovery"` test:

```javascript
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
    await waitForLibName(fnNode);
    (
      await invoke(helper, "fn", { payload: ["test:sentinel:fn", "fn-value"] })
    ).should.equal("fn-value");
    (
      await invoke(helper, "fn", { payload: ["test:sentinel:fn", "fn-value"] })
    ).should.equal("fn-value");
    // Seed and read back read-only.
    await invoke(helper, "fn", { payload: ["test:sentinel:ro", "ro-value"] });
    (
      await invoke(helper, "ro", { payload: ["test:sentinel:ro"] })
    ).should.equal("ro-value");
  });
```

> Note: the on-`ready` `FUNCTION LOAD REPLACE` is the same code path that re-runs after a Sentinel failover/reconnect (same hook as stored scripts), so reconnect reload is covered by this loader without a separate failover test.

- [ ] **Step 3: MemoryDB — gate FCALL on engine support; always run read-only EVAL**

In `test/memorydb_deployment_spec.js`, add the `waitForLibName` helper (copy from Step 1) and a capability probe, then add a gated test. Add this helper near the other `directMemoryDb`-based helpers:

```javascript
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
```

Then add this `it` inside the `describeMemoryDb(...)` block:

```javascript
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
    } else {
      this.skip();
    }

    await load(helper, redisNode, flow);

    const client = directMemoryDb();
    try {
      await client.set("test:memorydb:{lua}:ro", "ro-value");
    } finally {
      client.disconnect();
    }
    (
      await invoke(helper, "ro", { payload: ["test:memorydb:{lua}:ro"] })
    ).should.equal("ro-value");

    const fnNode = helper.getNode("fn-node");
    await waitForLibName(fnNode);
    (
      await invoke(helper, "fn", { payload: ["test:memorydb:{lua}:fn", "fn-value"] })
    ).should.equal("fn-value");
  });
```

> The keys `test:memorydb:{lua}:ro` / `:fn` reuse the `{lua}` hash tag already cleaned by `cleanupMemoryDbKeys`. If `cleanupMemoryDbKeys`'s explicit key list does not already include them, add `"test:memorydb:{lua}:ro"` and `"test:memorydb:{lua}:fn"` to that list.

- [ ] **Step 4 (optional): widen the cluster-prone matrix**

Optionally add `["eval_ro", "EVAL_RO"]` and `["evalsha_ro", "EVALSHA_RO"]` to the `COMMANDS` array in `test/helpers/cluster-prone.js` so the `redis-command` read-only variants get the same Cluster/Sentinel/MemoryDB sweep as `eval`/`evalsha`. Only do this if `runClusterProneSuccessCases` already knows how to supply numkeys/keys for `eval`/`evalsha` (read it first); otherwise skip — the dedicated tests above are the required coverage. `FCALL` is **not** added to the generic matrix (it needs a pre-loaded library + function name).

- [ ] **Step 5: Commit**

```bash
git add test/redis_cluster_deployment_spec.js test/redis_sentinel_deployment_spec.js test/memorydb_deployment_spec.js test/helpers/cluster-prone.js
git commit -m "test(deployments): FCALL and read-only Lua coverage for cluster, sentinel, memorydb"
```

---

## Task 10: Full suite + agent-doc cross-check

**Files:**
- Modify (if coverage notes changed): `docs/TESTING.md`, `docs/REFERENCE_MAP.md`, `.claude/skills/node-red-contrib-redis-maintainer/SKILL.md`

- [ ] **Step 1: Run the full Docker matrix**

Run: `npm test`
Expected: `single-noauth`, `single-auth`, `cluster-auth`, `sentinel-auth` stages PASS (MemoryDB stays skipped unless `MEMORYDB_*` are set). The new scripting tests run in both standalone stages; the new deployment tests run in their stages.

- [ ] **Step 2: Update agent-facing docs to match new behavior**

In `docs/TESTING.md`, in the `scripting_commands_spec.js` description and the cluster/sentinel/memorydb topology bullets, note the added `redis-lua-script` read-only and FCALL coverage. In `docs/REFERENCE_MAP.md`, update any `redis-lua-script` capability summary to mention Script/Function modes and read-only variants. In `.claude/skills/node-red-contrib-redis-maintainer/SKILL.md`, update the Lua node description / caution areas to mention `mode`/`readonly`/`fname` and the function loader. Keep edits factual and concise.

- [ ] **Step 3: Self-review the diff**

Run: `git diff master --stat` and skim the full diff. Confirm: no public node type / field renames; `defaults` are additive; the example JSON parses; help text matches runtime behavior.

- [ ] **Step 4: Commit**

```bash
git add docs/TESTING.md docs/REFERENCE_MAP.md .claude/skills/node-red-contrib-redis-maintainer/SKILL.md
git commit -m "docs: record lua read-only/function-mode behavior and test coverage"
```

---

## Self-Review (completed during authoring)

**Spec coverage:**
- Read-only execution (`EVAL_RO`/`EVALSHA_RO`, incl. `NOSCRIPT`→`EVAL_RO`) → Task 3.
- Function mode (`FUNCTION LOAD REPLACE` on ready, `FCALL`/`FCALL_RO`, "function not found"→reload→retry, empty-source misconfiguration) → Task 3.
- Cluster-aware loading (load on all masters) → Task 3 (`loadLibraryOn`); validated in Task 9.
- Editor fields + visibility + library round-trip wrinkle → Tasks 1, 2; Open/Save coverage in Tasks 1 and 8.
- Management-stays-in-`redis-command` in help + NODE_GUIDE + README (both node sides) → Tasks 4, 5, 6.
- All four deployments → standalone (Task 3, auto in both stages), cluster/sentinel/memorydb (Task 9), MemoryDB function gating (Task 9 Step 3).
- Example flow → Task 7.

**Refinement vs. spec:** the spec described `mode` as a "plain string field"; the plan makes `mode` an **object** library field with a `set` that re-applies visibility — this is required by the spec's own "library-open visibility wrinkle". `fname` stays a string field; `readonly` is an object field. This is the consistent reading of the spec.

**Placeholder scan:** none — every code/step is concrete. The one judgment call (Playwright library-dialog selectors in Task 8 Step 3) ships concrete selectors plus an explicit run-and-adjust instruction, with fixed post-load assertions.

**Type/name consistency:** `node.mode`/`node.readonly`/`node.fname`/`node.libname`/`node.sha1`/`node.command`, `loadLibraryOn`, `updateLuaModeVisibility`, the DOM ids (`node-input-mode`/`-readonly`/`-fname`, `redis-lua-fname-row`/`-stored-row`/`-editor-label`), and the test helper `waitForLib`/`waitForLibName` are used identically across tasks.
