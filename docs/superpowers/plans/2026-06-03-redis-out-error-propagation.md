# redis-out Error Propagation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `redis-out` await its Redis write so failures reach `done(err)`/Catch nodes and never become unhandled promise rejections.

**Architecture:** Convert the single `redis-out` `input` handler to `async`, `await` the one Redis command it issues, and let the existing `try/catch` route every failure to `done(err)`. The command is still issued before `await` suspends, so write order is unchanged; `await` only defers `done()` until the ack.

**Tech Stack:** Node-RED 4.1.x runtime node, ioredis 5.x, Mocha + `node-red-node-test-helper` + should.js, Docker-managed Redis (`redis:8.8-alpine`).

**Spec:** `docs/superpowers/specs/2026-06-03-redis-out-error-propagation-design.md`

**Branch:** work only on `claude-review`.

---

## File Structure

- `redis.js` — modify the `RedisOut` `input` handler (currently `redis.js:582-627`). One responsibility: turn the fire-and-forget write into an awaited write with error routing. No other function changes.
- `test/redis_out_spec.js` — add one regression test in the existing "error handling" section (after `redis_out_spec.js:554`, before the `describe` closes at line 555).
- `redis.html` — one sentence in the `redis-out` help block (`data-help-name="redis-out"`, intro near line 1486).
- `docs/NODE_GUIDE.md` — add an "Error handling" block to the `## redis-out` section.
- `docs/ARCHITECTURE.md` — add one line to the `### redis-out` summary.

---

## Task 0: Start a throwaway dev Redis for the red/green loop

**Files:** none (environment only)

- [ ] **Step 1: Start a standalone Redis matching the standalone test default**

Run:
```bash
sudo -n docker run -d --rm --name redis-dev -p 127.0.0.1:6379:6379 \
  redis:8.8-alpine redis-server --save "" --appendonly no
```
Expected: prints a container id. (`test/helpers/deployment.js` defaults to `127.0.0.1:6379`, no auth, so no env vars are needed for targeted mocha.)

- [ ] **Step 2: Confirm it answers**

Run:
```bash
sudo -n docker exec redis-dev redis-cli ping
```
Expected: `PONG`

> Note: this dev Redis occupies port 6379. It MUST be stopped (Task 4, Step 1) before any commit that triggers the husky hook, because `npm test` starts its own Redis on 6379 and TESTING.md forbids a second Redis on the test ports.

---

## Task 1: RED — add the failing async-write-error test

**Files:**
- Test: `test/redis_out_spec.js` (insert after line 554, before the closing `});` of the `describe` block on line 555)

- [ ] **Step 1: Write the failing test**

Insert this `it(...)` block immediately after the existing `zadd — calls node.error when payload is a plain string` test (after line 554):

```javascript
    it("rpush — calls node.error when the Redis write fails (WRONGTYPE)", function (done) {
        helper.load(redisNode, makeOutFlow("rpush", "test:out:err:wrongtype", false), function () {
            const out = helper.getNode("out");
            const c = direct();

            let unhandled = null;
            const onUnhandled = (reason) => { unhandled = reason; };
            process.on("unhandledRejection", onUnhandled);

            // Seed a STRING at the key so RPUSH fails with WRONGTYPE.
            c.set("test:out:err:wrongtype", "i-am-a-string")
                .then(() => {
                    out.receive({ payload: "item" });
                    setTimeout(() => {
                        process.removeListener("unhandledRejection", onUnhandled);
                        c.disconnect();
                        try {
                            out.error.callCount.should.be.above(0);
                            String(out.error.firstCall.args[0]).should.match(/WRONGTYPE/);
                            (unhandled === null).should.be.true();
                            done();
                        } catch (e) { done(e); }
                    }, 200);
                })
                .catch((e) => {
                    process.removeListener("unhandledRejection", onUnhandled);
                    c.disconnect();
                    done(e);
                });
        });
    });
```

Why this proves the bug: today the write is fire-and-forget, so `out.error` is never called (callCount 0) and the WRONGTYPE rejection is unhandled. The first assertion fails cleanly pre-fix.

- [ ] **Step 2: Run the test and verify it FAILS**

Run:
```bash
npm run test:mocha -- test/redis_out_spec.js --grep "WRONGTYPE"
```
Expected: 1 failing — `expected 0 to be above 0` (the `out.error.callCount` assertion). You may also see an `UnhandledPromiseRejection` WRONGTYPE warning printed by Node; that is the bug.

- [ ] **Step 3: Commit the failing test (skip the hook — the matrix would fail on the intentional RED)**

```bash
git add test/redis_out_spec.js
git commit --no-verify -m "Add failing test: redis-out swallows async write errors

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: GREEN — await the write in the redis-out handler

**Files:**
- Modify: `redis.js:582-627` (the `RedisOut` `node.on("input", ...)` handler)

- [ ] **Step 1: Make the handler async and await each command (minimal diff)**

Replace the entire current handler:

```javascript
    node.on("input", function (msg, send, done) {
      var topic;
      send = send || function() { node.send.apply(node,arguments) }
      done = done || function(err) { if(err)node.error(err, msg); }
      if (msg.topic !== undefined && msg.topic !== "") {
        topic = msg.topic;
      } else {
        topic = node.topic;
      }
      if (topic === "") {
        done(new Error("Missing topic, please send topic on msg or set Topic on node."));
      } else {
        try {
          if (node.command === 'xadd') {
            let fields;
            const p = msg.payload;
            if (p && typeof p === 'object' && !Array.isArray(p)) {
              fields = Object.entries(p).reduce((acc, pair) => acc.concat(pair), []);
            } else if (Array.isArray(p)) {
              fields = p;
            } else {
              fields = ['value', p != null ? String(p) : ''];
            }
            client.xadd(topic, '*', ...fields);
          } else if (node.command === 'zadd') {
            const p = msg.payload;
            if (p && typeof p === 'object' && !Array.isArray(p) && 'score' in p) {
              const member = node.obj ? JSON.stringify(p.member) : String(p.member);
              client.zadd(topic, p.score, member);
            } else if (Array.isArray(p)) {
              client.zadd(topic, ...p);
            } else {
              done(new Error("zadd requires payload {score, member} or [score, member, ...]"));
              return;
            }
          } else if (node.obj) {
            client[node.command](topic, JSON.stringify(msg.payload));
          } else {
            client[node.command](topic, msg.payload);
          }
          done();
        } catch (err) {
          done(err);
        }
      }
    });
```

with this version — identical except `function` → `async function` and an `await` before each of the five command calls:

```javascript
    node.on("input", async function (msg, send, done) {
      var topic;
      send = send || function() { node.send.apply(node,arguments) }
      done = done || function(err) { if(err)node.error(err, msg); }
      if (msg.topic !== undefined && msg.topic !== "") {
        topic = msg.topic;
      } else {
        topic = node.topic;
      }
      if (topic === "") {
        done(new Error("Missing topic, please send topic on msg or set Topic on node."));
      } else {
        try {
          if (node.command === 'xadd') {
            let fields;
            const p = msg.payload;
            if (p && typeof p === 'object' && !Array.isArray(p)) {
              fields = Object.entries(p).reduce((acc, pair) => acc.concat(pair), []);
            } else if (Array.isArray(p)) {
              fields = p;
            } else {
              fields = ['value', p != null ? String(p) : ''];
            }
            await client.xadd(topic, '*', ...fields);
          } else if (node.command === 'zadd') {
            const p = msg.payload;
            if (p && typeof p === 'object' && !Array.isArray(p) && 'score' in p) {
              const member = node.obj ? JSON.stringify(p.member) : String(p.member);
              await client.zadd(topic, p.score, member);
            } else if (Array.isArray(p)) {
              await client.zadd(topic, ...p);
            } else {
              done(new Error("zadd requires payload {score, member} or [score, member, ...]"));
              return;
            }
          } else if (node.obj) {
            await client[node.command](topic, JSON.stringify(msg.payload));
          } else {
            await client[node.command](topic, msg.payload);
          }
          done();
        } catch (err) {
          done(err);
        }
      }
    });
```

- [ ] **Step 2: Run the new test and verify it PASSES**

Run:
```bash
npm run test:mocha -- test/redis_out_spec.js --grep "WRONGTYPE"
```
Expected: `1 passing`, and no UnhandledPromiseRejection warning.

- [ ] **Step 3: Run the whole redis-out spec to confirm no regression**

Run:
```bash
npm run test:mocha -- test/redis_out_spec.js
```
Expected: all redis-out tests pass (the prior happy-path and sync-error tests still green).

(No commit yet — the docs change in Task 3 ships in the same commit as the fix in Task 4, so runtime + tests + docs land together.)

---

## Task 3: Docs — keep help and project docs in sync

**Files:**
- Modify: `redis.html` (`data-help-name="redis-out"` intro, near line 1486)
- Modify: `docs/NODE_GUIDE.md` (`## redis-out` section)
- Modify: `docs/ARCHITECTURE.md` (`### redis-out` summary)

- [ ] **Step 1: Update the redis-out help intro in `redis.html`**

Find:
```html
<p>Receives a message and writes its payload to Redis using the selected command.
The node has no output — it is a terminal node.</p>
```
Replace with:
```html
<p>Receives a message and writes its payload to Redis using the selected command.
The node has no output — it is a terminal node.</p>
<p>The write is awaited: the message completes only after Redis acknowledges it, and any
write failure (for example <code>WRONGTYPE</code> or a lost connection) is reported through
the node&rsquo;s error path, so a <em>catch</em> node can handle it.</p>
```

- [ ] **Step 2: Add an "Error handling" block to `docs/NODE_GUIDE.md`**

Find (in the `## redis-out` section):
```markdown
- list push operations accept plain or JSON-stringified payloads depending on `obj`

Read before editing:
```
Replace with:
```markdown
- list push operations accept plain or JSON-stringified payloads depending on `obj`

Error handling:

- the write is awaited; `done()` resolves only after Redis acknowledges it
- a failed write calls `done(err)`, so the message reaches a `catch` node and is marked errored
- no write is fire-and-forget, so a failure cannot become an unhandled promise rejection

Read before editing:
```

- [ ] **Step 3: Add one line to `docs/ARCHITECTURE.md`**

Find:
```markdown
Implements focused write operations with custom payload shaping for selected commands such as:
- list pushes
- stream add
- sorted-set add
```
Replace with:
```markdown
Implements focused write operations with custom payload shaping for selected commands such as:
- list pushes
- stream add
- sorted-set add

The write is awaited and any failure is routed to `done(err)` so Catch nodes fire; no write is fire-and-forget.
```

---

## Task 4: Verify the full matrix and commit the fix + docs

**Files:** none new (commits Tasks 2 + 3 changes)

- [ ] **Step 1: Stop the dev Redis so it does not clash with the test matrix**

Run:
```bash
sudo -n docker stop redis-dev
```
Expected: prints `redis-dev`. (The `--rm` flag removes the container on stop.)

- [ ] **Step 2: Stage the fix and docs, then commit (the husky hook runs the full `npm test` matrix as verification)**

```bash
git add redis.js redis.html docs/NODE_GUIDE.md docs/ARCHITECTURE.md
git commit -m "Fix redis-out: await writes and propagate errors to done()

redis-out issued its write fire-and-forget and called done() synchronously,
so failures (WRONGTYPE, OOM, CROSSSLOT, connection drops) became unhandled
promise rejections and never reached Catch nodes. Make the input handler
async and await the write; the existing try/catch now routes every failure
to done(err). Write order is unchanged (the command is issued before await
suspends). Docs and help updated to match.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```
Expected: the pre-commit hook runs `scripts/run-deployment-tests.js`, which brings up single-noauth, single-auth, cluster-auth, and sentinel-auth deployments. All stages pass (single-noauth and single-auth now include the new WRONGTYPE test). The commit completes.

- [ ] **Step 3 (optional): Run the MemoryDB stage too**

Only if MemoryDB coverage is wanted (requires the `MEMORYDB_*` env vars). Run:
```bash
MEMORYDB_ENABLED=1 \
MEMORYDB_ENDPOINT="<endpoint>" MEMORYDB_PORT="6379" \
MEMORYDB_USERNAME="<user>" MEMORYDB_PASSWORD="<password>" \
npm test
```
Expected: all five stages green, including AWS MemoryDB. Do not commit MemoryDB endpoints or credentials.

---

## Done criteria

- New `WRONGTYPE` test fails before the `redis.js` change and passes after.
- Full `npm test` matrix is green.
- `redis-out` write failures reach Catch nodes; no unhandled rejection.
- Help text, `NODE_GUIDE.md`, and `ARCHITECTURE.md` describe the awaited-write/error behavior.
- Package version stays `2.0.0`.
