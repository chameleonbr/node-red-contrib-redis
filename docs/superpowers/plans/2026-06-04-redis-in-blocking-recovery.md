# redis-in Blocking-Consumer Auto-Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `redis-in`'s two blocking polling loops survive connection drops by retrying with capped backoff instead of stopping silently.

**Architecture:** Add two small module-level helpers (`blockingBackoffDelay`, `blockingSleep`) and change only the two loops' `catch` blocks to warn + show a "retrying" status + back off + continue while `running`. The close handler cancels any pending backoff. pub/sub is untouched (ioredis auto-resubscribes).

**Tech Stack:** Node-RED 4.1.x runtime node, ioredis 5.x, Mocha + `node-red-node-test-helper` + should.js, Docker-managed Redis (`redis:8.8-alpine`).

**Spec:** `docs/superpowers/specs/2026-06-04-redis-in-blocking-recovery-design.md`

**Branch:** work only on `claude-review`.

---

## File Structure

- `redis.js` — add the two helpers near the existing module-level constants; modify the `xreadgroup` loop (`redis.js:479-521`), the generic blocking loop (`redis.js:523-557`), and the `RedisIn` close handler (`redis.js:428-438`). No connection/sharing/shutdown-model changes.
- `test/redis_in_spec.js` — add `const Redis = require("ioredis");` and three tests at the end of the `describe("redis-in node", ...)` block.
- `redis.html` — `redis-in` help: update the XREADGROUP "every 2 seconds" line and add one recovery sentence.
- `docs/NODE_GUIDE.md` — `redis-in` section: add a "Recovery" block.
- `docs/ARCHITECTURE.md` — `redis-in` summary: add one line.

---

## Task 0: Start a throwaway dev Redis for the red/green loop

**Files:** none (environment only)

- [ ] **Step 1: Start standalone Redis matching the test default**

Run:
```bash
sudo -n docker run -d --rm --name redis-dev -p 127.0.0.1:6379:6379 \
  redis:8.8-alpine redis-server --save "" --appendonly no
```
Expected: prints a container id.

- [ ] **Step 2: Confirm it answers**

Run:
```bash
sudo -n docker exec redis-dev redis-cli ping
```
Expected: `PONG`

> This dev Redis occupies port 6379. Stop it (Task 4, Step 1) before any commit that triggers the husky hook — `npm test` starts its own Redis on 6379.

---

## Task 1: RED — add recovery tests

**Files:**
- Modify: `test/redis_in_spec.js` (add ioredis import; append three tests before the final `});` of the `describe` block)

- [ ] **Step 1: Add the ioredis import**

Find (top of file):
```javascript
const { directRedis, redisConfigNode } = require("./helpers/deployment");
```
Replace with:
```javascript
const { directRedis, redisConfigNode } = require("./helpers/deployment");
const Redis = require("ioredis");
```

- [ ] **Step 2: Append the three tests**

Find the end of the last existing test and the `describe` close:
```javascript
            setTimeout(() => c.zadd("test:in:bzpopmax:float", 3.14, "pi-task"), 150);
        });
    });
});
```
Replace with:
```javascript
            setTimeout(() => c.zadd("test:in:bzpopmax:float", 3.14, "pi-task"), 150);
        });
    });

    // ── auto-recovery ────────────────────────────────────────────────────────

    it("blpop — recovers and keeps consuming after a transient connection error", function (done) {
        const originalBlpop = Redis.prototype.blpop;
        // Reject the first blpop (simulate a dropped connection), then restore the
        // real implementation for every subsequent call.
        Redis.prototype.blpop = function () {
            Redis.prototype.blpop = originalBlpop;
            return Promise.reject(new Error("Connection is closed."));
        };

        helper.load(redisNode, makeInFlow("blpop", "test:in:recover:blpop", false), function () {
            const h = helper.getNode("h");
            const c = direct();
            const giveUp = setTimeout(function () {
                Redis.prototype.blpop = originalBlpop;
                c.disconnect();
                done(new Error("no message received — blocking loop did not recover from the error"));
            }, 4000);

            h.on("input", function (msg) {
                clearTimeout(giveUp);
                Redis.prototype.blpop = originalBlpop;
                c.disconnect();
                try {
                    msg.payload.should.equal("after-recovery");
                    done();
                } catch (e) { done(e); }
            });

            setTimeout(() => c.rpush("test:in:recover:blpop", "after-recovery"), 400);
        });
    });

    it("xreadgroup — recovers and keeps consuming after a transient connection error", function (done) {
        const STREAM = "testinxrgrecover";
        const GROUP = "grprecover";
        const c = direct();
        const originalXreadgroup = Redis.prototype.xreadgroup;
        Redis.prototype.xreadgroup = function () {
            Redis.prototype.xreadgroup = originalXreadgroup;
            return Promise.reject(new Error("Connection is closed."));
        };

        c.xgroup("CREATE", STREAM, GROUP, "0", "MKSTREAM")
            .then(() => c.xadd(STREAM, "*", "k", "v"))
            .then(() => {
                helper.load(
                    redisNode,
                    makeInFlow("xreadgroup", `${STREAM}:>`, true, {
                        timeout: 0,
                        groupname: GROUP,
                        consumername: "consumer-1",
                    }),
                    function () {
                        const h = helper.getNode("h");
                        const giveUp = setTimeout(function () {
                            Redis.prototype.xreadgroup = originalXreadgroup;
                            c.disconnect();
                            done(new Error("no message received — xreadgroup loop did not recover"));
                        }, 4000);

                        h.on("input", function (msg) {
                            clearTimeout(giveUp);
                            Redis.prototype.xreadgroup = originalXreadgroup;
                            c.disconnect();
                            try {
                                msg.payload.k.should.equal("v");
                                done();
                            } catch (e) { done(e); }
                        });
                    }
                );
            })
            .catch((e) => {
                Redis.prototype.xreadgroup = originalXreadgroup;
                c.disconnect();
                done(e);
            });
    });

    it("blpop — stops cleanly when the node closes during a retry backoff", function (done) {
        const originalBlpop = Redis.prototype.blpop;
        // Always reject so the loop stays in the retry/backoff cycle.
        Redis.prototype.blpop = function () {
            return Promise.reject(new Error("Connection is closed."));
        };

        helper.load(redisNode, makeInFlow("blpop", "test:in:recover:close", false), function () {
            // Let the loop fail at least once and enter a backoff wait, then unload.
            setTimeout(function () {
                helper.unload()
                    .then(function () { Redis.prototype.blpop = originalBlpop; done(); })
                    .catch(function (e) { Redis.prototype.blpop = originalBlpop; done(e); });
            }, 300);
        });
    });
});
```

- [ ] **Step 3: Run the recovery tests and verify they FAIL**

Run:
```bash
npm run test:mocha -- test/redis_in_spec.js --grep "transient connection error"
```
Expected: 2 failing, each `no message received — ... did not recover` (~4s each). Today the loop sets `running=false` after the first rejection, so it never retries.

(The "stops cleanly" test passes today too — it's a regression guard for the new cancel path, added in the same commit.)

- [ ] **Step 4: Commit the failing tests (skip the hook — the matrix would fail on the intentional RED)**

```bash
git add test/redis_in_spec.js
git commit --no-verify -m "Add failing tests: redis-in blocking loops die on transient errors

blpop/xreadgroup loops set running=false on the first error, so a simulated
connection drop stops the consumer and no message is delivered after recovery.
These tests fail until the loops retry with backoff.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: GREEN — add backoff helpers and make the loops retry

**Files:**
- Modify: `redis.js` (helpers near line 8; close handler at 428-438; xreadgroup loop 479-521; generic loop 523-557)

- [ ] **Step 1: Add the backoff helpers after the existing constants**

Find:
```javascript
  const GRACEFUL_QUIT_TIMEOUT_MS = 2000;
  const TEST_CONNECTION_TIMEOUT_MS = 10000;
```
Replace with:
```javascript
  const GRACEFUL_QUIT_TIMEOUT_MS = 2000;
  const TEST_CONNECTION_TIMEOUT_MS = 10000;
  const BLOCKING_RETRY_BASE_MS = 250;
  const BLOCKING_RETRY_CAP_MS = 5000;

  // Equal-jitter capped exponential backoff for supervised blocking-input loops.
  // Never returns 0, so an immediately-rejecting command cannot become a busy loop.
  function blockingBackoffDelay(attempt) {
    const ceil = Math.min(BLOCKING_RETRY_CAP_MS, BLOCKING_RETRY_BASE_MS * Math.pow(2, attempt));
    const half = ceil / 2;
    return Math.floor(half + Math.random() * half);
  }

  // Interruptible backoff sleep. Stores a canceller on the node so the close
  // handler can wake a pending retry immediately on redeploy/shutdown.
  function blockingSleep(ms, node) {
    return new Promise(function (resolve) {
      var finish = function () { node._blockingRetryCancel = null; resolve(); };
      var timer = setTimeout(finish, ms);
      node._blockingRetryCancel = function () { clearTimeout(timer); finish(); };
    });
  }
```

- [ ] **Step 2: Cancel a pending backoff in the close handler**

Find:
```javascript
    node.on("close", async (undeploy, done) => {
      removeListeners();
      node.status({});
      running = false;
```
Replace with:
```javascript
    node.on("close", async (undeploy, done) => {
      removeListeners();
      node.status({});
      running = false;
      if (node._blockingRetryCancel) { node._blockingRetryCancel(); }
```

- [ ] **Step 3: Add backoff retry to the xreadgroup loop**

Find:
```javascript
        const [stream, lastid] = node.topic.split(':');
        (async () => {
            while (running) {
                try {
                    const data = await client.xreadgroup('GROUP', node.groupname, node.consumername, 'BLOCK', 0, 'STREAMS', stream, lastid);
                    if (data) {
```
Replace with:
```javascript
        const [stream, lastid] = node.topic.split(':');
        (async () => {
            let attempt = 0;
            while (running) {
                try {
                    const data = await client.xreadgroup('GROUP', node.groupname, node.consumername, 'BLOCK', 0, 'STREAMS', stream, lastid);
                    attempt = 0;
                    if (data) {
```

Then find:
```javascript
                } catch (err) {
                    if (!running) return;
                    if (err.message && err.message.startsWith('NOGROUP')) {
                        node.warn('Consumer group "' + node.groupname + '" not found on stream "' + stream + '". Retrying in 2s — run the setup step to create it.');
                        await new Promise(resolve => setTimeout(resolve, 2000));
                    } else {
                        node.error(err, { topic: node.topic });
                        running = false;
                    }
                }
```
Replace with:
```javascript
                } catch (err) {
                    if (!running) return;
                    attempt += 1;
                    if (err.message && err.message.startsWith('NOGROUP')) {
                        node.warn('Consumer group "' + node.groupname + '" not found on stream "' + stream + '". Retrying — run the setup step (XGROUP CREATE) to create it.');
                    } else {
                        node.warn('redis-in xreadgroup error, retrying: ' + err.message);
                    }
                    node.status({ fill: "yellow", shape: "ring", text: "retrying" });
                    await blockingSleep(blockingBackoffDelay(attempt), node);
                }
```

- [ ] **Step 4: Add backoff retry to the generic blocking loop**

Find:
```javascript
    else {
      (async () => {
        while (running) {
          try {
            const data = await client[node.command](node.topic, Number(node.timeout));
            if (data !== null && data.length >= 2) {
```
Replace with:
```javascript
    else {
      (async () => {
        let attempt = 0;
        while (running) {
          try {
            const data = await client[node.command](node.topic, Number(node.timeout));
            attempt = 0;
            if (data !== null && data.length >= 2) {
```

Then find:
```javascript
          } catch (e) {
            node.log(e.message);
            running = false;
          }
```
Replace with:
```javascript
          } catch (e) {
            if (!running) break;
            attempt += 1;
            node.warn("redis-in " + node.command + " error, retrying: " + e.message);
            node.status({ fill: "yellow", shape: "ring", text: "retrying" });
            await blockingSleep(blockingBackoffDelay(attempt), node);
          }
```

- [ ] **Step 5: Run the recovery tests and verify they PASS**

Run:
```bash
npm run test:mocha -- test/redis_in_spec.js --grep "transient connection error"
```
Expected: `2 passing`.

- [ ] **Step 6: Run the full redis-in spec to confirm no regression**

Run:
```bash
npm run test:mocha -- test/redis_in_spec.js
```
Expected: all redis-in tests pass, including the three new ones.

- [ ] **Step 7: Run the status spec (blocking status unaffected)**

Run:
```bash
npm run test:mocha -- test/redis_status_spec.js
```
Expected: all pass, including "blpop — shows red/error status when Redis is unreachable" (the client error event still drives red while the loop retries underneath).

(No commit yet — the docs change ships with the fix in Task 4.)

---

## Task 3: Docs — keep help and project docs in sync

**Files:**
- Modify: `redis.html` (`data-help-name="redis-in"`)
- Modify: `docs/NODE_GUIDE.md` (`## redis-in`)
- Modify: `docs/ARCHITECTURE.md` (`### redis-in`)

- [ ] **Step 1: Update the XREADGROUP retry note in `redis.html`**

Find:
```html
<p>Reads new messages from a Redis Stream via a consumer group. Each message is delivered
to exactly one consumer in the group, enabling parallel processing across multiple nodes.
If the consumer group does not exist, the node retries automatically every 2 seconds
while logging a warning &mdash; create the group with <code>XGROUP CREATE</code> first.</p>
```
Replace with:
```html
<p>Reads new messages from a Redis Stream via a consumer group. Each message is delivered
to exactly one consumer in the group, enabling parallel processing across multiple nodes.
If the consumer group does not exist, the node retries automatically with capped backoff
while logging a warning &mdash; create the group with <code>XGROUP CREATE</code> first.</p>
```

- [ ] **Step 2: Add a recovery sentence to the redis-in help intro in `redis.html`**

Find:
```html
<p>Listens to Redis and emits a message for every value received. The node opens a
dedicated connection that stays alive until the flow is stopped or redeployed.</p>
```
Replace with:
```html
<p>Listens to Redis and emits a message for every value received. The node opens a
dedicated connection that stays alive until the flow is stopped or redeployed.</p>
<p>Blocking inputs (BLPOP/BRPOP/BZPOPMIN/BZPOPMAX/XREADGROUP) auto-recover: if a command
fails or the connection drops, the node logs a warning, shows a <em>retrying</em> status,
and retries with capped backoff. The consumer stops only when the flow is stopped or
redeployed.</p>
```

- [ ] **Step 3: Add a "Recovery" block to `docs/NODE_GUIDE.md`**

Find:
```markdown
Shutdown rules:

- always remove listeners
- always clear status
- force disconnect for blocking shutdown
```
Replace with:
```markdown
Recovery:

- blocking loops (blpop/brpop/bzpop/xreadgroup) retry every error with capped backoff
  (250ms→5s, jitter) and keep running; only node close stops them
- retries log via `node.warn` and show a yellow `retrying` status; a persistent failure
  (e.g. WRONGTYPE) stays visible instead of stopping silently
- pub/sub is unaffected — ioredis re-subscribes automatically after reconnect

Shutdown rules:

- always remove listeners
- always clear status
- force disconnect for blocking shutdown
- cancel any pending retry backoff on close
```

- [ ] **Step 4: Add one line to `docs/ARCHITECTURE.md`**

Find:
```markdown
It sends Node-RED messages from Redis events or blocking loops.
```
Replace with:
```markdown
It sends Node-RED messages from Redis events or blocking loops.

The blocking loops (blpop/brpop/bzpop/xreadgroup) retry every error with capped backoff and
end only on node close; the close handler cancels any pending backoff. pub/sub recovery is
handled by ioredis re-subscription.
```

---

## Task 4: Verify the full matrix and commit the fix + docs

**Files:** none new (commits Tasks 2 + 3 changes)

- [ ] **Step 1: Stop the dev Redis so it does not clash with the test matrix**

Run:
```bash
sudo -n docker stop redis-dev
```
Expected: prints `redis-dev`.

- [ ] **Step 2: Stage the fix and docs, then commit (the husky hook runs the full `npm test` matrix as verification)**

```bash
git add redis.js redis.html docs/NODE_GUIDE.md docs/ARCHITECTURE.md
git commit -m "Fix redis-in: blocking consumers auto-recover with backoff

blpop/brpop/bzpop/xreadgroup loops set running=false on any error, so a
transient drop or Redis restart stopped the consumer silently while the
node went back to green. Retry every error with capped backoff (250ms->5s,
jitter), warn + 'retrying' status, and stop only on close. The close handler
cancels a pending backoff so redeploys stay prompt. pub/sub is unchanged.
Help text and docs updated to match.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```
Expected: the pre-commit hook runs `scripts/run-deployment-tests.js`; single-noauth, single-auth, cluster-auth, and sentinel-auth stages all pass (standalone stages now include the three new redis-in tests). The commit completes.

---

## Testing notes

- The two recovery tests are the failing-first proof. Backoff-reset (`attempt = 0` on success)
  is validated indirectly: after the induced failure the loop returns to normal blocking and
  delivers a message — a wedged attempt counter would still deliver but is not separately
  asserted, deliberately, to avoid brittle timing-based tests.
- The "stops cleanly" test exercises the close-during-backoff cancel path; it is a regression
  guard (passes before and after the fix).

## Done criteria

- The two recovery tests fail before the `redis.js` change and pass after.
- Full `npm test` matrix is green.
- A blocking `redis-in` consumer resumes after a connection drop without redeploy; a persistent
  error shows a steady "retrying" status and rate-limited warnings.
- Shutdown/redeploy stays prompt (pending backoff cancelled).
- Help text, `NODE_GUIDE.md`, and `ARCHITECTURE.md` describe the recovery behavior.
- Package version stays `2.0.0`.
