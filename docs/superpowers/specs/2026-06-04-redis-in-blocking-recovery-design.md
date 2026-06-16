# redis-in blocking-consumer auto-recovery — design

- **Date:** 2026-06-04
- **Branch:** `claude-review`
- **Status:** Approved design, pending implementation plan
- **Scope:** the two `redis-in` polling loops only. Change #2 of the three reliability/security
  items; #1 (redis-out error propagation) is done, #3 (credentials) is pending.

## Problem

`redis-in`'s two blocking polling loops terminate permanently on the first error:

- generic blocking branch (`redis.js:523-557`, blpop/brpop/bzpopmin/bzpopmax):
  ```js
  } catch (e) {
    node.log(e.message);
    running = false;          // loop exits for good
  }
  ```
- `xreadgroup` (`redis.js:479-521`): on any non-`NOGROUP` error it does
  `node.error(err); running = false;` — same permanent exit.

When the connection drops (Redis restart, failover, network blip), the in-flight blocking
command rejects and the loop exits. ioredis then reconnects the socket underneath, so
`attachStatusListeners` flips the node back to **green "connected"** — but the loop is already
gone. The consumer is **green and dead**: it silently stops delivering messages until the flow
is redeployed. This is the worst failure mode for a queue/stream consumer.

The gap is untested: `test/redis_in_spec.js` has no reconnect/recovery test, and
`test/redis_status_spec.js` only covers steady-state green/red status, not loop survival.

pub/sub (`subscribe`/`psubscribe`) is **not** affected: it registers a `message`/`pmessage`
handler once and ioredis re-subscribes automatically after reconnect.

## Goals

- A blocking consumer survives connection drops: after a transient failure it waits a backoff
  interval and resumes, with no redeploy required.
- The loop ends only on node close (`running = false`).
- A persistent failure (e.g. a misconfigured key giving `WRONGTYPE`) is visible — steady
  "retrying" status plus a rate-limited warning — never silent and never a hot loop.
- Shutdown/redeploy stays clean and prompt.

## Non-goals

- pub/sub recovery (already handled by ioredis re-subscription).
- Error classification / taxonomy. Per decision below, **every** error retries.
- New editor fields or config. Recovery is always on (it fixes a bug).
- Connection-id, sharing, or graceful-vs-forced shutdown changes.

## Decision

**Retry every error with capped exponential backoff; only `running = false` (close) stops the
loop.** Rationale: a consumer should never silently die. There is no error taxonomy to maintain
(message-string matching is brittle across ioredis/Redis versions). A permanent misconfig is
made visible through status + warnings rather than by stopping. Rejected: "stop on command
errors" (needs a fragile taxonomy) and "max-attempt cap" (a long outage exhausts the cap and
reintroduces the silent stall).

Retries log through `node.warn` (not `node.error`): a recovering source node should not spam
Catch nodes on every blip, and there is no `msg` to attach. The backoff rate-limits warnings to
at most one per cap interval.

## Implementation

### New module-level helpers

```js
const BLOCKING_RETRY_BASE_MS = 250;
const BLOCKING_RETRY_CAP_MS = 5000;

// Equal-jitter capped exponential backoff. Never returns 0, so a tight failure
// (e.g. immediate connection refusal) cannot become a busy loop.
function blockingBackoffDelay(attempt) {
  const ceil = Math.min(BLOCKING_RETRY_CAP_MS, BLOCKING_RETRY_BASE_MS * Math.pow(2, attempt));
  const half = ceil / 2;
  return Math.floor(half + Math.random() * half);
}

// Interruptible sleep: the node's close handler can cancel a pending backoff so a
// redeploy does not wait out the delay or fire a retry on a torn-down client.
function blockingSleep(ms, node) {
  return new Promise(function (resolve) {
    var done = function () { node._blockingRetryCancel = null; resolve(); };
    var timer = setTimeout(done, ms);
    node._blockingRetryCancel = function () { clearTimeout(timer); done(); };
  });
}
```

### Generic blocking branch (`redis.js:523-557`)

- declare `let attempt = 0;` before the `while (running)` loop
- reset `attempt = 0;` immediately after a command resolves (before processing `data`)
- replace the catch body:

```js
} catch (e) {
  if (!running) break;                                   // shutdown — do not retry
  attempt += 1;
  node.warn("redis-in " + node.command + " error, retrying: " + e.message);
  node.status({ fill: "yellow", shape: "ring", text: "retrying" });
  await blockingSleep(blockingBackoffDelay(attempt), node);
}
```

### `xreadgroup` branch (`redis.js:479-521`)

- declare `let attempt = 0;` before the loop; reset `attempt = 0;` after a successful
  `xreadgroup` resolves
- keep the existing `if (!running) return;` guard at the top of the catch
- keep the **NOGROUP-specific helpful warning** (group not found — create it), but route it
  through the same backoff instead of a fixed 2s sleep; all other errors get a generic warning.
  Both increment `attempt`, set the "retrying" status, and `await blockingSleep(...)`.

### Close handler (`redis.js:428-438`)

Add, before/around the existing `running = false` + forced disconnect, a cancel of any pending
backoff so the sleeping loop wakes immediately and exits on the `while (running)` check:

```js
if (node._blockingRetryCancel) { node._blockingRetryCancel(); }
```

## Invariants preserved

- **Shutdown still works via `running = false` + forced disconnect.** The catch's
  `if (!running) break/return` prevents a retry during close, and the backoff cancel wakes a
  sleeping loop so redeploys stay prompt. No command is issued on a torn-down client.
- **pub/sub branches are untouched.**
- **`attachStatusListeners` still owns connection status.** The loop's transient "retrying"
  status coexists with client `reconnecting`/`ready` events; on recovery the "ready" handler
  restores green.
- **The unreachable-server red status still appears** (`redis_status_spec.js` blpop bad-config
  test): the client `error` event still drives red regardless of the loop retrying underneath.
- **Write ordering / payload shaping unchanged** — only the catch paths and a per-iteration
  `attempt` counter change.

## Testing (TDD)

Deterministic via the existing `Redis.prototype` stub pattern (`redis_status_spec.js:100`
already stubs `Redis.prototype.quit`). Add to `test/redis_in_spec.js`.

1. **blpop recovery (failing-first):** stub `Redis.prototype.blpop` to reject once with a
   connection-style `Error("Connection is closed.")`, then delegate to the original. RPUSH a
   value. Assert the node still emits the value (the loop survived the error). Restore the stub
   in a `finally`. Fails today (loop exits after the first error).
2. **xreadgroup recovery:** same shape, stubbing `Redis.prototype.xreadgroup` to reject once
   then delegate; assert a subsequently-added stream entry is still delivered.
3. **backoff reset:** induce a second failure after a success and assert continued delivery
   (proves `attempt` resets and the loop keeps going).
4. **clean shutdown during retry:** with `blpop` stubbed to always reject (loop stuck retrying),
   load the node, then `helper.unload()`; assert no error after close and the process settles
   (cancel path exercised).
5. Full `test/redis_in_spec.js` and `test/redis_status_spec.js` stay green, including the
   unreachable-server red-status test.

## Documentation (Definition of Done)

- `redis.html` `redis-in` help (`data-help-name="redis-in"`):
  - update the XREADGROUP note that currently says it "retries automatically every 2 seconds"
    to describe capped backoff and that it applies to all transient errors.
  - add a short line that blocking inputs auto-recover from connection drops with backoff and
    stop only on flow stop/redeploy.
- `docs/NODE_GUIDE.md` `redis-in` section: add a "Recovery" note alongside "Shutdown rules".
- `docs/ARCHITECTURE.md` `redis-in` summary: one line that the blocking loops retry with
  backoff and end only on close.

## Versioning

Bug fix. Package version stays `2.0.0`. Note in release notes that blocking `redis-in`
consumers now auto-recover (previously they stopped silently after a connection drop) and that
the `xreadgroup` group-missing retry interval changed from a fixed 2s to capped backoff.
