# redis-out error propagation — design

- **Date:** 2026-06-03
- **Branch:** `redis-config-ui-update`
- **Status:** Approved design, pending implementation plan
- **Scope:** `redis-out` runtime node only. One of three independent reliability/security
  changes; the other two (blocking-consumer auto-recovery, credential storage) are out of
  scope here and will get their own spec → plan → implementation cycles.

## Problem

The `redis-out` input handler (`redis.js:582-627`) issues its Redis write **fire-and-forget**
and then calls `done()` synchronously:

```js
client.xadd(topic, "*", ...fields);   // not awaited, no callback, no .catch
// ...
client[node.command](topic, msg.payload);
done();                                // reports success regardless of outcome
```

ioredis command methods return a promise. When the write fails — `WRONGTYPE`, `OOM`,
cluster `CROSSSLOT`, a connection drop mid-write, or wrong arity — that promise rejects with
**no handler**, so:

1. The failure becomes an **unhandled promise rejection**.
2. The node has **already called `done()`**, so the message is marked complete/successful and
   **no Catch node fires**.
3. The flow has no way to know the write was lost.

The existing `try/catch` only catches *synchronous* throws (e.g. an unknown command method),
not the async rejection that carries every real Redis error.

This is inconsistent with the sibling nodes: `redis-command` (`redis.js:702-710`) and
`redis-lua-script` (`redis.js:789-815`) both route errors through `done(err)`. `redis-out` is
the outlier.

The gap is **untested**: every `catch`/`done(err)` in `test/redis_out_spec.js` is test-harness
plumbing that asserts the written value, never an assertion that a write *failure* surfaces.

## Goals

- A failed `redis-out` write calls `done(err)` so Catch nodes fire and the message is marked
  errored.
- `done()` is deferred until Redis acknowledges the write (success path).
- No unhandled promise rejection is ever produced by this node.
- The message contract is unchanged: `redis-out` stays a terminal node (`outputs: 0`, no
  `send`).

## Non-goals

- No new editor fields, no new config (the "await + opt-out checkbox" option was rejected as
  YAGNI).
- No change to payload shaping for `xadd`/`zadd`/`obj`.
- No connection-id, sharing, or shutdown changes.
- The other two reliability/security items are not addressed here.

## Decision

**Await the write; `done()` after the ack; route every failure to `done(err)`.**

Rejected alternatives:

- **Catch-only (keep `done()` immediate, attach `.catch`)** — errors would reach Catch, but a
  downstream Complete node would fire before the write is confirmed, so "complete" would no
  longer mean "written." Rejected: weaker semantics for no real benefit.
- **Await + a "Fire and forget" opt-out checkbox** — adds an editor field, a `defaults` entry,
  and help text to maintain for a case ioredis pipelining already makes cheap. Rejected as
  YAGNI; can revisit if a throughput need is demonstrated.

## Implementation

Convert the `input` handler at `redis.js:582-627` to `async` and `await` the single Redis call
inside one `try/catch`:

```js
node.on("input", async function (msg, send, done) {
  send = send || function () { node.send.apply(node, arguments); };
  done = done || function (err) { if (err) node.error(err, msg); };

  const topic =
    msg.topic !== undefined && msg.topic !== "" ? msg.topic : node.topic;
  if (topic === "") {
    done(new Error("Missing topic, please send topic on msg or set Topic on node."));
    return;
  }

  try {
    if (node.command === "xadd") {
      let fields;
      const p = msg.payload;
      if (p && typeof p === "object" && !Array.isArray(p)) {
        fields = Object.entries(p).reduce((acc, pair) => acc.concat(pair), []);
      } else if (Array.isArray(p)) {
        fields = p;
      } else {
        fields = ["value", p != null ? String(p) : ""];
      }
      await client.xadd(topic, "*", ...fields);
    } else if (node.command === "zadd") {
      const p = msg.payload;
      if (p && typeof p === "object" && !Array.isArray(p) && "score" in p) {
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
});
```

## Invariants preserved

- **Write ordering is unchanged.** `client[cmd](...)` is evaluated (and the command enqueued in
  ioredis) *before* `await` suspends, so writes enter the connection queue in message-arrival
  order exactly as today. `await` only defers `done()`, not the issue order.
- **Sync short-circuits stay synchronous.** The empty-topic guard and the `zadd`
  "requires {score, member}" validation still `return done(err)` before any command is issued.
- **One `try/catch` now covers both failure modes** — the synchronous throw it already caught
  (unknown command method) *and* the async rejection it used to drop.
- **Throughput.** ioredis still auto-pipelines on the wire; Node-RED does not block on `done()`
  before delivering the next message, so concurrent in-flight writes remain possible.

## Error semantics

- Failure path: `done(err)` with the original `msg`, so Node-RED routes it to Catch nodes and
  marks the message errored. The legacy `done` shim (`node.error(err, msg)`) preserves behavior
  on Node-RED < 1.0.
- Success path: `done()` after the ack, no payload mutation (terminal node, no `send`).

## Testing (TDD)

Add to `test/redis_out_spec.js`. Write the failing regression first.

1. **Failure surfaces (failing-first):** seed a string key with `SET k "x"`, then drive
   `redis-out` with command `rpush`, topic `k`. Assert the node reports the `WRONGTYPE` error on
   its error path. Confirm the exact assertion idiom supported by `node-red-node-test-helper`
   (`node.on("call:error", ...)` spy vs. a wired Catch node) during implementation. This test
   fails against current `redis.js` (error is swallowed, `done()` reports success).
2. **Happy path unchanged:** existing write-then-read-back assertions for
   rpush/lpush/publish/xadd/zadd stay green.
3. **No unhandled rejection:** assert no `process` `unhandledRejection` fires during the failing
   write.

Then run the targeted spec, then the full `npm test` matrix.

## Documentation (Definition of Done)

Update in the same change:

- `redis.html` `redis-out` help (`data-help-name="redis-out"`) — state that write failures are
  reported to the node (Catch) and the write is awaited before completion.
- `docs/NODE_GUIDE.md` `redis-out` section — add the error-handling behavior.
- `docs/ARCHITECTURE.md` — note `redis-out` now awaits its write and routes errors to `done`.

## Versioning / migration

Treat as a **bug fix**, not a breaking redesign — no major version bump proposed for this change
alone. The only observable differences are that previously-silent write failures now surface and
`done()` lands after the ack. Call this out in the release notes / changelog for the upstream PR
so maintainers of existing flows know failures will now appear (and may want Catch nodes where
they previously saw silent success).
