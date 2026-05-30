# Architecture

This package is a classic Node-RED node module with one runtime file and one editor/help file.

## Files and responsibilities

`../redis.js`
- exports the Node-RED module
- registers all runtime node types
- owns connection pooling and shutdown logic

`../redis.html`
- registers all editor node definitions
- defines forms, defaults, labels, help text, and command lists

`../package.json`
- registers the package with Node-RED
- defines scripts and dependencies

## Registered node types

Runtime and editor pairs:
- `redis-config`
- `redis-in`
- `redis-out`
- `redis-command`
- `redis-lua-script`
- `redis-instance`

## Connection model

Module-level state in `redis.js`:
- `connections` — id → live ioredis client
- `usedConn` — id → reference count

Use this as the source of truth for connection sharing.

Reference counting is the rule that ties it together: `getConn(config, id)` reuses an
existing client and increments `usedConn[id]`; `disconnect(id)` decrements it and only tears
the socket down when the count reaches 0. So a shared connection survives until the **last**
referencing node closes. Getting an id key wrong silently changes who shares with whom.

A per-node-type cheat-sheet of which id each node uses lives in `REFERENCE_MAP.md`
(Connection-id quick reference). Keep the two in sync.

### Shared vs dedicated connections

`redis-in`
- uses `getConn(this.server, n.id)`
- each node instance gets its own connection id
- this matches subscriber and blocking-input needs

`redis-out`
- uses `getConn(this.server, node.server.name)`
- write nodes share by config-node name

`redis-command`
- uses `n.id` when `block` is true
- otherwise shares by config-node name

`redis-lua-script`
- uses a block-sensitive connection id path
- treat this area as high-risk and verify intent before changing it

`redis-instance`
- uses a per-node id and stores the client into Node-RED context

## Status model

`attachStatusListeners` connects node status to Redis client events:
- `ready`
- `error`
- `close`
- `reconnecting`
- `end`

It also sets initial status immediately from `client.status` so reused connections still show the correct state when a node starts.

## Shutdown model

There are two shutdown paths:

### Graceful path

Used for non-blocking connections.
`gracefulQuit`:
- skips `quit()` if the client is not ready
- otherwise attempts `client.quit()`
- falls back to `client.disconnect()` after a timeout or error

### Forced path

Used for blocking `redis-in` shutdown.
This intentionally skips `quit()` and disconnects immediately because blocking commands can keep `QUIT` queued behind an in-flight command.

## Node behavior summary

### `redis-config`

Stores Redis connection options and cluster mode.
Options can come from typedInput values and are evaluated in runtime code.

### `redis-in`

Implements:
- blocking list pops
- sorted-set blocking pops
- `subscribe`
- `psubscribe`
- `xreadgroup`

It sends Node-RED messages from Redis events or blocking loops.

### `redis-out`

Implements focused write operations with custom payload shaping for selected commands such as:
- list pushes
- stream add
- sorted-set add

### `redis-command`

Implements generic command execution via `client.call(...)`.
Use this for wide command coverage and advanced Redis or module commands.

### `redis-lua-script`

Executes Lua via:
- `EVAL` for unstored scripts
- `SCRIPT LOAD` + `EVALSHA` for stored scripts
- `NOSCRIPT` fallback back to `EVAL`

This node has editor-library integration and dedicated UI regression tests.

### `redis-instance`

Stores the Redis client in flow or global context so Function nodes can reuse it.
The runtime indexes `this.context()[node.location]`, so `location` must be `flow` or
`global` — the editor only offers those two. There is no `this.context().node` accessor,
so a `node` value would throw (the store is wrapped in try/catch and would be silently lost).

## Editor/runtime coupling

Do not treat `redis.html` as documentation-only.
It contains behavior:
- field defaults
- validation
- command lists
- typedInput wiring
- library metadata for Lua
- help content that must match runtime semantics

Any user-facing runtime change may require a matching editor and help change.

## Architecture invariants

Keep these true:
- runtime and editor property names match
- shutdown always removes listeners and clears status
- tests describe the intended public behavior
- Redis connections are explicitly released
- blocking and subscriber code paths remain isolated where needed
- examples stay representative of supported patterns

## Extension hotspots

Safer extension points:
- add targeted command handling in existing node branches
- add tests before changing payload normalization
- expand help text and examples
- improve validation without renaming fields

High-risk extension points:
- changing connection-id keys
- changing close semantics
- changing stream payload shapes
- changing Lua library metadata
- changing context-storage semantics
