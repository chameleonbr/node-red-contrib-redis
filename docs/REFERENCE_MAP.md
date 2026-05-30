
# Reference Map

This document is the fastest entry point for humans and coding agents working on current branch.

## Read order by task

For any task:
1. `../CLAUDE.md`
2. `ARCHITECTURE.md`
3. `NODE_GUIDE.md`
4. `CHANGE_WORKFLOW.md`
5. `TESTING.md`

## Repository layout

Top-level implementation:
- `../package.json`
- `../redis.js`
- `../redis.html`

Tests (18 spec files, ~237 `it()` cases). Run them all with `npm test`; do not rely on this list staying exhaustive — confirm with `ls test/*_spec.js`.

Node behavior and lifecycle:
- `../test/redis_in_spec.js` — `redis-in` blocking pops, pub/sub, streams
- `../test/redis_out_spec.js` — `redis-out` payload shaping
- `../test/redis_command_spec.js` — `redis-command` basic SET/GET/DEL flow
- `../test/redis_status_spec.js` — status + shutdown across **all** node types
- `../test/redis_lua_ui_spec.js` — Lua editor/library UI (static HTML parse, no Redis needed)

Command-family coverage (all drive `redis-command` via `client.call`):
- `../test/bit_commands_spec.js`
- `../test/geo_commands_spec.js`
- `../test/hash_commands_spec.js`
- `../test/hyperloglog_commands_spec.js`
- `../test/key_commands_spec.js`
- `../test/list_commands_spec.js`
- `../test/scripting_commands_spec.js`
- `../test/server_commands_spec.js`
- `../test/set_commands_spec.js`
- `../test/sorted_set_commands_spec.js`
- `../test/stream_commands_spec.js`
- `../test/string_commands_spec.js`

Helper:
- `../test/helpers/cleanup.js`

Examples:
- `../examples/redis-list-queue.json`
- `../examples/redis-lua-script.json`
- `../examples/redis-priority-queue.json`
- `../examples/redis-psubscribe.json`
- `../examples/redis-pub-sub.json`
- `../examples/redis-set-and-get.json`
- `../examples/redis-streams.json`

User-facing docs:
- `../README.md` — public npm/GitHub readme; keep in sync when node types or fields change

Agent docs:
- `../AGENTS.md`
- `../.claude/skills/node-red-contrib-redis-maintainer/SKILL.md`

## Where to look by feature

### Config and connection options
Read:
- `../redis.html` `redis-config`
- `../redis.js` `RedisConfig`
- `ARCHITECTURE.md`

### Blocking inputs, pub/sub, streams
Read:
- `../redis.js` `RedisIn`
- `../redis.html` `redis-in`
- `../test/redis_in_spec.js`
- `../test/redis_status_spec.js`

### Write commands
Read:
- `../redis.js` `RedisOut`
- `../redis.html` `redis-out`
- `../test/redis_out_spec.js`

### Generic commands
Read:
- `../redis.js` `RedisCmd`
- `../redis.html` `redis-command`
- `../test/stream_commands_spec.js` for advanced command coverage

### Lua
Read:
- `../redis.js` `RedisLua`
- `../redis.html` `redis-lua-script`
- `../test/redis_lua_ui_spec.js`
- `../test/redis_status_spec.js`

### Context injection
Read:
- `../redis.js` `RedisInstance`
- `../redis.html` `redis-instance`
- `NODE_GUIDE.md`

### Shutdown and status
Read:
- `../redis.js` `attachStatusListeners`, `gracefulQuit`, `disconnect`
- `../test/redis_status_spec.js`

## What each project doc covers

- `ARCHITECTURE.md` — runtime/editor design, connection ownership, shutdown model
- `NODE_GUIDE.md` — node-by-node behavior and extension guidance
- `CHANGE_WORKFLOW.md` — minimal-change rules and edit checklist
- `TESTING.md` — Redis prerequisite, test layout, and regression strategy

## Quick warnings

Do not start with refactoring.
Start with the exact node type and exact spec file.

Do not change public fields casually:
- node type names
- `defaults` property names
- message field names
- tested DOM ids

Do not change connection-id logic casually.
It is central to subscriber mode, blocking behavior, status, and shutdown.

## Connection-id quick reference

The single most breakable thing in this repo. Each node picks a connection id and
`getConn`/`disconnect` refcount it via `usedConn` — a connection only closes when its
refcount reaches 0.

| Node | Connection id (`redis.js`) | Shared? | Shutdown path |
|---|---|---|---|
| `redis-in` | `n.id` | dedicated per node | forced disconnect (blocking) |
| `redis-out` | `server.name` | shared by config name | graceful quit |
| `redis-command` | `block ? n.id : server.name` | conditional | graceful quit |
| `redis-lua-script` | `block ? n.id : n.server.name` | conditional | graceful quit |
| `redis-instance` | `n.id` | dedicated per node | graceful quit |

`RedisLua` connection-id selection (previously flagged as ambiguous) is resolved: it uses
the same `block ? n.id : <config name>` rule as `redis-command`. Still confirm intent with a
human before changing any id key — it underpins subscriber mode, blocking behavior, status,
and shutdown.
