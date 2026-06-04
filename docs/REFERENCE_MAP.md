# Reference Map

This document is the fastest entry point for humans and coding agents working on current branch.

## Read order by task

For any task, start with the entry point for your agent:

- Codex: `../AGENTS.md` and
  `../.codex/skills/node-red-contrib-redis-maintainer/SKILL.md`
- Claude: `../CLAUDE.md` and
  `../.claude/skills/node-red-contrib-redis-maintainer/SKILL.md`

`AGENTS.md` is a symlink to `CLAUDE.md`, so both agents read the same guidance.

Then read:

1. `ARCHITECTURE.md`
2. `NODE_GUIDE.md`
3. `CHANGE_WORKFLOW.md`
4. `TESTING.md`
5. `TROUBLESHOOTING.md`

## Repository layout

Top-level implementation:

- `../package.json`
- `../redis.js`
- `../redis.html`

Tests (21 Mocha spec files). Run the Docker-managed matrix with `npm test`; do not
rely on this list staying exhaustive — confirm with `ls test/*_spec.js`.

Node behavior and lifecycle:

- `../test/redis_in_spec.js` — `redis-in` blocking pops, pub/sub, streams
- `../test/redis_out_spec.js` — `redis-out` payload shaping
- `../test/redis_command_spec.js` — `redis-command` basic SET/GET/DEL flow
- `../test/redis_status_spec.js` — status + shutdown across **all** node types
- `../test/redis_lua_conn_spec.js` — Lua connection isolation across config nodes
- `../test/redis_lua_ui_spec.js` — Lua editor/library UI (static HTML parse, no Redis needed)
- `../test/redis_credentials_spec.js` — `redis-config` secret merge from the `secrets` credential (single/cluster/sentinel/legacy/env) plus guarded end-to-end auth

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

Deployment topology coverage:

- `../test/redis_cluster_deployment_spec.js` — Redis Cluster auth, same-slot/cross-slot behavior, pub/sub, blocking list, Lua fallback, Redis 7.2 cluster-prone commands
- `../test/redis_sentinel_deployment_spec.js` — Sentinel discovery/auth, pub/sub, blocking list, Lua, failover/reconnect, Redis 7.2 cluster-prone commands
- `../test/memorydb_deployment_spec.js` — opt-in AWS MemoryDB cluster/auth (JSON and env-var optionsType), same-slot/cross-slot, Lua, and Redis 7.2 cluster-prone command coverage

Browser editor coverage:

- `../test/playwright/redis-editor.spec.js` — real Node-RED editor coverage for
  `redis-config`, Lua library save metadata, `redis-in` field visibility, and
  `redis-command` typedInput initialization
- `../test/playwright/helpers/node-red-editor.js` — Playwright Node-RED editor helpers

Helper:

- `../test/helpers/cleanup.js`
- `../test/helpers/cluster-prone.js`
- `../test/helpers/deployment.js`
- `../test/helpers/topology.js`

Docker test deployments:

- `../scripts/ensure-docker-ubuntu.sh`
- `../scripts/run-deployment-tests.js`
- `../scripts/run-playwright-tests.js`
- `../test/deployments/`
- `../test/deployments/playwright-editor/`

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

- `../AGENTS.md` - symlink to `../CLAUDE.md` (shared agent guide)
- `../.codex/skills/node-red-contrib-redis-maintainer/SKILL.md` - symlink to the shared maintainer skill text
- `../CLAUDE.md`
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
- `../test/playwright/redis-editor.spec.js` for real editor/library behavior
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
- `TESTING.md` — Docker-managed Redis deployment matrix, test layout, and regression strategy
- `TROUBLESHOOTING.md` — common local Redis, test, stream, Lua, and sandbox failures

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

| Node               | Connection id (`redis.js`)        | Shared?               | Shutdown path                |
| ------------------ | --------------------------------- | --------------------- | ---------------------------- |
| `redis-in`         | `n.id`                            | dedicated per node    | forced disconnect (blocking) |
| `redis-out`        | `server.name`                     | shared by config name | graceful quit                |
| `redis-command`    | `block ? n.id : this.server.name` | conditional           | graceful quit                |
| `redis-lua-script` | `block ? n.id : this.server.name` | conditional           | graceful quit                |
| `redis-instance`   | `n.id`                            | dedicated per node    | graceful quit                |

All four config-name keys use `this.server.name` — the **resolved** config node
(`this.server = RED.nodes.getNode(n.server)`), not `n.server` (which is just the config-node
id string, so `n.server.name` is `undefined`). `redis-lua-script` used `n.server.name` until
a fix made it consistent with the others; the bug had all non-blocking Lua nodes collapse
onto a single pool key `undefined` and share one client across different configs
(regression test: `test/redis_lua_conn_spec.js`). Always confirm intent with a human before
changing any id key — it underpins subscriber mode, blocking behavior, status, and shutdown.
