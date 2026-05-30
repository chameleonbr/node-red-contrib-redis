
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

Tests:
- `../test/redis_in_spec.js`
- `../test/redis_out_spec.js`
- `../test/redis_status_spec.js`
- `../test/stream_commands_spec.js`
- `../test/redis_lua_ui_spec.js`
- `../test/helpers/cleanup.js`

Examples:
- `../examples/redis-list-queue.json`
- `../examples/redis-lua-script.json`
- `../examples/redis-priority-queue.json`
- `../examples/redis-psubscribe.json`
- `../examples/redis-pub-sub.json`
- `../examples/redis-set-and-get.json`
- `../examples/redis-streams.json`

Agent docs:
- `../AGENTS.md`
- `../.claude/skills/node-red-redis-maintainer/SKILL.md`

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

## Open item to verify before changing

There is a potential ambiguity in `RedisLua` connection-id selection.
Check the current constructor code and confirm intent with a human before altering shared-connection behavior there.
