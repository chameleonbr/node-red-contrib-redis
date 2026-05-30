
**`.claude/skills/node-red-contrib-redis-maintainer/SKILL.md`**

name: node-red-contrib-redis-maintainer
description: Maintain and extend the node-red-contrib-redis package. Use for bug fixes, small features, tests, editor/runtime consistency work, and documentation updates in this repository.

# Node-RED Redis Node maintainer skill

Use this skill when working on this repository's custom Node-RED nodes.

## Outcome

Produce the smallest safe change that preserves:
- Node-RED compatibility
- existing flow contracts
- runtime/editor alignment
- test coverage
- connection lifecycle correctness

## Required read order

Read these before making changes:
1. `CLAUDE.md`
2. `docs/REFERENCE_MAP.md`
3. `docs/ARCHITECTURE.md`
4. `docs/NODE_GUIDE.md`
5. `docs/CHANGE_WORKFLOW.md`
6. `docs/TESTING.md`

Then read the source and test files for the exact node type you will touch.

## Source map

Core files:
- `package.json`
- `redis.js`
- `redis.html`

Tests:
- `test/redis_in_spec.js`
- `test/redis_out_spec.js`
- `test/redis_status_spec.js`
- `test/stream_commands_spec.js`
- `test/redis_lua_ui_spec.js`

Supporting files:
- `test/helpers/cleanup.js`
- `examples/*.json`

## Working rules

Do not use other branches as design input.

Prefer this workflow:
1. identify the affected node type and behavior
2. inspect matching tests first
3. write or adjust the narrowest failing test
4. implement the minimum code change
5. update help text if user-visible behavior changed
6. run targeted tests, then `npm test`

## Design constraints

Respect Node-RED patterns:
- config nodes are referenced by id and resolved with `RED.nodes.getNode`
- input handlers should preserve `msg`
- close handlers must release resources
- editor `defaults` names are part of compatibility

Respect ioredis constraints:
- pub/sub connections enter subscriber mode
- blocking commands can require dedicated connections
- reconnect and shutdown behavior matter
- Lua stored scripts can need reload/fallback handling

## Change heuristics

Good changes:
- narrow
- tested
- branch-specific
- easy to review
- low-risk for flows already deployed

Bad changes:
- broad refactors
- renaming public properties
- mixing unrelated cleanup with bug fixes
- changing runtime without editor/help/test updates
- changing connection ownership without deep review

## Common task recipes

### Fix a runtime bug

Read:
- matching node section in `docs/NODE_GUIDE.md`
- matching spec file
- relevant close/status logic in `redis.js`

Then:
- reproduce with a test
- change the smallest branch-specific code path
- verify status and shutdown behavior were not regressed

### Add a small feature

First decide whether it belongs in:
- `redis-in`
- `redis-out`
- `redis-command`
- `redis-lua-script`
- `redis-instance`

If the feature changes user configuration:
- update `redis.html`
- update help text
- add or update tests
- consider an example flow

### Modify Lua/library behavior

Always read:
- `test/redis_lua_ui_spec.js`

Be careful with:
- library type name
- file extension
- checkbox persistence
- DOM ids
- stored vs unstored runtime behavior

### Modify streams

Always read:
- `test/stream_commands_spec.js`
- the stream sections in `test/redis_in_spec.js` and `test/redis_out_spec.js`

Be careful with:
- flat vs object payloads
- message ids
- group/consumer semantics
- blocking behavior
- cleanup of test keys

## Verification

Assume a real Redis server must be running on `127.0.0.1:6379`.

Primary command:
```bash
npm test
