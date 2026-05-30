# Node Guide

This guide explains the branch-specific behavior of each node type and where to extend it safely.

## `redis-config`

Purpose:
- store Redis options
- select cluster mode

Key implementation points:
- editor uses typedInput for `options`
- runtime evaluates `options` using `optionsType`
- env-string values are parsed as JSON when possible
- cluster mode constructs `new Redis.Cluster(options)`

Safe changes:
- clearer help text
- stricter validation
- better examples for JSON/env input

Be careful with:
- `optionsType`
- env parsing
- cluster option shape

## `redis-in`

Purpose:
- receive values from Redis or blocking Redis commands

Current command families:
- list blocking pops
- sorted-set blocking pops
- `subscribe`
- `psubscribe`
- `xreadgroup`

Message patterns:
- pub/sub emits `topic` and `payload`
- pattern subscriptions also emit `pattern`
- blocking pops emit Redis key as `topic`
- `xreadgroup` emits `stream`, `messageId`, and `payload`

Payload handling:
- when `obj` is true, JSON or field-object parsing is attempted
- when parsing fails, raw values are forwarded
- `xreadgroup` returns an object map when `obj` is true, otherwise a flat field/value array

Shutdown rules:
- always remove listeners
- always clear status
- force disconnect for blocking shutdown

Read before editing:
- `../test/redis_in_spec.js`
- `../test/redis_status_spec.js`

## `redis-out`

Purpose:
- write focused values to Redis

Branch-specific payload shaping:
- `xadd`
  - object payload becomes flattened field/value pairs
  - array payload is passed through
  - primitive payload is wrapped as `["value", String(payload)]`
- `zadd`
  - object payload expects `{ score, member }`
  - array payload supports flat `[score, member, ...]`
  - object members are JSON-stringified when `obj` is true
- list push operations accept plain or JSON-stringified payloads depending on `obj`

Read before editing:
- `../test/redis_out_spec.js`

## `redis-command`

Purpose:
- run generic Redis commands and return the result in `msg.payload`

Behavior:
- `msg.topic` overrides the configured topic/key
- `msg.payload` overrides static params when provided
- static params come from JSON typedInput
- `block` forces a dedicated connection id

Use this node for:
- commands not modeled by `redis-out`
- Redis modules and advanced commands
- stream command coverage already exercised in tests

Read before editing:
- `../test/stream_commands_spec.js`
- `../test/redis_status_spec.js`

## `redis-lua-script`

Purpose:
- execute Lua scripts against Redis

Behavior:
- unstored scripts use `EVAL`
- stored scripts load with `SCRIPT LOAD`
- stored scripts run via `EVALSHA`
- `NOSCRIPT` falls back to `EVAL`

Input expectations:
- if `keyval > 0`, `msg.payload` must be an array
- result is always returned in `msg.payload`

Critical editor behaviors:
- library type must remain `lua`
- extension must remain `.lua`
- checkbox metadata for `stored` and `block` must use explicit get/set handling

Read before editing:
- `../test/redis_lua_ui_spec.js`
- `../test/redis_status_spec.js`

## `redis-instance`

Purpose:
- place a live Redis client into Node-RED context

Behavior:
- stores the client under `flow` or `global` context based on configuration
  (the editor offers only these two; the runtime does `this.context()[node.location]`,
  and there is no `node` accessor on `this.context()`)
- clears the stored reference on close
- shares most lifecycle expectations with other nodes

Use cases:
- advanced Function-node logic
- custom Redis calls not modeled by built-in nodes

Be careful with:
- context location names
- topic/key used as the context key
- close cleanup

## Editor guidance

When adding fields in `redis.html`:
- use stable `defaults` names
- add validation in the editor when possible
- keep runtime fallback validation too
- update help text for new fields
- verify typedInput wiring and hidden type fields

## Example flows

Use existing example flows as the first source of user-facing patterns:
- list queue
- priority queue
- pub/sub
- pattern subscription
- set/get
- Lua script
- streams

If a new feature is hard to discover, add or update one example flow.
