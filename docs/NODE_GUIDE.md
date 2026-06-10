# Node Guide

This guide explains the branch-specific behavior of each node type and where to extend it safely.

## `redis-config`

Purpose:

- store Redis options
- select cluster mode

Key implementation points:

- editor uses typedInput for `options`
- runtime evaluates `options` using `optionsType`
- env-string values are verified, then parsed as JSON when possible
- cluster mode constructs `new Redis.Cluster(options)`
- parsed option arrays are treated as Redis Cluster startup-node lists even if the
  saved UI mode flag is stale from a previous JSON single-node configuration
- cluster startup-node `username`/`password` values are also passed as ioredis
  `redisOptions` so discovered cluster nodes authenticate correctly
- `dnsLookupStrategy: "identity"` on a cluster startup node enables identity DNS lookup
  and TLS for AWS MemoryDB/ElastiCache-style configuration endpoints
- the editor Test connection button posts the current form values to a runtime admin
  endpoint, creates a temporary client, connects, runs `PING`, expects `PONG`, and
  disconnects with `QUIT` without adding the client to the shared connection pool
- saved environment-variable connection configs reopen on the ConnString tab with the
  variable name selected; saved JSON configs reopen on the Connection tab
- while environment-variable connection options are selected, the Connection tab is
  read-only and becomes editable again when JSON is selected

Safe changes:

- clearer help text
- stricter validation
- better examples for JSON/env input

Be careful with:

- `optionsType`
- env parsing
- cluster option shape
- never commit cloud Redis endpoints or credentials in tests, examples, or docs

Secret storage:

- in JSON mode, passwords are extracted into a `text`-type `secrets` credential (encrypted,
  out of `flows.json`) and merged back into `options` at runtime (`mergeSecrets`) and on editor
  load; `extractSecrets` strips them on save. The two helper copies in `redis.js` and `redis.html`
  must stay in sync
- a legacy password still embedded in `options` keeps working (merge is a no-op without a
  credential) and migrates to the credential when the config is reopened and saved
- `env` mode is untouched — the secret stays in the environment

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

Error handling:

- the write is awaited; `done()` resolves only after Redis acknowledges it
- a failed write calls `done(err)`, so the message reaches a `catch` node and is marked errored
- no write is fire-and-forget, so a failure cannot become an unhandled promise rejection

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
- `FUNCTION` and `SCRIPT` management subcommands (LOAD, LIST, FLUSH, EXISTS, KILL, …);
  the `redis-lua-script` node only executes

Read before editing:

- `../test/stream_commands_spec.js`
- `../test/redis_status_spec.js`

## `redis-lua-script`

Purpose:

- execute Lua scripts against Redis

Behavior (command resolved from `mode` + `stored` + `readonly`):

| mode | stored | readonly | on ready | on input | recovery |
|------|--------|----------|----------|----------|----------|
| script | no | no | — | `EVAL` | — |
| script | no | yes | — | `EVAL_RO` | — |
| script | yes | no | `SCRIPT LOAD` | `EVALSHA` | `NOSCRIPT` → `EVAL` |
| script | yes | yes | `SCRIPT LOAD` | `EVALSHA_RO` | `NOSCRIPT` → `EVAL_RO` |
| function | n/a | no | `FUNCTION LOAD REPLACE` | `FCALL` | "function not found" → reload → retry once |
| function | n/a | yes | `FUNCTION LOAD REPLACE` | `FCALL_RO` | "function not found" → reload → retry once |

- Function mode treats the editor as a Redis Functions library source (`#!lua name=…`);
  the node `FUNCTION LOAD REPLACE`s it on every connection `ready`, on all masters in
  cluster mode, so an `FCALL` routed to any shard can resolve. An empty library source in
  Function mode is a configuration error.
- This node is execution-only. `FUNCTION *` and `SCRIPT *` management subcommands stay in
  `redis-command`.

Input expectations:

- if `keyval > 0`, `msg.payload` must be an array
- result is always returned in `msg.payload`

Critical editor behaviors:

- library type must remain `lua`
- extension must remain `.lua`
- checkbox metadata for `stored` and `block` must use explicit get/set handling
- `mode` uses get/set so Open Library re-applies field visibility; `fname` is mandatory in
  Function mode (editor `validate` + runtime fail-fast)
- switching a pristine node (untouched starter content) to Function mode seeds a working
  `#!lua name=…` + `redis.register_function` template and pre-fills the function name;
  user-edited code is never replaced

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

Node icon (`icons/redis-logo.png`):

- the workspace renders **SVG icons at the full 30px box** but scales raster icons to
  ≤20px and centers them — so the icon must stay a high-resolution PNG (512px) to match
  the palette's look; the vector source lives at `docs/assets/redis-logo.svg`
- never place a same-named `.svg` next to the `.png` in `icons/` — Node-RED serves the
  SVG in preference, silently reintroducing the oversized workspace icon

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
- Redis Functions (`FCALL`)
- `FUNCTION`/`SCRIPT` management via `redis-command`
- streams

If a new feature is hard to discover, add or update one example flow.
