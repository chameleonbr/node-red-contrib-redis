# Testing

This repository uses Mocha with `node-red-node-test-helper`, but the tests also require a real Redis server.

## Prerequisite

Start Redis locally on:
- host: `127.0.0.1`
- port: `6379`

The current specs and cleanup helper assume that address directly.

Redis is expected to be installed and available locally already. If it is **not installed
at all**, stop and ask the human to install it — do not install the server package
yourself.

The suite does not start or stop Redis for you, and it writes/reads real keys. The
`test/helpers/cleanup.js` helper deletes only keys matching a given pattern, so tests are
responsible for namespacing and cleaning their own keys.

## Agent boundary — system/service changes are allowed *if restored*

This is a development machine with no production data, so for testing purposes an agent
**may** make system or service changes when a test genuinely needs them: installing,
uninstalling, upgrading, or downgrading Redis; editing `redis.conf`; changing runtime
config (`CONFIG SET`); modifying ACLs; etc. There is no risk in doing so.

The hard requirement is **reversibility**:

- Before changing anything, capture the original state (e.g. record the current
  `redis.conf`, `CONFIG GET` values, ACL list, installed version).
- After the test, **restore that original state exactly** so the environment — and the
  test result — is reproducible. Leave the machine as you found it.
- Never leave Redis stopped, reconfigured, flushed, or on a different version once your
  work is done.

The one exception to "you may change it" is initial provisioning: if Redis is not installed
at all, ask the human to install it rather than installing the server yourself.

## Main command

Run all tests:
```bash
npm test
```

This runs `mocha "test/**/*_spec.js"`. Husky also runs `npm test` on pre-commit, and
lint-staged formats staged files with Prettier — so a failing suite (or a Redis that is not
running) will block your commit.

To run a single spec while iterating:
```bash
npx mocha test/redis_in_spec.js
```

## Test layout

There are 17 spec files (~237 `it()` cases). Do not assume this list is exhaustive forever —
confirm with `ls test/*_spec.js`.

Node behavior and lifecycle:
- `redis_in_spec.js` — `redis-in`: blocking pops, pub/sub, `xreadgroup`
- `redis_out_spec.js` — `redis-out`: `xadd`/`zadd`/list payload shaping
- `redis_command_spec.js` — `redis-command`: basic SET/GET/DEL round-trip
- `redis_status_spec.js` — `node.status` and shutdown across **all** node types
- `redis_lua_ui_spec.js` — Lua editor/library UI; **static HTML parse, needs no Redis**

Command-family coverage (all drive `redis-command` through `client.call`):
- `bit_`, `geo_`, `hash_`, `hyperloglog_`, `key_`, `list_`, `scripting_`, `server_`,
  `set_`, `sorted_set_`, `stream_`, `string_commands_spec.js`

Every spec except `redis_lua_ui_spec.js` requires a live Redis.

## How the tests work (node-red-node-test-helper)

The behavioral specs follow one pattern:

1. `helper.load(redisNode, flow, cb)` — boot a flow made of plain JS objects, including a
   `redis-config` node and `helper` sink nodes.
2. `helper.getNode(id)` — grab a node instance.
3. drive it: `node.receive(msg)` to send input; `helper.getNode("sink").on("input", ...)`
   to assert on what came out.
4. assert inside the handler, calling Mocha's `done()` / `done(err)`.
5. clean up Redis keys (see `helpers/cleanup.js`) in `afterEach`.

`redis_lua_ui_spec.js` is different: it reads `redis.html` as text and asserts on the
`RED.library.create(...)` block (library `type`, `ext`, and the `stored`/`block` checkbox
get/set fields). It guards regressions in the editor template, not the runtime.

## Regression strategy

When you change behavior, add or adjust the narrowest test in the matching spec that fails
before your change and passes after. Prefer extending an existing spec over creating a new
file; if you do add a file, update the spec lists in `REFERENCE_MAP.md` and the skill file.
