# Testing

This repository uses Mocha with `node-red-node-test-helper` for runtime coverage and
Playwright for real Node-RED editor coverage. `npm test` manages Redis deployments with
Docker so the Mocha suite can verify unauthenticated standalone, authenticated
standalone, Redis Cluster, Redis Sentinel, and optional AWS MemoryDB behavior.

## Prerequisite

Docker Engine with the Compose plugin must be usable by the current user, or via
passwordless `sudo -n docker`.

`npm test` runs `scripts/ensure-docker-ubuntu.sh` first. On Ubuntu 22.04, 24.04, or
26.04 it checks for `docker` and `docker compose`; if either is missing, it installs Docker
Engine using Docker's official apt repository flow. Installation may prompt for `sudo`.
If Docker is installed but the current shell has not picked up docker-group membership yet,
the runner falls back to `sudo -n docker` when available. On non-Ubuntu hosts, install
Docker yourself before running the suite.

Do not start a separate Redis on the test ports while running `npm test`; the runner owns
one local deployment at a time and tears it down with volumes before continuing.

## Main command

Run all deployment tests:

```bash
npm test
```

The runner executes these deployments sequentially:

- `single-noauth`: Redis 8.8+ image on `127.0.0.1:6379`; standalone Mocha specs.
- `single-auth`: Redis 8.8+ image on `127.0.0.1:6379` with ACL username/password; standalone Mocha specs.
- `cluster-auth`: Redis 7.2 image, two authenticated Cluster masters with all slots assigned; topology specs plus Redis 7.2-supported cluster-prone command coverage.
- `sentinel-auth`: Redis 7.2 image, three authenticated Redis data nodes plus three Sentinel processes; topology specs plus Redis 7.2-supported cluster-prone command coverage.
- `memorydb`: optional AWS MemoryDB topology specs when `MEMORYDB_ENABLED=1`.

Run the browser editor suite:

```bash
npm run test:playwright
```

The Playwright runner starts its own Docker deployment with no-auth standalone Redis,
authenticated standalone Redis, and a two-node authenticated Redis Cluster. It then starts
real Node-RED editor instances and verifies `redis-config` JSON/env/cluster editing, Lua
library-save metadata, `redis-in` command field visibility, and `redis-command` typedInput
initialization. MemoryDB editor coverage is skipped unless all `MEMORYDB_*` variables are
set.

The raw Mocha command is still available for targeted iteration when you have already
started a compatible Redis yourself:

```bash
npm run test:mocha -- test/redis_in_spec.js
```

To run the raw full Mocha glob against a Redis you started yourself:

```bash
npm run test:mocha:all
```

The raw full glob includes topology specs outside their matching deployment, so Mocha may
report them as pending. `npm test` excludes those topology specs from standalone runs and
executes them only in their own deployment stage.

Standalone specs read connection details from:

- `REDIS_HOST`
- `REDIS_PORT`
- `REDIS_USERNAME`
- `REDIS_PASSWORD`

The Docker runner sets these automatically for local deployments.

## AWS MemoryDB

MemoryDB tests are opt-in and use environment variables only. Do not commit MemoryDB
endpoints or credentials.

Required variables:

```bash
export MEMORYDB_ENABLED=1
export MEMORYDB_ENDPOINT="clustercfg.example.memorydb.region.amazonaws.com"
export MEMORYDB_PORT="6379"
export MEMORYDB_USERNAME="..."
export MEMORYDB_PASSWORD="..."
```

The MemoryDB `redis-config` simulation uses cluster mode with a single startup node:

```json
[
  {
    "dnsLookupStrategy": "identity",
    "host": "$MEMORYDB_ENDPOINT",
    "port": 6379,
    "username": "$MEMORYDB_USERNAME",
    "password": "$MEMORYDB_PASSWORD"
  }
]
```

The runtime interprets `dnsLookupStrategy: "identity"` as ioredis identity DNS lookup with
TLS enabled for the cluster connection.

## Test layout

There are 21 Mocha spec files. Do not assume this list is exhaustive forever; confirm with
`ls test/*_spec.js`.

Node behavior and lifecycle:

- `redis_in_spec.js` — `redis-in`: blocking pops, pub/sub, `xreadgroup`
- `redis_out_spec.js` — `redis-out`: `xadd`/`zadd`/list payload shaping
- `redis_command_spec.js` — `redis-command`: basic SET/GET/DEL round-trip
- `redis_status_spec.js` — `node.status` and shutdown across all node types
- `redis_lua_conn_spec.js` — Lua connection isolation across config nodes
- `redis_lua_ui_spec.js` — Lua editor/library UI; static HTML parse, needs no Redis
- `redis_credentials_spec.js` — `redis-config` secret merge from the `secrets` credential; constructor-only (no Redis) plus a guarded end-to-end auth case in the auth stage

Command-family coverage, all driving `redis-command` through `client.call`:

- `bit_`, `geo_`, `hash_`, `hyperloglog_`, `key_`, `list_`, `scripting_`, `server_`,
  `set_`, `sorted_set_`, `stream_`, `string_commands_spec.js`
- `scripting_commands_spec.js` additionally drives the `redis-lua-script` node directly for
  read-only (`EVAL_RO`/`EVALSHA_RO`), Function mode (`FCALL`/`FCALL_RO`, reload recovery), and
  block mode — including a server-side dedicated-connection proof that sets an ioredis
  `connectionName` on the config and counts named connections via `CLIENT LIST`
  (non-block nodes must pool onto one connection; each block node must add its own)

Deployment topology coverage:

- `redis_cluster_deployment_spec.js` — Redis Cluster auth, same-slot success, cross-slot failure, pub/sub, blocking list, Lua fallback, same-slot FCALL + read-only Lua, block-mode Script/Function execution, Redis 7.2 cluster-prone commands
- `redis_sentinel_deployment_spec.js` — Sentinel discovery/auth, pub/sub, blocking list, Lua, FCALL + read-only Lua, block-mode Script/Function with a `CLIENT LIST` dedicated-connection proof on the discovered master, failover/reconnect, Redis 7.2 cluster-prone commands
- `memorydb_deployment_spec.js` — opt-in AWS MemoryDB cluster/auth (JSON and env-var optionsType)/same-slot/cross-slot/Lua, read-only Lua + FCALL and block-mode coverage (gated on engine function support), and Redis 7.2 cluster-prone command coverage

The block-mode server-side proof (counting `CLIENT LIST` entries by `connectionName`) runs in
the standalone and Sentinel specs only: the cluster config path cannot carry an ioredis
`connectionName`, and the block/shared connection keying in `RedisLua` is topology-independent,
so cluster and MemoryDB keep execution-level block coverage.

Helpers:

- `test/helpers/deployment.js` — active standalone Redis config and direct clients
- `test/helpers/cleanup.js` — pattern cleanup for standalone deployments
- `test/helpers/topology.js` — Node-RED flow invocation helpers for topology specs
- `test/helpers/cluster-prone.js` — shared same-slot and cross-slot Redis 7.2 command matrix for Cluster, Sentinel, and MemoryDB

Browser editor coverage:

- `test/playwright/redis-editor.spec.js` — real Node-RED editor tests
- `test/playwright/helpers/node-red-editor.js` — Node-RED editor launch and interaction helpers
- `test/deployments/playwright-editor/` — Docker Compose deployment used by `npm run test:playwright`

## How the tests work

Behavioral specs follow one pattern:

1. `helper.load(redisNode, flow, cb)` boots a flow made of plain JS objects, including a
   `redis-config` node and `helper` sink nodes.
2. `helper.getNode(id)` grabs a node instance.
3. Tests drive it with `node.receive(msg)` and assert messages from helper sink nodes.
4. `afterEach` unloads Node-RED, then deletes namespaced Redis keys.

All test keys must be namespaced and explicitly cleaned. Do not use shared-test `FLUSHDB`.
`SCRIPT FLUSH` is acceptable only when a test is specifically exercising script-cache
reload behavior.

Topology specs exercise Redis 7.2-supported commands that are easy to misuse in sharded
or discovered deployments: multi-key string/key commands, set and sorted-set algebra,
HyperLogLog merges, multi-stream reads, multi-key blocking pops, transactions, Lua
scripts, `KEYS`/`SCAN`/`DBSIZE`, and `SELECT`. Cluster and MemoryDB assert both hash-tagged
same-slot success and deliberate cross-slot failures; Sentinel asserts the same command
surface against the discovered primary.

## Regression strategy

When behavior changes, add or adjust the narrowest test in the matching spec that fails
before the change and passes after. Prefer extending an existing spec. If you add, rename,
or remove a spec file, update `REFERENCE_MAP.md`, this file, and the maintainer skill file.
