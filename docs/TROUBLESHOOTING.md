# Troubleshooting

Use this guide when tests fail, Redis behavior looks inconsistent, or a node works in the
editor but not at runtime.

## Docker Or Redis Deployment Is Not Running

`npm test` starts Redis deployments through Docker Compose. Most connection failures now
mean Docker is unavailable, a test port is already in use, or the deployment did not become
ready before the timeout.

Symptoms:

- connection refused
- Docker permission errors
- port allocation errors on `6379`, `7000`, `7001`, or Sentinel ports
- status tests stay red
- most specs fail quickly

Check `docker info` or `sudo -n docker info`, and confirm no separate Redis process is
bound to the runner ports before debugging node code.

## Redis Version Mismatch

The test suite covers modern Redis commands, including streams, newer sorted-set/list
commands, Lua scripting, and newer hash commands.

Symptoms:

- `ERR unknown command`
- command-family specs fail while basic `SET` / `GET` still work
- failures are clustered in one command family spec

First confirm the deployment under test uses the expected image. Standalone full-suite
deployments use Redis 8.8+; Cluster and Sentinel topology deployments use Redis 7.2.

## Stale Test Keys

Tests use real Redis keys. Most specs clean namespaced keys in `afterEach`, but an
interrupted run can leave keys behind.

Symptoms:

- expected empty list/set/hash is not empty
- consumer group already exists
- command returns unexpected previous data

Prefer cleaning only the affected namespace. Do not use database-wide cleanup unless you
intentionally want to remove everything in the local test Redis.

## Stream Consumer Groups

`redis-in` with `xreadgroup` expects the consumer group to already exist.

Symptoms:

- warning about `NOGROUP`
- no stream messages are emitted

Create the group before starting the node, for example:

```bash
XGROUP CREATE mystream workers $ MKSTREAM
```

The `redis-in` topic format for streams is:

```text
<stream-key>:<start-id>
```

Example:

```text
taskstream:>
```

The stream key itself must not contain a colon.

## Pub/Sub Timing

Pub/sub tests and flows can miss messages if the publisher sends before the subscriber is
fully subscribed.

Symptoms:

- intermittent pub/sub test timeouts
- publish command succeeds but subscriber receives nothing

Delay publishing until the subscriber node is loaded and connected. Existing tests use short
`setTimeout` delays for this reason.

## Blocking Commands

Blocking commands can occupy a Redis connection.

Examples:

- `BLPOP`
- `BRPOP`
- `BZPOPMIN`
- `BZPOPMAX`
- `XREADGROUP BLOCK 0`

Use dedicated/blocking connection settings where available. Be careful changing shutdown
behavior: blocking `redis-in` intentionally force-disconnects instead of sending `QUIT`.

## Lua Library UI

Lua editor/library behavior is guarded by `test/redis_lua_ui_spec.js`.

Symptoms:

- Open/Save Library button does not work
- stored/block checkbox values are lost
- Lua files save with the wrong extension

Check that `RED.library.create(...)` still uses:

- `type: "lua"`
- `ext: "lua"`
- explicit `get` / `set` handlers for `stored`
- explicit `get` / `set` handlers for `block`

## Node-RED Helper Lifecycle

Tests use `node-red-node-test-helper`.

Common pattern:

1. `helper.startServer(...)`
2. `helper.load(redisNode, flow, callback)`
3. `helper.getNode(id)`
4. `node.receive(msg)`
5. `helper.unload()`
6. `helper.stopServer(...)`

If status or close-handler assertions are flaky, check whether the test-helper sandbox
restores spies before flow shutdown. See `test/redis_status_spec.js` for the direct
`n.status = ...` pattern used to observe close-time status clearing.

## Agent Sandbox Issues

Some environments may block file edits, service commands, Redis restarts, or package
installation.

If a command fails with a sandbox or permission error, do not work around it with broad
destructive commands. Ask for explicit permission or use the narrowest approved command
needed for the task.

## Where To Look Next

- Runtime/editor behavior: `docs/NODE_GUIDE.md`
- Connection lifecycle: `docs/ARCHITECTURE.md`
- Test requirements: `docs/TESTING.md`
- Safe edit workflow: `docs/CHANGE_WORKFLOW.md`
- Source map: `docs/REFERENCE_MAP.md`
