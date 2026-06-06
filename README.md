# node-red-contrib-redis

[![npm version](https://img.shields.io/npm/v/node-red-contrib-redis.svg)](https://www.npmjs.com/package/node-red-contrib-redis)
[![Node-RED](https://img.shields.io/badge/Node--RED-4.x-red)](https://nodered.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Redis nodes for Node-RED built on ioredis: standalone connections, Redis Cluster,
Sentinel, AWS MemoryDB/ElastiCache-style cluster endpoints, blocking inputs, pub/sub,
streams, command coverage, Lua scripts, and Redis client injection into context.

Use this package when a flow needs Redis as an event source, queue, cache, stream,
coordination point, or custom command target without dropping into a Function node for
every call.

| Cloud-style config                                                                                                                                                                           | Stream consumer groups                                                                                                                                         | Stored Lua scripts                                                                                                                                                   |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ![Redis config editor in Cluster mode with AWS MemoryDB or ElastiCache provider, auth fields, TLS option, and successful Test connection result.](docs/assets/readme/redis-config-cloud.png) | ![Redis in editor configured for XREADGROUP with stream topic, consumer group, consumer name, and JSON parsing.](docs/assets/readme/redis-stream-consumer.png) | ![Redis Lua script editor with Keys set to 1, Stored Script and Block Commands enabled, and Lua code in the editor.](docs/assets/readme/redis-lua-stored-script.png) |

## Install

In the Node-RED editor, open **Menu > Manage palette > Install**, search for
`node-red-contrib-redis`, and install it.

From the command line, install it in your Node-RED user directory and restart Node-RED:

```bash
cd ~/.node-red
npm install node-red-contrib-redis
```

## Quickstart

1. Add a **redis-config** node and choose the connection shape:
   **Single**, **Cluster**, **Sentinel**, or **ConnString** from an environment variable.
2. Use **Test connection** before deploying. JSON-mode passwords are stored in
   Node-RED encrypted credentials; environment-variable mode keeps secrets in the
   environment.
3. Add one of the runtime nodes:
   **redis in** for subscriptions, blocking queues, and `XREADGROUP`;
   **redis out** for writes such as `RPUSH`, `PUBLISH`, `XADD`, and `ZADD`;
   **redis cmd** for broad Redis and module command coverage;
   **redis lua** for atomic Lua scripts; or
   **redis instance** to place an ioredis client in flow/global context.
4. Import an example flow from [`examples/`](examples/) rather than starting from a blank
   canvas.

Useful examples:

- [`redis-set-and-get.json`](examples/redis-set-and-get.json) - basic set/get commands.
- [`redis-list-queue.json`](examples/redis-list-queue.json) - list producer and blocking
  consumer.
- [`redis-priority-queue.json`](examples/redis-priority-queue.json) - sorted-set priority
  queue.
- [`redis-pub-sub.json`](examples/redis-pub-sub.json) and
  [`redis-psubscribe.json`](examples/redis-psubscribe.json) - channels and pattern
  subscriptions.
- [`redis-streams.json`](examples/redis-streams.json) - `XADD` with an `XREADGROUP`
  consumer.
- [`redis-lua-script.json`](examples/redis-lua-script.json) - Lua scripting patterns.

## Nodes

| Node               | Use it for                       | Highlights                                                                                                                                                 |
| ------------------ | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `redis-config`     | Shared connection configuration  | Single Redis, Cluster, Sentinel, AWS MemoryDB/ElastiCache-style endpoints, env JSON, encrypted credential storage, and Test connection.                    |
| `redis-in`         | Redis as an input source         | `BLPOP`, `BRPOP`, `BZPOPMIN`, `BZPOPMAX`, `SUBSCRIBE`, `PSUBSCRIBE`, and `XREADGROUP`; blocking consumers retry with capped backoff until the node closes. |
| `redis-out`        | Focused writes                   | List pushes, publish, `XADD`, and `ZADD`; writes are awaited and failures go through Node-RED error handling.                                              |
| `redis-command`    | General Redis commands           | Runs configured commands through ioredis, supports JSON params, message overrides, and a dedicated connection option for blocking work.                    |
| `redis-lua-script` | Atomic server-side logic         | `EVAL` or stored `SCRIPT LOAD` plus `EVALSHA`, `NOSCRIPT` recovery, Lua editor, library metadata, and optional dedicated connection.                       |
| `redis-instance`   | Advanced Function-node workflows | Stores a live ioredis client in flow or global context under a configured key.                                                                             |

## Configuration Notes

### Standalone

Use the Connection tab for the common case, or JSON like:

```json
{
  "host": "127.0.0.1",
  "port": 6379,
  "db": 0
}
```

Add `username`, `password`, and `tls: {}` when your Redis server requires ACL auth or TLS.
In JSON mode, saved passwords are moved into Node-RED credentials instead of remaining in
the exported flow.

### Redis Cluster

Cluster mode accepts a startup-node array. Username and password values on startup nodes
are also applied to ioredis `redisOptions`, so discovered cluster nodes authenticate too.

```json
[
  {
    "host": "127.0.0.1",
    "port": 7000,
    "username": "node_red",
    "password": "use-an-environment-secret"
  }
]
```

For AWS MemoryDB or ElastiCache cluster configuration endpoints, use the provider in the
Connection tab or include `dnsLookupStrategy: "identity"` in JSON. That enables identity
DNS lookup and TLS for the cluster connection.

```json
[
  {
    "host": "clustercfg.example.memorydb.region.amazonaws.com",
    "port": 6379,
    "username": "node_red",
    "password": "use-an-environment-secret",
    "dnsLookupStrategy": "identity"
  }
]
```

### Sentinel

Sentinel mode uses a master name plus Sentinel endpoints:

```json
{
  "name": "mymaster",
  "sentinels": [
    {
      "host": "127.0.0.1",
      "port": 26379
    }
  ],
  "username": "node_red",
  "password": "use-an-environment-secret"
}
```

If the Sentinel nodes themselves require auth or TLS, use `sentinelUsername`,
`sentinelPassword`, and `sentinelTLS`.

### Environment Variables

Choose **ConnString > Environment variable** and enter the variable name. The value can be
an ioredis connection string, standalone JSON object, cluster startup-node array, or
Sentinel JSON object.

```bash
export NODE_RED_REDIS_OPTIONS='{"host":"127.0.0.1","port":6379}'
```

Environment-variable mode is useful for deployments because secrets stay outside
`flows.json` and outside exported flow snippets.

## Message Patterns

`redis-in` emits Redis data as Node-RED messages. Pub/sub messages use `msg.topic` and
`msg.payload`; pattern subscriptions also include `msg.pattern`; stream consumer groups
include `msg.stream`, `msg.messageId`, and `msg.payload`.

`redis-out` and `redis-command` let incoming messages override configured keys and
arguments. Use Catch nodes around write-heavy flows; Redis command failures are surfaced
through Node-RED's normal error path instead of being swallowed.

For advanced commands or Redis modules, prefer `redis-command` before writing a Function
node. For custom client code that really needs ioredis directly, use `redis-instance`.

## Work with AI Agent

1. Support Claude and Codex
2. Install [supoerpowers plugin](https://github.com/obra/superpowers)
3. Install [codegraph](https://github.com/colbymchenry/codegraph)
4. Enjoy

## Development And Tests

Install dependencies:

```bash
npm install
```

Run the browser editor suite:

```bash
npm run test:playwright
```

Run the full Docker-managed deployment matrix:

```bash
npm test
```

The test matrix covers standalone no-auth/auth Redis, Redis Cluster, Redis Sentinel, and
optional AWS MemoryDB when `MEMORYDB_ENABLED=1` plus endpoint credentials are present in
the environment. See [`docs/TESTING.md`](docs/TESTING.md) for the Docker boundary and
[`docs/NODE_GUIDE.md`](docs/NODE_GUIDE.md) plus
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for implementation details.

## License

[MIT](LICENSE)
