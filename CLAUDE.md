# CLAUDE.md

This project is a Node-RED custom node package implemented in JavaScript ES6+ with Node.js v24 as the working runtime target, Node-RED v4.1.10 as the editor/runtime target, and ioredis v5.11.0 as the Redis client.
The package registers one Node-RED module entrypoint in `package.json` and is implemented primarily in `redis.js` and `redis.html`.

## Project purpose

The package provides Redis integration nodes for:

- shared configuration
- blocking and event-style input
- output/write commands
- generic commands
- Lua scripts
- context injection of a Redis client instance

Treat this codebase as a Node-RED node package first, not as a generic Redis SDK.

## Read this first

When starting work, read in this order:

1. `docs/REFERENCE_MAP.md`
2. `docs/ARCHITECTURE.md`
3. `docs/NODE_GUIDE.md`
4. `docs/CHANGE_WORKFLOW.md`
5. `docs/TESTING.md`

For Lua/library-related changes, also read:

- `test/redis_lua_ui_spec.js`
- the `redis-lua-script` section in `docs/NODE_GUIDE.md`

## Source map

Primary files:

- `package.json` — npm metadata, scripts, dependency versions, Node-RED registration
- `redis.js` — all runtime node implementations and connection lifecycle logic
- `redis.html` — all editor UI, help text, command lists, and validation
- `test/**/*_spec.js` — behavior and regression tests
- `test/helpers/cleanup.js` — Redis key cleanup for tests
- `examples/*.json` — sample flows

## Runtime architecture rules

The runtime is implemented as one CommonJS Node-RED module exported from `redis.js`.
Keep that deployment model unless a human explicitly approves a structural split.

Current runtime node types:

- `redis-config`
- `redis-in`
- `redis-out`
- `redis-command`
- `redis-lua-script`
- `redis-instance`

Connection management is shared and stateful:

- `connections` and `usedConn` are module-level registries
- some nodes intentionally share connections
- blocking or subscriber-style flows intentionally use dedicated connections
- shutdown behavior is different for blocking vs non-blocking nodes

Do not rewrite connection ownership casually. Small connection-id changes can break pub/sub, blocking commands, close handlers, and status tests.

## Editing policy

Create test case to reproduce bug before apply patch and re-run test to confirm bug fixed.
Prefer the smallest safe change that solves the requested issue.
Do not refactor for style alone.
Do not rename public node types, config fields, message fields, or editor ids unless required and covered by tests.
Keep existing flow JSON compatibility wherever possible.

Safe default approach:

1. locate the exact node type and code path
2. read the matching tests
3. add or update the narrowest test that proves the change
4. change runtime logic
5. change editor/help text only if user-visible behavior changed
6. run the relevant tests, then the full test suite

## Coding conventions

Follow the repository formatter, not personal preference:

- semicolons on
- double quotes
- trailing commas `es5`
- print width 100
- space width 2

Use modern JavaScript, but keep compatibility with the current code style:

- CommonJS module format
- `function` for Node-RED constructors
- `let`/`const` inside runtime logic
- explicit `done(err)` or `node.error(err, msg)` paths
- avoid hidden control flow

## Node-RED conventions

Every runtime constructor must call `RED.nodes.createNode(this, config)` first.
Config-node references should be resolved with `RED.nodes.getNode(...)`.
Input handlers should preserve `msg` and use `send`/`done` correctly.
Close handlers must clean up listeners, clear status, and release Redis connections.

Editor changes must preserve:

- property names in `defaults`
- typedInput wiring and hidden type fields
- help text consistency
- existing element ids used by tests and library integration
- use Node.js native async/await for async functions

## Redis and ioredis conventions

Assume Redis connections are long-lived and failure-prone.
Always think about:

- ready/error/reconnecting/end states
- subscriber mode restrictions
- blocking command shutdown
- JSON serialization/parsing behavior
- Redis stream argument shape
- Lua `NOSCRIPT` recovery
- cluster vs non-cluster construction

Prefer existing ioredis usage patterns already present in this branch before introducing new client APIs.

## Testing expectations

Run:

- `npm test`

`npm test` owns Redis through Docker: it checks Docker, starts one deployment at a time,
runs the matching Mocha specs, and tears the deployment down with volumes. It can fall back
to `sudo -n docker` when Docker was just installed and group membership has not refreshed. Use
`npm run test:mocha -- <spec>` only for targeted iteration when you have already started a
compatible Redis yourself. MemoryDB tests are opt-in through environment variables only;
never commit MemoryDB endpoints or credentials. See `docs/TESTING.md` for the full
environment boundary.

Before committing, also account for:

- Husky pre-commit calling `npm test`
- lint-staged formatting staged files with Prettier

If you change behavior, update or add tests in the matching spec file instead of relying on manual reasoning.

## Documentation expectations

When user-visible behavior changes:

- update the relevant help text in `redis.html`
- update or add an example flow if it improves discoverability
- update the matching document under `docs/`

When features are added, behavior changes, tests move, examples drift, or maintenance rules
change, update the relevant docs and agent guidance in the same change. Keep agent-facing
docs concise and factual.
The detailed procedures belong in `docs/`, while this file should stay high-signal.

## Known caution areas

Read the matching code and tests before touching:

- connection sharing keys
- shutdown and `quit()`/`disconnect()` behavior
- `redis-in` blocking loops
- `xreadgroup` object vs flat payload mapping
- `xadd` payload normalization
- `zadd` payload normalization
- Lua stored-script library metadata and checkbox persistence
- context storage in `redis-instance`
- config option evaluation from typedInput / env / JSON / JSONata

## Output quality bar

Any proposed change should be:

- minimal
- branch-specific
- test-backed
- performance-aware
- reliable on reconnect/shutdown
- consistent between runtime and editor
- understandable by the next maintainer

If a requested change appears to require broader redesign, explain why before changing architecture.
