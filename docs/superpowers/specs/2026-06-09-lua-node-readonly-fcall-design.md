# Design: read-only execution and Redis Functions in `redis-lua-script`

Date: 2026-06-09
Status: proposed (awaiting review)
Scope: `redis-lua-script` node only — runtime (`redis.js`), editor (`redis.html`), tests, docs.

## Problem

The `redis-lua-script` node is hardwired to `EVAL`/`EVALSHA` (`redis.js:835,887,903`). It
cannot run the read-only execution variants (`EVAL_RO`, `EVALSHA_RO`) or call Redis Functions
(`FCALL`, `FCALL_RO`) introduced in newer Redis. The goal is **parity/completeness** — keep the
node current with Redis scripting — not a specific flow requirement.

### What is already covered (do not duplicate)

The generic `redis-command` node already exposes `EVAL_RO`, `EVALSHA_RO`, `FCALL`, `FCALL_RO`,
`FUNCTION`, and `SCRIPT` in its dropdown (`redis.html:1770-1886`), and `scripting_commands_spec.js`
already tests the `EVAL`/`EVAL_RO`/`SCRIPT LOAD`+`EVALSHA`/`SCRIPT EXISTS` paths through it. All
`FUNCTION *` and `SCRIPT *` **management** subcommands (delete, dump, flush, kill, list, load,
restore, stats / debug, exists, flush, kill, load) remain reachable there by selecting
`FUNCTION`/`SCRIPT` and passing the subcommand + args in `msg.payload` (ioredis flattens the array).

**This design deliberately does not add management commands to the Lua node.** The Lua node grows
only along its two execution models below; management stays in `redis-command`.

## Approach

Mode + flags, with the command **derived in the runtime** from `(mode, stored, readonly)` — not an
explicit command dropdown. This preserves the existing `Stored` checkbox semantics (`EVALSHA` *is*
"stored") and keeps every saved flow byte-for-byte backward compatible (a node with no new fields
defaults to today's behavior).

The node supports two execution models that mirror each other:

- **Script model** (existing, extended): the editor holds a **script body**; an optional `Stored`
  checkbox pre-loads it with `SCRIPT LOAD` on connect and runs it via `EVALSHA`; a new `Read-only`
  flag selects the `_RO` variant.
- **Function model** (new): the editor holds a **library source**
  (`#!lua name=<lib>` + `redis.register_function(...)`); the node loads it with
  `FUNCTION LOAD REPLACE` on connect and invokes a named function via `FCALL`; `Read-only` selects
  `FCALL_RO`. This is the direct analogue of Script + Stored, with `FUNCTION LOAD` replacing
  `SCRIPT LOAD` and `FCALL <name>` replacing `EVALSHA <sha1>`.

### Command resolution

| mode | stored | readonly | on `ready` | on input | recovery |
|------|--------|----------|-----------|----------|----------|
| script | ✗ | ✗ | — | `EVAL` | — |
| script | ✗ | ✓ | — | `EVAL_RO` | — |
| script | ✓ | ✗ | `SCRIPT LOAD` | `EVALSHA` | `NOSCRIPT` → `EVAL` |
| script | ✓ | ✓ | `SCRIPT LOAD` | `EVALSHA_RO` | `NOSCRIPT` → `EVAL_RO` |
| function | (n/a) | ✗ | `FUNCTION LOAD REPLACE` | `FCALL` | "function not found" → reload → retry once |
| function | (n/a) | ✓ | `FUNCTION LOAD REPLACE` | `FCALL_RO` | "function not found" → reload → retry once |

ioredis exposes `eval_ro`, `evalsha_ro`, `fcall`, `fcall_ro` as methods accepting the same
args-array form already used for `eval`/`evalsha` (verified against ioredis v5.11.0). Called
without a callback they return a promise, so dispatch uses `async`/`await` + `try`/`catch`
(the node convention — no `.then()/.catch()` chains, no callback-style calls). Args remain
`[firstArg, keyval, ...msg.payload]`, where `firstArg` is the script body (`EVAL*`), the sha1
(`EVALSHA*`), or the function name (`FCALL*`).

## Editor changes (`redis.html`)

New `defaults` (added to the existing `server`/`name`/`keyval`/`func`/`stored`/`block`):

- `mode`: `"script"` (default) | `"function"`
- `readonly`: `false` (default)
- `fname`: `""` — the function name to call (Function mode only)

Field behavior:

- **Script mode** (default): shows the Lua editor (`func`), the `Stored Script (EVALSHA)` checkbox,
  and a new `Read-only` checkbox — i.e. today's UI plus one checkbox.
- **Function mode**: shows the Lua editor (now the **library source**) and the `Function name`
  field and `Read-only`; hides the `Stored` checkbox (not applicable to functions). The editor
  label may switch from "Lua Script" to "Lua Library" for clarity (optional polish).
- `func` is the Ace editor's contents in **both** modes (script body vs. library source); toggling
  mode does not clobber it. Only `fname` is Function-mode-specific.
- `oneditresize` resizes the Ace editor only when it is visible.

`RED.library.create` `fields` (currently `name`, `keyval`, `stored`/`block` as get/set objects):
add `mode` and `fname` as plain string fields and `readonly` as a get/set object returning the
string `"true"`/`"false"` — matching the existing checkbox pattern enforced by
`redis_lua_ui_spec.js` (Node-RED calls `text.replace()` on every metadata value, so checkbox
getters must return a string, not a boolean).

**Library-open visibility wrinkle:** when a library entry is opened, Node-RED restores each field
through its `set`/`.val()` *without firing a change event*. So the `mode` field must use a custom
`set` that both sets the value **and** re-applies the Script/Function field visibility (otherwise
opening a saved Function-mode library would leave the `Stored` checkbox showing and the
`Function name` field hidden). The same visibility refresh runs from `oneditprepare` so a node
opened directly (not via the library) also renders correctly.

## Runtime changes (`redis.js`, `RedisLua`)

1. Read the new config fields: `this.mode`, `this.readonly`, `this.fname` (with defaults
   `"script"`, `false`, `""`).
2. **On-`ready` loader** (via the existing `attachStatusListeners(node, client, onReady)` hook):
   - Script + Stored: unchanged — `SCRIPT LOAD` → capture `node.sha1`, status
     `script loaded` / `script not loaded`.
   - Function: `FUNCTION LOAD REPLACE <func>` → status `library loaded` / `library not loaded`
     (optionally capture the returned library name for status/debug).
   - Otherwise (Script, not Stored): no loader.
3. **On `input`:** keep the existing `keyval > 0 ⇒ payload must be array` guard and the
   "result in `msg.payload`" contract. Derive the command per the table:
   - Script mode dispatches `eval` / `eval_ro` / `evalsha` / `evalsha_ro`. The stored variants keep
     the `NOSCRIPT` fallback, with `EVALSHA_RO` falling back to `EVAL_RO` (not `EVAL`) to preserve
     read-only semantics.
   - Function mode dispatches `fcall` / `fcall_ro` with `[fname, keyval, ...payload]`. On an error
     whose message indicates a missing function/library (matched case-insensitively on
     `"function not found"`), re-run `FUNCTION LOAD REPLACE <func>` and retry the `FCALL` once;
     a second failure calls `done(err)`. Set `node.command` for status parity with the script path.
4. **Function-mode misconfiguration:** an empty/whitespace `func` in Function mode is a
   configuration error — surface a red status and do not attempt `FCALL` (the node always owns its
   library; there is no "call an externally loaded function" path).
5. Close handler unchanged (remove listeners, clear status, release connection).

## Backward compatibility

Saved nodes carry no `mode`/`readonly`/`fname`. Defaults (`script` / `false` / `""`) reproduce
today's behavior exactly: `EVAL` when unstored, `SCRIPT LOAD` + `EVALSHA` with `NOSCRIPT` → `EVAL`
when stored. No flow-JSON migration is required. Public node type, config field names, message
fields, and editor ids are preserved; only additive fields and one additive checkbox/select are
introduced.

## Testing (TDD — write failing tests first)

Template assertions — extend `test/redis_lua_ui_spec.js`:

- `defaults`/template include `mode`, `readonly`, `fname`; the template has `#node-input-mode`,
  `#node-input-readonly`, and `#node-input-fname`.
- (`RED.library.create` `fields` assertions live in the Library save / open round-trip section
  below, since that is what they protect.)

Runtime assertions — drive the `redis-lua-script` node directly (extend
`test/scripting_commands_spec.js` or add a sibling spec; live Redis required, like the rest):

- **Script + read-only** (`EVAL_RO`): unstored read-only script reads a pre-set key and returns it;
  assert `node.command === "eval_ro"`.
- **Stored + read-only** (`EVALSHA_RO`): happy path after the on-ready `SCRIPT LOAD`; then
  `SCRIPT FLUSH` out-of-band and re-fire input to exercise `NOSCRIPT` → `EVAL_RO` fallback.
- **Function** (`FCALL`): node configured with a library source registering a function and a
  matching `fname`; after the library loads, input returns the function's result.
- **Function read-only** (`FCALL_RO`): same against a read-only-flagged function.
- **Function recovery**: after the node loads its library, run `FUNCTION FLUSH` out-of-band, fire
  input, and assert the node reloads via `FUNCTION LOAD REPLACE` and the retried `FCALL` succeeds.
- **Function misconfiguration**: empty library source in Function mode surfaces a red status / error
  rather than attempting `FCALL`.

`redis-command` already covers the raw `EVAL_RO`/`SCRIPT`/etc. paths, so those are not duplicated
here.

### Library save / open round-trip (Open Library… / Save to Library…)

Because the new fields join `RED.library.create`, both directions of the library feature must be
covered:

- **Static parse** (`test/redis_lua_ui_spec.js`): `RED.library.create` `fields` include `mode` and
  `fname` (string) and `readonly` (get/set object returning `"true"`/`"false"`) — so all three are
  written on save and restored on open. The existing checkbox-string assertion already guards the
  `readonly` getter shape.
- **Real editor — Save** (`test/playwright/redis-editor.spec.js`): extend the existing
  "library save … checkbox metadata" test so that, with the node in Function mode, the saved
  `.lua` file also contains `// mode: function`, `// readonly: true`, and `// fname: <name>`
  alongside the existing `name`/`keyval`/`stored`/`block` metadata and the library source body.
- **Real editor — mode visibility** (new Playwright test): import a saved Function-mode node, open
  its editor, and assert the function fields render (Function name shown, `Stored` hidden, editor
  label "Lua Library"), then toggle the mode select both ways and assert visibility flips. This
  verifies `updateLuaModeVisibility` for the oneditprepare (saved-state) and change-event paths in a
  real browser.
  - Implementation note: an earlier attempt drove the real **Open Library…** dialog to assert the
    full field round-trip, but Node-RED indexes the library at startup, so a file written to disk
    after boot is not listed (the Save path works because it POSTs through the API). Rather than
    fight the cached library index + tree expansion (fragile, version-specific markup), the
    round-trip is covered by the static UI spec (the `mode`/`readonly`/`fname` get/set field shapes)
    plus the Save test (metadata written), and the browser test covers the visibility behavior
    deterministically.

### Deployment coverage (all four deployments)

This change must be validated against **every** Redis deployment the suite supports — single
(both `single-noauth` and `single-auth`), Cluster, Sentinel, and MemoryDB — because script and
function loading behave differently under sharding and discovery.

- **Single (`single-noauth` + `single-auth`):** the standalone runtime tests above
  (`scripting_commands_spec.js`) run automatically in both standalone stages. No extra work.
- **Cluster (`cluster-auth`):** extend `redis_cluster_deployment_spec.js` alongside its existing
  EVALSHA "Lua fallback" test to drive the `redis-lua-script` node through `EVAL_RO`/`EVALSHA_RO`
  and the `FCALL`/`FCALL_RO` function model, using **hash-tagged same-slot keys** (`{tag}` like the
  `cluster-prone` helper) and asserting the deliberate cross-slot failure surface.
- **Sentinel (`sentinel-auth`):** mirror the same `redis-lua-script` coverage in
  `redis_sentinel_deployment_spec.js` against the discovered primary, including a failover/reconnect
  case that confirms the on-`ready` `SCRIPT LOAD` / `FUNCTION LOAD REPLACE` re-runs after reconnect.
- **MemoryDB (`memorydb_deployment_spec.js`, opt-in via `MEMORYDB_*`):** add the same coverage,
  but **gate `FCALL`/`FUNCTION LOAD` on engine support** — Redis Functions are not available on all
  MemoryDB engine versions, so detect-and-skip (do not hard-fail) when unsupported; `EVAL_RO` /
  `EVALSHA_RO` should still be exercised.
- Optionally add `EVAL_RO`/`EVALSHA_RO` (and `FCALL` where supported) to the shared
  `test/helpers/cluster-prone.js` `COMMANDS` matrix so the `redis-command` variants get the same
  multi-deployment sweep that `EVAL`/`EVALSHA` already get.

Cluster/MemoryDB locality caveat to assert explicitly: `SCRIPT LOAD` and `FUNCTION LOAD` are
**node-local**, while `EVALSHA*`/`FCALL*` route by key slot — so a call can land on a node that
never received the load. The node's load-on-`ready` plus its recovery paths (`NOSCRIPT` → `EVAL*`,
"function not found" → `FUNCTION LOAD REPLACE` → retry) are exactly what make these succeed under
sharding; the deployment tests should prove that recovery, not just the happy path.

If the `cluster-prone` matrix or any deployment spec gains/loses cases, update `docs/TESTING.md`,
`docs/REFERENCE_MAP.md`, and the maintainer skill file in the same change (per the repo regression
strategy).

## Documentation

The split between *execution* (this node) and *management* (`redis-command`) must be stated
explicitly in user-facing help and in the maintainer docs, on **both** node sides so users find it
from either direction.

- `redis.html` **`redis-lua-script` help text**: document `Mode` (Script/Function), `Read-only`,
  `Function name`, the required `#!lua name=<lib>` shebang for the library source, the
  `FUNCTION LOAD REPLACE`-on-deploy behavior and missing-function recovery. Add an explicit
  **"Managing scripts and functions"** note stating that this node only *executes* scripts and
  functions, and that the management/admin subcommands are run through the **`redis-command`** node:
  `SCRIPT DEBUG/EXISTS/FLUSH/KILL/LOAD` and `FUNCTION DELETE/DUMP/FLUSH/KILL/LIST/LOAD/RESTORE/STATS`
  (select `SCRIPT`/`FUNCTION` and pass the subcommand + args in `msg.payload`).
- `redis.html` **`redis-command` help text**: add a short note that `FUNCTION` and `SCRIPT`
  management subcommands are issued here (pick the command, put the subcommand and its args in
  `msg.payload`, e.g. `["LOAD", "<library source>"]` / `["EXISTS", "<sha1>"]`), with one worked
  example — so a user looking at the command node also learns this is the management home.
- `docs/NODE_GUIDE.md` **`redis-lua-script` section**: replace the four-line behavior list with the
  command-resolution table and the function-load-on-ready / recovery behavior, and add an explicit
  line: management (`FUNCTION *` / `SCRIPT *`) stays in `redis-command`, this node is execution-only.
- `docs/NODE_GUIDE.md` **`redis-command` section**: add a matching line under "Use this node for"
  that `FUNCTION`/`SCRIPT` management subcommands belong here, cross-referencing the Lua node.
- `README.md` (user-facing landing page):
  - **Nodes table** — update the `redis-lua-script` Highlights cell (currently
    "`EVAL` or stored `SCRIPT LOAD` plus `EVALSHA`, `NOSCRIPT` recovery, …") to also mention the
    read-only variants (`EVAL_RO`/`EVALSHA_RO`) and the Function mode (`FUNCTION LOAD REPLACE` +
    `FCALL`/`FCALL_RO`). Optionally note in the `redis-command` row that it is the home for
    `FUNCTION`/`SCRIPT` management.
  - **Examples list** — add a bullet for the new `examples/redis-fcall.json` (Redis Functions /
    `FCALL` pattern), next to the existing `redis-lua-script.json` entry.
  - **"Message Patterns" / advanced-commands note** — extend the existing
    "prefer `redis-command` before writing a Function node" paragraph with the explicit
    execution-vs-management split: `redis-lua-script` *executes* scripts/functions, while
    `FUNCTION *` / `SCRIPT *` *management* subcommands run through `redis-command`.
  - Quickstart (optional): broaden the "**redis lua** for atomic Lua scripts" line to mention
    Redis Functions.
- **Example flow `examples/redis-fcall.json`** (new): demonstrates the Function model end to end.
  Examples are auto-discovered from `examples/`, so no `package.json` change is needed; mirror the
  structure of the existing `examples/redis-lua-script.json` (`redis-config` + `inject` +
  `redis-lua-script` + `debug`, with a `comment`/description node). The `redis-lua-script` node is
  configured in Function mode:
  - `mode: "function"`, `readonly: false`, `keyval: 1`
  - `func`: a library source with the required shebang, e.g.
    `#!lua name=mylib` + `redis.register_function('myfunc', function(keys, args) return redis.call('GET', keys[1]) end)`
  - `fname: "myfunc"`
  - the `inject` supplies the keys/args payload array (e.g. `["somekey"]`), the `debug` shows
    `msg.payload`, and the description node explains that the node `FUNCTION LOAD REPLACE`s the
    library on deploy and then `FCALL`s `fname`.
  - optionally a second branch showing the read-only variant (`readonly: true` → `FCALL_RO`).
  - The Playwright/example smoke step (if any) should still load cleanly; carry the new
    `mode`/`readonly`/`fname` fields so the example reflects the shipped node shape.

## Decisions captured during brainstorming

- Motivation: parity/completeness; no specific flow depends on it yet.
- Node scope: Tier 2 — add read-only execution variants and an `FCALL`/`FCALL_RO` function mode;
  `FUNCTION *` / `SCRIPT *` management stays in `redis-command`.
- Function loading is owned by the node, internally, via `FUNCTION LOAD` — analogous to stored
  scripts' `SCRIPT LOAD`.
- On-ready load uses `FUNCTION LOAD REPLACE` (self-healing; pushes library edits on redeploy).
- An empty library source in Function mode is treated as a misconfiguration (no external-function
  call path).
- Missing-function errors mid-run trigger one reload + retry, symmetric to `NOSCRIPT` → `EVAL`.
- Must be tested across all four deployments — single, Cluster, Sentinel, MemoryDB — with MemoryDB
  function coverage gated on engine support.
- The execution-vs-management split must be stated explicitly in user help and maintainer docs on
  both the `redis-lua-script` and `redis-command` sides: `FUNCTION *` / `SCRIPT *` management runs
  through `redis-command`; the Lua node executes only.

## Out of scope

- `FUNCTION` / `SCRIPT` management subcommands as first-class Lua-node features (use `redis-command`).
- Any change to connection ownership, blocking/`block` semantics, or other node types.
- Restructuring the single-module deployment model.
