# redis-config secret storage via Node-RED credentials — design

- **Date:** 2026-06-04
- **Branch:** `claude-review`
- **Status:** Approved design, pending implementation plan
- **Scope:** `redis-config` runtime + editor. Change #3 of three reliability/security items
  (#1 redis-out error propagation and #2 redis-in recovery are done). Largest of the three.

## Problem

`redis-config` stores the whole connection blob — including passwords — as a normal
`defaults.options` property (`redis.html:198-213`). For the JSON `optionsType`, that blob is
written verbatim into `flows.json`, so **passwords are persisted in plaintext** and travel with
flow exports. The editor serializes the single/cluster/sentinel password fields into that blob
(`buildSingleOptions`/`buildClusterOptions`/`buildSentinelOptions`), and the runtime parses it
in `evaluateConnectionOptions` (`redis.js:265-298`, `RedisConfig` `redis.js:370-382`).

The `env` `optionsType` already keeps secrets out of `flows.json` (the value is read from the
environment at runtime), so it needs no change.

## Goal

For JSON mode, no plaintext secret in `flows.json`: passwords are stored in Node-RED's
encrypted credential store and merged back into the options at runtime. Cover all three modes
(single, cluster, sentinel). Keep existing deployed flows and the topology tests working.

## Decision

Use a single **`text`-type** credential named `secrets` holding a JSON map of all secret values.
`text` (not `password`) was chosen deliberately: it is still encrypted at rest in the
credentials file and absent from `flows.json` (the actual goal), while remaining readable by the
editor — which preserves the current round-trip UI, per-node cluster passwords, and Test
Connection with minimal change. The decrypted secret reaching the editor during edit matches
today's behavior (the password already rides inside the `options` sent to the editor), so this
is a strict improvement, not a new exposure. (The `password`-type alternative — write-only,
never sent to the editor — was rejected for this iteration because it forces blank-on-reopen UX,
collapses per-node cluster passwords to one shared password, and cannot partially update a
multi-secret map.)

Secrets are diverted out of the options blob only at the **save boundary** and merged back at
**load** and at **runtime**; the live form↔editor sync and the `buildX*Options` builders are
left unchanged.

## Secret map shape

`credentials.secrets` is a JSON string:

```json
{ "password": "...", "sentinelPassword": "...", "nodes": ["pw0", "pw1"] }
```

- `password` — single mode, or the sentinel data-node password.
- `sentinelPassword` — sentinel mode only.
- `nodes[i]` — cluster startup-node `i`'s password (AWS provider = a single node, `nodes[0]`).

Only non-empty secrets are included.

## Shared helpers (mirrored in `redis.js` and the `redis-config` editor script)

Both copies must stay in sync; each will carry a comment saying so.

`extractSecrets(options)` → `{ stripped, secrets }`:
- array (cluster): `secrets.nodes = options.map(n => (n && n.password) || "")` (include only if any
  non-empty); `stripped` = each node copied without `password`.
- object with `sentinels` (sentinel): `secrets.password = options.password` and
  `secrets.sentinelPassword = options.sentinelPassword` when present; `stripped` = copy without
  those two keys.
- plain object (single): `secrets.password = options.password` when present; `stripped` = copy
  without `password`.
- anything else (e.g. a string): `stripped = options`, `secrets = {}`.

`mergeSecrets(options, secrets)` → options with secrets re-injected by the same paths. A secret
is applied only when non-empty, so a legacy password still in `options` is preserved when no
credential is set, and a credential value overrides when present. Empty/absent `secrets` → return
options unchanged.

## Runtime (`redis.js`)

- Register with credentials:
  ```js
  RED.nodes.registerType("redis-config", RedisConfig, {
    credentials: { secrets: { type: "text" } },
  });
  ```
- In `RedisConfig`, after `this.options = evaluateConnectionOptions(...)` and only when
  `this.optionsType !== "env"`:
  ```js
  this.options = mergeSecrets(this.options, parseSecrets(this.credentials && this.credentials.secrets));
  ```
  where `parseSecrets` is a guarded `JSON.parse` returning `{}` on empty/invalid input.
- **Backward compatible:** no credential → `mergeSecrets` is a no-op, so a legacy password in
  `options` still authenticates. Every existing topology/standalone test (password-in-options)
  and every deployed flow is unaffected.

## Editor (`redis.html`, `redis-config` script) — two touch-points

- Define `extractSecrets`/`mergeSecrets` at the top of the `redis-config` `<script>` so both
  `oneditprepare` and `oneditsave` can use them.
- Add `credentials: { secrets: { type: "text" } }` to the `registerType` definition and a hidden
  `<input type="hidden" id="node-config-input-secrets">` to the template so Node-RED binds and
  encrypts the credential.
- `oneditprepare`: before `populateFormFromOptions(initialOptions)`, merge the loaded credential
  into the options: `initialOptions = mergeSecrets(initialOptions || {}, parseSecrets($("#node-config-input-secrets").val()))`.
  The subsequent `syncConnectionToEditor()` then shows the with-password options during editing,
  exactly as today. (Everything else in `oneditprepare` is unchanged.)
- `oneditsave`: for JSON mode, take the final options object (parse the editor value, falling
  back to the hidden `#node-config-input-options` value as the current code already does), run
  `extractSecrets`, write the **stripped** options JSON to `#node-config-input-options` and
  `JSON.stringify(secrets)` to `#node-config-input-secrets`. For `env` mode, set
  `#node-config-input-secrets` to `""` (no credential) and keep the raw value as today.

`buildSingleOptions`/`buildClusterOptions`/`buildSentinelOptions` and the live sync are unchanged
— passwords remain in the in-memory options during editing and are only stripped at save.

## ConnString tab

Shows the password while editing (consistent with the secret already being in the editor), but
it is stripped on save, so the saved `options` in `flows.json` contains no secret. Pasting JSON
with a password into the ConnString editor works: `extractSecrets` on save moves it to the
credential and strips it from the stored options.

## Test Connection

No change. During editing the in-memory/editor options still carry the password, so the existing
`buildConnectionTestPayload` posts a complete options object and the admin endpoint authenticates
as it does today.

## Migration

Automatic on next resave. A legacy config (password in `options`, no credential) loads with the
password visible (`mergeSecrets` is a no-op because the credential is empty and the options
already hold it); on **save**, `extractSecrets` moves the password into the credential and strips
it from `options`. Un-opened legacy configs keep working via runtime backward-compat. No bulk
migration step.

## Testing

New spec `test/redis_credentials_spec.js` (add it to the spec lists in `REFERENCE_MAP.md`,
`TESTING.md`, and the maintainer skill per change discipline). Most cases need **no live Redis** —
they assert the constructor-time merge on the loaded config node (`RedisConfig` sets `this.options`
without opening a connection, since `getConn` is only called by consumer nodes):

1. **single merge:** load a `redis-config` with options `{host,port}` (no password) and
   `helper.load(redisNode, flow, { cfg: { secrets: '{"password":"s3cret"}' } }, cb)`; assert
   `helper.getNode("cfg").options.password === "s3cret"`.
2. **cluster merge:** options `[{host,port},{host,port}]` + `secrets {"nodes":["a","b"]}`; assert
   `options[0].password==="a"` and `options[1].password==="b"`.
3. **sentinel merge:** options `{sentinels:[...],name}` + `secrets {"password":"p","sentinelPassword":"sp"}`;
   assert both merged.
4. **back-compat:** options containing a password, no credential; assert `options.password`
   unchanged (legacy preserved, merge is a no-op).
5. **env untouched:** an `env` config is not merged (secret stays in the environment).
6. **end-to-end auth (single-auth only):** options with username but no password + a `secrets`
   credential carrying the password, plus a `redis-command` GET; assert it authenticates. Guard
   with `if (!process.env.REDIS_PASSWORD) this.skip();` so it is a no-op in the no-auth stage.

Editor (Playwright, `test/playwright/redis-editor.spec.js`): enter a password and save; assert the
persisted node's `options` contains no password while its credential is set; reopen and confirm
the password round-trips into the form. The Playwright harness already sets `credentialSecret`.

Back-compat regression: the full existing suite (password-in-options) must stay green — that is
the primary guard that the runtime still honors legacy flows.

## Documentation (Definition of Done)

- `redis.html` `redis-config` help: note that in JSON mode passwords are stored in Node-RED's
  encrypted credentials (not in the exported flow), and that `env` mode keeps the secret in the
  environment.
- `docs/NODE_GUIDE.md` `redis-config`: describe the secret extract/merge, the credential, and the
  resave migration.
- `docs/ARCHITECTURE.md` `redis-config`: note secrets are stored as a `text` credential and merged
  into options at runtime; `buildX*Options` and live sync unchanged.
- `docs/REFERENCE_MAP.md`, `docs/TESTING.md`, and the maintainer skill: add `redis_credentials_spec.js`
  to the spec lists.
- `README.md`: update any wording that implies passwords are stored in the flow.

## Versioning

Additive and backward-compatible. Package version stays `2.0.0`. Release note: passwords entered
in JSON mode now persist in the encrypted credential store instead of `flows.json`; existing
flows keep working and migrate automatically when a config node is reopened and saved.
