# Change Workflow

Use this workflow for safe changes in current branch.

## Core principle

Minimize change surface.
In this repository, reliability comes from preserving existing Node-RED and ioredis behavior while changing only the narrow path needed for the task.

## Standard workflow

### Understand the request
Identify:
- affected node type
- runtime behavior
- editor behavior
- expected message contract
- shutdown/status implications

### Read before editing
Always read:
- the exact node constructor in `../redis.js`
- the exact editor definition in `../redis.html`
- the matching spec file

### Write the test first when possible
Prefer a regression-style test that:
- fails before the change
- passes after the change
- isolates one behavior

### Implement the minimum fix
Prefer:
- a narrow branch in existing logic
- a local helper
- keeping existing field names
- existing serialization patterns

Avoid:
- unrelated cleanup
- file splits
- style-only edits
- renaming ids or properties

### Update user-facing documentation
If behavior changes:
- update help text in `../redis.html`
- update examples when useful
- update the relevant note in `docs/`

### Verify
Run:
```bash
npm test
```

A real Redis server must be listening on `127.0.0.1:6379` or the suite cannot run.
Run the targeted spec first, then the full suite. Husky also runs `npm test` on
pre-commit, so a failing suite (or missing Redis) will block your commit.

## Definition of Done

A change is complete only when runtime, editor, help text, tests, **and docs** agree.
Before you consider the task finished, confirm you updated everything the change touched:

- changed a node's runtime behavior → update its section in `NODE_GUIDE.md`
- changed a connection-id, refcount, or shutdown path → update `ARCHITECTURE.md` and the
  Connection-id quick reference in `REFERENCE_MAP.md`
- changed user-visible behavior → update the `data-help-name` help block in `../redis.html`
- added, renamed, or removed a test file → update the spec list in `REFERENCE_MAP.md`,
  `TESTING.md`, and the skill file
- added a node type or a `defaults` field → update the source map in `../CLAUDE.md` and
  the public `../README.md`
- made a pattern easier to discover → add or update an example flow under `../examples/`

If you are unsure whether a doc is affected, grep the docs for the symbol or node type you
changed and check each hit.

## Worked example

A good template to imitate: commit `c8625d0` "Fall back to EVAL on NOSCRIPT for stored Lua
scripts."

1. Symptom: a stored Lua script fails with `NOSCRIPT` after Redis restarts or flushes its
   script cache, because the cached SHA1 is gone.
2. Locate: `RedisLua` input handler in `../redis.js`; matching coverage in
   `../test/scripting_commands_spec.js` and `../test/redis_status_spec.js`.
3. Narrow change: in the `evalsha` error callback, detect `NOSCRIPT` and fall back to
   `runWithEval()`, which resends the body and re-caches it under the same SHA1. No new
   fields, no connection changes.
4. Verify: targeted scripting spec, then `npm test`; confirm status/shutdown unaffected.

Notice what it did **not** do: no refactor of the connection logic, no renamed fields, no
unrelated cleanup. That is the bar for every change here.
