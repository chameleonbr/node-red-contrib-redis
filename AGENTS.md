# AGENTS.md

## Working model

This is a Node-RED custom node package.
The main implementation lives in:

- `redis.js`
- `redis.html`

Treat runtime and editor behavior as a paired system.
A change is incomplete if runtime, editor validation, help text, tests, examples, and docs drift apart. When features are added or behavior changes, update the relevant docs and agent guidance in the same change.

## Agent-specific guidance

Codex starts from this `AGENTS.md`.
Claude starts from `CLAUDE.md` and `.claude/skills/node-red-contrib-redis-maintainer/SKILL.md`.

This repository also exposes `.codex/skills/node-red-contrib-redis-maintainer/SKILL.md` as a symlink to the shared maintainer skill text for environments that load repo-local Codex skills.

## Essential startup checklist

Before editing:

1. Read `docs/REFERENCE_MAP.md`.
2. Read the node-specific section in `docs/NODE_GUIDE.md`.
3. Read the matching tests for the behavior you plan to change.
4. Check `docs/CHANGE_WORKFLOW.md` for safe-edit rules.
5. Check `docs/TESTING.md` for verification steps.

## Bug Fixing

Create test case to reproduce bug before apply patch and re-run test to confirm bug fixed.

## Commands

Install dependencies:

```bash
npm install
```

Run the Docker-managed deployment test suite:

```bash
npm test
```

Run a targeted Mocha spec only when you have already started a compatible Redis yourself:

```bash
npm run test:mocha -- test/redis_in_spec.js
```
