# AGENTS.md

## Working model

This is a Node-RED custom node package.
The main implementation lives in:
- `redis.js`
- `redis.html`

Treat runtime and editor behavior as a paired system.
A change is incomplete if runtime, editor validation, help text, tests, and examples drift apart.

## Essential startup checklist

Before editing:
1. Read `docs/REFERENCE_MAP.md`.
2. Read the node-specific section in `docs/NODE_GUIDE.md`.
3. Read the matching tests for the behavior you plan to change.
4. Check `docs/CHANGE_WORKFLOW.md` for safe-edit rules.
5. Check `docs/TESTING.md` for verification steps.

## Commands

Install dependencies:
```bash
npm install
