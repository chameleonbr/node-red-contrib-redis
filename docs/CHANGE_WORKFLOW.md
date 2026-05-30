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
