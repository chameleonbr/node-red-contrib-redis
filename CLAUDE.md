# node-red-contrib-redis

This is Node-RED custom developed node that provides Node-RED accessing to Redis/Valkey with pub/sub, list, lua scripting and all other commands support.

## Tech Stack

- **Runtime:** Node.js >= v22, Node-RED >= v4.1.10
- **Library:** ioredis >= 5.11.0
- **In-memory database** Redis >= 8.0, Valkey

## Architecture

### Node Registration Pattern

All nodes follow a standard Node-RED registration pattern:

1. **Module export function** receives `RED` runtime object
2. **Node constructor function** receives config from editor and calls `RED.nodes.createNode(this, config)`
3. **Registration** via `RED.nodes.registerType(type, constructor, options)`
4. **HTML counterpart** (same filename but `.html`) defines editor UI, help text, and default values

### Shared Configuration: redis-config

All Redis nodes depend on a `redis-config` node that stores remote Redis/Valkey service information:

- Single instance deployment: domain name or IP address and port number
  or
- AWS memorydb cluster deployment: endpoint like clustercfg.xxx.bchgcd.memorydb.ap-southeast-2.amazonaws.com:6379
  The 2 deployment types above should have common options:
- TLS/SSL
- Username
- Password

## Common Commands

```bash`
npm install # Install dependencies
npm test # Run tests
npx lint-staged # Pre-commit linting and formatting
cd ~/.node-red && npm install /path/to/this/repo # Test node locally
codegraph init -i # Run this command before scanning files

```

## JavaScript Code Quality
- Use JavaScript, not CommonJS
- Use modern, standard JavaScript ES6+ features
- Async Patterns: Use `async/await` exclusively. Do not chain `.then()` or `.catch()`.
- Add easy to understand comments on complex or critical code

## ES6 Rules
### 1. Variable Declarations
- **Rule**: Use `const` by default.
- **Rule**: Use `let` only if reassignment is explicitly required.
- **Rule**: Never use `var` due to its unpredictable function-scoping behavior.

### 2. Function Writing
- **Rule**: Use arrow functions (`() => {}`) for short logic and inline callbacks.
- **Rule**: Avoid arrow functions inside object methods if you rely on a dynamic `this` context.
- **Rule**: Define default parameter values directly in the function signature instead of checking for `undefined`.

### 3. Strings & Formatting
- **Rule**: Use template literals with backticks (`` ` ``) for multi-line strings and dynamic value insertion.
- **Rule**: Do not use the `+` operator for basic string concatenation.

### 4. Data Extraction & Assignment
- **Rule**: Use object and array destructuring to break down values into distinct variables.
- **Rule**: Use shorthand object property syntax when the property name matches the variable name (`const user = { name };`).

### 5. Collection Handling
- **Rule**: Use the spread operator (`...`) to clone or combine arrays and objects without mutating original data.
- **Rule**: Use rest parameters (`...args`) to gather multiple trailing function arguments into a neat array.
- **Rule**: Rely on built-in array methods like `.map()`, `.filter()`, and `.reduce()` over standard `for` loops for data manipulation.

## Node-RED Core Rules & Architecture
- **Strict Payload Invariance:** Never replace `msg.payload` with a completely different data type without explicitly passing a tracking property or metadata payload.
- **Property Lifecycle:** Retain incoming properties (like `msg._msgid` and custom metadata) unless explicitly instructed to strip them.
- **Context Scope Overuse:** Avoid writing persistent state variables to `global` or `flow` contexts inside a standard Function node unless absolutely necessary. Prefer stateless processing or explicit `context` storage.
- **Fail-Safe Streams:** Every custom node or complex function block must include a clear `try/catch` block that accurately surfaces errors to `node.error(err, msg)` to trigger Node-RED catch nodes.

## Node-RED Style & Formatting
- **Naming Conventions:**
  - Custom node properties: camelCase.
  - Node display names: Sentence case describing the exact action (e.g., "Format sensor data" instead of "function").
  - Input/Output topics: snake_case or slash-delimited hierarchies (e.g., `device/status`).
- **Asynchronous Patterns:** Async functions, external API requests, or timeouts *must* explicit call `node.send(msg)` instead of returning a naked object. Use `node.done()` to release execution slots.

## Common Pitfalls to Prevent
- **Uncaught Loops:** Do not emit a mutated message back onto an identical input topic without passing control flags or conditional checks.
- **Blocking Thread Pools:** Keep computation inside individual JavaScript Function nodes light. Offload intense parsing to exterior services via HTTP/MQTT nodes.
- **Blind JSON Parsing:** Always validate standard buffers or string inputs before passing them into a `JSON.parse()` wrapper.

## Node-RED Node Structure

- `redis.js` — node logic registered with Node-RED
- `redis.html` — editor UI definition

## References

- Node-RED node general guidence: https://nodered.org/docs/creating-nodes/
- Node.js testing best practices: https://github.com/goldbergyoni/nodejs-testing-best-practices
```
