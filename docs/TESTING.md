# Testing

This repository uses Mocha with `node-red-node-test-helper`, but the tests also require a real Redis server.

## Prerequisite

Start Redis locally on:
- host: `127.0.0.1`
- port: `6379`

The current specs and cleanup helper assume that address directly.

## Main command

Run all tests:
```bash
npm test
