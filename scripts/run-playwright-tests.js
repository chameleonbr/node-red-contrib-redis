"use strict";

const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");
const Redis = require("ioredis");

const ROOT = path.resolve(__dirname, "..");
const PLAYWRIGHT = path.join(ROOT, "node_modules", ".bin", "playwright");
const AUTH_USERNAME = "node_red";
const AUTH_PASSWORD = "node-red-pass";
const DEPLOYMENT = "playwright-editor";
let DOCKER_COMMAND = ["docker"];

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: ROOT,
    env: Object.assign({}, process.env, options.env || {}),
    stdio: options.stdio || "inherit",
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} exited with ${result.status}`);
  }
  return result;
}

function tryRun(command, args) {
  const result = spawnSync(command, args, {
    cwd: ROOT,
    env: process.env,
    stdio: "ignore",
  });
  return !result.error && result.status === 0;
}

function canRun(command, args) {
  return tryRun(command, args);
}

function dockerCompose(args) {
  return [
    "compose",
    "-p",
    "node-red-contrib-redis-playwright-editor",
    "-f",
    path.join("test", "deployments", DEPLOYMENT, "compose.yml"),
    ...args,
  ];
}

function runDocker(args) {
  run(DOCKER_COMMAND[0], DOCKER_COMMAND.slice(1).concat(args));
}

function tryDocker(args) {
  const result = spawnSync(DOCKER_COMMAND[0], DOCKER_COMMAND.slice(1).concat(args), {
    cwd: ROOT,
    env: process.env,
    stdio: "ignore",
  });
  return !result.error && result.status === 0;
}

function resolveDockerCommand() {
  if (canRun("docker", ["info"]) && canRun("docker", ["compose", "version"])) {
    return ["docker"];
  }
  if (
    canRun("sudo", ["-n", "docker", "info"]) &&
    canRun("sudo", "-n docker compose version".split(" "))
  ) {
    return ["sudo", "-n", "docker"];
  }
  return ["docker"];
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function quietRedis(client) {
  client.on("error", () => {});
  return client;
}

function noauthOptions(port = 6379) {
  return {
    host: "127.0.0.1",
    port,
    connectTimeout: 500,
    maxRetriesPerRequest: 1,
    retryStrategy: null,
  };
}

function authOptions(port) {
  return {
    host: "127.0.0.1",
    port,
    username: AUTH_USERNAME,
    password: AUTH_PASSWORD,
    connectTimeout: 500,
    maxRetriesPerRequest: 1,
    retryStrategy: null,
  };
}

async function waitForRedis(options, label) {
  const deadline = Date.now() + 30000;
  let lastError;
  while (Date.now() < deadline) {
    const client = quietRedis(new Redis(options));
    try {
      await client.ping();
      client.disconnect();
      return;
    } catch (err) {
      lastError = err;
      client.disconnect();
      await sleep(500);
    }
  }
  throw new Error(`Timed out waiting for ${label}: ${lastError && lastError.message}`);
}

async function waitForCluster() {
  const deadline = Date.now() + 45000;
  let lastError;
  while (Date.now() < deadline) {
    const client = quietRedis(new Redis(authOptions(7000)));
    try {
      const info = await client.cluster("info");
      if (/cluster_state:ok/.test(info)) {
        client.disconnect();
        return;
      }
      lastError = new Error(info.trim());
    } catch (err) {
      lastError = err;
    }
    client.disconnect();
    await sleep(500);
  }
  throw new Error(`Timed out waiting for Redis Cluster: ${lastError && lastError.message}`);
}

function memoryDbEnvIfConfigured() {
  const required = ["MEMORYDB_ENDPOINT", "MEMORYDB_PORT", "MEMORYDB_USERNAME", "MEMORYDB_PASSWORD"];
  const missing = required.filter((name) => !process.env[name]);
  if (missing.length > 0) {
    return {
      MEMORYDB_PLAYWRIGHT_ENABLED: "0",
      MEMORYDB_PLAYWRIGHT_SKIP_REASON: `missing ${missing.join(", ")}`,
    };
  }
  return { MEMORYDB_PLAYWRIGHT_ENABLED: "1" };
}

async function main() {
  if (!fs.existsSync(PLAYWRIGHT)) {
    throw new Error("Playwright is not installed. Run npm install first.");
  }

  run("bash", [path.join("scripts", "ensure-docker-ubuntu.sh")]);
  DOCKER_COMMAND = resolveDockerCommand();

  console.log("\n==> playwright-editor: starting Docker deployment");
  tryDocker(dockerCompose(["down", "-v", "--remove-orphans"]));
  try {
    runDocker(dockerCompose(["up", "-d"]));
    await waitForRedis(noauthOptions(6379), "playwright noauth Redis");
    await waitForRedis(authOptions(6380), "playwright auth Redis");
    await waitForCluster();

    console.log("==> playwright-editor: running browser tests");
    run(PLAYWRIGHT, ["test", "--config", "playwright.config.js"], {
      env: Object.assign(
        {
          REDIS_PLAYWRIGHT_NOAUTH_OPTIONS: JSON.stringify(noauthOptions(6379)),
          REDIS_PLAYWRIGHT_AUTH_ENV: "NODE_RED_REDIS_PLAYWRIGHT_AUTH_OPTIONS",
          REDIS_PLAYWRIGHT_CLUSTER_ENV: "NODE_RED_REDIS_PLAYWRIGHT_CLUSTER_OPTIONS",
          NODE_RED_REDIS_PLAYWRIGHT_AUTH_OPTIONS: JSON.stringify({
            host: "127.0.0.1",
            port: 6380,
            username: AUTH_USERNAME,
            password: AUTH_PASSWORD,
            connectTimeout: 500,
            maxRetriesPerRequest: 1,
            retryStrategy: null,
          }),
          NODE_RED_REDIS_PLAYWRIGHT_CLUSTER_OPTIONS: JSON.stringify([
            {
              host: "127.0.0.1",
              port: 7000,
              username: AUTH_USERNAME,
              password: AUTH_PASSWORD,
              connectTimeout: 500,
              maxRetriesPerRequest: 1,
              retryStrategy: null,
            },
            {
              host: "127.0.0.1",
              port: 7001,
              username: AUTH_USERNAME,
              password: AUTH_PASSWORD,
              connectTimeout: 500,
              maxRetriesPerRequest: 1,
              retryStrategy: null,
            },
          ]),
        },
        memoryDbEnvIfConfigured()
      ),
    });
  } finally {
    console.log("==> playwright-editor: tearing down Docker deployment");
    tryDocker(dockerCompose(["down", "-v", "--remove-orphans"]));
  }
}

main().catch((err) => {
  console.error(err.stack || err.message || err);
  process.exit(1);
});
