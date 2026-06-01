"use strict";

const path = require("path");
const fs = require("fs");
const { spawnSync } = require("child_process");
const Redis = require("ioredis");

const ROOT = path.resolve(__dirname, "..");
const MOCHA = path.join(ROOT, "node_modules", ".bin", "mocha");
const AUTH_USERNAME = "node_red";
const AUTH_PASSWORD = "node-red-pass";
const TOPOLOGY_SPECS = new Set([
  "memorydb_deployment_spec.js",
  "redis_cluster_deployment_spec.js",
  "redis_sentinel_deployment_spec.js",
]);
let DOCKER_COMMAND = ["docker"];

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: ROOT,
    env: Object.assign({}, process.env, options.env || {}),
    stdio: "inherit",
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} exited with ${result.status}`);
  }
}

function tryRun(command, args) {
  const result = spawnSync(command, args, {
    cwd: ROOT,
    env: process.env,
    stdio: "inherit",
  });
  return !result.error && result.status === 0;
}

function canRun(command, args) {
  const result = spawnSync(command, args, {
    cwd: ROOT,
    env: process.env,
    stdio: "ignore",
  });
  return !result.error && result.status === 0;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function quietRedis(client) {
  client.on("error", () => {});
  return client;
}

function standaloneSpecs() {
  return fs
    .readdirSync(path.join(ROOT, "test"))
    .filter((name) => /_spec\.js$/.test(name) && !TOPOLOGY_SPECS.has(name))
    .sort()
    .map((name) => `test/${name}`);
}

function dockerCompose(name, args) {
  return [
    "compose",
    "-p",
    `node-red-contrib-redis-${name}`,
    "-f",
    path.join("test", "deployments", name, "compose.yml"),
    ...args,
  ];
}

function runDocker(args) {
  run(DOCKER_COMMAND[0], DOCKER_COMMAND.slice(1).concat(args));
}

function tryDocker(args) {
  return tryRun(DOCKER_COMMAND[0], DOCKER_COMMAND.slice(1).concat(args));
}

function resolveDockerCommand() {
  if (canRun("docker", ["info"]) && canRun("docker", ["compose", "version"])) {
    return ["docker"];
  }
  if (
    canRun("sudo", ["-n", "docker", "info"]) &&
    canRun("sudo", ["-n", "docker", "compose", "version"])
  ) {
    return ["sudo", "-n", "docker"];
  }
  return ["docker"];
}

function authEnv(name) {
  return {
    REDIS_DEPLOYMENT: name,
    REDIS_HOST: "127.0.0.1",
    REDIS_PORT: "6379",
    REDIS_USERNAME: AUTH_USERNAME,
    REDIS_PASSWORD: AUTH_PASSWORD,
  };
}

function unauthEnv(name) {
  return {
    REDIS_DEPLOYMENT: name,
    REDIS_HOST: "127.0.0.1",
    REDIS_PORT: "6379",
  };
}

function authOptions(port = 6379) {
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

function noauthOptions(port = 6379) {
  return {
    host: "127.0.0.1",
    port,
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

async function waitForSentinel() {
  const deadline = Date.now() + 45000;
  let lastError;
  while (Date.now() < deadline) {
    const sentinel = quietRedis(new Redis({
      host: "127.0.0.1",
      port: 26379,
      connectTimeout: 500,
      maxRetriesPerRequest: 1,
      retryStrategy: null,
    }));
    try {
      const master = await sentinel.call("SENTINEL", "get-master-addr-by-name", "mymaster");
      if (Array.isArray(master) && master.length === 2) {
        const redis = quietRedis(new Redis(authOptions(Number(master[1]))));
        await redis.ping();
        redis.disconnect();
        if (await hasPromotableSentinelReplica(sentinel)) {
          sentinel.disconnect();
          return;
        }
        lastError = new Error("Sentinel has no promotable replica yet");
      }
      if (!lastError) {
        lastError = new Error("Sentinel did not return a master address");
      }
    } catch (err) {
      lastError = err;
    }
    sentinel.disconnect();
    await sleep(500);
  }
  throw new Error(`Timed out waiting for Redis Sentinel: ${lastError && lastError.message}`);
}

function sentinelRowToObject(row) {
  const out = {};
  for (let i = 0; i < row.length; i += 2) {
    out[row[i]] = row[i + 1];
  }
  return out;
}

async function hasPromotableSentinelReplica(sentinel) {
  const rows = await sentinel.call("SENTINEL", "slaves", "mymaster");
  return rows.map(sentinelRowToObject).some((replica) => {
    const flags = String(replica.flags || "");
    return (
      !flags.includes("s_down") &&
      !flags.includes("o_down") &&
      !flags.includes("disconnected") &&
      replica["master-link-status"] === "ok" &&
      replica["slave-priority"] !== "0"
    );
  });
}

function runMocha(specs, env) {
  run(MOCHA, specs, { env });
}

async function runDeployment(deployment) {
  console.log(`\n==> ${deployment.name}: starting Docker deployment`);
  tryDocker(dockerCompose(deployment.name, ["down", "-v", "--remove-orphans"]));
  try {
    runDocker(dockerCompose(deployment.name, ["up", "-d"]));
    await deployment.wait();
    console.log(`==> ${deployment.name}: running tests`);
    runMocha(deployment.specs, deployment.env);
  } finally {
    console.log(`==> ${deployment.name}: tearing down Docker deployment`);
    tryDocker(dockerCompose(deployment.name, ["down", "-v", "--remove-orphans"]));
  }
}

function requireMemoryDbEnv() {
  if (process.env.MEMORYDB_ENABLED !== "1") {
    return null;
  }
  const required = ["MEMORYDB_ENDPOINT", "MEMORYDB_PORT", "MEMORYDB_USERNAME", "MEMORYDB_PASSWORD"];
  const missing = required.filter((name) => !process.env[name]);
  if (missing.length > 0) {
    throw new Error(`MEMORYDB_ENABLED=1 but missing: ${missing.join(", ")}`);
  }
  return { REDIS_DEPLOYMENT: "memorydb" };
}

async function main() {
  run("bash", [path.join("scripts", "ensure-docker-ubuntu.sh")]);
  DOCKER_COMMAND = resolveDockerCommand();

  const deployments = [
    {
      name: "single-noauth",
      env: unauthEnv("single-noauth"),
      specs: standaloneSpecs(),
      wait: () => waitForRedis(noauthOptions(), "single-noauth Redis"),
    },
    {
      name: "single-auth",
      env: authEnv("single-auth"),
      specs: standaloneSpecs(),
      wait: () => waitForRedis(authOptions(), "single-auth Redis"),
    },
    {
      name: "cluster-auth",
      env: Object.assign(authEnv("cluster-auth"), {
        REDIS_CLUSTER_NODES: "127.0.0.1:7000,127.0.0.1:7001",
      }),
      specs: ["test/redis_cluster_deployment_spec.js"],
      wait: waitForCluster,
    },
    {
      name: "sentinel-auth",
      env: Object.assign(authEnv("sentinel-auth"), {
        REDIS_SENTINELS: "127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381",
        REDIS_SENTINEL_MASTER_NAME: "mymaster",
      }),
      specs: ["test/redis_sentinel_deployment_spec.js"],
      wait: waitForSentinel,
    },
  ];

  for (const deployment of deployments) {
    await runDeployment(deployment);
  }

  const memoryDbEnv = requireMemoryDbEnv();
  if (memoryDbEnv) {
    console.log("\n==> memorydb: running AWS MemoryDB tests");
    runMocha(["test/memorydb_deployment_spec.js"], memoryDbEnv);
  } else {
    console.log("\n==> memorydb: skipped (MEMORYDB_ENABLED is not 1)");
  }
}

main().catch((err) => {
  console.error(err.stack || err.message || err);
  process.exit(1);
});
