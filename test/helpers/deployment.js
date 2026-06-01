"use strict";

const Redis = require("ioredis");

function intEnv(name, fallback) {
  const value = process.env[name];
  if (value === undefined || value === "") {
    return fallback;
  }
  return Number(value);
}

function withOptionalAuth(options) {
  if (process.env.REDIS_USERNAME) {
    options.username = process.env.REDIS_USERNAME;
  }
  if (process.env.REDIS_PASSWORD) {
    options.password = process.env.REDIS_PASSWORD;
  }
  return options;
}

function redisOptions(overrides = {}) {
  return Object.assign(
    withOptionalAuth({
      host: process.env.REDIS_HOST || "127.0.0.1",
      port: intEnv("REDIS_PORT", 6379),
    }),
    overrides
  );
}

function badRedisOptions(overrides = {}) {
  return Object.assign(
    withOptionalAuth({
      host: process.env.REDIS_BAD_HOST || "127.0.0.1",
      port: intEnv("REDIS_BAD_PORT", 6399),
      retryStrategy: null,
      maxRetriesPerRequest: 1,
      connectTimeout: 300,
    }),
    overrides
  );
}

function redisConfigNode(id = "config1", name = "Local", overrides = {}) {
  return {
    id,
    type: "redis-config",
    name,
    options: JSON.stringify(redisOptions(overrides)),
    optionsType: "json",
    cluster: false,
  };
}

function badRedisConfigNode(id = "cfg-bad", name = "BadConn", overrides = {}) {
  return {
    id,
    type: "redis-config",
    name,
    options: JSON.stringify(badRedisOptions(overrides)),
    optionsType: "json",
    cluster: false,
  };
}

function directRedis(overrides = {}) {
  return new Redis(redisOptions(overrides));
}

module.exports = {
  badRedisConfigNode,
  badRedisOptions,
  directRedis,
  redisConfigNode,
  redisOptions,
};
