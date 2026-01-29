module.exports = function (RED) {
  "use strict";
  const Redis = require("ioredis");
  const async = require("async");
  let connections = {};
  let usedConn = {};

function RedisConfig(n) {
    RED.nodes.createNode(this, n);
    this.name = n.name;
    this.cluster = n.cluster;

    // Store original options and type for lazy evaluation in getConn
    this.optionsTypeOriginal = n.optionsType;
    this.optionsOriginal = n.options;
    this.evaluatedOptions = null;

    if (this.optionsType === "json") {
      this.options = n.options;
    }
  }
  RED.nodes.registerType("redis-config", RedisConfig);

  function getConn(config, id) {
    if (connections[id]) {
      usedConn[id]++;
      return connections[id];
    }

    let options = config.options;

    // Evaluate JSONata/env if not already evaluated
    if (!options && config.optionsOriginal !== undefined) {
      try {
        options = RED.util.evaluateNodeProperty(
          config.optionsOriginal,
          config.optionsTypeOriginal,
          config,
          undefined
        );
        config.evaluatedOptions = options;
      } catch (e) {
        console.error("Failed to evaluate node properties:", e);
        return config.error(e, null);
      }
    }

    if (!options) {
      return config.error(
        "Missing options in the redis config - Are you upgrading from old version?",
        null
      );
    }
    try {
      if (config.cluster) {
        connections[id] = new Redis.Cluster(options);
      } else {
        connections[id] = new Redis(options);
      }

      connections[id].on("error", (e) => {
        config.error(e, null);
      });

      if (usedConn[id] === undefined) {
        usedConn[id] = 1;
      }
      return connections[id];
    } catch (e) {
      config.error(e.message, null);
    }
  }

  function disconnect(id) {
    if (usedConn[id] !== undefined) {
      usedConn[id]--;
    }
    if (connections[id] && usedConn[id] <= 0) {
      connections[id].disconnect();
      delete connections[id];
    }
  }
}
