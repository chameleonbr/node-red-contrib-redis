module.exports = function (RED) {
  "use strict";
  const Redis = require("ioredis");
  let connections = {};
  let usedConn = {};

  const GRACEFUL_QUIT_TIMEOUT_MS = 2000;
  const TEST_CONNECTION_TIMEOUT_MS = 10000;

  // Attaches ioredis connection-event listeners to drive node.status.
  // Returns a cleanup function that removes all attached listeners.
  // onReady is optional; defaults to showing green "connected".
  function attachStatusListeners(node, client, onReady) {
    if (typeof client.setMaxListeners === "function") {
      // Shared config connections can legitimately have many node status listeners.
      client.setMaxListeners(0);
    }
    var _onReady = onReady || function () {
      node.status({ fill: "green", shape: "dot", text: "connected" });
    };
    var onError = function (e) {
      node.status({ fill: "red", shape: "ring", text: e.message });
    };
    var onClose = function () {
      node.status({ fill: "yellow", shape: "ring", text: "disconnected" });
    };
    var onReconnecting = function () {
      node.status({ fill: "yellow", shape: "ring", text: "reconnecting" });
    };
    var onEnd = function () {
      node.status({ fill: "red", shape: "ring", text: "disconnected" });
    };

    client.on("ready", _onReady);
    client.on("error", onError);
    client.on("close", onClose);
    client.on("reconnecting", onReconnecting);
    client.on("end", onEnd);

    // Set status immediately based on the client's current state (handles shared connections).
    var s = client.status;
    if (s === "ready") {
      _onReady();
    } else if (s === "end") {
      onEnd();
    } else if (s === "reconnecting") {
      onReconnecting();
    } else {
      node.status({ fill: "yellow", shape: "ring", text: "connecting" });
    }

    return function () {
      client.removeListener("ready", _onReady);
      client.removeListener("error", onError);
      client.removeListener("close", onClose);
      client.removeListener("reconnecting", onReconnecting);
      client.removeListener("end", onEnd);
    };
  }

  function setDefault(target, key, value) {
    if (!Object.prototype.hasOwnProperty.call(target, key)) {
      target[key] = value;
    }
    return target;
  }

  function redactValue(value) {
    if (Array.isArray(value)) {
      return value.map(redactValue);
    }
    if (value && typeof value === "object") {
      var copy = {};
      Object.keys(value).forEach(function (key) {
        if (/password|pass|secret|token|auth/i.test(key)) {
          copy[key] = "[redacted]";
        } else if (
          key === "args" &&
          value.name &&
          /^(auth|hello)$/i.test(String(value.name))
        ) {
          copy[key] = value[key].map(function () {
            return "[redacted]";
          });
        } else {
          copy[key] = redactValue(value[key]);
        }
      });
      return copy;
    }
    return value;
  }

  function safeStringify(value) {
    try {
      return JSON.stringify(value, null, 2);
    } catch (e) {
      return String(value);
    }
  }

  function serializeError(err) {
    var out = {
      name: err && err.name,
      message: err && err.message,
      stack: err && err.stack,
    };
    Object.getOwnPropertyNames(err || {}).forEach(function (key) {
      if (!Object.prototype.hasOwnProperty.call(out, key)) {
        out[key] = err[key];
      }
    });
    return redactValue(out);
  }

  function addVerboseLog(log, message, data) {
    var entry = {
      at: new Date().toISOString(),
      message: message,
    };
    if (data !== undefined) {
      entry.data = data;
    }
    log.push(entry);
  }

  function withTimeout(promise, label) {
    var timer;
    return Promise.race([
      promise,
      new Promise(function (_resolve, reject) {
        timer = setTimeout(function () {
          reject(new Error(label + " timed out after " + TEST_CONNECTION_TIMEOUT_MS + "ms"));
        }, TEST_CONNECTION_TIMEOUT_MS);
      }),
    ]).finally(function () {
      clearTimeout(timer);
    });
  }

  function buildClusterClient(clusterOptions, clientOptions) {
    if (Array.isArray(clusterOptions)) {
      var identityNode = clusterOptions.find(function (nodeOptions) {
        return nodeOptions && nodeOptions.dnsLookupStrategy === "identity";
      });
      var authNode = clusterOptions.find(function (nodeOptions) {
        return (
          nodeOptions &&
          (nodeOptions.username ||
            nodeOptions.password ||
            Object.prototype.hasOwnProperty.call(nodeOptions, "tls"))
        );
      });
      var startupNodes = clusterOptions.map(function (nodeOptions) {
        var startupNode = Object.assign({}, nodeOptions);
        delete startupNode.dnsLookupStrategy;
        return startupNode;
      });

      var clusterClientOptions = Object.assign({}, clientOptions || {});
      if (authNode) {
        clusterClientOptions.redisOptions = Object.assign(
          {},
          clusterClientOptions.redisOptions || {}
        );
        if (authNode.username) {
          clusterClientOptions.redisOptions.username = authNode.username;
        }
        if (authNode.password) {
          clusterClientOptions.redisOptions.password = authNode.password;
        }
        if (Object.prototype.hasOwnProperty.call(authNode, "tls")) {
          clusterClientOptions.redisOptions.tls =
            authNode.tls === true ? {} : authNode.tls;
        }
      }

      if (identityNode) {
        clusterClientOptions.dnsLookup = function (address, callback) {
          callback(null, address);
        };
        clusterClientOptions.redisOptions =
          clusterClientOptions.redisOptions || {};
        if (
          !Object.prototype.hasOwnProperty.call(
            clusterClientOptions.redisOptions,
            "tls"
          )
        ) {
          clusterClientOptions.redisOptions.tls = {};
        }
      }

      return new Redis.Cluster(startupNodes, clusterClientOptions);
    }
    if (clientOptions) {
      return new Redis.Cluster(clusterOptions, clientOptions);
    }
    return new Redis.Cluster(clusterOptions);
  }

  function isClusterConnection(options, cluster) {
    return cluster === true || cluster === "true" || Array.isArray(options);
  }

  function buildRedisClient(options, cluster) {
    if (isClusterConnection(options, cluster)) {
      return buildClusterClient(options);
    }
    return new Redis(options);
  }

  function testRedisOptions(base) {
    var options = Object.assign({}, base || {});
    setDefault(options, "connectTimeout", 5000);
    setDefault(options, "maxRetriesPerRequest", 1);
    setDefault(options, "retryStrategy", null);
    setDefault(options, "showFriendlyErrorStack", true);
    setDefault(options, "lazyConnect", true);
    return options;
  }

  function buildTestRedisClient(options, cluster) {
    var redisOptions = testRedisOptions({});
    if (isClusterConnection(options, cluster)) {
      return buildClusterClient(options, {
        lazyConnect: true,
        slotsRefreshTimeout: 5000,
        redisOptions: redisOptions,
      });
    }
    if (typeof options === "string") {
      return new Redis(options, redisOptions);
    }
    return new Redis(testRedisOptions(options));
  }

  function attachVerboseRedisLogs(client, log) {
    var listeners = {};
    ["wait", "connecting", "connect", "ready", "close", "reconnecting", "end"].forEach(
      function (eventName) {
        listeners[eventName] = function (value) {
          addVerboseLog(log, "redis event: " + eventName, value);
        };
        client.on(eventName, listeners[eventName]);
      }
    );
    listeners.error = function (err) {
      addVerboseLog(log, "redis event: error", serializeError(err));
    };
    client.on("error", listeners.error);
    return function () {
      Object.keys(listeners).forEach(function (eventName) {
        client.removeListener(eventName, listeners[eventName]);
      });
    };
  }

  function normalizeEnvName(value) {
    var name = String(value || "").trim();
    var match = name.match(/^\${([^}]+)}$/);
    return match ? match[1] : name;
  }

  function evaluateConnectionOptions(value, valueType, node) {
    valueType = valueType || "json";
    if (valueType === "env") {
      var envName = normalizeEnvName(value);
      if (!envName) {
        var missingNameError = new Error("Environment variable name is required");
        missingNameError.statusCode = 400;
        throw missingNameError;
      }
      var envValue =
        typeof RED.util.getSetting === "function"
          ? RED.util.getSetting(node, envName)
          : process.env[envName];
      if (envValue === undefined) {
        var envError = new Error("Environment variable " + envName + " is not set");
        envError.statusCode = 400;
        throw envError;
      }
      value = envValue;
    }
    if (typeof value === "string") {
      try {
        return JSON.parse(value);
      } catch (e) {
        if (valueType === "env") {
          return value;
        }
        e.statusCode = 400;
        e.message = "Invalid Redis connection JSON: " + e.message;
        throw e;
      }
    }
    return value;
  }

  async function testRedisConnection(options, cluster) {
    var log = [];
    var started = Date.now();
    var client;
    var removeVerboseLogs = function () {};
    var quitCompleted = false;
    try {
      addVerboseLog(log, "creating temporary Redis client", {
        cluster: !!cluster,
        options: redactValue(options),
      });
      client = buildTestRedisClient(options, cluster);
      removeVerboseLogs = attachVerboseRedisLogs(client, log);
      if (client.status !== "ready") {
        addVerboseLog(log, "connecting");
        await withTimeout(client.connect(), "Redis connection");
      }
      addVerboseLog(log, "sending PING");
      var response = await withTimeout(client.ping(), "Redis PING");
      addVerboseLog(log, "received PING response", response);
      if (response !== "PONG") {
        throw new Error("Expected PONG from Redis PING, got " + response);
      }
      addVerboseLog(log, "disconnecting with QUIT");
      await gracefulQuit(client);
      quitCompleted = true;
      addVerboseLog(log, "graceful disconnect complete");
      return {
        success: true,
        response: response,
        message: "Connection test passed: PING -> " + response,
        durationMs: Date.now() - started,
        log: log,
      };
    } catch (err) {
      addVerboseLog(log, "connection test failed", serializeError(err));
      err.connectionTestLog = log;
      err.connectionTestOptions = redactValue(options);
      throw err;
    } finally {
      if (client && !quitCompleted) {
        try {
          addVerboseLog(log, "disconnecting after failed test");
          await gracefulQuit(client);
          addVerboseLog(log, "disconnect after failed test complete");
        } catch (e) {
          addVerboseLog(log, "disconnect after failed test failed", serializeError(e));
          try { client.disconnect(); } catch (_) {}
        }
      }
      removeVerboseLogs();
    }
  }

  function logConnectionTestError(node, payload) {
    var text = [
      "redis-config test connection failed",
      "message: " + payload.message,
      "options: " + safeStringify(payload.options),
      "log: " + safeStringify(payload.log),
      "error: " + safeStringify(payload.error),
    ].join("\n");
    if (node && typeof node.error === "function") {
      node.error(text, {
        topic: "redis-config test connection",
        payload: payload,
      });
    }
  }

function RedisConfig(n) {
    RED.nodes.createNode(this, n);
    this.name = n.name;
    this.cluster = isClusterConnection(undefined, n.cluster);
    this.optionsType = n.optionsType || "json";
    try {
      this.options = evaluateConnectionOptions(n.options, this.optionsType, this);
      this.cluster = isClusterConnection(this.options, this.cluster);
    } catch (err) {
      this.options = undefined;
      this.error(err.message, null);
    }
  }
  RED.nodes.registerType("redis-config", RedisConfig);

  RED.httpAdmin.post(
    "/redis-config/test",
    RED.auth.needsPermission("redis-config.write"),
    async function (req, res) {
      var body = req.body || {};
      var node = body.id ? RED.nodes.getNode(body.id) : null;
      try {
        var options = evaluateConnectionOptions(body.options, body.optionsType, node);
        var result = await testRedisConnection(options, isClusterConnection(options, body.cluster));
        res.json(result);
      } catch (err) {
        var statusCode = err.statusCode || 500;
        var payload = {
          success: false,
          message: "Connection test failed: " + (err.message || String(err)),
          error: serializeError(err),
          options: err.connectionTestOptions || redactValue(body.options),
          log: err.connectionTestLog || [],
        };
        if (statusCode >= 500) {
          logConnectionTestError(node, payload);
        }
        res.status(statusCode).json(payload);
      }
    }
  );

  function RedisIn(n) {
    RED.nodes.createNode(this, n);
    this.server = RED.nodes.getNode(n.server);
    this.command = n.command;
    this.name = n.name;
    this.topic = n.topic;
    this.groupname = n.groupname;
    this.consumername = n.consumername;
    this.obj = n.obj;
    this.timeout = n.timeout;
    let node = this;
    let client = getConn(this.server, n.id);
    let running = true;

    let removeListeners = attachStatusListeners(node, client);

    node.on("close", async (undeploy, done) => {
      removeListeners();
      node.status({});
      running = false;
      // Blocking commands (BLPOP/XREADGROUP BLOCK 0) queue QUIT behind themselves
      // and never release the socket — skip QUIT and disconnect immediately so the
      // in-flight command errors, the while loop sees !running and exits cleanly.
      await disconnect(node.id, true);
      client = null;
      done();
    });

    if (node.command === "psubscribe") {
      client.on("pmessage", function (pattern, channel, message) {
        var payload = null;
        try {
          if(node.obj){
            payload = JSON.parse(message);
          }else{
            payload = message;
          }
        } catch (err) {
          payload = message;
        } finally {
          node.send({
            pattern: pattern,
            topic: channel,
            payload: payload,
          });
        }
      });
      client[node.command](node.topic, (err, count) => {});
    } else if (node.command === "subscribe") {
      client.on("message", function (channel, message) {
        var payload = null;
        try {
          if(node.obj){
            payload = JSON.parse(message);
          }else{
            payload = message;
          }
        } catch (err) {
          payload = message;
        } finally {
          node.send({
            topic: channel,
            payload: payload,
          });
        }
      });
      client[node.command](node.topic, (err, count) => {});
    } else if (node.command === 'xreadgroup') {
        const [stream, lastid] = node.topic.split(':');
        (async () => {
            while (running) {
                try {
                    const data = await client.xreadgroup('GROUP', node.groupname, node.consumername, 'BLOCK', 0, 'STREAMS', stream, lastid);
                    if (data) {
                        data.forEach(function (streamResult) {
                            const streamName = streamResult[0];
                            const messages = streamResult[1];
                            messages.forEach(function (message) {
                                const messageId = message[0];
                                const keyValues = message[1];
                                let payload;
                                if (node.obj) {
                                    payload = {};
                                    for (let i = 0; i < keyValues.length; i += 2) {
                                        payload[keyValues[i]] = keyValues[i + 1];
                                    }
                                } else {
                                    payload = keyValues;
                                }
                                node.send({
                                    stream: streamName,
                                    messageId: messageId,
                                    payload: payload
                                });
                            });
                        });
                    }
                } catch (err) {
                    if (!running) return;
                    if (err.message && err.message.startsWith('NOGROUP')) {
                        node.warn('Consumer group "' + node.groupname + '" not found on stream "' + stream + '". Retrying in 2s — run the setup step to create it.');
                        await new Promise(resolve => setTimeout(resolve, 2000));
                    } else {
                        node.error(err, { topic: node.topic });
                        running = false;
                    }
                }
            }
        })();
    }

    else {
      (async () => {
        while (running) {
          try {
            const data = await client[node.command](node.topic, Number(node.timeout));
            if (data !== null && data.length >= 2) {
              var payload = null;
              var topic = data[0] || node.topic;
              try {
                if (node.command === 'bzpopmin' || node.command === 'bzpopmax') {
                  // data: [key, member, score]
                  let member = data[1];
                  if (node.obj) { try { member = JSON.parse(data[1]); } catch(e) {} }
                  payload = { member: member, score: parseFloat(data[2]) };
                } else if (node.obj) {
                  payload = JSON.parse(data[1]);
                } else {
                  payload = data[1];
                }
              } catch (err) {
                payload = data[1];
              } finally {
                node.send({
                  topic: topic,
                  payload: payload,
                });
              }
            }
          } catch (e) {
            node.log(e.message);
            running = false;
          }
        }
      })();
    }
  }

  RED.nodes.registerType("redis-in", RedisIn);

  function RedisOut(n) {
    RED.nodes.createNode(this, n);
    this.server = RED.nodes.getNode(n.server);
    this.command = n.command;
    this.name = n.name;
    this.topic = n.topic;
    this.obj = n.obj;
    var node = this;

    let client = getConn(this.server, node.server.name);
    let removeListeners = attachStatusListeners(node, client);

    node.on("close", async function (done) {
      removeListeners();
      node.status({});
      await disconnect(node.server.name);
      client = null;
      done();
    });

    node.on("input", function (msg, send, done) {
      var topic;
      send = send || function() { node.send.apply(node,arguments) }
      done = done || function(err) { if(err)node.error(err, msg); }
      if (msg.topic !== undefined && msg.topic !== "") {
        topic = msg.topic;
      } else {
        topic = node.topic;
      }
      if (topic === "") {
        done(new Error("Missing topic, please send topic on msg or set Topic on node."));
      } else {
        try {
          if (node.command === 'xadd') {
            let fields;
            const p = msg.payload;
            if (p && typeof p === 'object' && !Array.isArray(p)) {
              fields = Object.entries(p).reduce((acc, pair) => acc.concat(pair), []);
            } else if (Array.isArray(p)) {
              fields = p;
            } else {
              fields = ['value', p != null ? String(p) : ''];
            }
            client.xadd(topic, '*', ...fields);
          } else if (node.command === 'zadd') {
            const p = msg.payload;
            if (p && typeof p === 'object' && !Array.isArray(p) && 'score' in p) {
              const member = node.obj ? JSON.stringify(p.member) : String(p.member);
              client.zadd(topic, p.score, member);
            } else if (Array.isArray(p)) {
              client.zadd(topic, ...p);
            } else {
              done(new Error("zadd requires payload {score, member} or [score, member, ...]"));
              return;
            }
          } else if (node.obj) {
            client[node.command](topic, JSON.stringify(msg.payload));
          } else {
            client[node.command](topic, msg.payload);
          }
          done();
        } catch (err) {
          done(err);
        }
      }
    });
  }
  RED.nodes.registerType("redis-out", RedisOut);

  function RedisCmd(n) {
    RED.nodes.createNode(this, n);
    this.server = RED.nodes.getNode(n.server);
    this.command = n.command;
    this.name = n.name;
    this.topic = n.topic;
    this.params = n.params;
    var node = this;
    this.block = n.block || false;
    let id = this.block ? n.id : this.server.name;

    let client = getConn(this.server, id);
    let removeListeners = attachStatusListeners(node, client);

    node.on("close", async function (done) {
      removeListeners();
      node.status({});
      await disconnect(id);
      client = null;
      done();
    });

    node.on("input", function (msg, send, done) {
      let topic = undefined;
      send = send || function() { node.send.apply(node,arguments) }
      done = done || function(err) { if(err)node.error(err, msg); }

      if (msg.topic !== undefined && msg.topic !== "") {
        topic = msg.topic;
      } else if (node.topic && node.topic !== "") {
        try {
          topic = node.topic;
        } catch (e) {
          topic = undefined;
        }
      }
      let payload = undefined;

      if (msg.payload) {
        let type = typeof msg.payload;
        switch (type) {
          case "string":
            if (msg.payload.length > 0) {
              payload = msg.payload;
            }
            break;
          case "object":
            if (Array.isArray(msg.payload)) {
              if (msg.payload.length > 0) {
                payload = msg.payload;
              }
              break;
            }
            if (Object.keys(msg.payload).length > 0) {
              payload = msg.payload;
            }
            break;
        }
      } else if (
        node.params &&
        node.params !== "" &&
        node.params !== "[]" &&
        node.params !== "{}"
      ) {
        try {
          payload = JSON.parse(node.params);
        } catch (e) {
          payload = undefined;
        }
      }

      let response = function (err, res) {
        if (err) {
          done(err);
        } else {
          msg.payload = res;
          send(msg);
          done();
        }
      };

      if (!payload) {
        payload = topic;
        topic = undefined;
      }
      if (topic) {
        client.call(node.command, topic, payload, response);
      } else if (payload) {
        client.call(node.command, payload, response);
      } else {
        client.call(node.command, response);
      }
    });
  }
  RED.nodes.registerType("redis-command", RedisCmd);

  function RedisLua(n) {
    RED.nodes.createNode(this, n);
    this.server = RED.nodes.getNode(n.server);
    this.func = n.func;
    this.name = n.name;
    this.keyval = n.keyval;
    this.stored = n.stored;
    this.sha1 = "";
    this.command = "eval";
    var node = this;
    this.block = n.block || false;
    let id = this.block ? n.id : this.server.name;

    let client = getConn(this.server, id);

    let removeListeners;
    if (node.stored) {
      // On every "ready" (including reconnects) reload the script, since Redis
      // is volatile and loses loaded scripts on restart.
      var loadScript = function () {
        client.script("load", node.func, function (err, res) {
          if (err) {
            node.status({ fill: "red", shape: "dot", text: "script not loaded" });
          } else {
            node.status({ fill: "green", shape: "dot", text: "script loaded" });
            node.sha1 = res;
          }
        });
      };
      removeListeners = attachStatusListeners(node, client, loadScript);
    } else {
      removeListeners = attachStatusListeners(node, client);
    }

    node.on("close", async function (done) {
      removeListeners();
      node.status({});
      await disconnect(id);
      client = null;
      done();
    });

    node.on("input", function (msg, send, done) {
      send = send || function() { node.send.apply(node,arguments) }
      done = done || function(err) { if(err)node.error(err, msg); }
      if (node.keyval > 0 && !Array.isArray(msg.payload)) {
        throw Error("Payload is not Array");
      }

      // Sends the script result downstream and releases the execution slot.
      var handleResult = function (res) {
        msg.payload = res;
        send(msg);
        done();
      };

      // Runs the script with EVAL, shipping the full body so Redis (re)caches
      // it under its SHA1. Used directly for unstored scripts and as the
      // NOSCRIPT fallback for stored ones.
      var runWithEval = function () {
        node.command = "eval";
        var args = [node.func, node.keyval].concat(msg.payload);
        client.eval(args, function (err, res) {
          if (err) {
            done(err);
          } else {
            handleResult(res);
          }
        });
      };

      if (node.stored) {
        // Stored scripts prefer EVALSHA to avoid resending the body on every
        // call. Redis evicts cached scripts on restart/SCRIPT FLUSH, so a
        // NOSCRIPT error means the SHA1 is no longer known — fall back to EVAL
        // which reloads the body and re-caches it under the same SHA1.
        node.command = "evalsha";
        var args = [node.sha1, node.keyval].concat(msg.payload);
        client.evalsha(args, function (err, res) {
          if (err) {
            if (err.message && err.message.indexOf("NOSCRIPT") !== -1) {
              runWithEval();
            } else {
              done(err);
            }
          } else {
            handleResult(res);
          }
        });
      } else {
        runWithEval();
      }
    });
  }
  RED.nodes.registerType("redis-lua-script", RedisLua);

  function RedisInstance(n) {
    RED.nodes.createNode(this, n);
    this.server = RED.nodes.getNode(n.server);
    this.location = n.location;
    this.name = n.name;
    this.topic = n.topic;
    let id = n.id;
    var node = this;
    let client = getConn(this.server, id);

    try {
      this.context()[node.location].set(node.topic, client);
    } catch (e) {
      node.warn("redis-instance: failed to store client in context: " + e.message);
    }
    let removeListeners = attachStatusListeners(node, client);

    node.on("close", async function (done) {
      removeListeners();
      node.status({});
      try { this.context()[node.location].set(node.topic, null); } catch (e) {}
      await disconnect(id);
      client = null;
      done();
    });
  }
  RED.nodes.registerType("redis-instance", RedisInstance);

  function getConn(config, id) {
    if (connections[id]) {
      usedConn[id]++;
      return connections[id];
    }

    let options = config.options;

    if (!options) {
      return config.error(
        "Missing options in the redis config - Are you upgrading from old version?",
        null
      );
    }
    try {
      connections[id] = buildRedisClient(options, config.cluster);

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

  // Sends QUIT so in-flight replies drain before the socket closes.
  // Skipped when the connection is not ready (bad host, reconnecting) to avoid
  // queuing a command that can never be sent.
  // Falls back to a forced disconnect after GRACEFUL_QUIT_TIMEOUT_MS in case
  // QUIT itself stalls (e.g. server unresponsive).
  async function gracefulQuit(client) {
    if (client.status !== "ready") {
      try { client.disconnect(); } catch (e) {}
      return;
    }
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      try { client.disconnect(); } catch (e) {}
    }, GRACEFUL_QUIT_TIMEOUT_MS);
    try {
      await client.quit();
    } catch (e) {
      if (!timedOut) {
        try { client.disconnect(); } catch (_) {}
      }
    } finally {
      clearTimeout(timer);
    }
  }

  // force=true skips QUIT and disconnects the socket immediately.
  // Use for blocking connections (BLPOP/XREADGROUP BLOCK 0): QUIT would be
  // queued behind the in-flight command and never sent, so the timeout would
  // fire anyway — an immediate disconnect is both faster and correct because
  // running=false is already set before this is called.
  function disconnect(id, force) {
    if (usedConn[id] !== undefined) {
      usedConn[id]--;
    }
    if (connections[id] && usedConn[id] <= 0) {
      var client = connections[id];
      delete connections[id];
      delete usedConn[id];
      if (force) {
        try { client.disconnect(); } catch (e) {}
        return Promise.resolve();
      }
      return gracefulQuit(client);
    }
    return Promise.resolve();
  }
};
