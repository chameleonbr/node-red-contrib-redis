module.exports = function (RED) {
  "use strict";
  const Redis = require("ioredis");
  let connections = {};
  let usedConn = {};

  const GRACEFUL_QUIT_TIMEOUT_MS = 2000;

  // Attaches ioredis connection-event listeners to drive node.status.
  // Returns a cleanup function that removes all attached listeners.
  // onReady is optional; defaults to showing green "connected".
  function attachStatusListeners(node, client, onReady) {
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

function RedisConfig(n) {
    RED.nodes.createNode(this, n);
    this.name = n.name;
    this.cluster = n.cluster;
    if (this.optionsType === "") {
      this.options = n.options;
    } else {
      RED.util.evaluateNodeProperty(n.options, n.optionsType,this,undefined,(err,value) => {
          if(!err) {
            // Check if value is a string and optionsType is "env"
            if (typeof value === 'string' && n.optionsType === "env") {
                try {
                    this.options = JSON.parse(value); // Attempt to parse JSON
                } catch (e) {
                    console.warn("Failed to parse env as JSON string in redis-config node, use plain value:", e);
                    this.options = value;  // Keep the value as is if it's not valid JSON
                }
            } else {
                this.options = value;
            }
          }
      });
    }
  }
  RED.nodes.registerType("redis-config", RedisConfig);

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
            RED.log.info(e.message);
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
    let id = this.block ? n.id : n.server.name;

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
