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
    this.obj = n.obj;
    this.timeout = n.timeout;
    let node = this;
    let client = getConn(this.server, n.id);
    let running = true;

    node.on("close", async (undeploy, done) => {
      node.status({});
      disconnect(node.id);
      client = null;
      running = false;
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
      client[node.command](node.topic, (err, count) => {
        node.status({
          fill: "green",
          shape: "dot",
          text: "connected",
        });
      });
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
      client[node.command](node.topic, (err, count) => {
        node.status({
          fill: "green",
          shape: "dot",
          text: "connected",
        });
      });
    } else {
      async.whilst(
        (cb) => {
          cb(null, running);
        },
        (cb) => {
          client[node.command](node.topic, Number(node.timeout))
            .then((data) => {
              if (data !== null && data.length == 2) {
                var payload = null;
                try {
                  if(node.obj){
                    payload = JSON.parse(data[1]);
                  }else{
                    payload = data[1];
                  }
                } catch (err) {
                  payload = data[1];
                } finally {
                  node.send({
                    topic: node.topic,
                    payload: payload,
                  });
                }
              }
              cb(null);
            })
            .catch((e) => {
              RED.log.info(e.message);
              running = false;
            });
        },
        () => {}
      );
    }
    node.status({
      fill: "green",
      shape: "dot",
      text: "connected",
    });
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

    node.on("close", function (done) {
      node.status({});
      disconnect( node.server.name);
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
          if(node.obj){
            client[node.command](topic, JSON.stringify(msg.payload));
          }else{
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

    node.on("close", function (done) {
      node.status({});
      disconnect(id);
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

    node.on("close", function (done) {
      node.status({});
      disconnect(id);
      client = null;
      done();
    });
    if (node.stored) {
      client.script("load", node.func, function (err, res) {
        if (err) {
          node.status({
            fill: "red",
            shape: "dot",
            text: "script not loaded",
          });
        } else {
          node.status({
            fill: "green",
            shape: "dot",
            text: "script loaded",
          });
          node.sha1 = res;
        }
      });
    }

    node.on("input", function (msg, send, done) {
      send = send || function() { node.send.apply(node,arguments) }
      done = done || function(err) { if(err)node.error(err, msg); }
      if (node.keyval > 0 && !Array.isArray(msg.payload)) {
        throw Error("Payload is not Array");
      }

      var args = null;
      if (node.stored) {
        node.command = "evalsha";
        args = [node.sha1, node.keyval].concat(msg.payload);
      } else {
        args = [node.func, node.keyval].concat(msg.payload);
      }
      client[node.command](args, function (err, res) {
        if (err) {
          done(err);
        } else {
          msg.payload = res;
          send(msg);
          done();
        }
      });
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

    this.context()[node.location].set(node.topic, client);
    node.status({
      fill: "green",
      shape: "dot",
      text: "ready",
    });

    node.on("close", function (done) {
      node.status({});
      this.context()[node.location].set(node.topic, null);
      disconnect(id);
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

  function disconnect(id) {
    if (usedConn[id] !== undefined) {
      usedConn[id]--;
    }
    if (connections[id] && usedConn[id] <= 0) {
      connections[id].disconnect();
      delete connections[id];
    }
  }
};
