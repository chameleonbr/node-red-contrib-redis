module.exports = function (RED) {
  "use strict";
  var Redis = require("ioredis");
  var connection = {};
  var usingConn = {};
  var mustache = require("mustache");
  var urllib = require("url");

  function RedisConfig(n) {
    RED.nodes.createNode(this, n);
    this.host = n.host;
    this.port = n.port;
    this.dbase = n.dbase;
    this.pass = n.pass;
    this.usesentinel = n['use-sentinel'];
    this.sentinelname = n['sentinel-name'];
    this.sentinel = n.sentinel;
  }

  RED.nodes.registerType("redis-config", RedisConfig);

  function RedisIn(n) {
    RED.nodes.createNode(this, n);
    this.server = RED.nodes.getNode(n.server);
    this.command = n.command;
    this.name = n.name;
    this.topic = n.topic;
    this.timeout = n.timeout;
    this.sto = null;
    this.topics = [];
    this.client = connect(this.server, true);
    var node = this;

    node.client.on('error', function (err) {
      if (err) {
        clearInterval(node.sto);
        node.error(err);
      }
    });

    node.on('close', function (done) {
      if (node.command === "psubscribe") {
        node.client.punsubscribe();
      }
      else if (node.command === "subscribe") {
        node.client.unsubscribe();
      }
      if (node.sto !== null) {
        clearInterval(node.sto);
        node.sto = null;
      }
      node.status({});
      disconnect(node.server);
      node.topics = [];
      done();
    });

    node.topics = node.topic.split(' ');
    if (node.command === "psubscribe" || node.command === "subscribe") {
      node.client.on('subscribe', function (channel, count) {
        node.status({
          fill: "green",
          shape: "dot",
          text: "connected"
        });
      });
      node.client.on('psubscribe', function (channel, count) {
        node.status({
          fill: "green",
          shape: "dot",
          text: "connected"
        });
      });
      node.client.on('pmessage', function (pattern, channel, message) {
        var payload = null;
        try {
          payload = JSON.parse(message);
        }
        catch (err) {
          payload = message;
        }
        finally {
          node.send({
            pattern: pattern,
            topic: channel,
            payload: payload
          });
        }
      });
      node.client.on('message', function (channel, message) {
        var payload = null;
        try {
          payload = JSON.parse(message);
        }
        catch (err) {
          payload = message;
        }
        finally {
          node.send({
            topic: channel,
            payload: payload
          });
        }
      });
      node.client[node.command](node.topics);
    }
    else {
      node.topics.push(node.timeout);
      node.sto = setInterval(function () {
        node.client[node.command](node.topics, function (err, data) {
          if (err) {
            node.error(err);
          }
          else {
            if (data !== null && data.length == 2) {
              var payload = null;
              try {
                payload = JSON.parse(data[1]);
              }
              catch (err) {
                payload = data[1];
              }
              finally {
                node.send({
                  payload: payload
                });
              }
            }
            else {
              node.send({
                payload: null
              });
            }
          }
        });
      }, 100);
      node.status({
        fill: "green",
        shape: "dot",
        text: "connected"
      });
    }
  }

  RED.nodes.registerType("redis-in", RedisIn);

  function RedisOut(n) {
    RED.nodes.createNode(this, n);
    this.server = RED.nodes.getNode(n.server);
    this.command = n.command;
    this.name = n.name;
    this.topic = n.topic;
    var node = this;

    var client = connect(node.server);

    node.on('close', function (done) {
      node.status({});
      disconnect(node.server);
      done();
    });

    node.on('input', function (msg) {
      var topic;
      if (msg.topic !== undefined && msg.topic !== "") {
        topic = msg.topic;
      }
      else {
        topic = node.topic;
      }
      try {
        client[node.command](topic, JSON.stringify(msg.payload));
      }
      catch (err) {
        node.error(err);
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
    var node = this;

    var client = connect(node.server);

    node.on('close', function (done) {
      node.status({});
      disconnect(node.server);
      done();
    });

    node.on('input', function (msg) {
      if (!Array.isArray(msg.payload)) {
        throw Error('Payload is not Array');
      }

      client[node.command](msg.payload, function (err, res) {
        if (err) {
          node.error(err, msg);
        }
        else {
          msg.payload = res;
          node.send(msg);
        }
      });
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
    this.command = 'eval';
    var node = this;

    var client = connect(node.server);

    node.on('close', function (done) {
      node.status({});
      disconnect(node.server);
      done();
    });
    if (node.stored) {
      client.script('load', node.func, function (err, res) {
        if (err) {
          node.status({
            fill: "red",
            shape: "dot",
            text: "script not loaded"
          });
        }
        else {
          node.status({
            fill: "green",
            shape: "dot",
            text: "script loaded"
          });
          node.sha1 = res;
        }
      });
    }

    node.on('input', function (msg) {
      if (node.keyval > 0 && !Array.isArray(msg.payload)) {
        throw Error('Payload is not Array');
      }

      var args = null;
      if (node.stored) {
        node.command = "evalsha";
        args = [node.sha1, node.keyval].concat(msg.payload);
      }
      else {
        args = [node.func, node.keyval].concat(msg.payload);
      }
      client[node.command](args, function (err, res) {
        if (err) {
          node.error(err, msg);
        }
        else {
          msg.payload = res;
          node.send(msg);
        }
      });
    });

  }

  RED.nodes.registerType("redis-lua-script", RedisLua);

  function _setEnv(config) {
    var result = [];
    for (var key in config) {
      if (/{{/.test(config[key])) {
        result[key] = mustache.render(config[key], process.env);
      } else {
        result[key] = config[key];
      }
    }
    return result;
  }

  function connect(config, force) {
    var options = {};
    var idx = config.id;
    var config_env = _setEnv(config);
    var node = this;

    if (force !== undefined || usingConn[idx] === undefined ||
      usingConn[idx] === 0 || usingConn[idx].connector === undefined) {

      if (config_env.pass !== "") {
        options['password'] = config_env.pass;
      }
      if (config_env.dbase !== "") {
        options['db'] = config_env.dbase;
      }

      if (config_env.usesentinel && config_env.sentinel && config_env.sentinel.length > 0) {
        var sentinels = [];
        config_env.sentinel.split(',').forEach(function (_config) {
          var host = _config.split(':')[0].trim();
          var port;
          if (!host || host.length === 0) {
            console.error('sentinel host is empty, skipping host');
            return;
          }
          if (!_config.split(':')[1].trim() || _config.split(':')[1].trim().length === 0) {
            console.warn('sentinel port is empty; use default port 26379');
            port = 26379;
          } else {
            port = Number.parseInt(_config.split(':')[1], 10);
            if (Number.isNaN(port)) {
              console.error(`sentinel port is invalid, skipping host ${host}`);
              return;
            }
          }
          sentinels.push({
            host: host,
            port: port
          });
        });
        options.sentinels = sentinels;
        if (config_env.sentinelname && config_env.sentinelname.length > 0) {
          options.name = config_env.sentinelname || 'mymaster';
        } else {
          console.warn('[redis] sentinel name is missing, use default "mymaster"');
          options.name = 'mymaster';
        }
      } else {
        options.host = config_env.host;
        options.port = config_env.port;

        let url = config_env.host;
        let parsed = urllib.parse(url, true, true);

        if (!parsed.slashes && url[0] !== '/') {
          url = `//${url}`;
          parsed = urllib.parse(url, true, true);
        }

        if (parsed.auth) {
          options.password = parsed.auth.split(':')[1];
        }
        if (parsed.pathname) {
          if (parsed.protocol === 'redis:') {
            if (parsed.pathname.length > 1) {
              options.db = parsed.pathname.slice(1);
            }
          } else {
            options.path = parsed.pathname;
          }
        }
        if (parsed.host) {
          options.host = parsed.hostname;
        }
        if (parsed.port) {
          options.port = parsed.port;
        }
      }
      var conn = new Redis(options);
      conn.on('error', function (err) {
        console.error('[redis]', err);
      });
      if (force !== undefined && force === true) {
        return conn;
      }
      else {
        connection[idx] = conn;
        if (usingConn[idx] === undefined) {
          usingConn[idx] = 1;
        }
        else {
          usingConn[idx]++;
        }
      }
    }
    else {
      usingConn[idx]++;
    }
    return connection[idx];
  }

  function disconnect(config) {
    var idx = config.id;
    if (usingConn[idx] !== undefined) {
      usingConn[idx]--;

    }
    if (usingConn[idx] <= 0) {
      connection[idx].disconnect();
    }
  }
};
