module.exports = function (RED) {
    "use strict";
    const Redis = require('ioredis');
    const async = require('async');
    let connections = {};
    let usedConn = {}

    function iterate(opts, cb) {
        if (Array.isArray(opts)) {
            opts.forEach(cb);
        } else if (typeof opts === 'object') {
            Object.keys(opts).forEach(function (k) {
                cb(opts[k], k, opts);
            });
        }
    }

    function tryToBindEnvVar(value) {
        if (typeof value !== 'string') {
            return value;
        }

        if (value.indexOf('${') === -1) {
            return value;
        }

        const name = value.replace(/[${}]/g, '');
        return process.env[name];
    }

    function bindEnvVar(opts) {
        iterate(opts, function (value, key, src) {
            if (typeof value === 'string') {
                src[key] = tryToBindEnvVar(value);
            } else if (Array.isArray(value) || typeof value === 'object') {
                bindEnvVar(value);
            }
        });
    }

    function RedisConfig(n) {
        RED.nodes.createNode(this, n);
        this.name = n.name;
        this.cluster = n.cluster;
        if (this.optionsType === "") {
            this.options = n.options;
        } else {
            this.options = RED.util.evaluateNodeProperty(n.options, n.optionsType, this)
        }

        bindEnvVar(this.options);
    }
    RED.nodes.registerType("redis-config", RedisConfig);

    function RedisIn(n) {
        RED.nodes.createNode(this, n);
        this.server = RED.nodes.getNode(n.server);
        this.command = n.command;
        this.name = n.name;
        this.topic = n.topic;
        this.timeout = n.timeout;
        this.topics = [];
        let node = this;
        let client = getConn(this.server, n.id);
        let running = true;

        node.on('close', async (undeploy, done) => {
            node.status({});
            disconnect(node.id);
            client = null;
            node.topics = [];
            running = false;
            done();
        });

        node.topics = node.topic.split(' ');
        if (node.command === "psubscribe") {
            client.on('pmessage', function (pattern, channel, message) {
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
            client[node.command](node.topics, (err, count) => {
                node.status({
                    fill: "green",
                    shape: "dot",
                    text: "connected"
                });
            });
        } else if (node.command === "subscribe") {
            client.on('message', function (channel, message) {
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
            client[node.command](node.topics, (err, count) => {
                node.status({
                    fill: "green",
                    shape: "dot",
                    text: "connected"
                });
            });
        }
        else {
            async.whilst((cb) => {
                cb(null, running);
            }, (cb) => {
                client[node.command](node.topics, Number(node.timeout)).then((data) => {
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
                    cb(null);
                }).catch((e) => {
                    RED.log.info(e.message);
                    running = false;
                })
            }, () => { })
        }
        node.status({
            fill: "green",
            shape: "dot",
            text: "connected"
        });
    }

    RED.nodes.registerType("redis-in", RedisIn);

    function RedisOut(n) {
        RED.nodes.createNode(this, n);
        this.server = RED.nodes.getNode(n.server);
        this.command = n.command;
        this.name = n.name;
        this.topic = n.topic;
        var node = this;

        let client = getConn(this.server, node.z);

        node.on('close', function (done) {
            node.status({});
            disconnect(node.z);
            client = null;
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
            if (topic === "") {
                node.error('Missing topic, please send topic on msg or set Topic on node.', msg)
            } else {
                try {
                    client[node.command](topic, JSON.stringify(msg.payload));
                }
                catch (err) {
                    node.error(err, msg);
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
        let id = (this.block) ? (n.id) : (n.z);

        let client = getConn(this.server, id);

        node.on('close', function (done) {
            node.status({});
            disconnect(id);
            client = null;
            done();
        });

        node.on('input', function (msg) {

            let topic = undefined;

            if (msg.topic) {
                topic = msg.topic
            } else if (node.topic && node.topic !== "") {
                try {
                    topic = node.topic
                } catch (e) {
                    topic = undefined
                }
            }
            let payload = undefined;

            if (msg.payload) {
                let type = typeof msg.payload
                switch (type) {
                    case "string":
                        if (msg.payload.length > 0) {
                            payload = msg.payload
                        }
                        break;
                    case "object":
                        if (Object.keys(msg.payload).length > 0) {
                            payload = msg.payload
                        }
                        break;
                    case "array":
                        if (msg.payload.length > 0) {
                            payload = msg.payload
                        }
                        break;
                }
            } else if (node.params && node.params !== "" && node.params !== "[]" && node.params !== "{}") {
                try {
                    payload = JSON.parse(node.params)
                } catch (e) {
                    payload = undefined
                }
            }

            let response = function (err, res) {
                if (err) {
                    node.error(err, msg);
                }
                else {
                    msg.payload = res;
                    node.send(msg);
                }
            };

            if (!payload) {
                payload = topic
                topic = undefined
            }

            if (topic) {
                client[node.command](topic, payload, response);
            } else {
                client[node.command](payload, response);
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
        this.command = 'eval';
        var node = this;
        this.block = n.block || false;
        let id = (this.block) ? (n.id) : (n.z);

        let client = getConn(this.server, id);

        node.on('close', function (done) {
            node.status({});
            disconnect(id);
            client = null;
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

    function RedisInstance(n) {
        RED.nodes.createNode(this, n);
        this.server = RED.nodes.getNode(n.server);
        this.location = n.location;
        this.name = n.name;
        this.topic = n.topic;
        this.block = n.block;
        let id = (this.block) ? (n.id) : (n.z);
        var node = this;
        let client = getConn(this.server, id);

        this.context()[node.location].set(node.topic, client);

        node.on('close', function (done) {
            node.status({});
            this.context()[node.location].set(node.topic, null);
            disconnect(id);
            client = null;
            done();
        });
    }
    RED.nodes.registerType("redis-instance", RedisInstance);



    function getConn(config, id) {
        let options = config.options;
        if (connections[id]) {
            usedConn[id]++
            return connections[id]
        }
        if (config.cluster) {
            connections[id] = new Redis.Cluster(options);
        } else {
            connections[id] = new Redis(options);
        }
        if (usedConn[id] === undefined) {
            usedConn[id] = 1
        }
        return connections[id];
    }

    function disconnect(id) {
        if (usedConn[id] !== undefined) {
            usedConn[id]--
        }
        if (connections[id] && usedConn[id] <= 0) {
            connections[id].disconnect();
            delete connections[id];
        }
    }
};
