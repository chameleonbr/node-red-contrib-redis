module.exports = function(RED) {
    "use strict";
    var redis = require("redis");
    var connection = {};
    var usingConn = {};

    function RedisConfig(n) {
        RED.nodes.createNode(this, n);
        this.host = n.host;
        this.port = n.port;
        this.dbase = n.dbase;
        this.pass = n.pass;
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

        node.client.on('error', function(err) {
            if (err) {
                clearInterval(node.sto);
                node.error(err);
            }
        });

        node.on('close', function(done) {
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
            node.client.end();
            node.topics = [];
            done();
        });

        node.client.select(node.server.dbase, function() {
            node.topics = node.topic.split(' ');
            if (node.command === "psubscribe" || node.command === "subscribe") {
                node.client.on('subscribe', function(channel, count) {
                    node.status({
                        fill: "green",
                        shape: "dot",
                        text: "connected"
                    });
                });
                node.client.on('psubscribe', function(channel, count) {
                    node.status({
                        fill: "green",
                        shape: "dot",
                        text: "connected"
                    });
                });
                node.client.on('pmessage', function(pattern, channel, message) {
                    node.send({
                        pattern: pattern,
                        topic: channel,
                        payload: JSON.parse(message)
                    });
                });
                node.client.on('message', function(channel, message) {
                    node.send({
                        topic: channel,
                        payload: JSON.parse(message)
                    });
                });
                node.client[node.command](node.topics);
            }
            else {
                node.topics.push(node.timeout);
                node.sto = setInterval(function() {
                    node.client[node.command](node.topics, function(err, data) {
                        if (err) {
                            node.error(err);
                        }
                        else {
                            if (data !== null && data.length == 2) {
                                node.send({
                                    payload: JSON.parse(data[1])
                                });
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

        var client = connect(node.server);

        node.on('close', function(done) {
            node.status({});
            disconnect(node.server);
            done();
        });

        node.on('input', function(msg) {
            var topic;
            if (msg.topic !== undefined && msg.topic !== "") {
                topic = msg.topic;
            }
            else {
                topic = node.topic;
            }
            client.select(node.server.dbase, function() {
                client[node.command](topic, JSON.stringify(msg.payload));
            });
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

        node.on('close', function(done) {
            node.status({});
            disconnect(node.server);
            done();
        });

        node.on('input', function(msg) {
            if (!Array.isArray(msg.payload)) {
                throw Error('Payload is not Array');
            }

            client.select(node.server.dbase, function() {
                client[node.command](msg.payload, function(err, res) {
                    if (err) {
                        node.error(err, msg);
                    }
                    else {
                        msg.payload = res;
                        node.send(msg);
                    }
                });
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

        node.on('close', function(done) {
            node.status({});
            disconnect(node.server);
            done();
        });
        if (node.stored) {
            client.select(node.server.dbase, function() {
                client.script('load',node.func, function(err, res) {
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
                        console.log(res);
                        node.sha1 = res;
                    }
                });
            });
        }

        node.on('input', function(msg) {
            if (node.keyval > 0 && !Array.isArray(msg.payload)) {
                throw Error('Payload is not Array');
            }

            client.select(node.server.dbase, function() {
                var args = null;
                if (node.stored) {
                    node.command = "evalsha";
                    args = [node.sha1, node.keyval].concat(msg.payload);
                }else{
                    args = [node.func, node.keyval].concat(msg.payload);
                }
                client[node.command](args, function(err, res) {
                    if (err) {
                        node.error(err, msg);
                    }
                    else {
                        msg.payload = res;
                        node.send(msg);
                    }
                });
            });
        });

    }
    RED.nodes.registerType("redis-lua-script", RedisLua);

    function connect(config, force) {
        var options = {};

        var idx = config.pass + '@' + config.host + ':' + config.port + '/' + config.dbase;

        if (force !== undefined || usingConn[idx] === undefined || usingConn[idx] === 0) {
            if (config.pass !== "") {
                options['auth_pass'] = config.pass;
            }
            var conn = redis.createClient(config.port, config.host, options);
            conn.on('error', function(err) {
                console.log('[redis]', err);
            })
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
        var idx = config.pass + '@' + config.host + ':' + config.port + '/' + config.dbase;
        if (usingConn[idx] !== undefined) {
            usingConn[idx]--;

        }
        if (usingConn[idx] <= 0) {
            connection[idx].end();
        }
    }
};
