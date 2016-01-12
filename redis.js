module.exports = function (RED) {
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
        var node = this;
        var sto = null;
        var client = connect(node.server, true);

        client.on('error', function (err) {
            if (err) {
                node.error(err);
            }
        });

        node.on('close', function (done) {
            if (node.command === "psubscribe") {
                client.punsubscribe();
            } else if (node.command === "subscribe") {
                client.unsubscribe();
            }
            if (sto !== null) {
                clearInterval(sto);
            }
            node.status({});
            client.end();
            done();
        });

        client.select(node.server.dbase, function () {

            var topics = node.topic.split(' ');
            topics.push(node.timeout);

            if (node.command !== "psubscribe" && node.command !== "subscribe") {
                sto = setInterval(function () {
                    client[node.command](topics, function (err, data) {
                        if (err) {
                            node.error(err);
                        }
                        node.send({payload: JSON.parse(data[1])});
                    });
                }, 100);
                node.status({fill: "green", shape: "dot", text: "connected"});
            } else {
                client.on('subscribe', function (channel, count) {
                    node.status({fill: "green", shape: "dot", text: "connected"});
                });
                client.on('message', function (channel, message) {
                    node.send({payload: JSON.parse(message)});
                });
                client[node.command](topics);
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

        client.on('error', function (err) {
            if (err) {
                node.error(err);
            }
        });

        node.on('close', function (done) {
            node.status({});
            disconnect(node.server);
            done();
        });

        node.on('input', function (msg) {

            if (msg.topic !== undefined && msg.topic !== "") {
                topic = msg.topic;
            } else {
                topic = node.topic;
            }
            client.select(node.server.dbase, function () {
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

        client.on('error', function (err) {
            if (err) {
                node.error(err);
            }
        });

        node.on('close', function (done) {
            node.status({});
            disconnect(node.server);
            done();
        });

        node.on('input', function (msg) {
            if (!Array.isArray(msg.payload)) {
                throw Error('Payload is not Array');
            }

            client.select(node.server.dbase, function () {
                client[node.command](msg.payload, function (err, res) {
                    if (err) {
                        node.error(err);
                    }
                    node.send({payload: res});
                });
            });
        });

    }
    RED.nodes.registerType("redis-command", RedisCmd);


    function connect(config, force) {
        var options = {};

        var idx = config.pass + '@' + config.host + ':' + config.port + '/' + config.dbase;

        if (force !== undefined || usingConn[idx] === undefined || usingConn[idx] === 0) {
            if (config.pass !== "") {
                options['auth_pass'] = config.pass;
            }
            var conn = redis.createClient(config.port, config.host, options);
            if (force !== undefined && force === true) {
                return conn;
            } else {
                connection[idx] = conn;
                if (usingConn[idx] === undefined) {
                    usingConn[idx] = 1;
                } else {
                    usingConn[idx]++;
                }
            }
        } else {
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
