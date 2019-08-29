# node-red-contrib-redis
Node Red client for Redis with pub/sub, list, lua scripting and other commands support.

Now uses one connection per node to try bypass "Redis IN" issues.

Roadmap:
- Stream Support
- Flow or msg redis instance injection to use on function Node.
- Better Validations
- Custom Commands support(Modules)

Please test and make feedback.

I need contributors...

Redis Commands:

![Redis Command](https://github.com/chameleonbr/node-red-examples/raw/master/images/Node-RED_cmd_cfg.png "Redis Command")

Payload -> Redis

![Payload -> Redis](https://github.com/chameleonbr/node-red-examples/raw/master/images/Node-RED_redis_params.png "Payload -> Redis")

Redis Queue:

![Payload -> Redis](https://github.com/chameleonbr/node-red-examples/raw/master/images/Node-RED_in_out.png "Payload -> Redis")
