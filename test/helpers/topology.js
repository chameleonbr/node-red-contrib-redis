"use strict";

function commandNode(id, command, server = "config1", extra = {}) {
  return Object.assign(
    {
      id: `${id}-node`,
      type: "redis-command",
      server,
      command,
      name: command,
      topic: "",
      params: "[]",
      wires: [[`${id}-helper`]],
    },
    extra
  );
}

function helperNode(id) {
  return { id: `${id}-helper`, type: "helper" };
}

function invoke(helper, id, msg = {}, timeoutMs = 7000) {
  return new Promise((resolve, reject) => {
    const node = helper.getNode(`${id}-node`);
    const sink = helper.getNode(`${id}-helper`);
    const timer = setTimeout(() => reject(new Error(`Timed out waiting for ${id}`)), timeoutMs);

    node.once("call:error", (call) => {
      clearTimeout(timer);
      reject(call.args[0]);
    });
    sink.once("input", (out) => {
      clearTimeout(timer);
      resolve(out.payload);
    });

    node.receive(msg);
  });
}

function expectError(helper, id, msg = {}, timeoutMs = 7000) {
  return new Promise((resolve, reject) => {
    const node = helper.getNode(`${id}-node`);
    const sink = helper.getNode(`${id}-helper`);
    const timer = setTimeout(
      () => reject(new Error(`Timed out waiting for ${id} error`)),
      timeoutMs
    );

    node.once("call:error", (call) => {
      clearTimeout(timer);
      resolve(call.args[0]);
    });
    sink.once("input", (out) => {
      clearTimeout(timer);
      reject(new Error(`Expected ${id} to fail, got ${JSON.stringify(out.payload)}`));
    });

    node.receive(msg);
  });
}

function load(helper, redisNode, flow) {
  return new Promise((resolve, reject) => {
    helper.load(redisNode, flow, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

module.exports = {
  commandNode,
  expectError,
  helperNode,
  invoke,
  load,
};
