"use strict";

// Polls until node[prop] is truthy; rejects after timeoutMs. Used to wait for
// redis-lua-script load side effects that are set asynchronously on the
// connection "ready" event: node.sha1 (stored scripts via SCRIPT LOAD) and
// node.libname (function libraries via FUNCTION LOAD REPLACE).
async function waitForNodeProp(node, prop, timeoutMs = 5000) {
  const start = Date.now();
  while (!node[prop]) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`node.${prop} was never set within ${timeoutMs}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

module.exports = { waitForNodeProp };
