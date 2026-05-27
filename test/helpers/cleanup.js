const Redis = require("ioredis");

/**
 * Deletes all Redis keys matching the given pattern.
 * Safe: only keys matching the pattern are removed; nothing else is touched.
 */
function cleanupKeys(pattern, done) {
  const client = new Redis({ host: "127.0.0.1", port: 6379 });
  client
    .keys(pattern)
    .then((keys) => {
      return keys.length > 0 ? client.del(keys) : Promise.resolve();
    })
    .then(() => {
      client.disconnect();
      done();
    })
    .catch((err) => {
      client.disconnect();
      done(err);
    });
}

module.exports = { cleanupKeys };
