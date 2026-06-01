const Redis = require("ioredis");
const { redisOptions } = require("./deployment");

/**
 * Deletes all Redis keys matching the given pattern.
 * Safe: only keys matching the pattern are removed; nothing else is touched.
 */
function cleanupKeys(pattern, done) {
  const client = new Redis(redisOptions());
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
