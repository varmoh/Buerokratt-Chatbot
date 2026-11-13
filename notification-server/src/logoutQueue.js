const { createLogger, fingerprint } = require('./logger');

const log = createLogger('logout-queue');

const abortQueue = [];
const timeouts = new Map();

function addToLogoutQueue(id, timeout = 10, callback) {
  // `id` is the session cookie: only its fingerprint is ever logged.
  const session = fingerprint(id);
  const replaced = timeouts.has(id);
  if (replaced) {
    clearTimeout(timeouts.get(id));
  }

  const handle = setTimeout(async () => {
    const aborts = spliceAborts(id);
    timeouts.delete(id);

    if (aborts.length > 0) {
      log.info('Logout cancelled (user came back)', { session });
      return;
    }

    log.info('Logging user out', { session, afterSeconds: timeout });
    try {
      const response = await callback();
      if (response && !response.ok) {
        log.warn('Logout call returned an error status', { session, status: response.status });
      }
    } catch (error) {
      log.error('Logout call failed', { session, error });
    }
  }, timeout * 1000);

  timeouts.set(id, handle);
  log.info('Session added to logout queue', { session, timeoutSec: timeout, replacedPending: replaced });
}

function removeFromLogoutQueue(id) {
  abortQueue.push({ id, at: Date.now() });
  log.info('Logout cancel requested', { session: fingerprint(id), wasPending: timeouts.has(id) });
}

function spliceAborts(id) {
  const abortIndex = abortQueue.findIndex((x) => x.id === id && 5000 > Date.now() - x.at);
  if (abortIndex === -1) {
    return [];
  }
  return abortQueue.splice(abortIndex, 1);
}

module.exports = {
  addToLogoutQueue,
  removeFromLogoutQueue,
};
