const { createLogger } = require('./logger');

const log = createLogger('termination-queue');

const abortQueue = [];
const timeouts = new Map();
const terminationDelay = process.env.TERMINATION_TIMEOUT || 10;

function addToTerminationQueue(id, timeout = terminationDelay, callback) {
  const replaced = timeouts.has(id);
  if (replaced) {
    clearTimeout(timeouts.get(id));
  }

  const handle = setTimeout(async () => {
    const aborts = spliceAborts(id);
    timeouts.delete(id);

    if (aborts.length > 0) {
      log.info('Chat termination cancelled (client came back)', { chatId: id });
      return;
    }

    log.info('Terminating chat: CLIENT_LEFT_FOR_UNKNOWN_REASONS', { chatId: id, afterSeconds: timeout });
    try {
      const response = await callback();
      if (response && !response.ok) {
        log.warn('Chat termination call returned an error status', { chatId: id, status: response.status });
      }
    } catch (error) {
      log.error('Chat termination call failed', { chatId: id, error });
    }
  }, timeout * 1000);

  timeouts.set(id, handle);
  log.info('Chat added to termination queue', {
    chatId: id,
    timeoutSec: timeout,
    replacedPending: replaced,
    pending: timeouts.size,
  });
}

function removeFromTerminationQueue(id) {
  abortQueue.push({ id, at: Date.now() });
  log.info('Chat termination cancel requested', { chatId: id, wasPending: timeouts.has(id) });
}

function spliceAborts(id) {
  const abortIndex = abortQueue.findIndex((x) => x.id === id && 10000 > Date.now() - x.at);
  if (abortIndex === -1) {
    return [];
  }
  return abortQueue.splice(abortIndex, 1);
}

module.exports = {
  addToTerminationQueue,
  removeFromTerminationQueue,
};
