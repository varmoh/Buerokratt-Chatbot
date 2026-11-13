const { v4: uuidv4 } = require('uuid');

const { createLogger, cleanId } = require('./logger');

const log = createLogger('stream-queue');

class StreamQueue {
  constructor() {
    this.queue = new Map();
    this.maxRetries = 3;
    this.retryDelay = 2000;
  }

  addToQueue(channelId, requestData) {
    if (!this.queue.has(channelId)) {
      this.queue.set(channelId, []);
    }

    const requestWithMetadata = {
      ...requestData,
      retryCount: 0,
      timestamp: Date.now(),
      id: uuidv4(),
    };

    this.queue.get(channelId).push(requestWithMetadata);
    log.info('Stream request queued', {
      channelId: cleanId(channelId),
      requestId: requestWithMetadata.id,
      source: requestData.source || (requestData.message === undefined ? 'azure' : 'widget-llm'),
      channelQueueLength: this.queue.get(channelId).length,
      queuedChannels: this.queue.size,
    });
    return requestWithMetadata.id;
  }

  getPendingRequests(channelId) {
    return this.queue.get(channelId) || [];
  }

  removeFromQueue(channelId, requestId) {
    if (!this.queue.has(channelId)) return false;

    const requests = this.queue.get(channelId);
    const index = requests.findIndex((req) => req.id === requestId);

    if (index !== -1) {
      requests.splice(index, 1);
      if (requests.length === 0) {
        this.queue.delete(channelId);
      }
      return true;
    }
    return false;
  }

  clearChannelQueue(channelId) {
    const count = this.queue.get(channelId)?.length || 0;
    if (count > 0) log.info('Channel stream queue cleared', { channelId: cleanId(channelId), dropped: count });
    return this.queue.delete(channelId);
  }

  shouldRetry(request) {
    return request.retryCount < this.maxRetries;
  }

  incrementRetryCount(channelId, requestId) {
    if (!this.queue.has(channelId)) return false;

    const requests = this.queue.get(channelId);
    const request = requests.find((req) => req.id === requestId);

    if (request) {
      request.retryCount++;
      request.lastRetry = Date.now();
      log.warn('Stream request retry count increased', {
        channelId: cleanId(channelId),
        requestId,
        retryCount: request.retryCount,
        maxRetries: this.maxRetries,
      });
      return true;
    }
    return false;
  }
}

module.exports = new StreamQueue();
