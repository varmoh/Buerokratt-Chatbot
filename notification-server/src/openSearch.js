const { Client } = require("@opensearch-project/opensearch");
const { openSearchConfig } = require("./config");
const logger = require('./logger');  // Import the logger

let client = buildClient();

async function searchNotification({ channelId, connectionId, sender }) {
  try {
    logger.info(`Searching notifications for channelId: ${channelId}, connectionId: ${connectionId}`);

    const response = await client.search({
      index: openSearchConfig.notificationIndex,
      body: {
        query: {
          bool: {
            must: { match: { channelId } },
            must_not: { match: { sentTo: connectionId } },
          },
        },
        sort: { timestamp: { order: "asc" } },
      },
    }).catch(handleError);

    for (const hit of response.body.hits.hits) {
      await sender(hit._source.payload);
      await markAsSent(hit, connectionId);
      logger.info(`Notification sent to connectionId: ${connectionId}`);
    }
  } catch (e) {
    logger.error('Error in searchNotification:', e);
    await sender({});
  }
}

async function markAsSent({ _index, _id }, connectionId) {
  try {
    await client.update({
      index: _index,
      id: _id,
      retry_on_conflict: openSearchConfig.retry_on_conflict,
      body: {
        script: {
          source: `if (ctx._source.sentTo == null) {
            ctx._source.sentTo = [params.connectionId];
          } else {
            ctx._source.sentTo.add(params.connectionId);
          }`,
          lang: "painless",
          params: { connectionId },
        },
      },
    });
    logger.info(`Marked as sent for _id: ${_id} in index: ${_index}`);
  } catch (e) {
    logger.error('Error in markAsSent:', e);
  }
}

async function enqueueChatId(chatId) {
  if (await findChatId(chatId)) return;

  try {
    await client.index({
      index: openSearchConfig.chatQueueIndex,
      body: {
        chatId,
        timestamp: Date.now(),
      },
      refresh: true,
    });
    logger.info(`Chat ID enqueued: ${chatId}`);
  } catch (e) {
    logger.error('Error in enqueueChatId:', e);
  }
}

async function dequeueChatId(chatId) {
  try {
    await client.deleteByQuery({
      index: openSearchConfig.chatQueueIndex,
      body: {
        query: {
          match: {
            chatId: {
              query: chatId,
            },
          },
        },
      },
      refresh: true,
      conflicts: "proceed",
    });
    logger.info(`Chat ID dequeued: ${chatId}`);
  } catch (e) {
    logger.error('Error in dequeueChatId:', e);
  }
}

async function findChatId(chatId) {
  try {
    const found = await isQueueIndexExists();
    if (!found) return null;

    const response = await client.search({
      index: openSearchConfig.chatQueueIndex,
      body: {
        query: {
          match: {
            chatId: {
              query: chatId,
            },
          },
        },
      },
    });

    if (response.body.hits.hits.length === 0) {
      logger.info(`Chat ID not found: ${chatId}`);
      return null;
    }

    logger.info(`Chat ID found: ${chatId}`);
    return response.body.hits.hits[0]._source;
  } catch (e) {
    logger.error('Error in findChatId:', e);
    return null;
  }
}

async function isQueueIndexExists() {
  try {
    const res = await client.indices.exists({
      index: openSearchConfig.chatQueueIndex,
    });
    logger.info(`Queue index exists: ${res.body}`);
    return res.body;
  } catch (e) {
    logger.error('Error in isQueueIndexExists:', e);
    return false;
  }
}

async function findChatIdOrder(chatId) {
  const found = await findChatId(chatId);
  if (!found) return 0;

  try {
    const response = await client.search({
      index: openSearchConfig.chatQueueIndex,
      body: {
        query: {
          range: {
            timestamp: {
              lt: found.timestamp,
            },
          },
        },
        size: 0,
      },
    });

    const order = response.body.hits.total.value + 1;
    logger.info(`Order found for chatId: ${chatId} is ${order}`);
    return order;
  } catch (e) {
    logger.error('Error in findChatIdOrder:', e);
    return 0;
  }
}

function buildClient() {
  return new Client({
    node: openSearchConfig.getUrl(),
    ssl: openSearchConfig.ssl,
  });
}

function handleError(e) {
  if (e.name === 'ConnectionError') {
    logger.warn('ConnectionError occurred, rebuilding client.');
    client = buildClient();
  }
  logger.error('Error in OpenSearch operation:', e);
  throw e;
}

module.exports = {
  searchNotification,
  enqueueChatId,
  dequeueChatId,
  findChatIdOrder,
};
