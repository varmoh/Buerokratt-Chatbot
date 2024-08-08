const { Client } = require("@opensearch-project/opensearch");
const { openSearchConfig } = require("./config");
const logger = require('./logger');  // Import the logger

let client = buildClient();

async function searchNotification({ channelId, connectionId, sender }) {
  const query = {
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
  };

  try {
    logger.info('Executing searchNotification query:', query);

    const response = await client.search(query).catch(handleError);
    logger.info('SearchNotification response:', response.body);

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
  const update = {
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
  };

  try {
    logger.info('Executing markAsSent update:', update);

    await client.update(update);
    logger.info(`Marked as sent for _id: ${_id} in index: ${_index}`);
  } catch (e) {
    logger.error('Error in markAsSent:', e);
  }
}

async function enqueueChatId(chatId) {
  if (await findChatId(chatId)) return;

  const indexDoc = {
    index: openSearchConfig.chatQueueIndex,
    body: {
      chatId,
      timestamp: Date.now(),
    },
    refresh: true,
  };

  try {
    logger.info('Executing enqueueChatId index:', indexDoc);

    await client.index(indexDoc);
    logger.info(`Chat ID enqueued: ${chatId}`);
  } catch (e) {
    logger.error('Error in enqueueChatId:', e);
  }
}

async function dequeueChatId(chatId) {
  const deleteQuery = {
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
  };

  try {
    logger.info('Executing dequeueChatId deleteByQuery:', deleteQuery);

    await client.deleteByQuery(deleteQuery);
    logger.info(`Chat ID dequeued: ${chatId}`);
  } catch (e) {
    logger.error('Error in dequeueChatId:', e);
  }
}

async function findChatId(chatId) {
  try {
    const found = await isQueueIndexExists();
    if (!found) return null;

    const searchQuery = {
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
    };

    logger.info('Executing findChatId search:', searchQuery);

    const response = await client.search(searchQuery);
    logger.info('findChatId response:', response.body);

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
  const checkIndex = {
    index: openSearchConfig.chatQueueIndex,
  };

  try {
    logger.info('Executing isQueueIndexExists check:', checkIndex);

    const res = await client.indices.exists(checkIndex);
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

  const rangeQuery = {
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
  };

  try {
    logger.info('Executing findChatIdOrder search:', rangeQuery);

    const response = await client.search(rangeQuery);
    logger.info('findChatIdOrder response:', response.body);

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
