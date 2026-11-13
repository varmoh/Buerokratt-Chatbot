const { serverConfig } = require('./config');
const { createLogger, cleanId } = require('./logger');
const { searchNotification, findChatIdOrder } = require('./openSearch');

function buildNotificationSearchInterval({ channelId, interval = serverConfig.refreshInterval }) {
  return ({ connectionId, sender }) => {
    const intervalHandle = setInterval(
      () =>
        searchNotification({
          connectionId,
          channelId,
          sender,
        }),
      interval,
    );

    return () => clearInterval(intervalHandle);
  };
}

const log = createLogger('chat-queue');

function buildQueueCounter({ id, interval = serverConfig.queueRefreshInterval }) {
  return ({ sender }) => {
    let lastOrder = 0;
    const intervalHandle = setInterval(async () => {
      try {
        const order = await findChatIdOrder(id);

        if (order == lastOrder) return;
        log.info('Queue position changed', { chatId: cleanId(id), from: lastOrder, to: order });
        lastOrder = order;
        sender({ order });
      } catch (error) {
        log.error('Queue position lookup failed', { chatId: cleanId(id), error });
      }
    }, interval);

    return () => clearInterval(intervalHandle);
  };
}

module.exports = {
  buildNotificationSearchInterval,
  buildQueueCounter,
};
