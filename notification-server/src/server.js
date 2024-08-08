const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const cookieParser = require("cookie-parser");
const csurf = require("csurf");
const logger = require('./logger');  // Import the logger

const { buildSSEResponse } = require("./sseUtil");
const { serverConfig } = require("./config");
const {
  buildNotificationSearchInterval,
  buildQueueCounter,
} = require("./addOns");
const { enqueueChatId, dequeueChatId } = require("./openSearch");
const { addToTerminationQueue, removeFromTerminationQueue } = require("./terminationQueue");

const app = express();

app.use(cors());
app.use(helmet.hidePoweredBy());
app.use(express.json({ extended: false }));
app.use(cookieParser());
app.use(csurf({ cookie: true, ignoreMethods: ['GET', 'POST'] }));

// Middleware to log incoming requests
app.use((req, res, next) => {
  logger.info(`Incoming request: ${req.method} ${req.url}`);
  next();
});

app.get("/sse/notifications/:channelId", (req, res) => {
  const { channelId } = req.params;
  buildSSEResponse({
    req,
    res,
    buildCallbackFunction: buildNotificationSearchInterval({ channelId }),
  });
});

app.get("/sse/queue/:id", (req, res) => {
  const { id } = req.params;
  buildSSEResponse({
    req,
    res,
    buildCallbackFunction: buildQueueCounter({ id }),
  });
});

app.post("/enqueue", async (req, res) => {
  try {
    await enqueueChatId(req.body.id);
    res.status(200).json({ response: 'enqueued successfully' });
    logger.info(`Chat ID ${req.body.id} enqueued successfully`);
  } catch (error) {
    res.status(500).json({ response: 'error' });
    logger.error(`Error enqueuing Chat ID ${req.body.id}: ${error.message}`);
  }
});

app.post("/dequeue", async (req, res) => {
  try {
    await dequeueChatId(req.body.id);
    res.status(200).json({ response: 'dequeued successfully' });
    logger.info(`Chat ID ${req.body.id} dequeued successfully`);
  } catch (error) {
    res.status(500).json({ response: 'error' });
    logger.error(`Error dequeuing Chat ID ${req.body.id}: ${error.message}`);
  }
});

app.post("/add-chat-to-termination-queue", (req, res) => {
  try {
    addToTerminationQueue(
      req.body.chatId,
      () => fetch(`${process.env.RUUTER_URL}/chats/end`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'cookie': req.body.cookie || req.headers.cookie,
        },
        body: JSON.stringify({
          message: {
            chatId: req.body.chatId,
            authorRole: 'end-user',
            event: 'CLIENT_LEFT_FOR_UNKNOWN_REASONS',
            authorTimestamp: new Date().toISOString(),
          }
        }),
      })
    );
    res.status(200).json({ response: 'Chat will be terminated soon' });
    logger.info(`Chat ID ${req.body.chatId} added to termination queue`);
  } catch (error) {
    res.status(500).json({ response: 'error' });
    logger.error(`Error adding Chat ID ${req.body.chatId} to termination queue: ${error.message}`);
  }
});

app.post("/remove-chat-from-termination-queue", (req, res) => {
  try {
    removeFromTerminationQueue(req.body.chatId);
    res.status(200).json({ response: 'Chat termination will be canceled' });
    logger.info(`Chat ID ${req.body.chatId} removed from termination queue`);
  } catch (error) {
    res.status(500).json({ response: 'error' });
    logger.error(`Error removing Chat ID ${req.body.chatId} from termination queue: ${error.message}`);
  }
});

const server = app.listen(serverConfig.port, () => {
  logger.info(`Server running on port ${serverConfig.port}`);
});

module.exports = server;
