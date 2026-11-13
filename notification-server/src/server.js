const cookieParser = require('cookie-parser');
const cors = require('cors');
const csurf = require('csurf');
const express = require('express');
const helmet = require('helmet');

const { buildNotificationSearchInterval, buildQueueCounter } = require('./addOns');
const { initializeAzureOpenAI } = require('./azureOpenAI');
const { serverConfig } = require('./config');
const { activeConnections, stoppedChannels } = require('./connectionManager');
const { requestLogger, loggedFetch } = require('./httpLogging');
const { createLogger, cleanId, fingerprint, preview } = require('./logger');
const { addToLogoutQueue, removeFromLogoutQueue } = require('./logoutQueue');
const {
  enqueueChatId,
  dequeueChatId,
  sendBulkNotification,
  createAzureOpenAIStreamRequest,
  createLLMOrchestrationStreamRequest,
} = require('./openSearch');
const { buildSSEResponse } = require('./sseUtil');
const { createLLMOrchestrationStreamRequest: createLLMGuiStreamRequest } = require('./streamingService');
const streamQueue = require('./streamQueue');
const { addToTerminationQueue, removeFromTerminationQueue } = require('./terminationQueue');

const log = createLogger('server');

const app = express();

app.use(cors());
app.use(helmet.hidePoweredBy());
app.use(express.json({ extended: false }));
app.use(cookieParser());
// Before every route, so SSE connections are logged too. Cookies/secrets are redacted.
app.use(requestLogger);
app.use(csurf({ cookie: true, ignoreMethods: ['GET', 'POST'] }));

try {
  initializeAzureOpenAI();
  log.info('Azure OpenAI initialized successfully');
} catch (error) {
  log.warn('Azure OpenAI not initialized', { reason: error.message });
}

function parseMaybeText(body) {
  return typeof body === 'string' ? JSON.parse(body) : body;
}

app.get('/sse/notifications/:channelId', (req, res) => {
  const { channelId } = req.params;
  buildSSEResponse({
    req,
    res,
    buildCallbackFunction: buildNotificationSearchInterval({ channelId }),
    channelId,
  });
});

app.get('/sse/queue/:id', (req, res) => {
  const { id } = req.params;
  buildSSEResponse({
    req,
    res,
    buildCallbackFunction: buildQueueCounter({ id }),
  });
});

// LLM Module GUI streaming connection
app.get('/sse/stream/:channelId', (req, res) => {
  const { channelId } = req.params;
  buildSSEResponse({
    req,
    res,
    // For streaming SSE, we don't set up an interval.
    // Instead, we wait for POST requests to trigger streaming. Open/close is logged by sseUtil.
    buildCallbackFunction: () => () => {},
    channelId,
    llmStream: true,
  });
});

app.post('/bulk-notifications', async (req, res) => {
  try {
    await sendBulkNotification(req.body);
    res.status(200).json({ response: 'sent successfully' });
  } catch (error) {
    log.error('Bulk notification failed', { error });
    res.status(500).json({ response: 'error' });
  }
});

app.post('/add-to-logout-queue', async (req, res) => {
  const cookies = req.headers.cookie;

  try {
    await addToLogoutQueue(cookies, 5, () =>
      loggedFetch('ruuter-private', `${process.env.PRIVATE_RUUTER_URL}/backoffice/accounts/logout`, {
        method: 'GET',
        headers: {
          cookie: cookies,
        },
      }),
    );

    log.info('Logout scheduled', { session: fingerprint(cookies), inSeconds: 5 });
    res.sendStatus(200);
  } catch (error) {
    log.error('Error scheduling logout', { session: fingerprint(cookies), error });
    res.sendStatus(500);
  }
});

app.post('/remove-from-logout-queue', async (req, res) => {
  try {
    await removeFromLogoutQueue(req.headers.cookie);
    res.status(200).json({ response: 'Logout would be canceled' });
  } catch (error) {
    log.error('Error cancelling logout', { session: fingerprint(req.headers.cookie), error });
    res.status(500).json({ response: 'error' });
  }
});

app.post('/enqueue', async (req, res) => {
  try {
    await enqueueChatId(req.body.id);
    res.status(200).json({ response: 'enqueued successfully' });
  } catch (error) {
    log.error('Enqueue failed', { chatId: req.body?.id, error });
    res.status(500).json({ response: 'error' });
  }
});

app.post('/dequeue', async (req, res) => {
  try {
    await dequeueChatId(req.body.id);
    res.status(200).json({ response: 'dequeued successfully' });
  } catch (error) {
    log.error('Dequeue failed', { chatId: req.body?.id, error });
    res.status(500).json({ response: 'error' });
  }
});

app.post('/add-chat-to-termination-queue', express.json(), express.text(), async (req, res) => {
  try {
    const body = parseMaybeText(req.body);
    if (typeof req.body === 'string') log.info('text body parsed', { body });

    addToTerminationQueue(body.chatId, body.timeout, () =>
      loggedFetch('ruuter-public', `${process.env.RUUTER_URL}/backoffice/chats/end`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          cookie: body.cookie || req.headers.cookie,
        },
        body: JSON.stringify({
          message: {
            chatId: body.chatId,
            authorRole: 'end-user',
            event: 'CLIENT_LEFT_FOR_UNKNOWN_REASONS',
            authorTimestamp: new Date().toISOString(),
          },
        }),
      }),
    );

    res.status(200).json({ response: 'Chat will be terminated soon' });
  } catch (error) {
    log.error('Error adding chat to termination queue', { error });
    res.status(500).json({ response: 'error' });
  }
});

app.post('/remove-chat-from-termination-queue', express.json(), express.text(), async (req, res) => {
  const body = parseMaybeText(req.body);
  if (typeof req.body === 'string') log.info('text body parsed', { body });

  try {
    removeFromTerminationQueue(body.chatId);
    res.status(200).json({ response: 'Chat termination will be canceled' });
  } catch (error) {
    log.error('Error cancelling chat termination', { chatId: body?.chatId, error });
    res.status(500).json({ response: 'error' });
  }
});

app.post('/channels/:channelId/llm-stream', async (req, res) => {
  const { channelId } = req.params;
  const { chatId, message, authorId, conversationHistory = [] } = req.body;

  if (!message) {
    return res.status(400).json({ error: 'message is required' });
  }

  log.info('Widget LLM stream triggered', {
    channelId: cleanId(channelId),
    chatId,
    authorId,
    message: preview(message),
    historyLength: conversationHistory.length,
  });
  res.status(202).json({ response: 'stream triggered' });

  createLLMOrchestrationStreamRequest({
    channelId,
    chatId: chatId || channelId,
    message,
    authorId,
    conversationHistory,
  }).catch((error) => {
    log.error('Widget LLM stream failed', { channelId: cleanId(channelId), error });
  });
});

// LLM Module GUI streaming trigger - held open until the stream completes
app.post('/channels/:channelId/orchestrate/stream', async (req, res) => {
  try {
    const { channelId } = req.params;
    const { message, options = {} } = req.body;

    if (!message || typeof message !== 'string') {
      return res.status(400).json({ error: 'Message string is required' });
    }

    const result = await createLLMGuiStreamRequest({
      channelId,
      message,
      options,
    });

    res.status(200).json(result);
  } catch (error) {
    log.warn('LLM GUI stream request did not complete', {
      channelId: cleanId(req.params.channelId),
      reason: error.message,
    });
    if (error.message.includes('No active connections found for this channel - request queued')) {
      res.status(202).json({
        message: 'Request queued - will be processed when connection becomes available',
        status: 'queued',
      });
    } else if (error.message === 'No active connections found for this channel') {
      res.status(404).json({ error: error.message });
    } else {
      res.status(500).json({ error: 'Failed to start LLM orchestration streaming' });
    }
  }
});

app.post('/channels/:channelId/stream', (req, res) => {
  const { channelId } = req.params;
  const {
    messages,
    options = {},
    use_agentic = false,
    agent_name,
    agent_type,
    azure_client_id,
    azure_client_secret,
    azure_agentic_max_output_tokens,
    raw_response = false,
  } = req.body;

  if (!messages || !Array.isArray(messages)) {
    return res.status(400).json({ error: 'Messages array is required' });
  }

  const lastUser = [...messages].reverse().find((m) => m?.role === 'user');
  log.info('Azure stream triggered', {
    channelId: cleanId(channelId),
    mode: use_agentic ? 'agentic' : 'classic',
    stream: options.stream !== false,
    raw_response,
    agent_name,
    agent_type,
    customClientId: Boolean(azure_client_id),
    messageCount: messages.length,
    roles: messages.map((m) => m?.role).join(','),
    lastUserMessage: preview(lastUser?.content),
    options,
  });

  res.status(202).json({ response: 'stream triggered' });

  createAzureOpenAIStreamRequest({
    channelId,
    messages,
    options,
    use_agentic,
    agent_name,
    agent_type,
    azure_client_id,
    azure_client_secret,
    azure_agentic_max_output_tokens,
    raw_response,
  }).catch((error) => {
    log.error('Azure stream failed', { channelId: cleanId(channelId), error });
  });
});

app.post('/channels/:channelId/stream/stop', async (req, res) => {
  try {
    const { channelId } = req.params;

    stoppedChannels.add(channelId);
    const hadQueue = streamQueue.clearChannelQueue(channelId);
    log.info('Stream stop requested', { channelId: cleanId(channelId), clearedQueue: hadQueue });

    setTimeout(() => {
      stoppedChannels.delete(channelId);
    }, 1000);

    res.status(200).json();
  } catch (error) {
    log.error('Error stopping stream', { channelId: cleanId(req.params.channelId), error });
    res.status(200).json();
  }
});

setInterval(
  () => {
    const now = Date.now();
    const oneHour = 60 * 60 * 1000;

    for (const [channelId, requests] of streamQueue.queue.entries()) {
      const staleRequests = requests.filter((req) => now - req.timestamp > oneHour || !streamQueue.shouldRetry(req));

      staleRequests.forEach((staleReq) => {
        streamQueue.removeFromQueue(channelId, staleReq.id);
        log.info('Cleaned up stale stream request', {
          channelId: cleanId(channelId),
          requestId: staleReq.id,
          ageMin: Math.round((now - staleReq.timestamp) / 60000),
          retryCount: staleReq.retryCount,
        });
      });
    }
    log.debug('Periodic state', {
      activeConnections: activeConnections.size,
      queuedChannels: streamQueue.queue.size,
      stoppedChannels: stoppedChannels.size,
    });
  },
  5 * 60 * 1000,
);

const server = app.listen(serverConfig.port, () => {
  console.log(`Server running on port ${serverConfig.port}`);
});

// The SSE GET and the trigger POST are both long-lived by design. Disable the
// per-request cap and let the upstream idle watchdog in streamingService.js
// decide when a stream has genuinely stalled.
server.requestTimeout = 0;
server.headersTimeout = 65_000;
server.keepAliveTimeout = 61_000;

module.exports = server;
