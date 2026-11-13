const { v4: uuidv4 } = require('uuid');

const { activeConnections, abortConnectionRequests } = require('./connectionManager');
const { createLogger, cleanId, preview, runWithContext } = require('./logger');
const { createAzureOpenAIStreamRequest } = require('./openSearch');
const {
  LLM_GUI_QUEUE_SOURCE,
  createLLMOrchestrationStreamRequest: createLLMGuiStreamRequest,
} = require('./streamingService');
const streamQueue = require('./streamQueue');

const log = createLogger('sse');

// Comment frames are written this often so that every intermediate proxy sees
// traffic and does not close the connection on its idle timer. EventSource
// ignores comment frames, so this is invisible to the browser.
const HEARTBEAT_INTERVAL_MS = Number(process.env.SSE_HEARTBEAT_INTERVAL_MS || 15_000);

function buildSSEResponse({ res, req, buildCallbackFunction, channelId, llmStream = false }) {
  addSSEHeader(req, res, llmStream);
  keepStreamAlive(res);
  // Only LLM Module GUI connections get the heartbeat; the other SSE routes are unchanged.
  const heartbeat = llmStream ? startHeartbeat(res) : null;
  const connectionId = generateConnectionID();
  const kind = req.route?.path?.split('/')[2] || 'unknown'; // notifications | queue | stream
  const stats = createConnectionStats({ connectionId, channelId, kind });
  const sender = buildSender(res, stats);

  activeConnections.set(connectionId, {
    res,
    sender,
    channelId,
    abortControllers: new Set(),
  });

  log.info('SSE connection opened', {
    connectionId,
    kind,
    channelId: cleanId(channelId),
    queueId: kind === 'queue' ? cleanId(req.params?.id) : undefined,
    origin: req.headers.origin,
    channelConnections: countChannelConnections(channelId),
    totalConnections: activeConnections.size,
  });

  if (channelId) {
    setTimeout(() => {
      processPendingStreamsForChannel(channelId);
    }, 1000);
  }

  const cleanUp = runWithContext({ conn: connectionId.slice(0, 8) }, () =>
    buildCallbackFunction({ connectionId, sender }),
  );

  req.on('close', () => {
    clearInterval(heartbeat);
    // Cancel any in-flight upstream generation - nobody is left to read it.
    abortConnectionRequests(connectionId);
    activeConnections.delete(connectionId);
    cleanUp?.();
    if (stats.stream) logStreamSummary(stats, 'client_disconnected');
    log.info('SSE connection closed by client', {
      connectionId,
      kind,
      channelId: cleanId(channelId),
      durationSec: Math.round((Date.now() - stats.openedAt) / 1000),
      eventsSent: stats.events,
      bytesSent: stats.bytes,
      eventTypes: stats.types,
      channelConnectionsLeft: countChannelConnections(channelId),
      totalConnections: activeConnections.size,
    });
  });
}

function countChannelConnections(channelId) {
  if (!channelId) return undefined;
  let count = 0;
  for (const conn of activeConnections.values()) {
    if (conn.channelId === channelId) count++;
  }
  return count;
}

function createConnectionStats({ connectionId, channelId, kind }) {
  return {
    connectionId,
    channelId: cleanId(channelId),
    kind,
    openedAt: Date.now(),
    events: 0,
    bytes: 0,
    types: {},
    stream: null, // { startedAt, firstChunkAt, chunks, text } while an LLM answer is streaming
  };
}

/** One line per finished LLM answer: how long it took and what the browser actually received. */
function logStreamSummary(stats, outcome, extra = {}) {
  const s = stats.stream;
  if (!s) return;
  const now = Date.now();
  const level = outcome === 'stream_error' || outcome === 'response_error' ? 'warn' : 'info';
  log[level](`LLM answer ${outcome}`, {
    connectionId: stats.connectionId,
    channelId: stats.channelId,
    chunks: s.chunks,
    chars: s.text.length,
    timeToFirstChunkMs: s.firstChunkAt ? s.firstChunkAt - s.startedAt : undefined,
    durationMs: now - s.startedAt,
    answer: preview(s.text),
    ...extra,
  });
  stats.stream = null;
}

const STREAM_END_TYPES = new Set(['stream_complete', 'stream_end', 'stream_error', 'response_error']);

/** Log every event written to the browser; chunks individually only at debug, summarised at info. */
function trackSentEvent(stats, data, size) {
  stats.events++;
  stats.bytes += size;
  const type = (data && typeof data === 'object' && data.type) || (stats.kind === 'queue' ? 'queue_order' : 'event');
  stats.types[type] = (stats.types[type] || 0) + 1;
  const base = { connectionId: stats.connectionId, channelId: stats.channelId, bytes: size };

  if (type === 'stream_start') {
    stats.stream = { startedAt: Date.now(), firstChunkAt: null, chunks: 0, text: '' };
    log.info('⇒ SSE stream_start', base);
    return;
  }

  if (type === 'stream_chunk') {
    if (!stats.stream) stats.stream = { startedAt: Date.now(), firstChunkAt: null, chunks: 0, text: '' };
    stats.stream.chunks++;
    stats.stream.firstChunkAt ??= Date.now();
    stats.stream.text += typeof data.content === 'string' ? data.content : '';
    log.debug('⇒ SSE stream_chunk', {
      ...base,
      n: stats.stream.chunks,
      content: preview(data.content),
      buttons: data.buttons,
    });
    return;
  }

  if (STREAM_END_TYPES.has(type)) {
    log.debug(`⇒ SSE ${type}`, { ...base, event: data });
    if (!stats.stream) stats.stream = { startedAt: Date.now(), firstChunkAt: null, chunks: 0, text: '' };
    logStreamSummary(stats, type, {
      error: data.error || (type.endsWith('error') ? data.content : undefined),
      citations: data.context?.citations?.length,
    });
    return;
  }

  if (type === 'complete_response') {
    log.info('⇒ SSE complete_response', {
      ...base,
      chars: typeof data.content === 'string' ? data.content.length : undefined,
      answer: preview(data.content),
      citations: data.context?.citations?.length,
    });
    return;
  }

  // Notifications, queue order updates, raw Azure parts, errors, …
  const isRawLLMPart = data?.object === 'chat.completion.chunk' || String(type).startsWith('response.');
  const level = isRawLLMPart && !data?.error ? 'debug' : 'info';
  log[level](`⇒ SSE ${type}`, { ...base, data: typeof data === 'string' ? preview(data) : data });
}

function addSSEHeader(req, res, llmStream) {
  const origin = extractOrigin(req.headers.origin);

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
    // Stops nginx-style proxies buffering the stream into a single response.
    ...(llmStream && { 'X-Accel-Buffering': 'no' }),
    'Access-Control-Allow-Origin': origin,
    'Access-Control-Allow-Credentials': true,
    'Access-Control-Expose-Headers': 'Origin, X-Requested-With, Content-Type, Cache-Control, Connection, Accept',
  });
}

function extractOrigin(reqOrigin) {
  const corsWhitelist = process.env.CORS_WHITELIST_ORIGINS.split(',');
  const whitelisted = corsWhitelist.indexOf(reqOrigin) !== -1;
  return whitelisted ? reqOrigin : '*';
}

function keepStreamAlive(res) {
  res.write('');
}

/**
 * Keep the SSE connection warm with periodic comment frames, so proxies between
 * the browser and this server do not time it out during a long generation pause.
 * @returns {NodeJS.Timeout} interval handle; the caller must clear it on close.
 */
function startHeartbeat(res) {
  const heartbeat = setInterval(() => {
    try {
      // A `:` line is an SSE comment: ignored by EventSource, but it is traffic.
      res.write(': ping\n\n');
      log.trace('heartbeat sent');
      if (typeof res.flush === 'function') {
        res.flush();
      }
    } catch (error) {
      log.error('SSE heartbeat write failed', { error });
      clearInterval(heartbeat);
    }
  }, HEARTBEAT_INTERVAL_MS);

  // Do not hold the event loop open purely for a heartbeat.
  heartbeat.unref?.();
  return heartbeat;
}

function generateConnectionID() {
  return uuidv4();
}

function buildSender(res, stats) {
  return (data) => {
    try {
      const formattedData = typeof data === 'string' ? data : JSON.stringify(data);
      const ok = res.write(`data: ${formattedData}\n\n`);
      if (typeof res.flush === 'function') {
        res.flush();
      }
      trackSentEvent(stats, data, Buffer.byteLength(formattedData));
      if (ok === false) {
        log.debug('SSE socket buffer full (slow client)', { connectionId: stats.connectionId });
      }
    } catch (error) {
      log.error('SSE write error', { connectionId: stats.connectionId, channelId: stats.channelId, error });
    }
  };
}

function processPendingStreamsForChannel(channelId) {
  const pendingRequests = streamQueue.getPendingRequests(channelId);

  const logChannelId = cleanId(channelId);

  if (pendingRequests.length > 0) {
    log.info('Replaying queued stream requests for channel', {
      channelId: logChannelId,
      count: pendingRequests.length,
      requests: pendingRequests.map((r) => ({
        id: r.id,
        source: r.source || (r.message === undefined ? 'azure' : 'widget-llm'),
        retryCount: r.retryCount,
        ageMs: Date.now() - r.timestamp,
      })),
    });
    pendingRequests.forEach(async (requestData) => {
      if (streamQueue.shouldRetry(requestData)) {
        try {
          if (requestData.source === LLM_GUI_QUEUE_SOURCE) {
            await createLLMGuiStreamRequest({
              channelId,
              message: requestData.message,
              options: requestData.options,
            });
          } else if (requestData.message === undefined) {
            await createAzureOpenAIStreamRequest({
              use_agentic: requestData.use_agentic,
              agent_name: requestData.agent_name,
              agent_type: requestData.agent_type,
              azure_client_id: requestData.azure_client_id,
              azure_client_secret: requestData.azure_client_secret,
              azure_agentic_max_output_tokens: requestData.azure_agentic_max_output_tokens,
              channelId,
              messages: requestData.messages,
              options: requestData.options,
              raw_response: requestData.raw_response,
            });
          } else {
            const { createLLMOrchestrationStreamRequest } = require('./openSearch');
            await createLLMOrchestrationStreamRequest({
              channelId,
              chatId: requestData.chatId || channelId,
              message: requestData.message,
              authorId: requestData.authorId,
              conversationHistory: requestData.conversationHistory,
            });
          }

          streamQueue.removeFromQueue(channelId, requestData.id);
          log.info('Queued stream request replayed', { channelId: logChannelId, requestId: requestData.id });
        } catch (error) {
          log.error('Failed to process queued stream request', {
            channelId: logChannelId,
            requestId: requestData.id,
            retryCount: requestData.retryCount,
            error,
          });
          streamQueue.incrementRetryCount(channelId, requestData.id);
        }
      } else {
        log.warn('Dropping queued stream request: retries exhausted', {
          channelId: logChannelId,
          requestId: requestData.id,
          retryCount: requestData.retryCount,
        });
        streamQueue.removeFromQueue(channelId, requestData.id);
      }
    });
  }
}

module.exports = {
  activeConnections,
  buildSSEResponse,
  processPendingStreamsForChannel,
};
