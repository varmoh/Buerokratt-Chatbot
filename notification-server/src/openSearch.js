const { Client } = require('@opensearch-project/opensearch');

const { sendAzureAgenticRequest } = require('./azureAgenticRequest');
const { streamAzureOpenAIResponse } = require('./azureOpenAI');
const {
  extractMessageTextPart,
  formatAgenticCitations,
  mapAnnotationsToCitations,
  createAgenticStreamState,
  consumeAgenticStreamDelta,
  flushAgenticStreamBuffer,
  hasAzureAiSearchTool,
} = require('./citationFormatting');
const { openSearchConfig } = require('./config');
const { activeConnections, stoppedChannels } = require('./connectionManager');
const { loggedFetch, elapsedMs } = require('./httpLogging');
const { createLogger, cleanId, maskUrl, preview } = require('./logger');
const streamQueue = require('./streamQueue');

const log = createLogger('opensearch');
const llmLog = createLogger('llm');

let client = buildClient();

async function streamAgenticResponse({ response, connectionId, channelId, sender }) {
  const citationState = createAgenticStreamState();
  let finalAnnotations = [];

  for await (const part of response) {
    if (!activeConnections.has(connectionId) || stoppedChannels.has(channelId)) break;

    if (part.type === 'response.output_text.delta') {
      const emitText = consumeAgenticStreamDelta(citationState, part.delta || '');
      if (emitText) {
        sender({ type: 'stream_chunk', channelId, content: emitText, isComplete: false });
      }
    } else if (part.type === 'response.completed') {
      const fullResponse = part.response ?? part;
      finalAnnotations = hasAzureAiSearchTool(fullResponse)
        ? extractMessageTextPart(fullResponse)?.annotations || []
        : [];
    }
  }

  if (!activeConnections.has(connectionId) || stoppedChannels.has(channelId)) return;

  const trailingText = flushAgenticStreamBuffer(citationState);
  if (trailingText) {
    sender({ type: 'stream_chunk', channelId, content: trailingText, isComplete: false });
  }

  const citations = mapAnnotationsToCitations(finalAnnotations);

  sender({
    type: 'stream_complete',
    channelId,
    content: '',
    context: citations.length > 0 ? { citations } : {},
    isComplete: true,
  });
}

function isFallbackMessage(text, openAIFallback1, openAIFallback2) {
  return text === openAIFallback1 || text === openAIFallback2;
}

async function fetchLLMResponse({
  use_agentic,
  messages,
  options,
  stream,
  agent_name,
  agent_type,
  azure_client_id,
  azure_client_secret,
  azure_agentic_max_output_tokens,
}) {
  if (use_agentic) {
    return sendAzureAgenticRequest(messages, {
      ...options,
      stream,
      agent_name,
      agent_type,
      client_id: azure_client_id,
      client_secret: azure_client_secret,
      max_output_tokens: azure_agentic_max_output_tokens,
    });
  }
  return streamAzureOpenAIResponse(messages, options);
}

async function sendRawResponse({ response, connectionId, channelId, sender, stream }) {
  if (!stream) {
    sender(response);
    return;
  }

  for await (const part of response) {
    if (!activeConnections.has(connectionId) || stoppedChannels.has(channelId)) break;
    sender(part);
  }
}

async function deliverResponse({
  response,
  connectionId,
  channelId,
  sender,
  stream,
  use_agentic,
  openAIFallback1,
  openAIFallback2,
  estonianFallback,
}) {
  if (!stream) {
    const { content, context } = buildCompleteResponseContent({
      response,
      use_agentic,
      openAIFallback1,
      openAIFallback2,
      estonianFallback,
    });
    sender({ type: 'complete_response', channelId, content, context, isComplete: true });
    return;
  }

  sender({ type: 'stream_start', streamId: channelId, channelId });

  if (use_agentic) {
    await streamAgenticResponse({ response, connectionId, channelId, sender });
  } else {
    await streamClassicResponse({
      response,
      connectionId,
      channelId,
      sender,
      openAIFallback1,
      openAIFallback2,
      estonianFallback,
    });
  }
}

function buildCompleteResponseContent({ response, use_agentic, openAIFallback1, openAIFallback2, estonianFallback }) {
  if (use_agentic) {
    const textPart = extractMessageTextPart(response);
    const annotations = hasAzureAiSearchTool(response) ? textPart?.annotations : [];
    return formatAgenticCitations(textPart?.text, annotations);
  }

  const content = response.choices?.[0]?.message?.content || '';
  const context = response.choices?.[0]?.message?.context || {};
  const trimmed = content.trim();

  return {
    content: isFallbackMessage(trimmed, openAIFallback1, openAIFallback2) ? estonianFallback : content,
    context,
  };
}

function isFallbackPrefix(text, openAIFallback1, openAIFallback2) {
  return openAIFallback1.startsWith(text) || openAIFallback2.startsWith(text);
}

function processClassicDelta(state, part, openAIFallback1, openAIFallback2) {
  const choice = part.choices?.[0];
  if (!choice) return null;

  if (!state.context && choice.delta?.context) state.context = choice.delta.context;
  const content = choice.delta?.content || '';
  if (!content) return null;

  state.cumulative += content;

  if (state.startedStreaming) return content;

  if (isFallbackPrefix(state.cumulative, openAIFallback1, openAIFallback2)) return null;

  state.startedStreaming = true;
  return state.cumulative;
}

async function streamClassicResponse({
  response,
  connectionId,
  channelId,
  sender,
  openAIFallback1,
  openAIFallback2,
  estonianFallback,
}) {
  const state = { context: undefined, cumulative: '', startedStreaming: false };

  for await (const part of response) {
    if (!activeConnections.has(connectionId) || stoppedChannels.has(channelId)) break;

    const emitText = processClassicDelta(state, part, openAIFallback1, openAIFallback2);
    if (emitText) {
      sender({ type: 'stream_chunk', channelId, content: emitText, isComplete: false });
    }
  }

  if (!activeConnections.has(connectionId) || stoppedChannels.has(channelId)) return;

  if (!state.startedStreaming && isFallbackMessage(state.cumulative.trim(), openAIFallback1, openAIFallback2)) {
    sender({ type: 'stream_chunk', channelId, content: estonianFallback, isComplete: false });
  }

  sender({ type: 'stream_complete', channelId, content: '', context: state.context || {}, isComplete: true });
}

async function searchNotification({ channelId, connectionId, sender }) {
  try {
    const response = await client
      .search({
        index: openSearchConfig.notificationIndex,
        body: {
          query: {
            bool: {
              must: { match: { channelId } },
              must_not: { match: { sentTo: connectionId } },
            },
          },
          sort: { timestamp: { order: 'asc' } },
        },
      })
      .catch(handleError);

    const hits = response.body.hits.hits;
    log.trace('notification poll', { channelId: cleanId(channelId), connectionId, hits: hits.length });
    if (hits.length > 0) {
      log.info('Notifications found for channel', {
        channelId: cleanId(channelId),
        connectionId,
        count: hits.length,
        tookMs: response.body.took,
      });
    }

    for (const hit of hits) {
      const notification = hit._source;
      const createdMs = Number(notification.timestamp);

      log.info('Delivering notification', {
        channelId: cleanId(channelId),
        connectionId,
        notificationId: hit._id,
        createdAt: notification.timestamp,
        ageMs: Number.isFinite(createdMs) ? Date.now() - createdMs : undefined,
        alreadySentTo: notification.sentTo?.length || 0,
        payload: notification.payload,
      });

      await sender(notification.payload);

      await markAsSent(hit, connectionId);
    }
  } catch (e) {
    log.error('Processing notification failed', { channelId: cleanId(channelId), connectionId, error: e });
    await sender({ error: 'Notification processing failed' });
  }
}

async function createAzureOpenAIStreamRequest({
  channelId,
  messages,
  options = {},
  use_agentic = false,
  agent_name,
  agent_type,
  azure_client_id,
  azure_client_secret,
  azure_agentic_max_output_tokens,
  raw_response = false,
}) {
  const { stream = true } = options;
  const logChannelId = cleanId(channelId);

  try {
    stoppedChannels.delete(channelId);

    const connections = Array.from(activeConnections.entries()).filter(
      ([_, connData]) => connData.channelId === channelId,
    );

    llmLog.info('Azure request starting', {
      channelId: logChannelId,
      provider: use_agentic ? 'azure-agentic' : 'azure-openai',
      stream,
      connections: connections.length,
    });
    llmLog.debug('Azure request messages', { channelId: logChannelId, messages });

    if (connections.length === 0) {
      const requestId = streamQueue.addToQueue(channelId, {
        messages,
        options,
        use_agentic,
        agent_name,
        agent_type,
        azure_client_id,
        azure_client_secret,
        azure_agentic_max_output_tokens,
        raw_response,
      });
      llmLog.warn('No active SSE connection for channel - Azure request queued', {
        channelId: logChannelId,
        requestId,
        queueLength: streamQueue.getPendingRequests(channelId).length,
      });
    }

    const responsePromises = connections.map(async ([connectionId, connData]) => {
      const { sender } = connData;
      const started = process.hrtime.bigint();

      try {
        const response = await fetchLLMResponse({
          use_agentic,
          messages,
          options,
          stream,
          agent_name,
          agent_type,
          azure_client_id,
          azure_client_secret,
          azure_agentic_max_output_tokens,
        });

        llmLog.info('Azure response received', {
          channelId: logChannelId,
          connectionId,
          stream,
          durationMs: elapsedMs(started),
          ...(stream ? {} : summarizeAzureResponse(response, use_agentic)),
        });

        if (!activeConnections.has(connectionId)) {
          llmLog.info('SSE client left before the Azure answer could be delivered', { connectionId });
          return;
        }

        if (raw_response) {
          await sendRawResponse({ response, connectionId, channelId, sender, stream });
          return;
        }

        const openAIFallback1 =
          'The requested information is not found in the retrieved data. Please try another query or topic.';
        const openAIFallback2 =
          'The requested information is not available in the retrieved data. Please try another query or topic.';
        const estonianFallback =
          'Mulle kättesaadavates andmetes puudub teie küsimusele vastav info. Palun täpsustage oma küsimust.';

        await deliverResponse({
          response,
          connectionId,
          channelId,
          sender,
          stream,
          use_agentic,
          openAIFallback1,
          openAIFallback2,
          estonianFallback,
        });
      } catch (error) {
        llmLog.error('Azure request failed for connection', {
          channelId: logChannelId,
          connectionId,
          durationMs: elapsedMs(started),
          error,
        });
        if (activeConnections.has(connectionId)) {
          const errorMessage = `Failed to ${stream ? 'stream' : 'generate'} response: ${error.message}`;
          sender({
            type: stream ? 'stream_error' : 'response_error',
            channelId,
            content: errorMessage,
            isComplete: true,
          });
        }
        throw error;
      }
    });

    await Promise.all(responsePromises);

    return {
      success: true,
      channelId,
      connectionsCount: connections.length,
      message: `Azure OpenAI ${stream ? 'streaming' : 'response'} completed for all connections`,
    };
  } catch (error) {
    llmLog.error('createAzureOpenAIStreamRequest failed', { channelId: logChannelId, stream, error });
    throw error;
  }
}

function summarizeAzureResponse(response, use_agentic) {
  try {
    if (use_agentic) {
      return {
        responseId: response?.id,
        status: response?.status,
        usage: response?.usage,
        answer: preview(extractMessageTextPart(response)?.text),
      };
    }
    const choice = response?.choices?.[0];
    return {
      responseId: response?.id,
      model: response?.model,
      finishReason: choice?.finish_reason,
      usage: response?.usage,
      answer: preview(choice?.message?.content),
    };
  } catch {
    return {};
  }
}

async function sendBulkNotification({ operations }) {
  const ops = Array.isArray(operations) ? operations : [];
  const docs = ops.filter((op) => !op?.index && !op?.create && !op?.update && !op?.delete);
  log.info('Bulk notification write', {
    operations: ops.length,
    documents: docs.length,
    channels: [...new Set(docs.map((d) => d?.channelId).filter(Boolean))],
  });
  log.debug('Bulk notification documents', { documents: docs });

  const response = await client.bulk({ body: operations });
  const body = response?.body || {};

  if (body.errors) {
    const failed = (body.items || [])
      .map((item) => Object.values(item)[0])
      .filter((item) => item?.error)
      .map((item) => ({ index: item._index, id: item._id, status: item.status, error: item.error }));
    log.error('Bulk notification write had errors', { failed: failed.length, items: failed });
  } else {
    log.info('Bulk notification write ok', { items: body.items?.length, tookMs: body.took });
  }
}

async function markAsSent({ _index, _id }, connectionId) {
  log.debug('Marking notification as sent', { notificationId: _id, connectionId });
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
        lang: 'painless',
        params: { connectionId },
      },
    },
  });
}

async function enqueueChatId(chatId) {
  if (await findChatId(chatId)) {
    log.info('Chat already in queue - not enqueued again', { chatId });
    return;
  }

  log.info('Enqueuing chat', { chatId });
  await client
    .index({
      index: openSearchConfig.chatQueueIndex,
      body: {
        chatId,
        timestamp: Date.now(),
      },
      refresh: true,
    })
    .catch(handleError);
}

async function dequeueChatId(chatId) {
  const response = await client
    .deleteByQuery({
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
      conflicts: 'proceed',
    })
    .catch(handleError);
  log.info('Dequeued chat', { chatId, deleted: response?.body?.deleted });
}

async function findChatId(chatId) {
  const found = await isQueueIndexExists();
  if (!found) return null;

  const response = await client
    .search({
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
    })
    .catch(handleError);

  if (response.body.hits.hits.length == 0) return null;

  return response.body.hits.hits[0]._source;
}

async function isQueueIndexExists() {
  const res = await client.indices
    .exists({
      index: openSearchConfig.chatQueueIndex,
    })
    .catch(handleError);
  return res.body;
}

async function findChatIdOrder(chatId) {
  const found = await findChatId(chatId);
  if (!found) return 0;

  const response = await client
    .search({
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
    })
    .catch(handleError);

  return response.body.hits.total.value + 1;
}

function buildClient() {
  return new Client({
    node: openSearchConfig.getUrl(),
    ssl: openSearchConfig.ssl,
  });
}

function handleError(e) {
  log.error('OpenSearch call failed', { url: maskUrl(openSearchConfig.getUrl()), error: e });
  if (e.name === 'ConnectionError') {
    log.warn('OpenSearch connection error - rebuilding client');
    client = buildClient();
  }
  throw e;
}

async function createLLMOrchestrationStreamRequest({ channelId, chatId, message, authorId, conversationHistory = [] }) {
  const connections = Array.from(activeConnections.entries()).filter(
    ([_, connData]) => connData.channelId === channelId,
  );

  const logChannelId = cleanId(channelId);
  llmLog.info('Widget LLM orchestration request', {
    channelId: logChannelId,
    chatId,
    connections: connections.length,
  });

  if (connections.length === 0) {
    const requestId = streamQueue.addToQueue(channelId, {
      chatId: chatId || channelId,
      message,
      authorId,
      conversationHistory,
    });
    llmLog.warn('No active SSE connection for channel - LLM request queued', { channelId: logChannelId, requestId });
    return;
  }

  const orchestrationPayload = {
    chatId: chatId || channelId,
    message,
    authorId: authorId || `user-${channelId}`,
    conversationHistory: buildConversationHistory(conversationHistory),
    url: 'sse-stream-context',
  };

  await Promise.all(
    connections.map(([connectionId, connData]) =>
      handleLLMConnection(connectionId, connData, orchestrationPayload, channelId),
    ),
  );
  return { success: true, channelId };
}

function buildConversationHistory(conversationHistory) {
  return (conversationHistory || []).map((m) => {
    const rawRole = m.authorRole || m.role || 'user';
    const authorRole = rawRole === 'assistant' ? 'bot' : rawRole;
    return {
      authorRole,
      message: m.message || m.content,
      timestamp: m.timestamp || new Date().toISOString(),
    };
  });
}

function processSSELine(line, sender, channelId) {
  if (!line.trim() || !line.startsWith('data: ')) return false;
  llmLog.trace('upstream frame', { channelId: cleanId(channelId), line: preview(line) });
  try {
    const data = JSON.parse(line.slice(6));
    const content = data.payload?.content;
    if (!content) return false;
    if (content === 'END') {
      sender({ type: 'stream_complete', streamId: channelId, channelId, isComplete: true });
      return true;
    }
    sender({ type: 'stream_chunk', content, streamId: channelId, channelId, isComplete: false });
  } catch (parseError) {
    llmLog.warn('Failed to parse LLM SSE data', {
      channelId: cleanId(channelId),
      line: preview(line),
      error: parseError,
    });
  }
  return false;
}

async function readLLMStream(reader, decoder, sender, channelId, connectionId) {
  let buffer = '';
  while (activeConnections.has(connectionId)) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';
    for (const line of lines) {
      if (processSSELine(line, sender, channelId)) return;
    }
  }
}

async function handleLLMConnection(connectionId, connData, orchestrationPayload, channelId) {
  const { sender } = connData;
  try {
    const response = await loggedFetch(
      'llm-orchestrator',
      `${process.env.LLM_ORCHESTRATOR_URL || 'http://llm-orchestration-service:8100'}/orchestrate/stream`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(orchestrationPayload),
      },
    );
    if (!response.ok) {
      throw new Error(`LLM Orchestration API error: ${response.status} ${response.statusText}`);
    }
    if (!activeConnections.has(connectionId)) return;
    sender({ type: 'stream_start', streamId: channelId, channelId, isComplete: false });
    await readLLMStream(response.body.getReader(), new TextDecoder(), sender, channelId, connectionId);
  } catch (error) {
    llmLog.error('Widget LLM streaming error', { channelId: cleanId(channelId), connectionId, error });
    if (activeConnections.has(connectionId)) {
      sender({ type: 'stream_error', error: error.message, streamId: channelId, channelId, isComplete: true });
    }
  }
}

module.exports = {
  client,
  searchNotification,
  enqueueChatId,
  dequeueChatId,
  findChatIdOrder,
  sendBulkNotification,
  createAzureOpenAIStreamRequest,
  createLLMOrchestrationStreamRequest,
};
