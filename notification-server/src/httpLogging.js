const {
  createLogger,
  fingerprint,
  runWithContext,
  newRequestId,
  cleanId,
  maskUrl,
  preview,
  sanitize,
  serializeError,
} = require('./logger');

const inLog = createLogger('http-in');
const outLog = createLogger('http-out');

function elapsedMs(start) {
  return Math.round(Number(process.hrtime.bigint() - start) / 1e5) / 10;
}

function clientIp(req) {
  const forwarded = req.headers['x-forwarded-for'];
  return cleanId(forwarded ? String(forwarded).split(',')[0] : req.socket?.remoteAddress);
}

/**
 * Express middleware: one line when a request arrives (with its body), one line when
 * the response is sent (status, duration, response body). SSE connections get a
 * closing line with their lifetime. Everything logged during the request carries req=<id>.
 */
function requestLogger(req, res, next) {
  const reqId = cleanId(req.headers['x-request-id']) || newRequestId();
  const start = process.hrtime.bigint();
  const url = req.originalUrl;
  const isSSE = url.startsWith('/sse/');

  res.setHeader('X-Request-Id', reqId);

  // Capture what the route answers with, so it can be logged.
  let responseBody;
  const originalJson = res.json.bind(res);
  res.json = (body) => {
    responseBody = body;
    return originalJson(body);
  };

  runWithContext({ req: reqId }, () => {
    inLog.info(`→ ${req.method} ${url}`, {
      ip: clientIp(req),
      origin: req.headers.origin,
      contentType: req.headers['content-type'],
      contentLength: req.headers['content-length'],
      // Fingerprint of the cookie header: lets you correlate calls of one session without logging the cookie.
      session: req.headers.cookie ? fingerprint(req.headers.cookie) : undefined,
      body: req.body && typeof req.body === 'object' && Object.keys(req.body).length > 0 ? req.body : undefined,
    });
    inLog.debug('request headers', { headers: req.headers });

    res.on('finish', () => {
      const status = res.statusCode;
      const level = status >= 500 ? 'error' : status >= 400 ? 'warn' : 'info';
      inLog[level](`← ${req.method} ${url} ${status}`, { durationMs: elapsedMs(start), response: responseBody });
    });

    res.on('close', () => {
      if (res.writableFinished) return;
      const durationMs = elapsedMs(start);
      if (isSSE) {
        inLog.info(`× ${req.method} ${url} SSE closed`, { durationMs, durationSec: Math.round(durationMs / 1000) });
      } else {
        inLog.warn(`× ${req.method} ${url} client closed before response`, { durationMs });
      }
    });

    next();
  });
}

function parseBodyForLog(body) {
  if (body === undefined || body === null) return undefined;
  if (typeof body !== 'string') return body instanceof URLSearchParams ? '[form]' : '[stream/binary]';
  try {
    return JSON.parse(body);
  } catch {
    return body;
  }
}

async function readBodyPreview(response) {
  try {
    const text = await response.clone().text();
    try {
      return sanitize(JSON.parse(text));
    } catch {
      return preview(text);
    }
  } catch (error) {
    return `[unreadable body: ${error.message}]`;
  }
}

/**
 * fetch() with logging of the outgoing call and its outcome.
 * @param {string} target   short name of the upstream, e.g. 'ruuter', 'llm-orchestrator'
 * @param {string} url
 * @param {RequestInit} options
 * @param {{ logResponseBody?: boolean, logRequestBody?: boolean }} [opts]
 *   Streaming responses (text/event-stream) are never read here.
 */
async function loggedFetch(target, url, options = {}, { logResponseBody = true, logRequestBody = true } = {}) {
  const method = options.method || 'GET';
  const safeUrl = maskUrl(url);
  const start = process.hrtime.bigint();

  outLog.info(`⇢ ${target} ${method} ${safeUrl}`, {
    body: logRequestBody ? parseBodyForLog(options.body) : undefined,
  });
  outLog.debug(`⇢ ${target} request headers`, { headers: options.headers });

  let response;
  try {
    response = await fetch(url, options);
  } catch (error) {
    const aborted = error.name === 'AbortError';
    outLog[aborted ? 'info' : 'error'](`⇠ ${target} ${method} ${safeUrl} ${aborted ? 'aborted' : 'FAILED'}`, {
      durationMs: elapsedMs(start),
      error: serializeError(error),
    });
    throw error;
  }

  const contentType = response.headers.get('content-type') || '';
  const isStream = contentType.includes('text/event-stream');
  const fields = { durationMs: elapsedMs(start), contentType };

  if (!isStream && (logResponseBody || !response.ok)) {
    fields.body = await readBodyPreview(response);
  }

  const level = response.status >= 500 ? 'error' : response.status >= 400 ? 'warn' : 'info';
  outLog[level](`⇠ ${target} ${method} ${safeUrl} ${response.status}${isStream ? ' (stream opened)' : ''}`, fields);
  return response;
}

module.exports = { requestLogger, loggedFetch, elapsedMs };
