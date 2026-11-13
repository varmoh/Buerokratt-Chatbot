/**
 * Small dependency-free structured logger for the notification server.
 *
 * Environment:
 *   LOG_LEVEL             error | warn | info | debug | trace   (default: info)
 *   LOG_FORMAT            pretty | json                         (default: pretty)
 *   LOG_CONTENT           true | false  - log message texts / payloads (default: true).
 *                         With false only lengths and types are logged (no citizen data).
 *   LOG_MAX_VALUE_LENGTH  max characters of any single string value (default: 2000)
 *
 * Every line carries the request id (req=...) of the HTTP request that caused it,
 * via AsyncLocalStorage, so one request can be followed with `grep req=<id>`.
 */
const { AsyncLocalStorage } = require('node:async_hooks');
const { createHash, randomUUID } = require('node:crypto');

const LEVELS = { error: 0, warn: 1, info: 2, debug: 3, trace: 4 };

const settings = {
  level: LEVELS[String(process.env.LOG_LEVEL || 'info').toLowerCase()] ?? LEVELS.info,
  format: String(process.env.LOG_FORMAT || 'pretty').toLowerCase() === 'json' ? 'json' : 'pretty',
  content: String(process.env.LOG_CONTENT || 'true').toLowerCase() !== 'false',
  maxValueLength: Number(process.env.LOG_MAX_VALUE_LENGTH) || 2000,
};

const MAX_DEPTH = 6;
const MAX_ARRAY_ITEMS = 30;

// Keys whose values must never reach the logs. `max_tokens` etc. deliberately do not match.
const SENSITIVE_KEY =
  /(secret|password|passwd|authorization|cookie|api[-_]?key|access[-_]?token|refresh[-_]?token|connection[-_]?string|^token$|^key$|^jwt$|^bearer$)/i;

// Keys that hold user / LLM text. Hidden when LOG_CONTENT=false.
const CONTENT_KEY = /^(content|message|messages|input|payload|text|conversationHistory|delta)$/i;

const context = new AsyncLocalStorage();

function newRequestId() {
  return randomUUID().replace(/-/g, '').slice(0, 12);
}

/** Run `fn` with extra fields attached to every log line written inside it (also across awaits/timers). */
function runWithContext(fields, fn) {
  const parent = context.getStore() || {};
  return context.run({ ...parent, ...fields }, fn);
}

function getContext() {
  return context.getStore() || {};
}

/** Short stable hash, to correlate secret values (e.g. a session cookie) without logging them. */
function fingerprint(value) {
  if (value === undefined || value === null || value === '') return '-';
  return createHash('sha256').update(String(value)).digest('hex').slice(0, 10);
}

function truncate(str, max = settings.maxValueLength) {
  if (typeof str !== 'string') return str;
  if (str.length <= max) return str;
  return `${str.slice(0, max)}…[+${str.length - max} chars]`;
}

/** Text preview honouring LOG_CONTENT. */
function preview(text, max = settings.maxValueLength) {
  if (text === undefined || text === null) return text;
  const str = typeof text === 'string' ? text : safeStringify(text);
  if (!settings.content) return `[hidden len=${str.length}]`;
  return truncate(str, max);
}

/** Remove user:password@ and secret query params from a URL before logging it. */
function maskUrl(url) {
  if (!url) return url;
  return String(url)
    .replace(/\/\/([^/@\s]+)@/, '//***@')
    .replace(/([?&](?:api[-_]?key|key|token|code|secret|sig)=)[^&]*/gi, '$1***');
}

function serializeError(error, depth = 0) {
  if (!(error instanceof Error)) return error;
  const out = { name: error.name, message: error.message };
  if (error.code) out.code = error.code;
  if (error.status) out.status = error.status;
  if (error.statusCode) out.statusCode = error.statusCode;
  if (error.meta?.body) out.body = sanitize(error.meta.body, depth + 1);
  if (error.cause && depth < 3) out.cause = serializeError(error.cause, depth + 1);
  if (error.stack) out.stack = error.stack.split('\n').slice(1, 6).join(' | ').replace(/\s+/g, ' ');
  return out;
}

/**
 * Deep copy suitable for logging: secrets redacted, long strings truncated,
 * big arrays shortened, cycles and depth bounded.
 */
function sanitize(value, depth = 0, seen = new WeakSet(), key = '') {
  if (value === null || value === undefined) return value;
  if (key && SENSITIVE_KEY.test(key)) {
    return `[REDACTED fp=${fingerprint(typeof value === 'string' ? value : safeStringify(value))}]`;
  }
  if (key && !settings.content && CONTENT_KEY.test(key)) {
    const len = typeof value === 'string' ? value.length : Array.isArray(value) ? value.length : undefined;
    return `[hidden${len === undefined ? '' : ` len=${len}`}]`;
  }
  if (typeof value === 'string') return truncate(value);
  if (typeof value === 'bigint') return value.toString();
  if (typeof value !== 'object') return value;
  if (value instanceof Error) return serializeError(value, depth);
  if (Buffer.isBuffer(value)) return `[Buffer ${value.length} bytes]`;
  if (value instanceof Date) return value.toISOString();
  if (seen.has(value)) return '[Circular]';
  if (depth >= MAX_DEPTH) return Array.isArray(value) ? `[Array(${value.length})]` : '[Object]';
  seen.add(value);

  if (Array.isArray(value) || value instanceof Set) {
    const arr = Array.from(value);
    const out = arr.slice(0, MAX_ARRAY_ITEMS).map((item) => sanitize(item, depth + 1, seen));
    if (arr.length > MAX_ARRAY_ITEMS) out.push(`…[+${arr.length - MAX_ARRAY_ITEMS} items]`);
    return out;
  }

  const entries = value instanceof Map ? Array.from(value.entries()) : Object.entries(value);
  const out = {};
  for (const [k, v] of entries) {
    if (typeof v === 'function') continue;
    out[k] = sanitize(v, depth + 1, seen, String(k));
  }
  return out;
}

function safeStringify(value) {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function formatPrettyValue(value) {
  if (typeof value === 'string') {
    return /^[\w.:/@+\-=,#*[\]()]*$/.test(value) && value !== '' ? value : JSON.stringify(value);
  }
  return safeStringify(value);
}

function write(levelName, component, msg, fields) {
  const ctx = getContext();
  const data = sanitize({ ...ctx, ...fields });
  // Strip line breaks so request values can never forge extra log lines.
  const message = String(msg).replace(/[\r\n]+/g, ' ');
  const stream = LEVELS[levelName] <= LEVELS.warn ? process.stderr : process.stdout;
  const time = new Date().toISOString();

  if (settings.format === 'json') {
    stream.write(`${safeStringify({ time, level: levelName, component, msg: message, ...data })}\n`);
    return;
  }

  const kv = Object.entries(data)
    .filter(([, v]) => v !== undefined)
    .map(([k, v]) => `${k}=${formatPrettyValue(v)}`)
    .join(' ');
  stream.write(`${time} ${levelName.toUpperCase().padEnd(5)} [${component}] ${message}${kv ? ` ${kv}` : ''}\n`);
}

function createLogger(component, baseFields = {}) {
  const emit = (levelName) => (msg, fields) => {
    if (LEVELS[levelName] > settings.level) return;
    try {
      write(levelName, component, msg, { ...baseFields, ...fields });
    } catch (error) {
      process.stderr.write(`logger failure: ${error?.message}\n`);
    }
  };

  return {
    error: emit('error'),
    warn: emit('warn'),
    info: emit('info'),
    debug: emit('debug'),
    trace: emit('trace'),
    isEnabled: (levelName) => LEVELS[levelName] <= settings.level,
    child: (fields) => createLogger(component, { ...baseFields, ...fields }),
  };
}

/** Make a value from a URL/header safe to put in a single log token. */
function cleanId(value) {
  if (value === undefined || value === null) return value;
  return String(value).replace(/[\r\n\s]/g, '').slice(0, 128);
}

module.exports = {
  createLogger,
  runWithContext,
  getContext,
  newRequestId,
  fingerprint,
  preview,
  truncate,
  maskUrl,
  sanitize,
  serializeError,
  cleanId,
  settings,
};
