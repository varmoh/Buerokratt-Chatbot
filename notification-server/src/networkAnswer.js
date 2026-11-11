// Bürokrati võrgustiku demo: notifications-node lisand (pilt: images/notification/Dockerfile).
//
// 1) B bot vastab sama teega nagu vidinas, aga vastus läheb Ruuterile, mitte brauserisse:
//    POST /network/listen {channelId}             registreerib kanalile koguja (nagu vidina SSE ühendus)
//    (Ruuter kutsub internal/client-external-bot sender=channelId -> POST /channels/:id/stream)
//    POST /network/result {channelId, timeoutMs}  ootab stream_complete ja tagastab terve vastuse
//         -> {status: complete|error|timeout|unknown, content, chunks}
// 2) Valikuline ajapiir A poolel:
//    POST /network/timer {chatId, requestId, seconds, callbackUrl}  seconds pärast POST callbackUrl {chatId, requestId}
//
// Kogujat hoitakse activeConnections-is nagu SSE ühendust, nii et olemasolev voog
// (createAzureOpenAIStreamRequest, deliverResponse, streamQueue) jääb muutmata.
// Sisu ei logita (ainult pikkus).
const { activeConnections } = require('./connectionManager');

const SESSION_TTL_MS = 180000;
const MAX_TIMER_S = 600;
const sessions = new Map(); // channelId -> { connectionId, status, content, chunks, waiters }
const timers = new Map();   // requestId -> timeout

function log(msg) { console.log(`[network] ${msg}`); }
function short(id) { return String(id).slice(0, 8); }

function finish(s, status) {
  if (s.status !== 'listening') return;
  s.status = status;
  activeConnections.delete(s.connectionId);
  log(`channel=${short(s.channelId)} ${status} chunks=${s.chunks} len=${s.content.length} ${Date.now() - s.started} ms`);
  s.waiters.splice(0).forEach((w) => w());
}

function onEvent(s, data) {
  if (!data || typeof data !== 'object') return;
  switch (data.type) {
    case 'stream_start': s.content = ''; s.chunks = 0; break;
    case 'stream_chunk': s.content += data.content || ''; s.chunks++; break;
    case 'stream_complete': finish(s, 'complete'); break;
    case 'complete_response': s.content = data.content || ''; finish(s, 'complete'); break;
    case 'stream_error':
    case 'response_error': s.error = String(data.content || 'error'); finish(s, 'error'); break;
    default: break;
  }
}

function listen(channelId) {
  if (sessions.has(channelId)) return sessions.get(channelId);
  const connectionId = `network-${channelId}-${Date.now()}`;
  const s = { channelId, connectionId, status: 'listening', content: '', chunks: 0, waiters: [], started: Date.now() };
  // Sama kuju nagu sseUtil.buildSSEResponse: { res, sender, channelId, abortControllers }
  activeConnections.set(connectionId, {
    res: null,
    sender: (data) => onEvent(s, data),
    channelId,
    abortControllers: new Set(),
  });
  sessions.set(channelId, s);
  setTimeout(() => { finish(s, 'timeout'); sessions.delete(channelId); }, SESSION_TTL_MS).unref();
  log(`channel=${short(channelId)} listening`);
  return s;
}

async function result(channelId, timeoutMs) {
  const s = sessions.get(channelId);
  if (!s) return { status: 'unknown', content: '' };
  const wait = Math.min(Number(timeoutMs) || 35000, 120000);
  if (s.status === 'listening') {
    await new Promise((resolve) => {
      const t = setTimeout(resolve, wait);
      s.waiters.push(() => { clearTimeout(t); resolve(); });
    });
  }
  if (s.status === 'listening') finish(s, 'timeout');
  sessions.delete(channelId);
  return { status: s.status, content: s.content, chunks: s.chunks, error: s.error };
}

function startTimer({ chatId, requestId, seconds, callbackUrl }) {
  const sec = Math.min(Number(seconds) || 0, MAX_TIMER_S);
  if (!chatId || !requestId || !callbackUrl || sec <= 0) return false;
  if (timers.has(requestId)) clearTimeout(timers.get(requestId));
  timers.set(requestId, setTimeout(async () => {
    timers.delete(requestId);
    try {
      const r = await fetch(callbackUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chatId, requestId }),
      });
      log(`timer request=${short(requestId)} -> ${r.status}`);
    } catch (e) {
      log(`timer request=${short(requestId)} failed: ${e.message}`);
    }
  }, sec * 1000));
  log(`timer request=${short(requestId)} ${sec}s`);
  return true;
}

function register(app) {
  app.post('/network/listen', (req, res) => {
    const { channelId } = req.body || {};
    if (!channelId) return res.status(400).json({ error: 'channelId required' });
    listen(String(channelId));
    return res.status(200).json({ status: 'listening' });
  });
  app.post('/network/result', async (req, res) => {
    const { channelId, timeoutMs } = req.body || {};
    if (!channelId) return res.status(400).json({ error: 'channelId required' });
    return res.status(200).json(await result(String(channelId), timeoutMs));
  });
  app.post('/network/timer', (req, res) => {
    const ok = startTimer(req.body || {});
    return res.status(ok ? 200 : 400).json({ status: ok ? 'started' : 'invalid' });
  });
  log('routes registered: /network/listen, /network/result, /network/timer');
}

module.exports = { register, listen, result, onEvent, startTimer };
