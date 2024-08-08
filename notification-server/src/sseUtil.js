const { v4: uuidv4 } = require('uuid');
const logger = require('./logger');  // Import the logger

function buildSSEResponse({ res, req, buildCallbackFunction }) {
  addSSEHeader(req, res);
  keepStreamAlive(res);
  const connectionId = generateConnectionID();
  const sender = buildSender(res);

  logger.info(`SSE connection established with Connection ID: ${connectionId} for ${req.originalUrl}`);

  const cleanUp = buildCallbackFunction({ connectionId, sender });

  req.on('close', () => {
    logger.info(`Client with Connection ID: ${connectionId} disconnected from SSE`);
    cleanUp?.();
  });
}

function addSSEHeader(req, res) {
  const origin = extractOrigin(req.headers.origin);

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': origin,
    'Access-Control-Allow-Credentials': true,
    'Access-Control-Expose-Headers': 'Origin, X-Requested-With, Content-Type, Cache-Control, Connection, Accept'
  });

  logger.info(`SSE headers set for Connection ID: ${uuidv4()}`);
}

function extractOrigin(reqOrigin) {
  const corsWhitelist = process.env.CORS_WHITELIST_ORIGINS.split(',');
  const whitelisted = corsWhitelist.indexOf(reqOrigin) !== -1;
  const origin = whitelisted ? reqOrigin : '*';

  logger.info(`Extracted origin: ${origin} (whitelisted: ${whitelisted})`);
  return origin;
}

function keepStreamAlive(res) {
  res.write('');
  logger.debug('Stream kept alive with an empty message');
}

function generateConnectionID() {
  const connectionId = uuidv4();
  logger.info(`New client connected with Connection ID: ${connectionId}`);
  return connectionId;
}

function buildSender(res) {
  return data => {
    try {
      logger.info(`Sending data to client: ${JSON.stringify(data)}`);
      res.write(`data: ${JSON.stringify(data)}\n\n`);
    } catch (error) {
      logger.error('Error sending data:', error);
    }
  };
}

module.exports = {
  buildSSEResponse,
};
