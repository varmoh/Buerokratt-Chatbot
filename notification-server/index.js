require('dotenv').config();
const { openSearchConfig, serverConfig } = require('./src/config');
const { createLogger, maskUrl, settings } = require('./src/logger');
const { client } = require('./src/openSearch');

const log = createLogger('startup');

// Log crashes with full context; does not change Node's default crash behaviour.
process.on('uncaughtExceptionMonitor', (error, origin) => {
  log.error('FATAL uncaught error - process will exit', { origin, error });
});
process.on('warning', (warning) => log.warn('Node warning', { warning }));
process.on('exit', (code) => log.info('Process exiting', { code }));

log.info('Notification server starting', {
  node: process.version,
  port: serverConfig.port,
  refreshIntervalMs: serverConfig.refreshInterval,
  queueRefreshIntervalMs: serverConfig.queueRefreshInterval,
  opensearch: maskUrl(openSearchConfig.getUrl()),
  ruuterPublic: process.env.RUUTER_URL,
  ruuterPrivate: process.env.PRIVATE_RUUTER_URL,
  llmOrchestrator: process.env.LLM_ORCHESTRATOR_URL,
  terminationTimeoutSec: process.env.TERMINATION_TIMEOUT,
  corsWhitelist: process.env.CORS_WHITELIST_ORIGINS,
  azureOpenAI: Boolean(process.env.AZURE_OPENAI_API_KEY),
  azureAgentic: Boolean(process.env.AZURE_AGENTIC_ENDPOINT),
  logLevel: Object.keys({ error: 0, warn: 1, info: 2, debug: 3, trace: 4 })[settings.level],
  logFormat: settings.format,
  logContent: settings.content,
});

(async () => {
  try {
    await client.indices.putSettings({
      index: 'notifications',
      body: {
        refresh_interval: '5s',
      },
    });
    log.info('OpenSearch notifications index settings applied', { refresh_interval: '5s' });

    require('./src/server');
  } catch (error) {
    log.error('Startup failed - server NOT started', { error });
  }
})();
