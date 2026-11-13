const { AzureOpenAI } = require('openai');

const { createLogger, maskUrl } = require('./logger');

const log = createLogger('azure-openai');

const azureConfig = {
  endpoint: process.env.AZURE_OPENAI_ENDPOINT,
  apiKey: process.env.AZURE_OPENAI_API_KEY,
  deployment: process.env.AZURE_OPENAI_DEPLOYMENT,
  apiVersion: process.env.AZURE_OPENAI_API_VERSION,
  modelName: process.env.AZURE_OPENAI_MODEL,
};

let client = null;

function initializeAzureOpenAI() {
  if (!azureConfig.apiKey) {
    throw new Error('Azure OpenAI API key is required');
  }

  client = new AzureOpenAI({
    endpoint: azureConfig.endpoint,
    apiKey: azureConfig.apiKey,
    deployment: azureConfig.deployment,
    apiVersion: azureConfig.apiVersion,
  });

  log.info('Azure OpenAI client created', {
    endpoint: maskUrl(azureConfig.endpoint),
    deployment: azureConfig.deployment,
    apiVersion: azureConfig.apiVersion,
    model: azureConfig.modelName,
  });
  return client;
}

async function streamAzureOpenAIResponse(messages, options = {}) {
  if (!client) initializeAzureOpenAI();

  const { max_tokens = 4096, temperature = 1, top_p = 1, stream = true, data_sources } = options;

  try {
    const requestConfig = {
      messages,
      stream,
      max_tokens,
      temperature,
      top_p,
      model: azureConfig.modelName,
      data_sources,
    };

    log.info('⇢ chat.completions.create', {
      model: azureConfig.modelName,
      deployment: azureConfig.deployment,
      stream,
      max_tokens,
      temperature,
      top_p,
      messageCount: messages?.length,
      dataSources: data_sources?.map((d) => d?.type),
    });
    log.debug('chat.completions.create request', { requestConfig });

    if (stream) {
      return client.chat.completions.create(requestConfig);
    } else {
      return await client.chat.completions.create(requestConfig);
    }
  } catch (error) {
    log.error('Azure OpenAI API error', { error });
    throw error;
  }
}

module.exports = {
  initializeAzureOpenAI,
  streamAzureOpenAIResponse,
  azureConfig,
};
