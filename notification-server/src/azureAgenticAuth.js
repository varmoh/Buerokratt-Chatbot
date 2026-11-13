const { azureAgenticAuthConfig } = require('./config');
const { createLogger } = require('./logger');

const log = createLogger('azure-auth');

const tokenCache = new Map();

async function getAccessToken({ clientId, clientSecret } = {}) {
  const tenantId = azureAgenticAuthConfig.tenantId;
  const grantType = azureAgenticAuthConfig.grantType;
  const scope = azureAgenticAuthConfig.scope;
  const resolvedClientId = clientId || azureAgenticAuthConfig.clientId;
  const resolvedClientSecret = clientSecret || azureAgenticAuthConfig.clientSecret;

  if (!tenantId || !resolvedClientId || !resolvedClientSecret) {
    throw new Error('Azure Agentic authentication credentials are not configured');
  }

  const now = Date.now();
  const cached = tokenCache.get(resolvedClientId);

  if (cached && cached.expireTime - now > 60000) {
    log.debug('Using cached Azure token', {
      clientId: resolvedClientId,
      expiresInSec: Math.round((cached.expireTime - now) / 1000),
    });
    return cached.token;
  }

  log.info('Requesting new Azure token', { clientId: resolvedClientId, tenantId, scope });

  try {
    const tokenUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`;

    const params = new URLSearchParams();
    params.append('grant_type', grantType);
    params.append('client_id', resolvedClientId);
    params.append('client_secret', resolvedClientSecret);
    params.append('scope', scope);

    const response = await fetch(tokenUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: params.toString(),
    });

    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(`Azure token request failed: ${response.status} - ${JSON.stringify(errorData)}`);
    }

    const data = await response.json();
    const expiresIn = data.expires_in || 3600;
    tokenCache.set(resolvedClientId, { token: data.access_token, expireTime: now + expiresIn * 1000 });
    log.info('Azure token obtained', { clientId: resolvedClientId, expiresInSec: expiresIn });
    return data.access_token;
  } catch (error) {
    log.error('Failed to get Azure Agentic access token', { clientId: resolvedClientId, error });
    throw error;
  }
}

module.exports = {
  getAccessToken,
};
