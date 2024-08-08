require('dotenv').config();
const { client } = require('./src/openSearch');
const logger = require('./src/logger');  // Import the logger

(async () => {
  try {
    await client.indices.putSettings({
      index: 'notifications',
      body: {
        refresh_interval: '5s',
      },
    });

    logger.info('OpenSearch index settings updated successfully.');

    require('./src/server');
  } catch (error) {
    logger.error('Error updating OpenSearch index settings:', error);
  }
})();
