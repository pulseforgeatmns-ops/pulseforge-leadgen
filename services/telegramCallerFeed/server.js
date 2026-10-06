'use strict';

require('dotenv').config();

const express = require('express');
const { createTelegramCallerFeedEngine } = require('./engine');
const { buildFeedPayload } = require('./normalize');
const { loadTelegramCredentials } = require('./credentials');

const PORT = Number(process.env.TELEGRAM_CALLER_FEED_PORT || process.env.PORT || 3099);

function createApp(engine = createTelegramCallerFeedEngine()) {
  const app = express();
  app.disable('x-powered-by');

  app.get('/health', async (req, res) => {
    const creds = engine.credentialsStatus();
    let poll = { connected: false, errors: [] };
    if (creds.ok) {
      poll = await engine.pollOnce();
    }
    const health = engine.getHealth({
      connected: creds.ok && poll.connected,
      errors: poll.errors,
    });
    const status = creds.ok ? 200 : 503;
    res.status(status).json(health);
  });

  app.get('/feed', async (req, res) => {
    const creds = loadTelegramCredentials();
    if (!creds.ok) {
      return res.status(503).json({
        error: 'credentials_missing',
        message: creds.reason,
        calls: [],
      });
    }
    const poll = await engine.pollOnce();
    const calls = engine.getRecentCalls();
    const health = engine.getHealth({ connected: poll.connected, errors: poll.errors });
    res.json(buildFeedPayload(calls, health));
  });

  app.get('/sources', (req, res) => {
    const health = engine.getHealth();
    res.json({ sources: health.sources || [] });
  });

  return app;
}

async function main() {
  const creds = loadTelegramCredentials();
  if (!creds.ok) {
    console.error(`[telegram-caller-feed] fail closed: ${creds.reason}`);
    process.exit(1);
  }
  const engine = createTelegramCallerFeedEngine();
  engine.startPolling();
  const app = createApp(engine);
  app.listen(PORT, () => {
    console.log(`[telegram-caller-feed] listening on ${PORT}`);
  });
}

if (require.main === module) {
  main().catch(err => {
    console.error('[telegram-caller-feed] fatal:', err.message);
    process.exit(1);
  });
}

module.exports = {
  createApp,
  createTelegramCallerFeedEngine,
};
