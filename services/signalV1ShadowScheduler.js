'use strict';

const { ProspectivePostgresStore } = require('../packages/signal-v1/storage/ProspectivePostgresStore');
const {
  createShadowModeServiceFromStore,
  startShadowScheduler,
} = require('../packages/signal-v1/prospective/shadowScheduler');
const { GeckoTerminalMarketDataProvider } = require('../packages/signal-v1/providers/GeckoTerminalMarketDataProvider');

/** @type {Promise<{ service: import('../packages/signal-v1/prospective/ShadowModeService').ShadowModeService, stop?: Function }>|null} */
let bootPromise = null;

async function bootShadowScheduler() {
  if (!process.env.DATABASE_URL) return null;
  if (process.env.SIGNAL_SHADOW_MODE !== '1' && process.env.SIGNAL_SHADOW_MODE !== 'true') {
    return null;
  }
  const pool = require('../db');
  const store = await ProspectivePostgresStore.create(pool);
  const marketProvider = new GeckoTerminalMarketDataProvider();
  const service = await createShadowModeServiceFromStore(store, {
    marketProvider,
    requiredCallerSource: {
      sourceId: process.env.SIGNAL_REQUIRED_CALLER_SOURCE_ID || 'telegram-front-runners',
      channelId: process.env.SIGNAL_REQUIRED_CALLER_CHANNEL_ID || null,
    },
    providerVersions: { market: marketProvider.providerId },
  });
  const gate=process.env.SIGNAL_OPERATION_MODE ? require('./signalOperator/operationGate').operationFromEnv() : null;
  const stop = startShadowScheduler(service, { intervalMs: Number(process.env.SIGNAL_SHADOW_POLL_MS || 60000),gate });
  console.log('[signal-v1-shadow] prospective scheduler started');
  return { service, stop };
}

function startSignalV1ShadowScheduler() {
  if (bootPromise) return bootPromise;
  bootPromise = bootShadowScheduler().catch(err => {
    console.error('[signal-v1-shadow] failed to start:', err.message);
    return null;
  });
  return bootPromise;
}

module.exports = {
  startSignalV1ShadowScheduler,
};
