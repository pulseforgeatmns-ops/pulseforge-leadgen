'use strict';

/**
 * SPEC-PAIGE-ANCHOR-SOCIAL-002 — service entry for Anchor organic social pipeline.
 */

const pool = require('../db');
const {
  ANCHOR_CLIENT_ID,
  createPostgresOrganicSocialStore,
  createMemoryOrganicSocialStore,
  runAnchorOrganicSocialCycle,
  maintainAnchorContentBacklog,
  syncAnchorMediaLibrary,
  materializeBacklogIntoGeneration,
  ingestPublicationPerformance,
} = require('../packages/capabilities/paigeOrganicSocial');
const { getClientConfig } = require('../utils/clientContext');

let _store = null;

function getStore(override) {
  if (override) return override;
  if (!_store) _store = createPostgresOrganicSocialStore(pool);
  return _store;
}

function resetPaigeAnchorOrganicSocialForTests() {
  _store = null;
}

async function runOrganicPipeline(input = {}, deps = {}) {
  const clientId = Number(input.client_id ?? input.clientId ?? ANCHOR_CLIENT_ID);
  const store = getStore(deps.store);
  const clientConfig = input.clientConfig || await getClientConfig(clientId);
  return runAnchorOrganicSocialCycle({
    clientId,
    clientConfig,
    folderId: input.folderId,
    minBacklog: input.minBacklog,
    skipMediaSync: input.skipMediaSync,
  }, { ...deps, store });
}

async function syncMedia(input = {}, deps = {}) {
  const clientId = Number(input.client_id ?? input.clientId ?? ANCHOR_CLIENT_ID);
  const store = getStore(deps.store);
  await store.ensureSchema();
  const clientConfig = input.clientConfig || await getClientConfig(clientId);
  return syncAnchorMediaLibrary({ clientId, clientConfig, folderId: input.folderId }, { ...deps, store });
}

async function refreshBacklog(input = {}, deps = {}) {
  const clientId = Number(input.client_id ?? input.clientId ?? ANCHOR_CLIENT_ID);
  const store = getStore(deps.store);
  await store.ensureSchema();
  return maintainAnchorContentBacklog({ clientId, minSize: input.minSize }, { ...deps, store });
}

async function resolveOrganicGenerationContext(input = {}, deps = {}) {
  const clientId = Number(input.client_id ?? input.clientId ?? ANCHOR_CLIENT_ID);
  const store = getStore(deps.store);
  await store.ensureSchema();
  return materializeBacklogIntoGeneration({
    clientId,
    platform: input.platform || input.channel,
  }, { ...deps, store });
}

async function recordOrganicPerformance(input = {}, deps = {}) {
  const store = getStore(deps.store);
  await store.ensureSchema();
  return ingestPublicationPerformance(input, { store });
}

async function getOrganicPlatformSignals(clientId, platform, deps = {}) {
  const store = getStore(deps.store);
  await store.ensureSchema();
  return store.listPlatformSignals(clientId, platform);
}

module.exports = {
  ANCHOR_CLIENT_ID,
  createMemoryOrganicSocialStore,
  getStore,
  resetPaigeAnchorOrganicSocialForTests,
  runOrganicPipeline,
  syncMedia,
  refreshBacklog,
  resolveOrganicGenerationContext,
  recordOrganicPerformance,
  getOrganicPlatformSignals,
};
