'use strict';

/**
 * SPEC-143 — Load acquisition intelligence memory before new investigations.
 */

const { createMemoryIntelligenceStore } = require('./MemoryStore');
const { buildInvestigationStartingPoint } = require('./InvestigationStartingPoint');
const { computeEffectiveConfidence } = require('./MemoryConfidence');
const { asText } = require('./types');

let defaultStore = null;

function getDefaultStore(opts = {}) {
  if (opts.store) return opts.store;
  if (!defaultStore) defaultStore = createMemoryIntelligenceStore();
  return defaultStore;
}

function setDefaultStore(store) {
  defaultStore = store;
}

/**
 * Load tenant-scoped intelligence memory for a market investigation.
 * @param {object} input
 * @param {string} input.tenantId
 * @param {object} input.marketDefinition
 * @param {object} [input.opts]
 * @returns {Promise<object>}
 */
async function loadIntelligenceMemory(input = {}) {
  const tenantId = asText(input.tenantId || input.mission?.tenantId || input.mission?.clientId);
  const market = input.marketDefinition || {};
  const geography = asText(market.geography);
  const segment = asText(market.segment || market.segments?.[0]);
  const store = getDefaultStore(input.opts || {});

  if (!tenantId) {
    return {
      tenantId: null,
      marketKey: null,
      market: null,
      investigation: null,
      companies: [],
      people: [],
      claims: [],
      loaded: false,
    };
  }

  const memory = await store.loadForMarket(tenantId, geography, segment);

  const enrichedClaims = (memory.claims || []).map((claim) => ({
    ...claim,
    effectiveConfidence: computeEffectiveConfidence(claim, input.opts),
  }));

  return {
    ...memory,
    claims: enrichedClaims,
    loaded: true,
    geography,
    segment,
  };
}

/**
 * Load memory and build investigation starting point in one call.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function prepareInvestigationWithMemory(input = {}) {
  const memory = await loadIntelligenceMemory(input);
  const startingPoint = buildInvestigationStartingPoint(
    memory,
    input.marketDefinition || {},
    input.candidates || [],
    input.opts || {}
  );

  return {
    memory,
    startingPoint,
    hasPriorKnowledge:
      startingPoint.counts.known > 0 ||
      startingPoint.counts.skippedSteps > 0 ||
      (memory.companies || []).length > 0 ||
      (memory.market?.entities || []).length > 0 ||
      Boolean(memory.market?.historicalSnapshots?.length),
  };
}

module.exports = {
  loadIntelligenceMemory,
  prepareInvestigationWithMemory,
  getDefaultStore,
  setDefaultStore,
};
