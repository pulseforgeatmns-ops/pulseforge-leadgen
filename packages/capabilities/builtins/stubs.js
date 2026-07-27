'use strict';

/**
 * Built-in capabilities (SPEC-023 + SPEC-024 + SPEC-026).
 * Prospect Discovery + Opportunity Ranking are production.
 * Enrichment / Knowledge / Campaign Builder stay stubs until their SPECs land.
 */

const {
  CAPABILITY_CATEGORIES,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  CAPABILITY_RESULT_STATUS,
} = require('../types');
const {
  createProspectDiscoveryCapability,
  createDiscoveryProfileStore,
  createFixtureProvider,
  manchesterFixtureCandidates,
  createPlacesProvider,
} = require('../discovery');
const { createOpportunityRankingCapability } = require('../ranking');

function stubMeta(partial) {
  return {
    retryable: true,
    timeoutMs: 30_000,
    supportsRollback: false,
    idempotent: true,
    inputSchema: { required: [] },
    outputSchema: {},
    ...partial,
  };
}

/**
 * @deprecated Use createProspectDiscoveryCapability — kept for explicit stub tests.
 */
function createProspectDiscoveryStub() {
  return {
    id: BUILTIN_IDS.PROSPECT_DISCOVERY,
    name: 'Discovering Prospects',
    description: 'Discover qualified prospect companies for the mission objective',
    category: CAPABILITY_CATEGORIES.DISCOVERY,
    outcomeTags: ['prospects_discovered', 'companies_found'],
    ...stubMeta({
      inputSchema: { required: [] },
    }),
    canRun() {
      return true;
    },
    estimate() {
      return buildCapabilityEstimate({
        durationMs: 4000,
        confidence: 0.75,
        notes: ['stub: no live discovery'],
      });
    },
    async execute(context) {
      const target =
        (context.constraints && Number(context.constraints.targetCount)) || 50;
      const count = Math.min(target, 5);
      const prospects = Array.from({ length: count }, (_, i) => ({
        id: `stub_prospect_${i + 1}`,
        companyName: `Stub Company ${i + 1}`,
        priorityScore: 90 - i * 5,
        reasonSelected: 'Matched mission objective (stub discovery)',
        confidence: 0.7,
      }));
      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          prospectCount: prospects.length,
          targetCount: target,
          prospects,
        },
        evidence: [
          {
            kind: 'discovery',
            summary: `Discovered ${prospects.length} prospect stubs for review`,
          },
        ],
        artifacts: [{ type: 'prospect_list', count: prospects.length }],
      });
    },
  };
}

function createCompanyEnrichmentStub() {
  return {
    id: BUILTIN_IDS.COMPANY_ENRICHMENT,
    name: 'Enriching Companies',
    description: 'Enrich discovered companies with contact and firmographic data',
    category: CAPABILITY_CATEGORIES.ENRICHMENT,
    outcomeTags: ['companies_enriched', 'contacts_enriched'],
    ...stubMeta(),
    canRun() {
      return true;
    },
    estimate() {
      return buildCapabilityEstimate({ durationMs: 3000, confidence: 0.8 });
    },
    async execute(context) {
      const prior =
        (context.inputs && context.inputs.prospects) ||
        (context.inputs && context.inputs.priorOutputs && context.inputs.priorOutputs.prospects) ||
        [];
      const enriched = (Array.isArray(prior) ? prior : []).map((p, i) => ({
        ...p,
        email: p.email || `contact${i + 1}@example.com`,
        phone: p.phone || `555-010${i}`,
        website: p.website || `https://example-${i + 1}.com`,
        enriched: true,
      }));
      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: { prospects: enriched, enrichedCount: enriched.length },
        evidence: [
          {
            kind: 'enrichment',
            summary: `Enriched ${enriched.length} companies (stub)`,
          },
        ],
        artifacts: [{ type: 'enriched_list', count: enriched.length }],
      });
    },
  };
}

function createKnowledgeUpdateStub() {
  return {
    id: BUILTIN_IDS.KNOWLEDGE_UPDATE,
    name: 'Updating Knowledge',
    description: 'Persist discovery and enrichment into the knowledge graph',
    category: CAPABILITY_CATEGORIES.INTELLIGENCE,
    outcomeTags: ['knowledge_updated'],
    ...stubMeta(),
    canRun() {
      return true;
    },
    estimate() {
      return buildCapabilityEstimate({ durationMs: 1500, confidence: 0.9 });
    },
    async execute(context) {
      const prospects =
        (context.inputs && context.inputs.prospects) ||
        (context.inputs &&
          context.inputs.priorOutputs &&
          context.inputs.priorOutputs.prospects) ||
        [];
      const n = Array.isArray(prospects) ? prospects.length : 0;
      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          knowledgeNodesWritten: n,
          dualWrite: false,
          stub: true,
        },
        evidence: [
          {
            kind: 'knowledge',
            summary: `Queued ${n} knowledge updates (stub — dual-write adapter later)`,
          },
        ],
      });
    },
  };
}

/**
 * @deprecated Use createOpportunityRankingCapability — kept for explicit stub tests.
 */
function createOpportunityRankingStub() {
  return {
    id: BUILTIN_IDS.OPPORTUNITY_RANKING,
    name: 'Ranking Opportunities',
    description: 'Rank enriched prospects by opportunity score',
    category: CAPABILITY_CATEGORIES.CAMPAIGN,
    outcomeTags: ['opportunities_ranked', 'prospects_ranked'],
    ...stubMeta(),
    canRun() {
      return true;
    },
    estimate() {
      return buildCapabilityEstimate({ durationMs: 2000, confidence: 0.85 });
    },
    async execute(context) {
      const prospects =
        (context.inputs && context.inputs.prospects) ||
        (context.inputs &&
          context.inputs.priorOutputs &&
          context.inputs.priorOutputs.prospects) ||
        [];
      const ranked = (Array.isArray(prospects) ? [...prospects] : [])
        .map((p, i) => ({
          ...p,
          priorityScore:
            p.priorityScore != null
              ? p.priorityScore
              : Math.round((p.confidence || 0.5) * 100) - i,
          rank: i + 1,
        }))
        .sort((a, b) => Number(b.priorityScore) - Number(a.priorityScore))
        .map((p, i) => ({ ...p, rank: i + 1 }));
      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: { prospects: ranked, rankedCount: ranked.length },
        evidence: [
          {
            kind: 'ranking',
            summary: `Ranked ${ranked.length} opportunities (stub)`,
          },
        ],
        artifacts: [{ type: 'ranked_prospects', count: ranked.length }],
      });
    },
  };
}

function createCampaignBuilderStub() {
  return {
    id: BUILTIN_IDS.CAMPAIGN_BUILDER,
    name: 'Building Campaign',
    description: 'Assemble a review-gated campaign draft from ranked prospects',
    category: CAPABILITY_CATEGORIES.CAMPAIGN,
    outcomeTags: ['campaign_drafted', 'campaign_built'],
    ...stubMeta(),
    canRun() {
      return true;
    },
    estimate() {
      return buildCapabilityEstimate({ durationMs: 2500, confidence: 0.8 });
    },
    async execute(context) {
      const objective =
        typeof context.objective === 'string'
          ? context.objective
          : (context.objective && context.objective.text) || 'Campaign';
      const nameMatch = /campaign\s+(\d+|[\w-]+)/i.exec(String(objective));
      const campaignName = nameMatch
        ? `Campaign ${nameMatch[1]}`
        : 'Campaign Draft';
      const prospects =
        (context.inputs && context.inputs.prospects) ||
        (context.inputs &&
          context.inputs.priorOutputs &&
          context.inputs.priorOutputs.prospects) ||
        [];
      const list = Array.isArray(prospects) ? prospects : [];
      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          campaign: {
            name: campaignName,
            status: 'review_required',
            prospectCount: list.length,
            prospects: list,
            mailMerge: list.map((p) => ({
              companyName: p.companyName,
              personalizationSentence: `Noticed ${p.companyName} may need support (stub).`,
              openingHook: 'Quick question about your current vendor setup.',
            })),
          },
          outboundBlocked: true,
        },
        evidence: [
          {
            kind: 'campaign',
            summary: `${campaignName} draft ready for operator review — no outbound actions`,
          },
        ],
        artifacts: [
          {
            type: 'campaign_draft',
            name: campaignName,
            prospectCount: list.length,
          },
        ],
        nextRecommendations: [
          {
            action: 'review',
            summary: 'Review ranked prospects and approve before any outreach',
          },
        ],
      });
    },
  };
}

/**
 * Resolve Prospect Discovery deps for the built-in registry.
 * @param {object} [options]
 */
function resolveDiscoveryDeps(options = {}) {
  const discovery = options.discovery || {};
  const profileStore = discovery.profileStore || createDiscoveryProfileStore();
  const useFixture =
    discovery.useFixture === true ||
    process.env.DISCOVERY_USE_FIXTURE === '1' ||
    process.env.DISCOVERY_USE_FIXTURE === 'true';

  let searchProvider = discovery.searchProvider || null;
  let searchProviders = discovery.searchProviders || null;

  if (!searchProvider && !searchProviders) {
    if (useFixture) {
      searchProvider = createFixtureProvider(manchesterFixtureCandidates());
    } else {
      const places = createPlacesProvider();
      // Prefer live Places; fall back to fixture only when explicitly allowed
      // via DISCOVERY_FIXTURE_FALLBACK (local demos without API keys).
      if (
        !places.available() &&
        (process.env.DISCOVERY_FIXTURE_FALLBACK === '1' ||
          process.env.DISCOVERY_FIXTURE_FALLBACK === 'true')
      ) {
        searchProvider = createFixtureProvider(manchesterFixtureCandidates());
      } else {
        searchProvider = places;
      }
    }
  }

  return {
    profileStore,
    profileSelector: discovery.profileSelector,
    searchProvider,
    searchProviders,
    crmLookup: discovery.crmLookup || null,
  };
}

/**
 * Register all v1 built-ins onto a registry.
 * @param {import('../CapabilityRegistry').CapabilityRegistry} registry
 * @param {object} [options]
 */
function registerBuiltinCapabilities(registry, options = {}) {
  registry.register(createProspectDiscoveryCapability(resolveDiscoveryDeps(options)));
  registry.register(createCompanyEnrichmentStub());
  registry.register(createKnowledgeUpdateStub());
  registry.register(createOpportunityRankingCapability(options.ranking || {}));
  registry.register(createCampaignBuilderStub());
  return registry;
}

/**
 * @param {object} [options]
 * @returns {import('../CapabilityRegistry').CapabilityRegistry}
 */
function createBuiltinRegistry(options = {}) {
  const { createCapabilityRegistry } = require('../CapabilityRegistry');
  const registry = createCapabilityRegistry();
  registerBuiltinCapabilities(registry, options);
  return registry;
}

module.exports = {
  createProspectDiscoveryStub,
  createProspectDiscoveryCapability,
  createCompanyEnrichmentStub,
  createKnowledgeUpdateStub,
  createOpportunityRankingStub,
  createOpportunityRankingCapability,
  createCampaignBuilderStub,
  registerBuiltinCapabilities,
  createBuiltinRegistry,
  resolveDiscoveryDeps,
};
