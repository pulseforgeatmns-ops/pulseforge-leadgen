'use strict';

/**
 * Built-in capabilities (SPEC-023 + SPEC-024 + SPEC-026 + SPEC-027B).
 * Prospect Discovery + Opportunity Ranking + Proposal Generator are production.
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
const { createProposalGeneratorCapability } = require('../proposal');

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
  const {
    resolvePlaybookFromContext,
    campaignStrategyFromPlaybook,
    applyPlaybookConstraints,
  } = require('../playbook');

  return {
    id: BUILTIN_IDS.CAMPAIGN_BUILDER,
    name: 'Building Campaign',
    description:
      'Assemble a review-gated campaign draft from ranked prospects using the Client Playbook',
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

      // SPEC-028 / ADR-015: strategy from Client Playbook — no hardcoded outreach
      const playbook = resolvePlaybookFromContext(context);
      const strategy = campaignStrategyFromPlaybook(playbook);
      const applied = applyPlaybookConstraints(list, playbook);
      const kept = applied.prospects;

      const prior =
        (context.inputs && context.inputs.priorOutputs) || {};
      const profileMap =
        (context.inputs && context.inputs.salesIntelligenceByProspectId) ||
        prior.salesIntelligenceByProspectId ||
        {};
      const profiles =
        (context.inputs && context.inputs.salesIntelligenceProfiles) ||
        prior.salesIntelligenceProfiles ||
        prior.profiles ||
        [];
      const {
        openingFromProfile,
      } = require('../salesIntelligence/derive');

      const mailMerge = kept.map((p) => {
        let messagingPosture = p.messagingPosture || null;
        let messagingDescription = p.messagingDescription || null;
        let activeSignals = Array.isArray(p.activeSignals) ? p.activeSignals : null;
        if (!messagingPosture || !activeSignals) {
          try {
            const {
              buildBusinessSignalsForProspect,
            } = require('../signals');
            const pkg = buildBusinessSignalsForProspect(p, {
              playbook,
              profile: context.inputs && context.inputs.discoveryProfile,
            });
            activeSignals = pkg.activeSignals;
            messagingPosture = pkg.messagingPosture;
            messagingDescription = pkg.messagingDescription;
          } catch {
            // signals optional — campaign stub remains review-gated without them
          }
        }

        const salesProfile =
          p.salesIntelligenceProfile ||
          (p.id != null && profileMap[String(p.id)]) ||
          profileMap[`company:${String(p.companyName || '').toLowerCase()}`] ||
          (Array.isArray(profiles)
            ? profiles.find(
                (pr) =>
                  (p.id != null && String(pr.prospectId) === String(p.id)) ||
                  String(pr.company || '').toLowerCase() ===
                    String(p.companyName || '').toLowerCase()
              )
            : null) ||
          null;

        const fromProfile = salesProfile
          ? {
              personalizationSentence: openingFromProfile(salesProfile),
              openingHook:
                (salesProfile.messaging_strategy &&
                  salesProfile.messaging_strategy.cta) ||
                salesProfile.call_to_action ||
                null,
              recommendedOffer:
                salesProfile.call_to_action || p.recommendedOffer || null,
              messagingPosture:
                messagingPosture ||
                (salesProfile.messaging_strategy &&
                  salesProfile.messaging_strategy.opening_focus) ||
                null,
              recommendedAngle: salesProfile.recommended_angle || null,
              salesIntelligenceProfileId: salesProfile.prospectId || null,
              operatorConfidence:
                salesProfile.operatorConfidence &&
                salesProfile.operatorConfidence.overall != null
                  ? salesProfile.operatorConfidence.overall
                  : null,
              sendable: salesProfile.sendable !== false,
            }
          : null;

        return {
          companyName: p.companyName,
          prospectId: p.id != null ? String(p.id) : null,
          personalizationSentence:
            (fromProfile && fromProfile.personalizationSentence) ||
            p.personalizationSentence ||
            (playbook
              ? null
              : `Noticed ${p.companyName} may need support (no playbook).`),
          openingHook:
            (fromProfile && fromProfile.openingHook) ||
            p.openingHook ||
            (playbook ? null : 'Confirm outreach language after playbook is set.'),
          recommendedOffer:
            (fromProfile && fromProfile.recommendedOffer) ||
            p.recommendedOffer ||
            null,
          recommendedChannel: p.recommendedChannel || null,
          recommendedAngle:
            (fromProfile && fromProfile.recommendedAngle) || null,
          messagingPosture:
            (fromProfile && fromProfile.messagingPosture) || messagingPosture,
          messagingDescription,
          activeSignalTitles: (activeSignals || []).map((s) => s.title),
          playbookId: playbook ? playbook.id : null,
          playbookVersion: playbook ? playbook.version : null,
          salesIntelligenceProfileId:
            (fromProfile && fromProfile.salesIntelligenceProfileId) || null,
          operatorConfidence:
            (fromProfile && fromProfile.operatorConfidence) || null,
          sendable: fromProfile ? fromProfile.sendable : true,
          salesIntelligence: salesProfile || null,
        };
      });

      const warnings = [...(applied.warnings || [])];
      if (!playbook) {
        warnings.push(
          'No Client Playbook pinned — campaign draft lacks channel/sequence strategy (ADR-015).'
        );
      }

      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          campaign: {
            name: campaignName,
            status: 'review_required',
            prospectCount: kept.length,
            prospects: kept,
            excludedProspects: applied.excluded,
            mailMerge,
            playbook: strategy,
            preferredChannels: strategy ? strategy.preferredChannels : [],
            outreachSequence: strategy ? strategy.outreachSequence : [],
            offers: strategy ? strategy.offers : [],
            constraints: strategy ? strategy.constraints : [],
          },
          clientPlaybook: playbook,
          clientPlaybookId: playbook ? playbook.id : null,
          clientPlaybookVersion: playbook ? playbook.version : null,
          outboundBlocked: true,
        },
        evidence: [
          {
            kind: 'campaign',
            summary: playbook
              ? `${campaignName} draft from playbook ${playbook.name} v${playbook.version} — no outbound actions`
              : `${campaignName} draft ready for operator review — no outbound actions`,
            playbookId: playbook ? playbook.id : null,
            playbookVersion: playbook ? playbook.version : null,
          },
        ],
        artifacts: [
          {
            type: 'campaign_draft',
            name: campaignName,
            prospectCount: kept.length,
            playbookId: playbook ? playbook.id : null,
            playbookVersion: playbook ? playbook.version : null,
          },
        ],
        warnings,
        nextRecommendations: [
          {
            action: 'review',
            summary: playbook
              ? 'Review playbook-driven sequence, constraints, and prospects before any outreach'
              : 'Attach a Client Playbook, then review ranked prospects before outreach',
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
  const { withArtifactContracts } = require('../artifactContracts');
  const register = (cap) => registry.register(withArtifactContracts(cap));

  register(createProspectDiscoveryCapability(resolveDiscoveryDeps(options)));
  register(createCompanyEnrichmentStub());
  register(createKnowledgeUpdateStub());
  register(createOpportunityRankingCapability(options.ranking || {}));
  const {
    createBusinessIntelligenceCapability,
  } = require('../businessIntelligence');
  register(
    createBusinessIntelligenceCapability(options.businessIntelligence || {})
  );
  const {
    createSalesIntelligenceCapability,
  } = require('../salesIntelligence');
  register(
    createSalesIntelligenceCapability(options.salesIntelligence || {})
  );
  register(createCampaignBuilderStub());
  register(
    createProposalGeneratorCapability(options.proposal || {})
  );
  const {
    createMailPackageGeneratorCapability,
  } = require('../mail');
  register(
    createMailPackageGeneratorCapability(options.mail || {})
  );
  const {
    createCampaignReviewCapability,
  } = require('../campaignReview');
  register(
    createCampaignReviewCapability(options.campaignReview || {})
  );
  const {
    createDirectMailExecutionCapability,
  } = require('../directMailExecution');
  register(
    createDirectMailExecutionCapability(options.directMailExecution || {})
  );
  const {
    createOutcomeIntelligenceCapability,
  } = require('../outcomeIntelligence');
  register(
    createOutcomeIntelligenceCapability(options.outcomeIntelligence || {})
  );
  const {
    createOperatorInboxCapability,
  } = require('../operatorInbox');
  register(
    createOperatorInboxCapability(options.operatorInbox || {})
  );
  const {
    createDiscoveryDiagnosticsCapability,
  } = require('../discovery/DiscoveryDiagnostics');
  register(
    createDiscoveryDiagnosticsCapability(options.discoveryDiagnostics || {})
  );
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
  createBusinessIntelligenceCapability: require('../businessIntelligence')
    .createBusinessIntelligenceCapability,
  createSalesIntelligenceCapability: require('../salesIntelligence')
    .createSalesIntelligenceCapability,
  createCampaignBuilderStub,
  createProposalGeneratorCapability,
  createMailPackageGeneratorCapability: require('../mail')
    .createMailPackageGeneratorCapability,
  createCampaignReviewCapability: require('../campaignReview')
    .createCampaignReviewCapability,
  createDirectMailExecutionCapability: require('../directMailExecution')
    .createDirectMailExecutionCapability,
  createOutcomeIntelligenceCapability: require('../outcomeIntelligence')
    .createOutcomeIntelligenceCapability,
  createOperatorInboxCapability: require('../operatorInbox')
    .createOperatorInboxCapability,
  createDiscoveryDiagnosticsCapability: require('../discovery/DiscoveryDiagnostics')
    .createDiscoveryDiagnosticsCapability,
  registerBuiltinCapabilities,
  createBuiltinRegistry,
  resolveDiscoveryDeps,
};
