'use strict';

/**
 * Prospect Discovery capability (SPEC-024).
 * Discovers, verifies, scores, and returns high-quality prospects.
 * Operates from Discovery Profiles — never hardcoded targeting.
 */

const {
  CAPABILITY_CATEGORIES,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  CAPABILITY_RESULT_STATUS,
  PROGRESS_KINDS,
} = require('../types');
const {
  DISCOVERY_PROGRESS_STAGES,
  buildProspect,
  buildProspectDiscoveryResult,
  buildDiscoveryEvidence,
} = require('./types');
const { createDiscoveryProfileStore } = require('./DiscoveryProfileStore');
const { createProfileSelector } = require('./ProfileSelector');
const { rankAgainstProfile } = require('./ranking');
const { verifyCandidate } = require('./verification');
const { dedupeCandidates, flagCrmDuplicates } = require('./dedupe');
const { createPlacesProvider } = require('./providers/PlacesProvider');

/**
 * @param {object} [deps]
 * @param {object} [deps.profileStore]
 * @param {object} [deps.profileSelector]
 * @param {object} [deps.searchProvider] - primary search provider
 * @param {object[]} [deps.searchProviders] - ordered providers
 * @param {(q: object) => Promise<object[]>} [deps.crmLookup]
 */
function createProspectDiscoveryCapability(deps = {}) {
  const profileStore = deps.profileStore || createDiscoveryProfileStore();
  const profileSelector =
    deps.profileSelector || createProfileSelector({ store: profileStore });
  const providers = resolveProviders(deps);
  const crmLookup = deps.crmLookup || null;

  return {
    id: BUILTIN_IDS.PROSPECT_DISCOVERY,
    name: 'Discovering Prospects',
    description:
      'Discover, verify, and rank prospect companies using a Discovery Profile',
    category: CAPABILITY_CATEGORIES.DISCOVERY,
    outcomeTags: ['prospects_discovered', 'companies_found'],
    retryable: true,
    timeoutMs: 180_000,
    supportsRollback: false,
    idempotent: true,
    inputSchema: { required: [] },
    outputSchema: {
      prospects: 'Prospect[]',
      summary: 'object',
      evidence: 'array',
      warnings: 'string[]',
      confidence: 'number',
    },

    canRun() {
      return true;
    },

    estimate(context) {
      const target =
        (context.constraints && Number(context.constraints.targetCount)) || 50;
      const hasLive = providers.some((p) => p.available && p.available());
      return buildCapabilityEstimate({
        durationMs: hasLive ? Math.min(120_000, 3000 + target * 800) : 2500,
        confidence: hasLive ? 0.8 : 0.65,
        notes: hasLive
          ? ['Live discovery provider available']
          : ['No live Places key — inject provider or expect partial results'],
      });
    },

    async execute(context) {
      const emit = makeEmitter(context);
      const warnings = [];
      const rejected = [];

      // ── Profile ──────────────────────────────────────────────
      const selection = profileSelector.select({
        objective: context.objective,
        clientId: context.clientId,
        tenantId: context.tenantId,
        constraints: context.constraints || {},
      });
      const profile = selection.profile;
      const targetCount =
        (context.constraints && Number(context.constraints.targetCount)) ||
        profile.targetCount ||
        50;

      if (selection.selection === 'ambiguous' && selection.alternatives.length) {
        warnings.push(
          `Multiple Discovery Profiles matched; using ${profile.name}. Alternatives: ${selection.alternatives
            .map((a) => a.name)
            .join(', ')}`
        );
      }
      if (selection.selection === 'generated') {
        warnings.push(
          'No library profile matched — generated a temporary Discovery Profile for this mission.'
        );
      }

      // ── Searching ────────────────────────────────────────────
      emit(DISCOVERY_PROGRESS_STAGES.SEARCHING, { phase: 'searching', targetCount });

      const available = providers.filter((p) => !p.available || p.available());
      if (!available.length) {
        warnings.push(
          'No discovery providers available (set GOOGLE_PLACES_KEY or inject a search provider).'
        );
        return finalizeEmpty({
          profile,
          selection,
          targetCount,
          warnings,
          suggestedNextActions: [
            'Configure GOOGLE_PLACES_KEY for live discovery',
            'Or pin a Discovery Profile with an injected search provider',
          ],
        });
      }

      const location =
        profile.geography?.label ||
        (profile.geography?.cities || []).join(', ') ||
        '';
      const industries = profile.industryTargets || [];
      const raw = [];
      const perIndustry = Math.max(
        5,
        Math.ceil((targetCount * 1.5) / Math.max(industries.length, 1))
      );

      for (const industry of industries.length ? industries : ['professional offices']) {
        for (const provider of available) {
          try {
            const hits = await provider.search(
              { industry, location, limit: perIndustry },
              profile
            );
            for (const hit of hits || []) {
              raw.push({
                ...hit,
                industry: hit.industry || industry,
                source: hit.source || provider.id,
              });
            }
          } catch (err) {
            warnings.push(
              `Provider ${provider.id} failed for ${industry}: ${err.message || err}`
            );
          }
        }
      }

      // ── Filtering / Dedup ────────────────────────────────────
      emit(DISCOVERY_PROGRESS_STAGES.FILTERING, {
        phase: 'filtering',
        discovered: raw.length,
      });

      const { unique, duplicatesRemoved } = dedupeCandidates(
        raw,
        profile.deduplicationRules || {}
      );
      if (duplicatesRemoved) {
        warnings.push(`Removed ${duplicatesRemoved} duplicate companies`);
      }

      const crmResult = await flagCrmDuplicates(unique, {
        tenantId: context.tenantId,
        clientId: context.clientId,
        crmLookup,
        respectTenant: profile.deduplicationRules?.respectTenant !== false,
      });
      if (crmResult.warning) warnings.push(crmResult.warning);
      if (crmResult.flagged) {
        warnings.push(`Flagged ${crmResult.flagged} existing CRM matches`);
      }

      // ── Verifying ────────────────────────────────────────────
      emit(DISCOVERY_PROGRESS_STAGES.VERIFYING, {
        phase: 'verifying',
        candidates: unique.length,
      });

      const verified = [];
      for (const candidate of unique) {
        const verification = verifyCandidate(candidate, profile);
        if (!verification.ok) {
          rejected.push({
            companyName: candidate.companyName || candidate.company,
            website: candidate.website || candidate.url || null,
            reasons: verification.failures,
          });
          continue;
        }
        verified.push({ candidate, verification });
      }

      // ── Ranking ──────────────────────────────────────────────
      emit(DISCOVERY_PROGRESS_STAGES.RANKING, {
        phase: 'ranking',
        verified: verified.length,
      });

      const ranked = [];
      for (const { candidate, verification } of verified) {
        const ranking = rankAgainstProfile(candidate, profile);
        if (ranking.excluded) {
          rejected.push({
            companyName: candidate.companyName || candidate.company,
            website: candidate.website || candidate.url || null,
            reasons: [ranking.excludeReason || 'Excluded by profile signal'],
          });
          continue;
        }

        const confidence = Number(
          Math.min(1, (ranking.confidence * 0.7 + verification.confidence * 0.3)).toFixed(4)
        );

        if (
          confidence < (profile.minimumConfidence || 0.75) &&
          profile.reviewPolicy?.returnLowConfidenceForReview
        ) {
          // Keep for review but mark status
          ranked.push({
            candidate,
            verification,
            ranking,
            confidence,
            status: 'needs_review',
          });
          continue;
        }
        if (confidence < (profile.minimumConfidence || 0.75)) {
          rejected.push({
            companyName: candidate.companyName || candidate.company,
            website: candidate.website || candidate.url || null,
            reasons: [
              `Confidence ${confidence} below minimum ${profile.minimumConfidence}`,
            ],
          });
          continue;
        }

        ranked.push({
          candidate,
          verification,
          ranking,
          confidence,
          status: 'verified',
        });
      }

      ranked.sort((a, b) => b.confidence - a.confidence);

      const accepted = ranked.slice(0, targetCount);
      const prospects = accepted.map((row, index) => {
        const c = row.candidate;
        const evidence = buildDiscoveryEvidence({
          whySelected: row.ranking.industryMatch
            ? `Matched ${row.ranking.industryMatch} under ${profile.name}`
            : `Matched Discovery Profile ${profile.name}`,
          sources: [c.source || 'discovery'].concat(c._aliases || []),
          confidence: row.confidence,
          discoveryMethod: c.source || available[0]?.id || 'discovery',
          profileId: profile.id,
          profileVersion: profile.version,
        });
        return buildProspect({
          id: `pdisc_${context.missionId || 'local'}_${index + 1}`,
          companyName: c.companyName || c.company,
          website: c.website || c.url || null,
          industry: row.ranking.industryMatch || c.industry || null,
          address: c.address || null,
          phone: c.phone || null,
          confidence: row.confidence,
          evidence: [evidence],
          rankingSignals: row.ranking.signals,
          discoveryReason: evidence.whySelected,
          status: row.status,
          placeId: c.placeId || null,
          source: c.source || null,
        });
      });

      const overallConfidence =
        prospects.length === 0
          ? 0
          : Number(
              (
                prospects.reduce((s, p) => s + p.confidence, 0) / prospects.length
              ).toFixed(4)
            );

      const shortfall = targetCount - prospects.length;
      const suggestedNextActions = [];
      if (shortfall > 0) {
        warnings.push(
          `Requested ${targetCount} prospects; confidently verified ${prospects.length}.`
        );
        suggestedNextActions.push(
          'Widen geography in the Discovery Profile',
          'Add adjacent industry targets',
          'Lower minimumConfidence with operator review',
          'Approve current set and continue to enrichment'
        );
      }
      suggestedNextActions.push(
        'Approve',
        'Exclude prospect',
        'Lock prospect',
        'Request regeneration',
        'Continue to enrichment'
      );

      const topNotes = prospects.slice(0, 5).map((p) => ({
        companyName: p.companyName,
        confidence: p.confidence,
        reason: p.discoveryReason,
        topSignals: (p.rankingSignals || [])
          .filter((s) => s.matched && s.weight > 0)
          .slice(0, 3)
          .map((s) => s.detail || s.signal),
      }));

      const reviewPackage = {
        summary: {
          profileName: profile.name,
          profileVersion: profile.version,
          companiesSearched: raw.length,
          prospectsAccepted: prospects.length,
          prospectsRejected: rejected.length,
          targetCount,
        },
        rankedList: prospects,
        discoveryNotes: topNotes,
        operatorActions: [
          'approve',
          'exclude_prospect',
          'lock_prospect',
          'request_regeneration',
          'continue_to_enrichment',
        ],
        rejected,
      };

      const result = buildProspectDiscoveryResult({
        prospects,
        summary: {
          discovered: raw.length,
          verified: prospects.length,
          rejected: rejected.length,
          targetCount,
          companiesSearched: raw.length,
        },
        evidence: [
          {
            kind: 'discovery_profile',
            summary: selection.message,
            profileId: profile.id,
            profileVersion: profile.version,
          },
          {
            kind: 'discovery',
            summary: `Discovered ${raw.length}, verified ${prospects.length}, rejected ${rejected.length}`,
          },
        ],
        warnings,
        confidence: overallConfidence,
        discoveryProfile: {
          id: profile.id,
          name: profile.name,
          version: profile.version,
          selection: selection.selection,
        },
        reviewPackage,
        rejected,
        suggestedNextActions,
      });

      emit(DISCOVERY_PROGRESS_STAGES.COMPLETED, {
        phase: 'completed',
        verified: prospects.length,
        targetCount,
      });

      // Shortfall / empty is still a completed discovery pass — never silent fail.
      // Mission continues to review with warnings + suggested next actions.
      const partial = prospects.length === 0 || shortfall > 0;

      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          prospectCount: prospects.length,
          targetCount,
          prospects,
          summary: result.summary,
          discoveryProfile: result.discoveryProfile,
          reviewPackage,
          rejected,
          suggestedNextActions,
          confidence: overallConfidence,
          partial,
        },
        evidence: result.evidence,
        artifacts: [
          { type: 'prospect_list', count: prospects.length },
          {
            type: 'discovery_profile',
            id: profile.id,
            name: profile.name,
            version: profile.version,
          },
          { type: 'review_package', accepted: prospects.length, rejected: rejected.length },
        ],
        warnings,
        nextRecommendations: suggestedNextActions.map((summary) => ({
          action: 'review',
          summary,
        })),
      });
    },
  };
}

function resolveProviders(deps) {
  if (Array.isArray(deps.searchProviders) && deps.searchProviders.length) {
    return deps.searchProviders;
  }
  if (deps.searchProvider) return [deps.searchProvider];
  return [createPlacesProvider()];
}

function makeEmitter(context) {
  return (stage, payload = {}) => {
    if (typeof context.emitProgress === 'function') {
      context.emitProgress({
        kind: PROGRESS_KINDS.PROGRESS,
        stage,
        message: stage,
        ...payload,
      });
    }
  };
}

function finalizeEmpty(input) {
  const reviewPackage = {
    summary: {
      profileName: input.profile.name,
      profileVersion: input.profile.version,
      companiesSearched: 0,
      prospectsAccepted: 0,
      prospectsRejected: 0,
      targetCount: input.targetCount,
    },
    rankedList: [],
    discoveryNotes: [],
    operatorActions: ['request_regeneration'],
    rejected: [],
  };
  return buildCapabilityResult({
    status: CAPABILITY_RESULT_STATUS.COMPLETED,
    outputs: {
      prospectCount: 0,
      targetCount: input.targetCount,
      prospects: [],
      summary: {
        discovered: 0,
        verified: 0,
        rejected: 0,
        targetCount: input.targetCount,
        companiesSearched: 0,
      },
      discoveryProfile: {
        id: input.profile.id,
        name: input.profile.name,
        version: input.profile.version,
        selection: input.selection.selection,
      },
      reviewPackage,
      rejected: [],
      suggestedNextActions: input.suggestedNextActions,
      confidence: 0,
      partial: true,
    },
    evidence: [
      {
        kind: 'discovery_profile',
        summary: input.selection.message,
        profileId: input.profile.id,
        profileVersion: input.profile.version,
      },
      {
        kind: 'discovery',
        summary: 'No prospects discovered — providers unavailable or empty results',
      },
    ],
    warnings: input.warnings,
    artifacts: [
      {
        type: 'discovery_profile',
        id: input.profile.id,
        name: input.profile.name,
        version: input.profile.version,
      },
    ],
    nextRecommendations: (input.suggestedNextActions || []).map((summary) => ({
      action: 'review',
      summary,
    })),
  });
}

module.exports = {
  createProspectDiscoveryCapability,
};
