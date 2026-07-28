'use strict';

/**
 * Business Intelligence Engine capability (SPEC-053 / ADR-037).
 * Produces BusinessIntelligenceProfile artifacts — reason about businesses, not directories.
 */

const {
  CAPABILITY_CATEGORIES,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  CAPABILITY_RESULT_STATUS,
  PROGRESS_KINDS,
} = require('../types');
const { deriveBusinessIntelligenceStage } = require('./reason');
const { applyBusinessIntelligenceGates } = require('./gates');
const { resolvePlaybookFromContext } = require('../playbook');

/**
 * @param {object} context
 * @returns {object[]}
 */
function resolveProspects(context) {
  const inputs = context.inputs || {};
  const prior = inputs.priorOutputs || {};
  const candidates = [
    inputs.prospects,
    prior.prospects,
    inputs.rankedProspects,
  ];
  for (const c of candidates) {
    if (Array.isArray(c) && c.length) return c;
  }
  return Array.isArray(inputs.prospects) ? inputs.prospects : [];
}

/**
 * @param {object} [deps]
 */
function createBusinessIntelligenceCapability(deps = {}) {
  const deriveFn = deps.deriveStage || deriveBusinessIntelligenceStage;

  return {
    id: BUILTIN_IDS.BUSINESS_INTELLIGENCE,
    name: 'Building Business Intelligence',
    description:
      'Derive analytical Business Intelligence Profiles (how the business operates) for ranked prospects',
    category: CAPABILITY_CATEGORIES.INTELLIGENCE,
    outcomeTags: [
      'business_intelligence_ready',
      'business_reasoning_ready',
    ],
    retryable: true,
    timeoutMs: 60_000,
    supportsRollback: false,
    idempotent: true,
    inputSchema: {
      required: [],
      properties: {
        prospects: 'Prospect[]',
        clientPlaybook: 'ClientPlaybook?',
        companyIntelligence: 'object?',
      },
    },
    outputSchema: {
      profiles: 'BusinessIntelligenceProfile[]',
      businessIntelligenceProfiles: 'BusinessIntelligenceProfile[]',
      profileCount: 'number',
      byProspectId: 'object',
    },

    canRun(context) {
      return Array.isArray(resolveProspects(context || {}));
    },

    estimate(context) {
      const n = resolveProspects(context || {}).length;
      return buildCapabilityEstimate({
        durationMs: Math.min(20_000, 500 + n * 30),
        confidence: n > 0 ? 0.9 : 0.4,
        notes: n
          ? [`Derive business intelligence for ${n} prospect(s)`]
          : ['No prospects — complete Ranking first'],
      });
    },

    async execute(context, runtime = {}) {
      const started = Date.now();
      const emit = (stage, pct) => {
        if (typeof runtime.onProgress === 'function') {
          runtime.onProgress({
            kind: PROGRESS_KINDS.PROGRESS,
            stage,
            percent: pct,
            message: stage,
          });
        }
      };

      emit('Gathering Level-1 facts', 10);
      const prospects = resolveProspects(context);
      const playbook = resolvePlaybookFromContext(context);
      const inputs = context.inputs || {};
      const warnings = [];

      if (!prospects.length) {
        warnings.push(
          'No prospects for Business Intelligence — complete Discovery / Ranking first'
        );
      }

      emit('Reasoning business model', 40);
      const { profiles: rawProfiles, byProspectId } = deriveFn(prospects, {
        playbook,
        knowledge: context.knowledge || {},
        companyIntelligence: inputs.companyIntelligence || null,
        asOf: context.asOf,
      });

      emit('Applying quality gates', 75);
      const profiles = rawProfiles.map((raw) =>
        applyBusinessIntelligenceGates(raw)
      );

      /** @type {Record<string, object>} */
      const map = {};
      for (const profile of profiles) {
        if (profile.prospectId) map[profile.prospectId] = profile;
        if (profile.company) {
          map[`company:${profile.company.toLowerCase()}`] = profile;
        }
      }

      const uncertainCount = profiles.filter(
        (p) => (p.uncertainty || []).length > 0
      ).length;
      if (uncertainCount > 0) {
        warnings.push(
          `${uncertainCount} profile(s) expose explicit uncertainty — Sales Intelligence must not invent certainty`
        );
      }

      emit('Completed', 100);

      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          profiles,
          businessIntelligenceProfiles: profiles,
          business_intelligence_profile: {
            profiles,
            profileCount: profiles.length,
          },
          profileCount: profiles.length,
          byProspectId: map,
          prospects: prospects.map((p) => {
            const id = p.id != null ? String(p.id) : null;
            const profile =
              (id && map[id]) ||
              map[`company:${String(p.companyName || '').toLowerCase()}`] ||
              null;
            return profile
              ? { ...p, businessIntelligenceProfile: profile }
              : p;
          }),
        },
        evidence: [
          {
            kind: 'business_intelligence',
            summary: `Derived ${profiles.length} Business Intelligence Profile(s); ${uncertainCount} with explicit uncertainty`,
          },
        ],
        artifacts: [
          {
            type: 'business_intelligence_profile',
            count: profiles.length,
          },
        ],
        warnings,
        duration: Date.now() - started,
        nextRecommendations: [
          {
            action: 'sales_intelligence',
            summary:
              'Derive Sales Intelligence strategy from Business Intelligence reasoning',
          },
        ],
      });
    },
  };
}

module.exports = {
  createBusinessIntelligenceCapability,
  resolveProspects,
};
