'use strict';

/**
 * Sales Intelligence Engine capability (SPEC-048 / ADR-032).
 * Produces SalesIntelligenceProfile artifacts — strategy before language.
 */

const {
  CAPABILITY_CATEGORIES,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  CAPABILITY_RESULT_STATUS,
  PROGRESS_KINDS,
} = require('../types');
const { deriveSalesIntelligenceStage } = require('./derive');
const { applyProfileGates } = require('./gates');
const { attachOperatorConfidence } = require('./humanTest');
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
function createSalesIntelligenceCapability(deps = {}) {
  const deriveFn = deps.deriveStage || deriveSalesIntelligenceStage;

  return {
    id: BUILTIN_IDS.SALES_INTELLIGENCE,
    name: 'Building Sales Intelligence',
    description:
      'Derive structured Sales Intelligence Profiles (strategy before language) for ranked prospects',
    category: CAPABILITY_CATEGORIES.INTELLIGENCE,
    outcomeTags: [
      'sales_intelligence_ready',
      'messaging_strategy_ready',
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
      profiles: 'SalesIntelligenceProfile[]',
      salesIntelligenceProfiles: 'SalesIntelligenceProfile[]',
      profileCount: 'number',
      sendableCount: 'number',
      byProspectId: 'object',
    },

    canRun(context) {
      return Array.isArray(resolveProspects(context || {}));
    },

    estimate(context) {
      const n = resolveProspects(context || {}).length;
      return buildCapabilityEstimate({
        durationMs: Math.min(20_000, 600 + n * 35),
        confidence: n > 0 ? 0.9 : 0.4,
        notes: n
          ? [`Derive sales intelligence for ${n} prospect(s)`]
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

      emit('Gathering prospect context', 10);
      const prospects = resolveProspects(context);
      const playbook = resolvePlaybookFromContext(context);
      const inputs = context.inputs || {};
      const prior = inputs.priorOutputs || {};
      const warnings = [];

      if (!prospects.length) {
        warnings.push(
          'No prospects for Sales Intelligence — complete Discovery / Ranking first'
        );
      }

      emit('Deriving sales profiles', 45);
      const biMap =
        inputs.businessIntelligenceByProspectId ||
        prior.businessIntelligenceByProspectId ||
        {};
      const biProfiles = Array.isArray(
        inputs.businessIntelligenceProfiles || prior.businessIntelligenceProfiles
      )
        ? inputs.businessIntelligenceProfiles || prior.businessIntelligenceProfiles
        : [];

      const enrichedProspects = prospects.map((p) => {
        if (p.businessIntelligenceProfile || p.businessIntelligence) return p;
        const id = p.id != null ? String(p.id) : null;
        const fromMap =
          (id && biMap[id]) ||
          biMap[`company:${String(p.companyName || '').toLowerCase()}`] ||
          null;
        if (fromMap) {
          return { ...p, businessIntelligenceProfile: fromMap };
        }
        const fromList = biProfiles.find(
          (b) =>
            (id && String(b.prospectId) === id) ||
            String(b.company || '').toLowerCase() ===
              String(p.companyName || '').toLowerCase()
        );
        return fromList
          ? { ...p, businessIntelligenceProfile: fromList }
          : p;
      });

      const { profiles: rawProfiles } = deriveFn(enrichedProspects, {
        playbook,
        knowledge: context.knowledge || {},
        companyIntelligence: inputs.companyIntelligence || null,
        asOf: context.asOf,
        opportunityBriefs: inputs.opportunityBriefs || prior.opportunityBriefs,
      });

      emit('Applying quality gates', 75);
      const profiles = rawProfiles.map((raw) => {
        const prospect = enrichedProspects.find(
          (p) =>
            (raw.prospectId && String(p.id) === String(raw.prospectId)) ||
            String(p.companyName || '') === raw.company
        );
        const brief =
          (prospect && prospect.opportunityBrief) ||
          (inputs.opportunityBriefs &&
            inputs.opportunityBriefs[raw.prospectId]) ||
          null;
        const gated = applyProfileGates(
          {
            ...raw,
          },
          { playbook, opportunityBrief: brief }
        );
        return attachOperatorConfidence(gated);
      });

      // Refresh lookup map after gates
      /** @type {Record<string, object>} */
      const map = {};
      for (const profile of profiles) {
        if (profile.prospectId) map[profile.prospectId] = profile;
        if (profile.company) {
          map[`company:${profile.company.toLowerCase()}`] = profile;
        }
      }

      const sendableCount = profiles.filter((p) => p.sendable).length;
      const blocked = profiles.length - sendableCount;
      if (blocked > 0) {
        warnings.push(
          `${blocked} profile(s) non-sendable after quality gates — generators must not emit outreach`
        );
      }

      emit('Completed', 100);

      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          profiles,
          salesIntelligenceProfiles: profiles,
          sales_intelligence_profile: {
            profiles,
            profileCount: profiles.length,
            sendableCount,
          },
          profileCount: profiles.length,
          sendableCount,
          byProspectId: map,
          // Keep ranked prospects available for downstream stages
          prospects: enrichedProspects.map((p) => {
            const id = p.id != null ? String(p.id) : null;
            const profile =
              (id && map[id]) ||
              map[`company:${String(p.companyName || '').toLowerCase()}`] ||
              null;
            return profile
              ? { ...p, salesIntelligenceProfile: profile }
              : p;
          }),
        },
        evidence: [
          {
            kind: 'sales_intelligence',
            summary: `Derived ${profiles.length} Sales Intelligence Profile(s); ${sendableCount} sendable`,
          },
        ],
        artifacts: [
          {
            type: 'sales_intelligence_profile',
            count: profiles.length,
            sendableCount,
          },
        ],
        warnings,
        duration: Date.now() - started,
        nextRecommendations: [
          {
            action: 'campaign',
            summary:
              'Build campaign and mail packages from Sales Intelligence messaging strategy',
          },
        ],
      });
    },
  };
}

module.exports = {
  createSalesIntelligenceCapability,
  resolveProspects,
};
