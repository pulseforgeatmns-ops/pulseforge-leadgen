'use strict';

/**
 * Opportunity Ranking capability (SPEC-026).
 * Answers: "Who should we contact first?"
 * Evidence-only, explainable scores — no black box.
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
  RANKING_PROGRESS_STAGES,
  OPERATOR_ACTIONS,
  priorityFromScore,
  buildRankedOpportunity,
} = require('./types');
const { scoreOpportunity } = require('./factors');
const { buildBrief, recommendNextAction } = require('./brief');
const {
  buildBusinessSignalsForProspect,
} = require('../signals');

/**
 * Resolve Discovery Profile from context inputs / constraints / knowledge.
 * @param {object} context
 * @returns {object|null}
 */
function resolveProfile(context) {
  const inputs = context.inputs || {};
  const constraints = context.constraints || {};
  if (inputs.discoveryProfile && typeof inputs.discoveryProfile === 'object') {
    return inputs.discoveryProfile;
  }
  if (inputs.profile && typeof inputs.profile === 'object') {
    return inputs.profile;
  }
  const prior = inputs.priorOutputs || {};
  if (prior.discoveryProfile && typeof prior.discoveryProfile === 'object') {
    return prior.discoveryProfile;
  }
  if (constraints.discoveryProfile && typeof constraints.discoveryProfile === 'object') {
    return constraints.discoveryProfile;
  }
  return null;
}

/**
 * Collect prospect list from enrichment / discovery outputs.
 * @param {object} context
 * @returns {object[]}
 */
function resolveProspects(context) {
  const inputs = context.inputs || {};
  const prior = inputs.priorOutputs || {};
  const candidates = [
    inputs.prospects,
    inputs.enrichedProspects,
    prior.prospects,
    context.knowledge && context.knowledge.prospects,
  ];
  for (const c of candidates) {
    if (Array.isArray(c) && c.length) return c;
  }
  return Array.isArray(inputs.prospects) ? inputs.prospects : [];
}

/**
 * @param {object} context
 * @returns {object[]}
 */
function resolveHistoricalOutcomes(context) {
  const inputs = context.inputs || {};
  if (Array.isArray(inputs.historicalOutcomes)) return inputs.historicalOutcomes;
  if (Array.isArray(context.knowledge?.historicalOutcomes)) {
    return context.knowledge.historicalOutcomes;
  }
  if (Array.isArray(context.knowledge?.outcomes)) return context.knowledge.outcomes;
  return [];
}

/**
 * @param {object} [deps]
 */
function createOpportunityRankingCapability(deps = {}) {
  return {
    id: BUILTIN_IDS.OPPORTUNITY_RANKING,
    name: 'Ranking Opportunities',
    description:
      'Rank enriched prospects into an explainable priority queue with opportunity briefs',
    category: CAPABILITY_CATEGORIES.CAMPAIGN,
    outcomeTags: ['opportunities_ranked', 'prospects_ranked'],
    retryable: true,
    timeoutMs: 60_000,
    supportsRollback: false,
    idempotent: true,
    inputSchema: {
      required: [],
      properties: {
        prospects: 'Prospect[]',
        discoveryProfile: 'DiscoveryProfile?',
        historicalOutcomes: 'Outcome[]?',
      },
    },
    outputSchema: {
      prospects: 'RankedOpportunity[]',
      rankedCount: 'number',
      summary: 'object',
      reviewPackage: 'object',
      confidence: 'number',
    },

    canRun(context) {
      const prospects = resolveProspects(context || {});
      return Array.isArray(prospects);
    },

    estimate(context) {
      const n = resolveProspects(context || {}).length;
      return buildCapabilityEstimate({
        durationMs: Math.min(30_000, 800 + n * 40),
        confidence: n > 0 ? 0.9 : 0.5,
        notes:
          n > 0
            ? [`Rank ${n} enriched prospects with explainable factors`]
            : ['No prospects in inputs — ranking will return empty queue'],
      });
    },

    async execute(context) {
      const emit = makeEmitter(context);
      const warnings = [];
      const profile = resolveProfile(context);
      const prospects = resolveProspects(context);
      const historicalOutcomes = resolveHistoricalOutcomes(context);
      const knowledge = context.knowledge || {};

      if (!profile) {
        warnings.push(
          'No Discovery Profile in inputs — profile-match / geography use discovery signals only'
        );
      }
      if (!prospects.length) {
        warnings.push('No prospects to rank — complete Discovery and Enrichment first');
      }

      emit(RANKING_PROGRESS_STAGES.SCORING, {
        phase: 'scoring',
        prospectCount: prospects.length,
      });

      const scoredRows = prospects.map((p) => {
        const scored = scoreOpportunity(p, {
          profile,
          knowledge,
          historicalOutcomes,
        });
        return { prospect: p, scored };
      });

      emit(RANKING_PROGRESS_STAGES.BRIEFING, {
        phase: 'briefing',
        prospectCount: scoredRows.length,
      });

      const ranked = scoredRows.map((row, index) => {
        const { prospect: p, scored } = row;
        const priority = priorityFromScore(scored.overallScore);
        const withPriority = { ...scored, priority };
        const signalPkg = buildBusinessSignalsForProspect(p, {
          knowledge,
          profile,
          asOf: context.asOf,
        });
        const brief = buildBrief(p, withPriority, {
          profile,
          knowledge,
          activeSignals: signalPkg.activeSignals,
        });
        const recommendedNextAction = recommendNextAction(withPriority, p);
        return buildRankedOpportunity({
          id: p.id || `ranked_${context.missionId || 'local'}_${index + 1}`,
          companyName: p.companyName || p.company || '',
          website: p.website || null,
          industry: p.industry || null,
          address: p.address || null,
          email: p.email || null,
          phone: p.phone || null,
          overallScore: scored.overallScore,
          priority,
          confidence: scored.confidence,
          topReasons: scored.topReasons,
          risks: scored.risks,
          recommendedNextAction,
          factorScores: scored.factorScores,
          opportunityBrief: brief,
          discoveryConfidence: Number.isFinite(Number(p.confidence))
            ? Number(p.confidence)
            : null,
          rankingSignals: p.rankingSignals || [],
          source: p.source || null,
          enriched: p.enriched === true,
          businessSignals: signalPkg.signals,
          activeSignals: signalPkg.activeSignals,
          buyingSignals: signalPkg.buyingSignals,
          operatorSignals: signalPkg.operatorSignals,
          messagingPosture: signalPkg.messagingPosture,
          messagingDescription: signalPkg.messagingDescription,
        });
      });

      emit(RANKING_PROGRESS_STAGES.PRIORITIZING, {
        phase: 'prioritizing',
        prospectCount: ranked.length,
      });

      ranked.sort((a, b) => {
        if (b.overallScore !== a.overallScore) return b.overallScore - a.overallScore;
        return b.confidence - a.confidence;
      });

      const withRank = ranked.map((r, i) => ({ ...r, rank: i + 1 }));

      const summary = {
        high: withRank.filter((r) => r.priority === 'high').length,
        medium: withRank.filter((r) => r.priority === 'medium').length,
        low: withRank.filter((r) => r.priority === 'low').length,
        averageScore:
          withRank.length === 0
            ? 0
            : Number(
                (
                  withRank.reduce((s, r) => s + r.overallScore, 0) / withRank.length
                ).toFixed(1)
              ),
      };

      const overallConfidence =
        withRank.length === 0
          ? 0
          : Number(
              (
                withRank.reduce((s, r) => s + r.confidence, 0) / withRank.length
              ).toFixed(4)
            );

      const reviewPackage = {
        summary: {
          rankedCount: withRank.length,
          ...summary,
          profileName: profile?.name || null,
          profileVersion: profile?.version || null,
        },
        rankedList: withRank.map((r) => ({
          rank: r.rank,
          id: r.id,
          companyName: r.companyName,
          overallScore: r.overallScore,
          priority: r.priority,
          confidence: r.confidence,
          topReasons: r.topReasons,
          risks: r.risks,
          recommendedNextAction: r.recommendedNextAction,
        })),
        briefs: withRank.map((r) => ({
          id: r.id,
          companyName: r.companyName,
          opportunityBrief: r.opportunityBrief,
        })),
        operatorActions: [...OPERATOR_ACTIONS],
      };

      emit(RANKING_PROGRESS_STAGES.COMPLETED, {
        phase: 'completed',
        rankedCount: withRank.length,
        high: summary.high,
      });

      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          prospects: withRank,
          rankedCount: withRank.length,
          summary,
          reviewPackage,
          confidence: overallConfidence,
          // Campaign Builder consumes this list
          feedCampaignBuilder: true,
        },
        evidence: [
          {
            kind: 'ranking',
            summary: `Ranked ${withRank.length} opportunities (high=${summary.high}, medium=${summary.medium}, low=${summary.low})`,
          },
          {
            kind: 'ranking_factors',
            summary:
              'Scores use profile_match, buying_signals, company_size, decision_maker_confidence, personalization_opportunities, geographic_fit, historical_success, evidence_confidence',
          },
        ],
        artifacts: [
          { type: 'ranked_prospects', count: withRank.length },
          { type: 'review_package', ranked: withRank.length },
          {
            type: 'opportunity_briefs',
            count: withRank.length,
          },
        ],
        warnings,
        nextRecommendations: [
          {
            action: 'review',
            summary: 'Approve, re-rank, exclude, or lock before Campaign Builder',
          },
          {
            action: 'continue',
            summary: 'Continue to Campaign Builder with approved priority queue',
          },
        ],
      });
    },
  };
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

module.exports = {
  createOpportunityRankingCapability,
  resolveProfile,
  resolveProspects,
  resolveHistoricalOutcomes,
};
