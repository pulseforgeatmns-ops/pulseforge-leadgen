'use strict';

/**
 * Outcome Intelligence capability (SPEC-036 / ADR-023).
 * Capture campaign outcomes → evidence-backed learnings → pending recommendations.
 * Strategy mutation requires operator approval.
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
  OUTCOME_PROGRESS_STAGES,
  OPERATOR_ACTIONS,
  RECOMMENDATION_STATUS,
} = require('./types');
const { assembleOutcomeIntelligence } = require('./assemble');
const { normalizeActions, applyOutcomeActions } = require('./actions');
const {
  createInMemoryOutcomeIntelligenceStore,
} = require('./OutcomeIntelligenceStore');

/**
 * @param {object} [deps]
 */
function createOutcomeIntelligenceCapability(deps = {}) {
  const store =
    deps.outcomeIntelligenceStore ||
    createInMemoryOutcomeIntelligenceStore();

  return {
    id: BUILTIN_IDS.OUTCOME_INTELLIGENCE,
    name: 'Outcome Intelligence',
    description:
      'Capture campaign outcomes and convert evidence into pending recommendations — strategy updates require operator approval',
    category: CAPABILITY_CATEGORIES.INTELLIGENCE,
    outcomeTags: [
      'outcomes_captured',
      'learnings_generated',
      'recommendations_pending',
      'ranking_feedback',
      'outcome_summary',
    ],
    retryable: true,
    timeoutMs: 30_000,
    supportsRollback: false,
    idempotent: false,
    inputSchema: {
      required: [],
      properties: {
        execution: 'DirectMailExecution?',
        prospects: 'ProspectExecution[]?',
        responseEvents: 'ResponseEvent[]?',
        outcomes: 'OutcomeRecord[]?',
        companyIntelligence: 'CompanyIntelligence?',
        clientPlaybook: 'ClientPlaybook?',
        cost: 'number?',
        revenue: 'number?',
        outcomeActions: 'OutcomeAction[]?',
        operator: 'string?',
      },
    },
    outputSchema: {
      outcomes: 'OutcomeRecord[]',
      learnings: 'Learning[]',
      recommendations: 'Recommendation[]',
      rankingFeedback: 'RankingFeedback[]',
      historicalOutcomes: 'HistoricalOutcome[]',
      personalizationFeedback: 'PersonalizationFeedback',
      analytics: 'CampaignAnalytics',
      outcomeSummary: 'OutcomeSummary',
      missionEvents: 'MissionOutcomeEvent[]',
      timeline: 'MissionTimelineEntry[]',
    },

    canRun(context) {
      const inputs = (context && context.inputs) || {};
      const prior = inputs.priorOutputs || {};
      return (
        Boolean(inputs.execution || prior.execution) ||
        (Array.isArray(inputs.prospects) && inputs.prospects.length > 0) ||
        (Array.isArray(prior.prospects) && prior.prospects.length > 0) ||
        (Array.isArray(inputs.outcomes) && inputs.outcomes.length > 0) ||
        (Array.isArray(inputs.responseEvents) &&
          inputs.responseEvents.length > 0) ||
        Boolean(inputs.outcomeActions)
      );
    },

    estimate(context) {
      const inputs = (context && context.inputs) || {};
      const prior = inputs.priorOutputs || {};
      const n =
        (Array.isArray(inputs.prospects) && inputs.prospects.length) ||
        (Array.isArray(prior.prospects) && prior.prospects.length) ||
        (Array.isArray(inputs.outcomes) && inputs.outcomes.length) ||
        1;
      return buildCapabilityEstimate({
        durationMs: 400 + n * 20,
        confidence: n ? 0.85 : 0.4,
        notes: [`Outcome intelligence for ${n} record(s)`],
      });
    },

    /**
     * @param {object} context
     * @param {{ onProgress?: Function }} [runtime]
     */
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

      emit(OUTCOME_PROGRESS_STAGES.CAPTURING, 15);

      const inputs = { ...(context.inputs || {}) };
      const prior = inputs.priorOutputs || {};
      if (!inputs.execution && prior.execution) {
        inputs.execution = prior.execution;
      }
      if (!inputs.prospects && prior.prospects) {
        inputs.prospects = prior.prospects;
      }
      if (!inputs.metrics && prior.metrics) {
        inputs.metrics = prior.metrics;
      }
      if (!inputs.campaignId && prior.summary && prior.summary.campaignId) {
        inputs.campaignId = prior.summary.campaignId;
      }
      if (!inputs.campaignName && prior.summary && prior.summary.campaignName) {
        inputs.campaignName = prior.summary.campaignName;
      }

      const operator = inputs.operator || context.createdBy || 'operator';
      const ctx = { ...context, inputs };

      const campaignKey =
        inputs.campaignId ||
        (inputs.campaign && (inputs.campaign.id || inputs.campaign.name)) ||
        context.missionId ||
        null;

      // Resume prior package when approving/rejecting recommendations
      const existing = campaignKey ? store.getLatest(campaignKey) : null;
      const actions = normalizeActions(inputs);
      const reviewOnly =
        actions.length > 0 &&
        actions.every((a) =>
          /approve_recommendation|reject_recommendation|apply_recommendation|conclude_mission/.test(
            String(a.type || a.action || '')
          )
        );

      let pkg;
      if (reviewOnly && existing && existing.package) {
        pkg = existing.package;
      } else {
        emit(OUTCOME_PROGRESS_STAGES.LEARNING, 40);
        pkg = assembleOutcomeIntelligence(ctx, { operator });
      }

      emit(OUTCOME_PROGRESS_STAGES.RECOMMENDING, 60);

      const applied = applyOutcomeActions(pkg, actions, { operator });
      pkg = applied.package;

      emit(OUTCOME_PROGRESS_STAGES.ANALYTICS, 80);
      emit(OUTCOME_PROGRESS_STAGES.SUMMARY, 90);

      const saved = await Promise.resolve(
        store.create({
          campaignId: campaignKey,
          missionId: context.missionId || null,
          clientId: context.clientId != null ? context.clientId : null,
          tenantId: context.tenantId || '',
          campaignName: pkg.summary.campaignName,
          package: pkg,
          outcomes: pkg.outcomes,
          learnings: pkg.learnings,
          recommendations: pkg.recommendations,
          rankingFeedback: pkg.rankingFeedback,
          analytics: pkg.analytics,
          outcomeSummary: pkg.outcomeSummary,
          missionEvents: pkg.missionEvents,
          timeline: pkg.timeline,
          summary: pkg.summary,
          changeSummary: applied.changeSummary,
          operator,
        })
      );

      emit(OUTCOME_PROGRESS_STAGES.COMPLETED, 100);

      const pending = pkg.recommendations.filter(
        (r) => r.status === RECOMMENDATION_STATUS.PENDING
      );

      return buildCapabilityResult({
        status:
          applied.errors.length > 0
            ? CAPABILITY_RESULT_STATUS.PARTIAL
            : CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          outcomes: pkg.outcomes,
          learnings: pkg.learnings,
          recommendations: pkg.recommendations,
          rankingFeedback: pkg.rankingFeedback,
          historicalOutcomes: pkg.historicalOutcomes,
          personalizationFeedback: pkg.personalizationFeedback,
          analytics: pkg.analytics,
          outcomeSummary: pkg.outcomeSummary,
          missionEvents: pkg.missionEvents,
          timeline: pkg.timeline,
          summary: pkg.summary,
          outcomeSnapshotId: saved.id,
          outcomeSnapshot: saved,
          changeSummary: applied.changeSummary,
          actionErrors: applied.errors,
          operatorActions: [...OPERATOR_ACTIONS],
          pendingRecommendations: pending,
        },
        evidence: [
          {
            kind: 'outcome_intelligence',
            summary: `${pkg.summary.campaignName}: ${pkg.summary.outcomeCount} outcomes, ${pkg.summary.evidenceBackedLearnings} evidence-backed learnings, ${pkg.summary.pendingRecommendations} pending recommendations`,
            outcomeCount: pkg.summary.outcomeCount,
            evidenceBackedLearnings: pkg.summary.evidenceBackedLearnings,
            pendingRecommendations: pkg.summary.pendingRecommendations,
          },
        ],
        artifacts: [
          {
            type: 'outcome_intelligence',
            id: saved.id,
            outcomeCount: pkg.summary.outcomeCount,
            pendingRecommendations: pkg.summary.pendingRecommendations,
          },
          {
            type: 'outcome_summary',
            id: saved.id,
            objectiveAchieved: Boolean(
              pkg.outcomeSummary && pkg.outcomeSummary.objectiveAchieved
            ),
          },
        ],
        warnings: applied.errors.length
          ? applied.errors.slice(0, 10)
          : pending.length
            ? [
                `${pending.length} recommendation(s) pending operator approval before playbook/ranking/discovery updates (ADR-023)`,
              ]
            : [],
        errors: applied.errors.map((code) => ({
          code,
          message: String(code),
        })),
        nextRecommendations: nextRecs(pkg),
        duration: Date.now() - started,
      });
    },
  };
}

/**
 * @param {object} pkg
 * @returns {object[]}
 */
function nextRecs(pkg) {
  const pending = (pkg.recommendations || []).filter(
    (r) => r.status === RECOMMENDATION_STATUS.PENDING
  );
  if (pending.length) {
    return [
      {
        action: 'approve_recommendation',
        summary: `Review ${pending.length} pending recommendation(s) before updating strategy`,
      },
    ];
  }
  return [
    {
      action: 'conclude_mission',
      summary: 'Attach Outcome Summary and conclude the mission',
    },
  ];
}

module.exports = {
  createOutcomeIntelligenceCapability,
};
