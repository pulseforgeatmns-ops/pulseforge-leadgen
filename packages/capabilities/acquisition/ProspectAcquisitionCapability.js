'use strict';

/**
 * Prospect Acquisition capability (SPEC-060 / ADR-044).
 * Provider-agnostic ingress: acquire Candidates → verify → ProspectList.
 * Campaign Builder never sees acquisition source metadata as a dependency.
 */

const {
  CAPABILITY_CATEGORIES,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  CAPABILITY_RESULT_STATUS,
  PROGRESS_KINDS,
} = require('../types');
const { ACQUISITION_STRATEGIES } = require('./types');
const {
  createProspectAcquisition,
  selectAcquisitionStrategy,
} = require('./ProspectAcquisition');

/**
 * @param {object} [deps]
 */
function createProspectAcquisitionCapability(deps = {}) {
  const acquisition =
    deps.acquisition || createProspectAcquisition(deps);

  return {
    id: BUILTIN_IDS.PROSPECT_ACQUISITION,
    name: 'Acquiring Prospects',
    description:
      'Acquire prospect candidates from Discovery, Manual, CSV, or Existing lists, then verify into a ProspectList',
    category: CAPABILITY_CATEGORIES.DISCOVERY,
    outcomeTags: ['prospects_acquired', 'candidates_acquired'],
    retryable: true,
    timeoutMs: 180_000,
    supportsRollback: false,
    idempotent: true,
    acquisitionCost: 40,
    inputSchema: { required: [] },
    outputSchema: {
      prospects: 'Prospect[]',
      candidateSet: 'CandidateSet',
      summary: 'object',
      evidence: 'array',
      warnings: 'string[]',
    },

    canRun() {
      return true;
    },

    estimate(context) {
      const strategy = selectAcquisitionStrategy(
        (context && context.objective) || '',
        (context && context.constraints) || {}
      );
      const hasLive =
        strategy === ACQUISITION_STRATEGIES.DISCOVERY &&
        acquisition.registry &&
        typeof acquisition.registry.get === 'function' &&
        (() => {
          const p = acquisition.registry.get('google_places');
          return p && p.available && p.available();
        })();
      return buildCapabilityEstimate({
        durationMs:
          strategy === ACQUISITION_STRATEGIES.DISCOVERY
            ? hasLive
              ? 60_000
              : 2500
            : 1500,
        confidence: 0.85,
        notes: [`Acquisition strategy: ${strategy}`],
      });
    },

    async execute(context) {
      const emit = makeEmitter(context);
      const constraints = (context && context.constraints) || {};
      const strategy = selectAcquisitionStrategy(
        (context && context.objective) || '',
        constraints
      );

      emit(PROGRESS_KINDS.PROGRESS, {
        stage: 'acquiring',
        message: `Acquiring candidates (${strategy})`,
      });

      const result = await acquisition.acquireAndVerify({
        ...constraints,
        objective: context.objective,
        text: context.objective,
        acquisitionStrategy: strategy,
        missionId: context.missionId,
        operator:
          constraints.operator ||
          (context.createdBy != null ? String(context.createdBy) : null),
        targetCount:
          constraints.targetCount != null
            ? Number(constraints.targetCount)
            : undefined,
        profile:
          constraints.discoveryProfile ||
          (context.inputs && context.inputs.discoveryProfile) ||
          null,
        prospects: constraints.prospects || constraints.manualProspects,
        paste: constraints.paste || constraints.importPaste,
        csv: constraints.csv || constraints.csvText,
        prospectList:
          constraints.prospectList ||
          constraints.existingList ||
          (context.inputs && context.inputs.prospectList) ||
          null,
        listId: constraints.listId || constraints.listName,
        acceptSoftFailures: Boolean(constraints.acceptSoftFailures),
      });

      if (!result.ok || !result.prospectList) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          outputs: {
            prospectCount: 0,
            prospects: [],
            candidateSet: result.candidateSet || null,
            summary: {
              strategy: result.strategy,
              providerId: result.providerId,
              acquired:
                (result.candidateSet && result.candidateSet.candidateCount) || 0,
              verified: 0,
            },
            verificationReport: result.verificationReport || null,
            rejectedProspects: result.rejectedProspects || [],
            confidence: 0,
          },
          evidence: result.evidence || [],
          warnings: result.warnings || [],
          errors: (result.errors || ['Acquisition produced no ProspectList']).map(
            (message) => ({
              message: String(message),
              code: 'ACQUISITION_FAILED',
            })
          ),
          artifacts: [],
          nextRecommendations: (result.recommendedStrategies || []).map(
            (s) => `Try acquisition strategy: ${s}`
          ),
        });
      }

      emit(PROGRESS_KINDS.PROGRESS, {
        stage: 'verified',
        message: `Verified ${result.prospectList.prospectCount} prospects`,
      });

      const prospects = result.prospectList.prospects || [];
      const candidateSet = result.candidateSet || null;

      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: {
          prospects,
          prospectCount: prospects.length,
          targetCount:
            result.prospectList.targetCount != null
              ? result.prospectList.targetCount
              : prospects.length,
          summary: {
            ...(result.prospectList.summary || {}),
            strategy: result.strategy,
            providerId: result.providerId,
            acquisitionSource:
              (candidateSet && candidateSet.acquisitionSource) ||
              result.strategy,
          },
          candidateSet,
          candidateCount:
            (candidateSet && candidateSet.candidateCount) || 0,
          candidates: (candidateSet && candidateSet.candidates) || [],
          acquisitionSource:
            (candidateSet && candidateSet.acquisitionSource) || null,
          verificationReport: result.verificationReport || null,
          rejectedProspects: result.rejectedProspects || [],
          rejected: result.rejectedProspects || [],
          confidence: prospects.length > 0 ? 0.9 : 0,
        },
        evidence: result.evidence || [],
        warnings: result.warnings || [],
        errors: [],
        artifacts: [
          {
            type: 'candidate_set',
            payload: candidateSet,
          },
          {
            type: 'prospect_list',
            payload: result.prospectList,
          },
        ],
      });
    },
  };
}

function makeEmitter(context) {
  const onProgress =
    context && typeof context.onProgress === 'function'
      ? context.onProgress
      : null;
  return (kind, detail) => {
    if (!onProgress) return;
    try {
      onProgress({ kind, ...(detail || {}) });
    } catch {
      /* ignore progress errors */
    }
  };
}

module.exports = {
  createProspectAcquisitionCapability,
};
