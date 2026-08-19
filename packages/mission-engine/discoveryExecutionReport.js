'use strict';

/**
 * AUDIT-006 — Derive discovery execution report from Mission state after ScoutDiscoveryExecutor runs.
 */

const { BUILTIN_IDS } = require('../capabilities/types');
const { STAGE_OUTCOMES } = require('./PipelineGate');
const {
  DISCOVERY_STRATEGIES,
  DISCOVERY_OUTCOMES,
} = require('./ScoutDiscoveryAudit');

const EVIDENCE_SOURCE_IDS = Object.freeze({
  DISCOVERY_PROFILE: 'Discovery Profile Store',
  EXTERNAL_SEARCH: 'External Search',
  CRM_LOOKUP: 'CRM Lookup',
  MARKET_INTELLIGENCE: 'Market Intelligence Store',
  COMPANY_STORE: 'Company Store',
  PROSPECT_STORE: 'Prospect Store',
  ENRICHMENT: 'Enrichment',
  SOCIAL_INTELLIGENCE: 'Social Intelligence',
  PREVIOUS_CAMPAIGNS: 'Previous Campaigns',
});

/**
 * @param {object} mission
 * @returns {object|null}
 */
function findDiscoveryStepResult(mission) {
  const stepResults =
    (mission.deliverables && mission.deliverables.stepResults) || [];
  const fromDeliverables = stepResults.find(
    (s) =>
      s.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY ||
      s.capabilityId === 'prospect_discovery'
  );
  if (fromDeliverables) return fromDeliverables;

  const lastGate = mission.deliverables && mission.deliverables.lastGate;
  if (
    lastGate &&
    (lastGate.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY ||
      lastGate.capabilityId === 'prospect_discovery')
  ) {
    return {
      capabilityId: lastGate.capabilityId,
      outcome: lastGate.outcome,
      reviewSummary: { blockingIssues: lastGate.blockingIssues },
      warnings: [],
      result: { outputs: {}, errors: lastGate.blockingIssues },
    };
  }

  const stageReview = mission.stageReview;
  if (
    stageReview &&
    (stageReview.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY ||
      stageReview.capabilityId === 'prospect_discovery')
  ) {
    return {
      capabilityId: stageReview.capabilityId,
      outcome: stageReview.outcome,
      reviewSummary: stageReview.reviewSummary,
      warnings: stageReview.warnings || [],
      result: {
        outputs: {},
        errors: stageReview.blockingIssues,
        warnings: stageReview.warnings,
      },
    };
  }

  return null;
}

/**
 * @param {object} mission
 * @param {object} [scoutPayload]
 * @param {object} [scoutDiscoveryMeta] - SPEC-123 unified discovery metadata
 * @returns {object}
 */
function buildDiscoveryExecutionReport(mission, scoutPayload = null, scoutDiscoveryMeta = null) {
  const step = findDiscoveryStepResult(mission);
  const runResult = step && step.result ? step.result : null;
  const outputs = runResult && runResult.outputs ? runResult.outputs : {};
  const errors = flattenErrors(runResult && runResult.errors);
  const warnings = [
    ...(Array.isArray(step && step.warnings) ? step.warnings : []),
    ...(Array.isArray(runResult && runResult.warnings) ? runResult.warnings : []),
  ];
  const stageOutcome =
    (mission.progress && mission.progress.stageOutcome) ||
    (step && step.outcome) ||
    null;
  const prospectCount =
    outputs.prospectCount != null
      ? Number(outputs.prospectCount)
      : Array.isArray(outputs.prospects)
        ? outputs.prospects.length
        : 0;

  const profileResolved = Boolean(
    outputs.discoveryProfile ||
      mission.discoveryProfile ||
      (runResult &&
        runResult.evidence &&
        runResult.evidence.some((e) => e.kind === 'discovery_profile'))
  );
  const profileBlocked = errors.some((e) =>
    /NO_DISCOVERY_PROFILE|No Discovery Profile/i.test(String(e))
  );

  const providerWarnings = warnings.filter((w) =>
    /provider|GOOGLE_PLACES|fixture|search provider/i.test(String(w))
  );
  const externalAttempted =
    profileResolved &&
    !profileBlocked &&
    (prospectCount > 0 ||
      providerWarnings.length > 0 ||
      Boolean(outputs.summary && outputs.summary.discovered != null));
  const externalUnavailable =
    providerWarnings.some((w) => /No discovery providers available/i.test(String(w))) ||
    errors.some((e) => /provider/i.test(String(e)));

  const crmDedupeAttempted = warnings.some((w) =>
    /CRM|existing CRM/i.test(String(w))
  );

  const existingConsulted = Boolean(
    scoutDiscoveryMeta &&
      scoutDiscoveryMeta.existingIntelligence &&
      scoutDiscoveryMeta.existingIntelligence.consulted
  );
  const existingCompanyCount =
    (scoutDiscoveryMeta &&
      scoutDiscoveryMeta.existingIntelligence &&
      scoutDiscoveryMeta.existingIntelligence.companyCount) ||
    0;
  const existingProspectCount =
    (scoutDiscoveryMeta &&
      scoutDiscoveryMeta.existingIntelligence &&
      scoutDiscoveryMeta.existingIntelligence.prospectCount) ||
    0;
  const externalSkipped = Boolean(
    scoutDiscoveryMeta &&
      scoutDiscoveryMeta.phases &&
      scoutDiscoveryMeta.phases.some(
        (p) =>
          p.phase === 'external_discovery' &&
          p.result &&
          p.result.skipped === true
      )
  );

  const evidenceSources = [
    sourceRow(EVIDENCE_SOURCE_IDS.DISCOVERY_PROFILE, {
      attempted: true,
      succeeded: profileResolved && !profileBlocked,
      unavailable: profileBlocked,
      skipped: false,
    }),
    sourceRow(EVIDENCE_SOURCE_IDS.COMPANY_STORE, {
      attempted: existingConsulted,
      succeeded: existingConsulted && existingCompanyCount > 0,
      unavailable: Boolean(
        scoutDiscoveryMeta &&
          scoutDiscoveryMeta.existingIntelligence &&
          scoutDiscoveryMeta.existingIntelligence.error
      ),
      skipped: !existingConsulted,
    }),
    sourceRow(EVIDENCE_SOURCE_IDS.PROSPECT_STORE, {
      attempted: existingConsulted,
      succeeded: existingConsulted && existingProspectCount > 0,
      unavailable: false,
      skipped: !existingConsulted,
    }),
    sourceRow(EVIDENCE_SOURCE_IDS.EXTERNAL_SEARCH, {
      attempted: externalAttempted && !externalSkipped,
      succeeded: externalAttempted && !externalSkipped && prospectCount > 0,
      unavailable: externalUnavailable,
      skipped: profileBlocked || externalSkipped,
    }),
    sourceRow(EVIDENCE_SOURCE_IDS.CRM_LOOKUP, {
      attempted: crmDedupeAttempted || existingConsulted,
      succeeded: crmDedupeAttempted || (existingConsulted && existingCompanyCount > 0),
      unavailable: false,
      skipped: !crmDedupeAttempted && !existingConsulted,
    }),
    sourceRow(EVIDENCE_SOURCE_IDS.MARKET_INTELLIGENCE, {
      attempted: false,
      succeeded: false,
      unavailable: false,
      skipped: true,
    }),
    sourceRow(EVIDENCE_SOURCE_IDS.PREVIOUS_CAMPAIGNS, {
      attempted: existingConsulted,
      succeeded: existingConsulted && existingCompanyCount > 0,
      unavailable: false,
      skipped: !existingConsulted,
    }),
  ];

  const discoveryStrategy = resolveDiscoveryStrategy({
    profileBlocked,
    scoutDiscoveryMeta,
    existingConsulted,
    externalSkipped,
  });

  const { outcome, blockReason } = mapDiscoveryOutcome({
    stageOutcome,
    runStatus: runResult && runResult.status,
    prospectCount,
    profileBlocked,
    externalUnavailable,
    errors,
    warnings,
    blockingIssues: mission.blockingIssues,
    step,
  });

  const geography =
    (scoutPayload && scoutPayload.geography) ||
    (mission.constraints && mission.constraints.locationHint) ||
    (outputs.discoveryProfile &&
      outputs.discoveryProfile.geography &&
      (outputs.discoveryProfile.geography.label ||
        (outputs.discoveryProfile.geography.cities || []).join(', '))) ||
    null;

  const targetSegment =
    (scoutPayload && scoutPayload.targetSegment) ||
    (mission.constraints && mission.constraints.vertical) ||
    null;

  return {
    missionId: mission.id,
    objective:
      (scoutPayload && scoutPayload.objective) ||
      mission.objectiveText ||
      mission.title ||
      null,
    targetSegment,
    geography,
    discoveryStrategy,
    evidenceSources,
    outcome,
    blockReason,
    stageOutcome,
    prospectCount,
    externalDiscoveryAttempted: externalAttempted && !externalSkipped,
    storedIntelligenceOnly:
      discoveryStrategy === DISCOVERY_STRATEGIES.STORED_MARKET_INTELLIGENCE ||
      (externalSkipped && existingCompanyCount > 0),
    capabilityPath:
      (scoutDiscoveryMeta && scoutDiscoveryMeta.capabilityPath) || 'scout.discover',
    scoutAcquisitionPathInvoked: Boolean(
      scoutDiscoveryMeta && scoutDiscoveryMeta.scoutAcquisitionPathInvoked
    ),
    gapAnalysis: scoutDiscoveryMeta && scoutDiscoveryMeta.gapAnalysis,
    existingIntelligence:
      scoutDiscoveryMeta && scoutDiscoveryMeta.existingIntelligence,
    nextRecommendation: buildNextRecommendation(outcome, blockReason, discoveryStrategy),
  };
}

/**
 * Resolve discovery strategy — prefer SPEC-123 unified strategy when available.
 * @param {object} input
 * @returns {string}
 */
function resolveDiscoveryStrategy(input) {
  if (input.profileBlocked) {
    return DISCOVERY_STRATEGIES.NO_STRATEGY_SELECTED;
  }
  if (input.scoutDiscoveryMeta && input.scoutDiscoveryMeta.strategy) {
    const unified = input.scoutDiscoveryMeta.strategy;
    if (unified === 'Hybrid') return DISCOVERY_STRATEGIES.HYBRID;
    if (unified === 'Retrieve Only') return DISCOVERY_STRATEGIES.STORED_MARKET_INTELLIGENCE;
    if (unified === 'External Heavy') return DISCOVERY_STRATEGIES.EXTERNAL_DISCOVERY;
    if (unified === 'Verification Only') return DISCOVERY_STRATEGIES.HYBRID;
    return unified;
  }
  if (input.existingConsulted && !input.externalSkipped) {
    return DISCOVERY_STRATEGIES.HYBRID;
  }
  if (input.externalSkipped && input.existingConsulted) {
    return DISCOVERY_STRATEGIES.STORED_MARKET_INTELLIGENCE;
  }
  return DISCOVERY_STRATEGIES.EXTERNAL_DISCOVERY;
}

function sourceRow(source, state) {
  return {
    source,
    attempted: Boolean(state.attempted),
    succeeded: Boolean(state.succeeded),
    unavailable: Boolean(state.unavailable),
    skipped: Boolean(state.skipped),
  };
}

function flattenErrors(errors) {
  if (!Array.isArray(errors)) return [];
  return errors.map((e) =>
    typeof e === 'string' ? e : e.message || e.code || String(e)
  );
}

/**
 * @param {object} input
 * @returns {{ outcome: string, blockReason: string|null }}
 */
function mapDiscoveryOutcome(input) {
  const blockingIssues = [
    ...(Array.isArray(input.blockingIssues) ? input.blockingIssues : []),
    ...(Array.isArray(input.step && input.step.reviewSummary &&
      input.step.reviewSummary.blockingIssues)
      ? input.step.reviewSummary.blockingIssues
      : []),
  ].map(String);

  if (input.runStatus === 'failed' || input.stageOutcome === STAGE_OUTCOMES.FAILED) {
    return {
      outcome: DISCOVERY_OUTCOMES.FAILED,
      blockReason: blockingIssues[0] || input.errors[0] || 'Discovery capability failed.',
    };
  }

  if (input.profileBlocked) {
    return {
      outcome: DISCOVERY_OUTCOMES.BLOCKED,
      blockReason: input.errors[0] || 'No Discovery Profile available for this mission.',
    };
  }

  if (input.externalUnavailable && input.prospectCount <= 0) {
    return {
      outcome: DISCOVERY_OUTCOMES.BLOCKED,
      blockReason:
        'No external discovery connector available (configure GOOGLE_PLACES_KEY or inject a search provider).',
    };
  }

  if (
    input.stageOutcome === STAGE_OUTCOMES.BLOCKED ||
    (input.prospectCount <= 0 && blockingIssues.length)
  ) {
    return {
      outcome: DISCOVERY_OUTCOMES.BLOCKED,
      blockReason:
        blockingIssues[0] ||
        'Discovery returned zero verified companies. Campaign generation cannot continue.',
    };
  }

  if (
    input.stageOutcome === STAGE_OUTCOMES.COMPLETED_WITH_WARNINGS ||
    (input.prospectCount > 0 &&
      input.warnings.some((w) => /Requested|shortfall/i.test(String(w))))
  ) {
    return {
      outcome: DISCOVERY_OUTCOMES.PARTIAL,
      blockReason: null,
    };
  }

  if (input.prospectCount > 0) {
    return {
      outcome: DISCOVERY_OUTCOMES.COMPLETED,
      blockReason: null,
    };
  }

  return {
    outcome: DISCOVERY_OUTCOMES.BLOCKED,
    blockReason: blockingIssues[0] || 'Discovery produced no verified prospects.',
  };
}

function buildNextRecommendation(outcome, blockReason, strategy) {
  if (outcome === DISCOVERY_OUTCOMES.COMPLETED) {
    return 'Review discovered prospects and approve prioritization to continue.';
  }
  if (outcome === DISCOVERY_OUTCOMES.PARTIAL) {
    return 'Review partial discovery results or adjust target count before continuing.';
  }
  if (/Discovery Profile/i.test(String(blockReason))) {
    return 'Configure or pin a Discovery Profile for this mission before retrying Discovery.';
  }
  if (/external discovery connector|GOOGLE_PLACES/i.test(String(blockReason))) {
    return 'Configure GOOGLE_PLACES_KEY or inject a search provider, then retry Discovery.';
  }
  if (/zero verified companies|no verified prospects/i.test(String(blockReason))) {
    return 'Broaden Discovery Profile geography or industry targets, then retry Discovery.';
  }
  if (strategy === DISCOVERY_STRATEGIES.NO_STRATEGY_SELECTED) {
    return 'Resolve Discovery Profile selection before continuing Discovery.';
  }
  return 'Resolve the Discovery blocker, then retry the Discovery stage.';
}

/**
 * @param {object} report
 * @returns {string}
 */
function formatDiscoveryOperatorResponse(report) {
  const stage = 'Discovery';
  const outcomeLabel = report.outcome.replace(/^DISCOVERY_/, '');
  const lines = [
    'Mission Updated',
    '',
    `Stage: ${stage}`,
    `Outcome: ${outcomeLabel}`,
  ];
  if (report.blockReason) {
    lines.push(`Reason: ${report.blockReason}`);
  }
  lines.push(
    '',
    `Scout Discovery completed (${report.discoveryStrategy}).` +
      (report.prospectCount > 0
        ? ` Found ${report.prospectCount} verified prospect(s).`
        : ' No verified prospects were returned.')
  );
  if (report.nextRecommendation) {
    lines.push('', `Next Recommendation: ${report.nextRecommendation}`);
  }
  return lines.join('\n');
}

/**
 * Emit all AUDIT-006 events for a discovery execution report.
 * @param {object} report
 * @param {object} audit
 */
function emitDiscoveryAuditEvents(report, audit) {
  audit.logScoutDiscoveryStrategy({
    missionId: report.missionId,
    discoveryStrategy: report.discoveryStrategy,
    objective: report.objective,
    targetSegment: report.targetSegment,
    geography: report.geography,
    capabilityPath: report.capabilityPath,
    scoutAcquisitionPathInvoked: report.scoutAcquisitionPathInvoked,
  });

  for (const source of report.evidenceSources) {
    audit.logScoutEvidenceSource({
      missionId: report.missionId,
      discoveryStrategy: report.discoveryStrategy,
      source: source.source,
      attempted: source.attempted,
      succeeded: source.succeeded,
      unavailable: source.unavailable,
      skipped: source.skipped,
    });
  }

  audit.logScoutDiscoveryOutcome({
    missionId: report.missionId,
    discoveryStrategy: report.discoveryStrategy,
    outcome: report.outcome,
    prospectCount: report.prospectCount,
    externalDiscoveryAttempted: report.externalDiscoveryAttempted,
    storedIntelligenceOnly: report.storedIntelligenceOnly,
  });

  if (report.blockReason) {
    audit.logScoutBlockReason({
      missionId: report.missionId,
      discoveryStrategy: report.discoveryStrategy,
      outcome: report.outcome,
      blockReason: report.blockReason,
    });
  }

  audit.logMissionDiscoveryUpdate({
    missionId: report.missionId,
    stage: 'Discovery',
    outcome: report.outcome,
    blockReason: report.blockReason,
    missionStatus: report.missionStatus || null,
    stageOutcome: report.stageOutcome || null,
  });
}

module.exports = {
  EVIDENCE_SOURCE_IDS,
  findDiscoveryStepResult,
  buildDiscoveryExecutionReport,
  formatDiscoveryOperatorResponse,
  emitDiscoveryAuditEvents,
};
