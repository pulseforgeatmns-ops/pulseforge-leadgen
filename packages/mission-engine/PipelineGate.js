'use strict';

/**
 * Stage artifact contracts + pipeline gate (SPEC-040 / ADR-026).
 * Business success — not technical execution — determines pipeline progress.
 */

const { BUILTIN_IDS } = require('../capabilities/types');

const STAGE_OUTCOMES = Object.freeze({
  COMPLETED: 'completed',
  COMPLETED_WITH_WARNINGS: 'completed_with_warnings',
  BLOCKED: 'blocked',
  FAILED: 'failed',
  /** SPEC-043 — operator-supplied artifact satisfies the stage */
  SATISFIED_OPERATOR_SUPPLIED: 'satisfied_operator_supplied',
});

const ARTIFACT_VALIDATION_STATUS = Object.freeze({
  VALID: 'valid',
  VALID_WITH_WARNINGS: 'valid_with_warnings',
  INVALID: 'invalid',
  QUARANTINED: 'quarantined',
});

const STAGE_OUTCOME_LABELS = Object.freeze({
  [STAGE_OUTCOMES.COMPLETED]: 'Completed',
  [STAGE_OUTCOMES.COMPLETED_WITH_WARNINGS]: 'Completed With Warnings',
  [STAGE_OUTCOMES.BLOCKED]: 'Blocked',
  [STAGE_OUTCOMES.FAILED]: 'Failed',
  [STAGE_OUTCOMES.SATISFIED_OPERATOR_SUPPLIED]: 'Satisfied (Operator Supplied)',
});

/** @type {Record<string, object>} */
const STAGE_CONTRACTS = Object.freeze({
  [BUILTIN_IDS.PROSPECT_DISCOVERY]: {
    id: BUILTIN_IDS.PROSPECT_DISCOVERY,
    label: 'Discovery',
    requiredInputs: ['discovery_profile'],
    expectedOutputs: ['prospect_list'],
    validate: validateDiscovery,
  },
  [BUILTIN_IDS.COMPANY_ENRICHMENT]: {
    id: BUILTIN_IDS.COMPANY_ENRICHMENT,
    label: 'Company Intelligence',
    requiredInputs: ['prospect_list'],
    expectedOutputs: ['enriched_list'],
    validate: validateEnrichment,
  },
  [BUILTIN_IDS.KNOWLEDGE_UPDATE]: {
    id: BUILTIN_IDS.KNOWLEDGE_UPDATE,
    label: 'Knowledge Update',
    requiredInputs: [],
    expectedOutputs: [],
    validate: validatePassthrough,
  },
  [BUILTIN_IDS.OPPORTUNITY_RANKING]: {
    id: BUILTIN_IDS.OPPORTUNITY_RANKING,
    label: 'Ranking',
    requiredInputs: ['prospect_list'],
    expectedOutputs: ['ranked_prospects'],
    validate: validateRanking,
  },
  [BUILTIN_IDS.SALES_INTELLIGENCE]: {
    id: BUILTIN_IDS.SALES_INTELLIGENCE,
    label: 'Sales Intelligence',
    requiredInputs: ['ranked_prospects'],
    expectedOutputs: ['sales_intelligence_profile'],
    validate: validateSalesIntelligence,
  },
  [BUILTIN_IDS.CAMPAIGN_BUILDER]: {
    id: BUILTIN_IDS.CAMPAIGN_BUILDER,
    label: 'Campaign Builder',
    requiredInputs: ['ranked_prospects'],
    expectedOutputs: ['campaign'],
    validate: validateCampaign,
  },
});

/**
 * @param {string} capabilityId
 * @returns {object|null}
 */
function getStageContract(capabilityId) {
  return STAGE_CONTRACTS[capabilityId] || null;
}

/**
 * Evaluate business outcome after a capability run.
 *
 * @param {object} input
 * @param {string} input.capabilityId
 * @param {object} input.runResult - CapabilityRunner result { result, ... }
 * @param {object} [input.context] - mission context (constraints, priorOutputs)
 * @param {object} [input.mission]
 * @returns {object} gate decision
 */
function evaluatePipelineGate(input = {}) {
  const capabilityId = input.capabilityId;
  const runResult = input.runResult || {};
  const result = runResult.result || runResult;
  const context = input.context || {};
  const contract = getStageContract(capabilityId);

  if (
    result.status === 'failed' ||
    result.status === 'cancelled'
  ) {
    return buildGateDecision({
      outcome: STAGE_OUTCOMES.FAILED,
      capabilityId,
      contract,
      blockingIssues: flattenErrors(result.errors).length
        ? flattenErrors(result.errors)
        : ['Unexpected system failure'],
      warnings: result.warnings || [],
      publishedArtifacts: [],
      quarantinedArtifacts: stampArtifacts(result.artifacts || [], {
        validationStatus: ARTIFACT_VALIDATION_STATUS.QUARANTINED,
        reason: 'Capability failed',
      }),
      validation: {
        passed: false,
        reason: 'capability_failed',
      },
      advance: false,
      publishOutputs: false,
    });
  }

  if (!contract) {
    // Unknown capability — advance on technical complete (no contract)
    const warnings = result.warnings || [];
    return buildGateDecision({
      outcome: warnings.length
        ? STAGE_OUTCOMES.COMPLETED_WITH_WARNINGS
        : STAGE_OUTCOMES.COMPLETED,
      capabilityId,
      contract: null,
      blockingIssues: [],
      warnings,
      publishedArtifacts: stampArtifacts(result.artifacts || [], {
        validationStatus: warnings.length
          ? ARTIFACT_VALIDATION_STATUS.VALID_WITH_WARNINGS
          : ARTIFACT_VALIDATION_STATUS.VALID,
      }),
      quarantinedArtifacts: [],
      validation: { passed: true, reason: 'no_contract' },
      advance: true,
      publishOutputs: true,
    });
  }

  const validation = contract.validate(result, context, input.mission);
  return buildGateDecision({
    outcome: validation.outcome,
    capabilityId,
    contract: { id: contract.id, label: contract.label },
    blockingIssues: validation.blockingIssues || [],
    warnings: validation.warnings || [],
    publishedArtifacts: validation.publishedArtifacts || [],
    quarantinedArtifacts: validation.quarantinedArtifacts || [],
    validation: {
      passed: validation.outcome !== STAGE_OUTCOMES.BLOCKED &&
        validation.outcome !== STAGE_OUTCOMES.FAILED,
      details: validation.details || null,
      reason: validation.reason || null,
    },
    advance:
      validation.outcome === STAGE_OUTCOMES.COMPLETED ||
      validation.outcome === STAGE_OUTCOMES.COMPLETED_WITH_WARNINGS,
    publishOutputs:
      validation.outcome === STAGE_OUTCOMES.COMPLETED ||
      validation.outcome === STAGE_OUTCOMES.COMPLETED_WITH_WARNINGS,
    reviewSummary: validation.reviewSummary || null,
  });
}

function validateDiscovery(result, context) {
  const outputs = result.outputs || {};
  const warnings = [...(result.warnings || [])];
  const blockingIssues = [];
  const details = {};

  const profile =
    outputs.discoveryProfile ||
    (context.constraints && context.constraints.discoveryProfile) ||
    null;

  if (!profile || !profile.id) {
    blockingIssues.push('No Discovery Profile');
  } else {
    const geo = profile.geography;
    const geoOk =
      geo &&
      ((geo.label && String(geo.label).trim()) ||
        (Array.isArray(geo.cities) && geo.cities.length > 0));
    if (!geoOk && profile.status !== 'temporary') {
      blockingIssues.push('Discovery Profile geography is invalid');
    }
  }

  const prospects = Array.isArray(outputs.prospects) ? outputs.prospects : [];
  const prospectCount =
    outputs.prospectCount != null ? Number(outputs.prospectCount) : prospects.length;
  const targetCount =
    outputs.targetCount != null
      ? Number(outputs.targetCount)
      : (context.constraints && Number(context.constraints.targetCount)) ||
        null;

  details.prospectCount = prospectCount;
  details.targetCount = targetCount;
  details.summary = outputs.summary || null;

  const rawArtifacts = Array.isArray(result.artifacts) ? result.artifacts : [];
  const prospectListArtifact =
    rawArtifacts.find((a) => a.type === 'prospect_list') ||
    (prospectCount > 0
      ? { type: 'prospect_list', count: prospectCount }
      : null);

  if (prospectCount <= 0) {
    blockingIssues.push(
      'Discovery returned zero verified companies. Campaign generation cannot continue.'
    );
  }

  if (blockingIssues.length) {
    return {
      outcome: STAGE_OUTCOMES.BLOCKED,
      blockingIssues,
      warnings,
      publishedArtifacts: [],
      quarantinedArtifacts: stampArtifacts(
        rawArtifacts.length ? rawArtifacts : [{ type: 'prospect_list', count: 0 }],
        {
          validationStatus: ARTIFACT_VALIDATION_STATUS.QUARANTINED,
          reason: blockingIssues[0],
        }
      ),
      reason: 'empty_or_missing_inputs',
      details,
      reviewSummary: {
        stageStatus: STAGE_OUTCOME_LABELS[STAGE_OUTCOMES.BLOCKED],
        publishedCount: 0,
        blockingIssues,
      },
    };
  }

  if (targetCount != null && prospectCount < targetCount) {
    const msg = `Requested ${targetCount} prospects; found ${prospectCount}`;
    if (!warnings.some((w) => String(w).includes('Requested'))) {
      warnings.push(msg);
    }
    return {
      outcome: STAGE_OUTCOMES.COMPLETED_WITH_WARNINGS,
      blockingIssues: [],
      warnings,
      publishedArtifacts: stampArtifacts(
        prospectListArtifact ? [prospectListArtifact, ...rawArtifacts.filter((a) => a.type !== 'prospect_list')] : rawArtifacts,
        {
          validationStatus: ARTIFACT_VALIDATION_STATUS.VALID_WITH_WARNINGS,
          provenance: {
            profileId: profile.id,
            profileVersion: profile.version,
          },
        }
      ),
      reason: 'yield_shortfall',
      details,
      reviewSummary: {
        stageStatus: STAGE_OUTCOME_LABELS[STAGE_OUTCOMES.COMPLETED_WITH_WARNINGS],
        publishedCount: prospectCount,
        label: `${prospectCount} of ${targetCount} Prospects`,
        warnings,
      },
    };
  }

  return {
    outcome: STAGE_OUTCOMES.COMPLETED,
    blockingIssues: [],
    warnings,
    publishedArtifacts: stampArtifacts(
      prospectListArtifact
        ? [
            prospectListArtifact,
            ...rawArtifacts.filter((a) => a.type !== 'prospect_list'),
          ]
        : rawArtifacts,
      {
        validationStatus: ARTIFACT_VALIDATION_STATUS.VALID,
        provenance: {
          profileId: profile.id,
          profileVersion: profile.version,
        },
      }
    ),
    reason: 'ok',
    details,
    reviewSummary: {
      stageStatus: STAGE_OUTCOME_LABELS[STAGE_OUTCOMES.COMPLETED],
      publishedCount: prospectCount,
      label: `${prospectCount} Prospects`,
    },
  };
}

function validateEnrichment(result, context) {
  const outputs = result.outputs || {};
  const warnings = [...(result.warnings || [])];
  const prior =
    (context.inputs && context.inputs.prospects) ||
    (context.priorOutputs && context.priorOutputs.prospects) ||
    [];
  const priorCount = Array.isArray(prior) ? prior.length : 0;
  const enriched = Array.isArray(outputs.prospects) ? outputs.prospects : [];
  const enrichedCount =
    outputs.enrichedCount != null ? Number(outputs.enrichedCount) : enriched.length;

  if (priorCount > 0 && enrichedCount === 0) {
    return {
      outcome: STAGE_OUTCOMES.BLOCKED,
      blockingIssues: [
        'Company enrichment produced no packages for available prospects.',
      ],
      warnings,
      publishedArtifacts: [],
      quarantinedArtifacts: stampArtifacts(result.artifacts || [], {
        validationStatus: ARTIFACT_VALIDATION_STATUS.QUARANTINED,
        reason: 'Empty enrichment',
      }),
      reason: 'empty_enrichment',
      reviewSummary: {
        stageStatus: STAGE_OUTCOME_LABELS[STAGE_OUTCOMES.BLOCKED],
        publishedCount: 0,
      },
    };
  }

  if (priorCount > 0 && enrichedCount < priorCount) {
    warnings.push(
      `Enriched ${enrichedCount} of ${priorCount} prospects`
    );
    return warningPass(result, warnings, enrichedCount, 'enriched_list');
  }

  return successPass(result, warnings, enrichedCount);
}

function validateRanking(result, context) {
  const outputs = result.outputs || {};
  const warnings = [...(result.warnings || [])];
  const prior =
    (context.inputs && context.inputs.prospects) ||
    (context.priorOutputs && context.priorOutputs.prospects) ||
    [];
  const priorCount = Array.isArray(prior) ? prior.length : 0;
  const ranked = Array.isArray(outputs.prospects) ? outputs.prospects : [];
  const rankedCount =
    outputs.rankedCount != null ? Number(outputs.rankedCount) : ranked.length;

  if (priorCount > 0 && rankedCount === 0) {
    return {
      outcome: STAGE_OUTCOMES.BLOCKED,
      blockingIssues: ['Ranking produced no ranked opportunities.'],
      warnings,
      publishedArtifacts: [],
      quarantinedArtifacts: stampArtifacts(result.artifacts || [], {
        validationStatus: ARTIFACT_VALIDATION_STATUS.QUARANTINED,
        reason: 'Empty ranking',
      }),
      reason: 'empty_ranking',
      reviewSummary: {
        stageStatus: STAGE_OUTCOME_LABELS[STAGE_OUTCOMES.BLOCKED],
        publishedCount: 0,
      },
    };
  }

  const missingScores = ranked.filter(
    (p) => p.priorityScore == null && p.rank == null && p.confidence == null
  );
  if (missingScores.length) {
    warnings.push(
      `${missingScores.length} opportunities missing ranking score`
    );
  }

  if (warnings.length) {
    return warningPass(result, warnings, rankedCount, 'ranked_prospects');
  }
  return successPass(result, warnings, rankedCount);
}

function validateSalesIntelligence(result, context) {
  const outputs = result.outputs || {};
  const warnings = [...(result.warnings || [])];
  const prior =
    (context.inputs && context.inputs.prospects) ||
    (context.priorOutputs && context.priorOutputs.prospects) ||
    [];
  const priorCount = Array.isArray(prior) ? prior.length : 0;
  const profiles = Array.isArray(outputs.profiles)
    ? outputs.profiles
    : Array.isArray(outputs.salesIntelligenceProfiles)
      ? outputs.salesIntelligenceProfiles
      : [];
  const profileCount =
    outputs.profileCount != null ? Number(outputs.profileCount) : profiles.length;
  const sendableCount =
    outputs.sendableCount != null
      ? Number(outputs.sendableCount)
      : profiles.filter((p) => p && p.sendable).length;

  if (priorCount > 0 && profileCount === 0) {
    return {
      outcome: STAGE_OUTCOMES.BLOCKED,
      blockingIssues: [
        'Sales Intelligence produced no profiles for available prospects.',
      ],
      warnings,
      publishedArtifacts: [],
      quarantinedArtifacts: stampArtifacts(result.artifacts || [], {
        validationStatus: ARTIFACT_VALIDATION_STATUS.QUARANTINED,
        reason: 'Empty sales intelligence',
      }),
      reason: 'empty_sales_intelligence',
      reviewSummary: {
        stageStatus: STAGE_OUTCOME_LABELS[STAGE_OUTCOMES.BLOCKED],
        publishedCount: 0,
      },
    };
  }

  if (sendableCount < profileCount) {
    warnings.push(
      `${profileCount - sendableCount} of ${profileCount} profiles non-sendable after quality gates`
    );
  }

  if (warnings.length) {
    return warningPass(
      result,
      warnings,
      profileCount,
      'sales_intelligence_profile'
    );
  }
  return successPass(result, warnings, profileCount);
}

function validateCampaign(result) {
  const outputs = result.outputs || {};
  const warnings = [...(result.warnings || [])];
  const campaign = outputs.campaign || null;
  const prospectCount =
    (campaign && campaign.prospectCount != null
      ? Number(campaign.prospectCount)
      : null) ??
    (Array.isArray(campaign && campaign.prospects)
      ? campaign.prospects.length
      : 0);

  if (!campaign) {
    return {
      outcome: STAGE_OUTCOMES.BLOCKED,
      blockingIssues: ['Campaign artifact was not published.'],
      warnings,
      publishedArtifacts: [],
      quarantinedArtifacts: stampArtifacts(result.artifacts || [], {
        validationStatus: ARTIFACT_VALIDATION_STATUS.QUARANTINED,
        reason: 'Missing campaign',
      }),
      reason: 'missing_campaign',
      reviewSummary: {
        stageStatus: STAGE_OUTCOME_LABELS[STAGE_OUTCOMES.BLOCKED],
        publishedCount: 0,
      },
    };
  }

  if (prospectCount <= 0) {
    return {
      outcome: STAGE_OUTCOMES.BLOCKED,
      blockingIssues: [
        'Campaign requires prospect count > 0. Generation cannot continue.',
      ],
      warnings,
      publishedArtifacts: [],
      quarantinedArtifacts: stampArtifacts(result.artifacts || [], {
        validationStatus: ARTIFACT_VALIDATION_STATUS.QUARANTINED,
        reason: 'Empty campaign',
      }),
      reason: 'empty_campaign',
      reviewSummary: {
        stageStatus: STAGE_OUTCOME_LABELS[STAGE_OUTCOMES.BLOCKED],
        publishedCount: 0,
        blockingIssues: [
          'Campaign requires prospect count > 0. Generation cannot continue.',
        ],
      },
    };
  }

  const mailMerge = Array.isArray(campaign.mailMerge) ? campaign.mailMerge : [];
  const missingPersonalization = mailMerge.filter(
    (row) => !row.personalizationSentence && !row.openingHook
  ).length;
  if (missingPersonalization > 0) {
    warnings.push(
      `${missingPersonalization} prospects missing personalization`
    );
  }

  if (warnings.length) {
    return warningPass(result, warnings, prospectCount, 'campaign_draft');
  }
  return successPass(result, warnings, prospectCount);
}

function validatePassthrough(result) {
  const warnings = [...(result.warnings || [])];
  if (warnings.length) {
    return warningPass(result, warnings, null);
  }
  return successPass(result, warnings, null);
}

function successPass(result, warnings, count) {
  return {
    outcome: STAGE_OUTCOMES.COMPLETED,
    blockingIssues: [],
    warnings,
    publishedArtifacts: stampArtifacts(result.artifacts || [], {
      validationStatus: ARTIFACT_VALIDATION_STATUS.VALID,
    }),
    quarantinedArtifacts: [],
    reason: 'ok',
    details: count != null ? { count } : {},
    reviewSummary: {
      stageStatus: STAGE_OUTCOME_LABELS[STAGE_OUTCOMES.COMPLETED],
      publishedCount: count,
    },
  };
}

function warningPass(result, warnings, count) {
  return {
    outcome: STAGE_OUTCOMES.COMPLETED_WITH_WARNINGS,
    blockingIssues: [],
    warnings,
    publishedArtifacts: stampArtifacts(result.artifacts || [], {
      validationStatus: ARTIFACT_VALIDATION_STATUS.VALID_WITH_WARNINGS,
    }),
    quarantinedArtifacts: [],
    reason: 'warnings',
    details: count != null ? { count } : {},
    reviewSummary: {
      stageStatus: STAGE_OUTCOME_LABELS[STAGE_OUTCOMES.COMPLETED_WITH_WARNINGS],
      publishedCount: count,
      warnings,
    },
  };
}

function stampArtifacts(artifacts, meta) {
  return (Array.isArray(artifacts) ? artifacts : []).map((a) => ({
    ...a,
    validationStatus: meta.validationStatus,
    provenance: meta.provenance || a.provenance || null,
    validationReason: meta.reason || null,
    validatedAt: new Date().toISOString(),
  }));
}

function flattenErrors(errors) {
  if (!Array.isArray(errors)) return [];
  return errors.map((e) => {
    if (typeof e === 'string') return e;
    if (e && e.message) return String(e.message);
    return String(e);
  });
}

function buildGateDecision(partial) {
  return {
    outcome: partial.outcome,
    outcomeLabel: STAGE_OUTCOME_LABELS[partial.outcome] || partial.outcome,
    capabilityId: partial.capabilityId,
    contract: partial.contract,
    blockingIssues: partial.blockingIssues || [],
    warnings: partial.warnings || [],
    publishedArtifacts: partial.publishedArtifacts || [],
    quarantinedArtifacts: partial.quarantinedArtifacts || [],
    validation: partial.validation || { passed: false },
    advance: Boolean(partial.advance),
    publishOutputs: Boolean(partial.publishOutputs),
    reviewSummary: partial.reviewSummary || null,
  };
}

/**
 * Whether artifact validation gate is enabled (default on when Mission Engine on).
 */
function artifactValidationEnabled() {
  const flag = process.env.MISSION_ARTIFACT_VALIDATION;
  if (flag === '0' || flag === 'false' || flag === 'off') return false;
  return true;
}

module.exports = {
  STAGE_OUTCOMES,
  STAGE_OUTCOME_LABELS,
  ARTIFACT_VALIDATION_STATUS,
  STAGE_CONTRACTS,
  getStageContract,
  evaluatePipelineGate,
  artifactValidationEnabled,
  validateDiscovery,
  validateCampaign,
  validateRanking,
  validateEnrichment,
};
