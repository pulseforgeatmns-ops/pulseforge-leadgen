'use strict';

/**
 * Discovery Diagnostics — read-only diagnostic capability (SPEC-056 / ADR-040).
 *
 * Produces typed diagnostic artifacts explaining Discovery execution
 * (provider selection, candidate counts, verification, exceptions).
 * Never mutates business state. Never rebuilds campaigns.
 */

const {
  CAPABILITY_CATEGORIES,
  BUILTIN_IDS,
  buildCapabilityResult,
  buildCapabilityEstimate,
  CAPABILITY_RESULT_STATUS,
} = require('../types');

const DIAGNOSTIC_ARTIFACT_TYPES = Object.freeze([
  'DiscoveryExecution',
  'DiscoveryTrace',
  'DiscoveryDiagnostics',
  'ProviderSelection',
  'CandidateCounts',
  'VerificationResults',
  'Exceptions',
  'CapabilityExecution',
  'CapabilityFailure',
  'MissionDiagnostics',
]);

/**
 * @param {object} [deps]
 * @returns {object} capability descriptor
 */
function createDiscoveryDiagnosticsCapability(deps = {}) {
  return {
    id: BUILTIN_IDS.DISCOVERY_DIAGNOSTICS,
    name: 'Discovery Diagnostics',
    description:
      'Inspect Discovery execution traces and explain failures without mutating business state',
    category: CAPABILITY_CATEGORIES.DIAGNOSTICS,
    outcomeTags: ['discovery_diagnosed', 'diagnostics'],
    readOnly: true,
    diagnostic: true,
    mutatesBusinessState: false,
    retryable: false,
    timeoutMs: 15_000,
    supportsRollback: false,
    idempotent: true,
    inputSchema: { required: [] },
    outputSchema: {
      discoveryDiagnostics: 'object',
      discoveryTrace: 'object',
    },
    canRun() {
      return true;
    },
    estimate() {
      return buildCapabilityEstimate({
        durationMs: 800,
        confidence: 0.9,
        notes: ['read-only diagnostic inspection'],
      });
    },
    async execute(context) {
      const started = Date.now();
      const inspection = inspectDiscovery(context, deps);
      const duration = Date.now() - started;

      const discoveryExecution = {
        artifactType: 'DiscoveryExecution',
        readOnly: true,
        campaignId: inspection.campaignId,
        ran: inspection.ran,
        provider: inspection.provider,
        startedAt: inspection.startedAt,
        completedAt: inspection.completedAt,
        status: inspection.status,
      };

      const discoveryTrace = {
        artifactType: 'DiscoveryTrace',
        readOnly: true,
        steps: inspection.traceSteps,
        providerSelection: inspection.provider,
        rawCandidateCount: inspection.rawCandidateCount,
        verifiedCount: inspection.verifiedCount,
        rejectedCount: inspection.rejectedCount,
        rejectionReasons: inspection.rejectionReasons,
      };

      const candidateCounts = {
        artifactType: 'CandidateCounts',
        readOnly: true,
        raw: inspection.rawCandidateCount,
        verified: inspection.verifiedCount,
        rejected: inspection.rejectedCount,
        minimumRequired: inspection.minimumRequired,
      };

      const verificationResults = {
        artifactType: 'VerificationResults',
        readOnly: true,
        verifiedCount: inspection.verifiedCount,
        rejectedByConfidence: inspection.rejectedByConfidence,
        confidenceThreshold: inspection.confidenceThreshold,
      };

      const providerSelection = {
        artifactType: 'ProviderSelection',
        readOnly: true,
        provider: inspection.provider,
        reason: inspection.providerReason,
      };

      const exceptions = {
        artifactType: 'Exceptions',
        readOnly: true,
        items: inspection.exceptions,
      };

      const discoveryDiagnostics = {
        artifactType: 'DiscoveryDiagnostics',
        readOnly: true,
        summary: inspection.summary,
        explanation: inspection.explanation,
        blocked: inspection.blocked,
        blockReason: inspection.blockReason,
        provider: inspection.provider,
        candidateCounts,
        verificationResults,
        exceptions: inspection.exceptions,
      };

      const missionDiagnostics = {
        artifactType: 'MissionDiagnostics',
        readOnly: true,
        summary: inspection.summary,
        related: ['DiscoveryDiagnostics'],
      };

      const capabilityExecution = {
        artifactType: 'CapabilityExecution',
        readOnly: true,
        capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
        status: inspection.status,
        ran: inspection.ran,
      };

      const capabilityFailure =
        inspection.status === 'failed' || inspection.blocked
          ? {
              artifactType: 'CapabilityFailure',
              readOnly: true,
              capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
              reason: inspection.blockReason || inspection.summary,
            }
          : null;

      const artifacts = [
        { type: 'DiscoveryExecution', readOnly: true, payload: discoveryExecution },
        { type: 'DiscoveryTrace', readOnly: true, payload: discoveryTrace },
        {
          type: 'DiscoveryDiagnostics',
          readOnly: true,
          payload: discoveryDiagnostics,
        },
        { type: 'ProviderSelection', readOnly: true, payload: providerSelection },
        { type: 'CandidateCounts', readOnly: true, payload: candidateCounts },
        {
          type: 'VerificationResults',
          readOnly: true,
          payload: verificationResults,
        },
        { type: 'Exceptions', readOnly: true, payload: exceptions },
        {
          type: 'CapabilityExecution',
          readOnly: true,
          payload: capabilityExecution,
        },
        {
          type: 'MissionDiagnostics',
          readOnly: true,
          payload: missionDiagnostics,
        },
      ];
      if (capabilityFailure) {
        artifacts.push({
          type: 'CapabilityFailure',
          readOnly: true,
          payload: capabilityFailure,
        });
      }

      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        duration,
        outputs: {
          readOnly: true,
          mutatesBusinessState: false,
          discoveryExecution,
          discoveryTrace,
          discoveryDiagnostics,
          providerSelection,
          candidateCounts,
          verificationResults,
          exceptions,
          capabilityExecution,
          capabilityFailure,
          missionDiagnostics,
          explanation: inspection.explanation,
        },
        evidence: [
          {
            kind: 'diagnostics',
            summary: inspection.summary,
            readOnly: true,
          },
        ],
        artifacts,
        warnings: inspection.warnings || [],
      });
    },
  };
}

/**
 * Inspect prior Discovery outputs / mission constraints without side effects.
 * @param {object} context
 * @param {object} deps
 */
function inspectDiscovery(context, deps = {}) {
  const constraints =
    (context && context.constraints && typeof context.constraints === 'object'
      ? context.constraints
      : {}) || {};
  const inputs = (context && context.inputs) || {};
  const prior = (inputs && inputs.priorOutputs) || {};
  const campaignId =
    constraints.campaignId ||
    (context.missionPlan &&
      context.missionPlan.parameters &&
      context.missionPlan.parameters.campaign) ||
    null;

  // Prefer explicit diagnostic seed (tests / prior mission artifacts)
  const seed =
    inputs.discoveryDiagnosticsSeed ||
    constraints.discoveryDiagnosticsSeed ||
    prior.discoveryDiagnostics ||
    null;

  if (seed && typeof seed === 'object') {
    return normalizeInspection(seed, campaignId);
  }

  const prospects =
    inputs.prospects ||
    prior.prospects ||
    (prior.prospect_list && prior.prospect_list.prospects) ||
    [];
  const list = Array.isArray(prospects) ? prospects : [];
  const discoveryMeta =
    prior.discoveryMeta ||
    prior.discovery ||
    constraints.discoveryMeta ||
    {};

  const provider =
    discoveryMeta.provider ||
    discoveryMeta.providerName ||
    (deps.defaultProvider != null ? deps.defaultProvider : 'unknown');
  const rawCandidateCount =
    discoveryMeta.rawCandidateCount != null
      ? Number(discoveryMeta.rawCandidateCount)
      : discoveryMeta.rawCount != null
        ? Number(discoveryMeta.rawCount)
        : list.length;
  const verifiedCount =
    discoveryMeta.verifiedCount != null
      ? Number(discoveryMeta.verifiedCount)
      : list.filter((p) => p && p.verified !== false).length;
  const rejectedCount =
    discoveryMeta.rejectedCount != null
      ? Number(discoveryMeta.rejectedCount)
      : Math.max(0, rawCandidateCount - verifiedCount);
  const confidenceThreshold =
    discoveryMeta.confidenceThreshold != null
      ? Number(discoveryMeta.confidenceThreshold)
      : constraints.confidenceThreshold != null
        ? Number(constraints.confidenceThreshold)
        : 0.7;
  const rejectedByConfidence =
    discoveryMeta.rejectedByConfidence != null
      ? Number(discoveryMeta.rejectedByConfidence)
      : rejectedCount;
  const minimumRequired =
    discoveryMeta.minimumRequired != null
      ? Number(discoveryMeta.minimumRequired)
      : constraints.targetCount != null
        ? Number(constraints.targetCount)
        : 1;

  const ran =
    discoveryMeta.ran != null
      ? Boolean(discoveryMeta.ran)
      : rawCandidateCount > 0 || list.length > 0 || Boolean(discoveryMeta.provider);

  const blocked =
    discoveryMeta.blocked != null
      ? Boolean(discoveryMeta.blocked)
      : verifiedCount < minimumRequired;

  const blockReason = blocked
    ? discoveryMeta.blockReason ||
      `Campaign blocked because verified count (${verifiedCount}) < minimum (${minimumRequired}).`
    : null;

  const exceptions = Array.isArray(discoveryMeta.exceptions)
    ? discoveryMeta.exceptions
    : blocked
      ? [
          {
            code: 'verified_below_minimum',
            message: blockReason,
          },
        ]
      : [];

  const explanation = buildExplanation({
    provider,
    rawCandidateCount,
    verifiedCount,
    rejectedCount,
    rejectedByConfidence,
    confidenceThreshold,
    minimumRequired,
    blocked,
    ran,
  });

  const summary = blocked
    ? `Discovery diagnostics: ${verifiedCount} verified of ${rawCandidateCount} raw — below minimum ${minimumRequired}`
    : ran
      ? `Discovery diagnostics: ${verifiedCount} verified of ${rawCandidateCount} raw via ${provider}`
      : 'Discovery diagnostics: no prior Discovery execution evidence found';

  return {
    campaignId,
    ran,
    provider,
    providerReason:
      discoveryMeta.providerReason ||
      (provider !== 'unknown' ? `Selected ${provider}` : 'Provider not recorded'),
    startedAt: discoveryMeta.startedAt || null,
    completedAt: discoveryMeta.completedAt || null,
    status: blocked ? 'blocked' : ran ? 'completed' : 'missing',
    rawCandidateCount,
    verifiedCount,
    rejectedCount,
    rejectedByConfidence,
    confidenceThreshold,
    minimumRequired,
    rejectionReasons: discoveryMeta.rejectionReasons || [
      rejectedByConfidence > 0
        ? `${rejectedByConfidence} rejected by confidence threshold (${confidenceThreshold})`
        : null,
    ].filter(Boolean),
    exceptions,
    blocked,
    blockReason,
    summary,
    explanation,
    warnings: ran
      ? []
      : [
          'No Discovery execution record in workspace — diagnostics reflect absence of evidence',
        ],
    traceSteps: [
      {
        step: 'provider_selection',
        detail: provider,
      },
      {
        step: 'raw_candidates',
        detail: rawCandidateCount,
      },
      {
        step: 'verification',
        detail: { verified: verifiedCount, rejected: rejectedCount },
      },
      {
        step: 'threshold_gate',
        detail: {
          minimumRequired,
          blocked,
        },
      },
    ],
  };
}

function normalizeInspection(seed, campaignId) {
  const rawCandidateCount = Number(seed.rawCandidateCount || seed.raw || 0);
  const verifiedCount = Number(seed.verifiedCount || seed.verified || 0);
  const rejectedCount = Number(
    seed.rejectedCount != null
      ? seed.rejectedCount
      : Math.max(0, rawCandidateCount - verifiedCount)
  );
  const confidenceThreshold =
    seed.confidenceThreshold != null ? Number(seed.confidenceThreshold) : 0.7;
  const rejectedByConfidence =
    seed.rejectedByConfidence != null
      ? Number(seed.rejectedByConfidence)
      : rejectedCount;
  const minimumRequired =
    seed.minimumRequired != null ? Number(seed.minimumRequired) : 1;
  const provider = seed.provider || 'unknown';
  const blocked =
    seed.blocked != null
      ? Boolean(seed.blocked)
      : verifiedCount < minimumRequired;
  const ran = seed.ran != null ? Boolean(seed.ran) : true;
  const blockReason = blocked
    ? seed.blockReason ||
      `Campaign blocked because verified count (${verifiedCount}) < minimum (${minimumRequired}).`
    : null;

  return {
    campaignId: seed.campaignId || campaignId,
    ran,
    provider,
    providerReason: seed.providerReason || `Selected ${provider}`,
    startedAt: seed.startedAt || null,
    completedAt: seed.completedAt || null,
    status: seed.status || (blocked ? 'blocked' : 'completed'),
    rawCandidateCount,
    verifiedCount,
    rejectedCount,
    rejectedByConfidence,
    confidenceThreshold,
    minimumRequired,
    rejectionReasons: seed.rejectionReasons || [
      `${rejectedByConfidence} rejected by confidence threshold (${confidenceThreshold})`,
    ],
    exceptions: Array.isArray(seed.exceptions)
      ? seed.exceptions
      : blocked
        ? [{ code: 'verified_below_minimum', message: blockReason }]
        : [],
    blocked,
    blockReason,
    summary:
      seed.summary ||
      `Discovery selected ${provider}. Provider returned ${rawCandidateCount} raw candidates. ${verifiedCount} verified. ${rejectedByConfidence} rejected by confidence threshold.`,
    explanation:
      seed.explanation ||
      buildExplanation({
        provider,
        rawCandidateCount,
        verifiedCount,
        rejectedCount,
        rejectedByConfidence,
        confidenceThreshold,
        minimumRequired,
        blocked,
        ran,
      }),
    warnings: seed.warnings || [],
    traceSteps: seed.traceSteps || [
      { step: 'provider_selection', detail: provider },
      { step: 'raw_candidates', detail: rawCandidateCount },
      {
        step: 'verification',
        detail: { verified: verifiedCount, rejected: rejectedCount },
      },
      {
        step: 'threshold_gate',
        detail: { minimumRequired, blocked },
      },
    ],
  };
}

function buildExplanation(args) {
  const lines = [];
  if (!args.ran) {
    return 'No Discovery execution evidence is available to explain outcomes.';
  }
  lines.push(`Discovery selected ${args.provider}.`);
  lines.push(`Provider returned ${args.rawCandidateCount} raw candidates.`);
  lines.push(`${args.verifiedCount} verified.`);
  lines.push(
    `${args.rejectedByConfidence} rejected by confidence threshold (${args.confidenceThreshold}).`
  );
  if (args.blocked) {
    lines.push(
      `Campaign blocked because verified count (${args.verifiedCount}) < minimum (${args.minimumRequired}).`
    );
  }
  return lines.join(' ');
}

module.exports = {
  createDiscoveryDiagnosticsCapability,
  DIAGNOSTIC_ARTIFACT_TYPES,
  inspectDiscovery,
};
