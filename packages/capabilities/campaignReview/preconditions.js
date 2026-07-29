'use strict';

/**
 * Campaign Review canRun / diagnoseCanRun (SPEC-058 / ADR-042).
 *
 * Execution mode: boolean canRun gates work.
 * Diagnostic mode: diagnoseCanRun explains why work cannot run.
 */

const EXPECTED_ARTIFACTS = Object.freeze([
  {
    artifact: 'Campaign',
    alias: 'campaign',
    producer: 'Campaign Builder',
    producerId: 'campaign_builder',
  },
  {
    artifact: 'MailPackage',
    alias: 'mail_packages',
    producer: 'Mail Package Generator',
    producerId: 'mail_package_generator',
  },
]);

const DIAGNOSTIC_EVIDENCE_KEYS = Object.freeze([
  'discoveryDiagnostics',
  'discoveryTrace',
  'discoveryExecution',
  'DiscoveryDiagnostics',
  'DiscoveryTrace',
  'DiscoveryExecution',
  'providerSelection',
  'candidateCounts',
  'verificationResults',
  'exceptions',
  'missionDiagnostics',
  'capabilityFailure',
]);

/**
 * SPEC-058 diagnoseCanRun contract for Campaign Review.
 * @param {object} [context]
 * @returns {object}
 */
function diagnoseCampaignReviewCanRun(context = {}) {
  const inspection = inspectCampaignReviewPreconditions(context);
  if (inspection.runnable) {
    return Object.freeze({
      runnable: true,
      reason: null,
      failedPrecondition: null,
      expectedArtifact: null,
      actualState: inspection.actualState,
      producer: null,
      expectedProducer: null,
      recommendedNextAction: null,
      // Compat / workspace extras
      ok: true,
      status: 'Ready',
      capabilityId: 'campaign_review',
      capabilityName: 'Campaign Review',
      present: inspection.present,
      missing: [],
      hasDiagnosticEvidence: inspection.hasDiagnosticEvidence,
      diagnosticEvidence: inspection.diagnosticEvidence,
    });
  }

  return Object.freeze({
    runnable: false,
    reason: inspection.failedPrecondition,
    failedPrecondition: inspection.failedPrecondition,
    expectedArtifact: inspection.expectedArtifact,
    actualState: inspection.actualState,
    producer: inspection.producer,
    expectedProducer: inspection.producer,
    recommendedNextAction: inspection.recommendedNextAction,
    ok: false,
    status: 'Blocked',
    capabilityId: 'campaign_review',
    capabilityName: 'Campaign Review',
    expectedArtifacts: inspection.expectedArtifacts,
    producerId: inspection.producerId,
    present: inspection.present,
    missing: inspection.missing,
    hasDiagnosticEvidence: inspection.hasDiagnosticEvidence,
    diagnosticEvidence: inspection.diagnosticEvidence,
  });
}

/**
 * Internal precondition inspection shared by canRun + diagnoseCanRun.
 * @param {object} [context]
 * @returns {object}
 */
function inspectCampaignReviewPreconditions(context = {}) {
  const inputs = (context && context.inputs) || {};
  const prior = inputs.priorOutputs || {};
  const artifacts = Array.isArray(inputs.artifacts) ? inputs.artifacts : [];

  const campaign =
    inputs.campaign ||
    prior.campaign ||
    artifactPayload(artifacts, ['Campaign', 'campaign']);
  const packages = resolvePackages(inputs, prior, artifacts);
  const prospects = resolveProspects(inputs, prior, campaign, artifacts);

  const hasCampaign = Boolean(campaign && typeof campaign === 'object');
  const hasPackages = Array.isArray(packages) && packages.length > 0;
  const hasProspects = Array.isArray(prospects) && prospects.length > 0;
  const runnable = hasCampaign || hasPackages || hasProspects;

  const present = [];
  if (hasCampaign) present.push('Campaign');
  if (hasPackages) present.push('MailPackage');
  if (hasProspects) present.push('ProspectList');

  const diagnosticEvidence = collectDiagnosticEvidence(inputs, prior, artifacts);
  const hasDiagnosticEvidence = diagnosticEvidence.length > 0;

  const missing = [];
  if (!hasCampaign) missing.push(EXPECTED_ARTIFACTS[0]);
  if (!hasPackages) missing.push(EXPECTED_ARTIFACTS[1]);

  const primaryMissing = missing[0] || EXPECTED_ARTIFACTS[0];
  const failedPrecondition = runnable
    ? null
    : 'Campaign artifact required';

  const actualState = runnable
    ? `Present: ${present.join(', ')}`
    : present.length
      ? `Present: ${present.join(', ')}; Campaign missing`
      : hasDiagnosticEvidence
        ? `Not Present (diagnostic evidence: ${diagnosticEvidence.join(', ')})`
        : 'Not Present';

  const recommendedNextAction = runnable
    ? 'Proceed with Campaign Review'
    : hasDiagnosticEvidence
      ? 'Complete Campaign Builder after Discovery produces a ProspectList.'
      : 'Execute Campaign Builder after Discovery succeeds.';

  return {
    runnable,
    ok: runnable,
    failedPrecondition,
    expectedArtifact: primaryMissing.artifact,
    expectedArtifacts: missing.map((m) => m.artifact),
    actualState,
    producer: primaryMissing.producer,
    producerId: primaryMissing.producerId,
    recommendedNextAction,
    present,
    missing: missing.map((m) => ({
      artifact: m.artifact,
      producer: m.producer,
      producerId: m.producerId,
    })),
    hasDiagnosticEvidence,
    diagnosticEvidence,
    status: runnable ? 'Ready' : 'Blocked',
  };
}

/**
 * @param {object} diagnosis
 * @returns {object} CapabilityRunner error / blocked entry
 */
function toCanRunError(diagnosis) {
  const d = diagnosis || diagnoseCampaignReviewCanRun();
  return {
    code: 'can_run_precondition_blocked',
    message: d.failedPrecondition || d.reason || 'canRun precondition blocked',
    capabilityId: d.capabilityId || 'campaign_review',
    failedPrecondition: d.failedPrecondition || d.reason,
    expectedArtifact: d.expectedArtifact,
    actualState: d.actualState,
    producer: d.producer || d.expectedProducer,
    expectedProducer: d.expectedProducer || d.producer,
    recommendedNextAction: d.recommendedNextAction,
    diagnosis: d,
  };
}

function resolvePackages(inputs, prior, artifacts) {
  if (Array.isArray(inputs.packages) && inputs.packages.length) {
    return inputs.packages;
  }
  if (inputs.mailBatch && Array.isArray(inputs.mailBatch.packages)) {
    return inputs.mailBatch.packages;
  }
  if (Array.isArray(prior.packages) && prior.packages.length) {
    return prior.packages;
  }
  if (prior.mailBatch && Array.isArray(prior.mailBatch.packages)) {
    return prior.mailBatch.packages;
  }
  const fromArt = artifactPayload(artifacts, [
    'MailPackage',
    'mail_packages',
    'mail_package',
  ]);
  if (Array.isArray(fromArt)) return fromArt;
  if (fromArt && Array.isArray(fromArt.packages)) return fromArt.packages;
  return [];
}

function resolveProspects(inputs, prior, campaign, artifacts) {
  if (Array.isArray(inputs.prospects) && inputs.prospects.length) {
    return inputs.prospects;
  }
  if (campaign && Array.isArray(campaign.prospects) && campaign.prospects.length) {
    return campaign.prospects;
  }
  if (Array.isArray(prior.prospects) && prior.prospects.length) {
    return prior.prospects;
  }
  const fromArt = artifactPayload(artifacts, ['ProspectList', 'prospect_list']);
  if (fromArt && Array.isArray(fromArt.prospects)) return fromArt.prospects;
  if (Array.isArray(fromArt)) return fromArt;
  return [];
}

function collectDiagnosticEvidence(inputs, prior, artifacts) {
  const found = new Set();
  for (const key of DIAGNOSTIC_EVIDENCE_KEYS) {
    if (inputs[key] != null) found.add(normalizeEvidenceLabel(key));
    if (prior[key] != null) found.add(normalizeEvidenceLabel(key));
  }
  for (const art of artifacts) {
    const type = art && (art.artifactType || art.type);
    if (!type) continue;
    if (
      /Discovery|ProviderSelection|CandidateCounts|VerificationResults|Exceptions|MissionDiagnostics|CapabilityFailure|CapabilityExecution|MissionState/i.test(
        String(type)
      )
    ) {
      found.add(String(type));
    }
    if (art && art.readOnly && art.diagnostic) {
      found.add(String(type));
    }
  }
  return [...found];
}

function normalizeEvidenceLabel(key) {
  const map = {
    discoveryDiagnostics: 'DiscoveryDiagnostics',
    discoveryTrace: 'DiscoveryTrace',
    discoveryExecution: 'DiscoveryExecution',
    providerSelection: 'ProviderSelection',
    candidateCounts: 'CandidateCounts',
    verificationResults: 'VerificationResults',
    exceptions: 'Exceptions',
    missionDiagnostics: 'MissionDiagnostics',
    capabilityFailure: 'CapabilityFailure',
  };
  return map[key] || String(key);
}

function artifactPayload(artifacts, types) {
  const wanted = new Set(types.map(String));
  for (const art of artifacts || []) {
    const type = art && (art.artifactType || art.type);
    if (type && wanted.has(String(type))) {
      return art.payload != null ? art.payload : art;
    }
  }
  return null;
}

module.exports = {
  EXPECTED_ARTIFACTS,
  inspectCampaignReviewPreconditions,
  diagnoseCampaignReviewCanRun,
  toCanRunError,
};
