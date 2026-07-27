'use strict';

/**
 * Assemble Direct Mail Execution view model from approved artifacts (SPEC-035).
 */

const {
  EXECUTION_STATUS,
  RESPONSE_STATUS,
  RESPONSE_METRIC_KINDS,
  buildProspectExecution,
  buildExecutionSummary,
  buildExecutionMetrics,
  buildCampaignLock,
  buildAssemblyChecklist,
} = require('./types');

/**
 * Resolve packages from inputs / prior outputs.
 * @param {object} inputs
 * @returns {object[]}
 */
function resolvePackages(inputs = {}) {
  if (Array.isArray(inputs.packages) && inputs.packages.length) {
    return inputs.packages;
  }
  if (inputs.mailBatch && Array.isArray(inputs.mailBatch.packages)) {
    return inputs.mailBatch.packages;
  }
  const prior = inputs.priorOutputs || {};
  if (Array.isArray(prior.packages) && prior.packages.length) {
    return prior.packages;
  }
  if (prior.mailBatch && Array.isArray(prior.mailBatch.packages)) {
    return prior.mailBatch.packages;
  }
  // Fall back to approved queue rows from campaign review
  if (Array.isArray(inputs.queue) && inputs.queue.length) {
    return inputs.queue
      .filter((r) => r.status === 'approved' || r.required !== false)
      .map((r) => ({
        id: r.mailPackageId || `pkg_${r.prospectId}`,
        prospectId: r.prospectId,
        letter: r.letter,
        envelope: r.envelope,
        insertChecklist: r.insertChecklist,
        status: 'ready_to_print',
      }));
  }
  return [];
}

/**
 * Build per-prospect execution rows from packages + optional campaign prospects.
 * @param {object} inputs
 * @returns {object[]}
 */
function buildProspectRows(inputs = {}) {
  const packages = resolvePackages(inputs);
  const campaign = inputs.campaign || {};
  const prospects = Array.isArray(campaign.prospects)
    ? campaign.prospects
    : Array.isArray(inputs.prospects)
      ? inputs.prospects
      : [];
  const byId = new Map(
    prospects.map((p) => [String(p.id || p.prospectId), p])
  );

  // Prefer packages; if empty, use approved prospects only
  const source =
    packages.length > 0
      ? packages.filter((p) => !p.skipped && p.status !== 'skipped')
      : prospects.filter((p) => p.status !== 'skipped' && p.status !== 'rejected');

  return source.map((pkg) => {
    const prospectId = String(
      pkg.prospectId || pkg.id || (pkg.companyName && pkg.companyName) || ''
    );
    const prospect = byId.get(prospectId) || {};
    const envelope = pkg.envelope || {};
    const letter = pkg.letter || {};
    return buildProspectExecution({
      prospectId,
      company:
        envelope.companyName ||
        letter.companyName ||
        prospect.companyName ||
        pkg.company ||
        '',
      recipient:
        envelope.recipientName ||
        letter.recipientName ||
        prospect.contactName ||
        prospect.recipient ||
        '',
      address:
        envelope.mailingAddress ||
        prospect.address ||
        pkg.address ||
        '',
      mailPackageId: pkg.id != null ? String(pkg.id) : null,
      assembly: buildAssemblyChecklist({}),
      responseStatus: RESPONSE_STATUS.NO_RESPONSE,
    });
  });
}

/**
 * Compute metrics from prospect rows.
 * @param {object[]} prospects
 * @returns {object}
 */
function computeMetrics(prospects = []) {
  const active = prospects.filter((p) => !p.skipped);
  let printed = 0;
  let assembled = 0;
  let mailed = 0;
  let responses = 0;
  let meetings = 0;
  let proposals = 0;
  let wins = 0;

  for (const p of active) {
    if (p.printed || p.assemblyComplete || p.mailed) printed += 1;
    if (p.assemblyComplete) assembled += 1;
    if (p.mailed) mailed += 1;
    const rs = p.responseStatus || RESPONSE_STATUS.NO_RESPONSE;
    if (RESPONSE_METRIC_KINDS.response.has(rs)) responses += 1;
    if (RESPONSE_METRIC_KINDS.meeting.has(rs)) meetings += 1;
    if (RESPONSE_METRIC_KINDS.proposal.has(rs)) proposals += 1;
    if (RESPONSE_METRIC_KINDS.win.has(rs)) wins += 1;
  }

  return buildExecutionMetrics({
    printed,
    assembled,
    mailed,
    responses,
    meetings,
    proposals,
    wins,
  });
}

/**
 * Assemble a fresh execution workspace from approved inputs.
 * @param {object} context
 * @param {object} [opts]
 * @returns {object}
 */
function assembleExecution(context = {}, opts = {}) {
  const inputs = context.inputs || context;
  const campaign = inputs.campaign || {};
  const packages = resolvePackages(inputs);
  const mailBatch = inputs.mailBatch || {
    id: inputs.mailPackageBatchId || null,
    packages,
  };
  const executionPackage =
    inputs.executionPackage ||
    (inputs.priorOutputs && inputs.priorOutputs.executionPackage) ||
    null;

  const revision =
    opts.revision != null
      ? Number(opts.revision)
      : inputs.approvedRevision != null
        ? Number(inputs.approvedRevision)
        : inputs.reviewRevision != null
          ? Number(inputs.reviewRevision)
          : campaign.revision != null
            ? Number(campaign.revision)
            : 1;

  const prospects = buildProspectRows(inputs);
  const metrics = computeMetrics(prospects);

  const summary = buildExecutionSummary({
    campaignName:
      campaign.name ||
      inputs.campaignName ||
      (opts.campaignName) ||
      'Campaign',
    campaignId:
      inputs.campaignId ||
      campaign.id ||
      null,
    client:
      inputs.client != null
        ? String(inputs.client)
        : context.clientId != null
          ? String(context.clientId)
          : null,
    status: opts.status || EXECUTION_STATUS.DRAFT,
    revision,
    prospectCount: prospects.length,
    locked: false,
    metrics,
  });

  return {
    summary,
    prospects,
    printSessions: [],
    auditLog: [],
    lock: buildCampaignLock({
      locked: false,
      campaignRevision: revision,
      mailPackageBatchId:
        (mailBatch && mailBatch.id) ||
        inputs.mailPackageBatchId ||
        null,
      executionPackageId:
        (executionPackage &&
          (executionPackage.id || executionPackage.revisionId)) ||
        inputs.executionPackageId ||
        null,
    }),
    pinnedArtifacts: {
      campaignRevision: revision,
      mailBatch: mailBatch
        ? {
            id: mailBatch.id || null,
            packageCount: packages.length,
            packageIds: packages.map((p) => p.id).filter(Boolean),
          }
        : null,
      executionPackage: executionPackage
        ? {
            id:
              executionPackage.id ||
              executionPackage.revisionId ||
              null,
            hasPrintPackage: Boolean(
              executionPackage.printPackage || executionPackage.print
            ),
            hasMailMerge: Boolean(executionPackage.mailMerge),
            hasAddressLabels: Boolean(executionPackage.addressLabels),
          }
        : null,
    },
    missionEvents: [],
    timeline: [],
    executionPackage,
    mailBatch,
  };
}

module.exports = {
  resolvePackages,
  buildProspectRows,
  computeMetrics,
  assembleExecution,
};
