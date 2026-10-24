'use strict';

/**
 * Assemble Campaign Review workspace view model (SPEC-034).
 */

const {
  PROSPECT_REVIEW_STATUS,
  CAMPAIGN_REVIEW_STATUS,
  REVIEW_SORT,
  DEFAULT_CONFIDENCE_THRESHOLD,
  buildCampaignReviewSummary,
  buildProspectQueueRow,
} = require('./types');
const { summarizeQueue, validateProspectForApproval } = require('./validate');

/**
 * Resolve campaign + mail packages + prospects from context-like inputs.
 * @param {object} context
 * @returns {object}
 */
function resolveReviewInputs(context = {}) {
  const inputs = context.inputs || {};
  const prior = inputs.priorOutputs || {};
  const constraints = context.constraints || {};

  const campaign =
    inputs.campaign ||
    prior.campaign ||
    constraints.campaign ||
    null;

  const mailBatch =
    inputs.mailBatch ||
    inputs.mailPackageBatch ||
    prior.mailBatch ||
    null;

  let packages =
    (mailBatch && Array.isArray(mailBatch.packages) && mailBatch.packages) ||
    (Array.isArray(inputs.packages) && inputs.packages) ||
    (Array.isArray(prior.packages) && prior.packages) ||
    [];

  let prospects =
    inputs.prospects ||
    (campaign && campaign.prospects) ||
    prior.prospects ||
    [];
  if (!Array.isArray(prospects)) prospects = [];

  const playbook =
    inputs.clientPlaybook ||
    constraints.clientPlaybook ||
    prior.clientPlaybook ||
    null;

  const discoveryProfile =
    constraints.discoveryProfile ||
    inputs.discoveryProfile ||
    (campaign && campaign.discoveryProfile) ||
    null;

  const confidenceThreshold =
    Number.isFinite(Number(inputs.confidenceThreshold))
      ? Number(inputs.confidenceThreshold)
      : Number.isFinite(Number(constraints.confidenceThreshold))
        ? Number(constraints.confidenceThreshold)
        : DEFAULT_CONFIDENCE_THRESHOLD;

  return {
    campaign,
    mailBatch,
    packages,
    prospects,
    playbook,
    discoveryProfile,
    confidenceThreshold,
    companyIntelligencePackages:
      inputs.companyIntelligencePackages ||
      prior.companyIntelligencePackages ||
      {},
  };
}

/**
 * Find mail package for a prospect.
 * @param {object} prospect
 * @param {object[]} packages
 * @returns {object|null}
 */
function matchPackage(prospect, packages) {
  if (!Array.isArray(packages) || !packages.length) return null;
  const id = prospect.id != null ? String(prospect.id) : null;
  const name = String(prospect.companyName || prospect.name || '').toLowerCase();
  return (
    packages.find(
      (pkg) =>
        (id && String(pkg.prospectId) === id) ||
        String(
          (pkg.letter && pkg.letter.companyName) ||
            (pkg.envelope && pkg.envelope.companyName) ||
            ''
        ).toLowerCase() === name
    ) || null
  );
}

/**
 * Build personalization facts list for a prospect / package.
 * @param {object} prospect
 * @param {object|null} pkg
 * @returns {string[]}
 */
function collectPersonalizationFacts(prospect, pkg) {
  const facts = [];
  const summary =
    (pkg && pkg.personalizationSummary) ||
    prospect.personalizationSummary ||
    null;
  if (summary && Array.isArray(summary.personalizationFacts)) {
    facts.push(...summary.personalizationFacts.map(String));
  }
  if (Array.isArray(prospect.personalizationFacts)) {
    facts.push(...prospect.personalizationFacts.map(String));
  }
  if (prospect.industry) facts.push(String(prospect.industry));
  if (prospect.opportunityBrief && prospect.opportunityBrief.whyFit) {
    facts.push(String(prospect.opportunityBrief.whyFit));
  }
  // Dedupe preserve order
  const seen = new Set();
  return facts.filter((f) => {
    const key = f.trim().toLowerCase();
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/**
 * Map package / prospect into a queue row with validation status.
 * @param {object} prospect
 * @param {object|null} pkg
 * @param {object} opts
 * @returns {object}
 */
function buildRowFromProspect(prospect, pkg, opts = {}) {
  const company =
    (pkg && pkg.letter && pkg.letter.companyName) ||
    prospect.companyName ||
    prospect.name ||
    '';
  const recipient =
    (pkg && pkg.letter && pkg.letter.recipientName) ||
    prospect.contactName ||
    prospect.recipientName ||
    '';
  const address =
    (pkg && pkg.envelope && pkg.envelope.mailingAddress) ||
    prospect.address ||
    prospect.mailingAddress ||
    '';
  const confidence =
    (pkg && Number(pkg.confidence)) ||
    Number(prospect.confidence) ||
    Number(prospect.personalizationConfidence) ||
    0;
  const score =
    Number(prospect.overallScore) ||
    Number(prospect.score) ||
    Number(prospect.icp_score) ||
    confidence;

  const letter = (pkg && pkg.letter) || prospect.letter || null;
  const letterPreview =
    (letter && (letter.body || letter.personalizedOpening)) ||
    prospect.letterPreview ||
    '';

  const mailValidationFailed =
    pkg &&
    pkg.status &&
    pkg.status !== 'ready_to_print' &&
    pkg.status !== 'approved';

  const validationErrors = [];
  if (mailValidationFailed && Array.isArray(pkg.warnings)) {
    // Surface package status as validation_failed when not ready
    validationErrors.push('validation_failed');
  } else if (mailValidationFailed) {
    validationErrors.push('validation_failed');
  }
  if (pkg && Array.isArray(pkg.warnings)) {
    for (const w of pkg.warnings) {
      if (/missing/i.test(String(w))) validationErrors.push(String(w));
    }
  }

  const intel =
    opts.companyIntelligence ||
    prospect.companyIntelligence ||
    (pkg && pkg.companyIntelligence) ||
    null;

  const opportunityBrief =
    prospect.opportunityBrief ||
    (intel && intel.opportunityBrief) ||
    null;

  const signals =
    (intel && Array.isArray(intel.signals) && intel.signals) ||
    (Array.isArray(prospect.signals) && prospect.signals) ||
    [];

  const evidence =
    (intel && Array.isArray(intel.evidence) && intel.evidence) ||
    (Array.isArray(prospect.evidence) && prospect.evidence) ||
    [];

  const companySummary =
    (intel && (intel.summary || intel.companySummary)) ||
    prospect.companySummary ||
    null;

  let status = PROSPECT_REVIEW_STATUS.NEEDS_REVIEW;
  if (pkg && pkg.skipped) {
    status = PROSPECT_REVIEW_STATUS.SKIPPED;
  } else if (pkg && pkg.approved) {
    status = PROSPECT_REVIEW_STATUS.APPROVED;
  }

  const row = buildProspectQueueRow({
    prospectId: String(prospect.id != null ? prospect.id : company),
    status,
    company,
    recipient,
    score,
    confidence,
    personalization: collectPersonalizationFacts(prospect, pkg),
    letterPreview: String(letterPreview).slice(0, 280),
    address,
    lastModified: opts.now || new Date().toISOString(),
    validationErrors,
    operatorNote: prospect.operatorNote || null,
    insertChecklist:
      (pkg && pkg.insertChecklist) ||
      prospect.insertChecklist ||
      [],
    letter,
    envelope: (pkg && pkg.envelope) || null,
    companyIntelligence: intel,
    opportunityBrief,
    signals,
    evidence,
    companySummary,
    salesIntelligence:
      (pkg && pkg.salesIntelligence) ||
      prospect.salesIntelligenceProfile ||
      null,
    messagingStrategy:
      (pkg && pkg.messagingStrategy) ||
      (pkg &&
        pkg.salesIntelligence &&
        pkg.salesIntelligence.messaging_strategy) ||
      null,
    operatorConfidence:
      (pkg && pkg.operatorConfidence) ||
      (pkg &&
        pkg.salesIntelligence &&
        pkg.salesIntelligence.operatorConfidence) ||
      null,
    mailPackageId: pkg && pkg.id ? String(pkg.id) : null,
    skipped: Boolean(pkg && pkg.skipped),
    required: !(pkg && pkg.skipped),
    mailValidationFailed: Boolean(mailValidationFailed),
  });

  // Attach for validateProspectForApproval
  row.mailValidationFailed = Boolean(mailValidationFailed);

  if (row.status !== PROSPECT_REVIEW_STATUS.SKIPPED &&
      row.status !== PROSPECT_REVIEW_STATUS.APPROVED) {
    const gate = validateProspectForApproval(row, {
      confidenceThreshold: opts.confidenceThreshold,
    });
    if (!gate.ok) {
      row.status = PROSPECT_REVIEW_STATUS.BLOCKED;
      row.validationErrors = gate.errors;
    } else {
      row.status = PROSPECT_REVIEW_STATUS.NEEDS_REVIEW;
      row.validationErrors = [];
    }
  }

  return row;
}

/**
 * Sort queue rows.
 * @param {object[]} rows
 * @param {string} sort
 * @returns {object[]}
 */
function sortQueue(rows, sort = REVIEW_SORT.NEEDS_REVIEW) {
  const copy = rows.slice();
  const statusWeight = (s) => {
    if (s === PROSPECT_REVIEW_STATUS.BLOCKED) return 0;
    if (s === PROSPECT_REVIEW_STATUS.NEEDS_REVIEW) return 1;
    if (s === PROSPECT_REVIEW_STATUS.REJECTED) return 2;
    if (s === PROSPECT_REVIEW_STATUS.APPROVED) return 3;
    return 4;
  };

  if (sort === REVIEW_SORT.SCORE) {
    copy.sort((a, b) => Number(b.score) - Number(a.score));
  } else if (sort === REVIEW_SORT.CONFIDENCE) {
    copy.sort((a, b) => Number(b.confidence) - Number(a.confidence));
  } else if (sort === REVIEW_SORT.ALPHABETICAL) {
    copy.sort((a, b) =>
      String(a.company).localeCompare(String(b.company), undefined, {
        sensitivity: 'base',
      })
    );
  } else {
    copy.sort((a, b) => {
      const w = statusWeight(a.status) - statusWeight(b.status);
      if (w !== 0) return w;
      return Number(b.score) - Number(a.score);
    });
  }
  return copy;
}

/**
 * Assemble full workspace from inputs.
 * @param {object} context
 * @param {object} [opts]
 * @returns {object}
 */
function assembleWorkspace(context = {}, opts = {}) {
  const resolved = resolveReviewInputs(context);
  const now = opts.now || new Date().toISOString();
  const sort = opts.sort || REVIEW_SORT.NEEDS_REVIEW;

  const prospectMap = new Map();
  for (const p of resolved.prospects) {
    const id = String(p.id != null ? p.id : p.companyName || p.name || '');
    if (id) prospectMap.set(id, p);
  }

  // Prefer packages as source of truth when present; else prospects
  const sources =
    resolved.packages.length > 0
      ? resolved.packages.map((pkg) => {
          const pid = String(pkg.prospectId || '');
          const prospect =
            prospectMap.get(pid) ||
            resolved.prospects.find(
              (p) =>
                String(p.companyName || p.name || '').toLowerCase() ===
                String(
                  (pkg.letter && pkg.letter.companyName) || ''
                ).toLowerCase()
            ) ||
            { id: pid, companyName: pkg.letter && pkg.letter.companyName };
          return { prospect, pkg };
        })
      : resolved.prospects.map((prospect) => ({
          prospect,
          pkg: matchPackage(prospect, resolved.packages),
        }));

  const queue = sources.map(({ prospect, pkg }) => {
    const key = String(prospect.id != null ? prospect.id : '');
    const intel =
      resolved.companyIntelligencePackages[key] ||
      prospect.companyIntelligence ||
      null;
    return buildRowFromProspect(prospect, pkg, {
      confidenceThreshold: resolved.confidenceThreshold,
      companyIntelligence: intel,
      now,
    });
  });

  const sorted = sortQueue(queue, sort);
  const counts = summarizeQueue(sorted);
  const campaign = resolved.campaign || {};
  const mailPackageGenerated =
    Boolean(resolved.mailBatch) || resolved.packages.length > 0;

  const discoveryName =
    (resolved.discoveryProfile &&
      (resolved.discoveryProfile.name || resolved.discoveryProfile.id)) ||
    (campaign.discoveryProfile &&
      (campaign.discoveryProfile.name || campaign.discoveryProfile)) ||
    null;

  const clientName =
    (resolved.playbook && resolved.playbook.clientName) ||
    (resolved.playbook && resolved.playbook.name) ||
    campaign.clientName ||
    campaign.client ||
    null;

  let status = CAMPAIGN_REVIEW_STATUS.IN_REVIEW;
  if (counts.blockedCount > 0 && counts.readyCount === 0) {
    status = CAMPAIGN_REVIEW_STATUS.BLOCKED;
  }

  const summary = buildCampaignReviewSummary({
    campaignName: campaign.name || opts.campaignName || 'Campaign',
    client: clientName,
    discoveryProfile: discoveryName,
    generatedAt: now,
    revision: opts.revision || 1,
    activeRevision: opts.activeRevision != null ? opts.activeRevision : opts.revision || 1,
    ...counts,
    status,
    mailPackageGenerated,
  });

  return {
    summary,
    queue: sorted,
    mailBatch: resolved.mailBatch,
    mailPackageGenerated,
    campaign,
    playbook: resolved.playbook,
    discoveryProfile: resolved.discoveryProfile,
    confidenceThreshold: resolved.confidenceThreshold,
    selectedProspectId: opts.selectedProspectId || (sorted[0] && sorted[0].prospectId) || null,
    operatorActions: require('./types').OPERATOR_ACTIONS.slice(),
  };
}

/**
 * Build prospect detail pane for a selected row.
 * @param {object} workspace
 * @param {string} prospectId
 * @returns {object|null}
 */
function getProspectDetail(workspace, prospectId) {
  const row = (workspace.queue || []).find(
    (r) => String(r.prospectId) === String(prospectId)
  );
  if (!row) return null;
  return {
    prospectId: row.prospectId,
    status: row.status,
    companyIntelligence: {
      companySummary: row.companySummary,
      signals: row.signals,
      opportunityBrief: row.opportunityBrief,
      evidence: row.evidence,
      package: row.companyIntelligence,
    },
    personalization: {
      facts: row.personalization,
      confidence: row.confidence,
    },
    letterPreview: {
      editable: true,
      letter: row.letter,
      preview: row.letterPreview,
    },
    envelopePreview: row.envelope,
    insertChecklist: {
      editable: true,
      items: row.insertChecklist,
    },
    validationErrors: row.validationErrors,
    operatorNote: row.operatorNote,
  };
}

module.exports = {
  resolveReviewInputs,
  matchPackage,
  collectPersonalizationFacts,
  buildRowFromProspect,
  sortQueue,
  assembleWorkspace,
  getProspectDetail,
};
