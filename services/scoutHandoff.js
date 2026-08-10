'use strict';

/**
 * Max → Scout Prospect Sourcing Handoff.
 *
 * Turns a Scout Handoff Brief into an executable, review-first work request.
 * Guardrails: no outreach copy, sends, CRM writes, or account changes.
 * Results (when Scout sourcing is wired) are review-only until operator approval.
 */

const { randomUUID } = require('crypto');

const SCOUT_HANDOFF_KIND = 'scout_handoff';
const SCOUT_WORK_REQUEST_KIND = 'scout_work_request';
const SCOUT_CANDIDATE_BATCH_KIND = 'scout_sourced_candidate_batch';

const SCOUT_HANDOFF_STATUSES = Object.freeze({
  DRAFT: 'draft',
  APPROVED: 'approved',
  QUEUED: 'queued',
  IN_PROGRESS: 'in_progress',
  COMPLETED: 'completed',
  FAILED: 'failed',
  NEEDS_OPERATOR_REVIEW: 'needs_operator_review',
});

/** Operator-facing status labels (UI). */
const SCOUT_HANDOFF_UI_STATUS = Object.freeze({
  BRIEF_CREATED: 'Brief created',
  APPROVED_FOR_SCOUT: 'Approved for Scout',
  SCOUT_QUEUED: 'Scout queued',
  SCOUT_RUNNING: 'Scout running',
  SCOUT_RESULTS_READY: 'Scout results ready',
  SCOUT_UNAVAILABLE: 'Scout unavailable / not wired',
  SCOUT_FAILED: 'Scout failed',
  NEEDS_OPERATOR_REVIEW: 'Needs operator review',
});

const SCOUT_SOURCING_NOT_WIRED_MESSAGE =
  'Scout handoff brief is approved, but Scout sourcing execution is not wired yet.';

const DEFAULT_REQUIRED_FIELDS = Object.freeze([
  'Company or property manager name',
  'Website or source URL',
  'Location / market town',
  'Segment or subtype signal',
  'Suggested contact role',
  'Fit rationale',
  'Risks / uncertainties',
  'Confidence',
]);

const DEFAULT_SOURCE_TYPES = Object.freeze([
  'Public business directories and local listings for the approved segment',
  'Company websites and about/contact pages that confirm location and role signals',
  'Public property / facility / office manager listings when relevant to the segment',
  'Other openly published local-market sources — no private or gated scrapes',
]);

const DEFAULT_EVIDENCE_REQUIREMENTS = Object.freeze([
  'Source URL for each prospect record',
  'Location / market-town evidence matching approved bounds',
  'Segment or subtype signal from the public source',
  'Fit rationale grounded in visible public facts (not invented)',
  'Any disqualifying risk or uncertainty noted on the record',
]);

const DEFAULT_CONFIDENCE_RULES = Object.freeze([
  'High — source URL + in-market location + clear segment/subtype + reachable decision-maker signal',
  'Medium — source URL + in-market location + segment fit, but thin contact or subtype evidence',
  'Low / review_required — missing source URL, weak market match, or ambiguous fit; do not treat as ready',
]);

const DEFAULT_GUARDRAILS = Object.freeze([
  'Max does not build or fabricate the prospect list',
  'Scout inspects public sources only when sourcing execution is wired',
  'No outreach copy, sends, CRM writes, or account/DNS/GBP/social/tracking changes',
  'Scout results are review-only until operator approval',
  'Composer / CRM / export must not use candidates before operator approval',
]);

function nowIso() {
  return new Date().toISOString();
}

function asList(value, fallback = []) {
  if (Array.isArray(value) && value.length) {
    return value.map((v) => String(v || '').trim()).filter(Boolean);
  }
  return [...fallback];
}

function uiStatusForHandoff(handoff) {
  const h = handoff || {};
  if (h.sourcingUnavailable || h.executionWired === false) {
    if (
      h.status === SCOUT_HANDOFF_STATUSES.APPROVED ||
      h.status === SCOUT_HANDOFF_STATUSES.QUEUED ||
      h.status === SCOUT_HANDOFF_STATUSES.NEEDS_OPERATOR_REVIEW
    ) {
      return SCOUT_HANDOFF_UI_STATUS.SCOUT_UNAVAILABLE;
    }
  }
  switch (h.status) {
    case SCOUT_HANDOFF_STATUSES.DRAFT:
      return SCOUT_HANDOFF_UI_STATUS.BRIEF_CREATED;
    case SCOUT_HANDOFF_STATUSES.APPROVED:
      return SCOUT_HANDOFF_UI_STATUS.APPROVED_FOR_SCOUT;
    case SCOUT_HANDOFF_STATUSES.QUEUED:
      return SCOUT_HANDOFF_UI_STATUS.SCOUT_QUEUED;
    case SCOUT_HANDOFF_STATUSES.IN_PROGRESS:
      return SCOUT_HANDOFF_UI_STATUS.SCOUT_RUNNING;
    case SCOUT_HANDOFF_STATUSES.COMPLETED:
      return SCOUT_HANDOFF_UI_STATUS.SCOUT_RESULTS_READY;
    case SCOUT_HANDOFF_STATUSES.FAILED:
      return SCOUT_HANDOFF_UI_STATUS.SCOUT_FAILED;
    case SCOUT_HANDOFF_STATUSES.NEEDS_OPERATOR_REVIEW:
      return SCOUT_HANDOFF_UI_STATUS.NEEDS_OPERATOR_REVIEW;
    default:
      return SCOUT_HANDOFF_UI_STATUS.BRIEF_CREATED;
  }
}

/**
 * Build a Scout handoff object from approved criteria / brief fields.
 * Status starts as draft — creating the brief does not queue Scout.
 */
function buildScoutHandoff(fields = {}, opts = {}) {
  const createdAt = opts.createdAt || fields.createdAt || nowIso();
  const updatedAt = opts.updatedAt || fields.updatedAt || createdAt;
  const handoffId = fields.handoffId || opts.handoffId || randomUUID();

  const handoff = {
    kind: SCOUT_HANDOFF_KIND,
    handoffId,
    source: 'max',
    target: 'scout',
    status: fields.status || SCOUT_HANDOFF_STATUSES.DRAFT,
    campaignObjective: fields.campaignObjective || null,
    targetSegment: fields.targetSegment || null,
    targetSubtype: fields.targetSubtype || null,
    marketBounds: fields.marketBounds || fields.marketBound || null,
    inclusionCriteria: asList(fields.inclusionCriteria),
    exclusionCriteria: asList(fields.exclusionCriteria),
    requiredFields: asList(
      fields.requiredFields || fields.requiredProspectFields,
      DEFAULT_REQUIRED_FIELDS
    ),
    sourceTypes: asList(fields.sourceTypes, DEFAULT_SOURCE_TYPES),
    evidenceRequirements: asList(
      fields.evidenceRequirements || fields.evidenceRequired,
      DEFAULT_EVIDENCE_REQUIREMENTS
    ),
    confidenceRules: asList(fields.confidenceRules, DEFAULT_CONFIDENCE_RULES),
    guardrails: asList(fields.guardrails, DEFAULT_GUARDRAILS),
    createdAt,
    updatedAt,
    workRequestId: fields.workRequestId || null,
    workRequest: fields.workRequest || null,
    candidateBatch: fields.candidateBatch || null,
    executionWired: fields.executionWired == null ? null : Boolean(fields.executionWired),
    sourcingUnavailable: Boolean(fields.sourcingUnavailable),
    scoutRan: Boolean(fields.scoutRan),
    liveSourcingPerformed: false,
    prospectListGenerated: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
    crmWritesMade: false,
    resultsApproved: Boolean(fields.resultsApproved),
    reviewOnly: true,
  };

  handoff.uiStatus = uiStatusForHandoff(handoff);
  return handoff;
}

function buildScoutWorkRequest(handoff, opts = {}) {
  const createdAt = opts.createdAt || nowIso();
  return {
    kind: SCOUT_WORK_REQUEST_KIND,
    workRequestId: opts.workRequestId || randomUUID(),
    handoffId: handoff.handoffId,
    source: 'max',
    target: 'scout',
    status: opts.status || SCOUT_HANDOFF_STATUSES.QUEUED,
    campaignObjective: handoff.campaignObjective,
    targetSegment: handoff.targetSegment,
    targetSubtype: handoff.targetSubtype,
    marketBounds: handoff.marketBounds,
    inclusionCriteria: [...(handoff.inclusionCriteria || [])],
    exclusionCriteria: [...(handoff.exclusionCriteria || [])],
    requiredFields: [...(handoff.requiredFields || [])],
    sourceTypes: [...(handoff.sourceTypes || [])],
    evidenceRequirements: [...(handoff.evidenceRequirements || [])],
    confidenceRules: [...(handoff.confidenceRules || [])],
    guardrails: [...(handoff.guardrails || [])],
    targetCountMin: opts.targetCountMin || 15,
    targetCountMax: opts.targetCountMax || 25,
    reviewOnly: true,
    crmWritesAllowed: false,
    outreachAllowed: false,
    accountChangesAllowed: false,
    createdAt,
    updatedAt: createdAt,
    queuedAt: createdAt,
    executionWired: Boolean(opts.executionWired),
    sourcingUnavailable: Boolean(opts.sourcingUnavailable),
    boundaryMessage: opts.boundaryMessage || null,
  };
}

/**
 * Whether Scout public-source sourcing execution is available for this handoff.
 * Default: not wired unless a sync scoutSourcingFn or explicit flag is injected.
 */
function isScoutSourcingExecutionWired(opts = {}) {
  if (opts.scoutSourcingSupported === false) return false;
  if (opts.scoutSourcingSupported === true) return true;
  if (typeof opts.scoutSourcingFn === 'function') return true;
  return false;
}

function normalizeScoutCandidate(row, idx) {
  const r = row || {};
  const sourceUrl = r.sourceUrl || r.website || r.url || null;
  return {
    id: r.id || `scout-candidate-${idx + 1}`,
    companyName:
      r.companyName || r.name || r.propertyManagerName || r.company || null,
    sourceUrl,
    website: r.website || sourceUrl || null,
    location: r.location || r.marketTown || r.address || null,
    marketTown: r.marketTown || r.location || null,
    segment: r.segment || null,
    subtype: r.subtype || r.segmentSubtype || null,
    fitRationale: r.fitRationale || r.fitReason || r.rationale || null,
    risks: r.risks || r.disqualifyRisk || r.risk || r.uncertainty || null,
    suggestedContactRole: r.suggestedContactRole || r.contactRole || null,
    confidence: r.confidence || 'review_required',
    reviewOnly: true,
    placeholder: false,
  };
}

/**
 * Candidates without a source URL are dropped — never fabricated.
 */
function filterValidScoutCandidates(rows) {
  if (!Array.isArray(rows)) return [];
  return rows
    .map((row, idx) => normalizeScoutCandidate(row, idx))
    .filter((row) => row.companyName && row.sourceUrl);
}

function buildCandidateBatch(handoff, candidates, opts = {}) {
  const createdAt = opts.createdAt || nowIso();
  return {
    kind: SCOUT_CANDIDATE_BATCH_KIND,
    handoffId: handoff.handoffId,
    workRequestId: handoff.workRequestId || (handoff.workRequest && handoff.workRequest.workRequestId) || null,
    status: 'review_only',
    reviewOnly: true,
    resultsApproved: false,
    candidateCount: candidates.length,
    candidates,
    guardrails: [
      'Review-only — no CRM writes',
      'No outreach copy',
      'No sends',
      'No account, DNS, GBP, social, or tracking changes',
      'Operator must approve before Composer / CRM / export use',
    ],
    crmWritesMade: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
    createdAt,
  };
}

/**
 * Approve the brief and create a Scout work request.
 * If sourcing is not wired, return a clear capability boundary — no placeholders.
 */
function handBriefToScout(priorHandoffOrBrief, opts = {}) {
  const baseFields =
    priorHandoffOrBrief && priorHandoffOrBrief.kind === SCOUT_HANDOFF_KIND
      ? priorHandoffOrBrief
      : priorHandoffOrBrief || {};

  const wired = isScoutSourcingExecutionWired(opts);
  const updatedAt = nowIso();

  let handoff = buildScoutHandoff(
    {
      ...baseFields,
      status: SCOUT_HANDOFF_STATUSES.APPROVED,
      executionWired: wired,
      sourcingUnavailable: !wired,
      scoutRan: false,
      updatedAt,
    },
    {
      handoffId: baseFields.handoffId,
      createdAt: baseFields.createdAt,
      updatedAt,
    }
  );

  const workRequest = buildScoutWorkRequest(handoff, {
    status: wired
      ? SCOUT_HANDOFF_STATUSES.QUEUED
      : SCOUT_HANDOFF_STATUSES.NEEDS_OPERATOR_REVIEW,
    executionWired: wired,
    sourcingUnavailable: !wired,
    boundaryMessage: wired ? null : SCOUT_SOURCING_NOT_WIRED_MESSAGE,
    createdAt: updatedAt,
  });

  handoff = {
    ...handoff,
    workRequestId: workRequest.workRequestId,
    workRequest,
    status: wired
      ? SCOUT_HANDOFF_STATUSES.QUEUED
      : SCOUT_HANDOFF_STATUSES.NEEDS_OPERATOR_REVIEW,
    updatedAt,
  };
  handoff.uiStatus = uiStatusForHandoff(handoff);

  if (!wired) {
    return {
      ok: true,
      handoff,
      workRequest,
      candidateBatch: null,
      scoutRan: false,
      sourcingUnavailable: true,
      executionWired: false,
      intent: 'scout_sourcing_not_wired',
      message: [
        'Scout Handoff Brief approved for Scout.',
        '',
        SCOUT_SOURCING_NOT_WIRED_MESSAGE,
        '',
        `Handoff status: ${handoff.uiStatus}`,
        `Work request ${workRequest.workRequestId} created for tracking — Scout did not inspect public sources.`,
        'No prospect candidates were generated.',
        'No outreach copy, sends, CRM writes, or account changes were made.',
        '',
        'Next build gap: wire Scout public-source sourcing execution to this handoff.',
      ].join('\n'),
    };
  }

  // Queued — optionally run sync scoutSourcingFn immediately (tests / injected).
  handoff = {
    ...handoff,
    status: SCOUT_HANDOFF_STATUSES.IN_PROGRESS,
    updatedAt: nowIso(),
  };
  handoff.uiStatus = uiStatusForHandoff(handoff);
  workRequest.status = SCOUT_HANDOFF_STATUSES.IN_PROGRESS;
  workRequest.updatedAt = handoff.updatedAt;

  let raw = [];
  try {
    raw = opts.scoutSourcingFn({
      handoff,
      workRequest,
      opts,
    });
  } catch (_err) {
    raw = [];
  }
  if (!Array.isArray(raw)) raw = [];

  const candidates = filterValidScoutCandidates(raw);

  if (!candidates.length) {
    handoff = {
      ...handoff,
      status: SCOUT_HANDOFF_STATUSES.FAILED,
      scoutRan: true,
      executionWired: true,
      sourcingUnavailable: false,
      updatedAt: nowIso(),
      workRequest: {
        ...workRequest,
        status: SCOUT_HANDOFF_STATUSES.FAILED,
        updatedAt: nowIso(),
      },
    };
    handoff.uiStatus = uiStatusForHandoff(handoff);
    return {
      ok: false,
      handoff,
      workRequest: handoff.workRequest,
      candidateBatch: null,
      scoutRan: true,
      sourcingUnavailable: false,
      executionWired: true,
      intent: 'scout_sourcing_failed',
      message: [
        'Scout ran against the approved handoff but returned no usable candidates with source URLs.',
        'No fabricated placeholder rows were generated.',
        'No outreach copy, sends, CRM writes, or account changes were made.',
        `Handoff status: ${handoff.uiStatus}`,
      ].join('\n'),
    };
  }

  const candidateBatch = buildCandidateBatch(handoff, candidates);
  handoff = {
    ...handoff,
    status: SCOUT_HANDOFF_STATUSES.COMPLETED,
    scoutRan: true,
    prospectListGenerated: false,
    candidateBatch,
    workRequest: {
      ...workRequest,
      status: SCOUT_HANDOFF_STATUSES.COMPLETED,
      updatedAt: nowIso(),
    },
    updatedAt: nowIso(),
  };
  handoff.uiStatus = uiStatusForHandoff(handoff);

  const lines = [
    'Scout Handoff executed. Results are review-only.',
    '',
    `Handoff status: ${handoff.uiStatus}`,
    `Work request: ${workRequest.workRequestId}`,
    `Candidates returned: ${candidates.length} (source URLs required)`,
    '',
  ];
  for (const row of candidates) {
    lines.push(
      `- ${row.companyName} | ${row.location || '—'} | ${row.sourceUrl} | ${
        row.fitRationale || '—'
      } | role: ${row.suggestedContactRole || '—'} | risks: ${
        row.risks || '—'
      } | confidence: ${row.confidence}`
    );
  }
  lines.push('');
  lines.push('Guardrails:');
  for (const g of candidateBatch.guardrails) lines.push(`- ${g}`);
  lines.push('');
  lines.push(
    'Operator must approve these Scout results before Composer, CRM, or export can use them.'
  );

  return {
    ok: true,
    handoff,
    workRequest: handoff.workRequest,
    candidateBatch,
    scoutRan: true,
    sourcingUnavailable: false,
    executionWired: true,
    intent: 'scout_handoff_completed',
    message: lines.join('\n'),
  };
}

/**
 * Mark Scout results as operator-approved for downstream use.
 * Does not write CRM or generate outreach.
 */
function approveScoutResults(handoff, opts = {}) {
  if (!handoff || !handoff.candidateBatch) {
    return {
      ok: false,
      handoff,
      message: 'No Scout candidate batch is available to approve.',
    };
  }
  const updatedAt = nowIso();
  const next = {
    ...handoff,
    resultsApproved: true,
    candidateBatch: {
      ...handoff.candidateBatch,
      resultsApproved: true,
      status: 'approved_for_downstream',
      approvedAt: updatedAt,
    },
    updatedAt,
    crmWritesMade: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
  };
  next.uiStatus = uiStatusForHandoff(next);
  return {
    ok: true,
    handoff: next,
    message:
      opts.message ||
      'Scout results approved for downstream review. Still no CRM writes, outreach, or account changes from this step.',
  };
}

module.exports = {
  SCOUT_HANDOFF_KIND,
  SCOUT_WORK_REQUEST_KIND,
  SCOUT_CANDIDATE_BATCH_KIND,
  SCOUT_HANDOFF_STATUSES,
  SCOUT_HANDOFF_UI_STATUS,
  SCOUT_SOURCING_NOT_WIRED_MESSAGE,
  DEFAULT_REQUIRED_FIELDS,
  DEFAULT_SOURCE_TYPES,
  DEFAULT_EVIDENCE_REQUIREMENTS,
  DEFAULT_CONFIDENCE_RULES,
  DEFAULT_GUARDRAILS,
  buildScoutHandoff,
  buildScoutWorkRequest,
  buildCandidateBatch,
  isScoutSourcingExecutionWired,
  filterValidScoutCandidates,
  normalizeScoutCandidate,
  handBriefToScout,
  approveScoutResults,
  uiStatusForHandoff,
};
