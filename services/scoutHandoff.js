'use strict';

/**
 * Max → Scout Prospect Sourcing Handoff.
 *
 * Turns a Scout Handoff Brief into an executable, review-first work request.
 * Guardrails: no outreach copy, sends, CRM writes, or account changes.
 * Results (when Scout sourcing is wired) are review-only until operator approval.
 *
 * SPEC-077: approved handoffs run public-source sourcing (Google Places) and
 * store evidenced candidates for operator review — never CRM / outreach.
 */

const { randomUUID } = require('crypto');
const {
  isScoutPublicSourcingAvailable,
  sourceScoutCandidatesFromPublicSources,
} = require('./scoutPublicSourcing');
const {
  defaultScoutWorkRequestStore,
} = require('./scoutWorkRequestStore');

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
 * Wired when:
 * - sync/async scoutSourcingFn is injected, or
 * - scoutSourcingSupported is explicitly true, or
 * - public-source tooling is available (GOOGLE_PLACES_KEY / injected search).
 */
function isScoutSourcingExecutionWired(opts = {}) {
  if (opts.scoutSourcingSupported === false) return false;
  if (opts.scoutSourcingSupported === true) return true;
  if (typeof opts.scoutSourcingFn === 'function') return true;
  if (isScoutPublicSourcingAvailable(opts)) return true;
  return false;
}

function resolveWorkRequestStore(opts = {}) {
  return opts.workRequestStore || defaultScoutWorkRequestStore;
}

function persistWorkRequestRecord(handoff, workRequest, opts = {}, extra = {}) {
  const store = resolveWorkRequestStore(opts);
  const record = {
    workRequestId: workRequest.workRequestId,
    handoffId: handoff.handoffId,
    status: workRequest.status,
    handoff,
    workRequest,
    candidateBatch: handoff.candidateBatch || null,
    reviewOnly: true,
    crmWritesMade: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
    createdAt: workRequest.createdAt || nowIso(),
    updatedAt: nowIso(),
    ...extra,
  };
  store.save(record);
  return record;
}

function finishWithCandidates(handoff, workRequest, candidates, opts = {}) {
  const candidateBatch = buildCandidateBatch(handoff, candidates);
  const updatedAt = nowIso();
  const nextWorkRequest = {
    ...workRequest,
    status: SCOUT_HANDOFF_STATUSES.COMPLETED,
    updatedAt,
    completedAt: updatedAt,
  };
  const nextHandoff = {
    ...handoff,
    status: SCOUT_HANDOFF_STATUSES.COMPLETED,
    scoutRan: true,
    prospectListGenerated: false,
    liveSourcingPerformed: false,
    candidateBatch,
    workRequest: nextWorkRequest,
    workRequestId: nextWorkRequest.workRequestId,
    executionWired: true,
    sourcingUnavailable: false,
    updatedAt,
    crmWritesMade: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
  };
  nextHandoff.uiStatus = uiStatusForHandoff(nextHandoff);
  persistWorkRequestRecord(nextHandoff, nextWorkRequest, opts, {
    status: SCOUT_HANDOFF_STATUSES.COMPLETED,
    candidateBatch,
  });

  const lines = [
    'Scout Handoff executed. Results are review-only.',
    '',
    `Handoff status: ${nextHandoff.uiStatus}`,
    `Work request: ${nextWorkRequest.workRequestId}`,
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
    handoff: nextHandoff,
    workRequest: nextWorkRequest,
    candidateBatch,
    scoutRan: true,
    sourcingUnavailable: false,
    executionWired: true,
    intent: 'scout_handoff_completed',
    message: lines.join('\n'),
  };
}

function finishWithFailure(handoff, workRequest, opts = {}, detail = {}) {
  const updatedAt = nowIso();
  const nextWorkRequest = {
    ...workRequest,
    status: SCOUT_HANDOFF_STATUSES.FAILED,
    updatedAt,
    failedAt: updatedAt,
    failureReason: detail.error || detail.message || 'scout_sourcing_failed',
  };
  const nextHandoff = {
    ...handoff,
    status: SCOUT_HANDOFF_STATUSES.FAILED,
    scoutRan: true,
    executionWired: true,
    sourcingUnavailable: false,
    candidateBatch: null,
    workRequest: nextWorkRequest,
    workRequestId: nextWorkRequest.workRequestId,
    updatedAt,
    crmWritesMade: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
  };
  nextHandoff.uiStatus = uiStatusForHandoff(nextHandoff);
  persistWorkRequestRecord(nextHandoff, nextWorkRequest, opts, {
    status: SCOUT_HANDOFF_STATUSES.FAILED,
    candidateBatch: null,
    failureReason: nextWorkRequest.failureReason,
  });

  return {
    ok: false,
    handoff: nextHandoff,
    workRequest: nextWorkRequest,
    candidateBatch: null,
    scoutRan: true,
    sourcingUnavailable: false,
    executionWired: true,
    intent: 'scout_sourcing_failed',
    message: [
      detail.message ||
        'Scout ran against the approved handoff but returned no usable candidates with source URLs.',
      'No fabricated placeholder rows were generated.',
      'No outreach copy, sends, CRM writes, or account changes were made.',
      `Handoff status: ${nextHandoff.uiStatus}`,
      `Work request ${nextWorkRequest.workRequestId} preserved for retry / operator review.`,
      detail.error ? `Failure: ${detail.error}` : null,
    ]
      .filter(Boolean)
      .join('\n'),
  };
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
 *
 * When public-source tooling is available (and no sync scoutSourcingFn), the work
 * request is queued and marked for async execution via executeScoutWorkRequest /
 * handBriefToScoutAsync. Sync scoutSourcingFn still runs inline (tests).
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

  persistWorkRequestRecord(handoff, workRequest, opts, {
    status: handoff.status,
  });

  if (!wired) {
    return {
      ok: true,
      handoff,
      workRequest,
      candidateBatch: null,
      scoutRan: false,
      sourcingUnavailable: true,
      executionWired: false,
      shouldExecuteScoutSourcing: false,
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

  // Sync injected scoutSourcingFn — run immediately (unit tests / fixtures).
  if (typeof opts.scoutSourcingFn === 'function') {
    handoff = {
      ...handoff,
      status: SCOUT_HANDOFF_STATUSES.IN_PROGRESS,
      updatedAt: nowIso(),
    };
    handoff.uiStatus = uiStatusForHandoff(handoff);
    workRequest.status = SCOUT_HANDOFF_STATUSES.IN_PROGRESS;
    workRequest.updatedAt = handoff.updatedAt;
    persistWorkRequestRecord(handoff, workRequest, opts, {
      status: SCOUT_HANDOFF_STATUSES.IN_PROGRESS,
    });

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
    if (raw && typeof raw.then === 'function') {
      // Async fn passed to sync helper — queue for executeScoutWorkRequest.
      return {
        ok: true,
        handoff,
        workRequest,
        candidateBatch: null,
        scoutRan: false,
        sourcingUnavailable: false,
        executionWired: true,
        shouldExecuteScoutSourcing: true,
        intent: 'scout_handoff_queued',
        message: [
          'Scout work request queued for public-source sourcing.',
          `Work request: ${workRequest.workRequestId}`,
          `Handoff status: ${handoff.uiStatus}`,
          'No CRM writes, outreach copy, or account changes were made.',
        ].join('\n'),
      };
    }
    if (!Array.isArray(raw)) raw = [];

    const candidates = filterValidScoutCandidates(raw);

    if (!candidates.length) {
      return finishWithFailure(handoff, workRequest, opts, {
        message:
          'Scout ran against the approved handoff but returned no usable candidates with source URLs.',
      });
    }

    return finishWithCandidates(handoff, workRequest, candidates, opts);
  }

  // Public-source tooling available — queue for async executeScoutWorkRequest.
  return {
    ok: true,
    handoff,
    workRequest,
    candidateBatch: null,
    scoutRan: false,
    sourcingUnavailable: false,
    executionWired: true,
    shouldExecuteScoutSourcing: true,
    intent: 'scout_handoff_queued',
    message: [
      'Scout Handoff Brief approved. Scout work request queued for public-source sourcing.',
      '',
      `Handoff status: ${handoff.uiStatus}`,
      `Work request: ${workRequest.workRequestId}`,
      'Scout will inspect public sources and return evidenced candidates for operator review.',
      'No CRM writes, outreach copy, sends, or account changes will be made.',
    ].join('\n'),
  };
}

/**
 * Async handoff: approve, queue, and run public-source sourcing when wired.
 */
async function handBriefToScoutAsync(priorHandoffOrBrief, opts = {}) {
  const queued = handBriefToScout(priorHandoffOrBrief, opts);
  if (!queued.shouldExecuteScoutSourcing) {
    return queued;
  }
  return executeScoutWorkRequest({
    workRequestId: queued.workRequest && queued.workRequest.workRequestId,
    handoffId: queued.handoff && queued.handoff.handoffId,
    handoff: queued.handoff,
    workRequest: queued.workRequest,
    ...opts,
  });
}

/**
 * Load a preserved Scout work request by ID and queue/run sourcing.
 * Does NOT create a new handoff or work request. Suitable for execute/retry.
 *
 * Sync when `scoutSourcingFn` is sync (tests); otherwise sets
 * `shouldExecuteScoutSourcing` for the interview layer to await
 * `executeScoutWorkRequest`.
 *
 * @param {object} input
 * @param {string} input.workRequestId
 */
function queueOrExecuteExistingScoutWorkRequest(input = {}) {
  const opts = input.opts || input;
  const workRequestId = input.workRequestId
    ? String(input.workRequestId).trim()
    : '';
  const store = resolveWorkRequestStore(opts);

  if (!workRequestId) {
    return {
      ok: false,
      handoff: null,
      workRequest: null,
      candidateBatch: null,
      scoutRan: false,
      sourcingUnavailable: false,
      executionWired: isScoutSourcingExecutionWired(opts),
      shouldExecuteScoutSourcing: false,
      createdNewHandoff: false,
      intent: 'scout_sourcing_failed',
      message:
        'Scout work request is required to execute public-source sourcing. Provide workRequestId.',
    };
  }

  const record = store.get({ workRequestId }) || null;
  let handoff = (record && record.handoff) || input.handoff || null;
  let workRequest =
    (record && record.workRequest) ||
    input.workRequest ||
    (handoff && handoff.workRequest) ||
    null;

  if (!workRequest) {
    return {
      ok: false,
      handoff,
      workRequest: null,
      candidateBatch: null,
      scoutRan: false,
      sourcingUnavailable: false,
      executionWired: isScoutSourcingExecutionWired(opts),
      shouldExecuteScoutSourcing: false,
      createdNewHandoff: false,
      intent: 'scout_sourcing_failed',
      message: [
        `No Scout work request found for workRequestId=${workRequestId}.`,
        'Work request was not modified.',
      ].join('\n'),
    };
  }

  // Reconstruct a handoff shell from the preserved work request when needed —
  // never mint a new workRequestId / handoffId.
  if (!handoff) {
    handoff = buildScoutHandoff({
      handoffId: workRequest.handoffId,
      campaignObjective: workRequest.campaignObjective,
      targetSegment: workRequest.targetSegment,
      targetSubtype: workRequest.targetSubtype,
      marketBounds: workRequest.marketBounds,
      inclusionCriteria: workRequest.inclusionCriteria,
      exclusionCriteria: workRequest.exclusionCriteria,
      requiredFields: workRequest.requiredFields,
      sourceTypes: workRequest.sourceTypes,
      evidenceRequirements: workRequest.evidenceRequirements,
      confidenceRules: workRequest.confidenceRules,
      guardrails: workRequest.guardrails,
      workRequestId: workRequest.workRequestId,
      workRequest,
      status: workRequest.status || SCOUT_HANDOFF_STATUSES.QUEUED,
      executionWired: true,
    });
  }

  const wired = isScoutSourcingExecutionWired(opts);
  if (!wired) {
    return {
      ok: true,
      handoff,
      workRequest,
      candidateBatch: null,
      scoutRan: false,
      sourcingUnavailable: true,
      executionWired: false,
      shouldExecuteScoutSourcing: false,
      createdNewHandoff: false,
      intent: 'scout_sourcing_not_wired',
      message: [
        `Existing Scout work request ${workRequest.workRequestId} loaded — not re-approved, not replaced.`,
        '',
        SCOUT_SOURCING_NOT_WIRED_MESSAGE,
        '',
        'No new handoff was created.',
        'No prospect candidates were generated.',
        'No outreach copy, sends, CRM writes, or account changes were made.',
      ].join('\n'),
    };
  }

  // Sync injected scoutSourcingFn — run immediately (unit tests / fixtures).
  if (typeof opts.scoutSourcingFn === 'function') {
    const updatedAt = nowIso();
    handoff = {
      ...handoff,
      status: SCOUT_HANDOFF_STATUSES.IN_PROGRESS,
      workRequestId: workRequest.workRequestId,
      workRequest: {
        ...workRequest,
        status: SCOUT_HANDOFF_STATUSES.IN_PROGRESS,
        updatedAt,
        startedAt: workRequest.startedAt || updatedAt,
      },
      updatedAt,
      executionWired: true,
      sourcingUnavailable: false,
    };
    handoff.uiStatus = uiStatusForHandoff(handoff);
    workRequest = handoff.workRequest;
    persistWorkRequestRecord(handoff, workRequest, opts, {
      status: SCOUT_HANDOFF_STATUSES.IN_PROGRESS,
    });

    let raw = [];
    try {
      raw = opts.scoutSourcingFn({
        handoff,
        workRequest,
        opts,
      });
    } catch (err) {
      return {
        ...finishWithFailure(handoff, workRequest, opts, {
          error:
            err && err.message ? String(err.message) : 'scout_sourcing_fn_threw',
          message:
            'Scout sourcing function failed. Work request preserved as failed — no placeholders generated.',
        }),
        createdNewHandoff: false,
        shouldExecuteScoutSourcing: false,
      };
    }
    if (raw && typeof raw.then === 'function') {
      return {
        ok: true,
        handoff,
        workRequest,
        candidateBatch: null,
        scoutRan: false,
        sourcingUnavailable: false,
        executionWired: true,
        shouldExecuteScoutSourcing: true,
        createdNewHandoff: false,
        intent: 'scout_handoff_queued',
        message: [
          `Executing existing Scout work request ${workRequest.workRequestId}.`,
          `Handoff status: ${handoff.uiStatus}`,
          'No new handoff was created. No CRM writes, outreach copy, or account changes were made.',
        ].join('\n'),
      };
    }
    if (!Array.isArray(raw)) raw = [];
    const candidates = filterValidScoutCandidates(raw);
    if (!candidates.length) {
      return {
        ...finishWithFailure(handoff, workRequest, opts, {
          message:
            'Scout ran against the preserved work request but returned no usable candidates with source URLs.',
        }),
        createdNewHandoff: false,
        shouldExecuteScoutSourcing: false,
      };
    }
    return {
      ...finishWithCandidates(handoff, workRequest, candidates, opts),
      createdNewHandoff: false,
      shouldExecuteScoutSourcing: false,
    };
  }

  // Public-source tooling available — queue for async executeScoutWorkRequest.
  return {
    ok: true,
    handoff,
    workRequest,
    candidateBatch: null,
    scoutRan: false,
    sourcingUnavailable: false,
    executionWired: true,
    shouldExecuteScoutSourcing: true,
    createdNewHandoff: false,
    intent: 'scout_handoff_queued',
    message: [
      `Executing existing Scout work request ${workRequest.workRequestId}.`,
      '',
      `Handoff status: ${handoff.uiStatus || uiStatusForHandoff(handoff)}`,
      'Scout will inspect public sources and return evidenced candidates for operator review.',
      'No new handoff was created. No CRM writes, outreach copy, sends, or account changes will be made.',
    ].join('\n'),
  };
}

/**
 * Execute (or resume) Scout public-source sourcing for a work request.
 * Lookup by workRequestId or handoffId. Preserves the work request on failure.
 *
 * @param {object} input
 * @param {string} [input.workRequestId]
 * @param {string} [input.handoffId]
 * @param {object} [input.handoff]
 * @param {object} [input.workRequest]
 */
async function executeScoutWorkRequest(input = {}) {
  const opts = input.opts || input;
  const store = resolveWorkRequestStore(opts);

  let record =
    store.get({
      workRequestId: input.workRequestId,
      handoffId: input.handoffId,
    }) || null;

  let handoff =
    (record && record.handoff) ||
    input.handoff ||
    null;
  let workRequest =
    (record && record.workRequest) ||
    input.workRequest ||
    (handoff && handoff.workRequest) ||
    null;

  if (!workRequest && input.workRequestId) {
    return {
      ok: false,
      handoff,
      workRequest: null,
      candidateBatch: null,
      scoutRan: false,
      sourcingUnavailable: false,
      executionWired: isScoutSourcingExecutionWired(opts),
      intent: 'scout_sourcing_failed',
      message: [
        `No Scout work request found for workRequestId=${input.workRequestId}.`,
        'Work request was not modified.',
      ].join('\n'),
    };
  }
  if (!workRequest && input.handoffId) {
    return {
      ok: false,
      handoff,
      workRequest: null,
      candidateBatch: null,
      scoutRan: false,
      sourcingUnavailable: false,
      executionWired: isScoutSourcingExecutionWired(opts),
      intent: 'scout_sourcing_failed',
      message: [
        `No Scout work request found for handoffId=${input.handoffId}.`,
        'Work request was not modified.',
      ].join('\n'),
    };
  }
  if (!workRequest) {
    return {
      ok: false,
      handoff,
      workRequest: null,
      candidateBatch: null,
      scoutRan: false,
      intent: 'scout_sourcing_failed',
      message: 'Scout work request is required to execute public-source sourcing.',
    };
  }

  if (!handoff) {
    handoff = buildScoutHandoff({
      handoffId: workRequest.handoffId,
      campaignObjective: workRequest.campaignObjective,
      targetSegment: workRequest.targetSegment,
      targetSubtype: workRequest.targetSubtype,
      marketBounds: workRequest.marketBounds,
      inclusionCriteria: workRequest.inclusionCriteria,
      exclusionCriteria: workRequest.exclusionCriteria,
      requiredFields: workRequest.requiredFields,
      sourceTypes: workRequest.sourceTypes,
      evidenceRequirements: workRequest.evidenceRequirements,
      confidenceRules: workRequest.confidenceRules,
      guardrails: workRequest.guardrails,
      workRequestId: workRequest.workRequestId,
      workRequest,
      status: SCOUT_HANDOFF_STATUSES.QUEUED,
      executionWired: true,
    });
  }

  const updatedAt = nowIso();
  handoff = {
    ...handoff,
    status: SCOUT_HANDOFF_STATUSES.IN_PROGRESS,
    workRequestId: workRequest.workRequestId,
    workRequest: {
      ...workRequest,
      status: SCOUT_HANDOFF_STATUSES.IN_PROGRESS,
      updatedAt,
      startedAt: updatedAt,
    },
    updatedAt,
    executionWired: true,
    sourcingUnavailable: false,
  };
  handoff.uiStatus = uiStatusForHandoff(handoff);
  workRequest = handoff.workRequest;
  persistWorkRequestRecord(handoff, workRequest, opts, {
    status: SCOUT_HANDOFF_STATUSES.IN_PROGRESS,
  });

  // Prefer injected sync/async scoutSourcingFn when provided at execute time.
  if (typeof opts.scoutSourcingFn === 'function') {
    let raw = [];
    try {
      raw = await Promise.resolve(
        opts.scoutSourcingFn({ handoff, workRequest, opts })
      );
    } catch (err) {
      return finishWithFailure(handoff, workRequest, opts, {
        error: err && err.message ? String(err.message) : 'scout_sourcing_fn_threw',
        message:
          'Scout sourcing function failed. Work request preserved as failed — no placeholders generated.',
      });
    }
    if (!Array.isArray(raw)) raw = [];
    const candidates = filterValidScoutCandidates(raw);
    if (!candidates.length) {
      return finishWithFailure(handoff, workRequest, opts, {
        message:
          'Scout ran against the approved handoff but returned no usable candidates with source URLs.',
      });
    }
    return finishWithCandidates(handoff, workRequest, candidates, opts);
  }

  const sourced = await sourceScoutCandidatesFromPublicSources({
    workRequest,
    handoff,
    opts,
  });

  if (!sourced.ok || !sourced.candidates.length) {
    return finishWithFailure(handoff, workRequest, opts, {
      error: sourced.error || 'no_usable_candidates',
      message: [
        'Scout public-source sourcing failed or returned no usable candidates with source URLs.',
        ...(sourced.warnings || []),
        'Work request preserved — no fabricated placeholders, CRM writes, or outreach.',
      ].join('\n'),
    });
  }

  const candidates = filterValidScoutCandidates(sourced.candidates);
  if (!candidates.length) {
    return finishWithFailure(handoff, workRequest, opts, {
      error: 'candidates_failed_validation',
      message:
        'Scout returned rows but none had both a company name and source URL. Work request preserved.',
    });
  }

  const result = finishWithCandidates(handoff, workRequest, candidates, opts);
  if (sourced.warnings && sourced.warnings.length) {
    result.message = `${result.message}\n\nSourcing notes:\n- ${sourced.warnings.join(
      '\n- '
    )}`;
    result.warnings = sourced.warnings;
  }
  result.queried = sourced.queried || [];
  return result;
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
  handBriefToScoutAsync,
  queueOrExecuteExistingScoutWorkRequest,
  executeScoutWorkRequest,
  approveScoutResults,
  uiStatusForHandoff,
  persistWorkRequestRecord,
  resolveWorkRequestStore,
};
