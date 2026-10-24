'use strict';

/**
 * SPEC-075 — Service Mode Operator Loop v1.
 * Read-only manual action queue for Jake. Translates Prospect Operating Briefs
 * over recent committed Relationship Intelligence into prioritized next steps.
 * isEvidence: false (synthesis). No outbound, CRM mutation, or autonomous execution.
 */

const defaultPool = require('../db');

const DEFAULT_DAYS = 14;
const DEFAULT_LIMIT = 10;
const SCAN_MULTIPLIER = 5;
const SCAN_FLOOR = 50;

const ACTION_TYPES = Object.freeze([
  'prepare_service_agreement',
  'send_follow_up',
  'schedule_kickoff',
  'prepare_proposal',
  'clarify_open_questions',
  'research_company',
  'link_crm_record',
  'manual_review',
  'wait_for_reply',
]);

const PRIORITY_RANK = Object.freeze({ high: 0, medium: 1, low: 2 });

const PLACEHOLDER_PATTERNS = [
  /^paste notes here\.?$/i,
  /\bpaste notes here\b/i,
  /^tbd\.?$/i,
  /^todo\.?$/i,
  /^n\/?a\.?$/i,
  /^test(ing)?\.?$/i,
  /^lorem ipsum\b/i,
];

const READINESS_FIXTURE_SOURCES = new Set([
  'readiness_acceptance',
  'readiness_fixture',
]);

const THIN_SUMMARY_MAX = 40;

class ServiceModeOperatorLoopError extends Error {
  constructor(code, message, status = 400) {
    super(message);
    this.name = 'ServiceModeOperatorLoopError';
    this.code = code;
    this.status = status;
  }
}

function asText(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s || null;
}

function clampDays(days, fallback = DEFAULT_DAYS) {
  const n = Number(days);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.min(Math.floor(n), 3650);
}

function clampLimit(limit, fallback = DEFAULT_LIMIT) {
  const n = Number(limit);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.min(Math.floor(n), 100);
}

function parseTruthy(value, fallback = true) {
  if (value === undefined || value === null || value === '') return fallback;
  if (value === true || value === 1) return true;
  if (value === false || value === 0) return false;
  const s = String(value).trim().toLowerCase();
  if (s === '1' || s === 'true' || s === 'yes') return true;
  if (s === '0' || s === 'false' || s === 'no') return false;
  return fallback;
}

function uniq(values) {
  const out = [];
  const seen = new Set();
  for (const raw of values || []) {
    if (raw == null) continue;
    const v = String(raw);
    if (seen.has(v)) continue;
    seen.add(v);
    out.push(v);
  }
  return out;
}

function normalizeSummary(raw) {
  return String(raw || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
}

function defaultRelationshipService() {
  return require('./relationshipIntelligenceInterview');
}

function defaultBriefService() {
  return require('./prospectOperatingBrief');
}

function isPlaceholderNotes(rawSummary) {
  const text = String(rawSummary || '').trim();
  if (!text) return true;
  return PLACEHOLDER_PATTERNS.some((re) => re.test(text));
}

function isReadinessFixture(interaction) {
  const source = String(interaction.source || '').trim().toLowerCase();
  if (READINESS_FIXTURE_SOURCES.has(source)) return true;
  const summary = String(interaction.rawSummary || '');
  return /readiness acceptance fixture/i.test(summary);
}

function isThinInteraction(interaction) {
  const summary = String(interaction.rawSummary || '').trim();
  if (!summary) return true;
  if (isPlaceholderNotes(summary)) return true;
  if (summary.length < THIN_SUMMARY_MAX) return true;
  const useful =
    /\b(interest|interested|ready|buying|pilot|proposal|service agreement|msa|kickoff|walkthrough|next step|open question|budget|timeline|pain|goal|follow[- ]?up|commit)\b/i.test(
      summary
    );
  return !useful && summary.length < 120;
}

function windowBounds(days) {
  const until = new Date();
  const since = new Date(until.getTime() - days * 24 * 60 * 60 * 1000);
  return {
    days,
    since: since.toISOString(),
    until: until.toISOString(),
    sinceDate: since,
    untilDate: until,
  };
}

function withinWindow(occurredAt, sinceDate) {
  if (!occurredAt) return true;
  const ts = new Date(occurredAt);
  if (Number.isNaN(ts.getTime())) return true;
  return ts.getTime() >= sinceDate.getTime();
}

function candidateSignalScore(interaction) {
  const blob = String(interaction.rawSummary || '');
  let score = 0;
  if (/\b(interest|interested|ready|buying|pilot|liked)\b/i.test(blob)) score += 4;
  if (/\b(service agreement|msa|contract|sow)\b/i.test(blob)) score += 5;
  if (/\bkickoff\b/i.test(blob)) score += 5;
  if (/\b(proposal|estimate|quote|pricing)\b/i.test(blob)) score += 3;
  if (/\b(next step|follow[- ]?up|schedule|send|prepare)\b/i.test(blob)) score += 3;
  if (/\b(open question|clarify|final questions|before moving forward)\b/i.test(blob)) {
    score += 3;
  }
  if (/\b(commitment|promised|we will|they will)\b/i.test(blob)) score += 2;
  if (!interaction.companyId) score += 1;
  return score;
}

function hasFocusTarget(options) {
  return Boolean(
    asText(options.relationshipInteractionId) ||
      asText(options.companyId) ||
      asText(options.prospectId) ||
      asText(options.opportunityId)
  );
}

/**
 * Load committed relationship interaction candidates for the operator loop.
 */
async function loadCandidates(options, deps) {
  const relationshipService = deps.relationshipService || defaultRelationshipService();
  const window = deps.window;
  const limit = deps.limit;
  const scanLimit = Math.max(limit * SCAN_MULTIPLIER, SCAN_FLOOR);

  const relationshipInteractionId = asText(options.relationshipInteractionId);
  if (relationshipInteractionId) {
    const payload = await relationshipService.getInteraction(relationshipInteractionId, {
      pool: options.pool,
      store: options.store,
    });
    if (!payload || payload.status !== 'committed') {
      throw new ServiceModeOperatorLoopError(
        'relationship_interaction_not_committed',
        'Relationship interaction must be committed',
        400
      );
    }
    const interaction = payload.interaction || {};
    return [
      {
        id: relationshipInteractionId,
        status: 'committed',
        interactionType: interaction.interactionType || interaction.type || null,
        companyId: interaction.companyId || null,
        contactId: interaction.contactId || null,
        opportunityId: interaction.opportunityId || null,
        occurredAt: interaction.occurredAt || null,
        rawSummary: interaction.rawSummary || null,
        source: interaction.source || null,
        insights: payload.insights || [],
      },
    ];
  }

  const companyId = asText(options.companyId);
  const prospectId = asText(options.prospectId);
  const opportunityId = asText(options.opportunityId);

  // listInteractions only filters companyId server-side; prospect/opportunity
  // soft-refs are filtered client-side after a committed scan.
  const listed = await relationshipService.listInteractions(
    {
      status: 'committed',
      companyId: companyId || undefined,
      clientId: options.clientId,
      limit: scanLimit,
    },
    { pool: options.pool, store: options.store }
  );

  let candidates = (listed || []).filter((row) => row.status === 'committed');

  if (prospectId) {
    candidates = candidates.filter((row) => asText(row.contactId) === prospectId);
  }
  if (opportunityId) {
    candidates = candidates.filter((row) => asText(row.opportunityId) === opportunityId);
  }

  candidates = candidates.filter(
    (row) => !row.occurredAt || withinWindow(row.occurredAt, window.sinceDate)
  );

  const caveats = [];
  const filtered = [];
  const seenSummaries = new Set();
  let placeholderSkipped = 0;
  let fixtureSkipped = 0;
  let thinSkipped = 0;
  let dedupeSkipped = 0;

  const ranked = candidates
    .slice()
    .sort((a, b) => candidateSignalScore(b) - candidateSignalScore(a));

  for (const row of ranked) {
    if (isReadinessFixture(row)) {
      fixtureSkipped += 1;
      continue;
    }
    if (isPlaceholderNotes(row.rawSummary)) {
      placeholderSkipped += 1;
      continue;
    }
    if (isThinInteraction(row)) {
      thinSkipped += 1;
      continue;
    }
    const key = normalizeSummary(row.rawSummary);
    if (key && seenSummaries.has(key)) {
      dedupeSkipped += 1;
      continue;
    }
    if (key) seenSummaries.add(key);
    filtered.push(row);
  }

  if (fixtureSkipped) {
    caveats.push(`skipped_readiness_fixtures:${fixtureSkipped}`);
  }
  if (placeholderSkipped) {
    caveats.push(`skipped_placeholder_notes:${placeholderSkipped}`);
  }
  if (thinSkipped) {
    caveats.push(`skipped_thin_interactions:${thinSkipped}`);
  }
  if (dedupeSkipped) {
    caveats.push(`deduped_raw_summaries:${dedupeSkipped}`);
  }

  return { candidates: filtered, scanCaveats: caveats, scannedCount: candidates.length };
}

function briefTextBlob(brief) {
  const sections = (brief && brief.sections) || {};
  const parts = [];
  for (const key of [
    'buyingSignals',
    'commitmentsAndNextSteps',
    'openQuestions',
    'objectionsAndRisks',
  ]) {
    for (const item of sections[key] || []) {
      parts.push(String(item.label || ''), String(item.value || ''), String(item.sourceQuote || ''));
    }
  }
  const action = sections.suggestedNextAction || {};
  parts.push(String(action.rationale || ''), String(action.suggestedMessageAngle || ''));
  for (const c of brief.caveats || []) parts.push(String(c));
  return parts.join(' ').toLowerCase();
}

function mapBriefActionType(brief) {
  const action = (brief.sections && brief.sections.suggestedNextAction) || {};
  const briefType = action.actionType || 'manual_review';
  const blob = briefTextBlob(brief);
  const hasServiceAgreement = /\bservice agreement|msa\b|\bcontract\b|\bsow\b/.test(blob);
  const hasKickoff = /\bkickoff\b/.test(blob);

  // Service agreement is the prerequisite before kickoff when both appear.
  if (
    hasServiceAgreement &&
    (briefType === 'prepare_proposal' ||
      briefType === 'schedule_kickoff' ||
      hasKickoff)
  ) {
    return 'prepare_service_agreement';
  }
  if (briefType === 'schedule_kickoff') return 'schedule_kickoff';
  if (briefType === 'wait_for_reply') return 'wait_for_reply';
  if (briefType === 'research_company') return 'research_company';
  if (briefType === 'ask_clarifying_question') return 'clarify_open_questions';
  if (briefType === 'send_follow_up') return 'send_follow_up';
  if (briefType === 'prepare_proposal') return 'prepare_proposal';
  if (briefType === 'schedule_walkthrough') return 'manual_review';
  if (briefType === 'manual_review') return 'manual_review';
  return ACTION_TYPES.includes(briefType) ? briefType : 'manual_review';
}

function resolvePriority(brief, actionType) {
  const action = (brief.sections && brief.sections.suggestedNextAction) || {};
  const sections = brief.sections || {};
  const buying = sections.buyingSignals || [];
  const nextSteps = sections.commitmentsAndNextSteps || [];
  const openQs = sections.openQuestions || [];
  const blob = briefTextBlob(brief);
  const sellerSide = /\b(send|schedule|prepare|draft|book|deliver|share|kickoff|service agreement|msa|contract|proposal|walkthrough|follow[- ]?up|estimate|quote)\b/i.test(
    nextSteps.map((i) => `${i.label || ''} ${i.value || ''}`).join(' ')
  );

  let priority = action.priority || 'medium';

  if (
    actionType === 'prepare_service_agreement' ||
    actionType === 'schedule_kickoff' ||
    /\b30[- ]?day pilot|pilot idea|final questions before moving forward\b/i.test(blob)
  ) {
    priority = 'high';
  } else if (buying.length && sellerSide) {
    priority = 'high';
  } else if (actionType === 'wait_for_reply') {
    priority = 'low';
  } else if (
    openQs.length &&
    buying.length === 0 &&
    (priority === 'high' ? false : true)
  ) {
    if (priority !== 'high') priority = priority === 'low' ? 'medium' : priority;
    if (!sellerSide && buying.length === 0) priority = 'medium';
  } else if (!sellerSide && buying.length === 0 && nextSteps.length === 0) {
    if (priority === 'high') {
      // keep brief high only when action type is inherently urgent
    } else {
      priority = actionType === 'research_company' || actionType === 'manual_review'
        ? 'medium'
        : 'low';
    }
  }

  if (actionType === 'link_crm_record' && priority === 'high') {
    priority = 'medium';
  }

  if (!['high', 'medium', 'low'].includes(priority)) priority = 'medium';
  return priority;
}

function actionTitle(actionType, target) {
  const company = target.companyName || target.companyId || 'Unknown company';
  const labels = {
    prepare_service_agreement: 'Prepare service agreement',
    send_follow_up: 'Send follow-up',
    schedule_kickoff: 'Schedule kickoff',
    prepare_proposal: 'Prepare proposal',
    clarify_open_questions: 'Clarify open questions',
    research_company: 'Research company',
    link_crm_record: 'Link CRM record',
    manual_review: 'Manual review',
    wait_for_reply: 'Wait for reply',
  };
  return `${company} — ${labels[actionType] || 'Manual review'}`;
}

function suggestedManualStep(actionType, brief) {
  const action = (brief.sections && brief.sections.suggestedNextAction) || {};
  const angle = asText(action.suggestedMessageAngle);
  const defaults = {
    prepare_service_agreement:
      'Prepare/send service agreement, then schedule kickoff after acceptance.',
    schedule_kickoff: 'Propose kickoff windows and confirm attendees after agreement acceptance.',
    prepare_proposal: 'Prepare the proposal/estimate using committed discovery notes only.',
    send_follow_up: 'Draft and send a manual follow-up referencing recorded buying signals.',
    clarify_open_questions: 'Reply with clarifying questions from the open-questions list.',
    research_company: 'Gather firmographics and confirm the CRM/company match before outreach.',
    link_crm_record: 'Match this relationship interaction to a CRM company/prospect record.',
    manual_review: 'Review the evidence and decide the next manual seller step.',
    wait_for_reply: 'Hold outreach; set a follow-up reminder if silence continues.',
  };
  if (angle && actionType !== 'prepare_service_agreement') {
    return angle;
  }
  if (actionType === 'prepare_service_agreement') {
    return defaults.prepare_service_agreement;
  }
  return angle || defaults[actionType] || defaults.manual_review;
}

function buildActionId(actionType, target, index) {
  const ri = target.relationshipInteractionId || 'none';
  return `smo-${index + 1}-${actionType}-${ri}`;
}

function translateBriefToActions(brief, index, options = {}) {
  const target = brief.target || {};
  const actionType = mapBriefActionType(brief);
  const priority = resolvePriority(brief, actionType);
  const action = (brief.sections && brief.sections.suggestedNextAction) || {};
  const caveats = uniq([...(brief.caveats || []), ...(action.cautions || [])]);
  const missingCrm = (brief.caveats || []).includes('target_not_matched_to_company_record');

  const primary = {
    actionId: buildActionId(actionType, target, index),
    actionType,
    priority,
    title: actionTitle(actionType, target),
    rationale: action.rationale || 'Review committed relationship evidence and act manually.',
    target: {
      companyId: target.companyId || null,
      prospectId: target.prospectId || null,
      opportunityId: target.opportunityId || null,
      contactId: target.contactId || null,
      relationshipInteractionId: target.relationshipInteractionId || null,
      companyName: target.companyName || null,
      contactName: target.contactName || null,
    },
    suggestedManualStep: suggestedManualStep(actionType, brief),
    requiredInputs: Array.isArray(action.requiredInputs) ? action.requiredInputs.slice() : [],
    sourceRefs: {
      relationshipInteractionIds: (
        (brief.sourceRefs && brief.sourceRefs.relationshipInteractionIds) ||
        []
      ).slice(),
      relationshipInsightIds: (
        (brief.sourceRefs && brief.sourceRefs.relationshipInsightIds) ||
        []
      ).slice(),
      marketObservationIds: (
        (brief.sourceRefs && brief.sourceRefs.marketObservationIds) ||
        []
      ).slice(),
      prospectBriefId: null,
    },
    caveats,
    autonomousExecution: false,
  };

  const actions = [primary];

  if (missingCrm && actionType !== 'link_crm_record' && options.includeLinkCrm !== false) {
    actions.push({
      actionId: buildActionId('link_crm_record', target, index + 1000),
      actionType: 'link_crm_record',
      priority: 'medium',
      title: actionTitle('link_crm_record', {
        ...target,
        companyName: target.companyName || 'Unmatched interaction',
      }),
      rationale:
        'Relationship evidence is useful but not matched to a CRM company/prospect record.',
      target: { ...primary.target },
      suggestedManualStep: suggestedManualStep('link_crm_record', brief),
      requiredInputs: ['CRM company or prospect id to soft-link'],
      sourceRefs: { ...primary.sourceRefs },
      caveats: uniq([...caveats, 'target_not_matched_to_company_record']),
      autonomousExecution: false,
    });
  }

  return actions;
}

/**
 * @param {object} [options]
 * @returns {Promise<object>}
 */
async function getServiceModeOperatorLoop(options = {}) {
  const days = clampDays(options.days, DEFAULT_DAYS);
  const limit = clampLimit(options.limit, DEFAULT_LIMIT);
  const includeMarketContext = parseTruthy(options.includeMarketContext, true);
  const window = windowBounds(days);
  const pool = options.pool || defaultPool;
  const briefService = options.briefService || defaultBriefService();
  const getBrief =
    options.getProspectOperatingBrief ||
    ((opts) => briefService.getProspectOperatingBrief(opts));
  const relationshipService = options.relationshipService || defaultRelationshipService();

  const topCaveats = [];
  let candidates = [];
  let candidatesScanned = 0;

  const focused = hasFocusTarget(options);

  try {
    const loaded = await loadCandidates(
      {
        ...options,
        pool,
        store: options.store,
      },
      { relationshipService, window, limit }
    );

    if (Array.isArray(loaded)) {
      candidates = loaded;
      candidatesScanned = loaded.length;
    } else {
      candidates = loaded.candidates || [];
      candidatesScanned = loaded.scannedCount || candidates.length;
      topCaveats.push(...(loaded.scanCaveats || []));
    }
  } catch (err) {
    if (err instanceof ServiceModeOperatorLoopError) throw err;
    // getInteraction may throw RelationshipIntelligenceError
    if (err && err.code === 'not_found') {
      throw new ServiceModeOperatorLoopError('not_found', err.message, 404);
    }
    if (err && err.name === 'RelationshipIntelligenceError') {
      throw new ServiceModeOperatorLoopError(
        err.code || 'relationship_intelligence_error',
        err.message,
        err.status || 400
      );
    }
    throw err;
  }

  // Focused company/prospect/opportunity with no matching RI: still try one brief.
  if (focused && candidates.length === 0 && !asText(options.relationshipInteractionId)) {
    const brief = await getBrief({
      pool,
      store: options.store,
      companyId: options.companyId || undefined,
      prospectId: options.prospectId || undefined,
      opportunityId: options.opportunityId || undefined,
      days,
      includeMarketContext,
      includeRelationshipContext: true,
      clientId: options.clientId,
      loadCompanySnapshot: options.loadCompanySnapshot,
      marketBriefingService: options.marketBriefingService,
      relationshipService,
    });
    const actions = translateBriefToActions(brief, 0).slice(0, limit);
    return {
      ok: true,
      kind: 'service_mode_operator_loop',
      isEvidence: false,
      generatedAt: new Date().toISOString(),
      window: { days: window.days, since: window.since, until: window.until },
      summary: {
        candidatesScanned: 0,
        actionsReturned: actions.length,
        highPriorityCount: actions.filter((a) => a.priority === 'high').length,
        caveatCount: uniq([
          ...topCaveats,
          ...(brief.caveats || []),
          ...actions.flatMap((a) => a.caveats || []),
        ]).length,
      },
      actions,
      caveats: uniq([
        ...topCaveats,
        ...(brief.caveats || []),
        'no_committed_relationship_candidates_in_window',
      ]),
      autonomousExecution: false,
      internal: true,
    };
  }

  if (candidates.length === 0) {
    return {
      ok: true,
      kind: 'service_mode_operator_loop',
      isEvidence: false,
      generatedAt: new Date().toISOString(),
      window: { days: window.days, since: window.since, until: window.until },
      summary: {
        candidatesScanned: candidatesScanned,
        actionsReturned: 0,
        highPriorityCount: 0,
        caveatCount: uniq([
          ...topCaveats,
          'no_operator_candidates',
        ]).length,
      },
      actions: [],
      caveats: uniq([
        ...topCaveats,
        'no_operator_candidates',
        focused
          ? 'no_committed_relationship_candidates_for_target'
          : 'no_committed_relationship_candidates_in_window',
      ]),
      autonomousExecution: false,
      internal: true,
    };
  }

  const actions = [];
  const loopCaveats = [...topCaveats];
  let briefIndex = 0;

  for (const candidate of candidates) {
    if (actions.filter((a) => a.actionType !== 'link_crm_record').length >= limit) {
      break;
    }

    const brief = await getBrief({
      pool,
      store: options.store,
      relationshipInteractionId: candidate.id,
      companyId: options.companyId || candidate.companyId || undefined,
      prospectId: options.prospectId || undefined,
      opportunityId: options.opportunityId || candidate.opportunityId || undefined,
      contactId: candidate.contactId || undefined,
      days,
      includeMarketContext,
      includeRelationshipContext: true,
      clientId: options.clientId,
      loadCompanySnapshot: options.loadCompanySnapshot,
      marketBriefingService: options.marketBriefingService,
      relationshipService,
    });

    const translated = translateBriefToActions(brief, briefIndex);
    briefIndex += 1;

    for (const item of translated) {
      if (
        item.actionType === 'link_crm_record' ||
        actions.filter((a) => a.actionType !== 'link_crm_record').length < limit
      ) {
        // Cap primary actions at limit; allow one link_crm companion.
        if (item.actionType !== 'link_crm_record') {
          const primaryCount = actions.filter((a) => a.actionType !== 'link_crm_record').length;
          if (primaryCount >= limit) continue;
        }
        actions.push(item);
      }
    }
    loopCaveats.push(...(brief.caveats || []));
  }

  actions.sort((a, b) => {
    const pr = (PRIORITY_RANK[a.priority] ?? 9) - (PRIORITY_RANK[b.priority] ?? 9);
    if (pr !== 0) return pr;
    // Prefer commercial actions over CRM-link companions at same priority.
    if (a.actionType === 'link_crm_record' && b.actionType !== 'link_crm_record') return 1;
    if (b.actionType === 'link_crm_record' && a.actionType !== 'link_crm_record') return -1;
    return 0;
  });

  const primaryActions = [];
  const linkActions = [];
  for (const action of actions) {
    if (action.actionType === 'link_crm_record') linkActions.push(action);
    else primaryActions.push(action);
  }
  const capped = [
    ...primaryActions.slice(0, limit),
    ...linkActions.filter((link) =>
      primaryActions
        .slice(0, limit)
        .some(
          (p) =>
            p.target.relationshipInteractionId &&
            p.target.relationshipInteractionId === link.target.relationshipInteractionId
        )
    ),
  ];

  const allCaveats = uniq([
    ...loopCaveats,
    ...capped.flatMap((a) => a.caveats || []),
  ]);

  return {
    ok: true,
    kind: 'service_mode_operator_loop',
    isEvidence: false,
    generatedAt: new Date().toISOString(),
    window: { days: window.days, since: window.since, until: window.until },
    summary: {
      candidatesScanned,
      actionsReturned: capped.length,
      highPriorityCount: capped.filter((a) => a.priority === 'high').length,
      caveatCount: allCaveats.length,
    },
    actions: capped,
    caveats: allCaveats,
    autonomousExecution: false,
    internal: true,
  };
}

function formatOperatorLoopReport(loop) {
  const lines = [
    'Service Mode Operator Loop',
    `Generated: ${loop.generatedAt || ''}`,
    `Window: last ${loop.window && loop.window.days != null ? loop.window.days : DEFAULT_DAYS} days`,
    `isEvidence: ${loop.isEvidence === false ? 'false' : String(loop.isEvidence)}`,
    '',
  ];

  const summary = loop.summary || {};
  lines.push(
    `Summary: ${summary.actionsReturned || 0} action(s) from ${summary.candidatesScanned || 0} candidate(s); ${summary.highPriorityCount || 0} high priority`
  );
  lines.push('');

  const byPriority = { high: [], medium: [], low: [] };
  for (const action of loop.actions || []) {
    const bucket = byPriority[action.priority] ? action.priority : 'medium';
    byPriority[bucket].push(action);
  }

  for (const level of ['high', 'medium', 'low']) {
    const items = byPriority[level];
    const label =
      level === 'high' ? 'High Priority' : level === 'medium' ? 'Medium Priority' : 'Low Priority';
    lines.push(`${label}:`);
    if (!items.length) {
      lines.push('(none)');
      lines.push('');
      continue;
    }
    items.forEach((action, i) => {
      lines.push(`${i + 1}. ${action.title}`);
      lines.push(`   Why: ${action.rationale}`);
      lines.push(`   Manual step: ${action.suggestedManualStep}`);
      if (action.caveats && action.caveats.length) {
        lines.push(`   Caveats: ${action.caveats.join('; ')}`);
      }
      if (action.target && action.target.relationshipInteractionId) {
        lines.push(
          `   Source: relationship interaction ${action.target.relationshipInteractionId}`
        );
      }
    });
    lines.push('');
  }

  if (loop.caveats && loop.caveats.length) {
    lines.push('Loop Caveats:');
    for (const caveat of loop.caveats) {
      lines.push(`- ${caveat}`);
    }
    lines.push('');
  }

  lines.push('No autonomous execution performed.');
  return lines.join('\n');
}

module.exports = {
  ACTION_TYPES,
  DEFAULT_DAYS,
  DEFAULT_LIMIT,
  ServiceModeOperatorLoopError,
  clampDays,
  clampLimit,
  formatOperatorLoopReport,
  getServiceModeOperatorLoop,
  isPlaceholderNotes,
  isReadinessFixture,
  isThinInteraction,
  mapBriefActionType,
  normalizeSummary,
};
