'use strict';

/**
 * P0 #1089 — Operational evidence continuity and analytical follow-up ownership.
 *
 * Problem: after a read-only operational inspection (#1087), the operator's
 * next turn ("Using the operational evidence you just retrieved … explain why
 * outbound is intentionally paused … do not approve or resume missions …")
 * lost the inspection's ownership. It fell through objective resolution
 * (negated "approve/resume" + bag-of-words "next" → mission_execution), the
 * hydrated AMO mission's pending execution_approval claimed the turn, and Max
 * answered "This approval is the next operator gate … Authorize external
 * execution of prepared outreach?" instead of the requested analysis.
 *
 * Contract (the smallest routing contract that separates analysis from
 * mission continuation):
 *
 *  1. Evidence record — every successful inspection stores a compact,
 *     tenant-bound evidence record on the session object (survives envelope
 *     context switches; not echoed in responses) (aggregates, mission
 *     classifications, blockers, gate, source provenance; no prospect rows).
 *
 *  2. Ownership — a turn is an operational ANALYSIS turn when, after negated
 *     clauses are removed, it carries no action command or decision response,
 *     it does not single out one mission, and either
 *       (a) it explicitly refers to previously retrieved operational evidence
 *           and asks for analysis of it, or
 *       (b) the session already holds an evidence record for this tenant and
 *           the turn asks an analytical question about the operational
 *           portfolio (missions, outbound, gate, blockers, failures).
 *     (b) is stateful: without a recorded inspection the same words keep their
 *     normal routing, so this is not a global keyword bypass.
 *
 *  3. Evidence resolution — a current record (same tenant, ≤ EVIDENCE_TTL) is
 *     reused and cited with its as-of time. A missing, stale, or other-tenant
 *     record is refreshed through the same read-only inspection loader, and the
 *     answer says so. A named client that is not the session tenant is refused.
 *
 *  4. Authority — the analysis owner never calls a mission runtime, approval,
 *     execution router, session-state resolver, or outbound control. Pending
 *     decisions are reported as context ("left untouched"), never requested or
 *     consumed. Session writes: the transcript and the evidence record only.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const { stripNegatedDirectives, extractNegatedClauses } = require('./NegatedDirective');
const askPathTrace = require('./audit/AskPathTrace');
const {
  CATEGORIES,
  detectOperationalInspection,
  hasAffirmativeActionCommand,
  loadOperationalSnapshot,
  checkNamedClientScope,
  tenantBlockers,
  countByCategory,
  refusalTurn,
} = require('./OperationalInspection');

const SPEC = 'P0-1089-OPERATIONAL-EVIDENCE-CONTINUITY';

/** Reused evidence older than this is refreshed (read-only) before analysis. */
const EVIDENCE_TTL_MS = 30 * 60 * 1000;
/** Listing cap per mission group; the rest are counted, not dropped silently. */
const LIST_CAP = 8;

const EVIDENCE_STATES = Object.freeze({
  CURRENT: 'current',
  STALE: 'stale',
  MISSING: 'missing',
  OTHER_TENANT: 'other_tenant',
});

// ---------------------------------------------------------------------------
// 1. Evidence record
// ---------------------------------------------------------------------------

/**
 * Store a compact, tenant-bound record of an inspection on the session.
 * Contains no prospect rows — only aggregate counts and mission metadata.
 */
function recordOperationalEvidence(session, snapshot, { tenantName = null, recordedAt = null } = {}) {
  if (!session || typeof session !== 'object') return null;
  if (!snapshot || snapshot.refused || !Array.isArray(snapshot.sources)) return null;
  const record = buildEvidenceRecord(snapshot, { tenantName, recordedAt });
  // Held on the session object (like sessionState), not session.context: an
  // envelope context switch rebuilds session.context every turn, and the
  // context is echoed in API responses.
  session.operationalEvidence = record;
  return record;
}

function buildEvidenceRecord(snapshot, { tenantName = null, recordedAt = null } = {}) {
  const p = snapshot.prospecting && snapshot.prospecting.available ? snapshot.prospecting : null;
  const o = snapshot.outbound && snapshot.outbound.available ? snapshot.outbound : null;
  const a = snapshot.agentRuns && snapshot.agentRuns.available ? snapshot.agentRuns : null;
  const g = snapshot.outboundGate || null;
  return {
    spec: SPEC,
    tenantId: String(snapshot.tenantId),
    tenantName: tenantName || null,
    asOf: snapshot.asOf,
    recordedAt: recordedAt || new Date().toISOString(),
    staleAfterDays: snapshot.staleAfterDays,
    missionCounts: countByCategory(snapshot.missions),
    missions: snapshot.missions.map((m) => ({
      id: m.id,
      system: m.system,
      title: m.title || null,
      objective: m.objective || null,
      stage: m.stage || null,
      status: m.status || null,
      synthetic: Boolean(m.synthetic),
      category: m.classification.category,
      certainty: m.classification.certainty,
      basis: m.classification.basis,
      lastActivityAt: m.classification.lastActivityAt,
      ageDays: m.classification.ageDays,
      blockers: (m.blockers || []).map((b) => ({ kind: b.kind, label: b.label })),
    })),
    tenantBlockers: tenantBlockers(snapshot),
    prospecting: p ? { counts: { ...p.counts }, lastAddedAt: p.lastAddedAt, lastContactedAt: p.lastContactedAt } : null,
    outbound: o ? { channels: o.channels.map((c) => ({ ...c })) } : null,
    agentRuns: a ? { agents: a.agents.map((x) => ({ ...x })) } : null,
    outboundGate: g
      ? { available: g.available === true, applicable: g.applicable !== false, mode: g.mode || null, reason: g.reason || null }
      : null,
    sources: snapshot.sources.map((s) => ({ ...s })),
  };
}

function resolveEvidenceState(session, tenantId, now = new Date()) {
  const record = session ? session.operationalEvidence || null : null;
  if (!record) return { state: EVIDENCE_STATES.MISSING, record: null };
  if (String(record.tenantId) !== String(tenantId)) return { state: EVIDENCE_STATES.OTHER_TENANT, record: null };
  const age = now.getTime() - new Date(record.recordedAt || record.asOf).getTime();
  if (!Number.isFinite(age) || age > EVIDENCE_TTL_MS) return { state: EVIDENCE_STATES.STALE, record };
  return { state: EVIDENCE_STATES.CURRENT, record };
}

// ---------------------------------------------------------------------------
// 2. Ownership
// ---------------------------------------------------------------------------

const PRIOR_EVIDENCE_REFERENCE = [
  /\b(?:evidence|report|inspection|snapshot|findings|results|data|status(?:\s+report)?|numbers)\s+(?:that\s+|which\s+)?you\s+(?:just\s+)?(?:retrieved|pulled|gathered|collected|found|showed|shared|gave|reported|produced|returned|read|ran)\b/i,
  /\b(?:the|that|this|your)\s+(?:previous|prior|earlier|last|above)\s+(?:evidence|report|inspection|snapshot|findings|results|status\s+report)\b/i,
  /\b(?:based\s+on|using|from|given|according\s+to|with)\s+(?:the|that|this|your)\s+(?:operational\s+)?(?:evidence|report|inspection|snapshot|findings|status\s+report)\b/i,
  /\b(?:the|that|this)\s+operational\s+(?:evidence|report|inspection|snapshot|findings)\b/i,
];

const ANALYTICAL_REQUEST =
  /\b(?:identify|explain|why|distinguish|differentiate|separate|which|recommend|assess|analy[sz]e|interpret|evaluate|compare|summari[sz]e|classify|cite|reason|what\s+(?:should|does|do|is|are|would)|tell\s+me)\b/i;

const PORTFOLIO_SCOPE =
  /\bmissions\b|\boutbound\b|\b(?:safety\s+)?gate\b|\bkill[\s-]?switch\b|\bmaintenance\b|\bblockers?\b|\bstale\b|\bcanonical\s+(?:business\s+)?objective\b|\bexecution\s+(?:failures?|problems?|errors?|work)\b|\bpaused?\b|\btouchpoints?\b|\bprospects\b/i;

/** One specific mission is mission-owned (status, approval, continuation). */
const SPECIFIC_MISSION_REFERENCE =
  /\b(?:this|that|the\s+current|the\s+active|the\s+selected|my\s+current)\s+(?:acquisition\s+)?mission\b(?!s)|\bmission\s+(?:id\s+)?[a-z]{2,4}_[a-z0-9_]+\b/i;

/** A reply to a pending decision is never analysis, whatever else it says. */
const DECISION_RESPONSE =
  /\b(?:approv(?:e|ed|al)|authori[sz]e|reject|deny|cancel|go\s+ahead|proceed|yes|yep|confirm(?:ed)?|green[\s-]?light|sign\s+off)\b/i;

/**
 * @param {string} question
 * @param {object} [session]
 * @param {{ now?: Date }} [options]
 * @returns {{ matched: boolean, reason: string, cues: string[], evidenceState: string|null }}
 */
function detectOperationalAnalysisFollowUp(question, session = null, options = {}) {
  const raw = String(question || '').trim();
  const none = (reason, cues = []) => ({ matched: false, reason, cues, evidenceState: null });
  if (!raw) return none('empty');
  // An inspection request is owned by the inspection itself.
  if (detectOperationalInspection(raw).matched) return none('operational_inspection_request');

  const affirmative = stripNegatedDirectives(raw);
  if (!affirmative) return none('only_negated_constraints');
  if (hasAffirmativeActionCommand(raw)) return none('affirmative_action_command');
  if (DECISION_RESPONSE.test(affirmative)) return none('decision_response_language');

  const tenantId = session && session.context ? session.context.tenantId : null;
  const evidence = resolveEvidenceState(session, tenantId, options.now || new Date());
  const missionIds = evidence.record ? evidence.record.missions.map((m) => String(m.id).toLowerCase()) : [];
  const lower = affirmative.toLowerCase();
  if (SPECIFIC_MISSION_REFERENCE.test(affirmative) || missionIds.some((id) => id && lower.includes(id))) {
    return none('specific_mission_reference');
  }

  const cues = [];
  const refersToPrior = PRIOR_EVIDENCE_REFERENCE.some((re) => re.test(raw));
  if (refersToPrior) cues.push('prior_evidence_reference');
  if (ANALYTICAL_REQUEST.test(affirmative)) cues.push('analytical_request');
  if (PORTFOLIO_SCOPE.test(affirmative)) cues.push('portfolio_scope');
  const has = (c) => cues.includes(c);

  if (refersToPrior && (has('analytical_request') || has('portfolio_scope'))) {
    return { matched: true, reason: 'explicit_prior_evidence_reference', cues, evidenceState: evidence.state };
  }
  if (
    (evidence.state === EVIDENCE_STATES.CURRENT || evidence.state === EVIDENCE_STATES.STALE) &&
    has('analytical_request') &&
    has('portfolio_scope')
  ) {
    return { matched: true, reason: 'analytical_follow_up_on_session_evidence', cues, evidenceState: evidence.state };
  }
  return none(evidence.record ? 'no_analytical_follow_up_cue' : 'no_session_evidence', cues);
}

// ---------------------------------------------------------------------------
// 4. Analysis (deterministic, grounded only in the evidence record)
// ---------------------------------------------------------------------------

function fmt(ts) {
  return ts ? String(ts).replace('T', ' ').replace(/\.\d+Z$/, 'Z') : 'not recorded';
}

function missionRef(m) {
  return `${m.title || m.id} [${m.id}]`;
}

function capped(rows, render) {
  const out = rows.slice(0, LIST_CAP).map(render);
  if (rows.length > LIST_CAP) out.push(`  … and ${rows.length - LIST_CAP} more (listed in the inspection report).`);
  return out;
}

function pendingDecisionsOf(record) {
  return record.missions
    .map((m) => ({ m, b: m.blockers.find((x) => x.kind === 'awaiting_operator_decision') }))
    .filter((x) => x.b);
}

function composeCanonicalObjective(record, blueprintGoal) {
  const lines = ['1. Canonical business objective'];
  const active = record.missions.filter((m) => m.category === CATEGORIES.ACTIVE && !m.synthetic);
  const withObjective = active.filter((m) => m.objective);
  const declared = blueprintGoal
    ? `Declared business goal (approved Client Blueprint — operator-authored intent, not execution evidence): ${blueprintGoal}`
    : null;
  let canonical = null;
  if (withObjective.length === 1) {
    canonical = withObjective[0];
    lines.push(`- ${canonical.objective}`);
    lines.push(`- Source: objective persisted on the only active, non-synthetic mission, ${missionRef(canonical)} (${canonical.system === 'acquisition_mission' ? 'acquisition_missions' : 'missions'}).`);
    if (declared) lines.push(`- ${declared}`);
  } else if (withObjective.length > 1) {
    lines.push(`- I cannot single out one canonical objective from persisted state: ${withObjective.length} active missions carry objectives:`);
    lines.push(...capped(withObjective, (m) => `  • ${missionRef(m)}: ${m.objective}`));
    if (declared) lines.push(`- ${declared}`);
    lines.push('- Which of these is canonical is an operator decision; I have not chosen one.');
  } else {
    lines.push('- No active, non-synthetic mission carries a persisted objective, so the canonical objective cannot be verified from execution state.');
    if (declared) lines.push(`- ${declared}`);
  }
  return { lines, canonical };
}

function composeRelevance(record, canonical) {
  const lines = ['2. Missions relevant to that objective'];
  const relevant = record.missions.filter((m) => m.category === CATEGORIES.ACTIVE && !m.synthetic);
  const stale = record.missions.filter((m) => m.category === CATEGORIES.STALE);
  const closed = record.missions.filter((m) => m.category === CATEGORIES.COMPLETED);
  const devTest = record.missions.filter((m) => m.category === CATEGORIES.DEVELOPMENT_TEST);
  const uncertain = record.missions.filter((m) => m.category === CATEGORIES.UNCERTAIN);

  if (relevant.length) {
    lines.push(`Plausibly current work (${relevant.length}) — active, non-synthetic, activity within ${record.staleAfterDays} days:`);
    lines.push(...capped(relevant, (m) => `  • ${missionRef(m)} — ${[m.stage, m.status].filter(Boolean).join(' / ') || 'status not recorded'}; last activity ${fmt(m.lastActivityAt)}${canonical && canonical.id === m.id ? ' (carries the canonical objective)' : ''}`));
  } else {
    lines.push('Plausibly current work: none — no active, non-synthetic mission is recorded.');
  }
  const historical = stale.length + closed.length + devTest.length;
  lines.push(`Historical — should not be treated as current execution work (${historical}):`);
  if (stale.length) {
    lines.push(`  • Stale (${stale.length}): non-terminal, no recorded activity for more than ${record.staleAfterDays} days. Any pending decision on them is historical state, not a current request.`);
    lines.push(...capped(stale, (m) => `    – ${missionRef(m)}; last activity ${fmt(m.lastActivityAt)}`));
  }
  if (closed.length) lines.push(`  • Completed / closed (${closed.length}): persisted terminal status.`);
  if (devTest.length) lines.push(`  • Development / test (${devTest.length}): persisted synthetic-evidence or test markers.`);
  if (!historical) lines.push('  • none recorded');
  lines.push(`Ambiguous — needs operator confirmation before being treated either way (${uncertain.length}):`);
  if (uncertain.length) lines.push(...capped(uncertain, (m) => `  • ${missionRef(m)} — ${m.basis}`));
  else lines.push('  • none');
  return lines;
}

const KILL_SWITCH_RE = /kill[_\s-]?switch|maintenance|recovery/i;

function composeSafetyVersusFailure(record) {
  const lines = ['3. Why outbound is paused — safety gate vs. execution failures'];
  const g = record.outboundGate;
  lines.push('Safety gate (persisted control state):');
  if (!g || !g.available) {
    lines.push('- Gate state UNAVAILABLE — I cannot confirm from persisted state whether outbound is intentionally held.');
  } else if (!g.applicable) {
    lines.push('- No tenant-specific outbound recovery gate applies to this tenant, so a pause cannot be attributed to a gate.');
  } else if (g.mode && g.mode !== 'active' && g.mode !== 'legacy') {
    const intentional = KILL_SWITCH_RE.test(`${g.mode} ${g.reason || ''}`);
    lines.push(`- The outbound gate is in ${g.mode}${g.reason ? ` (${g.reason})` : ''}. ${intentional ? 'This is an armed safety control that holds sends by design — it is a deliberate hold, not an execution error.' : 'Sends are held by this control state.'}`);
    lines.push('- What the database establishes: the gate is engaged and its mode/reason code. What it does not record: the business rationale. If outbound is held pending Max reliability validation, that rationale is your operator directive — I treat it as operator-declared, not as persisted evidence.');
  } else {
    lines.push(`- The outbound gate is ${g.mode || 'in an unknown mode'} — it is not holding sends, so any lack of outbound activity is not explained by the gate.`);
  }

  lines.push('Actual execution failures (kept separate from the gate):');
  const failures = [];
  const agents = record.agentRuns ? record.agentRuns.agents : null;
  for (const a of agents || []) {
    if (a.errors7d > 0) failures.push(`- agent_log: ${a.agent} recorded ${a.errors7d} error run(s) in the last 7 days.`);
  }
  for (const m of record.missions) {
    for (const b of m.blockers) {
      if (b.kind === 'mission_failed') failures.push(`- ${missionRef(m)}: ${b.label}.`);
    }
  }
  if (failures.length) lines.push(...failures);
  else if (agents) lines.push('- No error runs or failed missions are recorded in the readable sources.');
  else lines.push('- agent_log was unavailable, so agent errors cannot be ruled out.');

  const email = record.outbound ? record.outbound.channels.find((c) => c.channel === 'email') : null;
  if (record.outbound) {
    const sent7 = email ? email.last7d : 0;
    lines.push(`- Email touchpoints in the last 7 days: ${sent7}${email ? ` (last ${fmt(email.lastAt)})` : ''}. ${sent7 === 0 && g && g.available && g.applicable && g.mode && g.mode !== 'active' && g.mode !== 'legacy' ? 'Zero sends is consistent with the gate holding outbound; on its own it is neither evidence of a failure nor of healthy delivery.' : 'This is activity data only; it does not establish delivery health.'}`);
  } else {
    lines.push('- touchpoints were unavailable, so recent outbound activity cannot be stated.');
  }

  const constraints = [];
  const synthetic = record.missions.filter((m) => m.blockers.some((b) => b.kind === 'synthetic_evidence'));
  if (synthetic.length) constraints.push(`- ${synthetic.length} mission(s) are refused outreach because they carry synthetic (fixture) Scout evidence — a governance block, not a runtime failure.`);
  const restricted = record.missions.filter((m) => m.blockers.some((b) => b.kind === 'operator_restriction'));
  if (restricted.length) constraints.push(`- ${restricted.length} mission(s) carry durable operator restrictions.`);
  const recorded = record.missions
    .filter((m) => m.category === CATEGORIES.ACTIVE)
    .flatMap((m) => m.blockers.filter((b) => b.kind === 'mission_blocker').map((b) => `- ${missionRef(m)}: ${String(b.label).replace(/[.\s]+$/, '')} (mission-recorded blocker).`));
  constraints.push(...recorded);
  const pending = pendingDecisionsOf(record);
  if (pending.length) {
    const current = pending.filter((x) => x.m.category === CATEGORIES.ACTIVE).length;
    constraints.push(`- ${pending.length} mission(s) hold a pending operator decision (${current} on active missions, ${pending.length - current} on historical or ambiguous missions). These are approval gates, not failures. I did not request, approve, or consume any of them.`);
  }
  if (constraints.length) {
    lines.push('Operator gates and governance holds (neither safety gate nor failure):');
    lines.push(...constraints);
  }
  return lines;
}

function composeRecommendation(record) {
  const lines = ['4. Recommended next reliability validation step'];
  const errored = (record.agentRuns ? record.agentRuns.agents : []).filter((a) => a.errors7d > 0);
  lines.push(
    '- Run one controlled Max reliability check with the outbound gate left exactly as it is: in a single production workspace session, repeat this inspect → analyze sequence, then send a turn containing only negated instructions and a mission-specific status question. Confirm from persisted state that acquisition_missions, mission events and pending decisions are unchanged, no approval was consumed or newly requested, the session execution policy is unchanged, and email touchpoints stay at zero.'
  );
  if (errored.length) {
    lines.push(`- Separately, review the ${errored.map((a) => `${a.agent} (${a.errors7d})`).join(', ')} error run(s) in agent_log before relying on that agent in the validation.`);
  }
  lines.push('- Only after that check passes should you consider an explicit, operator-approved step on the canonical mission. Lifting the outbound gate remains a separate operator decision; this is not a recommendation to approve, resume, or launch anything.');
  return lines;
}

function composeEvidenceQuality(record, { evidenceUse, refreshedReason, constraints }) {
  const lines = ['5. Evidence and limits'];
  lines.push(
    evidenceUse === 'reused'
      ? `- Basis: the read-only inspection already retrieved in this conversation (as of ${fmt(record.asOf)}). State may have changed since then.`
      : `- Basis: a fresh read-only inspection as of ${fmt(record.asOf)} (${refreshedReason}).`
  );
  for (const s of record.sources) {
    lines.push(s.available ? `- ✓ ${s.source}: ${s.provenance || 'read'}` : `- ✗ ${s.source}: UNAVAILABLE (${s.reason}) — nothing in this answer relies on it.`);
  }
  const cannot = ['the business rationale for the outbound pause (operator-declared; the gate records only mode and reason code)'];
  if (record.missionCounts.stale) cannot.push(`whether the ${record.missionCounts.stale} stale mission(s) are abandoned or deliberately parked (no persisted lifecycle marker)`);
  if (record.missionCounts.uncertain) cannot.push(`the true status of the ${record.missionCounts.uncertain} uncertain mission(s)`);
  cannot.push('provider-side delivery or mailbox health (not read)');
  const unavailable = record.sources.filter((s) => !s.available).map((s) => s.source);
  if (unavailable.length) cannot.push(`anything that depends on ${unavailable.join(', ')}`);
  lines.push(`- Cannot verify: ${cannot.join('; ')}.`);
  if (constraints.length) {
    lines.push(`- Your constraints were honoured: ${constraints.map((c) => `"${c.replace(/[.\s]+$/, '')}"`).join('; ')}.`);
  }
  return lines;
}

function composeOperationalAnalysis(record, ctx = {}) {
  const tenantLabel = record.tenantName ? `${record.tenantName} (tenant ${record.tenantId})` : `tenant ${record.tenantId}`;
  const objective = composeCanonicalObjective(record, ctx.blueprintGoal || null);
  const sections = [
    [
      `Operational analysis — ${tenantLabel}`,
      'Reasoning over persisted read-only evidence. Nothing was approved, resumed, created, executed, or changed; the outbound gate, pending decisions, and session settings were not touched.',
    ],
    objective.lines,
    composeRelevance(record, objective.canonical),
    composeSafetyVersusFailure(record),
    composeRecommendation(record),
    composeEvidenceQuality(record, ctx),
  ];
  return { prose: sections.map((s) => s.join('\n')).join('\n\n'), canonical: objective.canonical };
}

async function loadBlueprintGoal(service, tenantId) {
  if (!service || typeof service.getApprovedClientBlueprint !== 'function') return null;
  try {
    const bp = await service.getApprovedClientBlueprint(Number(tenantId));
    const goal = bp && bp.summary && (bp.summary.campaignGoals || bp.summary.goals);
    return goal ? String(goal).trim().slice(0, 400) : null;
  } catch (_) {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Turn handler
// ---------------------------------------------------------------------------

/**
 * @returns {Promise<null|{ prose, structured, reason, detection, evidence, mutationBoundary }>}
 */
async function maybeHandleOperationalAnalysisTurn(input = {}) {
  const { question, session } = input;
  const opts = input.opts || {};
  const now = opts.now instanceof Date ? opts.now : new Date();
  const detection = detectOperationalAnalysisFollowUp(question, session, { now });
  askPathTrace.traceBranch('operational_analysis_detection', {
    matched: detection.matched,
    reason: detection.reason,
    cues: detection.cues,
    evidenceState: detection.evidenceState,
  });
  if (!detection.matched) return null;

  const tenantId = session && session.context && session.context.tenantId;
  if (tenantId == null || tenantId === '') {
    const prose = 'Operational analysis — not performed\nNo active client is selected for this workspace session, so there is no tenant evidence to reason over.';
    return {
      prose,
      structured: buildStructuredResponse({ answer: prose, confidence: 1, metadata: { operationalAnalysis: true, operationalInspectionRefused: 'no_tenant' } }),
      reason: 'operational_analysis_no_tenant',
      detection,
      evidence: null,
      mutationBoundary: null,
    };
  }

  const { state, record: existing } = resolveEvidenceState(session, tenantId, now);
  let record = existing;
  let evidenceUse = 'reused';
  let refreshedReason = null;
  let mutationBoundary;

  if (state === EVIDENCE_STATES.CURRENT) {
    // Named-client authorization still applies to a turn that reuses evidence.
    const check = await checkNamedClientScope({ question, tenantId, opts });
    mutationBoundary = check.mutationBoundary;
    if (!check.scope.ok) {
      logAnalysis({ tenantId, detection, evidenceUse: 'none', refused: 'tenant_mismatch', mutationBoundary });
      return { ...refusalTurn({ scope: check.scope, tenantId }), detection, evidence: null, mutationBoundary };
    }
  } else {
    refreshedReason = {
      [EVIDENCE_STATES.MISSING]: 'no inspection evidence was available in this conversation, so I retrieved it now',
      [EVIDENCE_STATES.STALE]: `the earlier evidence was older than ${Math.round(EVIDENCE_TTL_MS / 60000)} minutes, so I re-read it`,
      [EVIDENCE_STATES.OTHER_TENANT]: 'the earlier evidence belonged to a different tenant and was not reused',
    }[state];
    const snapshot = await loadOperationalSnapshot({
      tenantId,
      question,
      opts,
      acquisitionMissionRuntime: input.acquisitionMissionRuntime || null,
    });
    mutationBoundary = snapshot.mutationBoundary;
    if (!snapshot.scope.ok) {
      logAnalysis({ tenantId, detection, evidenceUse: 'none', refused: 'tenant_mismatch', mutationBoundary });
      return { ...refusalTurn({ scope: snapshot.scope, tenantId }), detection, evidence: null, mutationBoundary };
    }
    record = recordOperationalEvidence(session, snapshot, {
      tenantName: (snapshot.scope.self && snapshot.scope.self.name) || null,
      recordedAt: now.toISOString(),
    });
    evidenceUse = 'refreshed';
  }

  const blueprintGoal = await loadBlueprintGoal(input.clientIntelligenceService, tenantId);
  // Imperative prohibitions only ("Do not …", "Never …"), not descriptive
  // phrases such as "missions that should not be treated as current work".
  const constraints = extractNegatedClauses(question).filter((c) => /^(?:do\s+not|don['’]?t|dont|never|must\s+not)\b/i.test(c));
  const { prose, canonical } = composeOperationalAnalysis(record, { blueprintGoal, evidenceUse, refreshedReason, constraints });
  const pending = pendingDecisionsOf(record);
  const unavailableSources = record.sources.filter((s) => !s.available).map((s) => `${s.source}: ${s.reason}`);

  const structured = buildStructuredResponse({
    answer: prose,
    reasoning: [
      'P0 #1089 — analytical follow-up over persisted read-only operational evidence; no mission runtime, pending decision, execution router, session-state resolver, or outbound control was invoked.',
    ],
    supportingEvidence: record.sources
      .filter((s) => s.available)
      .map((s) => ({ id: `operational:${s.source}`, kind: 'operational_source', label: s.provenance || s.source, source: s.source, timestamp: record.asOf })),
    confidence: unavailableSources.length ? 0.6 : 0.85,
    recommendedActions: [],
    metadata: {
      operationalAnalysis: true,
      businessIntelligenceUsed: false,
      asOf: record.asOf,
      unavailable: unavailableSources,
      operationalEvidenceContinuity: {
        spec: SPEC,
        tenantId: record.tenantId,
        evidenceUse,
        evidenceState: state,
        evidenceAsOf: record.asOf,
        refreshedReason,
        detection: detection.reason,
        canonicalObjectiveSource: canonical ? `${canonical.system}:${canonical.id}` : blueprintGoal ? 'client_blueprint_declared' : 'unverified',
        missionCounts: record.missionCounts,
        pendingDecisions: { present: pending.length, requested: 0, consumed: 0 },
        sources: record.sources.map((s) => ({ source: s.source, available: s.available, reason: s.reason })),
        mutationBoundary: mutationBoundary || null,
      },
    },
  });
  logAnalysis({ tenantId, detection, evidenceUse, evidenceState: state, pendingPresent: pending.length, mutationBoundary, record });
  return { prose, structured, reason: detection.reason, detection, evidence: record, mutationBoundary };
}

function logAnalysis({ tenantId, detection, evidenceUse, evidenceState, pendingPresent, mutationBoundary, refused, record }) {
  // Route, evidence provenance, counts and boundary only — no prospect data,
  // mission titles, or objectives.
  const row = {
    event: 'OPERATIONAL_ANALYSIS',
    spec: SPEC,
    tenantId: String(tenantId),
    route: 'operational_analysis',
    owner: 'operational_analysis',
    detection: detection.reason,
    cues: detection.cues,
    evidenceUse,
    evidenceState: evidenceState || null,
    refused: refused || null,
    missionCounts: record ? record.missionCounts : null,
    pendingDecisions: { present: pendingPresent || 0, requested: 0, consumed: 0 },
    sources: record ? record.sources.map((s) => ({ source: s.source, available: s.available })) : null,
    mutationBoundary: mutationBoundary || null,
    timestamp: new Date().toISOString(),
  };
  if (process.env.NODE_ENV !== 'test') console.info('[OPERATIONAL_ANALYSIS]', JSON.stringify(row));
  return row;
}

module.exports = {
  SPEC,
  EVIDENCE_TTL_MS,
  EVIDENCE_STATES,
  recordOperationalEvidence,
  buildEvidenceRecord,
  resolveEvidenceState,
  detectOperationalAnalysisFollowUp,
  composeOperationalAnalysis,
  maybeHandleOperationalAnalysisTurn,
};
