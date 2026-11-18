'use strict';

/**
 * Frame-directed reasoning coordinator (P0-MAX-REASONING-001 PR 5).
 * Default-off (MAX_FRAME_REASONING). Not wired into routing.
 *
 *   validated TurnFrame → conversational memory (referents) → evidence port
 *   (current, tenant-scoped) → ReasoningResult → structured response
 *
 * It adds no intent classifier, planner, agent loop or tool calls: every
 * decision reads fields of the validated TurnFrame, the memory's resolved
 * focus, or the evidence report. It never executes: state-changing requests
 * yield an explicit `not_executed` status naming the deterministic pathway
 * (pending-decision resolver / routeExecutionRequest / TME) that alone may act.
 *
 * Ports (plain-data contracts; no module dependency):
 *   memory   — the ConversationLedger read shape (PR 3 recorder.context()):
 *              { focus: { referent: { id, kind, label, scope, memberIds, missionId },
 *                         requiresRevalidation, evidenceRefreshRequired, verified:false },
 *                view: { remembered: { focus: [{ id, kind, ... }], constraints: [...], ... } } }
 *              (optionally `referents`: full referent list for candidate lookup)
 *   evidence — { retrieve({ tenantId, domains, referents }) } returning the
 *              EvidenceRegistry report shape (PR 4); optional `diagnose`:
 *              { scout(report), emmett(report), rank(report, opts) }.
 */

const { EVIDENCE_DOMAINS, STATE_CHANGING_VERBS } = require('../interpretation/TurnFrame');

const ID_RE = /^[A-Za-z0-9_.:-]{1,160}$/;
const MAX_DOMAINS = 4;
const RESULT_KINDS = Object.freeze(['answer', 'clarification', 'not_executed', 'refused']);

const SUBJECT_DOMAINS = Object.freeze({
  prospecting: ['crm_pipeline', 'scout_execution'],
  crm_pipeline: ['crm_pipeline'],
  mission: ['mission_lifecycle', 'operator_decisions'],
  agent_activity: ['agent_execution'],
  outbound: ['outbound_authorization', 'outbound_delivery'],
  campaign: ['campaign_performance'],
  pending_decision: ['operator_decisions', 'mission_lifecycle'],
  portfolio: ['mission_lifecycle', 'crm_pipeline', 'agent_execution'],
  business_process: [],
  none: [],
});
const FOLLOWING = new Set(['follow_up', 'correction', 'refinement', 'clarification_answer']);

// Which existing deterministic pathway owns a state-changing verb. Named so
// the response can say where authorization lives; never called from here.
const EXECUTION_PATHWAYS = Object.freeze({
  approve: 'pending-decision resolver, then routeExecutionRequest / TME (operator approval at the gate)',
  reject: 'pending-decision resolver',
  cancel: 'pending-decision resolver / mission runtime',
  pause: 'mission runtime via routeExecutionRequest',
  resume: 'mission runtime via routeExecutionRequest',
  restart: 'mission runtime via routeExecutionRequest',
  send: 'Emmett governed outbound via routeExecutionRequest (outbound gate + program authorization)',
  create: 'mission creation via the acquisition mission runtime',
  modify: 'CRM writes via an authorized specialist through routeExecutionRequest',
  run_agent: 'specialist dispatch via routeExecutionRequest',
});

function frameReasoningEnabled(env = process.env) { return env.MAX_FRAME_REASONING === 'true'; }

function refusal(tenantId, reason, frame) {
  return base('refused', tenantId, frame, { refusal: reason, unknowns: [{ kind: 'unknown', what: 'requested evidence', reason }] });
}

function base(kind, tenantId, frame, extra = {}) {
  return {
    kind,
    tenantId: String(tenantId),
    frame: frame ? { producer: frame.producer, intent: frame.intent, subjectKind: frame.subjectKind, relationToPrior: frame.relationToPrior,
      facets: [...(frame.facets || [])], action: { ...(frame.requestedAction || {}) }, confidence: frame.confidence } : null,
    referent: null,
    subjectRecordIds: [],
    revalidation: [],
    observed: [],
    calculated: [],
    inferences: [],
    contradictions: [],
    unknowns: [],
    recommendations: [],
    clarification: null,
    execution: { requested: null, executed: false, status: 'none', reason: null, constraints: [], pathway: null },
    domains: [],
    authority: { grantsAuthority: false, executionPermitted: false },
    ...extra,
  };
}

/** Plain-data memory referent → evidence-port referents (mirrors the PR 4 contract). */
function evidenceReferents(referent) {
  if (!referent) return [];
  if (referent.kind === 'prospect_set') {
    const ids = (referent.memberIds || []).map(String).filter((id) => ID_RE.test(id));
    return ids.length ? [{ kind: 'prospect_set', memberIds: ids }] : [];
  }
  if ((referent.kind === 'mission' || referent.kind === 'pending_decision') && ID_RE.test(String(referent.missionId || ''))) {
    return [{ kind: 'mission', missionId: String(referent.missionId) }];
  }
  return [];
}

function selectDomains(frame, referent) {
  const out = [];
  const add = (d) => { if (EVIDENCE_DOMAINS.includes(d) && !out.includes(d)) out.push(d); };
  for (const d of frame.evidenceNeeds || []) add(d);
  if (!out.length) for (const d of SUBJECT_DOMAINS[frame.subjectKind] || []) add(d);
  if (referent?.kind === 'prospect_set') add('crm_pipeline');
  if (referent?.kind === 'mission' || referent?.kind === 'pending_decision') add('mission_lifecycle');
  const facets = new Set(frame.facets || []);
  if (facets.has('history') && out.includes('mission_lifecycle')) add('operator_decisions');
  if ((facets.has('cause') || facets.has('consequence')) && out.includes('mission_lifecycle')) add('agent_execution');
  return out.slice(0, MAX_DOMAINS);
}

/**
 * Resolve what the turn is about. Returns { referent, clarification }.
 * Frame referents (candidate ids from the memory) win; a following turn with
 * no explicit referent uses the memory's focus; a correction with no explicit
 * target moves to the earlier subject only when exactly one exists.
 */
/**
 * The memory port exposes referents in two shapes: the resolved focus uses
 * `memberIds`; the ledger's stored referent list uses `members`. Normalise once.
 */
function normalizeReferent(r) {
  if (!r || typeof r !== 'object') return null;
  const memberIds = Array.isArray(r.memberIds) ? r.memberIds : Array.isArray(r.members) ? r.members : [];
  return { id: r.id, kind: r.kind, label: r.label || null, scope: r.scope || null, memberIds: memberIds.map(String), missionId: r.missionId || null };
}

function resolveReferent(frame, memoryInput) {
  const memory = memoryInput ? {
    ...memoryInput,
    referents: (memoryInput.referents || []).map(normalizeReferent).filter(Boolean),
    focus: memoryInput.focus ? { ...memoryInput.focus, referent: normalizeReferent(memoryInput.focus.referent) } : memoryInput.focus,
  } : null;
  const candidates = memory?.referents || [];
  const byId = new Map(candidates.map((r) => [r.id, r]));
  const focus = memory?.focus?.referent || null;
  const stack = memory?.view?.remembered?.focus || [];
  const ambiguity = (frame.ambiguities || []).find((a) => a.kind === 'referent');
  if (ambiguity) {
    const options = ambiguity.candidateIds.map((id) => byId.get(id) || stack.find((s) => s.id === id)).filter(Boolean);
    return { referent: null, clarification: { reason: 'ambiguous_referent', options: options.map((o) => ({ id: o.id, kind: o.kind, label: o.missionId ? `${o.label || o.kind} ${o.missionId}` : o.label })) } };
  }
  const primary = (frame.referents || []).find((r) => r.role === 'primary');
  if (primary) {
    const r = byId.get(primary.candidateId) || (focus && focus.id === primary.candidateId ? focus : null);
    if (r) return { referent: r, clarification: null };
  }
  if (frame.relationToPrior === 'correction' && !primary) {
    const earlier = stack.slice(1);
    const sameKind = focus ? earlier.filter((s) => s.kind === focus.kind) : earlier;
    if (sameKind.length === 1) return { referent: byId.get(sameKind[0].id) || sameKind[0], clarification: null, correctedFrom: focus ? focus.id : null };
    if (sameKind.length > 1) return { referent: null, clarification: { reason: 'which_earlier_subject', options: sameKind.map((o) => { const n = byId.get(o.id) || o; return { id: o.id, kind: o.kind, label: n.missionId ? `${n.label || n.kind} ${n.missionId}` : n.label }; }) } };
    // A scope correction (e.g. "existing, not new") is applied by the memory
    // itself; its focus is then the corrected referent.
    if (focus) return { referent: focus, clarification: null };
    return { referent: null, clarification: { reason: 'nothing_to_correct', options: [] } };
  }
  if (FOLLOWING.has(frame.relationToPrior)) {
    if (focus) return { referent: focus, clarification: null };
    // A follow-up that names its own subject ("and what about Emmett?") needs
    // no remembered referent; one that names nothing does.
    const ownSubject = frame.subjectKind !== 'none' || (frame.evidenceNeeds || []).length > 0;
    if (ownSubject && frame.subjectKind !== 'crm_pipeline') return { referent: null, clarification: null };
    return { referent: null, clarification: { reason: 'no_current_referent', options: [] } };
  }
  return { referent: null, clarification: null };
}

// ---------------------------------------------------------------------------
// Statements from observed facts (deterministic; never adds a fact)
// ---------------------------------------------------------------------------

const d10 = (iso) => (iso ? String(iso).slice(0, 10) : 'not recorded');
const age = (f) => (f && f.ageDays != null ? `${f.ageDays} day(s) ago` : 'age unknown');

function statementFor(item) {
  const f = item.facts || {};
  switch (item.subject.type) {
    case 'prospect_record': {
      const ch = f.contactChannels || {};
      const channels = ['email', 'phone', 'linkedin'].filter((k) => ch[k]).join(', ') || 'none recorded';
      return `Record ${item.subject.id}: pipeline ${f.pipelineStatus || 'not recorded'}, setter ${f.setterStatus || 'not recorded'}, opportunity ${f.opportunityState}; contact channels: ${channels}; last contacted ${d10(f.lastContactedAt)}; record updated ${age(item.freshness)}.`;
    }
    case 'mission':
      return `Mission ${item.subject.id}: status ${f.status || 'not recorded'}, classified ${f.classification} (${f.classificationBasis}).${f.awaitingOperatorDecision ? ` Awaiting operator decision${f.pendingDecisionKind ? ` (${f.pendingDecisionKind})` : ''}; not resolved by this read.` : ''}`;
    case 'pending_decision':
      return `Mission ${f.missionId} has a pending ${f.kind || 'operator'} decision. It remains pending; reading it does not approve or reject it.`;
    case 'mission_event':
      return `Mission ${f.missionId} event "${f.eventKind}"${f.specialist ? ` (${f.specialist})` : ''} recorded ${d10(item.observedAt)}.`;
    case 'agent_run':
      return `Agent ${f.agent}: last recorded run ${d10(f.lastRunAt)}; ${f.runs7d} run(s) and ${f.errors7d} error(s) in the last 7 days.`;
    case 'prospect_aggregate':
      return `${f.total} prospect record(s), ${f.qualified} at or above the qualification threshold; last added ${d10(f.lastAddedAt)}.`;
    case 'outbound_gate':
      return `Outbound gate mode: ${f.mode}${f.reason ? ` (${f.reason})` : ''}.`;
    case 'outbound_program':
      return `Outbound program ${item.subject.id}: mode ${f.mode}; authorization ${f.authorizationExpired ? `expired ${d10(f.expiresAt)}` : f.expiresAt ? `valid until ${d10(f.expiresAt)}` : 'expiry not recorded'}.`;
    case 'outbound_channel':
      return `${item.subject.id} touchpoints: ${f.last7d} in 7 days, ${f.last30d} in 30 days; last ${d10(f.lastAt)}.`;
    case 'campaign':
      return `Campaign email totals: ${f.sends} sent, ${f.opens} opened, ${f.replies} replied, ${f.bounces} bounced.`;
    default:
      return `${item.subject.type} ${item.subject.id} observed.`;
  }
}

function observedFrom(report, revalidation) {
  const items = [];
  for (const r of Object.values(report.domains || {})) for (const i of r.items || []) items.push(i);
  for (const r of revalidation) if (r.evidence && r.evidence.subject) items.push(r.evidence);
  const seen = new Set();
  return items.filter((i) => !seen.has(i.ref) && seen.add(i.ref)).map((i) => ({
    ref: i.ref, epistemic: 'verified_observed', statement: statementFor(i), source: i.source, tenantId: i.tenantId,
    observedAt: i.observedAt, retrievedAt: i.retrievedAt, freshness: i.freshness, missing: i.missing || [], subject: i.subject, facts: i.facts,
  }));
}

/** "Which of those has the freshest information?" — computed from observed timestamps only. */
function freshestAmong(observed, memberIds) {
  const pool = observed.filter((o) => o.subject.type === 'prospect_record' && memberIds.includes(o.subject.id) && o.observedAt);
  if (!pool.length) return null;
  const latest = Math.max(...pool.map((o) => Date.parse(o.observedAt)));
  const top = pool.filter((o) => Date.parse(o.observedAt) === latest);
  return { kind: 'calculated', claim: top.length === 1 ? `Most recently updated: record ${top[0].subject.id} (${d10(top[0].observedAt)}).` : `Tied for most recently updated: ${top.map((t) => t.subject.id).join(', ')} (${d10(top[0].observedAt)}).`,
    basis: pool.map((o) => o.ref), method: 'latest recorded update timestamp among the referenced records' };
}

class FrameDirectedReasoner {
  /**
   * @param {{ evidence: { retrieve: Function, diagnose?: object }, now?: () => Date }} ports
   */
  constructor({ evidence, now = () => new Date() } = {}) {
    if (!evidence || typeof evidence.retrieve !== 'function') throw new Error('FrameDirectedReasoner requires an evidence port');
    this.evidence = evidence;
    this.now = now;
  }

  /**
   * @param {{ tenantId: string, frame: object, memory?: object|null, constraints?: string[] }} input
   *   constraints: prohibitions stated in THIS turn (e.g. "…don't send anything"), which
   *   the memory only records after the turn; merged with remembered prohibitions.
   * @returns {Promise<object>} ReasoningResult
   */
  async reason({ tenantId, frame, memory = null, constraints: turnConstraints = [] }) {
    const tenant = String(tenantId ?? '');
    if (!ID_RE.test(tenant)) return refusal(tenant, 'invalid_tenant', null);
    if (!frame || frame.authority?.grantsAuthority !== false || String(frame.tenantId) !== tenant) return refusal(tenant, 'invalid_frame', null);
    if (frame.foreignTenantReference) return refusal(tenant, 'foreign_tenant_reference', frame);

    const verb = frame.requestedAction?.verb || 'none';
    const stateChanging = STATE_CHANGING_VERBS.includes(verb) || frame.intent === 'authorization';
    const constraints = [...new Set([
      ...(memory?.view?.remembered?.constraints || []).filter((c) => c.kind === 'prohibition').map((c) => c.action),
      ...(Array.isArray(turnConstraints) ? turnConstraints.filter((c) => typeof c === 'string' && /^[a-z_]{1,32}$/.test(c)) : []),
    ])];

    const { referent, clarification, correctedFrom } = resolveReferent(frame, memory);
    const result = base('answer', tenant, frame);
    result.referent = referent ? {
      id: referent.id, kind: referent.kind, label: referent.label, scope: referent.scope || null,
      memberIds: [...(referent.memberIds || [])], missionId: referent.missionId || null,
      remembered: Boolean(memory), requiresRevalidation: true, correctedFrom: correctedFrom || null,
    } : null;

    // Ambiguity — including an ambiguous or conditional authorization — is
    // answered with a targeted question, never by guessing or acting.
    if (frame.clarificationRequired || frame.intent === 'ambiguous' || clarification) {
      result.kind = 'clarification';
      result.clarification = clarification || { reason: frame.intent === 'ambiguous' ? 'ambiguous_request' : 'clarification_required', options: [] };
      if (verb !== 'none' || frame.intent === 'authorization' || frame.ambiguities?.some((a) => a.kind === 'action')) {
        result.execution = { requested: verb === 'none' ? 'unclear' : verb, executed: false, status: 'not_executed', reason: 'request is ambiguous; nothing was approved, consumed or executed', constraints, pathway: EXECUTION_PATHWAYS[verb] || null };
      }
      return result;
    }

    if (stateChanging || frame.intent === 'prohibition') {
      result.kind = 'not_executed';
      result.execution = {
        requested: verb, executed: false,
        status: frame.intent === 'prohibition' ? 'held_by_operator' : 'not_executed',
        reason: frame.intent === 'prohibition'
          ? 'the operator prohibited this action; it is remembered as a constraint and nothing was executed'
          : 'frame-directed reasoning is read-only; no records, missions, approvals or outreach were changed',
        constraints: [...new Set([...constraints, ...(frame.intent === 'prohibition' ? [verb] : [])])],
        pathway: EXECUTION_PATHWAYS[verb] || null,
      };
    }

    // Retrieve current evidence for the resolved subject (read-only, even for
    // a state-changing request: it identifies the targets, it does not act).
    const domains = selectDomains(frame, referent);
    result.domains = domains;
    if (!domains.length && !referent) {
      result.unknowns.push({ kind: 'unknown', what: 'operational evidence', reason: 'the request names no operational subject; nothing was retrieved' });
      return result;
    }
    let report;
    try {
      report = await this.evidence.retrieve({ tenantId: tenant, domains, referents: evidenceReferents(referent) });
    } catch (_) {
      result.unknowns.push({ kind: 'unknown', what: 'operational evidence', reason: 'evidence retrieval failed; no facts are asserted' });
      return result;
    }
    if (!report || report.tenantId !== tenant) {
      result.unknowns.push({ kind: 'unknown', what: 'operational evidence', reason: 'evidence report missing or for another tenant; discarded' });
      return result;
    }
    if (report.refused) return { ...refusal(tenant, report.refused, frame), execution: result.execution, kind: result.kind === 'not_executed' ? 'not_executed' : 'refused' };

    result.revalidation = (report.revalidation?.results || []).map((r) => ({ kind: r.kind, id: r.id, status: r.status, detail: r.detail || null }));
    const all = observedFrom(report, report.revalidation?.results || []);
    // A question about a remembered subject is answered about that subject:
    // other records or missions read alongside it are not presented.
    const keep = (o) => {
      if (!referent) return true;
      if (referent.kind === 'prospect_set' && referent.memberIds && referent.memberIds.length) {
        return o.subject.type !== 'prospect_record' || referent.memberIds.includes(o.subject.id);
      }
      if ((referent.kind === 'mission' || referent.kind === 'pending_decision') && referent.missionId) {
        if (o.subject.type === 'mission') return o.subject.id === referent.missionId;
        if (o.subject.type === 'pending_decision' || o.subject.type === 'mission_event') return o.facts.missionId === referent.missionId;
      }
      return true;
    };
    result.observed = all.filter(keep);
    const excluded = new Set(all.filter((o) => !keep(o)).map((o) => o.ref));
    result.contradictions = (report.contradictions || []).filter((c) => !(c.refs || []).some((r) => excluded.has(r))).map((c) => ({ ...c }));
    result.unknowns.push(...(report.unknowns || []).map((u) => ({ ...u })));

    const facets = new Set(frame.facets || []);
    if (referent?.kind === 'prospect_set' && (facets.has('comparison') || facets.has('status'))) {
      const current = result.revalidation.filter((r) => r.status === 'current').map((r) => r.id);
      const freshest = freshestAmong(result.observed, current);
      if (freshest) result.calculated.push(freshest);
    }

    // Inferences come only from the diagnosis port, each citing its basis.
    const dx = this.evidence.diagnose || {};
    const wantsCause = facets.has('cause') || facets.has('recommendation') || facets.has('next_step');
    if (wantsCause && domains.includes('scout_execution') && typeof dx.scout === 'function') result.inferences.push(...(dx.scout(report).inferences || []));
    if (wantsCause && (domains.includes('outbound_authorization') || domains.includes('outbound_delivery')) && typeof dx.emmett === 'function') result.inferences.push(...(dx.emmett(report).inferences || []));
    if (facets.has('recommendation') && domains.includes('crm_pipeline') && typeof dx.rank === 'function' && !referent) {
      const ranked = dx.rank(report, { top: 5 });
      result.subjectRecordIds = ranked.ranked.map((r) => r.ref.split(':').pop());
      if (ranked.ranked.length < 5 || (ranked.excluded || []).length) {
        const reasons = (ranked.excluded || []).map((e) => `${e.ref.split(':').pop()} (${e.reason})`);
        result.calculated.push({ kind: 'calculated', claim: `${ranked.ranked.length} record(s) are open and meet the recorded outreach gates${ranked.ranked.length < 5 ? ', fewer than the five asked for' : ''}.${reasons.length ? ` Excluded: ${reasons.join('; ')}.` : ''}`,
          basis: [...ranked.ranked.map((r) => r.ref), ...(ranked.excluded || []).map((e) => e.ref)], method: 'recorded opportunity state and outreach gates' });
      }
      result.calculated.push({ kind: 'calculated', claim: `Priority by recorded engagement tier: ${ranked.ranked.map((r) => `${r.position}. ${r.ref.split(':').pop()}`).join(', ') || 'no open eligible records'}.`, basis: ranked.ranked.map((r) => r.ref), method: 'recorded engagement tier; ties share a position' });
    }
    result.inferences = result.inferences.filter((i) => i && i.certainty === 'inferred' && Array.isArray(i.basis) && i.basis.length);

    // The records this turn is about: the ranked set, else the revalidated
    // referent ids still current, else every record read.
    if (!result.subjectRecordIds.length) {
      const current = result.revalidation.filter((r) => r.kind === 'prospect' && r.status === 'current').map((r) => r.id);
      result.subjectRecordIds = current.length ? current : result.observed.filter((o) => o.subject.type === 'prospect_record').map((o) => o.subject.id);
    }
    result.recommendations = recommend(result);
    return result;
  }
}

/** Proposals only. Anything that changes state names its authorization path. */
function recommend(result) {
  const out = [];
  const lost = result.revalidation.filter((r) => r.status !== 'current');
  if (lost.length) out.push({ action: `Confirm what happened to ${lost.map((r) => r.id).join(', ')} (${[...new Set(lost.map((r) => r.status))].join(', ')}); no replacement records were assumed.`, status: 'proposal', requiresAuthorization: false });
  const missing = result.observed.filter((o) => o.missing.includes('contact channel'));
  if (missing.length) out.push({ action: `Verify contact details for ${missing.map((o) => o.subject.id).join(', ')} before any outreach.`, status: 'proposal', requiresAuthorization: true, pathway: EXECUTION_PATHWAYS.modify });
  if (result.contradictions.length) out.push({ action: 'Resolve the contradictory records listed above at their source before relying on either value.', status: 'proposal', requiresAuthorization: false });
  if (result.unknowns.some((u) => /execution log/.test(u.what || ''))) out.push({ action: 'Check the agent execution logs directly; missing logs do not show whether the agent ran.', status: 'proposal', requiresAuthorization: false });
  if (result.execution.status === 'not_executed' && result.execution.pathway) out.push({ action: `To carry out "${result.execution.requested}", use ${result.execution.pathway}; it requires explicit operator authorization.`, status: 'proposal', requiresAuthorization: true, pathway: result.execution.pathway });
  return out;
}

module.exports = { FrameDirectedReasoner, frameReasoningEnabled, selectDomains, resolveReferent, evidenceReferents, normalizeReferent, EXECUTION_PATHWAYS, RESULT_KINDS };
