'use strict';

/**
 * ConversationLedger (P0-MAX-REASONING-001 PR 3) — pure, bounded, tenant-scoped
 * record of what one operator conversation is about. Default-off; no I/O here.
 *
 * It answers "what are we still working on, what changed, what was decided,
 * what remains unresolved", and nothing more. It is deliberately NOT:
 *   - evidence: referents are ids and generic labels; record contents are
 *     never stored, and every evidence reference carries a freshness verdict;
 *   - authority: a decision entry is a memory of what was said, fixed to
 *     authority 'none'. Approvals, execution permission and mission state stay
 *     with the pending-decision resolver, AuthorizationConstraints and the
 *     execution router. Prohibitions are remembered context, not enforcement;
 *   - field-note memory: contact facts and their corrections stay in
 *     packages/max/understanding. Ledger corrections only rebind what the
 *     conversation refers to (referent / record scope).
 *
 * All operations are deterministic and return a new ledger.
 */

const { redactText } = require('../../decision-service/privacy');
const { EVIDENCE_DOMAINS, RELATIONS } = require('../interpretation/TurnFrame');

const LEDGER_VERSION = 1;
const TTL_MS = 2 * 60 * 60 * 1000; // same lifetime as the durable context store
const STALE_EVIDENCE_MS = 30 * 60 * 1000; // same window as OperationalEvidenceContinuity
const OPEN_QUESTION_TURNS = 10;
const LIMITS = Object.freeze({
  referents: 24, members: 25, focus: 8, corrections: 16, openQuestions: 8,
  constraints: 16, decisions: 12, evidenceRefs: 16, label: 80,
});

const REFERENT_KINDS = Object.freeze(['mission', 'prospect_set', 'pending_decision', 'topic']);
const SCOPES = Object.freeze(['existing', 'new']);
const SOURCES = Object.freeze(['evidence', 'mission_runtime', 'operator']);
const CONSTRAINT_KINDS = Object.freeze(['prohibition', 'preference']);
const CONSTRAINT_ACTIONS = Object.freeze(['send', 'modify', 'approve', 'cancel', 'create', 'run_agent', 'any_change']);
const PREFERENCE_VALUES = Object.freeze(['record_scope:existing', 'record_scope:new']);
const DECISION_OUTCOMES = Object.freeze(['operator_affirmed', 'operator_rejected', 'operator_held', 'operator_ambiguous']);
const OPEN_QUESTION_KINDS = Object.freeze(['clarification', 'unresolved_read', 'foreign_tenant_reference', 'unresolved_correction']);
const OPEN_QUESTION_STATUS = Object.freeze(['open', 'answered', 'expired']);
const CORRECTION_KINDS = Object.freeze(['scope', 'referent']);
const EVIDENCE_KINDS = Object.freeze([...EVIDENCE_DOMAINS, 'operational_inspection']);
const DECISION_KINDS = Object.freeze([
  'plan_clarification', 'plan_approval', 'plan_edit', 'discovery_approval',
  'prioritization_approval', 'discovery_investigation', 'execution_approval', 'unknown',
]);

const ID = /^[A-Za-z0-9_.:-]{1,160}$/;
const isId = (v) => typeof v === 'string' && ID.test(v);
const iso = (ms) => new Date(ms).toISOString();
const toMs = (now) => (now instanceof Date ? now.getTime() : Number.isFinite(now) ? now : Date.now());

class LedgerValidationError extends Error {
  constructor(field) { super(`Invalid conversation ledger: ${field}`); this.code = 'invalid_conversation_ledger'; this.field = field; }
}

function safeLabel(value) {
  // Generic, adapter-generated labels; redaction is defence in depth.
  return redactText(String(value || ''), LIMITS.label).replace(/[^\w .,'():/-]/g, '').slice(0, LIMITS.label);
}

function createLedger({ tenantId, sessionId, now = Date.now(), expiredPredecessor = false } = {}) {
  if (!isId(String(tenantId ?? '')) || !isId(String(sessionId ?? ''))) throw new LedgerValidationError('scope');
  const t = toMs(now);
  return {
    version: LEDGER_VERSION, tenantId: String(tenantId), sessionId: String(sessionId),
    createdAt: iso(t), updatedAt: iso(t), turn: 0, nextId: 1,
    referents: [], focus: [], corrections: [], openQuestions: [], constraints: [],
    decisions: [], evidenceRefs: [], lastFrame: null, lastOwner: null, lastResolution: null,
    recovered: null, expiredPredecessor: Boolean(expiredPredecessor),
  };
}

const clone = (l) => JSON.parse(JSON.stringify(l));
function isExpired(ledger, now = Date.now()) {
  return !ledger || toMs(now) - Date.parse(ledger.updatedAt) > TTL_MS;
}

const active = (l, id) => l.referents.find((r) => r.id === id && r.status === 'active') || null;
const focusTop = (l) => (l.focus.length ? active(l, l.focus[0]) : null);

function pushFocus(l, id) {
  l.focus = [id, ...l.focus.filter((x) => x !== id)].filter((x) => active(l, x)).slice(0, LIMITS.focus);
}

function identityOf(r) {
  if (r.kind === 'mission') return `mission:${r.missionId}`;
  if (r.kind === 'pending_decision') return `pd:${r.missionId}:${r.decisionKind}`;
  if (r.kind === 'prospect_set') return `ps:${r.scope || '-'}:${[...(r.members || [])].sort().join(',')}`;
  return `topic:${r.label}`;
}

function sanitizeIntroduced(x, nowIso) {
  if (!x || !REFERENT_KINDS.includes(x.kind)) return null;
  const r = {
    kind: x.kind,
    label: safeLabel(x.label || x.kind.replace(/_/g, ' ')),
    scope: SCOPES.includes(x.scope) ? x.scope : null,
    source: SOURCES.includes(x.source) ? x.source : 'evidence',
    members: Array.isArray(x.members) ? [...new Set(x.members.map(String).filter(isId))].slice(0, LIMITS.members) : [],
    missionId: isId(x.missionId) ? x.missionId : null,
    decisionKind: DECISION_KINDS.includes(x.decisionKind) ? x.decisionKind : (x.kind === 'pending_decision' ? 'unknown' : null),
    observedAt: Number.isFinite(Date.parse(x.observedAt)) ? x.observedAt : nowIso,
  };
  if ((r.kind === 'mission' || r.kind === 'pending_decision') && !r.missionId) return null;
  return r;
}

function addReferent(l, r) {
  const id = `r${l.nextId++}`;
  l.referents.push({
    id, ...r, introducedTurn: l.turn, lastTurn: l.turn, status: 'active',
    supersededBy: null, requiresRevalidation: false, pendingEvidence: false,
  });
  return id;
}

/** Merge a referent the turn's production result showed. Returns its id. */
function upsertIntroduced(l, r) {
  const top = focusTop(l);
  // A correction left an empty replacement waiting for evidence: fill it.
  if (top && top.pendingEvidence && top.kind === r.kind && (top.scope || null) === (r.scope || null)) {
    Object.assign(top, { members: r.members, observedAt: r.observedAt, source: r.source, label: r.label, lastTurn: l.turn, pendingEvidence: false });
    return top.id;
  }
  if (r.kind === 'pending_decision') {
    // Mission state, not a correction: only the current gate stays active.
    for (const x of l.referents) {
      if (x.status === 'active' && x.kind === 'pending_decision' && x.missionId === r.missionId && x.decisionKind !== r.decisionKind) x.status = 'superseded';
    }
  }
  const key = identityOf(r);
  const existing = l.referents.find((x) => x.status === 'active' && identityOf(x) === key);
  if (existing) {
    Object.assign(existing, { lastTurn: l.turn, observedAt: r.observedAt, requiresRevalidation: false, source: r.source });
    return existing.id;
  }
  return addReferent(l, r);
}

function addOpenQuestion(l, kind, about) {
  if (l.openQuestions.some((q) => q.status === 'open' && q.kind === kind && q.about === about)) return;
  l.openQuestions.push({ id: `q${l.nextId++}`, kind, about: about || null, askedTurn: l.turn, status: 'open' });
}

function addConstraint(l, c) {
  const key = (x) => `${x.kind}|${x.action || ''}|${x.value || ''}`;
  const existing = l.constraints.find((x) => key(x) === key(c));
  if (existing) Object.assign(existing, { turn: l.turn, at: c.at, scope: c.scope });
  else l.constraints.push({ id: `c${l.nextId++}`, ...c });
}

function bound(l) {
  // Referents: never evict focus; evict superseded first, then least recent.
  if (l.referents.length > LIMITS.referents) {
    const keep = new Set(l.focus);
    const evictable = l.referents.filter((r) => !keep.has(r.id))
      .sort((a, b) => (a.status === 'superseded' ? 0 : 1) - (b.status === 'superseded' ? 0 : 1) || a.lastTurn - b.lastTurn);
    const drop = new Set(evictable.slice(0, l.referents.length - LIMITS.referents).map((r) => r.id));
    l.referents = l.referents.filter((r) => !drop.has(r.id));
  }
  const tail = (xs, n) => xs.slice(Math.max(0, xs.length - n));
  l.corrections = tail(l.corrections, LIMITS.corrections);
  l.constraints = tail(l.constraints, LIMITS.constraints);
  l.decisions = tail(l.decisions, LIMITS.decisions);
  l.evidenceRefs = tail(l.evidenceRefs, LIMITS.evidenceRefs);
  // Open questions: drop closed ones first.
  while (l.openQuestions.length > LIMITS.openQuestions) {
    const idx = l.openQuestions.findIndex((q) => q.status !== 'open');
    l.openQuestions.splice(idx >= 0 ? idx : 0, 1);
  }
  l.focus = l.focus.filter((id) => active(l, id)).slice(0, LIMITS.focus);
  return l;
}

function compactFrame(frame, nowIso) {
  if (!frame || frame.authority?.grantsAuthority !== false || typeof frame.intent !== 'string') return null;
  return {
    producer: String(frame.producer || 'lexical'), intent: frame.intent, subjectKind: frame.subjectKind,
    relationToPrior: frame.relationToPrior, clarificationRequired: Boolean(frame.clarificationRequired),
    action: { verb: frame.requestedAction?.verb || 'none', polarity: frame.requestedAction?.polarity || 'none' },
    // Recorded for audit only; a remembered frame never nominates anything.
    executionCandidate: false, at: nowIso,
  };
}

/**
 * Relation precedence: a model frame's relation, then the ledger-only
 * discourse signal (degraded path), then the lexical frame.
 */
function relationFor(turn) {
  const f = turn.frame;
  if (f && f.producer === 'model' && RELATIONS.includes(f.relationToPrior) && f.relationToPrior !== 'none') return f.relationToPrior;
  if (turn.discourse && RELATIONS.includes(turn.discourse.relation) && turn.discourse.relation !== 'none') return turn.discourse.relation;
  if (f && RELATIONS.includes(f.relationToPrior)) return f.relationToPrior;
  return 'none';
}

/**
 * @param {object} ledger
 * @param {{
 *   owner?: string|null, frame?: object|null,
 *   discourse?: { relation?: string, scope?: 'existing'|'new'|null, topicSwitch?: boolean, returnTo?: boolean, kindHint?: string|null },
 *   introduced?: object[], prohibitions?: string[],
 *   decision?: { outcome: string, decisionKind?: string, missionId?: string }|null,
 *   evidence?: { kind: string, observedAt?: string, introducedIndex?: number }[],
 *   clarificationAsked?: boolean, unresolvedRead?: boolean, foreignTenant?: boolean,
 * }} turn  produced by LedgerTurnAdapter from the turn's own production result
 */
function applyTurn(ledger, turn = {}, now = Date.now()) {
  const l = clone(ledger);
  const t = toMs(now);
  const nowIso = iso(t);
  l.turn += 1;
  l.updatedAt = nowIso;
  // Owner transitions are recorded but never clear the subject.
  l.lastOwner = typeof turn.owner === 'string' ? turn.owner.slice(0, 64) : null;
  const relation = relationFor(turn);
  const discourse = turn.discourse || {};
  const priorTop = focusTop(l);

  // Close what this turn answers before anything new opens.
  for (const q of l.openQuestions) {
    if (q.status !== 'open') continue;
    if (q.kind === 'clarification' && ['clarification_answer', 'correction', 'refinement'].includes(relation)) q.status = 'answered';
    else if (l.turn - q.askedTurn > OPEN_QUESTION_TURNS) q.status = 'expired';
  }

  // 1. Corrections rebind the focused referent, deterministically.
  if (relation === 'correction' || relation === 'refinement') {
    const target = priorTop;
    const frameTarget = (turn.frame?.referents || []).find((r) => r.role === 'primary' && active(l, r.candidateId));
    if (target && discourse.scope && SCOPES.includes(discourse.scope) && (target.scope || null) !== discourse.scope
        && (target.kind === 'prospect_set' || target.kind === 'topic')) {
      target.status = 'superseded';
      const replacement = addReferent(l, {
        kind: target.kind, label: safeLabel(`${discourse.scope} ${target.kind === 'prospect_set' ? 'prospect records' : 'topic'}`),
        scope: discourse.scope, source: 'operator', members: [], missionId: null, decisionKind: null, observedAt: nowIso,
      });
      const rep = active(l, replacement);
      rep.pendingEvidence = true;
      target.supersededBy = replacement;
      l.focus = l.focus.map((id) => (id === target.id ? replacement : id));
      l.corrections.push({ turn: l.turn, at: nowIso, kind: 'scope', fromId: target.id, toId: replacement, before: target.scope || null, after: discourse.scope });
      addConstraint(l, { kind: 'preference', action: null, value: `record_scope:${discourse.scope}`, scope: replacement, turn: l.turn, at: nowIso });
    } else if (frameTarget && (!target || frameTarget.candidateId !== target.id)) {
      l.corrections.push({ turn: l.turn, at: nowIso, kind: 'referent', fromId: target ? target.id : null, toId: frameTarget.candidateId, before: null, after: null });
      pushFocus(l, frameTarget.candidateId);
      if (target) l.focus = l.focus.filter((id) => id !== target.id);
    } else if (relation === 'correction') {
      // Known to be a correction, but not of what: ask, never guess.
      addOpenQuestion(l, 'unresolved_correction', target ? target.id : null);
    }
    for (const r of turn.frame?.referents || []) if (r.role === 'excluded') l.focus = l.focus.filter((id) => id !== r.candidateId);
  }

  // 2. Explicit return to an earlier subject.
  if (discourse.returnTo) {
    const want = REFERENT_KINDS.includes(discourse.kindHint) ? discourse.kindHint : null;
    const candidate = l.focus.slice(1).map((id) => active(l, id)).find((r) => r && (!want || r.kind === want));
    if (candidate) pushFocus(l, candidate.id);
  }

  // 3. Referents this turn's production result showed (tenant-checked upstream).
  const introducedIds = [];
  if (!turn.foreignTenant) {
    for (const raw of turn.introduced || []) {
      const r = sanitizeIntroduced(raw, nowIso);
      if (!r) { introducedIds.push(null); continue; }
      const id = upsertIntroduced(l, r);
      introducedIds.push(id);
    }
    // What Max just showed becomes the subject, unless the operator explicitly
    // returned to something else this turn. Missions and decisions provide
    // context but do not displace a record-level subject on a follow-up.
    const shown = introducedIds.filter(Boolean).map((id) => active(l, id)).filter(Boolean);
    const primary = shown.find((r) => r.kind === 'prospect_set') || shown.find((r) => r.kind === 'mission') || shown[0];
    if (primary && !discourse.returnTo) {
      // Records Max just showed become the subject. A session-bound mission or
      // its gate is ambient context, re-reported every turn: it takes focus
      // only when nothing is in focus, on an explicit topic switch, or when
      // the operator is talking about missions.
      const ambient = primary.kind === 'mission' || primary.kind === 'pending_decision';
      const displace = primary.kind === 'prospect_set' || !priorTop
        || (ambient ? relation === 'new_topic' || discourse.kindHint === 'mission' : relation !== 'follow_up');
      if (displace) pushFocus(l, primary.id);
      else if (!l.focus.includes(primary.id)) l.focus = [l.focus[0], primary.id, ...l.focus.slice(1)].slice(0, LIMITS.focus);
    }
  } else {
    addOpenQuestion(l, 'foreign_tenant_reference', priorTop ? priorTop.id : null);
  }

  // 4. Follow-ups resolve to the current subject.
  const top = focusTop(l);
  if (relation === 'follow_up' || relation === 'clarification_answer') {
    l.lastResolution = top ? { turn: l.turn, referentId: top.id } : null;
    if (!top) addOpenQuestion(l, 'clarification', null);
  }

  // 5. Prohibitions and decisions: remembered, never enforced or authorized here.
  for (const action of turn.prohibitions || []) {
    if (CONSTRAINT_ACTIONS.includes(action)) addConstraint(l, { kind: 'prohibition', action, value: null, scope: top ? top.id : null, turn: l.turn, at: nowIso });
  }
  const d = turn.decision;
  if (d && DECISION_OUTCOMES.includes(d.outcome)) {
    l.decisions.push({
      id: `d${l.nextId++}`, decisionKind: DECISION_KINDS.includes(d.decisionKind) ? d.decisionKind : 'unknown',
      missionId: isId(d.missionId) ? d.missionId : null, outcome: d.outcome, turn: l.turn, at: nowIso, authority: 'none',
    });
  }

  // 6. Evidence references (timestamps only) and open questions.
  for (const e of turn.evidence || []) {
    if (!EVIDENCE_KINDS.includes(e.kind)) continue;
    const observedAt = Number.isFinite(Date.parse(e.observedAt)) ? e.observedAt : nowIso;
    const referentId = Number.isInteger(e.introducedIndex) ? introducedIds[e.introducedIndex] || null : (top ? top.id : null);
    l.evidenceRefs.push({ kind: e.kind, referentId, observedAt, recordedTurn: l.turn });
  }
  if (turn.clarificationAsked) addOpenQuestion(l, 'clarification', top ? top.id : null);
  if (turn.unresolvedRead) addOpenQuestion(l, 'unresolved_read', top ? top.id : null);

  l.lastFrame = compactFrame(turn.frame, nowIso);
  return bound(l);
}

function evidenceFreshness(l, referentId, t) {
  const refs = l.evidenceRefs.filter((e) => e.referentId === referentId);
  if (!refs.length) return { latestObservedAt: null, fresh: false };
  const latest = refs.map((e) => Date.parse(e.observedAt)).reduce((a, b) => Math.max(a, b));
  return { latestObservedAt: iso(latest), fresh: !l.recovered && t - latest <= STALE_EVIDENCE_MS };
}

/** What "it / those / which of them" refers to now. Never evidence, never authority. */
function resolveFocus(ledger, now = Date.now()) {
  if (!ledger || isExpired(ledger, now)) return { referent: null, reason: 'no_current_context' };
  const top = focusTop(ledger);
  if (!top) return { referent: null, reason: 'no_focus' };
  const fresh = evidenceFreshness(ledger, top.id, toMs(now));
  return {
    referent: { id: top.id, kind: top.kind, label: top.label, scope: top.scope, memberIds: [...top.members], missionId: top.missionId },
    remembered: true,
    verified: false,
    requiresRevalidation: Boolean(top.requiresRevalidation) || Boolean(top.pendingEvidence),
    evidenceRefreshRequired: !fresh.fresh,
    latestEvidenceAt: fresh.latestObservedAt,
  };
}

/**
 * The three-way distinction consumers must keep: remembered context,
 * evidence references (with freshness), and authority (always none).
 */
function viewLedger(ledger, now = Date.now()) {
  const t = toMs(now);
  if (!ledger || isExpired(ledger, t)) return null;
  return {
    tenantId: ledger.tenantId,
    remembered: {
      focus: ledger.focus.map((id) => active(ledger, id)).filter(Boolean).map((r) => ({ id: r.id, kind: r.kind, label: r.label, scope: r.scope, members: r.members.length })),
      corrections: ledger.corrections.map((c) => ({ ...c })),
      constraints: ledger.constraints.map((c) => ({ ...c, enforcedBy: 'not_this_ledger' })),
      decisions: ledger.decisions.map((d) => ({ ...d })),
      openQuestions: ledger.openQuestions.filter((q) => q.status === 'open').map((q) => ({ ...q })),
      lastOwner: ledger.lastOwner,
      recovered: ledger.recovered ? { ...ledger.recovered } : null,
    },
    evidence: ledger.evidenceRefs.map((e) => ({ ...e, fresh: !ledger.recovered && t - Date.parse(e.observedAt) <= STALE_EVIDENCE_MS, mustReread: true })),
    authority: { grantsAuthority: false, executionPermitted: false, consumableApprovals: [], source: 'mission runtime and pending-decision resolver only' },
  };
}

// ---------------------------------------------------------------------------
// Validation, persistence projection and recovery
// ---------------------------------------------------------------------------

const KEYS = ['version', 'tenantId', 'sessionId', 'createdAt', 'updatedAt', 'turn', 'nextId', 'referents', 'focus', 'corrections',
  'openQuestions', 'constraints', 'decisions', 'evidenceRefs', 'lastFrame', 'lastOwner', 'lastResolution', 'recovered', 'expiredPredecessor'];
const REFERENT_KEYS = ['id', 'kind', 'label', 'scope', 'source', 'members', 'missionId', 'decisionKind', 'observedAt', 'introducedTurn',
  'lastTurn', 'status', 'supersededBy', 'requiresRevalidation', 'pendingEvidence'];

function need(cond, field) { if (!cond) throw new LedgerValidationError(field); }
const isIso = (v) => typeof v === 'string' && Number.isFinite(Date.parse(v));
const exactKeys = (o, keys, field) => {
  need(o && typeof o === 'object' && !Array.isArray(o), field);
  for (const k of Object.keys(o)) need(keys.includes(k), `${field}.unexpected`);
  for (const k of keys) need(k in o, `${field}.${k}`);
};
const intIn = (v, lo, hi) => Number.isInteger(v) && v >= lo && v <= hi;

/** Strict structural validation. Anything unexpected — including any authority — is rejected. */
function validateLedger(value) {
  exactKeys(value, KEYS, 'ledger');
  need(value.version === LEDGER_VERSION, 'version');
  need(isId(value.tenantId) && isId(value.sessionId), 'scope');
  need(isIso(value.createdAt) && isIso(value.updatedAt), 'timestamps');
  need(intIn(value.turn, 0, 1e6) && intIn(value.nextId, 1, 1e7), 'counters');
  need(Array.isArray(value.referents) && value.referents.length <= LIMITS.referents, 'referents');
  const ids = new Set();
  for (const r of value.referents) {
    exactKeys(r, REFERENT_KEYS, 'referent');
    need(isId(r.id) && !ids.has(r.id), 'referent.id'); ids.add(r.id);
    need(REFERENT_KINDS.includes(r.kind) && typeof r.label === 'string' && r.label.length <= LIMITS.label && r.label === safeLabel(r.label), 'referent.kind');
    need(r.scope === null || SCOPES.includes(r.scope), 'referent.scope');
    need(SOURCES.includes(r.source), 'referent.source');
    need(Array.isArray(r.members) && r.members.length <= LIMITS.members && r.members.every(isId), 'referent.members');
    need(r.missionId === null || isId(r.missionId), 'referent.missionId');
    need(r.decisionKind === null || DECISION_KINDS.includes(r.decisionKind), 'referent.decisionKind');
    need(isIso(r.observedAt) && intIn(r.introducedTurn, 0, 1e6) && intIn(r.lastTurn, 0, 1e6), 'referent.turns');
    need(['active', 'superseded'].includes(r.status) && (r.supersededBy === null || isId(r.supersededBy)), 'referent.status');
    need(typeof r.requiresRevalidation === 'boolean' && typeof r.pendingEvidence === 'boolean', 'referent.flags');
  }
  need(Array.isArray(value.focus) && value.focus.length <= LIMITS.focus && value.focus.every((id) => ids.has(id)), 'focus');
  need(Array.isArray(value.corrections) && value.corrections.length <= LIMITS.corrections, 'corrections');
  for (const c of value.corrections) {
    exactKeys(c, ['turn', 'at', 'kind', 'fromId', 'toId', 'before', 'after'], 'correction');
    need(CORRECTION_KINDS.includes(c.kind) && isIso(c.at) && (c.fromId === null || isId(c.fromId)) && isId(c.toId), 'correction.kind');
    need([c.before, c.after].every((v) => v === null || SCOPES.includes(v)), 'correction.scope');
  }
  need(Array.isArray(value.openQuestions) && value.openQuestions.length <= LIMITS.openQuestions, 'openQuestions');
  for (const q of value.openQuestions) {
    exactKeys(q, ['id', 'kind', 'about', 'askedTurn', 'status'], 'openQuestion');
    need(isId(q.id) && OPEN_QUESTION_KINDS.includes(q.kind) && OPEN_QUESTION_STATUS.includes(q.status) && (q.about === null || isId(q.about)), 'openQuestion.kind');
  }
  need(Array.isArray(value.constraints) && value.constraints.length <= LIMITS.constraints, 'constraints');
  for (const c of value.constraints) {
    exactKeys(c, ['id', 'kind', 'action', 'value', 'scope', 'turn', 'at'], 'constraint');
    need(isId(c.id) && CONSTRAINT_KINDS.includes(c.kind) && isIso(c.at) && (c.scope === null || isId(c.scope)), 'constraint.kind');
    need(c.kind === 'prohibition' ? CONSTRAINT_ACTIONS.includes(c.action) && c.value === null : c.action === null && PREFERENCE_VALUES.includes(c.value), 'constraint.value');
  }
  need(Array.isArray(value.decisions) && value.decisions.length <= LIMITS.decisions, 'decisions');
  for (const d of value.decisions) {
    exactKeys(d, ['id', 'decisionKind', 'missionId', 'outcome', 'turn', 'at', 'authority'], 'decision');
    need(isId(d.id) && DECISION_KINDS.includes(d.decisionKind) && DECISION_OUTCOMES.includes(d.outcome) && isIso(d.at), 'decision.kind');
    need(d.authority === 'none', 'decision.authority');
    need(d.missionId === null || isId(d.missionId), 'decision.missionId');
  }
  need(Array.isArray(value.evidenceRefs) && value.evidenceRefs.length <= LIMITS.evidenceRefs, 'evidenceRefs');
  for (const e of value.evidenceRefs) {
    exactKeys(e, ['kind', 'referentId', 'observedAt', 'recordedTurn'], 'evidenceRef');
    need(EVIDENCE_KINDS.includes(e.kind) && isIso(e.observedAt) && (e.referentId === null || isId(e.referentId)), 'evidenceRef.kind');
  }
  if (value.lastFrame !== null) {
    exactKeys(value.lastFrame, ['producer', 'intent', 'subjectKind', 'relationToPrior', 'clarificationRequired', 'action', 'executionCandidate', 'at'], 'lastFrame');
    need(value.lastFrame.executionCandidate === false, 'lastFrame.executionCandidate');
  }
  need(value.lastOwner === null || (typeof value.lastOwner === 'string' && value.lastOwner.length <= 64), 'lastOwner');
  if (value.lastResolution !== null) {
    exactKeys(value.lastResolution, ['turn', 'referentId'], 'lastResolution');
    need(isId(value.lastResolution.referentId), 'lastResolution.referentId');
  }
  if (value.recovered !== null) {
    exactKeys(value.recovered, ['at', 'droppedReferents'], 'recovered');
    need(isIso(value.recovered.at) && intIn(value.recovered.droppedReferents, 0, 1e3), 'recovered.at');
  }
  need(typeof value.expiredPredecessor === 'boolean', 'expiredPredecessor');
  return clone(value);
}

const PERSIST_BUDGET_BYTES = 12000; // leaves room inside the store's 16 KiB row cap

/** A bounded, validated copy that always fits the durable store's budget. */
function projectLedger(ledger) {
  const l = validateLedger(ledger);
  const size = () => Buffer.byteLength(JSON.stringify(l));
  const shrink = [
    () => l.evidenceRefs.length && l.evidenceRefs.shift(),
    () => l.corrections.length && l.corrections.shift(),
    () => l.openQuestions.length && l.openQuestions.shift(),
    () => l.decisions.length && l.decisions.shift(),
    () => {
      const idx = l.referents.findIndex((r) => !l.focus.includes(r.id));
      return idx >= 0 && l.referents.splice(idx, 1);
    },
  ];
  for (const step of shrink) while (size() > PERSIST_BUDGET_BYTES && step());
  l.focus = l.focus.filter((id) => l.referents.some((r) => r.id === id));
  need(size() <= PERSIST_BUDGET_BYTES, 'budget');
  return validateLedger(l);
}

/**
 * Restore a persisted ledger for the same authenticated tenant and session.
 * Fails closed (returns null) on any mismatch, expiry or corruption, so a
 * failed recovery never yields referents. A recovered ledger:
 *   - drops mission referents the tenant's records no longer confirm;
 *   - marks every other referent requiresRevalidation (ids are hints);
 *   - treats all evidence as stale (must be re-read);
 *   - keeps decisions as history with authority 'none' (validated).
 */
function recoverLedger(persisted, { tenantId, sessionId, now = Date.now(), validMissionIds = null } = {}) {
  let l;
  try { l = validateLedger(persisted); } catch (_) { return null; }
  if (l.tenantId !== String(tenantId) || l.sessionId !== String(sessionId)) return null;
  const t = toMs(now);
  if (isExpired(l, t)) return null;
  const confirmed = validMissionIds ? new Set(validMissionIds.map(String)) : null;
  const before = l.referents.length;
  l.referents = l.referents.filter((r) => !(confirmed && (r.kind === 'mission' || r.kind === 'pending_decision') && !confirmed.has(r.missionId)));
  for (const r of l.referents) r.requiresRevalidation = true;
  const kept = new Set(l.referents.map((r) => r.id));
  l.focus = l.focus.filter((id) => kept.has(id));
  l.evidenceRefs = l.evidenceRefs.filter((e) => e.referentId === null || kept.has(e.referentId));
  l.lastResolution = l.lastResolution && kept.has(l.lastResolution.referentId) ? l.lastResolution : null;
  l.recovered = { at: iso(t), droppedReferents: before - l.referents.length };
  return validateLedger(l);
}

module.exports = {
  LEDGER_VERSION, TTL_MS, STALE_EVIDENCE_MS, LIMITS, PERSIST_BUDGET_BYTES,
  REFERENT_KINDS, CONSTRAINT_ACTIONS, DECISION_OUTCOMES, EVIDENCE_KINDS,
  LedgerValidationError,
  createLedger, applyTurn, isExpired, resolveFocus, viewLedger,
  validateLedger, projectLedger, recoverLedger,
};
