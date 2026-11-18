'use strict';

/**
 * P0-MAX-REASONING-001 PR 3 — ConversationLedger (hermetic).
 * Drives the recorder + adapter with result shapes production emits
 * (record-intelligence items, Scout discovery, refusals, pending-decision
 * resolution) and checks focus, corrections, topic changes, tenant
 * boundaries, bounds, privacy, expiry and the authority boundary.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const L = require('../ConversationLedger');
const { ConversationLedgerRecorder } = require('../ConversationLedgerRecorder');
const { validateTurnFrame } = require('../../interpretation/TurnFrame');

const LEVI = '21';
const ANCHOR = '10';
const T0 = Date.parse('2026-11-18T10:00:00.000Z');
const ON = { MAX_CONVERSATION_LEDGER: 'true' };

function clock(start = T0) {
  let now = start;
  return { now: () => now, advance: (ms) => { now += ms; } };
}
const session = (tenantId, id = `s-${tenantId}`) => ({ id, context: { tenantId } });
const owner = (o) => ({ workspaceOwnership: { owner: o }, structured: { metadata: {} } });
function prospects(tenantId, ids, extra = {}) {
  return {
    workspaceOwnership: { owner: 'knowledge_retrieval' },
    structured: { metadata: { recordIntelligence: true, retrievalStatus: 'found',
      items: ids.map((id) => ({ recordRef: `prospect:${tenantId}:${id}`, recordId: id, sourceKind: 'prospects', epistemic: 'verified',
        claim: `Jane Borrower [prospect:${tenantId}:${id}]; recorded notes: private financing detail`, retrievalStatus: 'found' })), ...extra } },
  };
}
const scout = () => ({ workspaceOwnership: { owner: 'specialist_scout' }, structured: { metadata: { launchedScout: true } } });
const FIVE = ['p101', 'p102', 'p103', 'p104', 'p105'];

function recorder(c = clock(), extra = {}) {
  return new ConversationLedgerRecorder({ env: ON, now: c.now, ...extra });
}
function turn(rec, s, text, result) {
  rec.observe({ session: s, text, result });
  return s.conversationLedger;
}
const focus = (s, rec) => rec.context(s).focus;

describe('default-off', () => {
  it('does nothing without MAX_CONVERSATION_LEDGER', () => {
    const s = session(LEVI);
    const before = JSON.stringify(s);
    assert.equal(new ConversationLedgerRecorder({ env: {} }).observe({ session: s, text: 'Show me prospects', result: prospects(LEVI, FIVE) }), null);
    assert.equal(JSON.stringify(s), before);
    assert.equal(new ConversationLedgerRecorder({ env: {} }).context(s), null);
  });
});

describe('follow-ups across owner transitions', () => {
  it('"five strongest" then "which has the freshest information?" resolves to the same five records', () => {
    const c = clock(); const rec = recorder(c); const s = session(LEVI);
    turn(rec, s, "Show me Levi's five strongest prospects.", prospects(LEVI, FIVE));
    turn(rec, s, 'Which has the freshest information?', owner('reasoning'));
    const f = focus(s, rec);
    assert.equal(f.referent.kind, 'prospect_set');
    assert.deepEqual(f.referent.memberIds, FIVE);
    assert.equal(f.verified, false, 'remembered, not verified evidence');
    assert.equal(f.evidenceRefreshRequired, false, 'records were read this conversation, within the freshness window');
    assert.equal(s.conversationLedger.lastResolution.referentId, f.referent.id);
    assert.equal(s.conversationLedger.lastOwner, 'reasoning');
    c.advance(31 * 60 * 1000);
    assert.equal(focus(s, rec).evidenceRefreshRequired, true, 'old evidence must be re-read');
  });

  it('a session-bound mission is context: it does not steal the subject from records just shown', () => {
    const rec = recorder(); const s = session(LEVI);
    turn(rec, s, "Show me Levi's five strongest prospects.", prospects(LEVI, FIVE));
    s.context.acquisitionMissionId = 'm_a';
    turn(rec, s, 'Approved.', { ...owner('active_mission'), resolution: { action: 'plan_approved' }, routingTrace: {} });
    s.context.acquisitionMissionId = 'm_b';
    turn(rec, s, "Don't send anything yet. Which of those has the freshest information?", owner('reasoning'));
    assert.deepEqual(focus(s, rec).referent.memberIds, FIVE);
    assert.ok(s.conversationLedger.focus.length >= 2, 'the missions are still remembered as context');
  });

  it('turns owned by other components never erase the subject', () => {
    const rec = recorder(); const s = session(LEVI);
    turn(rec, s, 'Show me the five strongest prospects.', prospects(LEVI, FIVE));
    for (const o of ['conversation_layer', 'session_state_manager', 'reflection', 'specialist_paige', 'mission_inspection']) {
      turn(rec, s, 'Okay, and what else should I know?', owner(o));
    }
    assert.deepEqual(focus(s, rec).referent.memberIds, FIVE);
  });
});

describe('corrections', () => {
  it('"No, I meant the existing prospects" replaces the mistaken binding, once, and the next read fills it', () => {
    const rec = recorder(); const s = session(LEVI);
    turn(rec, s, "Show me Levi's five strongest prospects.", scout());
    const mistaken = focus(s, rec).referent;
    assert.equal(mistaken.scope, 'new');
    turn(rec, s, 'No, I meant the existing prospects.', owner('reasoning'));
    const l = s.conversationLedger;
    const old = l.referents.find((r) => r.id === mistaken.id);
    assert.equal(old.status, 'superseded');
    const f = focus(s, rec);
    assert.equal(f.referent.scope, 'existing');
    assert.equal(f.requiresRevalidation, true, 'no records yet: nothing is claimed about them');
    assert.equal(old.supersededBy, f.referent.id);
    assert.deepEqual(l.corrections.map((c) => [c.kind, c.before, c.after]), [['scope', 'new', 'existing']]);
    assert.ok(l.constraints.some((c) => c.kind === 'preference' && c.value === 'record_scope:existing'));
    assert.ok(!l.focus.includes(mistaken.id));

    turn(rec, s, 'Go ahead and show them.', prospects(LEVI, FIVE));
    const filled = focus(s, rec);
    assert.equal(filled.referent.id, f.referent.id, 'the replacement is filled, not duplicated');
    assert.deepEqual(filled.referent.memberIds, FIVE);
    assert.equal(filled.requiresRevalidation, false);
    assert.equal(s.conversationLedger.referents.filter((r) => r.kind === 'prospect_set' && r.status === 'active').length, 1);
    assert.equal(s.conversationLedger.corrections.length, 1);
  });

  it('a correction to the scope the subject already has is a confirmation, not an open question', () => {
    const rec = recorder(); const s = session(LEVI);
    turn(rec, s, "Show me Levi's five strongest existing prospects.", prospects(LEVI, FIVE));
    const before = focus(s, rec).referent;
    turn(rec, s, 'No, I meant the existing prospects, not new leads.', owner('reasoning'));
    const l = s.conversationLedger;
    assert.equal(focus(s, rec).referent.id, before.id, 'nothing rebound');
    assert.equal(l.corrections.length, 0);
    assert.ok(!l.openQuestions.some((q) => q.kind === 'unresolved_correction'));
    assert.ok(l.constraints.some((c) => c.kind === 'preference' && c.value === 'record_scope:existing'));
  });

  it('a correction that does not say what to change opens a question instead of guessing', () => {
    const rec = recorder(); const s = session(LEVI);
    turn(rec, s, 'Show me the strongest prospects.', prospects(LEVI, FIVE));
    turn(rec, s, "That's not what I meant.", owner('conversation_layer'));
    const l = s.conversationLedger;
    assert.deepEqual(focus(s, rec).referent.memberIds, FIVE);
    assert.equal(l.corrections.length, 0);
    assert.ok(l.openQuestions.some((q) => q.kind === 'unresolved_correction' && q.status === 'open'));
  });

  it('a model frame relation outranks the ledger discourse fallback, both ways', () => {
    const modelFrame = (relation) => ({ text, context }) => validateTurnFrame({
      intent: 'question', subjectKind: 'prospecting', relationToPrior: relation, referents: [], evidenceNeeds: [], facets: [],
      requestedOutcome: 'explanation', requestedAction: { verb: 'none', targetCandidateId: null, polarity: 'none' },
      scope: 'focused', foreignTenantReference: false, ambiguities: [], confidence: 0.9,
    }, { ...context, producer: 'model' });
    // Model: follow-up, although the words look like a correction.
    const a = session(LEVI);
    const ra = recorder(clock(), { frameProducer: modelFrame('follow_up') });
    turn(ra, a, 'Show me five strongest prospects.', scout());
    turn(ra, a, 'No, I meant the existing prospects.', owner('reasoning'));
    assert.equal(a.conversationLedger.corrections.length, 0);
    // Model: correction, although the words carry no marker.
    const b = session(LEVI);
    const rb = recorder(clock(), { frameProducer: modelFrame('correction') });
    turn(rb, b, 'Show me five strongest prospects.', scout());
    turn(rb, b, 'Rather the existing records.', owner('reasoning'));
    assert.equal(b.conversationLedger.corrections.length, 1);
  });
});

describe('topic changes and tenant boundaries', () => {
  it('a topic switch keeps the earlier subject on the stack and "back to" restores it', () => {
    const rec = recorder(); const s = session(LEVI);
    turn(rec, s, 'Show me the five strongest prospects.', prospects(LEVI, FIVE));
    s.context.acquisitionMissionId = 'm_crm';
    turn(rec, s, "Let's look at the CRM preparation mission.", owner('mission_inspection'));
    assert.equal(focus(s, rec).referent.kind, 'mission');
    delete s.context.acquisitionMissionId;
    turn(rec, s, 'Back to the prospects.', owner('reasoning'));
    assert.deepEqual(focus(s, rec).referent.memberIds, FIVE);
    assert.ok(s.conversationLedger.focus.length >= 2);
  });

  it('switching to Anchor Cleaning and back: each tenant keeps only its own context', () => {
    const rec = recorder();
    const levi = session(LEVI, 'levi-session');
    const anchor = session(ANCHOR, 'anchor-session');
    turn(rec, levi, "Show me Levi's five strongest prospects.", prospects(LEVI, FIVE));
    turn(rec, anchor, 'Show me the facility-manager prospects.', prospects(ANCHOR, ['a1', 'a2']));
    turn(rec, levi, 'Back to Levi — which of those has the freshest information?', owner('reasoning'));
    assert.deepEqual(focus(levi, rec).referent.memberIds, FIVE);
    assert.deepEqual(focus(anchor, rec).referent.memberIds, ['a1', 'a2']);
    assert.doesNotMatch(JSON.stringify(levi.conversationLedger), /\ba1\b|\ba2\b/);
    assert.doesNotMatch(JSON.stringify(anchor.conversationLedger), /p10[1-5]/);
  });

  it('another tenant\'s records or a refused cross-tenant request never enter the ledger', () => {
    const rec = recorder(); const s = session(LEVI);
    turn(rec, s, 'Show me the five strongest prospects.', prospects(LEVI, FIVE));
    turn(rec, s, "Use Anchor's prospects to update Levi's pipeline.", prospects(ANCHOR, ['a1']));
    turn(rec, s, "Show me Anchor Cleaning's status.", { workspaceOwnership: { owner: 'operational_inspection' }, structured: { metadata: { operationalInspectionRefused: 'tenant_mismatch' } } });
    const l = s.conversationLedger;
    assert.deepEqual(focus(s, rec).referent.memberIds, FIVE);
    assert.doesNotMatch(JSON.stringify(l), /\ba1\b/);
    assert.ok(l.openQuestions.some((q) => q.kind === 'foreign_tenant_reference' && q.status === 'open'));
  });

  it('a ledger carried on a session for another tenant or session is never reused', () => {
    const rec = recorder(); const s = session(LEVI, 's1');
    turn(rec, s, 'Show me the five strongest prospects.', prospects(LEVI, FIVE));
    const other = { id: 's1', context: { tenantId: ANCHOR }, conversationLedger: s.conversationLedger };
    assert.equal(rec.context(other), null);
    turn(rec, other, 'Anything new?', owner('reasoning'));
    assert.equal(other.conversationLedger.tenantId, ANCHOR);
    assert.equal(other.conversationLedger.referents.length, 0);
  });
});

describe('prohibitions, decisions and authority', () => {
  it('remembers prohibitions and decisions as history, never as authority', () => {
    const rec = recorder(); const s = session(LEVI);
    turn(rec, s, "Show me the five strongest prospects, what evidence supports each, and what's missing. Don't change anything.", prospects(LEVI, FIVE));
    s.context.acquisitionMissionId = 'm1';
    turn(rec, s, 'Approved.', { ...owner('active_mission'), structured: { metadata: { pendingDecisionResolution: { outcome: 'affirm', decisionKind: 'plan_approval' } } } });
    turn(rec, s, "Don't send anything yet.", owner('active_mission'));
    const l = s.conversationLedger;
    assert.deepEqual(l.constraints.filter((c) => c.kind === 'prohibition').map((c) => c.action).sort(), ['any_change', 'send']);
    assert.deepEqual(l.decisions.map((d) => [d.decisionKind, d.outcome, d.authority]), [['plan_approval', 'operator_affirmed', 'none'], ['unknown', 'operator_held', 'none']]);
    const view = rec.context(s).view;
    assert.deepEqual(view.authority, { grantsAuthority: false, executionPermitted: false, consumableApprovals: [], source: 'mission runtime and pending-decision resolver only' });
    assert.ok(view.remembered.constraints.every((c) => c.enforcedBy === 'not_this_ledger'));
    assert.ok(view.evidence.every((e) => e.mustReread === true));
  });

  it('rejects any ledger that claims authority or carries unexpected fields', () => {
    const rec = recorder(); const s = session(LEVI);
    turn(rec, s, 'Approved.', { ...owner('active_mission'), structured: { metadata: { pendingDecisionResolution: { outcome: 'affirm', decisionKind: 'plan_approval' } } } });
    s.context.acquisitionMissionId = 'm1';
    turn(rec, s, 'Approved.', { ...owner('active_mission'), structured: { metadata: { pendingDecisionResolution: { outcome: 'affirm', decisionKind: 'plan_approval' } } } });
    const good = s.conversationLedger;
    L.validateLedger(good);
    const tamper = (fn) => { const x = JSON.parse(JSON.stringify(good)); fn(x); return x; };
    assert.throws(() => L.validateLedger(tamper((x) => { x.decisions[0].authority = 'granted'; })), /decision.authority/);
    assert.throws(() => L.validateLedger(tamper((x) => { x.approved = true; })), /unexpected/);
    assert.throws(() => L.validateLedger(tamper((x) => { x.decisions[0].approvalToken = 'abc'; })), /unexpected/);
    assert.throws(() => L.validateLedger(tamper((x) => { x.lastFrame = { ...x.lastFrame, executionCandidate: true }; })), /executionCandidate/);
    assert.throws(() => L.validateLedger(tamper((x) => { x.referents.push({ ...x.referents[0], id: 'rX', label: 'Call jane@example.com' }); })), /referent/);
  });
});

describe('bounds, privacy and expiry', () => {
  it('stays bounded over a long conversation and always fits the persistence budget', () => {
    const rec = recorder(); const s = session(LEVI);
    for (let i = 0; i < 300; i += 1) {
      s.context.acquisitionMissionId = `m${i % 40}`;
      const ids = Array.from({ length: 30 }, (_, k) => `p${i}_${k}`);
      turn(rec, s, i % 3 ? `Don't send or change anything about batch ${i}.` : 'No, I meant the existing prospects.', i % 2 ? prospects(LEVI, ids) : scout());
    }
    const l = s.conversationLedger;
    for (const [k, max] of Object.entries({ referents: 24, focus: 8, corrections: 16, openQuestions: 8, constraints: 16, decisions: 12, evidenceRefs: 16 })) {
      assert.ok(l[k].length <= max, `${k} ${l[k].length} > ${max}`);
    }
    assert.ok(l.referents.every((r) => r.members.length <= 25));
    assert.ok(Buffer.byteLength(JSON.stringify(L.projectLedger(l))) <= L.PERSIST_BUDGET_BYTES);
    L.validateLedger(l);
  });

  it('keeps no operator wording and no record contents', () => {
    const rec = recorder(); const s = session(LEVI);
    turn(rec, s, "Show me Levi's five strongest borrower prospects for refinancing.", prospects(LEVI, FIVE));
    turn(rec, s, 'Which has the freshest information?', owner('reasoning'));
    const json = JSON.stringify(s.conversationLedger);
    for (const needle of ['Levi', 'borrower', 'refinancing', 'freshest', 'Jane', 'financing detail']) assert.ok(!json.includes(needle), needle);
  });

  it('expires after two idle hours: stale context is never silently reused', () => {
    const c = clock(); const rec = recorder(c); const s = session(LEVI);
    turn(rec, s, 'Show me the five strongest prospects.', prospects(LEVI, FIVE));
    c.advance(L.TTL_MS + 1);
    assert.equal(rec.context(s), null);
    assert.equal(L.resolveFocus(s.conversationLedger, c.now()).referent, null);
    turn(rec, s, 'Which of those has the freshest information?', owner('reasoning'));
    const l = s.conversationLedger;
    assert.equal(l.expiredPredecessor, true);
    assert.equal(l.referents.length, 0);
    assert.equal(focus(s, rec).referent, null);
    assert.ok(l.openQuestions.some((q) => q.kind === 'clarification'), 'asks instead of resolving "those" to stale records');
  });

  it('never throws into a turn on malformed input', () => {
    const rec = recorder(); const s = session(LEVI);
    for (const bad of [null, undefined, {}, { structured: null }, { structured: { metadata: { items: 'x' } } }, { structured: { metadata: { items: [null, { recordRef: 42 }] } } }]) {
      assert.doesNotThrow(() => rec.observe({ session: s, text: undefined, result: bad }));
    }
    assert.doesNotThrow(() => rec.observe({ session: null, text: 'x', result: {} }));
    assert.doesNotThrow(() => rec.observe({ session: { id: 's', context: null }, text: 'x', result: {} }));
  });
});

describe('recovery projection', () => {
  function sample() {
    const rec = recorder(); const s = session(LEVI, 'sess-1');
    turn(rec, s, 'Show me the five strongest prospects.', prospects(LEVI, FIVE));
    s.context.acquisitionMissionId = 'm_live';
    turn(rec, s, 'And the mission?', owner('mission_inspection'));
    s.context.acquisitionMissionId = 'm_deleted';
    turn(rec, s, 'Approved.', { ...owner('active_mission'), structured: { metadata: { pendingDecisionResolution: { outcome: 'affirm', decisionKind: 'execution_approval' } } } });
    return s.conversationLedger;
  }

  it('restores only the same tenant and session, drops unconfirmed missions, and treats everything as unverified', () => {
    const persisted = L.projectLedger(sample());
    const now = T0 + 60000;
    assert.equal(L.recoverLedger(persisted, { tenantId: ANCHOR, sessionId: 'sess-1', now }), null);
    assert.equal(L.recoverLedger(persisted, { tenantId: LEVI, sessionId: 'other', now }), null);
    assert.equal(L.recoverLedger(persisted, { tenantId: LEVI, sessionId: 'sess-1', now: T0 + L.TTL_MS + 60000 }), null);
    assert.equal(L.recoverLedger({ ...persisted, injected: 1 }, { tenantId: LEVI, sessionId: 'sess-1', now }), null);
    const r = L.recoverLedger(persisted, { tenantId: LEVI, sessionId: 'sess-1', now, validMissionIds: ['m_live'] });
    assert.ok(!r.referents.some((x) => x.missionId === 'm_deleted'));
    assert.ok(r.referents.some((x) => x.missionId === 'm_live'));
    assert.ok(r.referents.every((x) => x.requiresRevalidation));
    assert.ok(r.recovered.droppedReferents >= 1);
    const view = L.viewLedger(r, now);
    assert.ok(view.evidence.every((e) => e.fresh === false));
    assert.ok(view.remembered.decisions.every((d) => d.authority === 'none'));
    assert.equal(view.authority.grantsAuthority, false);
    assert.equal(L.resolveFocus(r, now).requiresRevalidation, true);
  });
});
