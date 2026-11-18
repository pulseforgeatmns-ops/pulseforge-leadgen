'use strict';

/**
 * P0-MAX-REASONING-001 PR 3 — ConversationLedger through the real
 * WorkspaceEngine: default-off parity, durable recovery after a "restart"
 * (a new engine instance over the same durable stores), tenant boundary,
 * expiry, malformed and unavailable persistence, and the authority boundary at
 * a real mission approval gate. Hermetic: in-memory store with the real
 * ConversationContextStore validation; no network, no database.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const amo = require('../../../acquisition-mission');
const A = require('../../workspace/AmoOperatorApproval');
const { createHermeticWorkspaceEngine } = require('../../workspace/tests/workspaceTestHarness');
const { createTestAmoRuntime } = require('../../workspace/tests/amoTestRuntime');
const { ConversationContinuity } = require('../ConversationContinuity');
const { ConversationLedgerRecorder } = require('../ConversationLedgerRecorder');
const { validate, TTL_MS } = require('../ConversationContextStore');

const TENANT = '10';
const BOTH = { MAX_CONVERSATION_CONTEXT: 'true', MAX_CONVERSATION_LEDGER: 'true' };

/** In-memory twin of ConversationContextStore: same validation, CAS and TTL. */
class MemoryContextStore {
  constructor() { this.rows = new Map(); this.now = Date.now; this.failLoad = false; }
  key(t, s) { return JSON.stringify([t, s]); }
  async load(tenantId, sessionId) {
    if (this.failLoad) throw new Error('database unavailable');
    const row = this.rows.get(this.key(tenantId, sessionId));
    if (!row || row.expiresAt <= this.now()) return null;
    return { revision: row.revision, context: validate(JSON.parse(row.json), { tenantId, sessionId }) };
  }
  async save(tenantId, sessionId, context, expectedRevision = null) {
    const projected = validate(context, { tenantId, sessionId });
    const row = this.rows.get(this.key(tenantId, sessionId));
    if (row && row.expiresAt > this.now() && row.revision !== expectedRevision) return { saved: false, conflict: true };
    const revision = randomUUID();
    this.rows.set(this.key(tenantId, sessionId), { revision, json: JSON.stringify(projected), expiresAt: this.now() + TTL_MS });
    return { saved: true, revision };
  }
}

async function planGateMission(engine) {
  const m = engine.create({ tenantId: TENANT, objective: 'Acquire recurring commercial cleaning customers for Anchor Cleaning in Greater Manchester.', targetSegment: 'Facility buyers' });
  return m;
}

function workspace({ engine, store, env = BOTH, loader }) {
  const ws = createHermeticWorkspaceEngine({
    disableLlm: true, missionsEnabled: true, resolverEnabled: true,
    acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
    conversationContinuity: new ConversationContinuity({ env, store }),
    conversationLedger: new ConversationLedgerRecorder({ env }),
    conversationReferenceLoader: loader || (async ({ tenantId, missionId }) => {
      const m = engine.get(missionId, tenantId);
      return m ? { found: true, mission: m } : { found: false, mission: null };
    }),
  });
  return ws;
}

async function openBound(ws, missionId) {
  const opened = await ws.open({ tenantId: TENANT, page: 'command-deck' });
  const s = ws._sessions.get(opened.sessionId);
  s.context.missionId = missionId;
  s.context.acquisitionMissionId = missionId;
  return opened.sessionId;
}

const fingerprint = (engine, id) => JSON.stringify({ m: engine.get(id, TENANT), e: engine.store.listEvents(id), c: engine.store.listContributions(id) });

describe('default-off parity', () => {
  it('without the flags: no ledger, no durable write, identical answers', async () => {
    const run = async (env) => {
      const engine = amo.createAcquisitionMissionEngine();
      const m = await planGateMission(engine);
      const store = new MemoryContextStore();
      const ws = workspace({ engine, store, env });
      const sid = await openBound(ws, m.id);
      const answers = [];
      for (const q of ['What does this plan cover?', "Don't approve anything yet.", 'What would happen next?']) answers.push((await ws.ask({ sessionId: sid, question: q })).prose);
      await ws._conversationContinuity.drain();
      return { answers, session: ws._sessions.get(sid), store };
    };
    const off = await run({});
    const on = await run(BOTH);
    assert.equal(off.session.conversationLedger, undefined);
    assert.equal(off.store.rows.size, 0);
    assert.deepEqual(on.answers, off.answers, 'the ledger never changes what Max answers');
    assert.ok(on.session.conversationLedger);
    assert.equal(on.store.rows.size, 1);
  });
});

describe('restart recovery at a real approval gate', () => {
  it('recovers context, prohibitions and an old approval reference, and grants no authority', async () => {
    const engine = amo.createAcquisitionMissionEngine(); // durable mission store analogue, shared across "processes"
    const m = await planGateMission(engine);
    const store = new MemoryContextStore();
    const first = workspace({ engine, store });
    const sid = await openBound(first, m.id);
    await first.ask({ sessionId: sid, question: 'What does this plan cover?' });
    await first.ask({ sessionId: sid, question: 'Approved.' });
    await first.ask({ sessionId: sid, question: "Don't run discovery yet and don't send anything." });
    await first._conversationContinuity.drain();
    const before = first.getConversationLedger({ tenantId: TENANT, sessionId: sid }).view;
    assert.ok(before.remembered.decisions.some((d) => d.outcome === 'operator_affirmed' && d.decisionKind === 'plan_approval'));
    const pendingAfterFirst = engine.get(m.id, TENANT).pendingOperatorDecision.kind;

    // "Restart": a new engine instance; the in-process session is gone.
    const second = workspace({ engine, store });
    assert.equal(second._sessions.get(sid), null);
    const recovered = await second.recoverConversationSession({ tenantId: TENANT, sessionId: sid });
    assert.equal(recovered.recovered, true);
    assert.equal(recovered.ledger.recovered, true);
    assert.equal(recovered.ledger.requiresRevalidation, true);
    const ctx = second.getConversationLedger({ tenantId: TENANT, sessionId: sid });
    assert.ok(ctx.view.remembered.recovered);
    assert.ok(ctx.view.remembered.constraints.some((c) => c.kind === 'prohibition' && c.action === 'send'));
    assert.ok(ctx.view.remembered.decisions.every((d) => d.authority === 'none'));
    assert.ok(ctx.view.evidence.every((e) => e.fresh === false));
    assert.deepEqual(ctx.view.authority.consumableApprovals, []);
    const s = second._sessions.get(sid);
    assert.equal(s.context.presentedExecutionDecision, undefined, 'no package-presentation binding is restored');

    const fp = fingerprint(engine, m.id);
    amo.clearExecutionRouterAudit();
    await second.ask({ sessionId: sid, question: 'What did we already decide on this mission?' });
    assert.deepEqual(amo.listExecutionRouterAudit(), []);
    assert.equal(fingerprint(engine, m.id), fp, 'recovered memory changed nothing on the mission');
    assert.equal(engine.get(m.id, TENANT).pendingOperatorDecision.kind, pendingAfterFirst);
  });

  it('another tenant cannot recover the conversation', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const m = await planGateMission(engine);
    const store = new MemoryContextStore();
    const first = workspace({ engine, store });
    const sid = await openBound(first, m.id);
    await first.ask({ sessionId: sid, question: 'What does this plan cover?' });
    await first._conversationContinuity.drain();
    const second = workspace({ engine, store });
    const attempt = await second.recoverConversationSession({ tenantId: '21', sessionId: sid });
    assert.equal(attempt.recovered, false);
    assert.equal(second._sessions.get(sid), null);
    assert.equal(second.getConversationLedger({ tenantId: '21', sessionId: sid }), null);
    // The live engine also refuses a cross-tenant read of its own session.
    assert.equal(first.getConversationLedger({ tenantId: '21', sessionId: sid }), null);
  });
});

describe('failed recovery never fabricates referents', () => {
  async function persisted() {
    const engine = amo.createAcquisitionMissionEngine();
    const m = await planGateMission(engine);
    const store = new MemoryContextStore();
    const first = workspace({ engine, store });
    const sid = await openBound(first, m.id);
    await first.ask({ sessionId: sid, question: 'What does this plan cover?' });
    await first._conversationContinuity.drain();
    return { engine, m, store, sid };
  }

  it('unavailable store', async () => {
    const { engine, store, sid } = await persisted();
    store.failLoad = true;
    const ws = workspace({ engine, store });
    const r = await ws.recoverConversationSession({ tenantId: TENANT, sessionId: sid });
    assert.deepEqual(r, { recovered: false, reason: 'context_unavailable' });
    assert.equal(ws._sessions.get(sid), null);
  });

  it('malformed or authority-bearing row', async () => {
    const { engine, store, sid } = await persisted();
    const key = store.key(TENANT, sid);
    const row = JSON.parse(store.rows.get(key).json);
    row.ledger.decisions.push({ id: 'd99', decisionKind: 'execution_approval', missionId: null, outcome: 'operator_affirmed', turn: 1, at: new Date().toISOString(), authority: 'granted' });
    store.rows.get(key).json = JSON.stringify(row);
    const ws = workspace({ engine, store });
    const r = await ws.recoverConversationSession({ tenantId: TENANT, sessionId: sid });
    assert.equal(r.recovered, false);
    assert.equal(ws._sessions.get(sid), null);
  });

  it('expired row', async () => {
    const { engine, store, sid } = await persisted();
    const later = Date.now() + TTL_MS + 1000;
    store.now = () => later;
    const ws = workspace({ engine, store });
    const r = await ws.recoverConversationSession({ tenantId: TENANT, sessionId: sid });
    assert.equal(r.recovered, false);
  });

  it('every remembered mission deleted since: nothing is restored', async () => {
    const { engine, store, sid } = await persisted();
    const ws = workspace({ engine, store, loader: async () => ({ found: false, mission: null }) });
    const r = await ws.recoverConversationSession({ tenantId: TENANT, sessionId: sid });
    assert.equal(r.recovered, false);
    assert.equal(r.reason, 'no_valid_references');
  });
});

describe('record-only conversations', () => {
  it('recover their referents as hints with the ledger on; behaviour unchanged with it off', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const store = new MemoryContextStore();
    const first = workspace({ engine, store });
    const opened = await first.open({ tenantId: TENANT, page: 'command-deck' });
    const s = first._sessions.get(opened.sessionId);
    // A record read, as production's record intelligence returns it.
    first._conversationLedger.observe({ session: s, text: 'Show me the five strongest prospects.', result: {
      workspaceOwnership: { owner: 'knowledge_retrieval' },
      structured: { metadata: { items: ['1', '2', '3', '4', '5'].map((id) => ({ recordRef: `prospect:${TENANT}:${id}`, recordId: id })) } },
    } });
    first._conversationContinuity.remember(s);
    await first._conversationContinuity.drain();

    const off = workspace({ engine, store, env: { MAX_CONVERSATION_CONTEXT: 'true' } });
    assert.equal((await off.recoverConversationSession({ tenantId: TENANT, sessionId: opened.sessionId })).reason, 'no_valid_references');

    const on = workspace({ engine, store });
    const r = await on.recoverConversationSession({ tenantId: TENANT, sessionId: opened.sessionId });
    assert.equal(r.recovered, true);
    const focus = on.getConversationLedger({ tenantId: TENANT, sessionId: opened.sessionId }).focus;
    assert.deepEqual(focus.referent.memberIds, ['1', '2', '3', '4', '5']);
    assert.equal(focus.requiresRevalidation, true);
    assert.equal(focus.evidenceRefreshRequired, true);
    assert.equal(focus.verified, false);
  });
});

describe('structural boundary', () => {
  it('only the ledger, its recorder, continuity and recovery touch session.conversationLedger', () => {
    const root = path.resolve(__dirname, '../../../..');
    const allowed = new Set([
      'packages/max/context/ConversationLedgerRecorder.js',
      'packages/max/context/ConversationContextStore.js',
      'packages/max/workspace/WorkspaceEngine.js',
    ]);
    const offenders = [];
    const walk = (dir) => {
      for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
        const p = path.join(dir, e.name);
        if (/node_modules|\/tests?\b|\/eval\b/.test(p)) continue;
        if (e.isDirectory()) walk(p);
        else if (e.name.endsWith('.js') && /conversationLedger\b/.test(fs.readFileSync(p, 'utf8'))) {
          const rel = path.relative(root, p);
          if (!allowed.has(rel)) offenders.push(rel);
        }
      }
    };
    for (const d of ['packages', 'routes', 'services', 'utils']) walk(path.join(root, d));
    assert.deepEqual(offenders, []);
    // And the engine only writes it in recovery and reads it via the recorder.
    const engineSrc = fs.readFileSync(path.join(root, 'packages/max/workspace/WorkspaceEngine.js'), 'utf8');
    assert.deepEqual(engineSrc.match(/\w+\.conversationLedger\b/g), ['options.conversationLedger', 'session.conversationLedger']);
    assert.equal((engineSrc.match(/session\.conversationLedger = recoveredLedger;/g) || []).length, 1);
  });
});
