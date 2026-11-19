'use strict';

/**
 * P0-MAX-REASONING-001 — cross-PR integration (PRs 1–5) on disposable
 * PostgreSQL with synthetic fixtures. Requires PR 3 (ConversationLedger) and
 * PR 4 (EvidenceRegistry) in the tree: it FAILS (never skips) without them.
 * Not matched by any CI glob until those PRs merge. Run:
 *   node --test packages/max/frameReasoning/integration/combinedArchitecture.integration.cjs
 *
 * Frames are hand-authored to represent the model producer's validated
 * output (no paid model call); every frame passes validateTurnFrame.
 */

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.resolve(__dirname, '../../../..');
for (const m of ['packages/max/context/ConversationLedgerRecorder.js', 'packages/max/context/LedgerDiscourse.js', 'packages/max/evidence/EvidenceRegistry.js', 'packages/max/evidence/EvidenceDiagnosis.js']) {
  if (!fs.existsSync(path.join(ROOT, m))) throw new Error(`cross-PR integration requires ${m} (PRs 3 and 4 merged into this tree)`);
}

const { Pool } = require('pg');
const net = require('node:net');
const realConnect = net.Socket.prototype.connect;
// The hermetic harness installs an offline network guard at load; it is
// required inside the test, after the disposable database has started.
let createHermeticWorkspaceEngine;
let guardedConnect;
const { startDisposablePostgres } = require('../../../../test/helpers/disposablePostgres');
const { ensureAcquisitionMissionSchema } = require('../../../../services/acquisitionMissionPersistence');
const { validateTurnFrame } = require('../../interpretation/TurnFrame');
const { ConversationLedgerRecorder } = require('../../context/ConversationLedgerRecorder');
const { ConversationContinuity } = require('../../context/ConversationContinuity');
const { ConversationContextStore } = require('../../context/ConversationContextStore');
const { extractProhibitions } = require('../../context/LedgerDiscourse');
const { EvidenceRegistry } = require('../../evidence/EvidenceRegistry');
const { diagnoseScout, diagnoseEmmett, rankOpportunities } = require('../../evidence/EvidenceDiagnosis');
const { FrameDirectedReasoner } = require('../FrameDirectedReasoner');
const { presentReasoning, claimsUnrecordedExecution } = require('../ReasoningPresentation');

const NOW = new Date();
const days = (n) => new Date(NOW.getTime() - n * 86400000).toISOString();
const L = (n) => `00000000-0000-4000-8000-0000000021${String(n).padStart(2, '0')}`;
const LEVI = '21';
const ANCHOR = '10';
const TABLES = ['prospects', 'agent_log', 'touchpoints', 'acquisition_missions', 'acquisition_mission_events', 'acquisition_outbound_programs'];
const ENV = { MAX_CONVERSATION_LEDGER: 'true', MAX_CONVERSATION_CONTEXT: 'true' };

async function hashTables(db) {
  const out = {};
  for (const t of TABLES) out[t] = (await db.query(`SELECT md5(COALESCE(string_agg(x::text, '|' ORDER BY x::text), '')) AS h FROM ${t} x`)).rows[0].h;
  return out;
}

async function seed(db) {
  await db.query(`
    CREATE TABLE clients(id INTEGER PRIMARY KEY, name TEXT, slug TEXT);
    CREATE TABLE companies(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL REFERENCES clients(id), name TEXT);
    CREATE TABLE prospects(id UUID PRIMARY KEY, client_id INTEGER NOT NULL REFERENCES clients(id), company_id UUID REFERENCES companies(id),
      first_name TEXT, last_name TEXT, email TEXT, phone TEXT, linkedin_url TEXT, source TEXT, icp_score INTEGER, status TEXT, notes TEXT,
      service_area_match TEXT, setter_status TEXT, setter_visible BOOLEAN DEFAULT false, is_hot BOOLEAN DEFAULT false, do_not_contact BOOLEAN DEFAULT false,
      ao_outreach_review_required BOOLEAN NOT NULL DEFAULT false, acquisition_metadata JSONB, last_contacted_at TIMESTAMPTZ,
      closed_at TIMESTAMPTZ, booked_at TIMESTAMPTZ, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ);
    CREATE TABLE agent_log(id SERIAL PRIMARY KEY, client_id INTEGER, agent_name TEXT, ran_at TIMESTAMPTZ, status TEXT);
    CREATE TABLE touchpoints(id SERIAL PRIMARY KEY, client_id INTEGER, channel TEXT, action_type TEXT, outcome TEXT, created_at TIMESTAMPTZ);
    INSERT INTO clients VALUES (21, 'Levi Lending', 'levi-lending'), (10, 'Anchor Cleaning', 'cleaning-co');`);
  await ensureAcquisitionMissionSchema(db);
  await db.query(fs.readFileSync(path.join(ROOT, 'migrations/2026-09-18-anchor-daily-outbound.sql'), 'utf8'));
  await db.query(fs.readFileSync(path.join(ROOT, 'migrations/2026-11-09-max-conversation-context.sql'), 'utf8'));
  const md = (x = {}) => JSON.stringify({ category: 'borrower', reviewState: 'contact_now', ...x });
  const p = (id, client, f) => db.query(`INSERT INTO prospects (id, client_id, first_name, email, phone, source, icp_score, status, notes, service_area_match,
    setter_status, is_hot, acquisition_metadata, last_contacted_at, closed_at, created_at, updated_at)
    VALUES ($1,$2,'Synthetic',$3,$4,'fixture',85,$5,'synthetic note','yes',$6,$7,$8::jsonb,$9,$10,$11,$12)`,
  [id, client, 'email' in f ? f.email : 'x@example.test', 'phone' in f ? f.phone : '555-0100', f.status, f.setter || 'new', f.hot || false,
    f.md || md(), f.contacted || days(10), f.closed || null, days(300), f.updated || days(3)]);
  await p(L(1), 21, { status: 'hot', hot: true, updated: days(2) });
  await p(L(2), 21, { status: 'warm', email: null, phone: null, updated: days(1) });
  await p(L(3), 21, { status: 'closed', closed: days(20), setter: 'follow_up' });
  await p(L(4), 21, { status: 'cold', updated: days(90), contacted: days(120) });
  await p(L(5), 21, { status: 'warm', md: md({ expiresAt: days(5) }) });
  await p(L(6), 21, { status: 'warm', updated: days(4) });
  for (const [n, status] of [[1, 'hot'], [2, 'warm'], [3, 'dead']]) await p(`00000000-0000-4000-8000-0000000010${String(n).padStart(2, '0')}`, 10, { status, md: md({ category: 'facility' }) });
  await db.query(`INSERT INTO agent_log (client_id, agent_name, ran_at, status) VALUES (10, 'scout', $1, 'error'), (10, 'scout', $2, 'error')`, [days(3), days(20)]);
  const mission = (id, status, updated, payload = {}) => db.query(`INSERT INTO acquisition_missions (id, tenant_id, client_id, stage, status, objective, title, payload, created_at, updated_at)
    VALUES ($1,'10',10,'discovery',$2,'Synthetic objective','Synthetic mission',$3::jsonb,$4,$4)`, [id, status, JSON.stringify(payload), updated]);
  await mission('m_anchor_live', 'active', days(1), { pendingOperatorDecision: { kind: 'plan_approval', prompt: 'Approve?' } });
  await mission('m_anchor_idle', 'active', days(40));
  await db.query(`INSERT INTO acquisition_mission_events (id, mission_id, tenant_id, kind, specialist, label, at) VALUES
    ('e1','m_anchor_live','10','emmett_dispatch_completed','emmett','Emmett dispatch completed',$1)`, [days(2)]);
  await db.query(`INSERT INTO acquisition_outbound_programs (id, tenant_id, source_mission_id, policy, policy_hash, scope_hash, authorized_by, authorized_at, mode)
    VALUES ('prog1','10','m_anchor_live',$1::jsonb,'h','s','operator',$2,'paused')`, [JSON.stringify({ expiresAt: days(26) }), days(60)]);
}

test('cross-PR: TurnFrame → ConversationLedger → EvidenceRegistry → evidence-grounded answer (scenarios A–G)', { timeout: 240000 }, async (t) => {
  const instance = await startDisposablePostgres('max-pr5-');
  ({ createHermeticWorkspaceEngine } = require('../../workspace/tests/workspaceTestHarness'));
  guardedConnect = net.Socket.prototype.connect;
  const port = Number(new URL(instance.connectionString).port);
  net.Socket.prototype.connect = function connect(...args) {
    const o = args[0] && typeof args[0] === 'object' ? args[0] : { port: args[0], host: args[1] };
    return o.host === '127.0.0.1' && Number(o.port) === port ? realConnect.apply(this, args) : guardedConnect.apply(this, args);
  };
  const db = new Pool({ connectionString: instance.connectionString });
  t.after(async () => { await db.end(); await instance.stop(); });
  await seed(db);
  const before = await hashTables(db);
  const statements = [];
  const pool = { query: (sql, params) => { statements.push(typeof sql === 'string' ? sql : sql.text); return db.query(sql, params); } };
  const gate = { loadOutboundGate: async ({ tenantId, asOf }) => (tenantId === ANCHOR
    ? { source: 'outbound_gate', available: true, applicable: true, provenance: 'fixture', asOf, mode: 'maintenance', reason: 'recovery_kill_switch' }
    : { source: 'outbound_gate', available: true, applicable: false, provenance: 'none', asOf }) };
  const registry = new EvidenceRegistry({ pool, now: () => NOW, env: {}, loaders: { snapshot: gate } });
  const evidence = { retrieve: (req) => registry.retrieve(req), diagnose: { scout: diagnoseScout, emmett: diagnoseEmmett, rank: rankOpportunities } };
  const reasoner = new FrameDirectedReasoner({ evidence, now: () => NOW });
  const store = () => new ConversationContextStore({ db: pool });
  const engine = () => createHermeticWorkspaceEngine({ conversationContinuity: new ConversationContinuity({ env: ENV, store: store() }), conversationLedger: new ConversationLedgerRecorder({ env: ENV }) });
  const transcript = [];

  // One operator turn: memory → frame → reasoning → presentation → memory update → durable save.
  async function turn(ws, sessionId, tenantId, text, frameFields) {
    const session = ws._sessions.get(sessionId);
    const recorder = ws._conversationLedger;
    const ctx = recorder.context(session);
    const live = (session.conversationLedger?.referents || []).filter((r) => r.status === 'active');
    const memory = ctx ? { ...ctx, referents: live } : null;
    const frame = validateTurnFrame({
      intent: 'question', subjectKind: 'none', relationToPrior: 'new_topic', referents: [], evidenceNeeds: [], facets: [], requestedOutcome: 'explanation',
      requestedAction: { verb: 'none', targetCandidateId: null, polarity: 'none' }, scope: 'focused', foreignTenantReference: false, ambiguities: [], confidence: 0.9,
      ...frameFields(memory),
    }, { tenantId, candidates: live.map((r) => ({ id: r.id, kind: r.kind, label: r.label })), pendingDecisionId: frameFields.pending || null, producer: 'model' });
    const result = await reasoner.reason({ tenantId, frame, memory, constraints: extractProhibitions(text) });
    const presented = await presentReasoning(result);
    assert.equal(claimsUnrecordedExecution(presented.prose, result), false, text);
    recorder.observe({ session, text, result: { structured: presented.structured, workspaceOwnership: { owner: 'frame_reasoning' } } });
    ws._conversationContinuity.remember(session);
    await ws._conversationContinuity.drain();
    transcript.push({ tenantId, operator: text, max: presented.prose });
    return { result, presented, session };
  }
  const focusOf = (memory) => memory?.focus?.referent?.id;

  const wsA = engine();
  const levi = (await wsA.open({ tenantId: LEVI, page: 'command-deck' })).sessionId;

  await t.test('A: Levi prospect investigation — existing records, missing info, freshness, correction', async () => {
    const a1 = await turn(wsA, levi, LEVI, "Show me Levi's five strongest existing borrower prospects and explain what's missing.",
      () => ({ subjectKind: 'crm_pipeline', evidenceNeeds: ['crm_pipeline'], facets: ['recommendation', 'status'] }));
    assert.equal(a1.result.kind, 'answer');
    assert.deepEqual(a1.result.subjectRecordIds, [L(1), L(2), L(6), L(4)], 'open, eligible, ordered by recorded tier (closed L3 and expired L5 excluded)');
    assert.match(a1.presented.prose, /Missing: [^\n]*contact channel/);
    assert.match(a1.presented.prose, /4 record\(s\) are open[^\n]*fewer than the five asked for\. Excluded: [^\n]*opportunity closed[^\n]*opportunity expired/);
    assert.match(a1.presented.prose, /Contradictions \(unresolved\)[\s\S]*terminal but setter status/);
    const a2 = await turn(wsA, levi, LEVI, 'Which of those has the freshest information?',
      (m) => ({ subjectKind: 'crm_pipeline', relationToPrior: 'follow_up', facets: ['comparison'], referents: [{ candidateId: focusOf(m), role: 'primary' }] }));
    assert.deepEqual(a2.result.revalidation.map((r) => [r.id, r.status]), [L(1), L(2), L(6), L(4)].map((id) => [id, 'current']));
    assert.match(a2.result.calculated[0].claim, new RegExp(`record ${L(2)}`));
    assert.deepEqual(a2.result.observed.map((o) => o.subject.id).sort(), [L(1), L(2), L(4), L(6)], 'answered about the four remembered records only');
    assert.deepEqual(a2.result.contradictions, [], 'the closed record outside the set is not part of this answer');
    const a3 = await turn(wsA, levi, LEVI, 'No, I meant the existing prospects, not new leads.',
      () => ({ subjectKind: 'crm_pipeline', intent: 'correction', relationToPrior: 'correction' }));
    assert.equal(a3.result.referent.scope, 'existing');
    assert.ok(a3.result.domains.includes('crm_pipeline') && !a3.result.domains.includes('scout_execution'));
    assert.ok(a3.result.observed.every((o) => o.tenantId === LEVI));
  });

  await t.test('B: enrichment intent without authority — identified, constrained, not executed', async () => {
    const b = await turn(wsA, levi, LEVI, "Get whatever verified contact information we can into those five records. Don't send anything.",
      (m) => ({ subjectKind: 'crm_pipeline', intent: 'instruction', relationToPrior: 'follow_up', requestedOutcome: 'action', referents: [{ candidateId: focusOf(m), role: 'primary' }],
        requestedAction: { verb: 'modify', targetCandidateId: focusOf(m), polarity: 'affirmative' } }));
    assert.equal(b.result.kind, 'not_executed');
    assert.deepEqual([b.result.execution.executed, b.result.execution.status], [false, 'not_executed']);
    assert.ok(b.result.execution.constraints.includes('send'));
    assert.match(b.presented.prose, /^Not executed: I identified a request to update records but did not carry it out/);
    assert.deepEqual(b.result.revalidation.map((r) => r.status), ['current', 'current', 'current', 'current']);
  });

  await t.test('E: cross-tenant request — refused before any read, nothing transferred', async () => {
    const evidenceReads = () => statements.filter((x) => !/max_conversation_context/.test(x)).length;
    const n = evidenceReads();
    const e = await turn(wsA, levi, LEVI, "Use Anchor's records to update Levi's pipeline.",
      () => ({ subjectKind: 'crm_pipeline', intent: 'instruction', foreignTenantReference: true, requestedAction: { verb: 'modify', targetCandidateId: null, polarity: 'affirmative' } }));
    assert.equal(e.result.refusal, 'foreign_tenant_reference');
    assert.equal(evidenceReads(), n, 'no evidence read (only the conversation memory was saved)');
  });

  const anchor = (await wsA.open({ tenantId: ANCHOR, page: 'command-deck' })).sessionId;

  await t.test('C: Anchor diagnosis — Scout, then Emmett, with uncertainty disclosed', async () => {
    const c1 = await turn(wsA, anchor, ANCHOR, "Why isn't Scout producing qualified prospects for Anchor?",
      () => ({ subjectKind: 'agent_activity', evidenceNeeds: ['scout_execution'], facets: ['cause'] }));
    assert.ok(c1.result.inferences.some((i) => /failed/.test(i.claim) && i.basis.includes('scout_execution:agent_run:scout')));
    assert.match(c1.presented.prose, /Scout acquisition state: not consulted/);
    assert.ok(c1.result.observed.some((o) => o.subject.type === 'prospect_aggregate'), 'prospect counts were read');
    const c2 = await turn(wsA, anchor, ANCHOR, 'And what about Emmett?',
      () => ({ subjectKind: 'outbound', relationToPrior: 'follow_up', evidenceNeeds: ['outbound_authorization', 'outbound_delivery'], facets: ['cause'] }));
    const claims = c2.result.inferences.map((i) => i.claim).join(' | ');
    assert.match(claims, /maintenance/);
    assert.match(claims, /authorization expired/);
    assert.match(c2.presented.prose, /does not establish that emmett never ran/);
    // "never ran" may appear only inside the disclosure that absence proves nothing.
    for (const line of c2.presented.prose.split('\n')) if (/\b(?:did not|never) r[au]n\b/i.test(line)) assert.match(line, /does not establish/, line);
  });

  await t.test('D: "the active mission", then "the one we discussed earlier"', async () => {
    const s = wsA._sessions.get(anchor);
    s.context.acquisitionMissionId = 'm_anchor_idle';
    await turn(wsA, anchor, ANCHOR, 'What is going on with the idle mission?', () => ({ subjectKind: 'mission', facets: ['status'] }));
    s.context.acquisitionMissionId = 'm_anchor_live';
    await turn(wsA, anchor, ANCHOR, 'Show me the live mission.', () => ({ subjectKind: 'mission', facets: ['status'] }));
    const d1 = await turn(wsA, anchor, ANCHOR, 'Look at the active mission.', (m) => {
      const r = (m?.referents || []).filter((x) => x.kind === 'mission');
      return { subjectKind: 'mission', facets: ['status'], ambiguities: [{ kind: 'referent', candidateIds: r.map((x) => x.id) }] };
    });
    assert.equal(d1.result.kind, 'clarification', 'two missions are active: ask which');
    assert.equal(d1.result.clarification.options.length, 2);
    assert.match(d1.presented.prose, /m_anchor_idle[\s\S]*m_anchor_live|m_anchor_live[\s\S]*m_anchor_idle/);
    delete s.context.acquisitionMissionId;
    const d2 = await turn(wsA, anchor, ANCHOR, 'No, the one we discussed earlier.', () => ({ subjectKind: 'mission', intent: 'correction', relationToPrior: 'correction' }));
    assert.equal(d2.result.referent.missionId, 'm_anchor_idle');
    assert.deepEqual(d2.result.revalidation.map((r) => [r.id, r.status]), [['m_anchor_idle', 'current']]);
    assert.ok(d2.result.observed.every((o) => o.subject.id === 'm_anchor_idle' || o.facts.missionId === 'm_anchor_idle'), 'answered about the earlier mission only');
    assert.ok(d2.result.contradictions.some((c) => c.kind === 'status_vs_activity'), 'marked active, no activity: the contradiction is shown, not resolved');
  });

  await t.test('F: "Maybe approve it, but hold the emails until Monday" — nothing consumed', async () => {
    const f = turn.bind(null, wsA, anchor, ANCHOR, 'Maybe approve it, but hold the emails until Monday.');
    const fields = () => ({ subjectKind: 'pending_decision', intent: 'authorization', relationToPrior: 'follow_up', requestedOutcome: 'action', confidence: 0.4,
      requestedAction: { verb: 'approve', targetCandidateId: 'pd1', polarity: 'affirmative' }, ambiguities: [{ kind: 'action', candidateIds: [] }] });
    const res = await f(fields);
    assert.equal(res.result.kind, 'clarification');
    assert.deepEqual([res.result.execution.requested, res.result.execution.executed], ['approve', false]);
    const row = (await db.query("SELECT payload FROM acquisition_missions WHERE id = 'm_anchor_live'")).rows[0];
    assert.equal(row.payload.pendingOperatorDecision.kind, 'plan_approval', 'decision still pending');
  });

  await t.test('G: durable recovery after restart — continuity without authority, evidence revalidated', async () => {
    const wsB = engine(); // a new engine: the in-process session is gone
    assert.equal(wsB._sessions.get(levi), null);
    const rec = await wsB.recoverConversationSession({ tenantId: LEVI, sessionId: levi });
    assert.equal(rec.recovered, true);
    const other = await engine().recoverConversationSession({ tenantId: ANCHOR, sessionId: levi });
    assert.equal(other.recovered, false, 'another tenant cannot recover it');
    const ctx = wsB.getConversationLedger({ tenantId: LEVI, sessionId: levi });
    assert.ok(ctx.view.remembered.recovered);
    assert.deepEqual(ctx.view.authority.consumableApprovals, []);
    assert.ok(ctx.view.remembered.constraints.some((c) => c.kind === 'prohibition' && c.action === 'send'));
    await db.query('DELETE FROM prospects WHERE id = $1', [L(4)]); // changed while the process was down
    const g = await turn(wsB, levi, LEVI, 'Which of those has the freshest information now?',
      (m) => ({ subjectKind: 'crm_pipeline', relationToPrior: 'follow_up', facets: ['comparison'], referents: [{ candidateId: focusOf(m), role: 'primary' }] }));
    assert.equal(g.result.referent.requiresRevalidation, true);
    assert.deepEqual(g.result.revalidation.map((r) => [r.id, r.status]), [[L(1), 'current'], [L(2), 'current'], [L(6), 'current'], [L(4), 'not_found_in_tenant']]);
    assert.match(g.presented.prose, /could not be confirmed[\s\S]*not found in tenant/);
    await db.query(`INSERT INTO prospects (id, client_id, status, created_at, updated_at) VALUES ($1, 21, 'cold', $2, $2)`, [L(4), days(90)]);
  });

  await t.test('read-only end to end: every evidence statement was a read; no business table changed', async () => {
    const evidenceStatements = statements.filter((s) => !/max_conversation_context/.test(s));
    assert.ok(evidenceStatements.length > 20);
    for (const s of evidenceStatements) assert.match(s, /^\s*(SELECT|WITH)\b/i, s.slice(0, 80));
    const after = await hashTables(db);
    for (const tbl of TABLES.filter((x) => x !== 'prospects')) assert.equal(after[tbl], before[tbl], tbl);
    const out = process.env.MAX_PR5_TRANSCRIPT;
    if (out) fs.writeFileSync(out, transcript.map((x) => `### [tenant ${x.tenantId}] Operator: ${x.operator}\n\n${x.max}\n`).join('\n'));
  });
});
