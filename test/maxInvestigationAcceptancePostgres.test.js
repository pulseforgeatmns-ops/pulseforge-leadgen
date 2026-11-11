'use strict';
// P0 acceptance (PR #1120): end-to-end Max investigation path for tenant 21
// against a disposable PostgreSQL database, never production.
//
// Real code on every hop: review-only import (importReviewOnlyContacts INSERT)
// -> Max operating-evidence prospect loader (defaultLoadProspects aggregates +
// ProspectRecordIntelligence.loadRecords keyset SQL) -> investigation policy ->
// deterministic presenter, through WorkspaceEngine.ask with an approved
// Blueprint that has NO numeric financing range (matches the v5 production
// audit). The six records reproduce the audited coverage pattern
// (3, 2, 2, 1, 0, 0) with synthetic names and sentences; Jon's sentence is the
// redacted audit phrase verbatim.
// Network guard: only the disposable PostgreSQL connection is allowed. HTTP(S),
// fetch, TLS and DNS are denied, so no provider, sender or model can be
// reached; any blocked attempt fails the run.
let allowedPort = null;
const blocked = [];
const deny = name => function () { blocked.push(name); throw new Error(`Unexpected network: ${name}`); };
for (const protocol of ['http', 'https']) { const t = require(`node:${protocol}`); t.request = deny(`${protocol}.request`); t.get = deny(`${protocol}.get`); }
require('node:tls').connect = deny('tls.connect');
globalThis.fetch = deny('fetch');
const net = require('node:net');
const originalConnect = net.Socket.prototype.connect;
net.Socket.prototype.connect = function (...args) {
  const o = typeof args[0] === 'object' && args[0] ? args[0] : { port: args[0], host: args[1] };
  const local = o.path || (['127.0.0.1', 'localhost', '::1', undefined].includes(o.host) && Number(o.port) === allowedPort);
  if (!local) { blocked.push(`net.connect ${o.host}:${o.port}`); throw new Error('Unexpected network: net.connect'); }
  return originalConnect.apply(this, args);
};
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { importReviewOnlyContacts } = require('../services/reviewOnlyProspectImportService');
const { createWorkspaceEngine } = require('../packages/max/workspace/WorkspaceEngine');
const { createTestAmoRuntime } = require('../packages/max/workspace/tests/amoTestRuntime');
const { createMemoryStore: createObjectiveMemoryStore } = require('../services/operatorObjectives');
const { createMemoryStore, startClientInterview, postInterviewMessage, approveBlueprint } = require('../services/clientIntelligenceInterview');
const { QUESTION } = require('../packages/max/workspace/tests/fixtures/investigationProspects');
const { EXTRACTOR } = require('../packages/max/workspace/ProspectDealClaims');
const { PRODUCTION_INVESTIGATION_PROMPT } = require('../packages/max/workspace/tests/fixtures/productionInvestigationPrompt');

const AUDIT_PHRASE = 'Current waterfront flip has an executed $260,000 purchase contract';
const COHORT = [
  ['Zackary Example', 'Requesting $35,000 for rehab on a flip with a $250k ARV. Proposed first lien on the subject property. Sale exit after rehab.', 3],
  ['Raul Example', 'Under contract on a duplex. Exit via DSCR refinance after a 4-month rehab; rehab budget of $60k.', 2],
  ['Jon Example', `${AUDIT_PHRASE}. Sale exit after a cosmetic rehab.`, 2],
  ['Reginald Example', 'Public request for $40k–$50k gap funding. Long-term BRRRR investor.', 1],
  ['John Example', 'Long-term BRRRR investor with rentals; has closed on 12 deals since 2019.', 0],
  ['David Example', 'Public profile describes fix-and-flip investing; purchase price and timeline not recorded.', 0],
];
const input = ([name, fit], i, extra = {}) => ({ name, company: `${name} Holdings`, fit, risks: 'Title, lien position and exit evidence unverified.',
  nextAction: 'Verify recorded claims against source documents.', reviewState: 'contact_now', market: 'Example County',
  category: 'borrower', sourceUrl: `https://example.invalid/acceptance/${i}`, ...extra });

async function schema(db) {
  await db.query(`
    CREATE TABLE clients(id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE companies(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL REFERENCES clients(id), name TEXT);
    CREATE TABLE prospects(
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL REFERENCES clients(id),
      company_id UUID REFERENCES companies(id), first_name TEXT, last_name TEXT, email TEXT, phone TEXT, job_title TEXT,
      linkedin_url TEXT, source TEXT, icp_score INTEGER, status TEXT, notes TEXT, vertical TEXT, service_area_match TEXT,
      setter_visible BOOLEAN DEFAULT false, setter_status TEXT, is_hot BOOLEAN DEFAULT false, do_not_contact BOOLEAN DEFAULT false,
      ao_outreach_review_required BOOLEAN NOT NULL DEFAULT false, discovery_method TEXT, acquisition_metadata JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW());
    INSERT INTO clients(id, name) VALUES (21, 'Tenant 21 (synthetic)'), (22, 'Other tenant (synthetic)');`);
}

async function approvedBlueprintWithoutRange() {
  const store = createMemoryStore();
  let interview = await startClientInterview({ clientId: 21, forceNew: true }, { store });
  for (const answer of ['Private lending to real-estate investors.', 'Short-term bridge and rehab loans.',
    'Real-estate operators.', 'Unsupported borrowers.', 'Example County.', 'Clear terms.', 'Professional.', 'Qualified pipeline.', 'Funded loans.']) {
    interview = await postInterviewMessage(interview.interviewId, answer, { store });
  }
  await approveBlueprint(interview.blueprint.id, { store });
  return store;
}

const snapshot = async db => JSON.stringify((await db.query(
  `SELECT to_jsonb(p) AS p FROM prospects p ORDER BY id`)).rows) + JSON.stringify((await db.query('SELECT * FROM companies ORDER BY id')).rows);

test('tenant 21 investigation request end to end on disposable PostgreSQL', { timeout: 180000 }, async () => {
  const postgres = await startDisposablePostgres('max-investigation-acceptance-');
  allowedPort = Number(postgres.port);
  const db = new Pool({ connectionString: postgres.connectionString });
  try {
    await schema(db);
    const imported = await importReviewOnlyContacts(COHORT.map((c, i) => input(c, i)), { clientId: 21, db, actorId: null });
    assert.equal(imported.inserted.length, 6);
    // Foreign-tenant record and a non-review-only tenant-21 record must not enter the cohort.
    await importReviewOnlyContacts([input(['Foreign Example', 'FOREIGN_SECRET requesting $99k with a first lien.'], 9)], { clientId: 22, db, actorId: null });
    const before = await snapshot(db);
    const expectedIds = (await db.query(`SELECT id FROM prospects WHERE client_id = 21 AND source = 'review_only_import'
      AND acquisition_metadata->>'reviewState' = 'contact_now' ORDER BY id`)).rows.map(r => r.id).sort();

    // Every statement Max sends during the turns is recorded.
    const statements = [];
    const recording = { query: (sql, params) => { statements.push(String(sql.text || sql)); return db.query(sql, params); } };
    const store = await approvedBlueprintWithoutRange();
    // Other operating-evidence sources are explicitly unavailable; the real
    // default prospect loader runs against the recording pool.
    const unknown = async () => ({ available: false, reason: 'acceptance_fixture_not_recorded' });
    const opts = Object.fromEntries(['loadCampaignAo', 'loadScout', 'loadMissions', 'loadObjectives', 'loadActivity',
      'loadOutcomes', 'loadOperatorAttested', 'loadCapability'].map(n => [n, unknown]));
    const engine = createWorkspaceEngine({ disableLlm: true, acquisitionMissionRuntime: createTestAmoRuntime(),
      clientIntelligenceOpts: { store }, operatorObjectiveOpts: { store: createObjectiveMemoryStore() },
      operatingEvidenceOpts: { ...opts, pool: recording } });
    const { sessionId } = engine.open({ tenantId: '21', page: 'command-deck' });
    const ask = question => engine.ask({ sessionId, question });

    // 1-7 on the original production prompt.
    const r = await ask(QUESTION);
    assert.equal(r.presentation, 'prospect_records');
    const evidence = r.structured.supportingEvidence;
    const meta = r.structured.metadata;
    // 1. The correct six prospects, nothing foreign.
    assert.deepEqual(evidence.map(e => e.recordId).sort(), expectedIds);
    assert.doesNotMatch(JSON.stringify(r), /FOREIGN_SECRET|prospect:22:|\$99k/);
    // 2. Evidence-based investigation order.
    assert.equal(meta.investigationPriority.ranked, true);
    assert.equal(meta.investigationPriority.extractor, EXTRACTOR);
    const rows = (await db.query('SELECT id, first_name FROM prospects WHERE client_id = 21')).rows;
    const nameOf = id => rows.find(x => x.id === id).first_name;
    const order = evidence.map(e => [nameOf(e.recordId), e.priority?.evidenceCoverage, e.priority?.position]);
    assert.deepEqual(order.map(([, c]) => c), [3, 2, 2, 1, 0, 0]);
    assert.equal(order[0][0], 'Zackary'); assert.equal(order[3][0], 'Reginald');
    assert.deepEqual(order.slice(1, 3).map(([n]) => n).sort(), ['Jon', 'Raul']);
    assert.equal(order[1][2], order[2][2], 'equal coverage shares a position');
    // Current presentation: zero-evidence records still receive a shared last
    // position (separately scoped follow-up; see PR #1120 acceptance notes).
    assert.deepEqual(order.map(([, , p]) => p), [1, 2, 2, 4, 5, 5]);
    const jon = evidence.find(e => nameOf(e.recordId) === 'Jon');
    assert.equal(jon.recordedClaims.claims.stage.snippet, `${AUDIT_PHRASE}.`);
    // 3. Imported claims are recorded, unverified.
    for (const e of evidence) for (const c of Object.values(e.recordedClaims.claims)) assert.equal(c.epistemic, 'recorded_unverified');
    assert.match(r.prose, /Recorded claims \(unverified\)/);
    // 4. The explanation uses the actual recorded text.
    assert.ok(r.prose.includes('Requesting $35,000 for rehab on a flip with a $250k ARV.'));
    assert.ok(r.prose.includes(`${AUDIT_PHRASE}.`));
    assert.match(r.prose, /3\/5 recorded deal claims/);
    // 5. No financing-fit conclusion without comparable approved criteria.
    assert.equal(meta.investigationPriority.criteria?.range ?? null, null);
    assert.doesNotMatch(r.prose, /(?:within|outside) the recorded business range/);
    assert.equal((r.prose.match(/Financing fit: Financing-fit comparison unavailable: no unambiguous authorized financing range\. No range assumed/g) || []).length, 5);
    assert.match(r.prose, /is a range, not a single amount \("\$40k–\$50k"\); financing fit cannot be assessed for this record/);
    assert.doesNotMatch(r.prose, /likelihood of (?:funding|progressing) is|will (?:fund|close)/i);
    // 6. review_only_import outreach restrictions hold.
    assert.equal((r.prose.match(/Outreach eligibility: blocked/g) || []).length, 6);
    assert.doesNotMatch(r.prose, /recorded gates pass/);
    // 7. No execution of any kind; reads only.
    assert.equal(meta.executed, false); assert.equal(meta.specialistDelegated, false); assert.equal(r.retrieval.delegated, false);
    assert.match(r.prose, /No writes, emails, calls, assignments, imports, review-state changes or specialist execution occurred/);

    // Routing of the short investigation prompt and the follow-up.
    const short = await ask('Which of these contact_now prospects should I prioritize for investigation?');
    assert.equal(short.presentation, 'prospect_records');
    assert.equal(short.structured.metadata.investigationPriority.ranked, true);
    const why = await ask('Why is the first prospect ranked highest?');
    assert.match(why.prose, /previous selected cohort/);
    assert.ok(why.prose.includes('Requesting $35,000 for rehab on a flip with a $250k ARV.'));

    assert.ok(statements.length > 0);
    for (const sql of statements) assert.match(sql.trim(), /^(?:SELECT|WITH)\b/i, `non-read statement: ${sql.slice(0, 80)}`);
    assert.equal(await snapshot(db), before, 'database unchanged');
    assert.deepEqual(blocked, [], 'no network beyond the disposable database');
    process.stdout.write(`\n----- prompt -----\n${QUESTION}\n----- response -----\n${r.prose}\n----- end -----\n`);
  } finally {
    await db.end();
    await postgres.stop();
  }
});

// Post-#1120 production acceptance failure: the operator's real prompt carries
// a read-only prohibition ("Do not send messages, assign prospects, modify
// records, or delegate execution"). It was classified as execution, skipped
// operational-read recovery and fell into General Conversation
// ("Fewer than two opportunities…", comparison_set unavailable).
test('the exact production investigation prompt, fresh and after a previous Workspace turn', { timeout: 180000 }, async () => {
  const postgres = await startDisposablePostgres('max-investigation-prod-prompt-');
  allowedPort = Number(postgres.port);
  const db = new Pool({ connectionString: postgres.connectionString });
  try {
    await schema(db);
    await importReviewOnlyContacts(COHORT.map((c, i) => input(c, i)), { clientId: 21, db, actorId: null });
    await importReviewOnlyContacts([input(['Foreign Example', 'FOREIGN_SECRET requesting $99k with a first lien.'], 9)], { clientId: 22, db, actorId: null });
    const before = await snapshot(db);
    const expectedIds = (await db.query(`SELECT id FROM prospects WHERE client_id = 21 AND source = 'review_only_import'
      AND acquisition_metadata->>'reviewState' = 'contact_now' ORDER BY id`)).rows.map(r => r.id).sort();
    const names = Object.fromEntries((await db.query('SELECT id, first_name FROM prospects WHERE client_id = 21')).rows.map(r => [r.id, r.first_name]));

    const statements = [];
    const recording = { query: (sql, params) => { statements.push(String(sql.text || sql)); return db.query(sql, params); } };
    const store = await approvedBlueprintWithoutRange();
    const unknown = async () => ({ available: false, reason: 'acceptance_fixture_not_recorded' });
    const opts = Object.fromEntries(['loadCampaignAo', 'loadScout', 'loadMissions', 'loadObjectives', 'loadActivity',
      'loadOutcomes', 'loadOperatorAttested', 'loadCapability'].map(n => [n, unknown]));
    const engine = createWorkspaceEngine({ disableLlm: true, acquisitionMissionRuntime: createTestAmoRuntime(),
      clientIntelligenceOpts: { store }, operatorObjectiveOpts: { store: createObjectiveMemoryStore() },
      operatingEvidenceOpts: { ...opts, pool: recording } });

    const verify = (r, label) => {
      assert.equal(r.presentation, 'prospect_records', label);
      assert.equal(r.executionDomain, 'workspace', label);
      assert.match(r.workspaceOwnership.reason, /^operational_read/, label);
      assert.doesNotMatch(r.prose, /Switching from|General Conversation|Fewer than two opportunities|comparison_set/, label);
      const evidence = r.structured.supportingEvidence;
      const meta = r.structured.metadata;
      // 2. The six tenant-21 prospects, ranked 1, 2, 2, 4, 5, 5.
      assert.deepEqual(evidence.map(e => e.recordId).sort(), expectedIds, label);
      assert.doesNotMatch(JSON.stringify(r), /FOREIGN_SECRET|prospect:22:|\$99k/, label);
      assert.equal(meta.investigationPriority.ranked, true, label);
      assert.deepEqual(evidence.map(e => e.priority?.position), [1, 2, 2, 4, 5, 5], label);
      assert.deepEqual(evidence.map(e => e.priority?.evidenceCoverage), [3, 2, 2, 1, 0, 0], label);
      assert.equal(names[evidence[0].recordId], 'Zackary', label);
      // 3. Recorded-unverified claims, financing-fit limits, outreach blocked.
      for (const e of evidence) for (const c of Object.values(e.recordedClaims.claims)) assert.equal(c.epistemic, 'recorded_unverified', label);
      assert.match(r.prose, /Recorded claims \(unverified\)/, label);
      assert.doesNotMatch(r.prose, /(?:within|outside) the recorded business range/, label);
      assert.equal((r.prose.match(/Financing-fit comparison unavailable: no unambiguous authorized financing range/g) || []).length, 5, label);
      assert.match(r.prose, /is a range, not a single amount \("\$40k–\$50k"\); financing fit cannot be assessed/, label);
      assert.equal((r.prose.match(/Outreach eligibility: blocked/g) || []).length, 6, label);
      // 4. No execution of any kind.
      assert.equal(meta.executed, false, label); assert.equal(meta.specialistDelegated, false, label);
      assert.equal(r.retrieval.delegated, false, label);
      assert.match(r.prose, /No writes, emails, calls, assignments, imports, review-state changes or specialist execution occurred/, label);
    };

    // 1. Fresh session.
    const fresh = engine.open({ tenantId: '21', page: 'command-deck' }).sessionId;
    verify(await engine.ask({ sessionId: fresh, question: PRODUCTION_INVESTIGATION_PROMPT }), 'fresh session');

    // 8. After a previous Workspace turn (the production session's state).
    const continued = engine.open({ tenantId: '21', page: 'command-deck' }).sessionId;
    const prior = await engine.ask({ sessionId: continued, question: 'Brief me on my prospects.' });
    assert.equal(prior.executionDomain, 'workspace', 'prior turn is a Workspace turn');
    assert.equal(engine.sessions.get(continued).executionDomain, 'workspace');
    const after = await engine.ask({ sessionId: continued, question: PRODUCTION_INVESTIGATION_PROMPT });
    verify(after, 'after a Workspace turn');
    assert.equal(after.domainSwitch, null);
    assert.equal(engine.sessions.get(continued).executionDomain, 'workspace');

    assert.ok(statements.length > 0);
    for (const sql of statements) assert.match(sql.trim(), /^(?:SELECT|WITH)\b/i, `non-read statement: ${sql.slice(0, 80)}`);
    assert.equal(await snapshot(db), before, 'database unchanged');
    assert.deepEqual(blocked, [], 'no network beyond the disposable database');
  } finally {
    await db.end();
    await postgres.stop();
  }
});
