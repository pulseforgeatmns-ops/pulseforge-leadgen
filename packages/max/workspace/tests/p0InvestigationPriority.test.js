'use strict';
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { composeRecords, evaluate } = require('../ProspectRecordIntelligence');
const { classifyOperationalRead } = require('../OperationalReadIntent');
const { fixture, bundle, QUESTION } = require('./fixtures/investigationProspects');
test('production failure: all six review-only records can receive read-only investigation priority while outreach stays blocked', () => {
  const rows = fixture();
  assert.ok(rows.every(row => !evaluate(row).eligible));
  const answer = composeRecords(bundle(rows), classifyOperationalRead(QUESTION));
  assert.equal(answer.items.length, 6);
  assert.equal(answer.items[0].priority?.mode, 'investigation');
  assert.match(answer.prose, /outreach.*blocked/i);
});
test('existing advisory conflict: should I prioritize these recorded contact_now prospects is a record read', () => {
  const read = classifyOperationalRead('Which of these contact_now prospects should I prioritize for investigation?');
  assert.equal(read.isRead, true);
});

const { tenantCriteria, recordedDetails, fingerprint } = require('../ProspectInvestigationPriority');
const { recordOperationalRead } = require('../OperationalReadIntent');
const { operatingStructured } = require('../OperatingEvidenceRetrieval');
const { createHermeticWorkspaceEngine, unavailableOperatingEvidence } = require('./workspaceTestHarness');
const { createMemoryStore, startClientInterview, postInterviewMessage, approveBlueprint } = require('../../../../services/clientIntelligenceInterview');
const rankRead = extra => ({ wantsRanking: true, reviewState: 'contact_now', priorityMode: 'investigation', ...extra });
const criteria = (clientId = 21, services = 'Bridge loans. Financing range: $9,000–$15,000.') => ({ _bundle: {
  tenantId: String(clientId), knowledgeState: 'available', summary: { clientId, approved: true,
    semanticAuthority: 'CANONICAL', canonicalSnapshotId: 'synthetic-approved-snapshot' },
  contract: { services, constraints: 'Verify title and exit evidence.' } } });
const answerFor = (rows = fixture(), read = rankRead(), understanding) =>
  composeRecords(bundle(rows), read, { businessUnderstanding: understanding });

test('all six represented without a top request; explicit top five returns five when differentiated', () => {
  assert.equal(answerFor().items.length, 6);
  const top = answerFor(fixture(), rankRead({ topCount: 5 }));
  assert.equal(top.items.length, 5); assert.match(top.prose, /showing 5/);
  assert.equal(classifyOperationalRead(QUESTION).topCount, null);
});
test('boundary ties share position, include every tied record and disclose why top five returns six', () => {
  const rows = fixture(); rows[5].notes = rows[4].notes;
  const r = answerFor(rows, rankRead({ topCount: 5 }));
  assert.equal(r.items.length, 6); assert.equal(r.items[4].priority.position, 5); assert.equal(r.items[5].priority.position, 5);
  assert.match(r.prose, /cutoff is tied/);
});
test('no meaningful differentiation leaves all six honestly unranked, even with qualification scores and review states', () => {
  const rows = fixture().map(row => ({ ...row, notes: 'Fit evidence: A public investor profile.\nRisks: Current deal unknown.' }));
  const r = answerFor(rows); assert.equal(r.items.length, 6);
  assert.ok(r.items.every(i => i.priority === null)); assert.match(r.prose, /Unranked investigation/);
});
test('missing contact channels do not block analytical priority; no outreach gate changes or input mutations', () => {
  const rows = fixture(); const before = JSON.stringify(rows);
  const r = answerFor(rows);
  assert.equal(r.items[0].priority.position, 1); assert.match(r.prose, /Contact channel not recorded/);
  assert.ok(rows.every(row => !evaluate(row).eligible)); assert.equal(JSON.stringify(rows), before);
  const conflictingFlags = rows.map(row => ({ ...row, ao_outreach_review_required: false }));
  const stillBlocked = answerFor(conflictingFlags);
  assert.doesNotMatch(stillBlocked.prose, /recorded gates pass/);
  assert.match(stillBlocked.prose, /Outreach eligibility: blocked/);
});
test('contact_now and qualify_first may be analyzed; hold and reject remain visible but excluded from priority', () => {
  const rows = fixture();
  rows[1].acquisition_metadata.reviewState = 'qualify_first';
  rows[2].acquisition_metadata.reviewState = 'hold';
  rows[3].acquisition_metadata.reviewState = 'reject';
  const r = answerFor(rows, rankRead({ reviewState: null }));
  assert.equal(r.items.length, 6);
  assert.ok(r.items.find(i => i.recordId === rows[1].id).priority);
  for (const n of [2,3]) assert.equal(r.items.find(i => i.recordId === rows[n].id).priority, null);
  assert.ok(rows.every(row => !evaluate(row).eligible));
});
test('DNC, setter holds and terminal statuses are not promoted by investigation policy', () => {
  const rows = fixture(); rows[0].do_not_contact = true; rows[1].setter_status = 'hold'; rows[2].status = 'disqualified';
  const r = answerFor(rows);
  for (const row of rows.slice(0,3)) assert.equal(r.items.find(i => i.recordId === row.id).priority, null);
});
test('authorized canonical tenant criteria are cited and financing concerns stay separate from investigation priority', () => {
  const r = answerFor(fixture(), rankRead(), criteria());
  assert.match(r.prose, /blueprint:21:synthetic-approved-snapshot/);
  assert.match(r.prose, /within the recorded business range/);
  assert.match(r.prose, /outside the recorded business range \$9,000–\$15,000/);
  assert.equal(r.items[1].recordId, fixture()[1].id); // broader financing request is still a case to investigate
  assert.match(r.prose, /not.*predicted funding likelihood/);
});
test('absent, unapproved, noncanonical, failed and foreign tenant criteria never supply a range', () => {
  const wrong = criteria(22); wrong._bundle.contract.services += ' FOREIGN_CRITERIA';
  const unapproved = criteria(); unapproved._bundle.summary.approved = false;
  const failed = criteria(); failed._bundle.knowledgeState = 'retrieval_failure';
  const noncanonical = criteria(); noncanonical._bundle.summary.semanticAuthority = 'LEGACY';
  for (const u of [undefined, wrong, unapproved, failed, noncanonical]) {
    const r = answerFor(fixture(), rankRead(), u);
    assert.match(r.prose, /criteria are unavailable/); assert.doesNotMatch(r.prose, /FOREIGN_CRITERIA|\$9,000–\$15,000/);
  }
});
test('a different tenant uses its own actual criteria; plain dollar ranges and ambiguous criteria are not assumed to be financing limits', () => {
  const b = bundle(fixture(77));
  const r = composeRecords(b, rankRead(), { businessUnderstanding: criteria(77, 'Financing range: $1,000–$2,000.') });
  assert.match(r.prose, /blueprint:77:/); assert.match(r.prose, /\$1,000–\$2,000/); assert.doesNotMatch(r.prose, /prospect:21:/);
  for (const services of ['Loan fees $100–$500.', 'Financing range: $1,000–$2,000. Loan range: $5,000–$6,000.']) {
    assert.equal(tenantCriteria(criteria(21, services), bundle(fixture())).range, null);
  }
});
test('conflicting amounts, unsupported amount formats and unknown security do not become financing facts', () => {
  const rows = fixture(); rows[0].notes += '\nRisks: Financing request: $18,000';
  const r = answerFor(rows, rankRead(), criteria());
  assert.ok(r.items.every(i => i.priority === null)); assert.match(r.prose, /Conflicting or unparsed/);
  assert.equal(recordedDetails('Financing request: $12k').amount, null);
  assert.equal(recordedDetails('Security position: unknown').count, 0);
  const blankStage = recordedDetails('Deal stage:   \nSecurity position: first lien claimed');
  assert.equal(blankStage.count, 1); assert.equal(blankStage.values.stage, undefined);
});
test('deterministic order and stable fingerprints are independent of input and object key order', () => {
  const rows = fixture(); assert.deepEqual(answerFor(rows).items, answerFor([...rows].reverse()).items);
  assert.equal(fingerprint({ a: 1, b: 2 }), fingerprint({ b: 2, a: 1 }));
});
test('missing identity/source/notes, duplicate identities, pagination and incomplete retrieval block global ranking', () => {
  for (const patch of [{ first_name: null, last_name: null }, { source: null }, { notes: null }]) {
    const rows = fixture(); Object.assign(rows[0], patch);
    assert.ok(answerFor(rows).items.every(i => !i.priority));
  }
  for (const patch of [{ complete: false, nextCursor: fixture()[5].id }, { after: fixture()[0].id }]) {
    const b = bundle(fixture()); Object.assign(b.prospects.records, patch);
    const r = composeRecords(b, rankRead()); assert.ok(r.items.every(i => !i.priority)); assert.match(r.prose, /global ranking is withheld/);
  }
  const rows = fixture(); assert.ok(answerFor([...rows, rows[0]]).items.every(i => !i.priority));
});
test('failed and unavailable retrieval never produce a verified ranking', () => {
  for (const status of ['failed', 'unavailable']) {
    const b = bundle(fixture()); b.prospects.records.status = status;
    const r = composeRecords(b, rankRead()); assert.equal(r.items.length, 0); assert.match(r.prose, /no records were verified/i);
  }
});
test('foreign and ownerless rows cannot enter analytical results, calculations or citations', () => {
  const rows = fixture(); const foreign = { ...fixture(22)[0], notes: 'FOREIGN_SECRET' };
  const b = bundle([...rows, foreign, { ...foreign, client_id: undefined }]);
  assert.doesNotMatch(JSON.stringify(composeRecords(b, rankRead())), /FOREIGN_SECRET|prospect:22:/);
});
test('feature rollback restores PR 1114 outreach-only behavior without relaxing any gate', () => {
  const old = process.env.MAX_PROSPECT_INVESTIGATION_PRIORITY;
  try {
    process.env.MAX_PROSPECT_INVESTIGATION_PRIORITY = 'false';
    const r = answerFor(); assert.ok(r.items.every(i => !i.priority)); assert.match(r.prose, /0 records meet.*outreach/);
  } finally { if (old === undefined) delete process.env.MAX_PROSPECT_INVESTIGATION_PRIORITY; else process.env.MAX_PROSPECT_INVESTIGATION_PRIORITY = old; }
});

async function openEngine() {
  const rows = fixture(); const store = createMemoryStore();
  let interview = await startClientInterview({ clientId: 21, forceNew: true }, { store });
  for (const answer of ['Lending to investors.', 'Bridge loans. Financing range: $9,000–$15,000.',
    'Real-estate operators.', 'Unsupported borrowers.', 'Example County.', 'Clear terms.', 'Professional.', 'Qualified pipeline.', 'Funded loans.']) {
    interview = await postInterviewMessage(interview.interviewId, answer, { store });
  }
  await approveBlueprint(interview.blueprint.id, { store });
  const engine = createHermeticWorkspaceEngine({ clientIntelligenceOpts: { store },
    operatingEvidenceOpts: { ...unavailableOperatingEvidence(), loadProspects: async input => {
      assert.equal(input.clientId, 21);
      return { available: true, counts: { total: rows.length }, records: { status: 'found', complete: true,
        rows: [...rows, { ...fixture(22)[0], notes: 'FOREIGN_SECRET' }] } };
    } } });
  const opened = engine.open({ tenantId: '21', page: 'command-deck' });
  return { rows, engine, ask: question => engine.ask({ sessionId: opened.sessionId, question }) };
}
test('original production request through approved Blueprint, deterministic presenter and session returns six with zero domain execution', async () => {
  const { ask, engine, rows } = await openEngine(); const before = JSON.stringify(rows);
  const r = await ask(QUESTION);
  assert.equal(r.structured.supportingEvidence.length, 6);
  assert.equal(r.structured.supportingEvidence[0].priority.mode, 'investigation');
  assert.equal(r.structured.metadata.executed, false); assert.equal(r.retrieval.delegated, false);
  assert.equal(r.structured.metadata.specialistDelegated, false);
  assert.doesNotMatch(JSON.stringify(r), /FOREIGN_SECRET|prospect:22:/);
  assert.equal(r.presentation, 'prospect_records'); assert.match(r.prose, /showing 6/);
  assert.match(r.prose, /\$9,000–\$15,000/);
  assert.equal(JSON.stringify(rows), before);
  assert.doesNotMatch(JSON.stringify(engine.sessions.get(r.sessionId)), /FOREIGN_SECRET/);
  const evidence = r.structured.supportingEvidence[0];
  assert.ok(evidence.recordFingerprint); assert.match(evidence.recordedClaims.notes, /Fit evidence:/);
  assert.equal(evidence.recordedClaims.epistemic, 'recorded_claim');
});
test('follow-up keeps selected IDs, review filter, evidence references and previous investigation order', async () => {
  const { ask, engine } = await openEngine(); const first = await ask(QUESTION);
  const ctx = engine.sessions.get(first.sessionId).context;
  assert.equal(ctx.lastOperationalRead.reviewState, 'contact_now');
  const selected = ctx.lastOperationalRead.investigationRanking.selectedRecordIds;
  const next = await ask('Why is the first prospect ranked highest?');
  assert.deepEqual(next.structured.supportingEvidence.map(i => i.recordId), selected);
  assert.match(next.prose, /previous selected cohort/);
  assert.equal(next.structured.supportingEvidence[0].priority.position, 1);
});
test('changed or removed records invalidate earlier positions without substituting unrelated records', async () => {
  for (const change of ['update', 'remove', 'review']) {
    const { ask, rows, engine } = await openEngine(); const first = await ask(QUESTION);
    const oldId = rows[0].id;
    if (change === 'update') rows[0].notes += '\nNew unverified information';
    if (change === 'remove') rows.shift();
    if (change === 'review') rows[0].acquisition_metadata.reviewState = 'hold';
    const next = await ask('Why is the first prospect ranked highest?');
    assert.match(next.prose, /previous ranking is stale/);
    assert.ok(next.structured.supportingEvidence.every(i => !i.priority));
    if (change === 'remove') assert.match(next.prose, new RegExp(oldId));
    if (change === 'review') assert.match(next.prose, /review: hold/);
    assert.equal(engine.sessions.get(first.sessionId).context.lastOperationalRead.investigationRanking, null);
  }
});
test('changed criteria invalidate previous rankings; a foreign session ranking cannot supply IDs or criteria', () => {
  const rows = fixture(); const b = bundle(rows); const first = answerFor(rows, rankRead(), criteria());
  const s = operatingStructured(first.prose, { ...first, recordIntelligence: true });
  const session = { operatorTurnSeq: 1, context: { tenantId: '21' } };
  recordOperationalRead(session, rankRead(), s); session.operatorTurnSeq++;
  const read = classifyOperationalRead('Why is the first prospect ranked highest?', { session });
  const next = composeRecords(b, read, { businessUnderstanding: criteria(21, 'Financing range: $20,000–$40,000.') });
  assert.match(next.prose, /previous ranking is stale/); assert.ok(next.items.every(i => !i.priority));
  const foreign = { ...read, previousRanking: { ...read.previousRanking, tenantId: '22', selectedRecordIds: ['FOREIGN_ID'] } };
  assert.doesNotMatch(JSON.stringify(composeRecords(b, foreign)), /FOREIGN_ID/);
});
test('should I is a bounded read-only fix: generic advisory and execution requests keep existing exclusions', () => {
  for (const q of ['What campaign should I run?', 'How should I improve my prospects?', 'Send emails to these contact_now prospects.']) {
    assert.equal(classifyOperationalRead(q).isRead, false, q);
  }
});
test('unranked prior response cannot become an invented first-ranked record in a follow-up', async () => {
  const { ask, rows } = await openEngine();
  rows.forEach(row => { row.notes = 'Fit evidence: Public investor profile only.'; });
  const first = await ask(QUESTION); assert.ok(first.structured.supportingEvidence.every(i => !i.priority));
  const next = await ask('Why is the first prospect ranked highest?');
  assert.match(next.prose, /No investigation ranking was produced/);
  assert.ok(next.structured.supportingEvidence.every(i => !i.priority));
});
test('top-five follow-up explains the actual selected five without introducing the sixth record', async () => {
  const { ask } = await openEngine(); const first = await ask('Rank my top five contact_now prospects for investigation.');
  assert.equal(first.structured.supportingEvidence.length, 5);
  const next = await ask('Why is the first prospect ranked highest?');
  assert.deepEqual(next.structured.supportingEvidence.map(i => i.recordId), first.structured.supportingEvidence.map(i => i.recordId));
});
test('approved original facts preserve monetary commas and provenance rather than trusting the lossy summary projection', () => {
  const u = criteria(); u._bundle.summary.semanticAuthority = 'session_archival'; u._bundle.summary.canonicalSnapshotId = null;
  Object.assign(u._bundle.summary, { blueprintId: 'approved-id', status: 'approved', businessFacts: {
    services: [{ id: 'fact-a', provenance: 'operator-answer-a', epistemic_state: 'KNOWN', value: 'Financing range: $11,000–$14,000.' }],
  } }); u._bundle.blueprintSource = 'blueprint'; u._bundle.contract.services = 'Financing range: $11, 000–$14, and 000';
  const r = answerFor(fixture(), rankRead(), u);
  assert.match(r.prose, /\$11,000–\$14,000/);
  assert.deepEqual(r.investigationPriority.criteriaEvidence.factRefs, [{ id: 'fact-a', provenance: 'operator-answer-a' }]);
});
test('review-state filters for qualify_first, hold and reject are retained by the existing record classifier', () => {
  for (const state of ['qualify_first', 'hold', 'reject']) {
    const read = classifyOperationalRead(`Rank my ${state} prospects for investigation.`);
    assert.equal(read.reviewState, state);
    const rows = fixture(); rows[1].acquisition_metadata.reviewState = state;
    const r = answerFor(rows, read); assert.equal(r.items.length, 1); assert.equal(r.items[0].recordId, rows[1].id);
  }
});
test('contradictory labelled security claims across Fit and Risks cannot establish a comparative ranking', () => {
  const rows = fixture(); rows[0].notes += '\nRisks: Security position: second lien claimed';
  const r = answerFor(rows); assert.ok(r.items.every(i => !i.priority)); assert.match(r.prose, /Conflicting or unparsed/);
});
test('multiline imported fit and risk evidence is retained and parsed without dropping later lines', () => {
  const rows = fixture(); rows[0].notes = 'Fit evidence: Financing request: $12,000\nDeal stage: purchase contract\nSecurity position: first lien claimed\nExit plan: sale\nClosing date: 2026-12-01\nRisks: Verify title\nand borrower information\nSources: https://example.invalid';
  const r = answerFor(rows); assert.equal(r.items[0].priority.evidenceCoverage, 5);
  assert.match(r.prose, /and borrower information/); assert.match(r.items[0].recordedClaims.notes, /Closing date/);
});
test('record timestamp changes invalidate fingerprints even when a loader supplies Date objects', () => {
  assert.notEqual(fingerprint({ updated_at: new Date('2026-11-10') }), fingerprint({ updated_at: new Date('2026-11-11') }));
});
