'use strict';
// P0 narrative-evidence correction: real review-only imports store deal
// details as Fit evidence prose. Before this correction every such record had
// 0/5 "labelled" claims, so a cohort of six was always unranked and Max listed
// recorded claims (e.g. a $35k request, proposed first lien, DSCR exit) as
// missing. Fixtures are built through the real normalizeContact/noteFor path.
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { composeRecords, evaluate } = require('../ProspectRecordIntelligence');
const { classifyOperationalRead, recordOperationalRead } = require('../OperationalReadIntent');
const { operatingStructured } = require('../OperatingEvidenceRetrieval');
const { extractDealClaims, section, EXTRACTOR } = require('../ProspectDealClaims');
const { tenantCriteria, recordedDetails } = require('../ProspectInvestigationPriority');
const { narrativeFixture, NARRATIVE_INPUTS } = require('./fixtures/narrativeInvestigationProspects');
const { fixture, bundle, QUESTION } = require('./fixtures/investigationProspects');
const { createHermeticWorkspaceEngine, unavailableOperatingEvidence } = require('./workspaceTestHarness');
const { createMemoryStore, startClientInterview, postInterviewMessage, approveBlueprint } = require('../../../../services/clientIntelligenceInterview');

const rankRead = extra => ({ wantsRanking: true, reviewState: 'contact_now', priorityMode: 'investigation', ...extra });
const criteria = (clientId = 21, services = 'Bridge loans. Financing range: $5,000–$20,000.') => ({ _bundle: {
  tenantId: String(clientId), knowledgeState: 'available', summary: { clientId, approved: true,
    semanticAuthority: 'CANONICAL', canonicalSnapshotId: 'synthetic-approved-snapshot' },
  contract: { services, constraints: 'Verify title and exit evidence.' } } });
const answerFor = (rows = narrativeFixture(), read = rankRead(), understanding) =>
  composeRecords(bundle(rows), read, { businessUnderstanding: understanding });
const fit = s => extractDealClaims(`Fit evidence: ${s}`);
const byName = (answer, rows, first) => answer.items.find(i => i.recordId === rows.find(r => r.first_name === first).id);

// 1. Real-format cohort ------------------------------------------------------
test('real import format: narrative fit evidence yields recorded-unverified claims and a supported ranking', () => {
  const rows = narrativeFixture();
  assert.ok(rows.every(r => !/^(?:Financing request|Security position|Exit plan|Deal stage|Closing date):/m.test(r.notes)),
    'fixture must not contain labelled claims');
  const zane = recordedDetails(rows[0].notes);
  assert.equal(zane.amount, 35000);
  assert.deepEqual(Object.keys(zane.claims).sort(), ['amount', 'closing', 'exit', 'security', 'stage']);
  assert.equal(zane.claims.security.normalized, 'first lien');
  assert.equal(zane.claims.exit.normalized, 'DSCR refinance');
  assert.ok(Object.values(zane.claims).every(c => c.origin === 'fit_narrative' && c.epistemic === 'recorded_unverified'));

  const r = answerFor(rows);
  assert.equal(r.investigationPriority.ranked, true);
  assert.equal(r.items.length, 6);
  assert.deepEqual(r.items.map(i => i.priority?.position), [1, 2, 3, 4, 5, 5]);
  assert.deepEqual(r.items.map(i => i.priority?.evidenceCoverage), [5, 4, 2, 1, 0, 0]);
  assert.equal(r.investigationPriority.extractor, EXTRACTOR);
});

test('every extracted claim is traceable to its exact recorded Fit evidence text', () => {
  for (const row of narrativeFixture()) {
    const fitText = section(row.notes, 'Fit evidence');
    const d = recordedDetails(row.notes);
    for (const claim of Object.values(d.claims)) {
      for (const snippet of claim.snippets) {
        assert.ok(row.notes.includes(snippet), `${snippet} not in notes`);
        assert.ok(fitText.includes(snippet), `${snippet} not in fit evidence`);
      }
      assert.ok(claim.snippet.includes(claim.match), `${claim.match} not in ${claim.snippet}`);
      assert.ok(claim.match.includes(claim.value), `${claim.value} not in ${claim.match}`);
    }
    for (const u of d.unparsed) for (const snippet of u.snippets) assert.ok(fitText.includes(snippet));
  }
  const r = answerFor();
  for (const item of r.items) {
    for (const claim of Object.values(item.recordedClaims.claims)) {
      assert.ok(item.recordedClaims.notes.includes(claim.snippet));
      assert.match(item.claim, new RegExp(claim.snippet.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
    }
  }
});

// 2. Risks are never mined ---------------------------------------------------
test('risk descriptions never produce claims, even when they mention liens, ranges or amounts', () => {
  const rows = narrativeFixture();
  for (const row of rows) {
    const risks = section(row.notes, 'Risks');
    for (const claim of Object.values(recordedDetails(row.notes).claims)) {
      for (const snippet of claim.snippets) assert.ok(!risks.includes(snippet), `claim from risks: ${snippet}`);
    }
  }
  const onlyRisks = extractDealClaims('Fit evidence: Public investor profile.\nRisks: Requesting $15k with a first lien; exit via sale; closing scheduled for 2026-12-01.');
  assert.equal(onlyRisks.count, 0); assert.deepEqual(onlyRisks.unparsed, []);
});

// 3. Notes and questions never supply criteria -------------------------------
test('ranges in prospect notes or in the operator question never become financing criteria', () => {
  const rows = narrativeFixture();
  const question = 'Of these six contact_now prospects, which should Levi prioritize first, considering his $5,000–$20,000 working range? Rank only where evidence supports it. Do not initiate outreach.';
  const read = classifyOperationalRead(question);
  for (const understanding of [undefined, criteria(21, 'Bridge loans for rehab projects.')]) {
    const r = composeRecords(bundle(rows), read, { businessUnderstanding: understanding });
    assert.doesNotMatch(r.prose, /(?:within|outside) the recorded business range/);
    assert.match(r.prose, /ranges stated in the request or in prospect notes are not authorized criteria/i);
    assert.match(r.prose, /Dollar ranges appear in recorded prospect notes \("\$5k–\$20k", "\$8k–\$12k"\)/);
    assert.match(r.prose, /not used for financing fit/);
  }
  const notesWithLabelledRange = narrativeFixture().map(row => ({ ...row, notes: `${row.notes}\nRisks: Financing range: $5,000–$20,000.` }));
  assert.equal(tenantCriteria(undefined, bundle(notesWithLabelledRange)).range, null);
});

// 4. Approved criteria still drive financing concerns ------------------------
test('an approved tenant range flags the $35k request as a financing concern without changing investigation position', () => {
  const rows = narrativeFixture();
  const r = answerFor(rows, rankRead(), criteria());
  const zane = byName(r, rows, 'Zane'); const brook = byName(r, rows, 'Brook'); const cam = byName(r, rows, 'Cam');
  assert.equal(zane.priority.position, 1);
  assert.match(zane.claim, /Recorded request \$35,000 is outside the recorded business range \$5,000–\$20,000; this is a potential mismatch, not a financing decision/);
  assert.match(brook.claim, /Recorded request is within the recorded business range/);
  assert.match(cam.claim, /A financing request is recorded but is a range, not a single amount \("\$8k–\$12k"\); financing fit cannot be assessed/);
});

// 5. Ambiguity is record-local -----------------------------------------------
test('ranges, competing amounts, multiple properties and conflicting narrative liens stay record-local', () => {
  const rows = narrativeFixture();
  const eli = recordedDetails(rows[4].notes);
  assert.equal(eli.count, 0); assert.deepEqual(eli.conflicts, []);
  assert.deepEqual(eli.unparsed.map(u => [u.field, u.reason]), [['amount', 'multiple_values'], ['security', 'multiple_values']]);
  const cam = recordedDetails(rows[2].notes);
  assert.deepEqual(cam.unparsed.map(u => [u.field, u.reason]), [['amount', 'range']]);
  const r = answerFor(rows);
  assert.equal(r.investigationPriority.ranked, true);
  assert.match(r.prose, /No record contains contradictory labelled claims\. 2 record\(s\) contain recorded details that could not be compared/);
  assert.match(byName(r, rows, 'Eli').claim, /Recorded but not comparable for this record: financing request \(more than one competing value: "\$12k", "\$18k"\); lien\/security position/);

  for (const [text, field, reason] of [
    ['Requesting about $20k.', 'amount', 'approximate'], ['Requesting $20k+.', 'amount', 'approximate'],
    ['Requesting $1.2M.', 'amount', 'unsupported_format'], ['Requesting $12k or $15k.', 'amount', 'multiple_values'],
    ['Exit via sale or refinance.', 'exit', 'alternatives'], ['Under contract on a duplex. Purchased a fourplex last month.', 'stage', 'multiple_values'],
  ]) {
    const d = fit(text);
    assert.deepEqual(d.unparsed.map(u => [u.field, u.reason]), [[field, reason]], text);
    assert.deepEqual(d.conflicts, [], text); assert.equal(d.values[field], undefined, text);
  }
  const repeated = fit('Requesting $20k. Confirmed requesting $20k again.');
  assert.equal(repeated.amount, 20000); assert.deepEqual(repeated.unparsed, []);
});

// 6. Absent vs recorded-unverified vs verified -------------------------------
test('absent, recorded-unverified and verified states stay distinct; nothing is marked verified by import', () => {
  for (const notes of ['Security position: unknown', 'Security position: TBD', 'Security position: not verified', 'Security position: none']) {
    const d = recordedDetails(notes); assert.equal(d.count, 0, notes); assert.deepEqual(d.absent.includes('security'), true, notes);
  }
  for (const notes of ['Security position: first lien (unverified)', 'Security position: first lien claimed, not yet verified']) {
    const d = recordedDetails(notes); assert.equal(d.count, 1, notes); assert.equal(d.claims.security.epistemic, 'recorded_unverified');
  }
  for (const text of ['Proposed first lien, not yet verified.', 'Proposed first lien not yet verified.']) {
    assert.equal(fit(text).claims.security?.normalized, 'first lien', text);
  }
  for (const text of ['Not seeking $20k.', 'No exit plan recorded.', 'Lien position unknown.', 'May need $15k.',
    'No first lien available.', 'Might exit via refinance.', 'Closing date unknown.']) {
    const d = fit(text); assert.equal(d.count, 0, text); assert.deepEqual(d.unparsed, [], text);
  }
  const labelledNoNegationLoss = recordedDetails('Exit plan: DSCR refinance, no seasoning issue');
  assert.equal(labelledNoNegationLoss.values.exit, 'DSCR refinance, no seasoning issue');
  const r = answerFor();
  for (const item of r.items) {
    assert.equal(item.recordedClaims.epistemic, 'recorded_claim');
    for (const claim of Object.values(item.recordedClaims.claims)) assert.equal(claim.epistemic, 'recorded_unverified');
    assert.doesNotMatch(JSON.stringify(item.recordedClaims), /"epistemic":"verified"/);
  }
  assert.match(r.prose, /every recorded claim above is unverified until checked against source documents/);
});

test('rehab budgets, purchase prices, ARVs and property values are never financing requests', () => {
  for (const text of ['Rehab budget is $40k.', 'Rehab budget of $60k on a $180k purchase; ARV $260k.', 'Purchase price $150k.',
    'ARV $250k.', 'Property valued at $300k.', 'Seeking a $250k property.', 'Looking for $200k+ deals.',
    'Needs $50k rehab budget.', 'Wants $300k ARV deals.', 'Needs $40k down payment.']) {
    const d = fit(text); assert.equal(d.amount, null, text); assert.equal(d.values.amount, undefined, text);
    assert.equal(d.unparsed.some(u => u.field === 'amount'), false, text);
  }
  const mixed = fit('Requesting $35k on a $180k purchase and $40k rehab budget.');
  assert.equal(mixed.amount, 35000); assert.deepEqual(mixed.unparsed, []);
  assert.equal(fit('$35k bridge loan request.').amount, 35000);
  assert.equal(fit('Funding request of $20k.').amount, 20000);
});

test('exit timelines are never closing dates; explicit closing statements are', () => {
  for (const text of ['Exit via sale after a 4-month rehab.', 'Exit via DSCR refinance, which should close in 6 months.',
    'Refinance should close in 6 months.', 'Exit: DSCR refinance in 6 months.', 'Closed on 3 flips in 2025.',
    'Located close to downtown in 2 weeks of searching.']) {
    assert.equal(fit(text).values.closing, undefined, text);
  }
  assert.equal(fit('Closing scheduled for 2026-12-01.').values.closing, '2026-12-01');
  assert.equal(fit('Closing scheduled Dec 15.').values.closing, 'Dec 15');
  const relative = fit('Targeting close in ~3 weeks.').claims.closing;
  assert.equal(relative.value, '~3 weeks'); assert.equal(relative.relative, true);
  const rows = narrativeFixture();
  assert.match(byName(answerFor(rows), rows, 'Zane').claim, /closing date: ~3 weeks \(relative to when the note was recorded\)/);
  assert.equal(recordedDetails(rows[3].notes).values.closing, undefined); // Dale: exit after a 5-month rehab
});

test('other debt, labelled precedence and missing evidence are handled narrowly', () => {
  assert.equal(fit('Requesting a second lien behind an existing first mortgage.').claims.security.normalized, 'second lien');
  const labelled = recordedDetails('Fit evidence: Exit plan: sale; plans to refinance later.');
  assert.equal(labelled.values.exit, 'sale'); assert.equal(labelled.claims.exit.origin, 'label');
  assert.equal(fit('Has purchased 12 properties since 2019.').values.stage, undefined);
  for (const notes of [null, '', 'Review state: contact now.\nRisks: Requesting $15k with a first lien.']) {
    const d = recordedDetails(notes); assert.equal(d.count, 0); assert.deepEqual(d.absent.length, 5);
  }
});

// 7. Genuine contradictions still block --------------------------------------
test('contradictory labelled claims still withhold the cohort ranking; narrative ambiguity does not', () => {
  const rows = narrativeFixture();
  rows[1].notes = rows[1].notes.replace('Fit evidence: ', 'Fit evidence: Security position: first lien\n');
  rows[1].notes += '\nRisks: Security position: second lien';
  const r = answerFor(rows);
  assert.equal(r.investigationPriority.ranked, false); assert.ok(r.items.every(i => !i.priority));
  assert.match(r.prose, /Conflicting or unparsed/);
  assert.match(r.prose, /1 record\(s\) contain contradictory labelled claims, so no comparative ranking is given/);
  assert.match(byName(r, rows, 'Brook').claim, /Contradictory labelled claims: lien\/security position/);
  assert.match(byName(r, rows, 'Brook').claim, /Financing fit: Contradictory recorded claims \(lien\/security position\)/);
  const conflictingAmounts = fixture(); conflictingAmounts[0].notes += '\nRisks: Financing request: $18,000';
  assert.deepEqual(recordedDetails(conflictingAmounts[0].notes).conflicts, ['amount']);
});

// 8. All-thin cohort stays unranked ------------------------------------------
test('six thin narrative records stay honestly unranked', () => {
  const thin = NARRATIVE_INPUTS.map((input, i) => ({ ...input, name: `Thin${i} Example`,
    fit: 'Public profile describes real-estate investing; recently completed several flips.', risks: 'No active deal recorded.' }));
  const r = answerFor(narrativeFixture(21, thin));
  assert.equal(r.investigationPriority.ranked, false); assert.ok(r.items.every(i => i.priority === null));
  assert.match(r.prose, /Unranked investigation candidates/);
  assert.match(r.items[0].claim, /Recorded claims \(unverified\): No explicit deal claims recorded\. Recorded fit narrative \(unverified\): Public profile/);
});

// Wording: recorded claims are not described as missing -----------------------
test('recorded-but-unverified information is not described as missing', () => {
  const rows = narrativeFixture(); const r = answerFor(rows);
  const zane = byName(r, rows, 'Zane').claim;
  assert.match(zane, /Investigation position 1: 5\/5 recorded deal claims available to check\./);
  assert.match(zane, /Recorded claims \(unverified\): financing request: \$35k \(= \$35,000\); deal stage: property acquired \(recorded as "a purchased SFR"\); lien\/security position: first lien; exit plan: DSCR refinance; closing date: ~3 weeks/);
  assert.match(zane, /Claim source text \(exact, as recorded\): "Active flipper requesting \$35k for rehab on a purchased SFR\." \| "Proposed first lien on the subject property\."/);
  assert.doesNotMatch(zane, /Not recorded:|Missing verification/);
  const brook = byName(r, rows, 'Brook').claim;
  assert.match(brook, /Not recorded: closing date\./);
  assert.doesNotMatch(brook, /Not recorded:[^\n]*(?:financing request|lien\/security|exit plan|deal stage)/);
  assert.match(brook, /Read-only next step: verify the recorded financing request, deal stage, lien\/security position, exit plan against source documents, and locate or reconcile evidence for closing date\./);
});

test('a ranking produced by the earlier extractor is stale for follow-ups', () => {
  const rows = narrativeFixture(); const b = bundle(rows); const first = answerFor(rows);
  const s = operatingStructured(first.prose, { ...first, recordIntelligence: true });
  const session = { operatorTurnSeq: 1, context: { tenantId: '21' } };
  recordOperationalRead(session, rankRead(), s); session.operatorTurnSeq++;
  const read = classifyOperationalRead('Why is the first prospect ranked highest?', { session });
  assert.match(composeRecords(b, read).prose, /previous selected cohort/);
  const legacy = { ...read, previousRanking: { ...read.previousRanking, extractor: undefined } };
  const next = composeRecords(b, legacy);
  assert.match(next.prose, /previous ranking is stale/); assert.ok(next.items.every(i => !i.priority));
});

// 9. No side effects ----------------------------------------------------------
async function openEngine(rows) {
  const store = createMemoryStore();
  let interview = await startClientInterview({ clientId: 21, forceNew: true }, { store });
  for (const answer of ['Lending to investors.', 'Bridge loans. Financing range: $5,000–$20,000.',
    'Real-estate operators.', 'Unsupported borrowers.', 'Example County.', 'Clear terms.', 'Professional.', 'Qualified pipeline.', 'Funded loans.']) {
    interview = await postInterviewMessage(interview.interviewId, answer, { store });
  }
  await approveBlueprint(interview.blueprint.id, { store });
  const engine = createHermeticWorkspaceEngine({ clientIntelligenceOpts: { store },
    operatingEvidenceOpts: { ...unavailableOperatingEvidence(), loadProspects: async input => {
      assert.equal(input.clientId, 21);
      return { available: true, counts: { total: rows.length }, records: { status: 'found', complete: true,
        rows: [...rows, { ...narrativeFixture(22)[0], notes: 'FOREIGN_SECRET requesting $99k' }] } };
    } } });
  const opened = engine.open({ tenantId: '21', page: 'command-deck' });
  return { engine, ask: question => engine.ask({ sessionId: opened.sessionId, question }) };
}

test('original production prompt over narrative imports: ranked, traceable, outreach blocked, zero execution or writes', async () => {
  const rows = narrativeFixture(); const before = JSON.stringify(rows);
  const { ask, engine } = await openEngine(rows);
  const r = await ask(QUESTION);
  assert.equal(r.presentation, 'prospect_records');
  assert.equal(r.structured.supportingEvidence.length, 6);
  assert.equal(r.structured.metadata.investigationPriority.ranked, true);
  assert.equal(r.structured.supportingEvidence[0].priority.position, 1);
  assert.equal(r.structured.supportingEvidence[0].recordedClaims.claims.amount.snippet,
    'Active flipper requesting $35k for rehab on a purchased SFR.');
  assert.match(r.prose, /outside the recorded business range \$5,000–\$20,000/);
  assert.match(r.prose, /Outreach eligibility: blocked/); assert.doesNotMatch(r.prose, /recorded gates pass/);
  assert.equal(r.structured.metadata.executed, false); assert.equal(r.retrieval.delegated, false);
  assert.equal(r.structured.metadata.specialistDelegated, false);
  assert.doesNotMatch(JSON.stringify(r), /FOREIGN_SECRET|prospect:22:|\$99k/);
  assert.equal(JSON.stringify(rows), before);
  assert.ok(rows.every(row => !evaluate(row).eligible));
  assert.match(r.prose, /No writes, emails, calls, assignments, imports, review-state changes or specialist execution occurred/);
  assert.doesNotMatch(JSON.stringify(engine.sessions.get(r.sessionId)), /FOREIGN_SECRET/);
});

test('MAX_PROSPECT_NARRATIVE_CLAIMS=false rolls back narrative extraction only, without relaxing any gate', () => {
  const old = process.env.MAX_PROSPECT_NARRATIVE_CLAIMS;
  try {
    process.env.MAX_PROSPECT_NARRATIVE_CLAIMS = 'false';
    const r = answerFor();
    assert.equal(r.investigationPriority.ranked, false); assert.ok(r.items.every(i => !i.priority));
    assert.equal(r.investigationPriority.extractor, 'labelled-claims-v1');
    assert.match(r.prose, /Outreach eligibility: blocked/);
    assert.equal(recordedDetails('Financing request: $35k').amount, 35000); // labelled support is unchanged
    const labelled = answerFor(fixture()); assert.equal(labelled.items[0].priority.position, 1);
  } finally { if (old === undefined) delete process.env.MAX_PROSPECT_NARRATIVE_CLAIMS; else process.env.MAX_PROSPECT_NARRATIVE_CLAIMS = old; }
  assert.equal(answerFor().investigationPriority.extractor, EXTRACTOR);
});

test('projections, unrelated amounts and existing debt never become request or security claims', () => {
  for (const text of ['Projected profit of $45k on resale.', 'Expects net proceeds of $60k.', 'Sold a duplex for $240k last year.',
    'Comps support $310k ARV.', 'Expects to need $30k.', 'Estimated need of $25k.', 'Raising $500k for a fund.',
    'Portfolio of 8 rentals worth $1.2M.', 'Seeking deals around $150k.']) {
    const d = fit(text); assert.equal(d.values.amount, undefined, text);
  }
  const owes = fit('Needs $20k, and owes $80k on a first mortgage.');
  assert.equal(owes.amount, 20000); assert.equal(owes.values.security, undefined);
  for (const text of ['Has an existing first lien; requesting second lien.', 'Prior lender held first position; offering second position.']) {
    assert.equal(fit(text).claims.security.normalized, 'second lien', text);
  }
  assert.equal(fit('Requesting $35k; property value $220k; purchase price $160k.').amount, 35000);
  const otherSections = extractDealClaims('Fit evidence: Public profile only.\nRisks: Requesting $40k, first lien, exit via sale.\nNext action: Confirm $25k request and first lien.\nMarket: Requesting $50k first lien');
  assert.equal(otherSections.count, 0); assert.deepEqual(otherSections.unparsed, []);
});

test('a record with only non-comparable details is not described as having no recorded details', () => {
  const rows = narrativeFixture(); const eli = byName(answerFor(rows), rows, 'Eli').claim;
  assert.match(eli, /Recorded claims \(unverified\): No comparable deal claims recorded \(see the non-comparable details below\)\./);
  assert.match(eli, /Verification: no comparable deal claims; the recorded but non-comparable details above need reconciliation/);
});
