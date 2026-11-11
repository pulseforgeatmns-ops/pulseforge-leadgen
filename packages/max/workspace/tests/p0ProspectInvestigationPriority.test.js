'use strict';
// P0 Max reliability — PR 2 production ranking failure (LA Endeavors shape).
//
// Production: Max retrieved six named contact_now prospects with record-level
// evidence, then refused to rank them because zero records satisfied the
// conservative outreach eligibility criteria (every review-only import carries
// ao_outreach_review_required=true). Analytical prioritization was coupled to
// outreach execution eligibility. These tests reproduce that exact shape via
// the real review-only import normalizer and pin the corrected behaviour:
// a read-only, provisional investigation priority, with outreach still blocked.
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createHermeticWorkspaceEngine, unavailableOperatingEvidence } = require('./workspaceTestHarness');
const { normalizeContact, noteFor } = require('../../../../services/reviewOnlyProspectImportService');
const { composeRecords, evaluate } = require('../ProspectRecordIntelligence');
const { classifyOperationalRead } = require('../OperationalReadIntent');
const { tenantFinancingCriteria, extractSignals, dominates } = require('../ProspectInvestigationPriority');

const CLIENT = 21;
const uuid = n => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;

// Synthetic people; fit/risk text shaped like review-only research notes.
const CONTACTS = [
  { name: 'Avery Stone', market: 'Kansas City, MO', sourceUrl: 'https://example.invalid/a',
    fit: 'Active fix-and-flip operator with 14 completed flips. Current project: 1824 Holmes Street single-family rehab under contract; seeking $180k rehab and equity-gap capital. Exit plan: retail sale after renovation. First-lien position on the property offered.',
    risks: 'Rehab budget may run over.' },
  { name: 'Blake Rivera', market: 'Memphis, TN', sourceUrl: 'https://example.invalid/b',
    fit: 'Experienced operator with 6 years of experience. Fix-and-flip duplex rehab needs $240k bridge capital. Exit via DSCR refinance.',
    risks: 'Collateral ownership unclear.' },
  { name: 'Casey Morgan', market: 'Phoenix, AZ', sourceUrl: 'https://example.invalid/c',
    fit: 'Buy-and-hold investor posting publicly about a rehab.',
    risks: 'No specific deal identified. Exit unclear. Track record unverified.' },
  { name: 'Devon Ellis', market: 'Columbus, OH', sourceUrl: 'https://example.invalid/d',
    fit: 'Active rehab operator seeking capital.', risks: 'Amount and timeline unknown.' },
  { name: 'Emery Hart', market: 'Tulsa, OK', sourceUrl: 'https://example.invalid/e',
    fit: 'First-time investor seeking $95k for a fix-and-flip at 77 Oak Street.', risks: 'No exit plan recorded.' },
  { name: 'Finley Park', market: 'Dayton, OH', sourceUrl: 'https://example.invalid/f',
    fit: 'Active rehab operator seeking capital.', risks: 'Amount and timeline unknown.' },
];

// Mirrors importReviewOnlyContacts' INSERT column values exactly.
function importedRow(input, n, clientId = CLIENT) {
  const c = normalizeContact({ ...input, status: 'Contact now', email: `p${n}@example.invalid` }, n);
  return {
    id: uuid(n), client_id: clientId, first_name: c.firstName, last_name: c.lastName || null,
    email: c.email, phone: c.phone, linkedin_url: c.linkedinUrl, source: 'review_only_import',
    icp_score: c.icpScore, status: c.status, notes: noteFor(c), vertical: 'private_lending',
    service_area_match: c.market || null, setter_visible: false, do_not_contact: false,
    ao_outreach_review_required: true, discovery_method: 'operator_import',
    acquisition_metadata: { importMode: 'review_only', outreachAuthorized: false, reviewState: c.reviewState,
      category: c.category, sourceUrls: c.sourceUrls },
  };
}
const six = () => CONTACTS.map((c, i) => importedRow(c, i + 1));
const bundle = rows => ({ clientId: CLIENT, tenantId: String(CLIENT),
  prospects: { records: { status: 'found', complete: true, rows } } });
const QUESTION = 'Which of my contact_now prospects are the highest priority? Rank them using actual workspace data.';
const byName = (r, first) => r.items.find(i => i.claim.startsWith(first));

test('reproduction: imported records are outreach-ineligible (the gate that caused the refusal is intact)', () => {
  for (const row of six()) {
    const e = evaluate(row);
    assert.equal(e.review, 'contact_now');
    assert.equal(e.eligible, false);
    assert.ok(e.blockers.includes('outreach review hold not cleared'));
  }
});

test('six contact_now records with zero outreach eligibility now receive a provisional investigation priority', () => {
  const read = classifyOperationalRead(QUESTION);
  assert.equal(read.isRead, true); assert.equal(read.wantsRanking, true); assert.equal(read.reviewState, 'contact_now');
  const r = composeRecords(bundle(six()), read);
  // The production refusal text is gone.
  assert.doesNotMatch(r.prose, /Unranked candidates: evidence is insufficient for a defensible outreach ordering/);
  assert.match(r.prose, /Provisional investigation priority/);
  assert.match(r.prose, /Outreach authorization: 0 of 6 candidates meet the recorded outreach criteria/);
  assert.match(r.prose, /Outreach remains blocked for every candidate/);
  assert.equal(r.items.length, 6);
  const pos = first => byName(r, first).investigationPriority?.position;
  // Avery: specific deal, first-lien, exit, track record, under contract and no flagged gap.
  assert.equal(pos('Avery'), 1);
  // Casey: contrary evidence on specificity, exit and track record; dominated by Devon/Finley and Emery.
  assert.equal(pos('Casey'), 3);
  for (const first of ['Blake', 'Devon', 'Emery', 'Finley']) assert.equal(pos(first), 2);
  assert.equal(r.investigationPriority.positions, 3);
  for (const item of r.items) {
    assert.equal(item.investigationPriority.epistemic, 'calculated');
    assert.equal(item.investigationPriority.provisional, true);
  }
});

test('investigation priority, financing suitability and outreach authorization are reported separately per candidate', () => {
  const r = composeRecords(bundle(six()), classifyOperationalRead(QUESTION));
  for (const item of r.items) {
    assert.match(item.claim, /investigation priority: /);
    assert.match(item.claim, /financing suitability: /);
    assert.match(item.claim, /outreach authorization: blocked — .*outreach review hold not cleared/);
    assert.match(item.claim, /relative position: /);
    assert.match(item.claim, /uncertainties: /);
    assert.match(item.claim, /next investigative step: .*Investigation only/);
    assert.doesNotMatch(item.claim.split('next investigative step:')[1], /\b(?:email|call|text|reach out|send)\b(?! the prospect to obtain)/i);
    assert.equal(item.outreachAuthorization, 'blocked');
    assert.equal(item.priority, null, 'no outreach order exists when no record is eligible');
  }
  const avery = byName(r, 'Avery');
  assert.match(avery.claim, /above .*Blake Rivera.*not weaker on any dimension/);
  assert.match(avery.claim, /recorded: "Exit plan: retail sale after renovation\."/);
  const devon = byName(r, 'Devon');
  assert.match(devon.claim, /shares position with Finley Park \(indistinguishable on recorded evidence\)/);
  assert.match(devon.claim, /shares position with .*Emery Hart.* \(stronger on different dimensions/);
  assert.match(byName(r, 'Blake').claim, /security position \/ collateral clarity: contrary evidence — recorded: "Risks: Collateral ownership unclear\."/);
  assert.match(byName(r, 'Blake').claim, /next investigative step: Resolve the recorded verification gaps/);
  assert.match(byName(r, 'Emery').claim, /operator track record: contrary evidence/);
  assert.match(byName(r, 'Emery').claim, /next investigative step: Establish the likely exit/);
  // Score 80 is shown as qualification only and is not a ranking input.
  assert.match(avery.claim, /qualification score: 80 \(not used for investigation priority\)/);
});

test('amounts are judged only against tenant-recorded criteria; none recorded means not assessed', () => {
  const none = composeRecords(bundle(six()), classifyOperationalRead(QUESTION));
  assert.match(none.prose, /no financing-amount range is recorded/);
  assert.match(byName(none, 'Avery').claim, /recorded amount \$180k; not assessable — no tenant amount criteria recorded/);
  const understanding = { contract: { services: 'Short-term bridge loans from $100k to $1.5M for active operators.' } };
  const withCriteria = composeRecords(bundle(six()), classifyOperationalRead(QUESTION), { businessUnderstanding: understanding });
  assert.match(withCriteria.prose, /\$100k to \$1\.5M/);
  assert.match(byName(withCriteria, 'Avery').claim, /\$180k is within the tenant's recorded criteria/);
  assert.match(byName(withCriteria, 'Emery').claim, /\$95k is outside the tenant's recorded criteria/);
  assert.match(byName(withCriteria, 'Emery').claim, /financing suitability: concerns recorded/);
  assert.deepEqual(tenantFinancingCriteria({ contract: { services: 'Loans up to $750k.' } }), { min: null, max: 750000, evidence: 'Loans up to $750k.' });
  assert.equal(tenantFinancingCriteria({ contract: { services: 'Flexible private capital.' } }), null);
  assert.equal(tenantFinancingCriteria(null), null);
});

test('indistinguishable candidates are never forced into an order', () => {
  const rows = six().map((r, i) => ({ ...importedRow(CONTACTS[3], i + 1), first_name: `Same${i + 1}` }));
  const r = composeRecords(bundle(rows), classifyOperationalRead(QUESTION));
  assert.match(r.prose, /No investigation order is given/);
  assert.match(r.prose, /indistinguishable on every assessed dimension/);
  assert.equal(r.items.length, 6);
  assert.ok(r.items.every(i => i.investigationPriority === null && i.priority === null));
});

test('candidates stronger on different dimensions share a position (no invented weights)', () => {
  const a = importedRow({ name: 'Ann One', fit: 'Exit via refinance.', sourceUrl: 'https://example.invalid/1' }, 1);
  const b = importedRow({ name: 'Ben Two', fit: '9 completed flips.', sourceUrl: 'https://example.invalid/2' }, 2);
  const r = composeRecords(bundle([a, b]), { wantsRanking: true, topCount: 5 });
  assert.match(r.prose, /each is stronger on different dimensions/);
  assert.ok(r.items.every(i => i.investigationPriority === null));
  assert.equal(dominates([1, 0], [0, 1]), false); assert.equal(dominates([1, 0], [0, 0]), true);
  assert.equal(dominates([0, 0], [0, 0]), false);
});

test('bounded extraction: negation, risk sections and workflow boilerplate never become positive evidence', () => {
  const row = importedRow({ name: 'Neg Case', sourceUrl: 'https://example.invalid/n',
    fit: 'Wants capital.', risks: 'Exit plan may rely on refinance at current rates. No track record.', nextAction: 'Verify first-lien position.' }, 9);
  const d = extractSignals(row, null);
  assert.equal(d.exitClarity.value, 0); assert.equal(d.exitClarity.state, 'flagged_as_risk');
  assert.equal(d.trackRecord.value, -1);
  assert.equal(d.securityPosition.value, 0, 'Next action text is a plan, not evidence');
  assert.equal(d.financingFit.state, 'amount_not_recorded');
  // Purchase prices without financing context are not capital needs.
  assert.equal(extractSignals({ ...row, notes: 'Bought a house for $300k in 2019.' }, null).financingFit.state, 'amount_not_recorded');
});

test('outreach gates are unchanged: eligible records keep a separate outreach order; blocked records never gain one', () => {
  const rows = six();
  rows[4] = { ...rows[4], ao_outreach_review_required: false, status: 'warm' };
  const r = composeRecords(bundle(rows), classifyOperationalRead(QUESTION));
  assert.match(r.prose, /Outreach authorization: 1 of 6 candidates/);
  assert.match(r.prose, /Even eligible records require separate operator authorization/);
  const emery = byName(r, 'Emery');
  assert.equal(emery.priority.scope, 'outreach_order_eligible_only');
  assert.equal(emery.outreachAuthorization, 'requires_operator_authorization');
  // Outreach eligibility does not raise investigation priority.
  assert.equal(emery.investigationPriority.position, 2);
  assert.ok(r.items.filter(i => i !== emery).every(i => i.priority === null && i.outreachAuthorization === 'blocked'));
});

test('partial, continuation or conflicting reads never produce an investigation order', () => {
  const partial = bundle(six()); partial.prospects.records.complete = false; partial.prospects.records.limit = 1000;
  const p = composeRecords(partial, classifyOperationalRead(QUESTION));
  assert.match(p.prose, /Investigation order withheld/); assert.ok(p.items.every(i => i.investigationPriority === null));
  const dup = composeRecords(bundle([...six(), { ...six()[0], status: 'warm' }]), classifyOperationalRead(QUESTION));
  assert.ok(dup.items.every(i => i.investigationPriority === null));
});

test('rejected, held and terminal records are excluded from investigation priority', () => {
  const rows = six();
  rows[0] = { ...rows[0], acquisition_metadata: { ...rows[0].acquisition_metadata, reviewState: 'hold' } };
  rows[1] = { ...rows[1], status: 'disqualified' };
  const r = composeRecords(bundle(rows), { wantsRanking: true, topCount: 5 });
  assert.match(r.prose, /2 record\(s\) excluded/);
  assert.ok(!r.items.some(i => /Avery|Blake/.test(i.claim)));
});

test('rollback flag restores the previous outreach-coupled refusal', () => {
  process.env.MAX_PROSPECT_INVESTIGATION_PRIORITY = 'false';
  try {
    const r = composeRecords(bundle(six()), classifyOperationalRead(QUESTION));
    assert.match(r.prose, /Unranked candidates/);
    assert.ok(r.items.every(i => !i.investigationPriority));
  } finally { delete process.env.MAX_PROSPECT_INVESTIGATION_PRIORITY; }
});

test('tenant-agnostic: no tenant names, owners or financing preferences are hardcoded', () => {
  const src = fs.readFileSync(path.join(__dirname, '../ProspectInvestigationPriority.js'), 'utf8');
  assert.doesNotMatch(src, /Levi|LA Endeavors|private_lending|clientId\s*===|client_id\s*===|\b21\b/);
  // Same records under another tenant rank identically.
  const other = six().map(r => ({ ...r, client_id: 44 }));
  const a = composeRecords(bundle(six()), classifyOperationalRead(QUESTION));
  const b = composeRecords({ ...bundle(other), clientId: 44 }, classifyOperationalRead(QUESTION));
  assert.deepEqual(a.items.map(i => i.investigationPriority?.position), b.items.map(i => i.investigationPriority?.position));
  assert.ok(b.items.every(i => i.recordRef.startsWith('prospect:44:')));
});

// Full chat path: approved Blueprint present, hermetic engine, no execution.
async function openEngine(rows, { services = 'Asset-backed bridge loans.' } = {}) {
  const { createMemoryStore, startClientInterview, postInterviewMessage, approveBlueprint } = require('../../../../services/clientIntelligenceInterview');
  const store = createMemoryStore();
  let interview = await startClientInterview({ clientId: CLIENT, forceNew: true }, { store });
  for (const answer of ['Private lending.', services, 'Real estate operators.', 'Unqualified applicants.',
    'Nationwide.', 'Clear decisions.', 'Professional.', 'Qualified pipeline.', 'Funded loans.']) {
    interview = await postInterviewMessage(interview.interviewId, answer, { store });
  }
  await approveBlueprint(interview.blueprint.id, { store });
  const calls = [];
  const engine = createHermeticWorkspaceEngine({ clientIntelligenceOpts: { store },
    operatingEvidenceOpts: { ...unavailableOperatingEvidence(), loadProspects: async input => {
      calls.push(input.clientId);
      return { available: true, counts: { total: rows.length }, records: { status: 'found', complete: true, rows } };
    } } });
  const opened = engine.open({ tenantId: String(CLIENT), page: 'command-deck' });
  return { engine, calls, ask: question => engine.ask({ sessionId: opened.sessionId, question }) };
}

test('production acceptance: the six-prospect request ranks for investigation through the full chat path with outreach blocked', async () => {
  const foreign = { ...importedRow(CONTACTS[0], 99, 22), notes: 'FOREIGN_SECRET 1 Main Street under contract $200k loan, 40 completed flips, exit via refinance, first-lien.' };
  const { ask, engine, calls } = await openEngine([...six(), foreign]);
  const r = await ask(QUESTION);
  assert.ok(calls.every(c => c === CLIENT));
  assert.match(r.prose, /Provisional investigation priority/);
  assert.match(r.prose, /Outreach remains blocked for every candidate/);
  assert.doesNotMatch(r.prose, /Unranked candidates: evidence is insufficient/);
  const ev = r.structured.supportingEvidence.filter(e => String(e.id).startsWith(`prospect:${CLIENT}:`));
  assert.equal(ev.length, 6);
  assert.equal(ev.find(e => e.detail.startsWith('Avery')).investigationPriority.position, 1);
  assert.ok(ev.every(e => e.outreachAuthorization === 'blocked' && e.priority === null));
  assert.doesNotMatch(JSON.stringify(r), /FOREIGN_SECRET|prospect:22:/);
  assert.equal(r.structured.metadata.executed, false);
  assert.equal(r.structured.metadata.specialistDelegated, false);
  assert.equal(r.retrieval.delegated, false);
  const ctx = engine.sessions.get(r.sessionId).context.lastOperationalRead;
  assert.equal(ctx.reviewState, 'contact_now');
  assert.equal(ctx.investigationRankedRecordIds[0], uuid(1));
  assert.equal(ctx.rankedRecordIds.length, 0, 'no outreach order was produced');
  // Follow-up re-reads and keeps the contact_now scope.
  const next = await ask('Why is the top one ranked first?');
  assert.match(next.prose, /fresh record read/);
  assert.match(next.prose, /with recorded review state contact_now/);
  assert.equal(next.structured.metadata.executed, false);
});
