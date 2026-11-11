'use strict';
// fit-narrative-claims-v5: deal stage from an executed purchase contract with
// a contract price between the status word and the noun, found by the v4
// read-only production audit of PR #1120 (tenant 21). The phrase
//   "Current waterfront flip has an executed $260,000 purchase contract"
// is the redacted audit fragment, verbatim. Names, record IDs and the other
// sentences in the records below are synthetic.
//
// Also covers the audit script's possible-false-positive attribution for
// financing requests: a value cue (ARV, budget, …) that describes a DIFFERENT
// amount in the sentence is informational, while a cue that touches the
// claimed amount, or cannot be attributed to another amount, still warns.
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { extractDealClaims, EXTRACTOR } = require('../ProspectDealClaims');
const { recordedDetails } = require('../ProspectInvestigationPriority');
const { loadRecords } = require('../ProspectRecordIntelligence');
const { importReviewOnlyContacts, normalizeContact, noteFor } = require('../../../../services/reviewOnlyProspectImportService');
const audit = require('../../../../scripts/investigationNarrativeAudit');

const AUDIT_PHRASE = 'Current waterfront flip has an executed $260,000 purchase contract';
const fit = s => extractDealClaims(`Fit evidence: ${s}`);
const stageOf = s => fit(s).claims.stage?.normalized || null;

test('extractor version is bumped so v4 rankings are treated as stale', () => {
  assert.equal(EXTRACTOR, 'fit-narrative-claims-v5');
});

test('the audit phrase is an executed purchase contract for the current deal', () => {
  for (const text of [`${AUDIT_PHRASE}.`, AUDIT_PHRASE, `${AUDIT_PHRASE}; sale exit after rehab.`]) {
    const d = fit(text);
    assert.equal(d.claims.stage?.normalized, 'under contract', text);
    assert.equal(d.claims.stage.match, 'executed $260,000 purchase contract');
    assert.equal(d.claims.stage.epistemic, 'recorded_unverified');
    // The contract price is never a financing request.
    assert.equal(d.claims.amount, undefined, text);
    assert.equal(d.amount, null);
    assert.deepEqual(d.unparsed.filter(u => u.field === 'amount'), []);
  }
});

test('other price forms between the status word and the noun are recognized', () => {
  for (const text of ['Has an executed $260k purchase contract on the current flip.', 'Signed $1.2M purchase agreement on the subject property.',
    'Executed $260,000 purchase contract.', 'Executed $260 K purchase contract on the subject SFR.']) {
    assert.equal(stageOf(text), 'under contract', text);
  }
  // Unchanged forms.
  for (const text of ['Has an executed purchase contract.', 'Purchase contract is executed.', 'Executed purchase contract with no financing contingency.']) {
    assert.equal(stageOf(text), 'under contract', text);
  }
});

test('historical contracts abstain', () => {
  for (const text of ['Previously had an executed $260,000 purchase contract on a waterfront flip.',
    'Had an executed $260,000 purchase contract last year.', 'On a previous flip, had an executed $260,000 purchase contract.',
    'Prior executed $260,000 purchase contract was cancelled.', 'Executed $260,000 purchase contract in 2023.',
    'Executed $260,000 purchase contract fell through.', 'Executed $260,000 purchase contract was terminated.',
    'Current flip has an executed $260,000 purchase contract that fell through.', 'Executed $260,000 purchase contract expired.',
    'Has executed $260,000 purchase contracts on 14 homes.', 'Executed $260,000 purchase contracts over the years.',
    'Previously had an executed purchase contract.', 'Executed purchase contract fell through.']) {
    assert.equal(stageOf(text), null, text);
  }
});

test('unsigned, proposed, awaited or expected contracts abstain', () => {
  for (const text of ['Unsigned $260,000 purchase contract.', 'Proposed $260,000 purchase contract.',
    'Draft $260,000 purchase contract awaiting signature.', 'Awaiting an executed $260,000 purchase contract.',
    'Expects an executed $260,000 purchase contract next week.', 'Current flip expects an executed $260,000 purchase contract.',
    'Needs an executed $260,000 purchase contract before funding.', 'Will have an executed $260,000 purchase contract.',
    'May have an executed $260,000 purchase contract.', 'Executed $260,000 purchase contract to be signed Friday.',
    '$260,000 purchase contract on the current flip.', 'Current waterfront flip has a $260,000 purchase contract.']) {
    assert.equal(stageOf(text), null, text);
  }
});

test('negated execution abstains', () => {
  for (const text of ['No executed $260,000 purchase contract yet.', 'Has not executed the $260,000 purchase contract.',
    'Current waterfront flip has no executed $260,000 purchase contract.', 'Current waterfront flip does not have an executed $260,000 purchase contract.',
    'Without an executed $260,000 purchase contract.']) {
    assert.equal(stageOf(text), null, text);
  }
});

test('contracts unrelated to the current deal abstain', () => {
  for (const text of ['Executed $260,000 purchase contract on a different property.', 'Executed $260,000 purchase contract on another flip.',
    'Executed $260,000 purchase contract on the next deal.', 'Executed $260,000 purchase contract on a separate house.',
    'Executed purchase contract on another flip.']) {
    assert.equal(stageOf(text), null, text);
  }
  // A history clause elsewhere does not cancel an explicitly current clause.
  assert.equal(stageOf('Completed 14 prior flips, current waterfront flip has an executed $260,000 purchase contract.'), 'under contract');
  assert.equal(stageOf(`${AUDIT_PHRASE}; previous flip fell through.`), 'under contract');
});

// Minimal in-memory Postgres stand-in for the two real code paths: the
// review-only import (INSERT) and Max's prospect record read (SELECT).
function memoryPool() {
  const prospects = [];
  const companies = [];
  const query = async (sql, params = []) => {
    if (/^(BEGIN|COMMIT|ROLLBACK)/.test(sql) || /pg_advisory_xact_lock/.test(sql)) return { rows: [] };
    if (/FROM prospects p\s+LEFT JOIN companies/.test(sql)) return { rows: [] };
    if (/SELECT id, name FROM companies/.test(sql)) return { rows: companies.filter(c => c.client_id === params[0] && c.name.toLowerCase() === params[1].toLowerCase()) };
    if (/INSERT INTO companies/.test(sql)) { const c = { id: companies.length + 1, name: params[0], client_id: params[1] }; companies.push(c); return { rows: [c] }; }
    if (/INSERT INTO prospects/.test(sql)) {
      const id = `00000000-0000-4000-8000-${String(prospects.length + 1).padStart(12, '0')}`;
      const row = { id, client_id: params[0], company_id: params[1], first_name: params[2], last_name: params[3], email: params[4],
        phone: params[5], job_title: params[6], linkedin_url: params[7], source: 'review_only_import', icp_score: params[8],
        status: params[9], notes: params[10], vertical: 'private_lending', service_area_match: params[11], setter_visible: false,
        do_not_contact: false, ao_outreach_review_required: true, acquisition_metadata: JSON.parse(params[12]) };
      prospects.push(row);
      return { rows: [row] };
    }
    if (/FROM prospects p\s+WHERE p\.client_id = \$1/.test(sql)) {
      const rows = prospects.filter(p => p.client_id === params[0]).map(p => {
        const { acquisition_metadata, ...record } = p;
        return { id: p.id, client_id: p.client_id, record, acquisition_metadata, ao_outreach_review_required: true };
      });
      return { rows };
    }
    throw new Error(`unexpected SQL: ${sql.slice(0, 60)}`);
  };
  return { query, connect: async () => ({ query, release() {} }), prospects };
}

test('full record through the real import and read path: deal stage and sale exit are both recorded', async () => {
  const pool = memoryPool();
  const jon = { name: 'Jon Example', company: 'Example Holdings', reviewState: 'contact_now', market: 'Example County', category: 'borrower',
    fit: `${AUDIT_PHRASE}. Sale exit after a cosmetic rehab.`, risks: 'Contract, title and rehab scope unverified. No financing request recorded.',
    nextAction: 'Confirm the executed contract and closing timeline.', sourceUrl: 'https://example.invalid/jon' };
  const imported = await importReviewOnlyContacts([jon], { clientId: 21, db: pool, actorId: null });
  assert.equal(imported.inserted.length, 1);
  assert.equal(pool.prospects[0].notes, noteFor(normalizeContact(jon, 0)));

  const loaded = await loadRecords(pool, 21);
  assert.equal(loaded.status, 'found');
  const [row] = loaded.rows;
  const d = recordedDetails(row.notes);
  assert.equal(d.claims.stage.normalized, 'under contract');
  assert.equal(d.claims.stage.snippet, `${AUDIT_PHRASE}.`);
  assert.equal(d.claims.exit.normalized, 'sale');
  assert.equal(d.claims.amount, undefined); // the $260,000 contract price is not a request
  assert.deepEqual(d.absent.sort(), ['amount', 'closing', 'security']);
  assert.equal(d.count, 2);

  // The audit no longer reports a possible missed deal-stage claim.
  const r = audit.classifyRecord(row);
  assert.equal(r.categories.stage.state, 'recorded_unverified');
  assert.equal(r.categories.exit.state, 'recorded_unverified');
  assert.equal(r.findings.find(f => f.field === 'stage'), undefined);
  assert.equal(r.extractor, 'fit-narrative-claims-v5');
});

test('full record: near-match notes keep the stage absent and the audit still flags it for review', async () => {
  const pool = memoryPool();
  const inputs = [
    'Previously had an executed $260,000 purchase contract on a waterfront flip. Sale exit after rehab.',
    'Unsigned $260,000 purchase contract on the current waterfront flip. Sale exit after rehab.',
    'Current waterfront flip has no executed $260,000 purchase contract. Sale exit after rehab.',
    'Executed $260,000 purchase contract on a different property. Sale exit after rehab.',
  ].map((f, i) => ({ name: `Near Example${i}`, company: `Near Co ${i}`, reviewState: 'contact_now', market: 'Example County',
    category: 'borrower', fit: f, risks: 'Unverified.', sourceUrl: `https://example.invalid/near/${i}` }));
  await importReviewOnlyContacts(inputs, { clientId: 21, db: pool, actorId: null });
  const loaded = await loadRecords(pool, 21);
  assert.equal(loaded.rows.length, 4);
  for (const row of loaded.rows) {
    const d = recordedDetails(row.notes);
    assert.equal(d.claims.stage, undefined, row.notes);
    assert.equal(d.claims.exit.normalized, 'sale');
    const r = audit.classifyRecord(row);
    assert.equal(r.categories.stage.state, 'absent');
    assert.ok(r.findings.some(f => f.type === 'possible_missed_claim' && f.field === 'stage'), 'operator still reviews the contract wording');
  }
});

// ------------------------------------------------ audit warning attribution
const rowFor = fitText => {
  const c = normalizeContact({ name: 'Zackary Example', fit: fitText, risks: 'Unverified.', reviewState: 'contact_now',
    market: 'Example County', category: 'borrower', sourceUrl: 'https://example.invalid/z' }, 0);
  return { id: '22222222-bbbb-4bbb-8bbb-bbbbbbbbbbbb', client_id: 21, first_name: c.firstName, last_name: c.lastName,
    source: 'review_only_import', notes: noteFor(c) };
};
const fp = r => r.findings.filter(f => f.type === 'possible_false_positive' && f.field === 'amount');

test('a correctly extracted $35,000 request with an ARV on another amount is informational, not a false-positive finding', () => {
  for (const text of ['Requesting $35,000 for rehab on a flip with a $250k ARV.', 'Requesting $35,000 for rehab, ARV of $250k.',
    'ARV $250k and requesting $35,000 for rehab.', 'Requesting $35,000 for rehab on a $180k purchase price with a $250k ARV.']) {
    const r = audit.classifyRecord(rowFor(text));
    assert.equal(r.categories.amount.state, 'recorded_unverified', text);
    assert.deepEqual(fp(r), [], text);
    assert.ok(r.notes.some(n => n.type === 'value_cue_on_other_amount' && n.field === 'amount'), text);
  }
  const r = audit.classifyRecord(rowFor('Requesting $35,000 for rehab on a flip with a $250k ARV.'));
  const text = audit.formatReport({ tenantId: 21, cohort: { expected: 6, found: 1, retrieval: 'found', complete: true }, records: [r],
    blueprint: null, ranking: null, conclusion: 'PASS', reasons: [] });
  assert.match(text, /NOTE financing request: value cue "ARV" describes a different amount/);
  assert.doesNotMatch(text, /REVIEW possible_false_positive/);
});

test('false-positive detection is not weakened: cues on the claimed amount or unattributable cues still warn', () => {
  for (const text of [
    'Requesting $35k, projected.', // cue with no amount of its own
    'Requesting $35,000 for the budget.', // cue describes the claimed amount
    'Requesting $35,000 for rehab with an after-repair value of roughly $250k.', // cue too far from the other amount
  ]) {
    const r = audit.classifyRecord(rowFor(text));
    assert.equal(r.categories.amount.state, 'recorded_unverified', text);
    assert.equal(fp(r).length, 1, text);
    assert.deepEqual(r.notes, [], text);
  }
  // Direct checks of the attribution helper.
  const claim = (snippet, match) => ({ snippet, match });
  assert.equal(audit.valueCuesOnOtherAmounts(claim('Requesting $35,000 for rehab, ARV of $250k.', 'Requesting $35,000')), true);
  assert.equal(audit.valueCuesOnOtherAmounts(claim('Requesting $35,000 ARV, $250k purchase.', 'Requesting $35,000')), false); // cue touches the claim
  assert.equal(audit.valueCuesOnOtherAmounts(claim('Requesting $35,000; budget is tight.', 'Requesting $35,000')), false); // no other amount
  assert.equal(audit.valueCuesOnOtherAmounts(claim('Requesting $35,000, ARV $250k, budget pending.', 'Requesting $35,000')), false); // one cue unattributed
  assert.equal(audit.valueCuesOnOtherAmounts(claim('ARV $250k.', 'Requesting $35,000')), false); // claim not in snippet: fail-safe
});
