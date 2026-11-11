'use strict';
// Explainable missed-claim warnings in scripts/investigationNarrativeAudit.js:
// short redacted context, confirmed extractor reasons kept separate from
// diagnostic hypotheses, and fail-closed redaction.
require('./workspaceTestHarness');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const audit = require('../../../../scripts/investigationNarrativeAudit');
const { normalizeContact, noteFor } = require('../../../../services/reviewOnlyProspectImportService');

const rowFor = (fit, { name = 'Zane Example', risks = 'Unverified.', extraNotes = '' } = {}) => {
  const c = normalizeContact({ name, fit, risks, reviewState: 'contact_now', market: 'Example County', category: 'borrower',
    sourceUrl: 'https://example.invalid/x' }, 0);
  return { id: '11111111-aaaa-4aaa-8aaa-aaaaaaaaaaaa', client_id: 21, first_name: c.firstName, last_name: c.lastName,
    source: 'review_only_import', notes: noteFor(c) + extraNotes, email: 'zane@private.example', phone: '(603) 555-0100' };
};
const finding = (record, field, type = 'possible_missed_claim') => record.findings.find(f => f.type === type && f.field === field);

test('a missed-claim warning carries a short context window, confirmed extractor facts and separately labelled hypotheses', () => {
  const r = audit.classifyRecord(rowFor('Rehab budget of $60k on a $180k purchase; ARV $260k. Exit via sale after a 5-month rehab.'));
  const f = finding(r, 'amount');
  assert.ok(f && f.occurrences.length >= 1 && f.occurrences.length <= 3);
  const o = f.occurrences[0];
  assert.match(o.context, /budget of \$60k on a \$180k purchase/);
  assert.ok(o.context.split(' ').filter(w => w !== '…').length <= 13);
  assert.deepEqual(o.confirmed.map(c => c.id), ['no_pattern_matches_sentence']);
  assert.ok(o.hypotheses.some(h => h.id === 'non_request_amount_context'));
  assert.ok(o.hypotheses.some(h => h.id === 'no_request_wording_before_amount'));
  const text = audit.formatReport({ tenantId: 21, cohort: { expected: 6, found: 1, retrieval: 'found', complete: true }, records: [r],
    blueprint: null, ranking: null, conclusion: 'FAIL', reasons: [] });
  assert.match(text, /cue "\$6" — context: ".*budget of \$60k/);
  assert.match(text, /confirmed: the extractor finds no claim for this category in this sentence on its own/);
  assert.match(text, /hypothesis \(unconfirmed\): sentence describes a budget, price, value, profit, rent or existing debt/);
});

test('confirmed reasons come from the real extractor: labelled precedence and in-isolation matches are reported as facts, not hypotheses', () => {
  const r = audit.classifyRecord(rowFor('Exit plan: unknown\nExit via sale after rehab.'));
  const o = finding(r, 'exit').occurrences.find(x => x.context && /sale/.test(x.context));
  assert.deepEqual(o.confirmed.map(c => c.id), ['labelled_field_governs', 'sentence_matches_in_isolation']);
  assert.deepEqual(o.hypotheses, []); // no heuristics when the extractor itself explains the abstention
});

test('closing and exit hypotheses distinguish missing dates, history and exit timelines without asserting a claim', () => {
  const r = audit.classifyRecord(rowFor('Closing scheduled. Closing on 4 deals this year. Exit via DSCR refinance, which should close in 6 months.'));
  const ids = finding(r, 'closing').occurrences.flatMap(o => o.hypotheses.map(h => h.id));
  assert.ok(ids.includes('no_date_or_timeframe'));
  assert.ok(ids.includes('past_tense_or_history'));
  assert.ok(ids.includes('exit_or_refinance_context'));
  const generic = audit.classifyRecord(rowFor('Long-term BRRRR investor with rentals.'));
  assert.ok(finding(generic, 'exit').occurrences[0].hypotheses.some(h => h.id === 'generic_strategy_term'));
});

test('redaction masks the record\'s names, contacts, URLs, addresses, long numbers and unknown proper nouns', () => {
  const cases = [
    ['Zane needs $35k for the rehab on a duplex referred by Mitchell last month', /\[name\] needs \$35k for the rehab on a duplex referred by \[word\] last month/],
    ['the property at 123 Elm Street needs a $35k rehab loan this month', /the property at \[address\] needs a \$35k rehab loan this month/],
    ['requesting $35k for the rehab, call (603) 555-0199 or the office', /requesting \$35k for the rehab, call \[phone\] or the office/],
    ['requesting $35k for the rehab, email zane@private.example for the docs', /requesting \$35k for the rehab, email \[email\] for the docs/],
    ['requesting $35k for the rehab, see https://x.example/p for the deal', /requesting \$35k for the rehab, see \[url\] for the deal/],
    ['requesting $35k for the rehab on loan 9876543 with the bank', /requesting \$35k for the rehab on loan \[number\] with the bank/],
  ];
  for (const [text, expected] of cases) {
    const ctx = audit.safeContext(text, { first_name: 'Zane', last_name: 'Example' });
    assert.ok(ctx.context, `${text} -> ${JSON.stringify(ctx)}`);
    assert.match(ctx.context, expected);
    assert.doesNotMatch(ctx.context, /Zane|Mitchell|Elm|555|private\.example|https?:|9876543/);
  }
});

test('redaction fails closed: residual personal markers or mostly-redacted windows are withheld entirely', () => {
  assert.deepEqual(audit.safeContext('needs $35k, ask @zane_flips on social', { first_name: 'Zane' }), { context: null, withheld: 'redaction_unverified' });
  assert.deepEqual(audit.safeContext('Brook Hollow Capital Partners Group LLC', {}), { context: null, withheld: 'mostly_redacted' });
  const r = audit.classifyRecord(rowFor('Contact @zane_flips; requesting gap funding of $35k for Brook Hollow Capital.'));
  for (const f of r.findings) for (const o of f.occurrences) {
    assert.ok(o.context === null || !/@|zane|Brook|Hollow/i.test(o.context), JSON.stringify(o));
  }
});

test('report output never contains the record\'s name, contact details, URLs or full notes, in text or JSON', () => {
  const row = rowFor('Zane Example needs $35K in rehab funds before closing; ARV $250k. Long-term BRRRR investor.');
  const r = audit.classifyRecord(row);
  const report = { tenantId: 21, cohort: { expected: 6, found: 1, retrieval: 'found', complete: true }, records: [r],
    blueprint: null, ranking: null, conclusion: 'FAIL', reasons: [] };
  for (const output of [audit.formatReport(report), JSON.stringify(report)]) {
    assert.doesNotMatch(output, /Zane|Example|private\.example|555-0100|example\.invalid|https?:/);
    assert.doesNotMatch(output, /needs \$35K in rehab funds before closing; ARV \$250k\. Long-term BRRRR investor\./); // never the full sentence
  }
});

test('possible false positives also carry redacted context', () => {
  const r = audit.classifyRecord(rowFor('Requesting $35k; proposed first lien behind nothing else. Targeting close in ~3 weeks, after rehab budget review.'));
  // A claim sentence containing a guard word is flagged for review with context.
  const fp = r.findings.filter(f => f.type === 'possible_false_positive');
  for (const f of fp) assert.ok(f.occurrences[0].context || f.occurrences[0].contextWithheld);
});
