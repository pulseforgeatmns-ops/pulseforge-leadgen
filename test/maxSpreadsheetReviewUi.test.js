'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { create, intent, proposalHtml } = require('../public/shared/spreadsheetReview');
const { readFileSync } = require('node:fs');
const vm = require('node:vm');

const proposal = () => ({ id: 'proposal-1', digest: 'digest-1', sourceHash: 'sha256', actorId: 1, tenantId: 10, aoId: 7, conversationId: 'conversation-1', can_approve: true, plan: {
  rows: [{ sourceRow: 7, account: 'University conflict', status: 'HELD', conflicts: ['Organization identity unresolved'] }],
  operations: [
    { id: 'op-1', type: 'ADD_NOTE', target: { accountId: 5 }, before: null, after: 'Mark is now the decision maker', evidence: { cell: 'L8' }, dependsOn: [] },
    { id: 'op-held', type: 'MATCH_ACCOUNT', target: null, before: null, after: null, evidence: { cell: 'A7' }, blocked: true },
  ],
} });
const scope = () => ({ tenant_id: 10, actor_id: 1, ao_id: 7, can_approve: true, aos: [{ id: 7, name: 'Tony' }] });
function harness(overrides = {}) {
  const calls = [], messages = [];
  let currentScope = scope();
  const review = create({ host: null, onMessage: value => messages.push(value), makeId: () => 'stable-key', fetch: async (url, opts) => {
    if (url.endsWith('/scope')) return { ok: true, json: async () => currentScope };
    calls.push({ url, opts });
    return overrides.reply ? overrides.reply(calls.length) : { ok: true, json: async () => ({ ok: true, committed: true, operational_response: 'One change verified; identity conflict held.' }) };
  } });
  review.accept({ spreadsheet_proposal: proposal() });
  return { review, calls, messages, setScope: value => { currentScope = value; } };
}

test('negative, conditional and ambiguous language never approves', async () => {
  const { review, calls } = harness();
  for (const text of ['Do not save those updates yet', "Don't save", 'save if this looks right', 'yes', 'Save everything except row 7', 'approve after I check']) {
    assert.notEqual(intent(text), 'approve');
    assert.equal(await review.handleText(text), true);
  }
  assert.equal(calls.length, 0);
});

test('commit includes exact eligible identifiers, never mutable plan, source rows or CRM values', async () => {
  const { review, calls } = harness();
  await review.handleText('Save');
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, '/api/v1/max/spreadsheet/proposals/proposal-1/commit');
  assert.equal(calls[0].opts.credentials, 'same-origin');
  assert.deepEqual(JSON.parse(calls[0].opts.body), { conversation_id: 'conversation-1', ao_id: 7, source_hash: 'sha256', proposal_digest: 'digest-1', operation_ids: ['op-1'], idempotency_key: 'stable-key', text: 'approve selected operations' });
  assert.equal(review.hasPending(), true, 'held evidence remains visible after save');
  await assert.rejects(review.commit('Save'), /Jake approval/);
});

test('lost response retries the identical key and selected operations', async () => {
  const { review, calls } = harness({ reply: async n => {
    if (n === 1) throw new Error('connection closed after commit');
    return { ok: true, json: async () => ({ ok: true, committed: true }) };
  } });
  await assert.rejects(review.commit('Save'), /connection closed/);
  await review.commit('Save selected changes');
  assert.equal(calls[0].opts.body, calls[1].opts.body);
});

test('stale approval is invalidated and cannot be retried as a new save', async () => {
  const { review, calls } = harness({ reply: async () => ({ ok: false, status: 409, json: async () => ({ error: 'baseline_changed' }) }) });
  await assert.rejects(review.commit('Save'), /baseline_changed/);
  await assert.rejects(review.commit('Save'), /Jake approval/);
  assert.equal(calls.length, 1);
});

test('changed actor or tenant clears proposal before commit', async () => {
  for (const next of [{ ...scope(), actor_id: 2 }, { ...scope(), tenant_id: 11 }]) {
    const { review, calls, setScope } = harness();
    await review.loadScope(); setScope(next);
    await assert.rejects(review.commit('Save'), /No current proposal/);
    assert.equal(calls.length, 0); assert.equal(review.hasPending(), false);
  }
});

test('tenant-switch generation rejects late preview responses', () => {
  const { review } = harness(); const generation = review.token(); review.clear();
  assert.equal(review.accept({ spreadsheet_proposal: proposal() }, generation), false);
  assert.equal(review.hasPending(), false);
});

test('non-approvers have no save control; full evidence is escaped and held rows visible', async () => {
  const item = proposal(); item.can_approve = false; item.plan.operations[0].after = '<img src=x onerror=alert(1)>';
  const html = proposalHtml(item, new Set(['op-1']));
  assert.doesNotMatch(html, /data-spreadsheet-save|<img/);
  for (const text of ['Before', 'Proposed', 'L8', 'Organization identity unresolved', '&lt;img', 'Jake approval required']) assert.ok(html.includes(text));
  const { review, calls } = harness(); review.accept({ spreadsheet_proposal: item });
  await assert.rejects(review.commit('Save'), /Jake approval/); assert.equal(calls.length, 0);
});

function extracted(file, start, end) { const source = readFileSync(file, 'utf8'); return source.slice(source.indexOf(start), source.indexOf(end, source.indexOf(start))); }
for (const surface of ['ao', 'deck']) test(`${surface}: actual text submission routes pending Save and no-save through review, never generic chat`, async () => {
  const { review, calls } = harness();
  const context = { spreadsheetReview: review, pendingComposerAttachments: [], mxPendingComposerFiles: [], pendingVoiceReview: null, wizardSessionId: null, workspaceAskInFlight: false, messageInput: { value: '' }, addMsg() {}, appendOperatorMessage() {}, appendSystemMessage() {}, resetAskInput() {}, els: { mxAskSend: { disabled: false } }, console: { info() {} }, askMaxFreeform: () => { throw new Error('generic chat must not run'); } };
  vm.createContext(context);
  const fn = surface === 'ao' ? extracted('public/ao-dashboard.html', 'async function sendMessage(text)', 'async function startNewConversation()') : extracted('public/command-deck/command-deck.js', '  async function askWorkspace(question)', '  function inviteAskMax(');
  vm.runInContext(fn, context);
  const submit = surface === 'ao' ? context.sendMessage : context.askWorkspace;
  await submit('Do not save those updates yet'); assert.equal(calls.length, 0);
  await submit('Save'); assert.equal(calls.length, 1);
});

test('reviewed resolutions require evidence and only create a fresh server proposal', async () => {
  const { review, calls } = harness({ reply: async () => ({ ok: true, json: async () => ({ spreadsheet_proposal: { ...proposal(), id: 'fresh-proposal', digest: 'fresh-digest' }, preview_only: true }) }) });
  await assert.rejects(review.resolve([{ sheet: 'Sheet1', rowNumber: 7, identityEvidence: ' ' }]), /Explain the verified/);
  assert.equal(calls.length, 0);
  await review.resolve([{ sourceHash: 'sha256', sheet: 'Sheet1', rowNumber: 7, accountId: 5, identityEvidence: 'Corroborated direct source identity.' }]);
  assert.equal(calls[0].url, '/api/v1/max/spreadsheet/proposals/proposal-1/resolve');
  const body = JSON.parse(calls[0].opts.body);
  assert.equal(body.plan, undefined); assert.equal(body.operation_ids, undefined);
  assert.equal(body.resolutions[0].identityEvidence, 'Corroborated direct source identity.');
  assert.equal(review.hasPending(), true);
});

test('configured Jake may resume a source actor proposal; saved receipt recovers without another write', async () => {
  const original = { ...proposal(), actorId: 7, status: 'committed', receipt: { committed: true, selectedOperationIds: ['op-1'] } };
  const { review, calls } = harness({ reply: async () => ({ ok: true, json: async () => ({ spreadsheet_proposal: original }) }) });
  await review.loadScope();
  await review.resume({ id: original.id, conversationId: original.conversationId });
  assert.equal(calls[0].opts.method, undefined, 'resume is a read');
  assert.equal(review.hasPending(), true);
  await assert.rejects(review.commit('Save'), /Jake approval/);
  assert.equal(calls.length, 1, 'no commit is issued for the recovered receipt');
});

test('non-approver cannot resume another source actor proposal', async () => {
  const { review, setScope } = harness();
  setScope({ ...scope(), actor_id: 3, can_approve: false });
  await review.loadScope();
  assert.equal(review.accept({ spreadsheet_proposal: { ...proposal(), actorId: 7 } }), false);
});


test('UI positive phrases send the canonical server approval grammar only after explicit intent', async () => {
  const { explicitlyApproves } = require('../utils/maxSpreadsheetAuthorization');
  for (const phrase of ['Please save selected changes', 'Approve proposal', 'Save selected operations']) {
    const { review, calls } = harness();
    await review.handleText(phrase);
    const body = JSON.parse(calls[0].opts.body);
    assert.equal(body.text, 'approve selected operations');
    assert.equal(explicitlyApproves(body.text), true);
  }
});

test('resolution choices identify scoped account/contact/provider candidates and never preselect a match', () => {
  const item = proposal();
  item.plan.rows = [{ sheet: 'Sheet1', rowNumber: 13, company: 'TD Bank', conflicts: [{ code: 'UNRESOLVED_ACCOUNT' }], accountResolution: { status: 'ambiguous', candidates: ['bank-1'] },
    candidateSummaries: [{ id: 'bank-1', name: 'TD Bank Concord', address: '143 N Main St', phone: '603-229-5722' }],
    contacts: [{ name: 'Roffa', status: 'unresolved', candidates: ['person-1'], candidateSummaries: [{ id: 'person-1', name: 'Roffa Person', email: 'roffa@example.test', title: 'Manager' }] }],
    contactAssertions: [{ name: 'Mark', status: 'unresolved_source_assertion', candidates: ['person-2'], candidateSummaries: [{ id: 'person-2', name: 'Mark Person', phone: '603-222-1111' }] }],
    provider: { raw: 'Nash Family Investment Properties', status: 'unresolved', candidateSummaries: [{ id: 'provider-1', name: 'Nash Family Investment Properties', address: '40 Temple Street' }] },
  }];
  const html = proposalHtml(item, new Set());
  for (const text of ['TD Bank Concord · 603-229-5722 · 143 N Main St · ID bank-1', 'Roffa Person · Manager · roffa@example.test · ID person-1', 'Mark Person · 603-222-1111 · ID person-2', 'Nash Family Investment Properties · 40 Temple Street · ID provider-1', 'data-resolution-provider', 'Leave relationship unresolved']) assert.ok(html.includes(text), text);
  assert.doesNotMatch(html, /<option[^>]+selected/);
});

test('new-account and phone/email update operations explicitly disclose the separate outreach hold', () => {
  const item = proposal();
  item.plan.operations = [
    { id: 'create', type: 'CREATE_ACCOUNT', after: { name: 'New account', outreachReviewRequired: true } },
    { id: 'phone', type: 'SET_ACCOUNT_FIELD', field: 'phone', outreachReviewRequired: true, after: '603-111-2222' },
    { id: 'email', type: 'SET_ACCOUNT_FIELD', field: 'email', outreachReviewRequired: true, after: 'a@example.test' },
  ];
  item.plan.rows[0].admissionHolds = [{ operationId: 'phone', before: false, after: true, reason: 'Separate outreach admission review required.' }];
  const html = proposalHtml(item, new Set(['create', 'phone', 'email']));
  assert.ok(html.includes('Automated outreach hold — before: false; proposed: true.'));
  assert.ok(html.includes('This hold is not an opt-out.'));
  assert.equal((html.match(/Outreach held for separate authorization/g) || []).length, 3);
  assert.equal((html.match(/does not authorize automated calls, emails or messages/g) || []).length, 3);
});
