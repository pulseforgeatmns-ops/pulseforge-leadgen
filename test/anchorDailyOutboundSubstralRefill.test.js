'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { selectInventoryRefillEntries, selectRefillEntries } = require('../services/governedOutboundRefill');

const sender = { senderEmail: 'sender@substral.example', senderName: 'Studio Substral' };
const program = { tenant_id: '17', policy: { tenantId: '17', allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL'] } };
function contact(id = 'p1', tenantId = 17, extra = {}) {
  const crm = { id, prospect_id: id, client_id: tenantId, company_id: `company-${id}`,
    company_name: `Company ${id}`, domain: 'customer.example',
    email: `${id}@customer.example`, email_verified: true, email_status: 'valid', do_not_contact: false,
    enrichment_provenance: { email: { source: 'website_email' } }, ...extra };
  crm.acquisition_metadata = extra.acquisition_metadata || {
    contactResolution: { finalState: 'VERIFIED_FOUNDER_EMAIL', bestEmail: crm.email },
  };
  return crm;
}
function rowFor(crm) {
  return { candidateId: crm.id, prospectId: crm.id, companyId: crm.company_id,
    company: crm.company_name, domain: crm.domain, email: crm.email };
}
function preparedFor(contacts) {
  return { revision: 'reviewed-paige-revision', sender,
    candidates: contacts.map(crm => ({ candidateId: crm.id,
      item: { ...rowFor(crm), sendable: true, dnc: false, paige: { candidateId: crm.id } },
      message: { candidateId: crm.id, companyId: crm.company_id, companyName: crm.company_name,
        subject: 'A Studio Substral question', body: 'Would a conversation about your customer journey be useful?' } })) };
}

test('Substral inventory fallback fails closed before the Anchor copy builder is reached', async () => {
  const crm = contact();
  const row = rowFor(crm);
  delete row.company;
  // Only the copy builder needs this field when the row has no company label.
  Object.defineProperty(crm, 'company_name', { get() { assert.fail('Anchor copy must never be built for tenant 17'); } });
  const decisions = [];
  let suppressionCalls = 0;
  const selected = await selectInventoryRefillEntries({ cleanRows: [row], program,
    prepared: { sender, revision: 'reviewed-paige-revision' },
    adapters: { contact: async () => crm },
    store: { suppression: async () => { suppressionCalls++; return null; } }, limit: 5, decisions });
  assert.deepEqual(selected, []);
  assert.deepEqual(decisions, [{ candidateId: crm.id, prospectId: crm.id, source: 'inventory',
    outcome: 'rejected', reason: 'tenant_specific_paige_copy_required' }]);
  assert.equal(suppressionCalls, 0, 'copy rejection stops the fallback before subsequent gates');
});

test('tenant 17 inventory cannot fall through when only one tenant identity source is present', async () => {
  for (const [context, crm] of [
    [{ tenant_id: '17' }, contact()],
    [{ policy: program.policy }, contact()],
    [undefined, contact()],
    [undefined, contact('p1', undefined, { client_id: undefined, tenant_id: '17' })],
    [{ tenant_id: 17, policy: { tenantId: '13' } }, contact('p1', 13)],
  ]) {
    const decisions = [];
    const selected = await selectInventoryRefillEntries({ cleanRows: [rowFor(crm)], program: context,
      prepared: { sender }, adapters: { contact: async () => crm },
      store: { suppression: async () => null }, limit: 5, decisions });
    assert.deepEqual(selected, []);
    assert.equal(decisions[0].reason, 'tenant_specific_paige_copy_required');
  }
});

test('Substral can still refill with its existing reviewed, bound Paige candidates', async () => {
  const crm = contact();
  const prepared = preparedFor([crm]);
  const decisions = [];
  const checked = [];
  const selected = await selectRefillEntries({ prepared, program,
    adapters: { contact: async () => crm }, store: { suppression: async entry => { checked.push(entry.candidateId); return null; } },
    limit: 1, decisions });
  assert.equal(selected.length, 1);
  assert.equal(selected[0].message, prepared.candidates[0].message);
  assert.equal(selected[0].revision, prepared.revision);
  assert.equal(selected[0].sender, sender);
  assert.equal(selected[0].candidateId, crm.id);
  assert.deepEqual(checked, [crm.id]);
  assert.deepEqual(decisions.map(d => [d.source, d.outcome, d.reason]), [['prepared', 'selected', null]]);
});

test('Substral prepared candidates retain contact, tenant, address and copy binding gates', async () => {
  for (const [change, expected] of [
    [f => { f.crm.client_id = 13; }, 'contact_tenant_mismatch'],
    [f => { f.crm.do_not_contact = true; }, 'do_not_contact'],
    [f => { f.crm.acquisition_metadata.contactResolution.finalState = 'REVIEW_REQUIRED'; }, 'contact_classification_not_sendable'],
    [f => { f.crm.acquisition_metadata.contactResolution.finalState = 'VERIFIED_ROLE_EMAIL'; }, 'contact_classification_not_authorized'],
    [f => { f.crm.acquisition_metadata.contactResolution.bestEmail = 'other@customer.example'; }, 'contact_classification_email_changed'],
    [f => { f.prepared.candidates[0].message.candidateId = 'someone-else'; }, 'copy_binding_changed'],
    [f => { f.prepared.candidates[0].message.body = 'Mission focus: internal'; }, 'unsafe_paige_copy'],
    [f => { f.prepared.candidates[0].message.companyId = 'someone-else'; }, 'company_binding_mismatch'],
  ]) {
    const crm = contact();
    const prepared = preparedFor([crm]);
    change({ crm, prepared });
    const decisions = [];
    assert.deepEqual(await selectRefillEntries({ prepared, program,
      adapters: { contact: async () => crm }, store: { suppression: async () => null }, limit: 5, decisions }), []);
    assert.equal(decisions[0].reason, expected);
  }
});

test('Substral prepared refill keeps suppression, envelope/email/company dedupe and bounded selection', async () => {
  const contacts = [contact('good'), contact('existing'), contact('email-duplicate', 17, { email: 'good@customer.example' }),
    contact('company-duplicate', 17, { company_id: 'company-good', company_name: 'Company good' }),
    contact('suppressed'), contact('over-limit')];
  const decisions = [];
  const selected = await selectRefillEntries({ prepared: preparedFor(contacts), program,
    adapters: { contact: async id => contacts.find(crm => crm.id === id) },
    store: { suppression: async entry => entry.candidateId === 'suppressed' ? 'reply_received' : null },
    existingItems: [{ candidate_id: 'existing', prospect_id: 'existing', company_id: 'company-existing', email: 'existing@customer.example' }],
    limit: 1, decisions });
  assert.deepEqual(selected.map(entry => entry.candidateId), ['good']);
  assert.deepEqual(decisions.map(d => d.reason), [null, 'already_in_envelope', 'duplicate_email', 'duplicate_company',
    'reply_received', 'preparation_limit_reached']);
  assert.equal(decisions.at(-1).outcome, 'deferred');
});

test('Anchor and Babrun inventory behavior, suppression and dedupe remain unchanged', async () => {
  for (const tenantId of [10, 13]) {
    const contacts = [contact('good', tenantId), contact('dnc', tenantId, { do_not_contact: true }),
      contact('suppressed', tenantId), contact('existing', tenantId),
      contact('email-duplicate', tenantId, { email: 'good@customer.example' }),
      contact('company-duplicate', tenantId, { company_id: 'company-good', company_name: 'Company good' }), contact('over-limit', tenantId)];
    const decisions = [];
    const selected = await selectInventoryRefillEntries({ cleanRows: contacts.map(rowFor),
      program: { tenant_id: String(tenantId), policy: { tenantId: String(tenantId) } },
      prepared: { sender: { senderEmail: 'sender@business.example', senderName: 'Jacob Maynard' }, revision: 'legacy-revision' },
      adapters: { contact: async id => contacts.find(crm => crm.id === id) },
      store: { suppression: async entry => entry.candidateId === 'suppressed' ? 'reply_received' : null },
      existingItems: [{ candidate_id: 'existing', prospect_id: 'existing', company_id: 'company-existing', email: 'existing@customer.example' }],
      limit: 1, decisions });
    assert.deepEqual(selected.map(entry => entry.candidateId), ['good']);
    assert.ok(selected[0].message.subject);
    assert.ok(selected[0].message.body);
    assert.equal(selected[0].message.candidateId, 'good');
    assert.deepEqual(decisions.map(d => d.reason), [null, 'do_not_contact', 'reply_received', 'already_in_envelope',
      'duplicate_email', 'duplicate_company', 'preparation_limit_reached']);
  }
});
