'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { candidateReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { governedContactReason } = require('../utils/governedContactEligibility');
const { selectInventoryRefillEntries, selectRefillEntries, observabilityFromRefill } = require('../services/governedOutboundRefill');
const { loadCleanInventory } = require('../services/maxOutboundControlLoop');
const { persistProviderChainEmail } = require('../scripts/lib/anchorMissionBoundEnrichment');
const { filterScrapedWebsiteEmails } = require('../leadgen');

const policy = { tenantId: '10' };
const program = { policy };
const prepared = { sender: { senderName: 'Jacob Maynard', senderEmail: 'sender@anchor.example' }, revision: 'rev' };
const store = { clientId: 10, candidateOwnership: async () => null, suppression: async () => null };
const contact = (id = 'p1', extra = {}) => ({ id, prospect_id: id, client_id: 10, company_id: `company-${id}`,
  company_name: `Company ${id}`, domain: 'customer.example', company_domain: 'customer.example',
  email: `${id}@customer.example`, email_verified: true, email_status: 'valid', do_not_contact: false,
  enrichment_provenance: { email: { source: 'website_email' } }, service_area_match: true,
  vertical: 'property_management', ...extra });
const rowFor = c => ({ candidateId: c.id, prospectId: c.id, companyId: c.company_id,
  company: c.company_name, domain: c.domain, email: c.email });
const copyFor = id => ({ candidateId: id, subject: 'Cleaning support', body: 'Would a written quote help?' });
const itemFor = c => ({ ...rowFor(c), sendable: true, paige: { candidateId: c.id } });

// Minimal reproduction of the four CRM records captured read-only on 2026-10-01.
const incident = [
  ['factory', 'The Factory on Willow', 'factoryonwillow.com', 'leasing@orbitgroup.com'],
  ['metropolis', 'Metropolis Property Management Group', 'metro-pmg.com', 'info@anagnost.com'],
  ['blue-door', 'Blue Door Living Property Management', 'bluedoorliving.org', 'ryan.weiss@bluedoorliving.org'],
  ['landlord', 'Your Landlord LLC Property Management', 'yourlandlordllc.com', 'info@yourlandlordllc.com'],
].map(([id, company_name, domain, email]) => contact(id, { company_name, domain, company_domain: domain, email, enrichment_provenance: {} }));

test('all four incident contacts receive exactly one missing-provenance rejection and are not clean inventory', async () => {
  const decisions = [];
  const selected = await selectInventoryRefillEntries({ cleanRows: incident.map(rowFor), prepared, program, store,
    adapters: { contact: async id => incident.find(c => c.id === id) }, limit: 5, decisions });
  assert.deepEqual(selected, []);
  assert.equal(decisions.length, 4);
  assert.equal(new Set(decisions.map(x => x.candidateId)).size, 4);
  assert.ok(decisions.every(x => x.outcome === 'rejected' && x.reason === 'missing_email_provenance'));
  const db = { query: async sql => ({ rows: /acquisition_knowledge_objects/.test(sql) ? [] : incident }) };
  const inventory = await loadCleanInventory(db, store, { payload: { targetSegment: 'short_term_rental' } }, 10, policy);
  assert.equal(inventory.clean.length, 0);
  assert.deepEqual(inventory.exclusionCounts, { missing_email_provenance: 4 });
  assert.equal(observabilityFromRefill({}, { preparationDecisions: decisions }).preparationDecisions.length, 4);
});

test('MoxiWorks incident is blocked at scrape, provider persistence, preparation and final candidate gate', async () => {
  assert.deepEqual(filterScrapedWebsiteEmails('support@moxiworks.com contact@adoptapetrealestate.com', 'adoptapetrealestate.com'), ['contact@adoptapetrealestate.com']);
  const crm = contact('danielle', { company_name: 'Danielle Alto - Realtor NH & MA', domain: 'adoptapetrealestate.com',
    company_domain: 'adoptapetrealestate.com', email: 'support@moxiworks.com', enrichment_provenance: { email: { source: 'scraped' } } });
  const writes = [];
  const saved = await persistProviderChainEmail({ query: async (...args) => writes.push(args) }, crm,
    { email: crm.email, source: ['scraped'] }, { emailVerified: true, emailStatus: 'valid' }, false);
  assert.equal(saved.reason, 'recipient_company_domain_mismatch');
  assert.equal(writes.length, 0);
  assert.equal(candidateReason(itemFor(crm), crm, copyFor(crm.id), policy), 'recipient_company_domain_mismatch');
  const decisions = [];
  const selected = await selectRefillEntries({ prepared: { ...prepared, candidates: [{ candidateId: crm.id, item: itemFor(crm), message: copyFor(crm.id) }] },
    program, store, adapters: { contact: async () => crm }, limit: 5, decisions });
  assert.deepEqual(selected, []);
  assert.equal(decisions[0].reason, 'recipient_company_domain_mismatch');
});

test('exact contact, company, tenant, domain and message context must agree', () => {
  const crm = contact(); const item = itemFor(crm); const copy = copyFor(crm.id);
  assert.equal(candidateReason(item, crm, copy, policy), null);
  for (const [change, reason] of [
    [{ crmProspectId: 'another' }, 'crm_contact_binding_mismatch'],
    [{ crmCompanyId: 'another' }, 'crm_company_binding_mismatch'],
    [{ companyId: 'another' }, 'company_binding_mismatch'],
    [{ company: 'Someone Else' }, 'message_company_context_mismatch'],
    [{ domain: 'other.example' }, 'company_domain_binding_mismatch'],
  ]) assert.equal(candidateReason({ ...item, ...change }, crm, copy, policy), reason);
  assert.equal(candidateReason(item, crm, { ...copy, companyName: 'Someone Else' }, policy), 'message_company_context_mismatch');
  const resolved = require('../packages/acquisition-mission/OutboundExecution').resolvePaigeVariant({ variants: [
    { ...copy, companyId: crm.company_id, companyName: 'Someone Else', label: 'Primary' },
  ] }, { candidateId: crm.id, includeIdentity: true });
  assert.equal(candidateReason(item, crm, resolved, policy), 'message_company_context_mismatch');
  assert.equal(candidateReason({ ...item, paige: { ...item.paige, subject: 'Different approved copy' } }, crm, copy, policy), 'capacity_copy_binding_mismatch');
  assert.equal(candidateReason(item, { ...crm, client_id: 13 }, copy, policy), 'contact_tenant_mismatch');
  assert.equal(candidateReason(item, crm, { ...copy, candidateId: 'another' }, policy), 'copy_binding_changed');
  assert.equal(governedContactReason({ ...crm, domain: null, company_domain: null }), 'missing_company_domain');
});

test('valid contacts prepare; failures, duplicates, stale bindings and limits each receive one outcome', async () => {
  const rows = ['good', 'missing', 'lookup-error', 'dnc', 'owned', 'wrong-company', 'wrong-contact', 'duplicate', 'limit'].map(id => rowFor(contact(id)));
  const decisions = [];
  const selected = await selectInventoryRefillEntries({ cleanRows: rows, prepared, program,
    store: { ...store, suppression: async entry => entry.candidateId === 'owned' ? 'ao_owned' : null },
    adapters: { contact: async id => {
      if (id === 'missing') return null;
      if (id === 'lookup-error') throw Object.assign(new Error('db failed'), { code: '08006' });
      return contact(id, id === 'dnc' ? { do_not_contact: true } : id === 'wrong-company' ? { company_id: 'changed' }
        : id === 'wrong-contact' ? { id: 'other', prospect_id: 'other' } : {});
    } }, existingItems: [{ candidate_id: 'duplicate', prospect_id: 'duplicate', company_id: 'company-duplicate', email: 'duplicate@customer.example' }],
    limit: 1, decisions });
  assert.deepEqual(selected.map(x => x.candidateId), ['good']);
  assert.equal(selected[0].message.companyId, 'company-good');
  assert.equal(selected[0].message.companyName, 'Company good');
  assert.deepEqual(decisions.map(x => x.reason), [null, 'missing_row', 'preparation_evaluation_failed', 'do_not_contact', 'ao_owned',
    'crm_company_binding_mismatch', 'crm_contact_binding_mismatch', 'already_in_envelope', 'preparation_limit_reached']);
  assert.equal(decisions.length, rows.length);
  assert.equal(decisions.at(-1).outcome, 'deferred');
});

test('adding only a source label never makes unrelated corporate domains eligible', () => {
  const repaired = incident.map(c => ({ ...c, enrichment_provenance: { email: { source: 'website_email' } } }));
  assert.deepEqual(repaired.map(c => governedContactReason(c)), ['recipient_company_domain_mismatch', 'recipient_company_domain_mismatch', null, null]);
});
