'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const offer = require('../utils/pulseforgeCashflowOffer');
const { pulseforgeStructuredMission, ensurePulseforgeCashflowMission } = require('../utils/pulseforgeCashflowTenant');
const { freezeStructuredMission } = require('../packages/acquisition-mission/StructuredMission');
const { buildPerProspectVariants, runPaigeVariants } = require('../packages/max/workspace/PaigeVariantsExecutor');
const { policy, candidateReason, hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const now = new Date('2026-10-13T15:00:00Z');
function mission() {
  return { id: 'source', tenantId: '1', clientId: 1, stage: 'ready', objective: 'Review', targetSegment: 'HVAC',
    structuredMission: freezeStructuredMission(pulseforgeStructuredMission(), { approvedAt: now.toISOString(), approvedBy: 'test-operator' }) };
}
function contact(extra = {}) {
  return { id: 'prospect-1', prospect_id: 'prospect-1', company_id: 'company-1', client_id: 1,
    company_name: 'Acme Plumbing', first_name: 'Avery', company_domain: 'acme.example', company_website: 'https://acme.example',
    email: 'owner@acme.example', email_verified: true, email_status: 'valid', vertical: 'plumbing',
    company_location: 'Manchester, NH', service_area_match: true,
    acquisition_metadata: { contactResolution: { finalState: 'VERIFIED_FOUNDER_EMAIL', bestEmail: 'owner@acme.example' } },
    enrichment_provenance: { email: { source: 'website_email', source_url: 'https://acme.example/contact' } }, ...extra };
}
function message(crm = contact()) {
  return { ...offer.buildPulseforgeReviewVariant({ candidate: { name: crm.company_name }, crmRecord: crm }), candidateId: 'prospect-1' };
}
function grant() {
  return policy({ tenantId: '1', sourceMissionId: 'source', senderEmail: 'jacob@pulseforge.com',
    inboxIntegrationId: 'tmi_1_pulseforge_jacob', sendingIdentityId: 'tsi_1_pulseforge_jacob',
    dailyCap: 3, totalCap: 30, spacingMinutes: 240, startsAt: now.toISOString(), expiresAt: '2026-11-12T15:00:00Z' }, now);
}
function queue(crm = contact()) { return { email: crm.email, sendable: true, paige: { candidateId: 'prospect-1' } }; }

test('tenant 1 retains isolated Jacob mailbox, and policy stays bounded first-touch only', () => {
  const { pulseforgeMailboxConfig } = require('../services/tenantMailbox');
  const cfg = pulseforgeMailboxConfig('1');
  assert.equal(cfg.integration.id, 'tmi_1_pulseforge_jacob');
  assert.equal(cfg.identity.senderEmail, 'jacob@pulseforge.com');
  assert.equal(cfg.integration.oauthRefreshSecretRef, 'PULSEFORGE_GOOGLE_REFRESH_TOKEN');
  assert.deepEqual(grant().allowedContactClassifications, ['VERIFIED_FOUNDER_EMAIL', 'VERIFIED_ROLE_EMAIL']);
  assert.equal(grant().maxSequenceStep, 1);
});
test('new canonical $297 review draft makes no payment or turnaround commitment', () => {
  const draft = pulseforgeStructuredMission();
  assert.equal(offer.PULSEFORGE_CASHFLOW_OFFER.price, 297);
  assert.equal(draft.immutable, undefined);
  assert.equal(draft.offer.paymentTerms, undefined);
  assert.equal(draft.offer.turnaroundTargetHours, undefined);
  assert.equal(draft.geography.region, 'Southern New Hampshire');
  assert.deepEqual(draft.market.eligibleSubsegments, ['plumbing', 'heating', 'hvac']);
  assert.equal(offer.pulseforgeMissionReason({ structuredMission: draft }), offer.SOURCE_REVIEW_BLOCKER);
  assert.equal(offer.pulseforgeMissionReason(mission()), null);
});
test('offer fields, scope, exclusions and geography cannot silently drift in a reviewed source', () => {
  for (const change of [
    m => m.structuredMission.offer.id = 'pulseforge_cashflow_repair_sprint_v1',
    m => m.structuredMission.offer.price = 500,
    m => m.structuredMission.offer.paymentTerms = 'paid_upfront',
    m => m.structuredMission.offer.scope = ['Implementation included'],
    m => m.structuredMission.geography.scope = 'nationwide',
    m => m.structuredMission.geography.cities = [],
    m => m.structuredMission.market.eligibleSubsegments.push('restaurant'),
    m => delete m.structuredMission.firstTouchExclusionsVersion,
  ]) {
    const m = structuredClone(mission()); change(m);
    assert.equal(offer.pulseforgeMissionReason(m), offer.SOURCE_REVIEW_BLOCKER);
  }
});
test('all manual emails/domains block only tenant-1 automated first touch', () => {
  for (const row of offer.PULSEFORGE_MANUAL_FIRST_TOUCHES) {
    const cases = [ { email: ` ${row.email.toUpperCase()} ` },
      ...['domain','website','company_domain','company_website','website_url'].map(key => ({ [key]: `https://WWW.${row.domain.toUpperCase()}/contact?q=1` })),
      { domain: `booking.${row.domain}` } ];
    for (const candidate of cases) {
      assert.equal(offer.pulseforgeFirstTouchExclusionReason(candidate, { tenantId: '1', firstTouch: true }), offer.MANUAL_FIRST_TOUCH_BLOCKER);
      for (const tenantId of ['10','13','17']) assert.equal(offer.pulseforgeFirstTouchExclusionReason(candidate, { tenantId, firstTouch: true }), null);
      assert.equal(offer.pulseforgeFirstTouchExclusionReason(candidate, { tenantId: '1', firstTouch: false }), null);
    }
    assert.equal(offer.pulseforgeFirstTouchExclusionReason({ domain: `${row.domain}.example` }, { tenantId: '1', firstTouch: true }), null);
  }
  assert.equal(offer.pulseforgeFirstTouchExclusionReason({ email: 'unrelated@gmail.com' }, { tenantId: '1', firstTouch: true }), null);
});
test('targeting requires plumbing/heating/HVAC and actual southern NH evidence', () => {
  assert.equal(offer.pulseforgeTargetReason(contact()), null);
  for (const vertical of ['plumbing','heating','hvac']) assert.equal(offer.pulseforgeTargetReason(contact({ vertical })), null);
  assert.equal(offer.pulseforgeTargetReason(contact({ vertical: 'restaurant' })), 'mission_segment_mismatch');
  for (const company_location of ['Manchester, CT', 'Boston, MA', 'Berlin, NH', '', 'United States']) {
    assert.equal(offer.pulseforgeTargetReason(contact({ company_location, service_area_match: true })), 'service_area_not_confirmed');
  }
});
test('Paige creates claim-free review copy with exact bounded deliverables', async () => {
  const crm = contact();
  const input = { clientId: 1, mission: mission(), max: { rankedTargets: [{ id: crm.id, prospectId: crm.id, companyId: crm.company_id, name: crm.company_name }] },
    crmByProspectId: new Map([[crm.id, crm]]) };
  const copy = buildPerProspectVariants(input)[0];
  assert.equal(offer.validatePulseforgeCashflowCopy(copy).ok, true);
  assert.match(copy.body, /\$297/);
  assert.match(copy.body, /30-minute walkthrough/);
  assert.match(copy.body, /quoted separately/);
  assert.doesNotMatch(copy.body, /\$500|48 hours|upfront|I found|did not complete/i);
  const result = await runPaigeVariants({ ...input, transactionId: 'tx-1', workspaceContext: { max: input.max, scout: {} }, missionPlan: {} });
  assert.equal(result.status, 'SUCCESS', JSON.stringify(result));
  input.mission = { ...mission(), structuredMission: { immutable: true, offer: { id: 'pulseforge_cashflow_repair_sprint_v1', price: 500 } } };
  const held = buildPerProspectVariants(input);
  assert.equal(held.length, 0);
  assert.equal(held._heldProspects[0].holdReason, offer.SOURCE_REVIEW_BLOCKER);
});
test('candidate guard rejects stale copy and manual contacts, even on approved artifacts', () => {
  assert.equal(candidateReason(queue(), contact(), message(), grant()), null);
  for (const suffix of [' Paid upfront.', ' In 48 hours.', ' I found a broken form.', ' We guarantee revenue.']) {
    assert.equal(candidateReason(queue(), contact(), { ...message(), body: message().body + suffix }, grant()), offer.DOCTRINE_BLOCKER);
  }
  for (const row of offer.PULSEFORGE_MANUAL_FIRST_TOUCHES) {
    assert.equal(candidateReason(queue(), contact({ company_domain: row.domain }), message(), grant()), offer.MANUAL_FIRST_TOUCH_BLOCKER);
  }
  // Other tenants keep their existing doctrine, not Pulseforge's price/scope.
  const other = contact({ client_id: 13 });
  assert.equal(candidateReason(queue(other), other, { candidateId: 'prospect-1', subject: 'A question', body: 'Would a conversation help?' }, { ...grant(), tenantId: '13' }), null);
});
test('approved-copy lookup cannot bypass prior manual contact exclusions', () => {
  const crm = contact({ company_domain: 'donerightheating.com' });
  const variants = buildPerProspectVariants({ clientId: 1, mission: mission(),
    max: { rankedTargets: [{ id: crm.id, prospectId: crm.id, companyId: crm.company_id, name: crm.company_name }] },
    crmByProspectId: new Map([[crm.id, crm]]), approvedCopies: { [crm.id]: { id: 'asset', content: message() } } });
  assert.equal(variants.length, 0);
  assert.equal(variants._heldProspects[0].holdReason, offer.MANUAL_FIRST_TOUCH_BLOCKER);
});
test('inventory is scoped and obsolete source cannot fall through to nationwide eligibility', async () => {
  const { loadCleanInventory } = require('../services/maxOutboundControlLoop');
  const crm = contact();
  const pool = { query: async sql => ({ rows: sql.includes('SELECT p.*') ? [crm] : [] }) };
  const store = { clientId: 1, candidateOwnership: async () => null, suppression: async () => null };
  assert.equal((await loadCleanInventory(pool, store, { mission: mission() }, 1, grant())).clean.length, 1);
  crm.company_domain = 'overflowplumbing.co';
  assert.equal((await loadCleanInventory(pool, store, { mission: mission() }, 1, grant())).exclusionCounts[offer.MANUAL_FIRST_TOUCH_BLOCKER], 1);
  const old = { structuredMission: { offer: { id: 'pulseforge_cashflow_repair_sprint_v1' }, geography: { scope: 'nationwide' } } };
  assert.equal((await loadCleanInventory(pool, store, old, 1, grant())).exclusionCounts[offer.SOURCE_REVIEW_BLOCKER], 1);
});
test('bootstrap preserves existing reviewed mission/tenant bytes and propagates read failures', async () => {
  const existing = { id: 'historical', payload: { structuredMission: { immutable: true, approvedAt: 'old', offer: { price: 500 } }, historicalManualCopy: 'Original offer, unchanged' } };
  const before = JSON.stringify(existing);
  const calls = [];
  const db = { query: async (sql, args) => { calls.push([sql,args]); return { rows: sql.includes('clients') ? [{ id: 1, lead_with: 'Original profile' }] : [existing] }; } };
  const result = await ensurePulseforgeCashflowMission(db);
  assert.equal(result.created, false);
  assert.equal(JSON.stringify(existing), before);
  assert.ok(calls.every(([sql]) => sql.startsWith('SELECT')));
  assert.deepEqual(calls[1][1], ['1', offer.PULSEFORGE_CASHFLOW_OFFER_ID]);
  await assert.rejects(ensurePulseforgeCashflowMission({ query: async sql => { if (sql.includes('clients')) return { rows: [{ id: 1 }] }; throw new Error('offline'); } }), /offline/);
});
test('new bootstrap source is a canonical unapproved plan with an approval decision', async () => {
  let created;
  const db = { query: async sql => ({ rows: sql.includes('clients') ? [{ id: 1 }] : [] }) };
  const result = await ensurePulseforgeCashflowMission(db, { create: async input => {
    created = require('../packages/acquisition-mission').createAcquisitionMissionEngine().create(input);
    return created;
  } });
  assert.equal(result.reviewRequired, true);
  assert.equal(created.structuredMission, null);
  assert.equal(created.structuredMissionApproved, false);
  assert.equal(created.pendingOperatorDecision.kind, 'plan_approval');
  assert.equal(created.missionPlanDraft.offer.id, offer.PULSEFORGE_CASHFLOW_OFFER_ID);
  const approved = freezeStructuredMission(created.missionPlanDraft, { approvedBy: 'test' });
  assert.equal(offer.pulseforgeMissionReason({ structuredMission: approved }), null);
});
test('authorization rejects stale source without writes or provider calls; current source requires review', async () => {
  let writes = 0;
  let source = mission();
  const svc = require('../services/governedOutbound').service({ tenantId: '1', pool: { query: async () => { writes++; throw new Error('unexpected write'); } },
    adapters: { loadMission: async () => ({ mission: source }) }, now: () => now, enabled: () => false });
  const reviewed = await svc.authorize(grant(), { id: 'Jacob', role: 'admin' });
  assert.equal(reviewed.reviewRequired, true);
  source = { ...mission(), structuredMission: { immutable: true, offer: { id: 'pulseforge_cashflow_repair_sprint_v1' } } };
  await assert.rejects(svc.authorize(grant(), { id: 'Jacob', role: 'admin' }), { code: offer.SOURCE_REVIEW_BLOCKER });
  assert.equal(writes, 0);
});
test('old activation token and create-program without reviewed mission cannot mutate anything', async () => {
  const activation = require('../scripts/pulseforgeCashflowActivation');
  assert.throws(() => activation.confirmed({ confirm: 'pulseforge-cashflow-2026-10-13' }), { code: 'confirm_required' });
  await assert.rejects(activation.createGovernedProgram(), { code: 'reviewed_mission_id_required' });
  await assert.rejects(activation.activateProgram(), { code: 'reviewed_program_binding_required' });
});

test('location cannot borrow a matching city from a different state/source', () => {
  assert.equal(offer.pulseforgeTargetReason(contact({ company_location: 'Southern NH' })), 'service_area_not_confirmed');
  assert.equal(offer.pulseforgeTargetReason(contact({ company_location: 'Portsmouth, Virginia', location: 'Berlin, NH' })), 'service_area_not_confirmed');
  assert.equal(offer.validatePulseforgeCashflowCopy(null).ok, false);
});
test('obsolete source stops Max before research or any other external calls', async () => {
  const result = await require('../services/maxOutboundControlLoop').runMaxOutboundControlLoop({
    tenantId: '1', program: { id: 'old', tenant_id: '1', mode: 'active', policy: grant() },
    pool: { query: async () => { throw new Error('unexpected DB call'); } },
    source: { payload: { structuredMission: { immutable: true, offer: { id: 'pulseforge_cashflow_repair_sprint_v1' } } } },
    governedAdapters: { infrastructure: async () => { throw new Error('unexpected infrastructure/research'); } },
  });
  assert.equal(result.halted, offer.SOURCE_REVIEW_BLOCKER);
});

function scheduleFixture({ oldSource = false, staleCopy = false, excluded = false } = {}) {
  const m = mission();
  if (oldSource) m.structuredMission = { immutable: true, offer: { id: 'pulseforge_cashflow_repair_sprint_v1' } };
  const p = grant();
  const program = { id: 'program', tenant_id: '1', source_mission_id: 'source', mode: 'active', policy: p, policy_hash: hash(p), scope_hash: hash(missionScope(m)) };
  const copy = message();
  if (staleCopy) copy.body = '$500 repair sprint, paid upfront, 48 hours.\nJacob';
  const crm = contact(excluded ? { company_website: 'https://clearallcoolingandheating.com' } : {});
  const snapshot = { candidateId: crm.id, prospectId: crm.id, companyId: crm.company_id, email: crm.email, message: copy, sender: { senderEmail: p.senderEmail } };
  const item = { id: 'item', candidate_id: crm.id, prospect_id: crm.id, company_id: crm.company_id, email: crm.email, snapshot, status: 'attempted', attempted_at: now };
  const envelope = { id: 'envelope', program_id: program.id, status: 'authorized', approval_id: 'approval', revision: 'rev', mission_id: m.id, manifest: [snapshot] };
  const binding = { programId: program.id, policyHash: program.policy_hash, envelopeId: envelope.id, itemId: item.id, manifestHash: hash(envelope.manifest), approvalId: envelope.approval_id, revision: envelope.revision, mailboxIntegrationId: p.inboxIntegrationId, outreachAssetId: 'asset' };
  const schedule = { id: 'schedule', tenantId: '1', prospectId: crm.id, missionId: m.id, sendingIdentityId: p.sendingIdentityId, recipientEmail: crm.email, outreachAssetId: 'asset', outreachAssetVersion: '1', authorizationSnapshot: { subject: copy.subject, body: copy.body, recipientEmail: crm.email, governed: binding } };
  const store = { program: async () => program, envelope: async () => envelope, items: async () => [item], counts: async () => ({ today: 1, total: 1, uncertain: 0 }), suppression: async () => null };
  let liveCalls = 0;
  const adapters = { loadMission: async () => ({ mission: m }), prepared: async () => ({ revision: 'rev', candidates: [{ candidateId: crm.id, item: queue(crm), message: copy }] }), contact: async () => crm, liveGate: async () => { liveCalls++; } };
  const outreachAsset = { version: 1, content: { subject: copy.subject, body: copy.body, prospectId: crm.id, preparedArtifactRevision: 'rev' } };
  return { schedule, store, adapters, outreachAsset, liveCalls: () => liveCalls };
}
test('durable dispatch rejects old sources, stale copy and manual contacts before live/provider gate', async () => {
  const { validateGovernedSchedule } = require('../services/governedTenantSchedule');
  const prior = process.env.PULSEFORGE_GOVERNED_OUTBOUND_ENABLED;
  process.env.PULSEFORGE_GOVERNED_OUTBOUND_ENABLED = 'true'; // local stub fixture only
  try {
    for (const [change, code] of [ [{ oldSource: true }, offer.SOURCE_REVIEW_BLOCKER], [{ staleCopy: true }, offer.DOCTRINE_BLOCKER], [{ excluded: true }, offer.MANUAL_FIRST_TOUCH_BLOCKER] ]) {
      const f = scheduleFixture(change);
      await assert.rejects(validateGovernedSchedule(f.schedule, { pool: {}, now, governedStore: f.store, governedAdapters: f.adapters, outreachAsset: f.outreachAsset }), { code });
      assert.equal(f.liveCalls(), 0);
    }
    const f = scheduleFixture();
    assert.equal((await validateGovernedSchedule(f.schedule, { pool: {}, now, governedStore: f.store, governedAdapters: f.adapters, outreachAsset: f.outreachAsset })).item.id, 'item');
  } finally { if (prior === undefined) delete process.env.PULSEFORGE_GOVERNED_OUTBOUND_ENABLED; else process.env.PULSEFORGE_GOVERNED_OUTBOUND_ENABLED = prior; }
});
test('review holds are terminal definitely-unsent results, without DNC writes or retries', async () => {
  const { finishGovernedSchedule } = require('../services/governedTenantSchedule');
  const { isPreProviderOutboundFailure, isRetryablePreProviderScheduleRejection } = require('../services/governedOutboundProviderBoundary');
  for (const reason of [offer.SOURCE_REVIEW_BLOCKER, offer.DOCTRINE_BLOCKER, offer.MANUAL_FIRST_TOUCH_BLOCKER, 'service_area_not_confirmed', 'mission_segment_mismatch', 'public_website_required']) {
    const f = scheduleFixture(); const finished = []; const events = [];
    assert.equal(isPreProviderOutboundFailure({ code: reason }), true);
    assert.equal(isRetryablePreProviderScheduleRejection(reason), false);
    f.store.finish = async (...args) => finished.push(args);
    f.store.event = async (...args) => events.push(args);
    f.store.releaseUnsent = async () => { throw new Error('must not automatically retry'); };
    await finishGovernedSchedule(f.schedule, { result: 'failed', error: { code: reason } }, { pool: {}, governedStore: f.store });
    assert.equal(finished[0][1], 'suppressed');
    assert.equal(events[0][2].providerBoundaryCrossed, false);
  }
});
