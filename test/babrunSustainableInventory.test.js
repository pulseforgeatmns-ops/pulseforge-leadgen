'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { buildMarketDefinition } = require('../packages/scout/intelligence/MarketUnderstanding');
const { expandPlacesQueriesForVertical } = require('../packages/scout/hypothesis/MarketHypothesisRegistry');
const { evaluateReplenishmentAdmission } = require('../utils/replenishmentVertical');
const {
  loadCleanInventory,
  defaultScoutRamp,
  loadScoutDiscoveryBackoff,
  _test: { persistDiscoveredCompanies },
} = require('../services/maxOutboundControlLoop');
const {
  MemoryScheduleStore,
  SCHEDULE_STATUS,
  executeScheduledSend,
} = require('../services/tenantOutreachScheduler');
const {
  MemoryTenantMailboxStore,
  MAILBOX_STATUS,
  IDENTITY_STATUS,
} = require('../services/tenantMailbox');
const governedScheduleBridge = require('../services/governedTenantSchedule');
const { classifyProvenUnsent } = require('../services/governedUncertainSendReconciliation');

const BABRUN_SOURCE = {
  id: 'mission_babrun',
  objective: 'Enroll founder-led small business owners in the Babrun program.',
  payload: {
    structuredMission: {
      market: { segment: 'small_business_owners', industry: 'general', buyer: 'business_owner' },
      geography: { region: 'United States', scope: 'nationwide', cities: [] },
    },
  },
};

test('Scout projects Babrun mission into the same concrete taxonomy consumed by admission', async () => {
  const market = buildMarketDefinition({
    mission: {
      tenantId: '13',
      objective: BABRUN_SOURCE.objective,
      structuredMission: BABRUN_SOURCE.payload.structuredMission,
    },
    delegation: {
      tenantId: '13',
      targetContext: { geography: 'United States', segments: ['small_business_owners'] },
      businessContext: { serviceGeography: 'United States', preferredSegments: ['small_business_owners'] },
    },
  });
  assert.equal(market.segmentKey, 'small_business_owners');
  assert.ok(market.terminology.includes('Painting Contractor'));
  assert.ok(expandPlacesQueriesForVertical('small_business_owners', { city: 'Denver', state: 'CO' })
    .some(query => /painting contractor Denver CO/i.test(query)));

  const candidate = {
    name: "Barco's Painting of Colorado",
    description: 'Founder-owned painting company and painting contractor',
    location: 'Castle Rock, Colorado',
    website: 'https://barcospainting.example',
    domain: 'barcospainting.example',
    placeTypes: ['painter'],
  };
  const admission = evaluateReplenishmentAdmission(candidate, {
    missionSegment: 'small_business_owners',
    region: 'United States',
  });
  assert.deepEqual({ admitted: admission.admitted, vertical: admission.vertical }, {
    admitted: true,
    vertical: 'painting',
  });

  let persistedVertical = null;
  const persistencePool = {
    query: async (sql, params = []) => {
      if (/SELECT p\.id, p\.company_id/.test(sql)) return { rows: [] };
      if (/INSERT INTO scout_unenriched/.test(sql)) {
        persistedVertical = params[3];
        return { rowCount: 1, rows: [{ id: 'unenriched-barco' }] };
      }
      return { rows: [], rowCount: 0 };
    },
  };
  const persisted = await persistDiscoveredCompanies(persistencePool, {
    tenantId: '13', clientId: 13, candidateOwnership: async () => null,
  }, {
    tenantId: '13',
    companies: [candidate],
    scoutContext: {
      authorizedTenantId: '13',
      clientId: 13,
      scope: { segment: 'small_business_owners', region: 'United States', scope: 'nationwide' },
    },
  });
  assert.equal(persisted.inserted, 1);
  assert.equal(persistedVertical, 'painting');

  const founder = {
    id: 'prospect-barco', prospect_id: 'prospect-barco', client_id: 13,
    company_id: 'company-barco', company_name: candidate.name,
    email: 'jeremy@barcospainting.example', email_verified: true, email_status: 'valid',
    vertical: persistedVertical, service_area_match: null, do_not_contact: false,
    acquisition_metadata: {
      contactResolution: {
        finalState: 'VERIFIED_FOUNDER_EMAIL',
        bestEmail: 'jeremy@barcospainting.example',
      },
    },
    knowledge_id: 'ak-barco',
    knowledge_content: {
      company: candidate.name,
      contact: 'Jeremy Barton', role: 'Owner', location: candidate.location,
      vertical: persistedVertical,
      icpEvaluation: { fit: true, reasons: [{ kind: 'OBSERVED', text: 'Founder-operated painting company' }] },
      contactResolution: {
        finalState: 'VERIFIED_FOUNDER_EMAIL',
        bestEmail: 'jeremy@barcospainting.example',
      },
    },
    knowledge_provenance: { sourceUrls: [candidate.website] },
  };
  const projectionPool = {
    query: async (sql) => {
      if (/SELECT p\.\*, p\.id AS prospect_id/.test(sql)) return { rows: [founder] };
      if (/SELECT p\.\*, c\.name AS company_name/.test(sql)) return { rows: [founder] };
      return { rows: [] };
    },
  };
  const clean = await loadCleanInventory(projectionPool, {
    clientId: 13,
    candidateOwnership: async () => null,
    suppression: async () => null,
  }, BABRUN_SOURCE, 13, { allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL'] });
  assert.equal(clean.clean.length, 1);
  assert.equal(clean.clean[0].prospectId, 'prospect-barco');
});

test('three identical zero-yield discoveries produce explicit cooldown without bypassing existing recovery', async () => {
  const now = new Date('2026-10-06T14:00:00.000Z');
  const pool = {
    query: async () => ({ rows: [
      { created_at: '2026-10-06T13:45:00.000Z', payload: { newCleanInventoryAdded: 0, newPromotions: 0, recoveredExisting: 0 } },
      { created_at: '2026-10-06T13:30:00.000Z', payload: { newCleanInventoryAdded: 0, newPromotions: 0, recoveredExisting: 0 } },
      { created_at: '2026-10-06T13:15:00.000Z', payload: { newCleanInventoryAdded: 0, newPromotions: 0, recoveredExisting: 0 } },
    ] }),
  };
  const backoff = await loadScoutDiscoveryBackoff(pool, '13', 'same-search', now);
  assert.equal(backoff.reason, 'repeated_zero_yield_identical_search');
  assert.equal(backoff.retryAt, '2026-10-06T14:45:00.000Z');

  let recoveryChecks = 0;
  let discoveryCalls = 0;
  await defaultScoutRamp({
    pool: { query: async () => ({ rows: [] }) },
    store: { tenantId: '13', clientId: 13, event: async () => {} },
    program: { id: 'program', tenant_id: '13', source_mission_id: BABRUN_SOURCE.id },
    source: BABRUN_SOURCE,
    plan: { deficit: 1 },
    skipVerificationRetry: true,
    enrichment: { run: async () => ({ considered: 0, promoted: 0, recovered: 0 }) },
    recoverExistingInventory: async () => {
      recoveryChecks += 1;
      return { payload: { qualifiedCount: 1, source: 'governed_clean_inventory' } };
    },
    loadDiscoveryBackoff: async () => {
      throw new Error('backoff lookup should occur only after existing recovery is exhausted');
    },
    runDiscovery: async () => { discoveryCalls += 1; },
  });
  assert.equal(recoveryChecks, 1);
  assert.equal(discoveryCalls, 0);

  const events = [];
  const ramp = await defaultScoutRamp({
    pool: { query: async () => ({ rows: [] }) },
    store: {
      tenantId: '13', clientId: 13,
      event: async (type, _key, payload) => events.push({ type, payload }),
    },
    program: { id: 'program', tenant_id: '13', source_mission_id: BABRUN_SOURCE.id },
    source: BABRUN_SOURCE,
    plan: { deficit: 1 },
    skipVerificationRetry: true,
    enrichment: { run: async () => ({ considered: 0, promoted: 0, recovered: 0 }) },
    recoverExistingInventory: async () => ({ payload: { qualifiedCount: 0 } }),
    loadDiscoveryBackoff: async () => backoff,
    runDiscovery: async () => { discoveryCalls += 1; },
  });
  assert.equal(discoveryCalls, 0);
  assert.equal(ramp.discovery.kind, 'backoff');
  assert.equal(ramp.discoveryBackoff.reason, 'repeated_zero_yield_identical_search');
  assert.equal(events[0].type, 'scout_replenishment_backoff');
});

async function governedScheduleFixture() {
  const mailboxStore = new MemoryTenantMailboxStore();
  await mailboxStore.saveIntegration({
    id: 'mailbox', tenantId: '13', providerType: 'GENERIC_SMTP_IMAP',
    mailboxAddress: 'hello@babrun.com', status: MAILBOX_STATUS.ACTIVE,
  });
  await mailboxStore.saveIdentity({
    id: 'identity', tenantId: '13', mailboxIntegrationId: 'mailbox',
    senderEmail: 'hello@babrun.com', status: IDENTITY_STATUS.ACTIVE,
  });
  const scheduleStore = new MemoryScheduleStore({
    tenants: [{ tenantId: '13', active: true }],
    prospects: [{ tenantId: '13', prospectId: 'prospect', email: 'founder@example.com' }],
    outreachAssets: [{ tenantId: '13', id: 'asset', lifecycleState: 'STAKEHOLDER_VALIDATED' }],
  });
  const schedule = await scheduleStore.saveSchedule({
    id: 'schedule', tenantId: '13', prospectId: 'prospect', outreachAssetId: 'asset',
    outreachAssetVersion: '1', sendingIdentityId: 'identity', recipientEmail: 'founder@example.com',
    missionId: 'mission', sequenceStep: 1, scheduledFor: '2026-10-06T14:00:00.000Z',
    status: SCHEDULE_STATUS.EXECUTING, authorizationSource: 'governed_outbound',
    authorizedBy: 'operator', idempotencyKey: 'governed-item',
    authorizationSnapshot: {
      subject: 'Question', body: 'Hello', recipientEmail: 'founder@example.com',
      governed: { programId: 'program', envelopeId: 'envelope', itemId: 'item' },
    },
  });
  const calls = { release: 0, finish: 0, events: [] };
  const item = { id: 'item', status: 'attempted', attempted_at: '2026-10-06T13:59:59.000Z' };
  const governedStore = {
    items: async () => [item],
    releaseUnsent: async () => { calls.release += 1; item.status = 'pending'; item.attempted_at = null; },
    finish: async (_item, status) => { calls.finish += 1; item.status = status; },
    event: async (type, _key, payload) => calls.events.push({ type, payload }),
  };
  const governedSchedule = {
    validateGovernedSchedule: async () => ({}),
    finishGovernedSchedule: (s, result, opts) => governedScheduleBridge.finishGovernedSchedule(s, result, opts),
  };
  return { mailboxStore, scheduleStore, schedule, governedStore, governedSchedule, calls, item };
}

test('Emmett pre-provider capacity rejection makes zero provider calls and releases candidate eligibility', async () => {
  const fixture = await governedScheduleFixture();
  let providerCalls = 0;
  const result = await executeScheduledSend(fixture.schedule, {
    now: new Date('2026-10-06T14:00:00.000Z'),
    scheduleStore: fixture.scheduleStore,
    mailboxStore: fixture.mailboxStore,
    governedStore: fixture.governedStore,
    governedSchedule: fixture.governedSchedule,
    sendTenantEmail: async () => { providerCalls += 1; },
    emmettCapacity: {
      validateExecution: async () => ({ eligible: false, reason: 'emmett_capacity_exhausted' }),
      finalize: async () => {},
    },
  });
  assert.equal(result.result, 'skipped');
  assert.equal(providerCalls, 0);
  assert.equal(fixture.calls.release, 1);
  assert.equal(fixture.calls.finish, 0);
  assert.equal(fixture.item.status, 'pending');
  assert.equal(fixture.item.attempted_at, null);
});

test('genuine provider attempt remains terminally protected against duplicate sends', async () => {
  const fixture = await governedScheduleFixture();
  let providerCalls = 0;
  const error = Object.assign(new Error('provider outcome unknown'), {
    code: 'provider_or_persistence_error',
    providerBoundaryCrossed: true,
  });
  const result = await executeScheduledSend(fixture.schedule, {
    now: new Date('2026-10-06T14:00:00.000Z'),
    scheduleStore: fixture.scheduleStore,
    mailboxStore: fixture.mailboxStore,
    governedStore: fixture.governedStore,
    governedSchedule: fixture.governedSchedule,
    sendTenantEmail: async () => { providerCalls += 1; throw error; },
    emmettCapacity: {
      validateExecution: async () => ({ eligible: true }),
      markExecuting: async () => {}, finalize: async () => {}, ingestOutcome: async () => {},
    },
  });
  assert.equal(result.result, 'failed');
  assert.equal(providerCalls, 1);
  assert.equal(fixture.calls.release, 0);
  assert.equal(fixture.calls.finish, 1);
  assert.equal(fixture.item.status, 'uncertain');
});

test('existing suppressed capacity rejection is canonically PROVEN_UNSENT only with matching evidence', () => {
  const classification = classifyProvenUnsent({
    id: 'item', status: 'suppressed', reason: 'emmett_capacity_exhausted',
    provider_message_id: null,
  }, {
    schedules: [{ status: 'SKIPPED', skip_reason: 'emmett_capacity_exhausted', outbound_message_id: null }],
    mailboxMessages: [], executions: [{ status: 'attempted', provider_message_id: null }],
    events: [], emmettReservation: [], tickBlocks: [],
  });
  assert.equal(classification.outcome, 'PROVEN_UNSENT');
});
