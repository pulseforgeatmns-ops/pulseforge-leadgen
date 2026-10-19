'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  buildReplenishmentYield,
  computeCleanInventoryGrowth,
  classifyOwnershipRow,
  classifyInventoryOwnership,
  OWNERSHIP_KINDS,
  evaluateColdOutboundEligibility,
} = require('../services/outboundInventory');
const { _test: { persistDiscoveredCompanies } } = require('../services/maxOutboundControlLoop');
const { promoteRecord } = require('../scripts/promoteUnenriched');

test('recovery does not inflate clean inventory growth when canonical rows were already counted', () => {
  const growth = computeCleanInventoryGrowth(
    [{ prospectId: '1' }, { prospectId: '2' }, { prospectId: '3' }],
    [{ prospectId: '1' }, { prospectId: '2' }, { prospectId: '3' }],
  );
  assert.equal(growth.netCleanInventoryDelta, 0);
  assert.equal(growth.newCleanInventoryAdded, 0);
  const report = buildReplenishmentYield({
    admission: { evaluated: 14 },
    enrichment: { promoted: 0, recovered: 9 },
    recovered: 9,
    inventoryGrowth: growth,
  });
  assert.equal(report.recoveredExisting, 9);
  assert.equal(report.newCleanInventoryAdded, 0);
  assert.equal(report.cleanInventoryAdded, 0);
  assert.equal(report.rates.cleanInventoryYield, 0);
});

test('replenishment yield rates distinguish discovery vs verification loss', () => {
  const report = buildReplenishmentYield({
    admission: { discovered: 40, evaluated: 40, fit: 8, admittedToEnrichment: 6 },
    enrichment: { considered: 6, emailResolved: 3, emailVerified: 2, promoted: 2, recovered: 1 },
    inventoryGrowth: {
      preCycleCleanInventory: 3,
      postCycleCleanInventory: 5,
      newCleanInventoryAdded: 2,
      netCleanInventoryDelta: 2,
    },
  });
  assert.equal(report.newPromotions, 2);
  assert.equal(report.recoveredExisting, 1);
  assert.equal(report.newCleanInventoryAdded, 2);
  assert.equal(report.netCleanInventoryDelta, 2);
  assert.equal(report.cleanInventoryAdded, 2);
  assert.equal(report.rates.fitRate, 0.2);
  assert.equal(report.rates.enrichmentAdmissionRate, 0.75);
  assert.equal(report.rates.contactResolutionRate, 0.5);
  assert.equal(report.rates.verificationRate, 0.6667);
  assert.equal(report.rates.cleanInventoryYield, 0.05);
});

test('ownership classifier distinguishes stale, human-owned, and clear rows', () => {
  const now = Date.parse('2026-09-26T12:00:00Z');
  assert.equal(classifyOwnershipRow({ assigned_ao_id: 7 }, now).kind, OWNERSHIP_KINDS.VALID_COLLISION);
  assert.equal(classifyOwnershipRow({
    last_contacted_at: '2025-01-01T00:00:00Z',
    prior_touch: true,
  }, now).kind, OWNERSHIP_KINDS.STALE);
  assert.equal(classifyOwnershipRow({
    last_contacted_at: '2026-09-20T00:00:00Z',
    prior_touch: true,
  }, now).kind, OWNERSHIP_KINDS.VALID_COLLISION);
  assert.equal(classifyOwnershipRow({ email: 'ops@example.com' }, now).kind, OWNERSHIP_KINDS.CLEAR);
});

test('already usable canonical prospects are recovered instead of queued again', async () => {
  const pool = {
    query: async (sql, params) => {
      if (/FROM prospects p/i.test(sql)) {
        return {
          rows: [{
            id: 44,
            company_id: 9,
            email: 'ops@recovered.example', enrichment_provenance: { email: { source: 'website_email' } },
            email_verified: true,
            email_status: 'valid',
            do_not_contact: false,
            assigned_ao_id: null,
            closer_id: null,
            last_contacted_at: null,
            last_reply_at: null,
            name: 'Recovered STR',
            domain: 'recovered.example',
            website: 'https://recovered.example',
            has_ao_task: false,
            prior_touch: false,
          }],
        };
      }
      if (/INSERT INTO scout_unenriched/i.test(sql)) {
        throw new Error('should not insert a duplicate unenriched row');
      }
      return { rows: [], rowCount: 0 };
    },
  };
  const store = { pool, tenantId: '10', clientId: 10, candidateOwnership: async () => null };
  const persisted = await persistDiscoveredCompanies(pool, store, {
    tenantId: '10',
    companies: [{
      name: 'Recovered STR',
      description: 'Airbnb and vacation rental management',
      location: 'Manchester, NH',
      website: 'https://recovered.example',
      domain: 'recovered.example',
    }],
    scoutContext: {
      scope: { segment: 'short_term_rental' },
      serviceAreas: ['Manchester', 'Bedford', 'Goffstown', 'Hooksett', 'Londonderry', 'Auburn'],
      clientId: 10,
    },
  });
  assert.equal(persisted.inserted, 0);
  assert.equal(persisted.admission.recovered, 0);
  assert.equal(persisted.admission.alreadyUsable, 1);
  assert.equal(persisted.admission.admittedToEnrichment, 0);
});

function ownershipContact(overrides = {}) {
  return {
    id: 44, client_id: 13, company_id: 9, email: 'owner@fixture-business.example',
    email_verified: true, email_status: 'valid', do_not_contact: false,
    enrichment_provenance: { email: { source: 'website_email' } },
    acquisition_metadata: {}, contact_classification: null, is_synthetic: false,
    ao_outreach_review_required: false, assigned_ao_id: null, closer_id: null,
    last_contacted_at: null, last_reply_at: null, has_ao_task: false, prior_touch: false,
    name: 'Fixture Business', domain: 'fixture-business.example',
    website: 'https://fixture-business.example', ...overrides,
  };
}

function classifiedContact(classification, overrides = {}) {
  return ownershipContact({ acquisition_metadata: { contactResolution: {
    finalState: classification, bestEmail: 'owner@fixture-business.example',
  } }, ...overrides });
}

function ownershipFixture(rows, tenantId = '13') {
  const queries = [];
  const pool = { query: async (sql, params) => {
    assert.match(sql, /^\s*SELECT/, 'ownership classification is read-only');
    assert.match(sql, /SELECT p\.id, p\.company_id, p\.client_id,/);
    assert.match(sql, /p\.acquisition_metadata/);
    assert.match(sql, /to_jsonb\(p\)->>'contact_classification' AS contact_classification/);
    assert.match(sql, /AS is_synthetic/);
    assert.equal(params[3], Number(tenantId));
    queries.push({ sql, params });
    return { rows };
  } };
  const store = { pool, tenantId, clientId: Number(tenantId),
    candidateOwnership: async () => null, suppression: async () => null };
  return { pool, store, queries };
}

test('ownership uses the actual tenant and persisted contact evidence', async t => {
  const cases = [
    ['Babrun missing classification', ownershipContact(), false],
    ['Babrun role email is not authorized by default', classifiedContact('VERIFIED_ROLE_EMAIL'), false],
    ['Babrun verified founder', classifiedContact('VERIFIED_FOUNDER_EMAIL'), true],
    ['Babrun changed classified email', ownershipContact({ acquisition_metadata: { contactResolution: {
      finalState: 'VERIFIED_FOUNDER_EMAIL', bestEmail: 'different@fixture-business.example',
    } } }), false],
    ['Babrun missing classified address', ownershipContact({ contact_classification: 'VERIFIED_FOUNDER_EMAIL' }), false],
    ['Babrun synthetic founder', classifiedContact('VERIFIED_FOUNDER_EMAIL', { is_synthetic: true }), false],
    ['Babrun DNC founder', classifiedContact('VERIFIED_FOUNDER_EMAIL', { do_not_contact: true }), false],
    ['Babrun unverified founder', classifiedContact('VERIFIED_FOUNDER_EMAIL', { email_verified: false }), false],
    ['Babrun review-held founder', classifiedContact('VERIFIED_FOUNDER_EMAIL', { ao_outreach_review_required: true }), false],
    ['Anchor observed verified contact without classification', ownershipContact({ client_id: 10 }), true],
    ['Anchor verified role', classifiedContact('VERIFIED_ROLE_EMAIL', { client_id: 10 }), true],
    ['Pulseforge missing classification', ownershipContact({ client_id: 1 }), false],
    ['Substral missing classification', ownershipContact({ client_id: 17 }), false],
  ];
  for (const [name, row, usable] of cases) {
    await t.test(name, async () => {
      const { store, pool } = ownershipFixture([row], String(row.client_id));
      for (const email of [undefined, row.email]) {
        const result = await classifyInventoryOwnership(store, { company: row.name, domain: row.domain, email },
          { pool, clientId: row.client_id });
        assert.equal(result.kind === OWNERSHIP_KINDS.ALREADY_USABLE_CANONICAL, usable);
        if (!usable && !email) assert.ok([OWNERSHIP_KINDS.CONTACT_POLICY_HOLD,
          OWNERSHIP_KINDS.SAME_COMPANY_DIFFERENT_CONTACT].includes(result.kind));
      }
    });
  }
});

test('ownership honors caller policy without falling back to tenant 10', async () => {
  const row = classifiedContact('VERIFIED_ROLE_EMAIL');
  const { store, pool } = ownershipFixture([row]);
  const candidate = { company: row.name, domain: row.domain };
  const allowed = await classifyInventoryOwnership(store, candidate, { pool, clientId: 13,
    policy: { tenantId: '13', allowedContactClassifications: ['VERIFIED_ROLE_EMAIL'] } });
  assert.equal(allowed.kind, OWNERSHIP_KINDS.ALREADY_USABLE_CANONICAL);
  const denied = await classifyInventoryOwnership(store, candidate, { pool, clientId: 13,
    policy: { allowedContactClassifications: [] } });
  assert.equal(denied.kind, OWNERSHIP_KINDS.CONTACT_POLICY_HOLD);
  // Even incomplete test/adaptor projections cannot select Anchor's default rules.
  const incomplete = ownershipFixture([ownershipContact({ client_id: undefined })]);
  const missing = await classifyInventoryOwnership(incomplete.store, candidate, { clientId: 13 });
  assert.equal(missing.kind, OWNERSHIP_KINDS.CONTACT_POLICY_HOLD);
});

test('ownership fails closed on mismatched bindings or returned tenants', async t => {
  for (const mismatch of ['policy', 'store', 'context', 'row']) {
    await t.test(mismatch, async () => {
      const f = ownershipFixture([classifiedContact('VERIFIED_FOUNDER_EMAIL',
        mismatch === 'row' ? { client_id: 10 } : {})]);
      if (mismatch === 'store') f.store.tenantId = '10';
      if (mismatch === 'context') f.store.ctx = { tenantId: '10' };
      f.store.candidateOwnership = async () => { throw Error('must reject before ownership checks'); };
      f.store.suppression = async () => { throw Error('must reject before suppression checks'); };
      await assert.rejects(classifyInventoryOwnership(f.store, { company: 'Fixture Business' }, {
        clientId: 13, policy: { tenantId: mismatch === 'policy' ? '10' : '13' },
      }), { code: 'governed_outbound_tenant_mismatch' });
      assert.equal(f.queries.length, mismatch === 'row' ? 1 : 0);
    });
  }
});

test('ownership and suppression still take precedence over a valid Babrun founder', async () => {
  for (const blocker of ['candidateOwnership', 'suppression']) {
    const f = ownershipFixture([classifiedContact('VERIFIED_FOUNDER_EMAIL')]);
    f.store[blocker] = async () => 'prior_contact_or_human_owned';
    const result = await classifyInventoryOwnership(f.store, { company: 'Fixture Business' }, { clientId: 13 });
    assert.equal(result.kind, OWNERSHIP_KINDS.VALID_COLLISION);
    assert.equal(result.reason, 'prior_contact_or_human_owned');
  }
});

test('Babrun persistence defers corrected contact-policy holds and forwards the program policy', async () => {
  const company = { name: 'Fixture Business', domain: 'fixture-business.example',
    website: 'https://fixture-business.example', description: 'We are a cleaning company', location: 'Manchester, NH' };
  for (const classification of [null, 'VERIFIED_ROLE_EMAIL']) {
    const f = ownershipFixture([classification ? classifiedContact(classification) : ownershipContact()]);
    f.store.one = async () => null;
    f.store.event = async () => { throw Error('unsupported recovery must not record a provider attempt'); };
    const receipt = await persistDiscoveredCompanies(f.pool, f.store, {
      tenantId: '13', companies: [company], scoutContext: {
        scope: { segment: 'small_business_owner' }, allowedCities: ['Manchester'],
        program: { tenant_id: '13', policy: { tenantId: '13', allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL'] } },
      },
    });
    assert.equal(receipt.inserted, 0);
    assert.equal(receipt.admission.alreadyUsable || 0, 0);
    assert.equal(receipt.admission.rejected.same_company_different_contact, 1);
    assert.equal(receipt.admission.contactPolicyDeferred, 1);
    assert.equal(receipt.admission.sameCompanyCandidatesAttempted, 0);
    assert.equal(receipt.checkpointComplete, false);
    assert.equal(f.queries.length, 1, 'no recovery SQL or queue writes');
  }
  const f = ownershipFixture([classifiedContact('VERIFIED_ROLE_EMAIL')]);
  const receipt = await persistDiscoveredCompanies(f.pool, f.store, {
    tenantId: '13', companies: [company], scoutContext: {
      program: { tenant_id: '13', policy: { tenantId: '13', allowedContactClassifications: ['VERIFIED_ROLE_EMAIL'] } },
    },
  });
  assert.equal(receipt.admission.alreadyUsable, 1, 'persistence passes the explicit existing program policy');
  assert.equal(receipt.checkpointComplete, true);
});

test('newly exposed Anchor contact evidence defers without recovery, providers or queue writes', async () => {
  const bad = classifiedContact('VERIFIED_FOUNDER_EMAIL', { client_id: 10,
    email: 'changed@fixture-business.example' });
  const recoveryEligible = ownershipContact({ id: 45, client_id: 10, email_verified: false });
  for (const rows of [[bad], [recoveryEligible, bad], [bad, recoveryEligible]]) {
    const f = ownershipFixture(rows, '10');
    const ownership = await classifyInventoryOwnership(f.store, { company: bad.name, domain: bad.domain }, { clientId: 10 });
    assert.equal(ownership.kind, OWNERSHIP_KINDS.CONTACT_POLICY_HOLD);
    assert.equal(ownership.contactReason, 'contact_classification_email_changed');
    assert.equal(ownership.recoverable, false);
    f.queries.length = 0;
    f.store.one = async () => { throw Error('policy hold must not enter recovery cooldown lookup'); };
    f.store.event = async () => { throw Error('policy hold must not record recovery attempts'); };
    const receipt = await persistDiscoveredCompanies(f.pool, f.store, {
      tenantId: '10', companies: [{ name: bad.name, domain: bad.domain, website: bad.website,
        location: 'Manchester, NH' }],
      scoutContext: { scope: { segment: 'property_manager' }, allowedCities: ['Manchester'],
        investigateBusinessEvidence: true, recoverySources: Object.fromEntries(
          ['pfIntelligence', 'website', 'prospeo', 'hunter'].map(name => [name, async () => {
            throw Error(`Policy hold must not call ${name}`);
          }])) },
    });
    assert.equal(receipt.inserted, 0);
    assert.equal(receipt.admission.alreadyUsable || 0, 0);
    assert.equal(receipt.admission.contactPolicyDeferred, 1);
    assert.equal(receipt.admission.sameCompanyCandidatesAttempted, 0);
    assert.equal(receipt.admission.websiteInvestigations, 0);
    assert.equal(receipt.checkpointComplete, false);
    assert.equal(f.queries.length, 1, 'only ownership read; no recovery or queue queries');
  }
  // A genuinely usable current contact still wins. An already-unverified contact
  // still follows the pre-existing same-company route, covered by recovery tests.
  const usable = ownershipFixture([bad, ownershipContact({ client_id: 10, id: 46 })], '10');
  assert.equal((await classifyInventoryOwnership(usable.store, { company: bad.name }, { clientId: 10 })).kind,
    OWNERSHIP_KINDS.ALREADY_USABLE_CANONICAL);
  const existing = ownershipFixture([recoveryEligible], '10');
  assert.equal((await classifyInventoryOwnership(existing.store, { company: bad.name }, { clientId: 10 })).kind,
    OWNERSHIP_KINDS.SAME_COMPANY_DIFFERENT_CONTACT);
});

test('promoteRecord recovers an existing same-tenant verified prospect without duplicating', async () => {
  const queries = [];
  const db = {
    query: async (sql, params) => {
      queries.push(sql);
      if (/to_regclass/.test(sql)) return { rows: [{ control_table: null }] };
      if (/ALTER TABLE companies/i.test(sql)) return { rows: [], rowCount: 0 };
      if (/SELECT id, domain, website FROM companies/i.test(sql)) return { rows: [{ id: 3 }], rowCount: 1 };
      if (/INSERT INTO prospects/i.test(sql)) return { rows: [], rowCount: 0 };
      if (/FROM prospects/.test(sql) && /lower\(email\)/i.test(sql)) {
        return {
          rows: [{
            id: 88, company_id: 3,
            client_id: 10,
            do_not_contact: false,
            email_verified: true,
            email_status: 'valid',
          }],
        };
      }
      if (/UPDATE prospects/i.test(sql)) {
        const provenance = JSON.parse(params[9]);
        assert.equal(provenance.email.source, 'website_email');
        assert.equal(provenance.email.verifier, 'bouncer');
      }
      if (/DELETE FROM scout_unenriched/i.test(sql)) return { rowCount: 1, rows: [] };
      return { rows: [], rowCount: 0 };
    },
  };
  const result = await promoteRecord({
    id: 'unenriched-9',
    client_id: 10,
    company: 'Existing STR',
    domain: 'existing-str.example',
    website_url: 'https://existing-str.example',
    location: 'Manchester, NH',
    vertical: 'str_manager',
  }, {
    db,
    enrich: async () => ({ email: 'ops@existing-str.example', contact: 'Owner', source: ['website_email'] }),
    verify: async () => ({
      emailVerified: true,
      emailVerificationMethod: 'bouncer',
      verifiedAt: '2026-09-26T00:00:00.000Z',
      doNotContact: false,
      emailStatus: 'valid',
      verifierResponse: { status: 'valid' },
      verifierCheckedAt: '2026-09-26T00:00:00.000Z',
      reject: false,
    }),
    loadClientConfig: async () => ({
      id: 10,
      service_area: ['Manchester', 'Bedford', 'Goffstown', 'Hooksett', 'Londonderry', 'Auburn'],
    }),
  });
  assert.equal(result.recovered, true);
  assert.equal(result.promoted, false);
  assert.equal(result.prospectId, 88);
  assert.ok(queries.some(sql => /DELETE FROM scout_unenriched/i.test(sql)));
  assert.equal(queries.filter(sql => /INSERT INTO prospects/i.test(sql)).length, 1);
});

test('classifyInventoryOwnership reports same-company different-contact without treating it as AO ownership', async () => {
  const pool = {
    query: async () => ({
      rows: [{
        id: 12,
        company_id: 3,
        email: 'other@pm.example',
        email_verified: false,
        email_status: 'unknown',
        do_not_contact: false,
        assigned_ao_id: null,
        closer_id: null,
        last_contacted_at: null,
        last_reply_at: null,
        name: 'Granite PM',
        domain: 'pm.example',
        has_ao_task: false,
        prior_touch: false,
      }],
    }),
  };
  const result = await classifyInventoryOwnership({ pool, tenantId: '10', clientId: 10 }, {
    company: 'Granite PM',
    domain: 'pm.example',
    email: 'ops@pm.example',
  });
  assert.equal(result.kind, OWNERSHIP_KINDS.SAME_COMPANY_DIFFERENT_CONTACT);
  assert.equal(result.recoverable, true);
  assert.equal(result.companyId, '3');
});

test('unknown buyer readiness stays eligible when fail-closed gates are clear', () => {
  const result = evaluateColdOutboundEligibility({
    businessFit: 'qualified',
    geography: 'in_scope',
    contactVerified: true,
    buyerReadiness: 'unknown',
  });
  assert.equal(result.eligible, true);
});

// A later cycle consumes an existing queue; newly queued rows are a different cohort.
test('contact resolution uses processed queue entries, including backlog-only cycles', () => {
  for (const newlyQueued of [7, 0]) {
    const report = buildReplenishmentYield({
      admission: { admittedToEnrichment: newlyQueued },
      enrichment: { considered: 20, emailResolved: 16, emailVerified: 13, promoted: 11 },
    });
    assert.equal(report.enrichmentConsidered, 20);
    assert.equal(report.rates.contactResolutionRate, 0.8);
  }
  const unknown = buildReplenishmentYield({ enrichment: { emailResolved: 3 } });
  assert.equal(unknown.enrichmentConsidered, null);
  assert.equal(unknown.rates.contactResolutionRate, null);
});
