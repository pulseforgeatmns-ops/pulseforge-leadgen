'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  buildReplenishmentYield,
  classifyOwnershipRow,
  classifyInventoryOwnership,
  OWNERSHIP_KINDS,
  evaluateColdOutboundEligibility,
} = require('../services/outboundInventory');
const { _test: { persistDiscoveredCompanies } } = require('../services/maxOutboundControlLoop');
const { promoteRecord } = require('../scripts/promoteUnenriched');

test('replenishment yield rates distinguish discovery vs verification loss', () => {
  const report = buildReplenishmentYield({
    admission: { discovered: 40, evaluated: 40, fit: 8, admittedToEnrichment: 6 },
    enrichment: { emailResolved: 3, emailVerified: 2, promoted: 2, recovered: 1 },
  });
  assert.equal(report.cleanInventoryAdded, 3);
  assert.equal(report.rates.fitRate, 0.2);
  assert.equal(report.rates.enrichmentAdmissionRate, 0.75);
  assert.equal(report.rates.contactResolutionRate, 0.5);
  assert.equal(report.rates.verificationRate, 0.6667);
  assert.equal(report.rates.cleanInventoryYield, 0.075);
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
            email: 'ops@recovered.example',
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
  const store = { pool, candidateOwnership: async () => null };
  const persisted = await persistDiscoveredCompanies(pool, store, {
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
    },
  });
  assert.equal(persisted.inserted, 0);
  assert.equal(persisted.admission.recovered, 1);
  assert.equal(persisted.admission.admittedToEnrichment, 0);
});

test('promoteRecord recovers an existing same-tenant verified prospect without duplicating', async () => {
  const queries = [];
  const db = {
    query: async (sql, params) => {
      queries.push(sql);
      if (/ALTER TABLE companies/i.test(sql)) return { rows: [], rowCount: 0 };
      if (/SELECT id FROM companies/i.test(sql)) return { rows: [{ id: 3 }], rowCount: 1 };
      if (/INSERT INTO prospects/i.test(sql)) return { rows: [], rowCount: 0 };
      if (/FROM prospects/.test(sql) && /lower\(email\)/i.test(sql)) {
        return {
          rows: [{
            id: 88,
            client_id: 10,
            do_not_contact: false,
            email_verified: true,
            email_status: 'valid',
          }],
        };
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
  const result = await classifyInventoryOwnership({ pool }, {
    company: 'Granite PM',
    domain: 'pm.example',
    email: 'ops@pm.example',
  });
  assert.equal(result.kind, OWNERSHIP_KINDS.SAME_COMPANY_DIFFERENT_CONTACT);
  assert.equal(result.recoverable, false);
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
