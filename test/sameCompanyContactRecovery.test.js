'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  attemptSameCompanyAlternateRecovery,
  emptyAlternateContactTelemetry,
} = require('../services/sameCompanyContactRecovery');
const { OWNERSHIP_KINDS } = require('../services/outboundInventory');

test('same-company recovery admits a verified alternate under the existing company', async () => {
  let insertedEmail = null;
  const pool = {
    query: async (sql, params) => {
      if (/FROM prospects p/i.test(sql) && /JOIN companies c/i.test(sql)) {
        return {
          rows: [{
            id: 12,
            company_id: 3,
            email: 'blocked@pm.example',
            email_verified: false,
            email_status: 'unknown',
            do_not_contact: false,
            assigned_ao_id: null,
            closer_id: null,
            last_contacted_at: null,
            last_reply_at: null,
            vertical: 'str_manager',
            service_area_match: true,
            name: 'Granite PM',
            domain: 'pm.example',
            has_ao_task: false,
            prior_touch: false,
          }],
        };
      }
      if (/acquisition_outbound_items/i.test(sql)) return { rows: [] };
      if (/INSERT INTO prospects/i.test(sql)) {
        insertedEmail = params[1];
        return { rows: [{ id: 901 }] };
      }
      return { rows: [], rowCount: 0 };
    },
  };
  const store = {
    pool,
    candidateOwnership: async () => null,
    suppression: async () => null,
  };

  const result = await attemptSameCompanyAlternateRecovery(store, pool, {
    company: {
      name: 'Granite PM',
      domain: 'pm.example',
      website: 'https://pm.example',
      email: 'ops@pm.example',
      vertical: 'str_manager',
    },
    telemetry: emptyAlternateContactTelemetry(),
    enrich: async () => [{ email: 'ops@pm.example', contact: 'Ops Lead', source: ['prospeo'] }],
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
  });

  assert.equal(result.ok, true);
  assert.equal(insertedEmail, 'ops@pm.example');
  assert.equal(result.telemetry.alternateContactsAddedToCleanInventory, 1);
  assert.equal(result.telemetry.sameCompanyCandidatesAttempted, 1);
});

test('same-company recovery rejects already-attempted alternates', async () => {
  const pool = {
    query: async (sql) => {
      if (/FROM prospects p/i.test(sql)) {
        return {
          rows: [{
            id: 12,
            company_id: 3,
            email: 'old@pm.example',
            email_verified: false,
            email_status: 'unknown',
            do_not_contact: false,
            assigned_ao_id: null,
            closer_id: null,
            last_contacted_at: null,
            last_reply_at: null,
            vertical: 'str_manager',
            service_area_match: true,
            name: 'Granite PM',
            domain: 'pm.example',
            has_ao_task: false,
            prior_touch: false,
          }],
        };
      }
      if (/acquisition_outbound_items/i.test(sql)) {
        return { rows: [{ email: 'ops@pm.example' }] };
      }
      return { rows: [], rowCount: 0 };
    },
  };
  const store = {
    pool,
    candidateOwnership: async () => null,
    suppression: async () => null,
  };

  const result = await attemptSameCompanyAlternateRecovery(store, pool, {
    company: {
      name: 'Granite PM',
      domain: 'pm.example',
      website: 'https://pm.example',
      email: 'ops@pm.example',
    },
    telemetry: emptyAlternateContactTelemetry(),
    enrich: async () => [],
    verify: async () => ({ emailVerified: true, reject: false }),
  });

  assert.equal(result.ok, false);
  assert.equal(result.telemetry.alternateContactLossReasons.no_alternate_contact_found, 1);
});
