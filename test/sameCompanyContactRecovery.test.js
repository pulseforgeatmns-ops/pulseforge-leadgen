'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  attemptSameCompanyAlternateRecovery,
  emptyAlternateContactTelemetry,
  mergeAlternateTelemetry,
  clampCohortCounters,
} = require('../services/sameCompanyContactRecovery');

function companyPool(existingEmail = 'blocked@pm.example') {
  return {
    query: async (sql, params) => {
      if (/FROM prospects p/i.test(sql) && /JOIN companies c/i.test(sql)) {
        return {
          rows: [{
            id: 12,
            company_id: 3,
            email: existingEmail,
            email_verified: false,
            email_status: 'unknown',
            do_not_contact: false,
            assigned_ao_id: null,
            closer_id: null,
            last_contacted_at: null,
            last_reply_at: null,
            vertical: 'str_manager',
            service_area_match: true,
            first_name: 'Old',
            last_name: 'Contact',
            job_title: 'Owner',
            name: 'Granite PM',
            domain: 'pm.example',
            has_ao_task: false,
            prior_touch: false,
          }],
        };
      }
      if (/acquisition_outbound_items/i.test(sql)) return { rows: [] };
      if (/INSERT INTO prospects/i.test(sql)) {
        const provenance = JSON.parse(params[16]);
        assert.equal(provenance.email.source, 'prospeo');
        assert.equal(provenance.email.verifier, 'bouncer');
        return { rows: [{ id: 901 }] };
      }
      if (/UPDATE prospects/i.test(sql)) return { rows: [], rowCount: 1 };
      return { rows: [], rowCount: 0 };
    },
  };
}

const clearStore = {
  tenantId: '10',
  clientId: 10,
  candidateOwnership: async () => null,
  suppression: async () => null,
};

const verified = async () => ({
  emailVerified: true,
  emailVerificationMethod: 'bouncer',
  verifiedAt: '2026-09-26T00:00:00.000Z',
  doNotContact: false,
  emailStatus: 'valid',
  verifierResponse: { status: 'valid' },
  verifierCheckedAt: '2026-09-26T00:00:00.000Z',
  reject: false,
});

test('fresh provider evidence repairs the existing email without duplicating its company or contact', async () => {
  const pool = companyPool('owner@pm.example');
  const original = pool.query;
  let updates = 0;
  pool.query = async (sql, args) => {
    assert.doesNotMatch(sql, /INSERT INTO (companies|prospects)/i);
    if (/UPDATE prospects/i.test(sql)) {
      updates++;
      assert.equal(args[0], 12);
      assert.equal(String(args[10]), '3');
      assert.equal(JSON.parse(args[9]).email.source, 'prospeo');
      assert.equal(args[12], 'property_manager');
      assert.equal(JSON.parse(args[9]).business.source_url, 'https://pm.example/about');
    }
    return original(sql, args);
  };
  const result = await attemptSameCompanyAlternateRecovery({ ...clearStore, pool }, pool, {
    company: { name:'Granite PM', domain:'pm.example', website:'https://pm.example' },
    scoutContext: { admittedVertical: 'property_manager', businessEvidence: { source_url: 'https://pm.example/about', quote: 'We provide property management.' } },
    enrich: async () => [{ email:'owner@pm.example', source:['prospeo'] }], verify:verified,
  });
  assert.equal(result.ok, true);
  assert.equal(result.prospectId, '12');
  assert.equal(updates, 1);
});

test('same-company recovery admits a verified alternate under the existing company', async () => {
  const pool = companyPool();
  const store = { ...clearStore, pool };
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
    verify: verified,
  });

  assert.equal(result.ok, true);
  assert.equal(result.reason, 'alternate_contact_resolved');
  assert.equal(result.email, 'ops@pm.example');
  assert.equal(result.telemetry.alternateContactsAddedToCleanInventory, 1);
  assert.equal(result.telemetry.sameCompanyCandidatesAttempted, 1);
  assert.equal(result.telemetry.alternateContactsResolved, 1);
  assert.equal(result.telemetry.alternateContactsVerified, 1);
});

test('same-company recovery emits already-attempted instead of collapsing to not-found', async () => {
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
  const store = { ...clearStore, pool };
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
  assert.equal(result.reason, 'alternate_contact_already_attempted');
  assert.equal(result.telemetry.alternateContactLossReasons.alternate_contact_already_attempted, 1);
  assert.equal(result.telemetry.alternateContactLossReasons.no_alternate_contact_found, 0);
});

test('same-company recovery prefers website/owner contacts and records unverified separately', async () => {
  const pool = companyPool();
  const store = { ...clearStore, pool };
  const result = await attemptSameCompanyAlternateRecovery(store, pool, {
    company: { name: 'Granite PM', domain: 'pm.example', website: 'https://pm.example' },
    telemetry: emptyAlternateContactTelemetry(),
    enrich: async () => [],
    sources: {
      website: async () => ({
        status: 'ok',
        contacts: [
          { email: 'info@pm.example', title: 'office', source: ['website'] },
          { email: 'owner@pm.example', title: 'Owner', source: ['website'] },
        ],
      }),
      hunter: async () => ({ status: 'ok', contacts: [] }),
      pfIntelligence: async () => ({ status: 'ok', contacts: [] }),
    },
    verify: async (email) => ({
      emailVerified: false,
      emailStatus: 'unknown',
      reject: false,
      doNotContact: false,
    }),
  });
  assert.equal(result.ok, false);
  assert.equal(result.reason, 'alternate_email_unverified');
  assert.equal(result.telemetry.alternateContactsResolved, 2);
  assert.equal(result.telemetry.alternateContactLossReasons.alternate_email_unverified, 1);
});

test('same-company recovery emits dnc and owned as distinct terminal reasons', async () => {
  const pool = companyPool();
  const dnc = await attemptSameCompanyAlternateRecovery({
    tenantId: '10', clientId: 10,
    candidateOwnership: async () => null,
    suppression: async () => null,
  }, pool, {
    company: { name: 'Granite PM', domain: 'pm.example', website: 'https://pm.example', email: 'ops@pm.example' },
    telemetry: emptyAlternateContactTelemetry(),
    enrich: async () => [{ email: 'ops@pm.example', contact: 'Ops', source: ['prospeo'] }],
    verify: async () => ({
      emailVerified: true,
      emailStatus: 'valid',
      doNotContact: true,
      reject: false,
    }),
  });
  assert.equal(dnc.reason, 'alternate_contact_dnc');

  const owned = await attemptSameCompanyAlternateRecovery({
    tenantId: '10', clientId: 10,
    candidateOwnership: async () => 'ao_owned',
    suppression: async () => null,
  }, pool, {
    company: { name: 'Granite PM', domain: 'pm.example', website: 'https://pm.example', email: 'ops@pm.example' },
    telemetry: emptyAlternateContactTelemetry(),
    enrich: async () => [{ email: 'ops@pm.example', contact: 'Ops', source: ['prospeo'] }],
    verify: verified,
  });
  assert.equal(owned.reason, 'alternate_contact_owned');
});

test('same-company recovery reports provider_unavailable when no source can run', async () => {
  const pool = companyPool();

  const store = { ...clearStore, pool };
  const result = await attemptSameCompanyAlternateRecovery(store, pool, {
    company: { name: 'Granite PM', domain: 'pm.example', website: 'https://pm.example' },
    telemetry: emptyAlternateContactTelemetry(),
    sources: {
      pfIntelligence: async () => ({ status: 'unavailable', contacts: [] }),
      website: async () => ({ status: 'unavailable', contacts: [] }),
      prospeo: async () => ({ status: 'unavailable', contacts: [] }),
      hunter: async () => ({ status: 'unavailable', contacts: [] }),
    },
    verify: verified,
  });
  assert.equal(result.ok, false);
  assert.equal(result.reason, 'provider_unavailable');
  assert.equal(result.telemetry.alternateContactLossReasons.provider_unavailable, 1);
});

test('mergeAlternateTelemetry does not double-count the same object (131070 guard)', () => {
  const telemetry = emptyAlternateContactTelemetry();
  telemetry.sameCompanyCandidatesAttempted = 16;
  mergeAlternateTelemetry(telemetry, telemetry);
  assert.equal(telemetry.sameCompanyCandidatesAttempted, 16);
  const admission = clampCohortCounters({
    evaluated: 16,
    sameCompanyCandidatesAttempted: 131070,
    rejected: { same_company_different_contact: 131070 },
  });
  assert.equal(admission.sameCompanyCandidatesAttempted, 16);
  assert.equal(admission.rejected.same_company_different_contact, 16);
});

test('persistDiscoveredCompanies attempts every same-company candidate without exploding counters', async () => {
  const { _test: { persistDiscoveredCompanies } } = require('../services/maxOutboundControlLoop');
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
      if (/acquisition_outbound_items/i.test(sql)) return { rows: [] };
      if (/INSERT INTO prospects/i.test(sql)) return { rows: [{ id: 901 }] };
      return { rows: [], rowCount: 0 };
    },
  };
  const store = {
    tenantId: '10', clientId: 10, pool,
    candidateOwnership: async () => null,
    suppression: async () => null,
  };
  const companies = Array.from({ length: 16 }, (_, i) => ({
    name: `Granite PM ${i}`,
    industry: 'property_manager',
    domain: `pm${i}.example`,
    website: `https://pm${i}.example`,
  }));
  const persisted = await persistDiscoveredCompanies(pool, store, {
    tenantId: '10',
    companies,
    scoutContext: {
      scope: { segment: 'short_term_rental' },
      clientId: 10,
      recoverySources: {
        pfIntelligence: async () => ({ status: 'ok', contacts: [] }),
        website: async () => ({ status: 'ok', contacts: [] }),
        prospeo: async () => ({ status: 'ok', contacts: [] }),
        hunter: async () => ({ status: 'ok', contacts: [] }),
      },
    },
  });
  assert.equal(persisted.admission.evaluated, 16);
  assert.equal(persisted.admission.sameCompanyCandidatesAttempted, 16);
  assert.equal(persisted.admission.rejected.same_company_different_contact, 16);
  assert.equal(persisted.admission.alternateContactLossReasons.no_alternate_contact_found, 16);
  assert.notEqual(persisted.admission.sameCompanyCandidatesAttempted, 131070);
});
