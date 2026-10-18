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

const recoveryTenantBindings = [
  ['tenantId'],
  ['scoutContext', 'authorizedTenantId'],
  ['scoutContext', 'tenantId'],
  ['scoutContext', 'clientId'],
  ['scoutContext', 'governedContext', 'tenantId'],
  ['scoutContext', 'governedContext', 'clientId'],
  ['scoutContext', 'governedContext', 'transport', 'tenantId'],
  ['scoutContext', 'governedContext', 'transport', 'clientId'],
  ['scoutContext', 'program', 'tenant_id'],
  ['scoutContext', 'program', 'tenantId'],
  ['scoutContext', 'program', 'policy', 'tenantId'],
  ['store', 'tenantId'],
  ['store', 'clientId'],
  ['store', 'ctx', 'tenantId'],
  ['store', 'ctx', 'clientId'],
];

function setTenantBinding(input, path, value) {
  let target = input;
  for (const key of path.slice(0, -1)) target = target[key] ||= {};
  target[path.at(-1)] = value;
  return input;
}

test('same-company recovery fails closed before queries, store checks, or providers for unsafe tenant bindings', async (t) => {
  const cases = [
    { name: 'missing identity', input: {}, reason: 'recovery_tenant_required' },
    { name: 'null identities', input: { tenantId: null, scoutContext: { clientId: null } }, reason: 'recovery_tenant_required' },
    ...[
      ['empty string', ''],
      ['blank string', ' '],
      ['array', ['10']],
      ['boolean', true],
      ['coercible object', { toString() { return '10'; } }],
      ['object', { toString() { throw new Error('Identity object must not be coerced'); } }],
    ].map(([name, tenantId]) => ({
      name: `malformed ${name} with tenant-10 store`,
      input: { tenantId, store: { tenantId: '10', clientId: 10 } },
      reason: 'recovery_tenant_unsupported',
    })),
    ...['1', '13', '17', 'invalid'].map(tenantId => ({
      name: `non-Anchor tenant ${tenantId}`,
      input: { tenantId, store: { tenantId }, scoutContext: { authorizedTenantId: tenantId } },
      reason: 'recovery_tenant_unsupported',
    })),
    ...recoveryTenantBindings.map(path => ({
      name: `conflicting ${path.join('.')}`,
      input: setTenantBinding({ tenantId: '10', store: { tenantId: '10', clientId: 10 } }, path, '13'),
      reason: 'recovery_tenant_mismatch',
    })),
  ];
  for (const scenario of cases) {
    for (const ownership of [null, { kind: 'same_company_different_contact', companyId: '3' }]) {
      await t.test(`${scenario.name}; ownership ${ownership ? 'supplied' : 'unresolved'}`, async () => {
        const calls = [];
        const unexpected = name => async () => {
          calls.push(name);
          throw new Error(`Unexpected ${name} for a blocked tenant`);
        };
        const pool = { query: unexpected('query') };
        const result = await attemptSameCompanyAlternateRecovery({
          ...scenario.input.store,
          pool,
          candidateOwnership: unexpected('candidateOwnership'),
          suppression: unexpected('suppression'),
        }, pool, {
          ...scenario.input,
          ownership,
          company: { name: 'Granite PM', domain: 'pm.example', email: 'owner@pm.example' },
          verify: unexpected('verify'),
          enrich: unexpected('enrich'),
          sources: Object.fromEntries(['pfIntelligence', 'website', 'prospeo', 'hunter']
            .map(name => [name, unexpected(name)])),
        });
        assert.equal(result.ok, false);
        assert.equal(result.reason, scenario.reason);
        assert.deepEqual(calls, []);
        assert.equal(result.telemetry.sameCompanyCandidatesAttempted, 0);
        assert.equal(result.telemetry.alternateContactsRejected, 1);
        assert.equal(result.telemetry.alternateContactLossReasons[scenario.reason], 1);
        assert.deepEqual(result.telemetry.terminalReasons, [scenario.reason]);
        const merged = mergeAlternateTelemetry(emptyAlternateContactTelemetry(), result.telemetry);
        assert.equal(merged.alternateContactLossReasons[scenario.reason], 1);
      });
    }
  }
});

test('same-company recovery accepts tenant 10 with optional bindings absent', async (t) => {
  for (const path of recoveryTenantBindings) {
    await t.test(path.join('.'), async () => {
      const input = setTenantBinding({}, path, path.at(-1) === 'clientId' ? 10 : '10');
      const pool = companyPool();
      const result = await attemptSameCompanyAlternateRecovery({
        ...input.store,
        pool,
        candidateOwnership: async () => null,
        suppression: async () => null,
      }, pool, {
        ...input,
        company: { name: 'Granite PM', domain: 'pm.example', website: 'https://pm.example' },
        enrich: async () => [{ email: 'owner@pm.example', source: ['prospeo'] }],
        verify: verified,
      });
      assert.equal(result.ok, true);
      assert.equal(result.reason, 'alternate_contact_resolved');
      assert.equal(result.prospectId, '901');
    });
  }
});

test('same-company recovery preserves the complete production Anchor tenant context', async () => {
  const { createGovernedOutboundContext } = require('../services/governedOutboundContext');
  const { GovernedOutboundStore } = require('../services/governedOutboundStore');
  const pool = companyPool();
  const store = new GovernedOutboundStore(pool, '10');
  store.candidateOwnership = async () => null;
  store.suppression = async () => null;
  const program = { id: 'anchor-program', tenant_id: '10', tenantId: null, policy: { tenantId: '10' } };
  const result = await attemptSameCompanyAlternateRecovery(store, pool, {
    tenantId: '10',
    company: { name: 'Granite PM', domain: 'pm.example', website: 'https://pm.example' },
    ownership: { kind: 'same_company_different_contact', companyId: '3' },
    scoutContext: {
      authorizedTenantId: '10', clientId: 10, tenantId: null,
      governedContext: createGovernedOutboundContext({ program }),
      program,
    },
    enrich: async () => [{ email: 'owner@pm.example', source: ['prospeo'] }],
    verify: verified,
  });
  assert.equal(result.ok, true);
  assert.equal(result.prospectId, '901');
  assert.equal(result.telemetry.alternateContactsAddedToCleanInventory, 1);
});

test('persistDiscoveredCompanies passes its resolved tenant to the Anchor-only recovery guard', async (t) => {
  const { _test: { persistDiscoveredCompanies } } = require('../services/maxOutboundControlLoop');
  for (const storeTenantId of ['13', '10']) {
    await t.test(`tenant 13 with store tenant ${storeTenantId}`, async () => {
      const queries = [];
      const events = [];
      const providerCalls = [];
      const fixturePool = companyPool();
      const pool = { query: async (sql, params) => {
        queries.push(sql);
        // Only the caller's tenant-scoped ownership classification may query.
        assert.match(sql, /WHERE p\.client_id=\$4/);
        assert.equal(params[3], 13);
        return fixturePool.query(sql, params);
      } };
      const result = await persistDiscoveredCompanies(pool, {
        ...clearStore, tenantId: storeTenantId, clientId: Number(storeTenantId), pool,
        event: async (type, key, payload) => events.push({ type, payload }),
      }, {
        tenantId: '13',
        companies: [{
          name: 'Granite PM', domain: 'pm.example', website: 'https://pm.example',
          industry: 'property_manager',
        }],
        scoutContext: {
          scope: { segment: 'short_term_rental' },
          recoverySources: Object.fromEntries(['pfIntelligence', 'website', 'prospeo', 'hunter']
            .map(name => [name, async () => {
              providerCalls.push(name);
              throw new Error('Unexpected recovery provider call');
            }])),
        },
      });
      const reason = storeTenantId === '13' ? 'recovery_tenant_unsupported' : 'recovery_tenant_mismatch';
      assert.equal(queries.length, 1);
      assert.deepEqual(providerCalls, []);
      assert.equal(result.inserted, 0);
      assert.equal(result.admission.recovered, 0);
      assert.equal(result.admission.sameCompanyCandidatesAttempted, 0);
      assert.equal(result.admission.alternateContactLossReasons[reason], 1);
      assert.deepEqual(events, []);
    });
  }
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
