'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { inspectMissingCity, backfillMissingCity } = require('../services/leviScoutCityBackfill');
const { asContacts } = require('../services/leviScoutProductionDeps');

function pool(rows, { onUpdate } = {}) {
  return {
    async query(sql, params) {
      if (/FOR UPDATE/i.test(sql)) {
        return { rows: rows.filter(row => row.id === params[0] && row.client_id === params[1]) };
      }
      if (/UPDATE prospects/i.test(sql)) {
        const id = params[0];
        const clientId = params[1];
        const metadata = JSON.parse(params[2]);
        const row = rows.find(item => item.id === id && item.client_id === clientId);
        if (!row) return { rows: [] };
        if (row.acquisition_metadata && row.acquisition_metadata.city) return { rows: [] };
        row.acquisition_metadata = metadata;
        if (onUpdate) onUpdate(row, metadata);
        return { rows: [{ id }] };
      }
      return { rows };
    },
  };
}

test('asContacts stores inferred city so later imports keep Chicago provenance', () => {
  const contacts = asContacts([{
    candidate: {
      name: 'Ben Operator LLC',
      company: 'Ben Operator LLC',
      service_area_match: null,
      notes: 'Review state: qualify first.',
      acquisition_metadata: {
        sourceUrls: ['https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=1'],
        review: { bucket: 'research_candidate' },
      },
    },
  }]);
  assert.equal(contacts[0].extraMetadata.city, 'Chicago');
  assert.equal(contacts[0].extraMetadata.provider, 'chicago_open_data_permits');
  assert.equal(contacts[0].extraMetadata.cityInference.source, 'source_url_host');
  assert.equal(contacts[0].market, 'Chicago');
});

test('city backfill is tenant-scoped, dry-run by default, and preserves existing evidence', async () => {
  const rows = [
    {
      id: 11,
      client_id: 21,
      source: 'scout_research_only',
      service_area_match: '100 N MICHIGAN AVE, Chicago, IL',
      acquisition_metadata: {
        sourceUrls: ['https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=1'],
        signals: [{ layer: 'property_activity' }],
      },
    },
    {
      id: 12,
      client_id: 21,
      source: 'scout_research_only',
      service_area_match: '10 WALL STREET, Manhattan, New York, NY',
      acquisition_metadata: {
        city: 'Chicago',
        sourceUrls: ['https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=2'],
        signals: [{ layer: 'property_activity' }],
      },
    },
  ];
  const dry = await inspectMissingCity({ db: pool(rows) });
  assert.equal(dry.tenantId, 21);
  assert.equal(dry.missingCity, 1);
  assert.equal(dry.writable, 1);
  assert.equal(dry.plans[0].afterCity, 'Chicago');
  assert.equal(dry.plans[1].wouldWrite, false);

  const updates = [];
  const persisted = await backfillMissingCity({
    db: pool(rows, { onUpdate: (row, metadata) => updates.push({ id: row.id, metadata }) }),
    persist: true,
  });
  assert.equal(persisted.persisted, 1);
  assert.equal(persisted.otherTenantWrites, 0);
  assert.equal(updates[0].id, 11);
  assert.equal(updates[0].metadata.city, 'Chicago');
  assert.deepEqual(updates[0].metadata.signals, [{ layer: 'property_activity' }]);
  assert.equal(rows[1].acquisition_metadata.city, 'Chicago');
});

test('city backfill refuses any tenant other than 21', async () => {
  await assert.rejects(inspectMissingCity({ db: pool([]), tenantId: 10 }), {
    code: 'levi_research_tenant_invalid',
  });
});
