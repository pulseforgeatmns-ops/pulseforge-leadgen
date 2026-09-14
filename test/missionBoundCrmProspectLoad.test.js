'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  loadProspectRowsByIds,
  CLIENT_ID,
} = require('../scripts/lib/anchorMissionBoundEnrichment');

const PRODUCTION_UUIDS = Object.freeze([
  '31bcc7e2-fe86-4208-9525-3c97abe8ecd7',
  '585d9f94-ab88-4535-9337-82e70cf38750',
  'b04466e1-a4ff-41aa-9e19-eaf3011a4f2c',
  '392b9b51-e202-4700-8c34-5feeee952bf0',
  '001c9b7e-5659-4a54-892c-05493a148f9b',
]);

function mockProspectRow(id, index) {
  return {
    prospect_id: id,
    client_id: CLIENT_ID,
    company_id: `co-${index}`,
    company_name: `Law Firm ${index + 1}`,
    email: null,
    email_verified: false,
    email_status: null,
    do_not_contact: false,
    domain: `lawfirm${index + 1}.example`,
  };
}

describe('mission-bound CRM prospect row load', () => {
  it('loadProspectRowsByIds batch-loads tenant-scoped UUID rows in mission order', async () => {
    const calls = [];
    const shuffled = [PRODUCTION_UUIDS[2], PRODUCTION_UUIDS[0], PRODUCTION_UUIDS[4]];
    const db = {
      query: async (sql, params) => {
        calls.push({ sql, params });
        return {
          rows: shuffled.map((id, index) => mockProspectRow(id, index)),
        };
      },
    };

    const rows = await loadProspectRowsByIds(db, CLIENT_ID, PRODUCTION_UUIDS);

    assert.equal(calls.length, 1);
    assert.match(calls[0].sql, /client_id = \$1/);
    assert.match(calls[0].sql, /p\.id = ANY\(\$2::uuid\[\]\)/);
    assert.doesNotMatch(calls[0].sql, /int\[\]/);
    assert.equal(calls[0].params[0], CLIENT_ID);
    assert.deepEqual(calls[0].params[1], [...PRODUCTION_UUIDS]);
    assert.equal(rows.length, 3);
    assert.deepEqual(
      rows.map((row) => String(row.prospect_id)),
      [PRODUCTION_UUIDS[0], PRODUCTION_UUIDS[2], PRODUCTION_UUIDS[4]]
    );
  });

  it('loadProspectRowsByIds loads all five production-shaped UUID rows for tenant 10', async () => {
    const db = {
      query: async (sql, params) => {
        assert.match(sql, /p\.id = ANY\(\$2::uuid\[\]\)/);
        return {
          rows: PRODUCTION_UUIDS.map((id, index) => mockProspectRow(id, index)),
        };
      },
    };

    const rows = await loadProspectRowsByIds(db, CLIENT_ID, PRODUCTION_UUIDS);
    assert.equal(rows.length, PRODUCTION_UUIDS.length);
    assert.deepEqual(
      rows.map((row) => String(row.prospect_id)),
      [...PRODUCTION_UUIDS]
    );
  });
});
