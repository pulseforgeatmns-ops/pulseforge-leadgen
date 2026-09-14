'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  normalizeProspectIds,
  loadCrmProspectsByIds,
} = require('../MissionBoundCrmResolver');

const PRODUCTION_UUIDS = Object.freeze([
  '31bcc7e2-fe86-4208-9525-3c97abe8ecd7',
  '585d9f94-ab88-4535-9337-82e70cf38750',
  'b04466e1-a4ff-41aa-9e19-eaf3011a4f2c',
  '392b9b51-e202-4700-8c34-5feeee952bf0',
  '001c9b7e-5659-4a54-892c-05493a148f9b',
]);

describe('MissionBoundCrmResolver UUID CRM load', () => {
  it('normalizeProspectIds keeps production-shaped UUIDs and rejects numeric coercion', () => {
    assert.deepEqual(
      normalizeProspectIds([
        PRODUCTION_UUIDS[0],
        `  ${PRODUCTION_UUIDS[1].toUpperCase()}  `,
        '101',
        'not-a-uuid',
        null,
      ]),
      [
        PRODUCTION_UUIDS[0],
        PRODUCTION_UUIDS[1],
      ]
    );
  });

  it('loadCrmProspectsByIds queries with uuid[] and tenant scope', async () => {
    const calls = [];
    const pool = {
      query: async (sql, params) => {
        calls.push({ sql, params });
        return {
          rows: params[1].map((id, index) => ({
            id,
            email: `contact${index}@lawfirm.example`,
            email_status: 'verified',
            email_verified: true,
            do_not_contact: false,
          })),
        };
      },
    };

    const map = await loadCrmProspectsByIds({
      clientId: 10,
      pool,
      prospectIds: PRODUCTION_UUIDS,
    });

    assert.equal(calls.length, 1);
    assert.match(calls[0].sql, /client_id = \$1/);
    assert.match(calls[0].sql, /id = ANY\(\$2::uuid\[\]\)/);
    assert.match(calls[0].sql, /enrichment_provenance/);
    assert.match(calls[0].sql, /email_verified/);
    assert.doesNotMatch(calls[0].sql, /int\[\]/);
    assert.equal(calls[0].params[0], 10);
    assert.deepEqual(calls[0].params[1], [...PRODUCTION_UUIDS]);
    assert.equal(map.size, PRODUCTION_UUIDS.length);
    assert.equal(map.get(PRODUCTION_UUIDS[0]).email, 'contact0@lawfirm.example');
  });

  it('loadCrmProspectsByIds returns empty map when UUID normalization yields nothing', async () => {
    let queried = false;
    const map = await loadCrmProspectsByIds({
      clientId: 10,
      pool: {
        query: async () => {
          queried = true;
          return { rows: [] };
        },
      },
      prospectIds: ['101', '202'],
    });
    assert.equal(queried, false);
    assert.equal(map.size, 0);
  });
});
