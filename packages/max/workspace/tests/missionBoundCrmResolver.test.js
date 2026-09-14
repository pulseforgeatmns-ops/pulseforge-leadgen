'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  normalizeProspectIds,
  loadCrmProspectsByIds,
  loadBestCrmProspectForMissionBoundKey,
  inspectMissionBoundCrmForQueueItem,
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

  it('loadBestCrmProspectForMissionBoundKey resolves company_id, not prospects.id', async () => {
    const companyId = PRODUCTION_UUIDS[4];
    const contactId = '7adbb294-b94c-45c0-85df-e040f027ece0';
    const calls = [];
    const pool = {
      query: async (sql, params) => {
        calls.push({ sql, params });
        return {
          rows: [{
            prospect_id: contactId,
            company_id: companyId,
            client_id: 10,
            email: 'jmeyer@backusmeyer.com',
            email_status: 'verified',
            email_verified: true,
            do_not_contact: false,
            enrichment_provenance: { email: { source: 'provider_chain' } },
          }],
        };
      },
    };

    const row = await loadBestCrmProspectForMissionBoundKey({
      clientId: 10,
      pool,
      missionBoundKey: companyId,
    });
    assert.equal(calls.length, 1);
    assert.match(calls[0].sql, /company_id::text = \$2/);
    assert.equal(calls[0].params[1], companyId);
    assert.equal(row.prospect_id, contactId);
    assert.equal(row.email, 'jmeyer@backusmeyer.com');
  });

  it('inspectMissionBoundCrmForQueueItem marks Backus projectable and Deliverability Test excluded', async () => {
    const backusCompany = PRODUCTION_UUIDS[4];
    const pool = {
      query: async (_sql, params) => {
        if (params[1] === backusCompany) {
          return {
            rows: [{
              prospect_id: '7adbb294-b94c-45c0-85df-e040f027ece0',
              company_id: backusCompany,
              email: 'jmeyer@backusmeyer.com',
              email_status: 'verified',
              email_verified: true,
              do_not_contact: false,
              enrichment_provenance: { email: { source: 'provider_chain' } },
            }],
          };
        }
        return { rows: [] };
      },
    };

    const backus = await inspectMissionBoundCrmForQueueItem({
      pool,
      clientId: 10,
      missionBoundKey: backusCompany,
      companyName: 'Backus Meyer',
    });
    assert.equal(backus.crmEmailPresent, true);
    assert.equal(backus.crmProjectable, true);
    assert.equal(backus.projectionBlockReason, null);

    const deliverability = await inspectMissionBoundCrmForQueueItem({
      pool,
      clientId: 10,
      missionBoundKey: '392b9b51-e202-4700-8c34-5feeee952bf0',
      companyName: 'Deliverability Test Firm',
    });
    assert.equal(deliverability.deliverabilityTestExcluded, true);
    assert.equal(deliverability.projectionBlockReason, 'deliverability_test_excluded');
  });
});
