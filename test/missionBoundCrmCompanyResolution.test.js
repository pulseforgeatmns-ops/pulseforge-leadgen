'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  buildMissionBoundCandidates,
  listMissionBoundCompanyIds,
  listMissionBoundProspectIds,
} = require('../packages/max/workspace/EmmettMissionCandidates');
const {
  loadBestCrmProspectForMissionBoundKey,
  loadCrmProspectsForMissionBoundCompanies,
} = require('../packages/max/workspace/MissionBoundCrmResolver');

const MISSION = {
  id: 'mission_test',
  tenantId: '10',
  clientId: 10,
  targetSegment: 'Law Firms',
  structuredMission: { market: { label: 'Law Firms', segment: 'law_firm' } },
};

const COMPANY_KLUG = '31bcc7e2-1111-4111-8111-111111111111';
const COMPANY_SOLOMON = '585d9f94-2222-4222-8222-222222222222';
const CONTACT_KLUG = 'a1111111-3333-4333-8333-333333333333';
const CONTACT_SOLOMON = 'b2222222-4444-4444-8444-444444444444';

function buildAnchorContributions() {
  return [
    {
      specialist: 'scout',
      kind: 'discovery',
      payload: {
        opportunities: [
          { id: COMPANY_KLUG, companyId: COMPANY_KLUG, name: 'Klug Law Offices, PLLC' },
          { id: COMPANY_SOLOMON, companyId: COMPANY_SOLOMON, name: 'Solomon Law Firm' },
        ],
        prospects: [
          {
            id: CONTACT_KLUG,
            companyId: COMPANY_KLUG,
            company: 'Klug Law Offices, PLLC',
            email: null,
          },
          {
            id: CONTACT_SOLOMON,
            companyId: COMPANY_SOLOMON,
            company: 'Solomon Law Firm',
            email: null,
          },
        ],
      },
    },
    {
      specialist: 'max',
      kind: 'prioritization',
      payload: {
        rankedTargets: [
          {
            id: COMPANY_KLUG,
            companyId: COMPANY_KLUG,
            name: 'Klug Law Offices, PLLC',
            rank: 1,
            fit: 0.9,
          },
          {
            id: COMPANY_SOLOMON,
            companyId: COMPANY_SOLOMON,
            name: 'Solomon Law Firm',
            rank: 2,
            fit: 0.85,
          },
        ],
      },
    },
  ];
}

function crmRows() {
  return [
    {
      prospect_id: CONTACT_KLUG,
      company_id: COMPANY_KLUG,
      client_id: 10,
      company_name: 'Klug Law Offices, PLLC',
      email: null,
      email_verified: false,
      email_status: null,
      do_not_contact: false,
      icp_score: 88,
      is_synthetic: false,
    },
    {
      prospect_id: CONTACT_SOLOMON,
      company_id: COMPANY_SOLOMON,
      client_id: 10,
      company_name: 'Solomon Law Firm',
      email: null,
      email_verified: false,
      email_status: null,
      do_not_contact: false,
      icp_score: 82,
      is_synthetic: false,
    },
    {
      prospect_id: 'c3333333-5555-4555-8555-555555555555',
      company_id: COMPANY_KLUG,
      client_id: 10,
      company_name: 'Klug Law Offices, PLLC',
      email: null,
      email_verified: false,
      email_status: null,
      do_not_contact: false,
      icp_score: 70,
      is_synthetic: false,
    },
  ];
}

function mockDb(rows = crmRows()) {
  return {
    query: async (sql, params = []) => {
      const clientId = params[0];
      const keys = Array.isArray(params[1]) ? params[1] : [params[1]];
      const matches = rows.filter((row) => {
        if (row.client_id !== clientId) return false;
        if (row.is_synthetic) return false;
        return keys.some((key) =>
          String(row.company_id) === String(key)
          || String(row.prospect_id) === String(key)
          || (row.domain && String(row.domain).toLowerCase() === String(key).toLowerCase())
        );
      });
      const ranked = keys.flatMap((key) => {
        const candidates = matches
          .filter((row) =>
            String(row.company_id) === String(key)
            || String(row.prospect_id) === String(key)
            || (row.domain && String(row.domain).toLowerCase() === String(key).toLowerCase())
          )
          .sort((a, b) => {
            const aCompanyMatch = String(a.company_id) === String(key) ? 0 : 1;
            const bCompanyMatch = String(b.company_id) === String(key) ? 0 : 1;
            if (aCompanyMatch !== bCompanyMatch) return aCompanyMatch - bCompanyMatch;
            return (b.icp_score || 0) - (a.icp_score || 0);
          });
        return candidates.length ? [{ mission_bound_key: String(key), ...candidates[0] }] : [];
      });
      if (/DISTINCT ON \(k\.mission_bound_key\)/.test(sql)) {
        return { rows: ranked };
      }
      if (/LIMIT 1/.test(sql)) {
        const key = String(params[1]);
        const hit = ranked.find((row) => row.mission_bound_key === key) || ranked[0] || null;
        return { rows: hit ? [hit] : [] };
      }
      return { rows: [] };
    },
  };
}

describe('mission-bound company → CRM contact resolution', () => {
  it('listMissionBoundCompanyIds returns Max company/candidate IDs, not scout contact IDs', () => {
    const contributions = buildAnchorContributions();
    assert.deepEqual(listMissionBoundCompanyIds(MISSION, contributions), [
      COMPANY_KLUG,
      COMPANY_SOLOMON,
    ]);
    assert.deepEqual(listMissionBoundProspectIds(MISSION, contributions), [
      CONTACT_KLUG,
      CONTACT_SOLOMON,
    ]);
    assert.notEqual(listMissionBoundCompanyIds(MISSION, contributions)[0], CONTACT_KLUG);
  });

  it('buildMissionBoundCandidates keeps candidate id separate from scout contact id', () => {
    const candidates = buildMissionBoundCandidates(MISSION, buildAnchorContributions());
    const klug = candidates.find((row) => row.company.includes('Klug'));
    assert.equal(klug.id, COMPANY_KLUG);
    assert.equal(klug.candidateId, COMPANY_KLUG);
    assert.equal(klug.crmProspectId, CONTACT_KLUG);
  });

  it('loadCrmProspectsForMissionBoundCompanies resolves via prospects.company_id', async () => {
    const map = await loadCrmProspectsForMissionBoundCompanies({
      pool: mockDb(),
      clientId: 10,
      companyIds: [COMPANY_KLUG, COMPANY_SOLOMON],
    });
    assert.equal(map.size, 2);
    assert.equal(String(map.get(COMPANY_KLUG).prospect_id), CONTACT_KLUG);
    assert.equal(String(map.get(COMPANY_SOLOMON).prospect_id), CONTACT_SOLOMON);
  });

  it('prefers the highest icp_score contact for a company', async () => {
    const row = await loadBestCrmProspectForMissionBoundKey({
      pool: mockDb(),
      clientId: 10,
      missionBoundKey: COMPANY_KLUG,
    });
    assert.equal(String(row.prospect_id), CONTACT_KLUG);
    assert.equal(row.icp_score, 88);
  });

  it('does not load contacts outside the supplied company universe', async () => {
    const map = await loadCrmProspectsForMissionBoundCompanies({
      pool: mockDb([
        ...crmRows(),
        {
          prospect_id: '99999999-9999-4999-8999-999999999999',
          company_id: 'outside-universe',
          client_id: 10,
          company_name: 'Outside Firm',
          icp_score: 99,
          is_synthetic: false,
        },
      ]),
      clientId: 10,
      companyIds: [COMPANY_KLUG],
    });
    assert.equal(map.size, 1);
    assert.ok(map.has(COMPANY_KLUG));
    assert.ok(!map.has('outside-universe'));
  });

  it('resolves verified CRM contact via exact company domain when candidate key is a Place ID', async () => {
    const placeId = 'ChIJ43Z_V2dP4okRCRcDHefV8OU';
    const map = await loadCrmProspectsForMissionBoundCompanies({
      pool: mockDb([
        {
          prospect_id: CONTACT_KLUG,
          company_id: COMPANY_KLUG,
          client_id: 10,
          company_name: 'Blue Door Living Property Management',
          domain: 'bluedoorliving.com',
          email: 'ops@bluedoorliving.com',
          email_verified: true,
          email_status: 'verified',
          icp_score: 90,
          is_synthetic: false,
        },
      ]),
      clientId: 10,
      companyIds: [placeId, 'bluedoorliving.com'],
    });
    assert.equal(map.size, 1);
    assert.equal(String(map.get('bluedoorliving.com').prospect_id), CONTACT_KLUG);
    assert.ok(!map.has(placeId));
  });

  it('still resolves when mission key equals CRM prospect id (legacy integer fixtures)', async () => {
    const row = await loadBestCrmProspectForMissionBoundKey({
      pool: mockDb([
        {
          prospect_id: 101,
          company_id: 'co-harbor',
          client_id: 10,
          company_name: 'Harbor Law',
          icp_score: 75,
          is_synthetic: false,
        },
      ]),
      clientId: 10,
      missionBoundKey: '101',
    });
    assert.equal(row.prospect_id, 101);
    assert.equal(row.company_id, 'co-harbor');
  });
});
