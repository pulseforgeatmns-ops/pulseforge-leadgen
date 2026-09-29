'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  resolveMissionBoundWebsiteIntel,
  collectWebsiteFieldTraces,
  applyWebsiteIntelToCandidate,
  candidateWebsiteFields,
} = require('../packages/max/workspace/MissionBoundWebsiteIntel');
const {
  buildMissionBoundCandidates,
} = require('../packages/max/workspace/EmmettMissionCandidates');
const {
  admitMissionBoundCandidate,
  resolvedCandidateWebsite,
} = require('../packages/max/workspace/MissionBoundCrmAdmission');
const { buildRepairPlan } = require('../scripts/backfillMissionBoundWebsiteDomains');

const PLACE_BLUE = 'ChIJ43Z_V2dP4okRCRcDHefV8OU';
const PLACE_MILL = 'ChIJgyDf-cxO4okRSlEJCi27f94';
const PLACE_STEWART = 'ChIJstewart-example-place-id01';
const MISSION_ID = 'mission_82e8102f-249c-4f44-b88e-2de76b13898e';
const CLIENT_ID = 10;

const MISSION = {
  id: MISSION_ID,
  tenantId: '10',
  clientId: 10,
  targetSegment: 'Short-term rental operators',
  structuredMission: { market: { label: 'Short-term rental operators', segment: 'str' } },
};

function anchorStrContributions() {
  return [
    {
      missionId: MISSION_ID,
      specialist: 'scout',
      kind: 'discovery',
      payload: {
        opportunities: [
          {
            id: PLACE_BLUE,
            companyId: PLACE_BLUE,
            placeId: PLACE_BLUE,
            name: 'Blue Door Living Property Management',
            website: 'https://www.bluedoorliving.com/about',
          },
          {
            id: PLACE_MILL,
            companyId: PLACE_MILL,
            placeId: PLACE_MILL,
            name: 'Mill City Property Management',
            sourceUrl: 'https://www.millcitypm.com/',
          },
          {
            id: PLACE_STEWART,
            companyId: PLACE_STEWART,
            placeId: PLACE_STEWART,
            name: 'Stewart Property Management',
          },
          {
            id: 'ChIJlot202-example-place-id01',
            companyId: 'ChIJlot202-example-place-id01',
            placeId: 'ChIJlot202-example-place-id01',
            name: 'Lot 202 - Property Management Company',
          },
          {
            id: 'ChIJnhcore-example-place-id01',
            companyId: 'ChIJnhcore-example-place-id01',
            placeId: 'ChIJnhcore-example-place-id01',
            name: 'NH Core Properties',
            domain: 'nhcoreproperties.com',
          },
        ],
      },
    },
    {
      missionId: MISSION_ID,
      specialist: 'max',
      kind: 'prioritization',
      payload: {
        rankedTargets: [
          { id: PLACE_BLUE, companyId: PLACE_BLUE, placeId: PLACE_BLUE, name: 'Blue Door Living Property Management', rank: 1, fit: 0.9 },
          { id: PLACE_MILL, companyId: PLACE_MILL, placeId: PLACE_MILL, name: 'Mill City Property Management', rank: 2, fit: 0.86 },
          {
            id: PLACE_STEWART,
            companyId: PLACE_STEWART,
            placeId: PLACE_STEWART,
            name: 'Stewart Property Management',
            rank: 3,
            fit: 0.84,
            website: 'https://stewartproperty.net/',
          },
          {
            id: 'ChIJlot202-example-place-id01',
            companyId: 'ChIJlot202-example-place-id01',
            placeId: 'ChIJlot202-example-place-id01',
            name: 'Lot 202 - Property Management Company',
            rank: 4,
            fit: 0.8,
          },
          {
            id: 'ChIJnhcore-example-place-id01',
            companyId: 'ChIJnhcore-example-place-id01',
            placeId: 'ChIJnhcore-example-place-id01',
            name: 'NH Core Properties',
            rank: 5,
            fit: 0.78,
          },
        ],
      },
    },
  ];
}

function createMockDb(seed = {}) {
  const companies = new Map((seed.companies || []).map((row) => [String(row.id), { ...row }]));
  const prospects = new Map((seed.prospects || []).map((row) => [String(row.id), { ...row }]));
  let companySeq = seed.companySeq || 1;
  let prospectSeq = seed.prospectSeq || 1;

  return {
    companies,
    prospects,
    query: async (sql, params = []) => {
      const text = String(sql);
      if (/ALTER TABLE companies/.test(text)) return { rows: [] };
      if (/CREATE UNIQUE INDEX/.test(text)) return { rows: [] };
      if (/ADD COLUMN IF NOT EXISTS business_name_short/.test(text)) return { rows: [] };

      if (/google_place_id = \$2/.test(text)) {
        const hit = [...companies.values()].find(
          (row) => row.client_id === params[0] && row.google_place_id === params[1]
        );
        return { rows: hit ? [hit] : [] };
      }

      if (/lower\(domain\) = lower\(\$2\)/.test(text)) {
        const hit = [...companies.values()].find(
          (row) => row.client_id === params[0]
            && row.domain
            && String(row.domain).toLowerCase() === String(params[1]).toLowerCase()
        );
        return { rows: hit ? [hit] : [] };
      }

      if (/UPDATE companies/.test(text)) {
        const company = companies.get(String(params[4]));
        if (!company) return { rows: [] };
        Object.assign(company, {
          google_place_id: company.google_place_id || params[0],
          domain: company.domain || params[1],
          website: company.website || params[2],
          enrichment_provenance: {
            ...(company.enrichment_provenance || {}),
            ...(JSON.parse(params[3])),
          },
        });
        return { rows: [company] };
      }

      if (/INSERT INTO companies/.test(text)) {
        const id = `company-${companySeq++}`;
        const row = {
          id,
          name: params[0],
          domain: params[4],
          website: params[5],
          google_place_id: params[6],
          client_id: params[9],
          enrichment_provenance: JSON.parse(params[10]),
        };
        companies.set(String(id), row);
        return { rows: [row] };
      }

      if (/UPDATE prospects/.test(text)) {
        const prospect = prospects.get(String(params[3]));
        if (!prospect) return { rows: [] };
        Object.assign(prospect, {
          website_url: prospect.website_url || params[0],
          has_website: prospect.has_website || params[1],
          enrichment_provenance: {
            ...(prospect.enrichment_provenance || {}),
            ...(JSON.parse(params[2])),
          },
        });
        return { rows: [prospect] };
      }

      if (/INSERT INTO prospects/.test(text)) {
        const id = `prospect-${prospectSeq++}`;
        const row = {
          id,
          company_id: params[0],
          source: params[1],
          client_id: params[4],
          vertical: params[3],
          website_url: params[5],
          has_website: params[7],
          email: null,
          is_synthetic: false,
        };
        prospects.set(String(id), row);
        return { rows: [row] };
      }

      if (/AND company_id = \$2::uuid/.test(text)) {
        const matches = [...prospects.values()]
          .filter((row) => row.client_id === params[0] && String(row.company_id) === String(params[1]));
        return { rows: matches.slice(0, 1) };
      }

      return { rows: [] };
    },
  };
}

describe('MissionBoundWebsiteIntel', () => {
  it('1. candidate with website persists canonical website/domain on new admission', async () => {
    const db = createMockDb();
    const candidate = buildMissionBoundCandidates(MISSION, anchorStrContributions())
      .find((row) => row.candidateId === PLACE_BLUE);
    assert.equal(candidate.domain, 'bluedoorliving.com');
    assert.equal(candidate.website, 'https://www.bluedoorliving.com/about');

    const result = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    const company = db.companies.get(String(result.companyId));
    const prospect = db.prospects.get(String(result.prospectId));
    assert.equal(company.domain, 'bluedoorliving.com');
    assert.equal(company.website, 'https://www.bluedoorliving.com/about');
    assert.equal(prospect.website_url, 'https://www.bluedoorliving.com/about');
    assert.equal(prospect.has_website, true);
  });

  it('2. candidate with domain persists normalized domain', async () => {
    const candidate = buildMissionBoundCandidates(MISSION, anchorStrContributions())
      .find((row) => row.company === 'NH Core Properties');
    assert.equal(candidate.domain, 'nhcoreproperties.com');
    assert.equal(resolvedCandidateWebsite(candidate).domain, 'nhcoreproperties.com');
  });

  it('3. candidate without website/domain does not fabricate one', () => {
    const candidate = buildMissionBoundCandidates(MISSION, anchorStrContributions())
      .find((row) => row.company === 'Lot 202 - Property Management Company');
    assert.equal(candidate.domain, null);
    assert.equal(candidate.website, null);
    const traces = collectWebsiteFieldTraces({
      target: { name: candidate.company },
      opp: { name: candidate.company },
      prospect: null,
    });
    assert.equal(traces.length, 0);
  });

  it('4. existing CRM domain is not overwritten by weaker candidate data', () => {
    const candidate = buildMissionBoundCandidates(MISSION, anchorStrContributions())
      .find((row) => row.candidateId === PLACE_STEWART);
    const plan = buildRepairPlan(candidate, {
      company_id: 'co-stewart',
      prospect_id: 'pr-stewart',
      domain: 'stewartproperty.net',
      website: 'https://stewartproperty.net/',
      website_url: 'https://stewartproperty.net/',
    });
    assert.equal(plan.action, 'noop_already_canonical');
  });

  it('5. existing admitted row with missing domain can be backfilled idempotently', () => {
    const candidate = buildMissionBoundCandidates(MISSION, anchorStrContributions())
      .find((row) => row.candidateId === PLACE_MILL);
    const first = buildRepairPlan(candidate, {
      company_id: 'co-mill',
      prospect_id: 'pr-mill',
      domain: null,
      website: null,
      website_url: null,
    });
    assert.equal(first.action, 'repair');
    assert.equal(first.updates.companyDomain, 'millcitypm.com');
    assert.equal(first.updates.companyWebsite, 'https://www.millcitypm.com/');

    const second = buildRepairPlan(candidate, {
      company_id: 'co-mill',
      prospect_id: 'pr-mill',
      domain: 'millcitypm.com',
      website: 'https://www.millcitypm.com/',
      website_url: 'https://www.millcitypm.com/',
    });
    assert.equal(second.action, 'noop_already_canonical');
  });

  it('6. Place ID linkage preserved', async () => {
    const db = createMockDb();
    const candidate = buildMissionBoundCandidates(MISSION, anchorStrContributions())[0];
    const result = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    assert.equal(result.placeId, candidate.placeId);
    assert.equal(db.companies.get(String(result.companyId)).google_place_id, candidate.placeId);
  });

  it('7. tenant isolation preserved', async () => {
    const db = createMockDb();
    const candidate = buildMissionBoundCandidates(MISSION, anchorStrContributions())[0];
    const result = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    assert.equal(db.companies.get(String(result.companyId)).client_id, 10);
    assert.equal(db.prospects.get(String(result.prospectId)).client_id, 10);
  });

  it('8. per-company upstream field paths are traceable', () => {
    const contributions = anchorStrContributions();
    const candidates = buildMissionBoundCandidates(MISSION, contributions);
    const blue = candidates.find((row) => row.candidateId === PLACE_BLUE);
    const mill = candidates.find((row) => row.candidateId === PLACE_MILL);
    const stewart = candidates.find((row) => row.candidateId === PLACE_STEWART);
    const lot202 = candidates.find((row) => row.company.includes('Lot 202'));
    const nhCore = candidates.find((row) => row.company === 'NH Core Properties');

    assert.match(blue.websiteFieldPath, /scout\.discovery\.opportunities\.website/);
    assert.match(mill.websiteFieldPath, /scout\.discovery\.opportunities\.sourceUrl/);
    assert.match(stewart.websiteFieldPath, /max\.prioritization\.rankedTargets\.website/);
    assert.equal(lot202.domain, null);
    assert.match(nhCore.websiteFieldPath, /scout\.discovery\.opportunities\.domain/);
  });

  it('9. resolveMissionBoundWebsiteIntel prefers Max over Scout when both present', () => {
    const intel = resolveMissionBoundWebsiteIntel({
      target: { website: 'https://stewartproperty.net/' },
      opp: { website: 'https://maps.example/stewart' },
      prospect: null,
    });
    assert.equal(intel.domain, 'stewartproperty.net');
    assert.equal(intel.source, 'max.prioritization');
  });

  it('10. applyWebsiteIntelToCandidate leaves rows unchanged without upstream website', () => {
    const row = { id: 'x', company: 'No Site Co' };
    assert.deepEqual(applyWebsiteIntelToCandidate(row, { domain: null, website: null }), {
      ...row,
      domain: null,
      website: null,
      website_url: null,
      websiteTraces: [],
    });
    assert.deepEqual(candidateWebsiteFields(row), {
      domain: null,
      website: null,
      websiteSource: null,
      websiteFieldPath: null,
      websiteEvidenceRefs: null,
    });
  });
});
