'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const pool = require('../db');
const {
  matchServiceAreaFromLocation,
  matchServiceAreaLocality,
} = require('../utils/serviceArea');
const {
  STUDIO_SUBSTRAL_SERVICE_AREA,
  ensureStudioSubstralServiceArea,
} = require('../utils/studioSubstralTenant');
const { STUDIO_SUBSTRAL_SCOUT_PLAN } = require('../services/studioSubstralScoutIntelligence');
const {
  parseArgs,
  fitScoreTen,
  buildBatchFromRows,
} = require('../scripts/exportStudioSubstralBatch001');
const {
  dedupeProspectRows,
  assignFirstWaveCandidates,
  pickSpecificWebsiteIssue,
  isGenericIssueText,
} = require('../scripts/lib/studioSubstralBatch001Export');
const { STUDIO_PRIORITY_THRESHOLD } = require('../services/studioSubstralScoutIntelligence');

const substralClient = {
  id: 17,
  slug: 'studio-substral',
  scoring_profile: 'studio_substral',
  service_area: ['United States'],
  verticals: STUDIO_SUBSTRAL_SCOUT_PLAN.batch_mix,
};

describe('Studio Substral Batch 001 scout regressions', () => {
  it('Manchester NH businesses match Studio Substral service area (locality or address fallback)', () => {
    assert.ok(STUDIO_SUBSTRAL_SERVICE_AREA.includes('Manchester'));
    assert.equal(
      matchServiceAreaLocality('Manchester', STUDIO_SUBSTRAL_SERVICE_AREA),
      'Manchester'
    );
    assert.equal(
      matchServiceAreaFromLocation('591 Mast Rd, Manchester, NH 03102, USA', STUDIO_SUBSTRAL_SERVICE_AREA),
      'Manchester'
    );
    // Places rows sometimes omit locality; address fallback must still qualify in-area leads.
    assert.equal(matchServiceAreaLocality(null, STUDIO_SUBSTRAL_SERVICE_AREA), null);
    assert.equal(
      matchServiceAreaFromLocation('764 Chestnut St, Manchester, NH 03104, USA', STUDIO_SUBSTRAL_SERVICE_AREA),
      'Manchester'
    );
  });

  it('ensureStudioSubstralServiceArea repairs country-wide service_area to NH scout cities', async () => {
    const originalQuery = pool.query;
    const updates = [];
    try {
      pool.query = async (sql, params = []) => {
        const text = String(sql);
        if (/SELECT \* FROM clients WHERE slug/i.test(text)) {
          return { rows: [{ ...substralClient }] };
        }
        if (/UPDATE clients SET service_area/i.test(text)) {
          updates.push(params);
          return {
            rows: [{
              ...substralClient,
              service_area: params[0],
            }],
          };
        }
        return { rows: [] };
      };

      const client = await ensureStudioSubstralServiceArea(pool);
      assert.deepEqual(client.service_area, [...STUDIO_SUBSTRAL_SERVICE_AREA]);
      assert.equal(updates.length, 1);
    } finally {
      pool.query = originalQuery;
    }
  });

  it('resolveScoutPlanVertical uses explicit category vertical, not the search string', () => {
    delete require.cache[require.resolve('../leadgen')];
    const { _test } = require('../leadgen');
    const industry = 'accounting firm Manchester NH';
    const fromSearch = _test.resolveScoutPlanVertical({ industry });
    assert.equal(fromSearch, 'accounting_firm_manchester_nh');
    const fromCategory = _test.resolveScoutPlanVertical({
      industry,
      vertical: 'professional_services',
    });
    assert.equal(fromCategory, 'professional_services');
    assert.ok(Object.keys(STUDIO_SUBSTRAL_SCOUT_PLAN.verticals).includes(fromCategory));
  });

  it('Studio Substral scout accepts phone-only contact when email enrichment fails', async () => {
    delete require.cache[require.resolve('../leadgen')];
    const leadgen = require('../leadgen');
    const originalQuery = pool.query;
    try {
      pool.query = async (sql) => {
        if (/SELECT \* FROM clients WHERE id/.test(String(sql))) {
          return { rows: [{ ...substralClient, service_area: STUDIO_SUBSTRAL_SERVICE_AREA }] };
        }
        return { rows: [] };
      };
      await leadgen.configureScoringContext({
        client_id: 17,
        vertical: 'property_and_home_services',
        industry: 'HVAC company Manchester NH',
        location: 'Manchester NH',
      });
      const candidate = leadgen._test.resolveScoutContactCandidate({
        company: 'Example HVAC',
        phone: '(603) 555-0100',
        email: '—',
      });
      assert.equal(candidate.insertTarget, 'prospect');
      assert.equal(candidate.phone, '(603) 555-0100');
      assert.equal(candidate.email, null);
    } finally {
      pool.query = originalQuery;
    }
  });

  it('Places Manchester lead persists in-area when locality is missing but address matches', async () => {
    delete require.cache[require.resolve('../leadgen')];
    const leadgen = require('../leadgen');
    const originalQuery = pool.query;
    const inserts = [];
    try {
      pool.query = async (sql, params = []) => {
        const text = String(sql);
        if (/SELECT \* FROM clients WHERE id/.test(text)) {
          return {
            rows: [{
              ...substralClient,
              service_area: STUDIO_SUBSTRAL_SERVICE_AREA,
            }],
          };
        }
        if (/SELECT p\.id[\s\S]+FROM prospects p/.test(text)) return { rows: [] };
        if (/SELECT id[\s\S]+FROM companies/.test(text)) return { rows: [] };
        if (/INSERT INTO companies/.test(text)) return { rows: [{ id: 'co-1' }] };
        if (/INSERT INTO prospects/.test(text)) {
          inserts.push(params);
          return { rows: [{ id: 'prospect-1' }] };
        }
        if (/INSERT INTO scout_skip_log/.test(text)) return { rows: [] };
        if (/assessWebsiteOpportunity|website_opportunity/i.test(text)) return { rows: [] };
        return { rows: [] };
      };

      await leadgen.configureScoringContext({
        client_id: 17,
        vertical: 'property_and_home_services',
        location: 'Manchester NH',
      });

      const assessDiscoveredBusiness = async () => ({
        assessment: {
          score_components: {},
          confidence: 0.5,
          recommended_action: 'AUDIT_WORTH_REVIEWING',
          commercial_diagnosis: { diagnosis_class: 'REDESIGN_CANDIDATE' },
          evidence_refs: [],
        },
      });
      const webDesignScoutPath = require.resolve('../services/webDesignScout');
      require.cache[webDesignScoutPath] = {
        id: webDesignScoutPath,
        filename: webDesignScoutPath,
        loaded: true,
        exports: {
          assessDiscoveredBusiness,
          mapOpportunityScoreToIcp: () => 70,
        },
      };

      const result = await leadgen._test.saveToDatabase([{
        company: 'Patriot Roofing LLC',
        url: 'https://patriotroofingnh.com',
        address: '591 Mast Rd, Manchester, NH 03102, USA',
        places_locality: null,
        source: ['google_places'],
        score: 80,
        phone: '(603) 623-5388',
        email: '—',
      }], { runId: 'batch001-in-area-test' });

      assert.equal(result.skipped_breakdown.out_of_area || 0, 0);
      assert.ok(inserts.length >= 1, 'expected prospect insert for in-area Manchester address');
    } finally {
      pool.query = originalQuery;
      delete require.cache[require.resolve('../services/webDesignScout')];
      delete require.cache[require.resolve('../leadgen')];
    }
  });

  it('dedupes export rows by domain keeping highest studio_fit_score', () => {
    const rows = [
      {
        id: 'dup-low',
        studio_fit_score: 72,
        website_url: 'https://www.keyrenter-newengland.com',
        studio_scout_intelligence: { company_name: 'Keyrenter New England Property Management' },
        created_at: '2026-10-01T00:00:00Z',
      },
      {
        id: 'dup-high',
        studio_fit_score: 86,
        website_url: 'keyrenter-newengland.com',
        studio_scout_intelligence: { company_name: 'Keyrenter New England PM' },
        created_at: '2026-10-02T00:00:00Z',
      },
      {
        id: 'unique',
        studio_fit_score: 75,
        website_url: 'https://heritagehomeservice.com',
        studio_scout_intelligence: { company_name: 'Heritage Home Service' },
        created_at: '2026-10-02T00:00:00Z',
      },
    ];
    const deduped = dedupeProspectRows(rows);
    assert.equal(deduped.length, 2);
    assert.equal(deduped[0].id, 'dup-high');
    assert.ok(deduped.some((r) => r.id === 'unique'));
  });

  it('assigns first-wave flags from strong-tier ranking after dedupe', async () => {
    const rows = [
      {
        id: 'strong-a',
        studio_fit_score: STUDIO_PRIORITY_THRESHOLD + 4,
        website_url: 'https://alpha.example',
        studio_scout_intelligence: {
          company_name: 'Alpha Co',
          website_issues_observed: ['Primary navigation contains no Contact link'],
        },
        studio_outreach_status: 'review_needed',
      },
      {
        id: 'strong-b',
        studio_fit_score: STUDIO_PRIORITY_THRESHOLD + 1,
        website_url: 'https://beta.example',
        studio_scout_intelligence: {
          company_name: 'Beta Co',
          website_issues_observed: ['Missing viewport meta tag'],
        },
        studio_outreach_status: 'review_needed',
      },
      {
        id: 'mid',
        studio_fit_score: STUDIO_PRIORITY_THRESHOLD - 5,
        website_url: 'https://gamma.example',
        studio_scout_intelligence: { company_name: 'Gamma Co' },
        studio_outreach_status: 'new',
      },
    ];
    const batch = await buildBatchFromRows(rows, {
      clientId: 17,
      minFitTen: 7,
      limit: 25,
      skipDeepAudit: true,
    });
    assert.equal(batch.prospect_count, 3);
    assert.equal(batch.strong_tier_count, 3);
    assert.equal(batch.first_wave_candidates.length, 3);
    assert.equal(batch.prospects[0].first_wave_candidate, true);
    assert.ok(batch.prospects[0].max_priority_score >= batch.prospects[2].max_priority_score);
  });

  it('pickSpecificWebsiteIssue avoids generic trust/conversion fallback when findings exist', () => {
    const issue = pickSpecificWebsiteIssue({
      intel: { website_issues_observed: ['Homepage trust and conversion path need strengthening'] },
      assessmentPayload: {
        assessment: {
          verified_findings: [{
            id: 'dom_no_contact_nav',
            summary: 'Primary navigation contains no Contact link',
            category: 'conversion_structure',
            evidence_class: 'OBSERVED',
          }],
        },
      },
    });
    assert.ok(issue);
    assert.equal(isGenericIssueText(issue), false);
    assert.match(issue, /homepage CTA|Contact/i);
  });

  it('export batch builder reflects qualified inventory rows passed in', async () => {
    const savedArgv = process.argv;
    process.argv = ['node', 'export', '--min-fit=7', '--limit=25'];
    const { minStudioScore } = parseArgs();
    process.argv = savedArgv;
    assert.equal(minStudioScore, 65);

    const rows = [
      {
        id: 'p1',
        studio_fit_score: 74,
        studio_outreach_status: 'review_needed',
        studio_scout_intelligence: {
          company_name: 'Example Co',
          website_url: 'https://example.com',
          studio_category: 'property_and_home_services',
          website_issues_observed: ['Weak CTA on homepage'],
          recommended_outreach_angle: 'Example Co shows stronger reviews than the homepage trust cues suggest.',
          confidence: 'high',
        },
        website_url: 'example.com',
        service_area_match: 'Manchester',
        phone: '603-555-0100',
      },
      {
        id: 'p2',
        studio_fit_score: 55,
        studio_outreach_status: 'not_fit',
        studio_scout_intelligence: {},
        website_url: 'weak.example',
      },
    ];
    const batch = await buildBatchFromRows(rows, {
      clientId: 17,
      minFitTen: 7,
      limit: 25,
      skipDeepAudit: true,
    });
    assert.equal(batch.prospect_count, 2);
    assert.equal(batch.prospects[0].fit_score_1_to_10, fitScoreTen(74));
    assert.equal(batch.prospects[0].business_name, 'Example Co');
    assert.match(batch.prospects[0].suggested_first_touch_email, /Studio Substral/);
    assert.equal(batch.tracking.prospects_contacted, 0);
  });

  it('batch scout and export scripts do not invoke outbound send paths', () => {
    const root = path.join(__dirname, '..');
    for (const rel of [
      'scripts/exportStudioSubstralBatch001.js',
      'scripts/studioSubstralScoutBatch001.js',
      'leadgen.js',
    ]) {
      const src = fs.readFileSync(path.join(root, rel), 'utf8');
      assert.doesNotMatch(src, /\bemmettAgent\b/);
      assert.doesNotMatch(src, /\bsendTransactionalEmail\b/);
      assert.doesNotMatch(src, /\bBREVO_API_KEY\b.*send/i);
    }
    const exportSrc = fs.readFileSync(path.join(root, 'scripts/exportStudioSubstralBatch001.js'), 'utf8');
    assert.doesNotMatch(exportSrc, /fetch\s*\(\s*['"]https?:\/\/.*send/i);
    const batchSrc = fs.readFileSync(path.join(root, 'scripts/studioSubstralScoutBatch001.js'), 'utf8');
    assert.match(batchSrc, /Outreach remains disabled/i);
  });
});
