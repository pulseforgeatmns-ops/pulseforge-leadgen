'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const {
  admitMissionBoundCandidate,
  admitMissionBoundCandidates,
  hasSufficientAdmissionIdentity,
  resolveExistingCompany,
  resolveMissionBoundCrmVertical,
  ADMISSION_SOURCE,
} = require('../packages/max/workspace/MissionBoundCrmAdmission');
const {
  isCanonicalBusinessVertical,
  CANONICAL_BUSINESS_VERTICALS,
} = require('../utils/canonicalVerticals');
const {
  buildMissionBoundCandidates,
} = require('../packages/max/workspace/EmmettMissionCandidates');
const {
  loadCrmProspectsForMissionBoundCompanies,
  resolveMissionBoundRecipientEmail,
  isProjectableCrmProspect,
} = require('../packages/max/workspace/MissionBoundCrmResolver');
const { aliasCrmMapToIdentities } = require('../packages/max/workspace/CanonicalOutboundIdentity');
const {
  loadMissionBoundProspects,
  enrichProspectRow,
  DEFAULT_MISSION_ID,
} = require('../scripts/lib/anchorMissionBoundEnrichment');
const {
  DEFAULT_MISSION_ID: RUNNER_MISSION_ID,
  RAILWAY_COMMAND,
} = require('../scripts/enrichAnchorMissionBoundContacts');
const {
  classifyQueueItems,
  chooseNextRecoveryIntent,
} = require('../scripts/lib/anchorCanonicalOutbound');
const {
  mapAssessedToCapacityPayload,
  sanitizeQueueItem,
  fixtureInfrastructureSnapshot,
} = require('../packages/max/workspace/EmmettCapacityExecution');
const eoi = require('../packages/emmett-outbound');

const PLACE_BLUE = 'ChIJ43Z_V2dP4okRCRcDHefV8OU';
const PLACE_MILL = 'ChIJgyDf-cxO4okRSlEJCi27f94';
const COMPANY_EXISTING = '31bcc7e2-1111-4111-8111-111111111111';
const PROSPECT_EXISTING = 'a1111111-3333-4333-8333-333333333333';
const CLIENT_ID = 10;
const MISSION_ID = 'mission_82e8102f-249c-4f44-b88e-2de76b13898e';

const MISSION = {
  id: MISSION_ID,
  tenantId: '10',
  clientId: 10,
  targetSegment: 'Short-term rental operators',
  structuredMission: { market: { label: 'Short-term rental operators', segment: 'str' } },
};

function strContributions({ includeDomain = true, includeProspectUuid = false } = {}) {
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
            website: includeDomain ? 'https://bluedoorliving.com' : null,
          },
          {
            id: PLACE_MILL,
            companyId: PLACE_MILL,
            placeId: PLACE_MILL,
            name: 'Mill City Property Management',
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
          {
            id: PLACE_BLUE,
            companyId: PLACE_BLUE,
            placeId: PLACE_BLUE,
            name: 'Blue Door Living Property Management',
            rank: 1,
            fit: 0.9,
            website: includeDomain ? 'https://bluedoorliving.com' : null,
          },
          {
            id: PLACE_MILL,
            companyId: PLACE_MILL,
            placeId: PLACE_MILL,
            name: 'Mill City Property Management',
            rank: 2,
            fit: 0.86,
          },
        ],
      },
    },
  ];
}

function createMockDb(seed = {}) {
  const companies = new Map((seed.companies || []).map((row) => [String(row.id), { ...row }]));
  const prospects = new Map((seed.prospects || []).map((row) => [String(row.id), { ...row }]));
  const agentLog = [];
  let companySeq = seed.companySeq || 1;
  let prospectSeq = seed.prospectSeq || 1;

  const db = {
    companies,
    prospects,
    agentLog,
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

      if (/id::text = \$2/.test(text) && /FROM companies/.test(text)) {
        const hit = companies.get(String(params[1]));
        return { rows: hit && hit.client_id === params[0] ? [hit] : [] };
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

      if (/AND id = \$2::uuid/.test(text) && /COALESCE\(is_synthetic/.test(text)) {
        const hit = prospects.get(String(params[1]));
        return { rows: hit && hit.client_id === params[0] ? [hit] : [] };
      }

      if (/AND company_id = \$2::uuid/.test(text) && /ORDER BY icp_score/.test(text)) {
        const matches = [...prospects.values()]
          .filter((row) => row.client_id === params[0] && String(row.company_id) === String(params[1]))
          .sort((a, b) => (b.icp_score || 0) - (a.icp_score || 0));
        return { rows: matches.slice(0, 1) };
      }

      if (/INSERT INTO prospects/.test(text)) {
        const id = `prospect-${prospectSeq++}`;
        const row = {
          id,
          company_id: params[0],
          source: params[1],
          client_id: params[4],
          vertical: params[3],
          email: null,
          email_verified: false,
          email_status: null,
          do_not_contact: false,
          icp_score: params[2] || 0,
          is_synthetic: false,
        };
        prospects.set(String(id), row);
        return { rows: [row] };
      }

      if (/INSERT INTO agent_log/.test(text)) {
        agentLog.push(params);
        return { rows: [] };
      }

      if (/DISTINCT ON \(k\.mission_bound_key\)/.test(text)) {
        const keys = params[1];
        const rows = keys.flatMap((key) => {
          for (const prospect of prospects.values()) {
            if (prospect.client_id !== params[0]) continue;
            const company = companies.get(String(prospect.company_id));
            if (!company) continue;
            const match = String(prospect.company_id) === String(key)
              || String(prospect.id) === String(key)
              || (company.domain && String(company.domain).toLowerCase() === String(key).toLowerCase())
              || (company.google_place_id && company.google_place_id === key);
            if (match) {
              return [{
                mission_bound_key: String(key),
                prospect_id: prospect.id,
                company_id: prospect.company_id,
                client_id: prospect.client_id,
                company_name: company.name,
                domain: company.domain,
                google_place_id: company.google_place_id,
                website: company.website,
                email: prospect.email,
                email_verified: prospect.email_verified,
                email_status: prospect.email_status,
                do_not_contact: prospect.do_not_contact,
                icp_score: prospect.icp_score || 0,
                is_synthetic: false,
              }];
            }
          }
          return [];
        });
        return { rows };
      }

      return { rows: [] };
    },
  };

  return db;
}

describe('MissionBoundCrmAdmission', () => {
  it('1. admits Place-ID candidate with no CRM entity deterministically', async () => {
    const db = createMockDb();
    const candidate = buildMissionBoundCandidates(MISSION, strContributions())[0];
    const result = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    assert.equal(result.blocked, false);
    assert.ok(result.companyId);
    assert.ok(result.prospectId);
    assert.equal(result.placeId, PLACE_BLUE);
    assert.equal(db.companies.size, 1);
    assert.equal(db.prospects.size, 1);
  });

  it('2. preserves tenant_id=10 and external identity provenance', async () => {
    const db = createMockDb();
    const candidate = buildMissionBoundCandidates(MISSION, strContributions())[0];
    const result = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    const company = db.companies.get(String(result.companyId));
    assert.equal(company.client_id, 10);
    assert.equal(company.google_place_id, PLACE_BLUE);
    assert.equal(company.domain, 'bluedoorliving.com');
    assert.equal(company.enrichment_provenance.mission_bound_admission.mission_id, MISSION_ID);
    assert.equal(company.enrichment_provenance.mission_bound_admission.place_id, PLACE_BLUE);
  });

  it('3. repeat admission of same candidate creates no duplicate company', async () => {
    const db = createMockDb();
    const candidate = buildMissionBoundCandidates(MISSION, strContributions())[0];
    const first = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    const second = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    assert.equal(first.companyId, second.companyId);
    assert.equal(first.prospectId, second.prospectId);
    assert.equal(db.companies.size, 1);
    assert.equal(db.prospects.size, 1);
    assert.equal(second.reason, 'linked_existing');
  });

  it('4. exact domain match reuses existing company', async () => {
    const db = createMockDb({
      companies: [{
        id: COMPANY_EXISTING,
        name: 'Blue Door Living Property Management',
        domain: 'bluedoorliving.com',
        client_id: 10,
        google_place_id: null,
      }],
    });
    const candidate = buildMissionBoundCandidates(MISSION, strContributions())[0];
    const result = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    assert.equal(result.companyId, COMPANY_EXISTING);
    assert.equal(db.companies.size, 1);
    assert.equal(db.companies.get(COMPANY_EXISTING).google_place_id, PLACE_BLUE);
  });

  it('5. existing linked CRM prospect is reused, not duplicated', async () => {
    const db = createMockDb({
      companies: [{
        id: COMPANY_EXISTING,
        name: 'Blue Door Living Property Management',
        domain: 'bluedoorliving.com',
        google_place_id: PLACE_BLUE,
        client_id: 10,
      }],
      prospects: [{
        id: PROSPECT_EXISTING,
        company_id: COMPANY_EXISTING,
        client_id: 10,
        email: null,
        icp_score: 90,
        is_synthetic: false,
      }],
    });
    const candidate = buildMissionBoundCandidates(MISSION, strContributions())[0];
    const result = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    assert.equal(result.prospectId, PROSPECT_EXISTING);
    assert.equal(db.prospects.size, 1);
  });

  it('6. unsafe ambiguous identity blocks admission without fuzzy merge', async () => {
    const db = createMockDb({
      companies: [
        {
          id: 'co-place',
          name: 'Blue Door Living Property Management',
          google_place_id: PLACE_BLUE,
          client_id: 10,
        },
        {
          id: 'co-domain',
          name: 'Different Entity LLC',
          domain: 'bluedoorliving.com',
          client_id: 10,
        },
      ],
    });
    const candidate = buildMissionBoundCandidates(MISSION, strContributions())[0];
    const result = await resolveExistingCompany(db, CLIENT_ID, candidate);
    assert.equal(result.blocked, true);
    assert.equal(result.reason, 'identity_admission_blocked');
    assert.equal(result.detail, 'conflicting_company_identity');
    assert.equal(db.prospects.size, 0);
  });

  it('7. admitted company enters canonical enrichment path', async () => {
    const db = createMockDb();
    const candidate = buildMissionBoundCandidates(MISSION, strContributions())[0];
    const admission = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    const crmRow = {
      prospect_id: admission.prospectId,
      company_id: admission.companyId,
      client_id: 10,
      company_name: candidate.company,
      domain: 'bluedoorliving.com',
      website: 'https://bluedoorliving.com',
      email: null,
      email_verified: false,
      email_status: null,
      do_not_contact: false,
    };
    const result = await enrichProspectRow(crmRow, {
      db,
      dryRun: true,
      configureScoringContext: async () => {},
      processProspect: async () => ({ selectedEmail: null, resolved: false, errors: [] }),
      runEnrichmentChain: async () => ({
        email: 'ops@bluedoorliving.com',
        source: ['prospeo'],
        contact: 'Ops Team',
      }),
      resolveEmailVerification: async () => ({
        emailVerified: true,
        emailStatus: 'valid',
        doNotContact: false,
        emailVerificationMethod: 'bouncer',
      }),
    });
    assert.equal(result.path, 'provider_chain');
    assert.equal(result.email, 'ops@bluedoorliving.com');
  });

  it('8. verified email persists to canonical CRM record', async () => {
    const db = createMockDb();
    const updates = [];
    const origQuery = db.query.bind(db);
    db.query = async (sql, params) => {
      if (/UPDATE prospects/.test(String(sql))) {
        updates.push({ sql, params });
        const prospect = db.prospects.get(String(params[9]));
        if (prospect) {
          prospect.email = params[0];
          prospect.email_verified = params[3];
          prospect.email_status = params[5];
        }
        return { rows: [] };
      }
      return origQuery(sql, params);
    };

    const candidate = buildMissionBoundCandidates(MISSION, strContributions())[0];
    const admission = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    const crmRow = {
      prospect_id: admission.prospectId,
      company_id: admission.companyId,
      client_id: 10,
      company_name: candidate.company,
      domain: 'bluedoorliving.com',
      email: null,
    };
    const { persistProviderChainEmail } = require('../scripts/lib/anchorMissionBoundEnrichment');
    const persisted = await persistProviderChainEmail(
      db,
      crmRow,
      { email: 'ops@bluedoorliving.com', source: ['prospeo'] },
      { emailVerified: true, emailStatus: 'valid', doNotContact: false, emailVerificationMethod: 'bouncer' },
      false
    );
    assert.equal(persisted.persisted, true);
    assert.ok(updates.some((row) => /UPDATE prospects/.test(row.sql)));
    assert.equal(db.prospects.get(String(admission.prospectId)).email, 'ops@bluedoorliving.com');
  });

  it('9. verified email projects through MissionBoundCrmResolver', async () => {
    const db = createMockDb({
      companies: [{
        id: COMPANY_EXISTING,
        name: 'Blue Door Living Property Management',
        domain: 'bluedoorliving.com',
        google_place_id: PLACE_BLUE,
        client_id: 10,
      }],
      prospects: [{
        id: PROSPECT_EXISTING,
        company_id: COMPANY_EXISTING,
        client_id: 10,
        email: 'ops@bluedoorliving.com',
        email_verified: true,
        email_status: 'verified',
        do_not_contact: false,
        icp_score: 90,
        is_synthetic: false,
      }],
    });
    const map = await loadCrmProspectsForMissionBoundCompanies({
      pool: db,
      clientId: 10,
      companyIds: [PLACE_BLUE],
    });
    assert.equal(map.size, 1);
    assert.equal(String(map.get(PLACE_BLUE).prospect_id), PROSPECT_EXISTING);
    const email = resolveMissionBoundRecipientEmail({
      missionBoundKey: PLACE_BLUE,
      crmByProspectId: aliasCrmMapToIdentities([map], buildMissionBoundCandidates(MISSION, strContributions())),
    });
    assert.equal(email, 'ops@bluedoorliving.com');
  });

  it('10. Paige copy + verified email makes queue item sendable', () => {
    const contributions = [
      ...strContributions(),
      {
        missionId: MISSION_ID,
        specialist: 'paige',
        kind: 'variants',
        payload: {
          variants: [{
            candidateId: PLACE_BLUE,
            companyId: PLACE_BLUE,
            placeId: PLACE_BLUE,
            subject: 'STR walkthrough',
            body: 'Hello from Anchor',
            bindingScope: 'prospect',
          }],
        },
      },
    ];
    const baseCandidates = buildMissionBoundCandidates(MISSION, contributions);
    const crmByProspectId = aliasCrmMapToIdentities([
      new Map([[PLACE_BLUE, {
        prospect_id: PROSPECT_EXISTING,
        company_id: COMPANY_EXISTING,
        email: 'ops@bluedoorliving.com',
        email_verified: true,
        email_status: 'verified',
        do_not_contact: false,
      }]]),
    ], baseCandidates);
    const candidates = buildMissionBoundCandidates(MISSION, contributions, { crmByProspectId });
    const blue = candidates.find((row) => row.candidateId === PLACE_BLUE);
    assert.ok(blue.paige?.subject);
    assert.equal(blue.email, 'ops@bluedoorliving.com');
    const engine = eoi.createOutboundEngine();
    const assessed = engine.assess({
      tenantId: '10',
      clientId: 10,
      snapshot: fixtureInfrastructureSnapshot('10'),
      prospects: candidates,
    });
    const capacity = mapAssessedToCapacityPayload(assessed, {
      infrastructureSnapshot: fixtureInfrastructureSnapshot('10'),
    });
    const item = capacity.queue.items.find((row) => String(row.prospectId) === PLACE_BLUE);
    assert.equal(item.sendable, true);
    assert.equal(item.email, 'ops@bluedoorliving.com');
  });

  it('11. no verified email remains blocked for missing_recipient_email_on_queue_item', () => {
    const contributions = [
      ...strContributions(),
      {
        missionId: MISSION_ID,
        specialist: 'paige',
        kind: 'variants',
        payload: {
          variants: [{
            candidateId: PLACE_MILL,
            subject: 'STR walkthrough',
            body: 'Hello from Anchor',
            bindingScope: 'prospect',
          }],
        },
      },
    ];
    const candidates = buildMissionBoundCandidates(MISSION, contributions);
    const engine = eoi.createOutboundEngine();
    const assessed = engine.assess({
      tenantId: '10',
      clientId: 10,
      snapshot: fixtureInfrastructureSnapshot('10'),
      prospects: candidates,
    });
    const capacity = mapAssessedToCapacityPayload(assessed, {
      infrastructureSnapshot: fixtureInfrastructureSnapshot('10'),
    });
    const classified = classifyQueueItems(capacity, contributions[2]);
    const blocked = classified.blocked.find((row) => row.prospectId === PLACE_MILL);
    assert.ok(blocked.reasons.includes('missing_recipient_email_on_queue_item'));
    assert.ok(!blocked.reasons.includes('missing_paige_copy'));
  });

  it('12. does not approve execution when sendableCount is 0', () => {
    const chosen = chooseNextRecoveryIntent({
      stage: 'ready',
      pendingIntent: 'APPROVE_EXECUTION',
      sendableCount: 0,
      capacityItemCount: 5,
      scoutCandidateCount: 15,
      contributions: { scout: {}, max: {}, paige: {}, emmett: {} },
    });
    assert.notEqual(chosen.intent, 'APPROVE_EXECUTION');
    assert.notEqual(chosen.intent, 'EXECUTE_OUTBOUND');
  });

  it('13. does not execute outbound from admission/enrichment modules', () => {
    const admissionSource = fs.readFileSync(
      path.join(__dirname, '..', 'packages', 'max', 'workspace', 'MissionBoundCrmAdmission.js'),
      'utf8'
    );
    const enrichmentSource = fs.readFileSync(
      path.join(__dirname, '..', 'scripts', 'lib', 'anchorMissionBoundEnrichment.js'),
      'utf8'
    );
    for (const source of [admissionSource, enrichmentSource]) {
      assert.doesNotMatch(source, /EXECUTE_OUTBOUND/);
      assert.doesNotMatch(source, /APPROVE_EXECUTION/);
      assert.doesNotMatch(source, /sendEmail\s*\(/);
      assert.doesNotMatch(source, /api\.brevo\.com/);
    }
  });

  it('14. keeps autosend false — no client mutation in admission path', () => {
    const source = fs.readFileSync(
      path.join(__dirname, '..', 'packages', 'max', 'workspace', 'MissionBoundCrmAdmission.js'),
      'utf8'
    );
    assert.doesNotMatch(source, /autosend/i);
    assert.doesNotMatch(source, /UPDATE\s+clients/i);
    assert.doesNotMatch(source, /enabled_agents/i);
  });

  it('15. runner documentation references the correct STR mission', () => {
    assert.equal(DEFAULT_MISSION_ID, 'mission_82e8102f-249c-4f44-b88e-2de76b13898e');
    assert.equal(RUNNER_MISSION_ID, 'mission_82e8102f-249c-4f44-b88e-2de76b13898e');
    assert.match(RAILWAY_COMMAND, /mission_82e8102f-249c-4f44-b88e-2de76b13898e/);
    assert.doesNotMatch(RAILWAY_COMMAND, /mission_ad7753b0-6def-441d-bb1a-3764656f5750/);
    const runnerSource = fs.readFileSync(
      path.join(__dirname, '..', 'scripts', 'enrichAnchorMissionBoundContacts.js'),
      'utf8'
    );
    assert.match(runnerSource, /mission_82e8102f-249c-4f44-b88e-2de76b13898e/);
  });

  it('blocks candidate with insufficient identity (name only)', async () => {
    const candidate = {
      id: 'name-only',
      candidateId: 'name-only',
      company: 'Mystery Co',
      vertical: 'str',
    };
    assert.equal(hasSufficientAdmissionIdentity(candidate), false);
    const result = await admitMissionBoundCandidate(createMockDb(), candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    assert.equal(result.blocked, true);
    assert.equal(result.reason, 'identity_admission_blocked');
  });

  it('admitMissionBoundCandidates processes prioritized universe only', async () => {
    const db = createMockDb();
    const { results, targets } = await admitMissionBoundCandidates(db, MISSION, strContributions(), {
      clientId: CLIENT_ID,
      missionId: MISSION_ID,
    });
    assert.equal(targets.length, 2);
    assert.equal(results.length, 2);
    assert.equal(db.companies.size, 2);
    assert.equal(db.prospects.size, 2);
  });

  it('16. buildMissionBoundCandidates does not ReferenceError on crmProspectId', () => {
    assert.doesNotThrow(() => {
      const candidates = buildMissionBoundCandidates(MISSION, strContributions());
      assert.ok(candidates.length >= 2);
      for (const row of candidates) {
        assert.ok(Object.prototype.hasOwnProperty.call(row, 'crmProspectId'));
        assert.ok(Object.prototype.hasOwnProperty.call(row, 'candidateId'));
        assert.ok(Object.prototype.hasOwnProperty.call(row, 'placeId'));
        assert.ok(Object.prototype.hasOwnProperty.call(row, 'crmCompanyId'));
      }
    });
  });

  it('17. admission returns explicit identity fields with crmProspectId defined (nullable)', async () => {
    const db = createMockDb();
    const candidate = buildMissionBoundCandidates(MISSION, strContributions())[0];
    const result = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    assert.equal(result.blocked, false);
    assert.ok(Object.prototype.hasOwnProperty.call(result, 'candidateId'));
    assert.ok(Object.prototype.hasOwnProperty.call(result, 'placeId'));
    assert.ok(Object.prototype.hasOwnProperty.call(result, 'crmCompanyId'));
    assert.ok(Object.prototype.hasOwnProperty.call(result, 'crmProspectId'));
    assert.ok(result.crmCompanyId);
    assert.ok(result.crmProspectId);
    assert.equal(result.crmCompanyId, result.companyId);
    assert.equal(result.crmProspectId, result.prospectId);
  });

  it('18. preserves existing CRM prospect UUID on admission', async () => {
    const db = createMockDb({
      companies: [{
        id: COMPANY_EXISTING,
        name: 'Blue Door Living Property Management',
        domain: 'bluedoorliving.com',
        google_place_id: PLACE_BLUE,
        client_id: 10,
      }],
      prospects: [{
        id: PROSPECT_EXISTING,
        company_id: COMPANY_EXISTING,
        client_id: 10,
        email: null,
        icp_score: 90,
        is_synthetic: false,
      }],
    });
    const candidate = buildMissionBoundCandidates(MISSION, strContributions())[0];
    const result = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    assert.equal(result.crmProspectId, PROSPECT_EXISTING);
    assert.equal(result.prospectId, PROSPECT_EXISTING);
    assert.equal(db.prospects.size, 1);
  });

  it('19. returns newly created placeholder prospect UUID when admission creates one', async () => {
    const db = createMockDb();
    const candidate = buildMissionBoundCandidates(MISSION, strContributions({ includeDomain: false }))[1];
    const result = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
    });
    assert.equal(result.blocked, false);
    assert.ok(result.crmProspectId);
    assert.equal(result.crmProspectId, result.prospectId);
    assert.equal(db.prospects.size, 1);
  });

  it('20. STR mission-bound admission inserts property_management vertical (not segment label)', async () => {
    const db = createMockDb();
    const candidate = buildMissionBoundCandidates(MISSION, strContributions())[0];
    assert.equal(candidate.vertical, 'short-term rental operators');

    const resolved = resolveMissionBoundCrmVertical(MISSION, candidate);
    assert.equal(resolved, 'property_management');
    assert.equal(isCanonicalBusinessVertical(resolved), true);

    const result = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
      mission: MISSION,
    });
    assert.equal(result.blocked, false);
    const prospect = db.prospects.get(String(result.crmProspectId));
    assert.equal(prospect.vertical, 'property_management');
    assert.equal(prospect.source, ADMISSION_SOURCE);
    assert.notEqual(prospect.vertical, candidate.vertical);
  });

  it('21. inserted vertical satisfies prospects_vertical_canonical_chk allow-list', () => {
    const allowed = new Set(CANONICAL_BUSINESS_VERTICALS.map((entry) => entry.value));
    const candidate = buildMissionBoundCandidates(MISSION, strContributions())[0];
    const vertical = resolveMissionBoundCrmVertical(MISSION, candidate);
    assert.ok(allowed.has(vertical));
  });

  it('22. existing prospect reuse preserves stored vertical without overwrite', async () => {
    const db = createMockDb({
      companies: [{
        id: COMPANY_EXISTING,
        name: 'Blue Door Living Property Management',
        domain: 'bluedoorliving.com',
        google_place_id: PLACE_BLUE,
        client_id: 10,
      }],
      prospects: [{
        id: PROSPECT_EXISTING,
        company_id: COMPANY_EXISTING,
        client_id: 10,
        email: null,
        icp_score: 90,
        vertical: 'commercial_cleaning',
        is_synthetic: false,
      }],
    });
    const candidate = buildMissionBoundCandidates(MISSION, strContributions())[0];
    const result = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
      mission: MISSION,
    });
    assert.equal(result.crmProspectId, PROSPECT_EXISTING);
    assert.equal(db.prospects.get(PROSPECT_EXISTING).vertical, 'commercial_cleaning');
    assert.equal(db.prospects.size, 1);
  });

  it('23. invalid free-form vertical blocks admission before DB write', async () => {
    const db = createMockDb();
    const candidate = {
      id: PLACE_BLUE,
      candidateId: PLACE_BLUE,
      placeId: PLACE_BLUE,
      company: 'Mystery Co',
      domain: 'mystery.example',
      vertical: 'founder_led_agencies',
    };
    const mission = {
      targetSegment: 'Founder-led agencies',
      structuredMission: { market: { label: 'Founder-led agencies', segment: 'founder_led_agencies' } },
    };
    assert.equal(resolveMissionBoundCrmVertical(mission, candidate), null);
    const result = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
      mission,
    });
    assert.equal(result.blocked, true);
    assert.equal(result.detail, 'non_canonical_vertical');
    assert.equal(db.prospects.size, 0);
    assert.equal(db.companies.size, 0);
  });

  it('24. repeat admission creates no duplicate prospect after vertical fix', async () => {
    const db = createMockDb();
    const candidate = buildMissionBoundCandidates(MISSION, strContributions())[0];
    const first = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
      mission: MISSION,
    });
    const second = await admitMissionBoundCandidate(db, candidate, {
      missionId: MISSION_ID,
      clientId: CLIENT_ID,
      mission: MISSION,
    });
    assert.equal(first.crmProspectId, second.crmProspectId);
    assert.equal(db.prospects.size, 1);
    assert.equal(db.prospects.get(String(first.crmProspectId)).source, ADMISSION_SOURCE);
  });

  it('25. mission-bound enrichment path proceeds after admission without send actions', async () => {
    const db = createMockDb();
    const contributions = strContributions();
    const candidates = buildMissionBoundCandidates(MISSION, contributions);
    assert.ok(candidates.length >= 1);

    const admission = await admitMissionBoundCandidates(db, MISSION, contributions, {
      clientId: CLIENT_ID,
      missionId: MISSION_ID,
    });
    const candidate = candidates[0];
    const admissionResult = admission.byCandidateId.get(String(candidate.id));
    assert.ok(admissionResult);
    assert.ok(admissionResult.crmProspectId);

    const crmRow = {
      prospect_id: admissionResult.crmProspectId,
      company_id: admissionResult.crmCompanyId,
      client_id: 10,
      company_name: candidate.company,
      domain: 'bluedoorliving.com',
      website: 'https://bluedoorliving.com',
      email: null,
      email_verified: false,
      email_status: null,
      do_not_contact: false,
    };
    const enriched = await enrichProspectRow(crmRow, {
      db,
      dryRun: true,
      configureScoringContext: async () => {},
      processProspect: async () => ({ selectedEmail: null, resolved: false, errors: [] }),
      runEnrichmentChain: async () => ({
        email: 'ops@bluedoorliving.com',
        source: ['prospeo'],
        contact: 'Ops Team',
      }),
      resolveEmailVerification: async () => ({
        emailVerified: true,
        emailStatus: 'valid',
        doNotContact: false,
        emailVerificationMethod: 'bouncer',
      }),
    });
    assert.equal(enriched.path, 'provider_chain');
    assert.equal(enriched.email, 'ops@bluedoorliving.com');
    assert.doesNotMatch(
      fs.readFileSync(path.join(__dirname, '..', 'scripts', 'lib', 'anchorMissionBoundEnrichment.js'), 'utf8'),
      /sendEmail\s*\(/
    );
  });
});
