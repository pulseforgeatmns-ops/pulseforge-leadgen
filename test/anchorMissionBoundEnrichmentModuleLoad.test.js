'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');

const LIB_PATH = path.join(__dirname, '..', 'scripts', 'lib', 'anchorMissionBoundEnrichment.js');
const RUNNER_PATH = path.join(__dirname, '..', 'scripts', 'enrichAnchorMissionBoundContacts.js');
const libSource = fs.readFileSync(LIB_PATH, 'utf8');
const runnerSource = fs.readFileSync(RUNNER_PATH, 'utf8');

const STALE_SYMBOLS = Object.freeze([
  'loadProspectRowsByIds',
  'listMissionBoundProspectIds',
  'listProspectIdsFromCapacityPayload',
  'loadActiveCapacityForMission',
  'normalizeProspectIds',
  'PROSPECT_ENRICHMENT_SELECT',
]);

const CANONICAL_SYMBOLS = Object.freeze([
  'listMissionBoundCompanyIds',
  'loadCrmProspectsForMissionBoundCompanies',
  'loadBestCrmProspectForMissionBoundKey',
]);

function exportBlock(source) {
  const match = source.match(/module\.exports\s*=\s*\{([\s\S]*?)\};/);
  assert.ok(match, 'expected module.exports block');
  return match[1];
}

describe('anchor mission-bound enrichment module load regression', () => {
  it('loads anchorMissionBoundEnrichment.js without stale export references', () => {
    const lib = require('../scripts/lib/anchorMissionBoundEnrichment');
    assert.equal(typeof lib.loadMissionBoundProspects, 'function');
    assert.equal(typeof lib.loadProspectRow, 'function');
    assert.equal(typeof lib.enrichProspectRow, 'function');
    assert.equal(lib.loadProspectRowsByIds, undefined);
  });

  it('loads enrichAnchorMissionBoundContacts.js', () => {
    const runner = require('../scripts/enrichAnchorMissionBoundContacts');
    assert.equal(typeof runner.run, 'function');
    assert.equal(typeof runner.parseArgs, 'function');
    assert.equal(runner.DEFAULT_MISSION_ID, 'mission_ad7753b0-6def-441d-bb1a-3764656f5750');
  });

  it('keeps only canonical post-#585 identity symbols in the enrichment lib source', () => {
    for (const symbol of STALE_SYMBOLS) {
      assert.doesNotMatch(exportBlock(libSource), new RegExp(`\\b${symbol}\\b`));
      if (symbol !== 'PROSPECT_ENRICHMENT_SELECT') {
        assert.doesNotMatch(libSource, new RegExp(`\\bfunction ${symbol}\\b`));
      }
    }
    for (const symbol of CANONICAL_SYMBOLS) {
      assert.match(libSource, new RegExp(`\\b${symbol}\\b`));
    }
    assert.match(runnerSource, /rowsByCompanyId/);
    assert.match(runnerSource, /missionBoundCompanyIds/);
  });

  it('module.exports identifiers are all defined at load time', () => {
    const exportNames = [...exportBlock(libSource).matchAll(/^\s*(\w+)\s*,?\s*$/gm)]
      .map((match) => match[1])
      .filter(Boolean);
    assert.ok(exportNames.length > 0);
    assert.equal(exportNames.includes('loadProspectRowsByIds'), false);

    const lib = require('../scripts/lib/anchorMissionBoundEnrichment');
    for (const name of exportNames) {
      assert.notEqual(lib[name], undefined, `${name} export must be defined`);
    }
  });

  it('loadProspectRow resolves a company UUID to the best CRM contact', async () => {
    const COMPANY_KLUG = '31bcc7e2-1111-4111-8111-111111111111';
    const CONTACT_KLUG = 'a1111111-3333-4333-8333-333333333333';
    const CONTACT_ALT = 'c3333333-5555-4555-8555-555555555555';

    const db = {
      query: async (sql, params) => {
        assert.match(sql, /p\.company_id::text = \$2/);
        assert.equal(params[1], COMPANY_KLUG);
        return {
          rows: [{
            prospect_id: CONTACT_KLUG,
            company_id: COMPANY_KLUG,
            client_id: 10,
            company_name: 'Klug Law Offices, PLLC',
            icp_score: 88,
          }],
        };
      },
    };

    const { loadProspectRow } = require('../scripts/lib/anchorMissionBoundEnrichment');
    const row = await loadProspectRow(db, 10, COMPANY_KLUG);
    assert.equal(String(row.prospect_id), CONTACT_KLUG);
    assert.notEqual(String(row.prospect_id), CONTACT_ALT);
    assert.equal(String(row.company_id), COMPANY_KLUG);
  });
});
