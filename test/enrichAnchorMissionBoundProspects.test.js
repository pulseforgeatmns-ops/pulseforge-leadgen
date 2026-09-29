'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const {
  isExcludedCompany,
  enrichProspectRow,
  EXCLUDED_COMPANY_RE,
} = require('../scripts/enrichAnchorMissionBoundProspects');

const SCRIPT = path.join(__dirname, '..', 'scripts', 'enrichAnchorMissionBoundProspects.js');
const LIB = path.join(__dirname, '..', 'scripts', 'lib', 'anchorMissionBoundEnrichment.js');

describe('enrichAnchorMissionBoundProspects', () => {
  it('loads the runner module without duplicate top-level helper declarations', () => {
    const scriptSource = fs.readFileSync(SCRIPT, 'utf8');
    const libSource = fs.readFileSync(LIB, 'utf8');
    assert.doesNotThrow(() => require('../scripts/enrichAnchorMissionBoundProspects'));
    assert.equal((scriptSource.match(/function isExcludedCompany/g) || []).length, 0);
    assert.equal((libSource.match(/function isExcludedCompany/g) || []).length, 1);
    assert.match(scriptSource, /require\('\.\/lib\/anchorMissionBoundEnrichment'\)/);
  });

  it('excludes Deliverability Test from enrichment', () => {
    assert.match('Deliverability Test', EXCLUDED_COMPANY_RE);
    assert.equal(isExcludedCompany('Deliverability Test'), true);
    assert.equal(isExcludedCompany('Klug Law Offices, PLLC'), false);
  });

  it('returns existing projectable CRM email without re-enriching', async () => {
    const row = {
      prospect_id: 'p-1',
      client_id: 10,
      company_name: 'Backus, Meyer & Branch',
      email: 'jmeyer@backusmeyer.com',
      email_verified: true,
      email_status: 'verified',
      do_not_contact: false,
    };
    const result = await enrichProspectRow(row, { db: {}, dryRun: true });
    assert.equal(result.excluded, false);
    assert.equal(result.verified, true);
    assert.equal(result.path, 'existing_crm');
    assert.equal(result.email, 'jmeyer@backusmeyer.com');
    assert.equal(result.verificationSource, 'existing_crm');
  });

  it('keeps DNC Klug ineligible even with a verified CRM email', async () => {
    const result = await enrichProspectRow({
      prospect_id: 'p-klug',
      client_id: 10,
      company_name: 'Klug Law Offices, PLLC',
      email: 'aklug@kluglawoffices.com',
      email_verified: true,
      email_status: 'verified',
      do_not_contact: true,
      enrichment_provenance: { email: { source: 'hunter' } },
    }, {
      db: {},
      dryRun: true,
      processProspect: async () => ({ selectedEmail: null, resolved: false, errors: [] }),
      runEnrichmentChain: async () => null,
    });
    assert.equal(result.dnc, true);
    assert.equal(result.verified, false);
    assert.notEqual(result.path, 'existing_crm');
  });

  it('marks Deliverability Test as excluded without calling providers', async () => {
    const row = {
      prospect_id: 'p-test',
      client_id: 10,
      company_name: 'Deliverability Test',
      email: null,
      email_verified: false,
      email_status: null,
      do_not_contact: false,
    };
    const result = await enrichProspectRow(row, { db: {}, dryRun: true });
    assert.equal(result.excluded, true);
    assert.equal(result.reason, 'deliverability_test_excluded');
    assert.equal(result.verified, false);
  });
});
