'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const {
  parseArgs,
  RAILWAY_COMMAND,
  DEFAULT_MISSION_ID,
  isEligibleForCapacityProjection,
  formatContactLine,
  printReport,
  missingCrmResult,
  run,
} = require('../scripts/enrichAnchorMissionBoundContacts');

const SCRIPT = path.join(__dirname, '..', 'scripts', 'enrichAnchorMissionBoundContacts.js');
const source = fs.readFileSync(SCRIPT, 'utf8');

describe('enrichAnchorMissionBoundContacts', () => {
  it('requires --confirm-production and defaults to the Anchor mission', () => {
    assert.equal(parseArgs([]).confirmProduction, false);
    assert.equal(parseArgs([]).missionId, DEFAULT_MISSION_ID);
    assert.equal(parseArgs(['--confirm-production']).confirmProduction, true);
    assert.equal(
      parseArgs(['--confirm-production', '--mission-id', 'mission_other']).missionId,
      'mission_other'
    );
  });

  it('refuses without --confirm-production', async () => {
    await assert.rejects(
      () => run({ confirmProduction: false, print: false }),
      (err) => err.code === 'confirm_production_required'
    );
  });

  it('does not send mail, revise CAPACITY, enable autosend, or change enabled_agents', () => {
    assert.doesNotMatch(source, /regenerateAnchorCapacityRevision/);
    assert.doesNotMatch(source, /REVISE_PREPARED_OUTREACH/);
    assert.doesNotMatch(source, /GENERATE_CAPACITY/);
    assert.doesNotMatch(source, /executeAnchorOneOutbound/);
    assert.doesNotMatch(source, /EXECUTE_OUTBOUND/);
    assert.doesNotMatch(source, /APPROVE_EXECUTION/);
    assert.doesNotMatch(source, /routeExecutionRequest/);
    assert.doesNotMatch(source, /api\.brevo\.com\/v3\/smtp/);
    assert.doesNotMatch(source, /sendEmail\s*\(/);
    assert.doesNotMatch(source, /UPDATE\s+clients/i);
    assert.doesNotMatch(source, /SET\s+autosend_enabled/i);
    assert.doesNotMatch(source, /array_append\(\s*enabled_agents/i);
    assert.match(source, /Never regenerates CAPACITY/);
    assert.match(source, /isProjectableCrmProspect/);
    assert.match(source, /invalidOutreachEmailReason/);
  });

  it('excludes Deliverability Test from CAPACITY eligibility even with a verified email', () => {
    assert.equal(isEligibleForCapacityProjection({
      prospectId: 'p-test',
      company: 'Deliverability Test',
      excluded: true,
      verified: true,
      email: 'test@example.com',
      emailStatus: 'valid',
      dnc: false,
    }), false);
    assert.equal(isEligibleForCapacityProjection({
      prospectId: 'p-klug',
      company: 'Klug Law Offices, PLLC',
      excluded: false,
      verified: true,
      email: 'partner@kluglaw.com',
      emailStatus: 'verified',
      dnc: false,
    }), true);
    assert.equal(isEligibleForCapacityProjection({
      prospectId: 'p-klug',
      company: 'Klug Law Offices, PLLC',
      excluded: false,
      verified: true,
      email: 'partner@kluglaw.com',
      emailStatus: 'verified',
      dnc: true,
    }), false);
  });

  it('prints prospect, company, email, verification, persist, DNC, and eligibility count', () => {
    const text = printReport({
      tenantId: '10',
      missionId: DEFAULT_MISSION_ID,
      dryRun: false,
      eligibleForCapacityProjection: 1,
      contacts: [{
        prospectId: 'p-klug',
        company: 'Klug Law Offices, PLLC',
        email: 'partner@kluglaw.com',
        emailStatus: 'verified',
        verificationSource: 'prospeo',
        persisted: true,
        path: 'provider_chain',
        verified: true,
        excluded: false,
        dnc: false,
      }, {
        prospectId: 'p-test',
        company: 'Deliverability Test',
        email: null,
        emailStatus: null,
        verificationSource: null,
        persisted: false,
        path: null,
        verified: false,
        excluded: true,
        dnc: false,
      }],
    });
    assert.match(text, /Prospect ID: p-klug/);
    assert.match(text, /Company: Klug Law Offices, PLLC/);
    assert.match(text, /Discovered email: partner@kluglaw\.com/);
    assert.match(text, /Verification status\/source: verified \/ prospeo/);
    assert.match(text, /Persisted to CRM: yes/);
    assert.match(text, /DNC state: false/);
    assert.match(text, /Count of mission-bound prospects now eligible for CAPACITY projection: 1/);
    assert.match(text, /Deliverability Test/);
    assert.match(text, /regenerateAnchorCapacityRevision\.js/);
    assert.match(text, /enrichAnchorMissionBoundContacts\.js --confirm-production/);
    assert.equal(RAILWAY_COMMAND.includes(DEFAULT_MISSION_ID), true);
  });

  it('does not treat existing CRM emails as newly persisted', () => {
    const line = formatContactLine({
      prospectId: 'p-1',
      company: 'Solomon Law Firm',
      email: 'a@solomon.example',
      emailStatus: 'valid',
      verificationSource: 'existing_crm',
      persisted: true,
      path: 'existing_crm',
      verified: true,
      excluded: false,
      dnc: false,
    });
    assert.match(line, /Persisted to CRM: no/);
    assert.match(line, /Eligible for CAPACITY projection: yes/);
  });

  it('reports mission-bound IDs missing from CRM without inventing email', () => {
    const row = missingCrmResult('missing-1');
    assert.equal(row.email, null);
    assert.equal(row.persisted, false);
    assert.equal(row.reason, 'not_found_in_crm');
    assert.equal(isEligibleForCapacityProjection(row), false);
  });
});
