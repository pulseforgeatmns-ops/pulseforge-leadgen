'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  applyTaintedCrmEmailRemediation,
  remediateTaintedCrmEmail,
} = require('../utils/crmEmailProvenance');
const { TAINTED_EMAIL_ACTIONS } = require('../utils/canonicalEmailEligibility');
const { snapshotRow, AUDIT_CONTACTS } = require('../utils/crmEmailProvenance');
const { isProjectableCrmProspect } = require('../packages/max/workspace/MissionBoundCrmResolver');

describe('crmEmailProvenance remediator', () => {
  it('preserves inferred pattern_first email and stamps original provenance', async () => {
    const queries = [];
    const db = {
      query: async (sql, params) => {
        queries.push({ sql, params });
        return { rows: [] };
      },
    };
    const row = {
      prospect_id: 'cde8f588-6969-47c3-8219-3f539b2b23cc',
      client_id: 10,
      email: 'peter@solomonlawfirm.com',
      email_verified: true,
      email_status: 'valid',
      do_not_contact: false,
      enrichment_provenance: { email: { source: 'pattern_first' } },
    };
    const result = await remediateTaintedCrmEmail(db, row, { dryRun: false });
    assert.equal(result.applied, true);
    assert.equal(result.plan.action, TAINTED_EMAIL_ACTIONS.PRESERVE_UNTRUSTED_PROVENANCE);
    assert.equal(result.email, 'peter@solomonlawfirm.com');
    const update = queries.find(({ sql }) => /UPDATE prospects/i.test(sql));
    assert.ok(update);
    assert.match(update.sql, /enrichment_provenance/);
    assert.doesNotMatch(update.sql, /email = NULL/);
    const provenance = JSON.parse(update.params[0]);
    assert.equal(provenance.email.original_source, 'pattern_first');
    assert.equal(provenance.email.outbound_eligible, false);
    assert.equal(isProjectableCrmProspect({
      ...row,
      verificationSource: 'existing_crm',
      enrichment_provenance: provenance,
    }), false);
  });

  it('invalidates contaminated social-domain email without deleting the evidence', async () => {
    const queries = [];
    const db = {
      query: async (sql, params) => {
        queries.push({ sql, params });
        return { rows: [] };
      },
    };
    const row = {
      prospect_id: '5418dbf1-0ed1-4dab-854b-df9a4dfcf8d3',
      client_id: 10,
      email: 'michael@linkedin.com',
      email_verified: true,
      email_status: 'valid',
      notes: null,
      do_not_contact: false,
      enrichment_provenance: { email: { source: 'pattern_first' } },
    };
    const result = await applyTaintedCrmEmailRemediation(db, row, {
      action: TAINTED_EMAIL_ACTIONS.INVALIDATE_CONTAMINATED,
      reason: 'contaminated_email_domain',
      email: 'michael@linkedin.com',
      provenance: 'pattern_first',
    }, { dryRun: false });
    assert.equal(result.applied, true);
    assert.equal(result.email, null);
    assert.equal(result.quarantinedEmail, 'michael@linkedin.com');
    const update = queries.find(({ sql }) => /UPDATE prospects/i.test(sql));
    assert.match(update.sql, /email = NULL/);
    assert.match(update.sql, /email_status = 'quarantined'/);
    assert.match(update.params[0], /QUARANTINED:contaminated_email_domain/);
    const provenance = JSON.parse(update.params[1]);
    assert.equal(provenance.email.quarantined_email, 'michael@linkedin.com');
    assert.equal(provenance.email.invalidated, true);
  });

  it('does not mutate a legitimate existing CRM address such as Backus', async () => {
    const queries = [];
    const db = {
      query: async (sql) => {
        queries.push(sql);
        return { rows: [] };
      },
    };
    const result = await remediateTaintedCrmEmail(db, {
      prospect_id: '7adbb294-b94c-45c0-85df-e040f027ece0',
      client_id: 10,
      email: 'jmeyer@backusmeyer.com',
      email_verified: true,
      email_status: 'valid',
      do_not_contact: false,
    }, { dryRun: false });
    assert.equal(result.applied, false);
    assert.equal(result.plan.action, TAINTED_EMAIL_ACTIONS.NONE);
    assert.equal(queries.length, 0);
  });

  it('snapshots stored provenance separately from the existing_crm read-path label', () => {
    const snap = snapshotRow({
      prospect_id: AUDIT_CONTACTS[0].prospectId,
      company_name: 'Solomon Law Firm',
      email: 'peter@solomonlawfirm.com',
      email_status: 'valid',
      email_verified: true,
      do_not_contact: false,
      verificationSource: 'existing_crm',
      enrichment_provenance: { email: { source: 'pattern_first', original_source: 'pattern_first' } },
    });
    assert.equal(snap.storedProvenance, 'pattern_first');
    assert.equal(snap.projectable, false);
    assert.equal(snap.ineligibility, 'inferred_pattern_provenance');
  });
});
