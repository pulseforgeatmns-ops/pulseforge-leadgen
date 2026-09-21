'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  run,
  parseArgs,
  DEFAULT_MISSION_ID,
  scriptRejectReason,
} = require('../scripts/auditAnchorCapacitySendability');
const {
  selectActiveCapacityContribution,
  isSupersededContribution,
} = require('../scripts/lib/activeCapacitySelection');
const { MESSAGE_BINDING_SCOPES } = require('../packages/acquisition-mission/types');

const MISSION_ID = DEFAULT_MISSION_ID;
const OLD_CAPACITY_ID = 'contrib_55e11312-3837-4485-b95a-58134dcd7601';
const NEW_CAPACITY_ID = 'contrib_01f70b44-183d-4ae3-bb1e-598c34ee0d88';

function boundQueueItem(email = 'partner@harborlaw.com') {
  return {
    prospectId: 'co-harbor',
    email,
    sendable: true,
    paige: {
      author: 'paige',
      source: 'paige',
      ready: true,
      candidateId: 'co-harbor',
      bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
      attributableIntelligence: { companyName: 'Harbor Law' },
    },
  };
}

function capacityPayload(items) {
  return {
    id: NEW_CAPACITY_ID,
    specialist: 'emmett',
    kind: 'capacity',
    payload: {
      capacity: { recommended: items.length },
      queue: { items },
      governor: { outcome: 'proceed' },
    },
  };
}

function mockPool({ missionBody, capacityRows, missionBoundContacts = [] }) {
  return {
    query(sql, params) {
      if (sql.includes('FROM acquisition_missions')) {
        return {
          rows: [{
            mission_id: MISSION_ID,
            payload: missionBody,
            updated_at: '2026-09-13T22:00:00.000Z',
          }],
        };
      }
      if (sql.includes('FROM acquisition_mission_contributions')) {
        return { rows: capacityRows };
      }
      if (sql.includes('FROM prospects p') && sql.includes('company_id::text = $2')) {
        const missionBoundKey = params[1];
        const row = missionBoundContacts.find(
          (p) => String(p.company_id) === String(missionBoundKey)
            || String(p.prospect_id || p.id) === String(missionBoundKey)
        );
        return { rows: row ? [row] : [] };
      }
      throw new Error(`Unexpected query: ${sql}`);
    },
  };
}

describe('auditAnchorCapacitySendability', () => {
  it('refuses without --confirm-production', () => {
    assert.throws(
      () => parseArgs([]),
      (err) => err.code === 'confirm_production_required'
    );
  });

  it('selects active non-superseded CAPACITY instead of a hardcoded superseded id', async () => {
    const staleCapacity = {
      capacity_id: OLD_CAPACITY_ID,
      at: '2026-09-13T23:00:00.000Z',
      payload: {
        id: OLD_CAPACITY_ID,
        specialist: 'emmett',
        kind: 'capacity',
        payload: {
          superseded: true,
          supersededBy: NEW_CAPACITY_ID,
          queue: { items: [{ prospectId: 'co-harbor', email: null, sendable: true }] },
        },
      },
    };
    const freshCapacity = {
      capacity_id: NEW_CAPACITY_ID,
      at: '2026-09-13T22:30:00.000Z',
      payload: capacityPayload([boundQueueItem()]),
    };

    assert.equal(isSupersededContribution(staleCapacity), true);
    assert.equal(isSupersededContribution(freshCapacity), false);

    const selected = selectActiveCapacityContribution({
      revisionState: { emmettContributionId: NEW_CAPACITY_ID },
    }, [staleCapacity, freshCapacity]);
    assert.equal(selected.capacity_id, NEW_CAPACITY_ID);

    const report = await run({
      confirmProduction: true,
      missionId: MISSION_ID,
      pool: mockPool({
        missionBody: {
          objective: 'Anchor law firms',
          revisionState: { emmettContributionId: NEW_CAPACITY_ID },
        },
        capacityRows: [staleCapacity, freshCapacity],
        missionBoundContacts: [{
          prospect_id: '7adbb294-b94c-45c0-85df-e040f027ece0',
          company_id: 'co-harbor',
          email: 'partner@harborlaw.com',
          email_status: 'verified',
          email_verified: true,
          do_not_contact: false,
          enrichment_provenance: { email: { source: 'provider_chain' } },
          company_name: 'Harbor Law',
        }],
      }),
    });

    assert.equal(report.capacityContributionId, NEW_CAPACITY_ID);
    assert.notEqual(report.capacityContributionId, OLD_CAPACITY_ID);
    assert.ok(report.sendableCount >= 1);
    assert.equal(report.firstBlocker, null);
  });

  it('prefers mission revision pointer over newest-at superseded row', async () => {
    const staleCapacity = {
      capacity_id: OLD_CAPACITY_ID,
      at: '2026-09-13T23:30:00.000Z',
      payload: {
        id: OLD_CAPACITY_ID,
        payload: {
          superseded: true,
          queue: { items: [] },
        },
      },
    };
    const freshCapacity = {
      capacity_id: NEW_CAPACITY_ID,
      at: '2026-09-13T22:00:00.000Z',
      payload: capacityPayload([boundQueueItem('ops@granitelegal.com')]),
    };

    const report = await run({
      confirmProduction: true,
      missionId: MISSION_ID,
      pool: mockPool({
        missionBody: {
          objective: 'Anchor law firms',
          revisionState: { emmettContributionId: NEW_CAPACITY_ID },
        },
        capacityRows: [staleCapacity, freshCapacity],
      }),
    });

    assert.equal(report.capacityContributionId, NEW_CAPACITY_ID);
  });

  it('scriptRejectReason matches executeAnchorOneOutbound sendable predicate', () => {
    assert.equal(scriptRejectReason({ email: 'a@b.com', sendable: true }), null);
    assert.equal(scriptRejectReason({ email: '', sendable: true }), 'missing_recipient_email_on_queue_item');
    assert.equal(scriptRejectReason({ email: 'a@b.com', sendable: false }), 'item.sendable=false');
    assert.equal(scriptRejectReason({ email: 'a@b.com', dnc: true }), 'item.dnc=true');
  });

  it('resolves CRM email by mission-bound company key, not prospects.id', async () => {
    const BACKUS_COMPANY = '001c9b7e-5659-4a54-892c-05493a148f9b';
    const BACKUS_CONTACT = '7adbb294-b94c-45c0-85df-e040f027ece0';
    const freshCapacity = {
      capacity_id: NEW_CAPACITY_ID,
      at: '2026-09-13T22:30:00.000Z',
      payload: capacityPayload([
        {
          prospectId: BACKUS_COMPANY,
          company: 'Backus Meyer',
          email: null,
          sendable: true,
          paige: {
            author: 'paige',
            source: 'paige',
            ready: true,
            candidateId: BACKUS_COMPANY,
            bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
            attributableIntelligence: { companyName: 'Backus Meyer' },
          },
        },
      ]),
    };

    const report = await run({
      confirmProduction: true,
      missionId: MISSION_ID,
      pool: mockPool({
        missionBody: {
          objective: 'Anchor law firms',
          revisionState: { emmettContributionId: NEW_CAPACITY_ID },
        },
        capacityRows: [freshCapacity],
        missionBoundContacts: [{
          prospect_id: BACKUS_CONTACT,
          company_id: BACKUS_COMPANY,
          email: 'jmeyer@backusmeyer.com',
          email_status: 'verified',
          email_verified: true,
          do_not_contact: false,
          enrichment_provenance: { email: { source: 'provider_chain', verifier: 'bouncer' } },
        }],
      }),
    });

    const backus = report.queueItems[0];
    assert.equal(backus.missionBoundKey, BACKUS_COMPANY);
    assert.equal(backus.crmContactId, BACKUS_CONTACT);
    assert.equal(backus.crmEmailPresent, true);
    assert.equal(backus.crmProjectable, true);
    assert.equal(backus.projectionBlockReason, null);
    assert.equal(report.upstreamVerifiedEmailExists, true);
  });

  it('classifies blocked recipients with correct projection reasons', async () => {
    const companies = {
      klug: '31bcc7e2-fe86-4208-9525-3c97abe8ecd7',
      solomon: '585d9f94-ab88-4535-9337-82e70cf38750',
      stLouis: 'b04466e1-a4ff-41aa-9e19-eaf3011a4f2c',
      deliverability: '392b9b51-e202-4700-8c34-5feeee952bf0',
      backus: '001c9b7e-5659-4a54-892c-05493a148f9b',
    };

    const queueItems = [
      { key: companies.klug, company: 'Klug Law' },
      { key: companies.solomon, company: 'Solomon Law' },
      { key: companies.stLouis, company: 'St. Louis Law' },
      { key: companies.deliverability, company: 'Deliverability Test Firm' },
      { key: companies.backus, company: 'Backus Meyer' },
    ].map(({ key, company }) => ({
      prospectId: key,
      company,
      email: null,
      sendable: true,
      paige: {
        author: 'paige',
        source: 'paige',
        ready: true,
        candidateId: key,
        bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
        attributableIntelligence: { companyName: company },
      },
    }));

    const report = await run({
      confirmProduction: true,
      missionId: MISSION_ID,
      pool: mockPool({
        missionBody: { revisionState: { emmettContributionId: NEW_CAPACITY_ID } },
        capacityRows: [{
          capacity_id: NEW_CAPACITY_ID,
          at: '2026-09-13T22:30:00.000Z',
          payload: capacityPayload(queueItems),
        }],
        missionBoundContacts: [
          {
            prospect_id: '11111111-1111-4111-8111-111111111111',
            company_id: companies.klug,
            email: 'contact@kluglaw.com',
            email_status: 'verified',
            email_verified: true,
            do_not_contact: true,
            enrichment_provenance: { email: { source: 'provider_chain' } },
          },
          {
            prospect_id: '22222222-2222-4222-8222-222222222222',
            company_id: companies.solomon,
            email: 'info@solomonlaw.com',
            email_status: 'verified',
            email_verified: true,
            do_not_contact: false,
            enrichment_provenance: { email: { source: 'pattern_first' } },
          },
          {
            prospect_id: '33333333-3333-4333-8333-333333333333',
            company_id: companies.stLouis,
            email: 'bad@linkedin.com',
            email_status: 'quarantined',
            email_verified: false,
            do_not_contact: false,
            enrichment_provenance: { email: { source: 'website_scrape' } },
          },
          {
            prospect_id: '44444444-4444-4444-8444-444444444444',
            company_id: companies.deliverability,
            email: 'test@example.com',
            email_status: 'verified',
            email_verified: true,
            do_not_contact: false,
            enrichment_provenance: { email: { source: 'provider_chain' } },
          },
          {
            prospect_id: '7adbb294-b94c-45c0-85df-e040f027ece0',
            company_id: companies.backus,
            email: 'jmeyer@backusmeyer.com',
            email_status: 'verified',
            email_verified: true,
            do_not_contact: false,
            enrichment_provenance: { email: { source: 'provider_chain', verifier: 'bouncer' } },
          },
        ],
      }),
    });

    const byCompany = Object.fromEntries(report.queueItems.map((row) => [row.company, row]));
    assert.equal(byCompany['Klug Law'].crmEmailPresent, true);
    assert.equal(byCompany['Klug Law'].crmProjectable, false);
    assert.equal(byCompany['Klug Law'].projectionBlockReason, 'do_not_contact');
    assert.equal(byCompany['Solomon Law'].crmEmailPresent, true);
    assert.equal(byCompany['Solomon Law'].crmProjectable, false);
    assert.equal(byCompany['Solomon Law'].projectionBlockReason, 'inferred_pattern_provenance');
    assert.equal(byCompany['St. Louis Law'].crmProjectable, false);
    assert.ok(['email_not_verified', 'contaminated_email_domain', 'email_status_not_verified']
      .includes(byCompany['St. Louis Law'].projectionBlockReason));
    assert.equal(byCompany['Deliverability Test Firm'].deliverabilityTestExcluded, true);
    assert.equal(byCompany['Deliverability Test Firm'].projectionBlockReason, 'deliverability_test_excluded');
    assert.equal(byCompany['Backus Meyer'].crmEmailPresent, true);
    assert.equal(byCompany['Backus Meyer'].crmProjectable, true);
    assert.equal(byCompany['Backus Meyer'].projectionBlockReason, null);
  });
});
