'use strict';
// Scout P0 recovery D: unit-level outcome routing. No database, provider or
// website access. A pass that may not investigate websites parks unclassifiable
// businesses; every other pass keeps its existing ticket outcome.
const test = require('node:test');
const assert = require('node:assert/strict');
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: {
  query: async () => { throw new Error('Unexpected shared database call'); },
} };
const { _test: { persistDiscoveredCompanies } } = require('../services/maxOutboundControlLoop');
const { REASONS } = require('../services/scoutCandidateTickets');

const unclassifiable = { name: 'Granite Works', website: 'https://granite-works.example', location: 'Manchester, NH',
  placeTypes: ['point_of_interest', 'establishment'] };
const school = { name: 'Synthetic Academy', website: 'https://synthetic-academy.example', location: 'Manchester, NH',
  placeTypes: ['school', 'point_of_interest'] };

async function outcomes(scoutContext, companies) {
  const finished = [];
  const ticketPass = { renew: async () => true,
    finish: async (_refs, outcome) => { finished.push(outcome); return { applied: true, result: { rowCount: 0 } }; } };
  const pool = { query: async () => ({ rows: [], rowCount: 0 }) };
  const store = { tenantId: '10', clientId: 10, candidateOwnership: async () => null };
  const receipt = await persistDiscoveredCompanies(pool, store, { tenantId: '10', companies: companies.map(row => ({ ...row, _scoutTicketRefs: [] })),
    scoutContext: { scope: { eligibleSubsegments: ['managed_facilities', 'restaurant_foh'] }, allowedCities: ['manchester'],
      ticketPass, ...scoutContext } });
  return { finished, receipt };
}

test('the parked reason is a valid blocked ticket outcome', () => {
  assert.ok(REASONS.blocked.includes('business_evidence_not_authorized'));
  assert.ok(!REASONS.deferred.includes('business_evidence_not_authorized'), 'never a zero-delay deferral');
});

test('research-only pass parks a business that needs website evidence, with evidence of why', async t => {
  const website = require.resolve('../services/scoutWebsiteBusinessEvidence');
  const original = require.cache[website];
  require.cache[website] = { id: website, filename: website, loaded: true,
    exports: { acquireBusinessEvidence: async () => assert.fail('no website request without authority') } };
  t.after(() => { if (original) require.cache[website] = original; else delete require.cache[website]; });
  const { finished, receipt } = await outcomes({ investigateBusinessEvidence: false, businessEvidenceNotAuthorized: true }, [unclassifiable]);
  assert.deepEqual(finished, [{ state: 'blocked', reason: 'business_evidence_not_authorized',
    evidence: { admissionReason: 'unclassifiable_vertical', websiteInvestigation: 'not_authorized' } }]);
  assert.equal(receipt.admission.rejected.unclassifiable_vertical, 1);
  assert.equal(receipt.inserted, 0);
});

test('eligibility is unchanged: a classifiable facility is still accepted in a research-only pass', async () => {
  const { finished, receipt } = await outcomes({ investigateBusinessEvidence: false, businessEvidenceNotAuthorized: true }, [school]);
  assert.deepEqual(finished.map(row => [row.state, row.reason]), [['handled', 'queued']]);
  assert.equal(receipt.admission.fit, 1);
});

test('other passes without website investigation keep their existing zero-delay deferral', async () => {
  // Bounded acquisition and backlog recovery run with investigateBusinessEvidence:false
  // and no research authority marker; their outcome is not changed here.
  const { finished } = await outcomes({ investigateBusinessEvidence: false }, [unclassifiable]);
  assert.deepEqual(finished, [{ state: 'deferred', reason: 'evidence_budget_deferred', retryAfterMs: 0 }]);
});
