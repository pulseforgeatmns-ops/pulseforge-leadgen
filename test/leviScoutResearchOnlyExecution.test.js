'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const execution = require('../services/leviScoutResearchOnlyExecution');

const criteria = { approved: true, source: 'approved_blueprint', minAmount: 5000, maxAmount: 20000 };
const authority = (extra = {}) => ({ version: 1, kind: 'levi_scout_research_only', tenantId: '21', cycleId: 'levi-week-2026-11-16',
  approvalRef: 'P0-SCOUT-LEVI-001', criteria, limits: { maxCandidates: 10, maxProviderRequests: 0 },
  permissions: { discover: true, persistReviewOnly: true, outreach: false, campaignEnrollment: false, modifySuppression: false }, ...extra });
const candidate = (extra = {}) => ({ name: 'Example Operator', job_title: 'Real-estate operator', email: 'operator@example.test',
  service_area_match: 'Akron, OH', do_not_contact: false,
  notes: 'Review state: contact now.\nFit evidence: The operator is under contract and needs $12,000 rehab funding. Exit plan: sale. Security position: first lien.\nSources: https://example.test/project',
  acquisition_metadata: { reviewState: 'contact_now', category: 'borrower', sourceUrls: ['https://example.test/project'], evidenceTimestamp: '2026-11-18T11:00:00Z' }, ...extra });

function deps(rows = [candidate()], existing = []) {
  const state = { discovered: 0, persisted: null, diagnostics: [] };
  return { state,
    claimRun: async () => ({ claimed: true }),
    discover: async () => { state.discovered += 1; return rows; },
    existingIdentityKeys: async () => existing,
    persistReviewOnly: async input => { state.persisted = input; return { persisted: input.records.length }; },
    recordDiagnostic: async value => state.diagnostics.push(value),
  };
}

test('disabled by default and paid providers require separate approval', async () => {
  await assert.rejects(execution.runResearchOnlyCycle({ authority: authority(), env: {}, deps: deps() }), { code: 'levi_research_disabled' });
  await assert.rejects(execution.runResearchOnlyCycle({ authority: authority({ limits: { maxCandidates: 10, maxProviderRequests: 1 } }),
    env: { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true' }, deps: deps() }), { code: 'levi_research_paid_provider_approval_required' });
});

test('tenant, approved Blueprint criteria and no-outreach permissions are immutable', () => {
  for (const invalid of [
    authority({ tenantId: '10' }), authority({ criteria: { minAmount: 5000, maxAmount: 20000 } }),
    authority({ permissions: { ...authority().permissions, outreach: true } }),
  ]) assert.throws(() => execution.validateAuthority(invalid), { code: 'levi_research_authority_invalid' });
});

test('controlled cycle persists review-only records and never exposes outreach authority', async () => {
  const d = deps();
  const result = await execution.runResearchOnlyCycle({ authority: authority(),
    env: { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true', EMMETT_AUTOSEND_ENABLED: 'true' }, deps: d });
  assert.equal(result.qualified, 1);
  assert.equal(result.outreachAttempted, false);
  assert.equal(d.state.persisted.tenantId, '21');
  assert.equal(d.state.persisted.records[0].outreachAuthorized, false);
  assert.equal(d.state.persisted.records[0].campaignEnrollmentAuthorized, false);
  assert.equal(d.state.persisted.records[0].setterVisible, false);
  assert.equal(d.state.persisted.records[0].aoOutreachReviewRequired, true);
});

test('existing and same-run identities are blocked from qualified production', async () => {
  const duplicate = candidate({ email: 'operator@example.test' });
  const d = deps([candidate(), duplicate], ['email:operator@example.test']);
  const result = await execution.runResearchOnlyCycle({ authority: authority(),
    env: { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true', EMMETT_AUTOSEND_ENABLED: 'false' }, deps: d });
  assert.equal(result.qualified, 0);
  assert.equal(result.duplicateBlocked, 2);
});

test('replayed cycle performs no discovery or persistence', async () => {
  const d = deps();
  d.claimRun = async () => ({ claimed: false });
  const result = await execution.runResearchOnlyCycle({ authority: authority(),
    env: { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true', EMMETT_AUTOSEND_ENABLED: 'false' }, deps: d });
  assert.equal(result.status, 'replay_blocked');
  assert.equal(d.state.discovered, 0);
  assert.equal(d.state.persisted, null);
});

test('incomplete candidates are Qualify First, not qualified', async () => {
  const d = deps([candidate({ email: null, phone: null, linkedin_url: null })]);
  const result = await execution.runResearchOnlyCycle({ authority: authority(),
    env: { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true', EMMETT_AUTOSEND_ENABLED: 'false' }, deps: d });
  assert.equal(result.qualified, 0);
  assert.equal(d.state.persisted.records[0].classification, 'Qualify First');
  assert.ok(d.state.persisted.records[0].blockers.includes('contact_path_missing'));
});

test('unverified Blueprint criteria still run discovery and never invent a range', async () => {
  const d = deps();
  const result = await execution.runResearchOnlyCycle({
    authority: authority({ criteria: { approved: false, source: 'approved_blueprint' } }),
    env: { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true' }, deps: d,
  });
  assert.equal(result.status, 'completed');
  assert.equal(result.discovered, 1);
  assert.equal(result.qualified, 0);
  assert.ok(d.state.persisted.records[0].blockers.includes('financing_criteria_unverified'));
  assert.throws(() => execution.validateAuthority(authority({ criteria: { approved: false, source: 'none', minAmount: 0, maxAmount: 0 } })),
    { code: 'levi_research_authority_invalid' });
});

test('a completed cycle never reports paid providers or outreach as used', async () => {
  const d = deps();
  const result = await execution.runResearchOnlyCycle({ authority: authority(),
    env: { SCOUT_LEVI_RESEARCH_ONLY_ENABLED: 'true' }, deps: d });
  assert.equal(result.outreachAttempted, false);
  assert.equal(result.paidProvidersUsed, false);
  assert.equal(d.state.persisted.records[0].outreachAuthorized, false);
  assert.equal(d.state.diagnostics[0].paidProvidersUsed, false);
});
