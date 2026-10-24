'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createHash } = require('node:crypto');
const { validateCoverageEpochConfig, loadCoverageEpochConfig, projectCoverageApprovalManifest,
  epochRootIdentity, permissionStatus } = require('../services/scoutCoverageEpoch');
const { createCheckpointStore, createCheckpointSession, queryIdentity } = require('../services/scoutQueryCheckpoints');
const key = text => createHash('sha256').update(text).digest('hex');
const scope = { tenantId: '13', fingerprint: key('unchanged mission') };
const manifest = () => ({ version: 1, epoch: 1, approvalRef: 'fixture-review-only', ...scope,
  rootQueryKeys: [key('legacy2'),key('legacy1')], notBefore: '2026-10-19T12:00:00.000Z', expiresAt: '2026-10-20T12:00:00.000Z' });
test('epoch is absent unless explicitly enabled and scoped, with strict tagged validation', () => {
  assert.equal(loadCoverageEpochConfig({ ...scope, env: {} }), null);
  assert.equal(loadCoverageEpochConfig({ ...scope, env: { SCOUT_LEGACY_COVERAGE_EPOCH_JSON: JSON.stringify(manifest()) } }), null);
  const enabled = value => ({ SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED: 'true', SCOUT_LEGACY_COVERAGE_EPOCH_JSON: value });
  for (const value of [undefined, '', 'null', '{}', '[]', '{malformed']) {
    assert.throws(() => loadCoverageEpochConfig({ ...scope, env: enabled(value) }), { code: 'SCOUT_COVERAGE_EPOCH_CONFIG_INVALID' });
  }
  const config = loadCoverageEpochConfig({ ...scope, env: enabled(JSON.stringify(manifest())) });
  assert.equal(config.epoch, 1); assert.equal(config.allocation.maxTotalPages, 6);
  assert.equal(config.allocation.maxTotalHttpAttempts, 138);
  assert.equal(loadCoverageEpochConfig({ tenantId: '10', fingerprint: scope.fingerprint, env: enabled(JSON.stringify(manifest())) }), null);
  assert.equal(loadCoverageEpochConfig({ ...scope, fingerprint: key('different mission'), env: enabled(JSON.stringify(manifest())) }), null);
  assert.throws(() => validateCoverageEpochConfig(config, { ...scope, tenantId: '10' }), { code: 'SCOUT_COVERAGE_EPOCH_CONFIG_INVALID' });
});
test('manifest is immutable and cannot request arbitrary epochs, root expansion or invalid windows', () => {
  for (const change of [ { epoch: 2 }, { epoch: '1' }, { version: 2 }, { tenantId: 13 }, { tenantId: '17' },
    { rootQueryKeys: [] }, { rootQueryKeys: [key('same'),key('same')] }, { rootQueryKeys: Array.from({length:55},(_,i)=>key(String(i))) },
    { rootQueryKeys: ['raw query or token'] }, { expiresAt: 'never' }, { notBefore: '2026-10-22T12:00:00.000Z' },
    { approvalRef: 'text with spaces' }, { reset: true } ]) {
    assert.throws(() => validateCoverageEpochConfig({ ...manifest(), ...change }, scope), { code: 'SCOUT_COVERAGE_EPOCH_CONFIG_INVALID' });
  }
  const a = validateCoverageEpochConfig(manifest(), scope);
  const b = validateCoverageEpochConfig({ ...manifest(), rootQueryKeys: [...manifest().rootQueryKeys].reverse() }, scope);
  assert.equal(a.manifestHash, b.manifestHash);
  assert.throws(() => a.rootQueryKeys.push(key('new')), TypeError);
  assert.notEqual(a.manifestHash, validateCoverageEpochConfig({ ...manifest(), approvalRef: 'different-review' },scope).manifestHash);
  assert.notEqual(epochRootIdentity(a.rootQueryKeys[0]), a.rootQueryKeys[0]);
  assert.equal(epochRootIdentity(a.rootQueryKeys[0]), epochRootIdentity(a.rootQueryKeys[0]));
  assert.equal(permissionStatus(a,a.rootQueryKeys[0],Date.parse(a.notBefore)-1),'coverage_epoch_not_started');
  assert.equal(permissionStatus(a,a.rootQueryKeys[0],Date.parse(a.notBefore)),null);
  assert.equal(permissionStatus(a,a.rootQueryKeys[0],Date.parse(a.expiresAt)),'coverage_epoch_expired');
  assert.equal(permissionStatus(a,key('unlisted'),Date.parse(a.notBefore)),'coverage_epoch_permission_required');
});
test('natural manifest projection is bounded hashes only and is not approval or complete-market evidence', () => {
  const projection = projectCoverageApprovalManifest({ ...scope, rootQueryKeys: Array.from({length:55},(_,i)=>key(String(i))) });
  assert.equal(projection.rootQueryKeys.length,54); assert.equal(projection.truncated,true);
  assert.equal(projection.observedOnly,true); assert.equal(projection.legacyEligibilityRequiresRecheck,true);
  assert.equal(projection.maxTotalPages,162); assert.equal(projection.maxTotalHttpAttempts,3726);
  assert.equal(projectCoverageApprovalManifest({ ...scope, rootQueryKeys:['private query'] }),null);
  assert.equal(projectCoverageApprovalManifest({ ...scope, fingerprint:'private mission',rootQueryKeys:[key('x')] }),null);
  assert.throws(() => validateCoverageEpochConfig(projection,scope),{code:'SCOUT_COVERAGE_EPOCH_CONFIG_INVALID'});
});
test('ordinary fetched legacy replay emits safe approval candidates without enabling any epoch', async () => {
  const query = 'Private provider query not logged'; const root = queryIdentity(query,false);
  const store = createCheckpointStore({ ...scope, db: { query: async sql => {
    assert.match(sql,/SELECT query_key, state, candidates/);
    return { rows:[{query_key:root,state:'fetched',candidates:[{companyName:'Private company'}]}] };
  } } });
  const session = createCheckpointSession({store});
  await session.lookup(query,false);
  const report = session.snapshot();
  assert.deepEqual(report.coverageApprovalManifest.rootQueryKeys,[root]);
  assert.equal(report.coverageApprovalManifest.fingerprint,scope.fingerprint);
  assert.ok(!JSON.stringify(report).includes('Private'));
  assert.equal(report.continuation,undefined);
});
