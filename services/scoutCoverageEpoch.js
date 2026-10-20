'use strict';
const { createHash } = require('node:crypto');
const MAX_ROOTS = 54;
const validated = new WeakSet();
const configError = message => Object.assign(Error(message), { code: 'SCOUT_COVERAGE_EPOCH_CONFIG_INVALID' });
const SHA256 = /^[a-f0-9]{64}$/;
const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const scopeValid = (tenantId, fingerprint) => ['10', '13'].includes(String(tenantId)) && SHA256.test(fingerprint);
function allocation(rootCount) {
  return { maxInitialReacquisitions: rootCount, maxTotalPages: rootCount * 3,
    maxSearchHttpAttempts: rootCount * 3 * 3, maxDetailsHttpAttempts: rootCount * 3 * 20,
    maxTotalHttpAttempts: rootCount * 3 * 23 };
}
function projectCoverageApprovalManifest({ tenantId, fingerprint, rootQueryKeys, truncated = false } = {}) {
  if (!scopeValid(tenantId, fingerprint) || !Array.isArray(rootQueryKeys)
    || rootQueryKeys.some(key => typeof key !== 'string' || !SHA256.test(key))) return null;
  const keys = [...new Set(rootQueryKeys)].sort();
  if (!keys.length) return null;
  const selected = keys.slice(0, MAX_ROOTS);
  return { version: 1, tenantId: String(tenantId), fingerprint, rootQueryKeys: selected,
    observedOnly: true, legacyEligibilityRequiresRecheck: true, truncated: truncated === true || keys.length > MAX_ROOTS,
    maximumEpoch: 1, ...allocation(selected.length) };
}
function validateCoverageEpochConfigInner(value, { tenantId, fingerprint } = {}) {
  if (value && validated.has(value)) {
    if (value.tenantId !== String(tenantId) || value.fingerprint !== fingerprint) throw Error('Scout coverage epoch scope mismatch');
    return value;
  }
  if (value == null) return null;
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw Error('Invalid Scout coverage epoch manifest');
  const fields = ['version','epoch','approvalRef','tenantId','fingerprint','rootQueryKeys','notBefore','expiresAt'];
  if (Object.keys(value).some(key => !fields.includes(key)) || fields.some(key => !Object.hasOwn(value,key))) throw Error('Invalid Scout coverage epoch fields');
  if (value.version !== 1 || value.epoch !== 1 || !scopeValid(value.tenantId,value.fingerprint)
    || typeof value.tenantId !== 'string' || value.tenantId !== String(tenantId) || value.fingerprint !== fingerprint) throw Error('Scout coverage epoch scope mismatch');
  if (typeof value.approvalRef !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$/.test(value.approvalRef)) throw Error('Invalid Scout coverage approval reference');
  if (!Array.isArray(value.rootQueryKeys) || !value.rootQueryKeys.length || value.rootQueryKeys.length > MAX_ROOTS
    || value.rootQueryKeys.some(key => typeof key !== 'string' || !SHA256.test(key))
    || new Set(value.rootQueryKeys).size !== value.rootQueryKeys.length) throw Error('Invalid Scout coverage root allowlist');
  for (const key of ['notBefore','expiresAt']) {
    if (typeof value[key] !== 'string' || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/.test(value[key])
      || !Number.isFinite(Date.parse(value[key])) || new Date(value[key]).toISOString() !== value[key]) throw Error('Invalid Scout coverage approval window');
  }
  if (Date.parse(value.expiresAt) <= Date.parse(value.notBefore)) throw Error('Invalid Scout coverage approval window');
  const manifest = { version: 1, epoch: 1, approvalRef: value.approvalRef, tenantId: value.tenantId,
    fingerprint: value.fingerprint, rootQueryKeys: [...value.rootQueryKeys].sort(), notBefore: value.notBefore, expiresAt: value.expiresAt };
  const result = Object.freeze({ ...manifest, rootQueryKeys: Object.freeze(manifest.rootQueryKeys), manifestHash: digest(manifest),
    allocation: Object.freeze(allocation(manifest.rootQueryKeys.length)) });
  validated.add(result); return result;
}
function validateCoverageEpochConfig(value, scope) {
  if (value?.version === 2) return require('./scoutCoverageGrant').validateCoverageGrantConfig(value, scope);
  try { return validateCoverageEpochConfigInner(value, scope); }
  catch (error) { throw configError(error.message); }
}
function loadCoverageEpochConfig({ tenantId, fingerprint, env = process.env } = {}) {
  if (env.SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED !== 'true') return null;
  let parsed;
  try { parsed = JSON.parse(env.SCOUT_LEGACY_COVERAGE_EPOCH_JSON); }
  catch (_) { throw configError('Scout coverage epoch enabled without a valid manifest'); }
  if (parsed == null) throw configError('Scout coverage epoch enabled without a valid manifest');
  const result = validateCoverageEpochConfig(parsed, { tenantId: parsed?.tenantId, fingerprint: parsed?.fingerprint });
  return result.tenantId === String(tenantId) && result.fingerprint === fingerprint ? result : null;
}
function epochRootIdentity(rootKey) {
  if (!SHA256.test(rootKey)) throw Error('Invalid Scout legacy root identity');
  return digest(['places-approved-legacy-coverage-epoch-v1', rootKey, 1]);
}
function permissionStatus(config, rootKey, now = Date.now()) {
  if (!config || !config.rootQueryKeys.includes(rootKey)) return 'coverage_epoch_permission_required';
  if (now < Date.parse(config.notBefore)) return 'coverage_epoch_not_started';
  if (now >= Date.parse(config.expiresAt)) return 'coverage_epoch_expired';
  return null;
}
module.exports = { MAX_ROOTS, allocation, projectCoverageApprovalManifest, validateCoverageEpochConfig,
  loadCoverageEpochConfig, epochRootIdentity, permissionStatus };
