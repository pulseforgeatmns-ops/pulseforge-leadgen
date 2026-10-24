'use strict';

const { createHash } = require('node:crypto');

const MAX_ROOTS = 54;
const MAX_PAGES_PER_ROOT = 3;
const ERROR_CODE = 'SCOUT_COVERAGE_EPOCH_CONFIG_INVALID';
// The final negative lookahead requires the true end, not a trailing line break.
const SHA256 = /^[a-f0-9]{64}(?![\s\S])/;
const APPROVAL_REF = /^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}(?![\s\S])/;
const TIMESTAMP = /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/;
const FIELDS = ['version', 'epoch', 'approvalRef', 'tenantId', 'fingerprint',
  'rootQueryKeys', 'notBefore', 'expiresAt', 'maxNewPages'];
const validated = new WeakSet();
const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const configError = message => Object.assign(new Error(message), { code: ERROR_CODE });

// A grant authorizes a bounded run. It never creates a fresh cumulative allocation.
const ALLOCATION = Object.freeze({
  maxInitialReacquisitions: MAX_ROOTS,
  maxTotalPages: MAX_ROOTS * MAX_PAGES_PER_ROOT,
  maxSearchHttpAttempts: MAX_ROOTS * MAX_PAGES_PER_ROOT * 3,
  maxDetailsHttpAttempts: MAX_ROOTS * MAX_PAGES_PER_ROOT * 20,
  maxTotalHttpAttempts: MAX_ROOTS * MAX_PAGES_PER_ROOT * 23,
});

function normalizeTenantId(tenantId) {
  if ((typeof tenantId !== 'string' && typeof tenantId !== 'number')
    || !['10', '13'].includes(String(tenantId))) return null;
  return String(tenantId);
}

function allocationIdentity(tenantId, fingerprint) {
  const tenant = normalizeTenantId(tenantId);
  if (!tenant || typeof fingerprint !== 'string' || !SHA256.test(fingerprint)) {
    throw configError('Invalid Scout coverage allocation scope');
  }
  return digest(['places-coverage-allocation-v2', tenant, fingerprint, 1, MAX_ROOTS, MAX_PAGES_PER_ROOT]);
}

function readManifest(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
    || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) {
    throw configError('Invalid Scout coverage grant manifest');
  }
  const descriptors = Object.getOwnPropertyDescriptors(value);
  const keys = Reflect.ownKeys(descriptors);
  if (keys.length !== FIELDS.length || keys.some(key => !FIELDS.includes(key))
    || FIELDS.some(key => !Object.hasOwn(descriptors, key)
      || !Object.hasOwn(descriptors[key], 'value') || !descriptors[key].enumerable)) {
    throw configError('Invalid Scout coverage grant fields');
  }
  return Object.fromEntries(FIELDS.map(key => [key, descriptors[key].value]));
}

function validateCoverageGrantConfigInner(value, { tenantId, fingerprint } = {}) {
  if (value == null) return null;
  const tenant = normalizeTenantId(tenantId);
  if (validated.has(value)) {
    if (!tenant || value.tenantId !== tenant || value.fingerprint !== fingerprint) {
      throw configError('Scout coverage grant scope mismatch');
    }
    return value;
  }
  const manifest = readManifest(value);
  if (manifest.version !== 2 || manifest.epoch !== 1
    || typeof manifest.tenantId !== 'string' || !tenant || manifest.tenantId !== tenant
    || typeof manifest.fingerprint !== 'string' || !SHA256.test(manifest.fingerprint)
    || manifest.fingerprint !== fingerprint) {
    throw configError('Scout coverage grant scope mismatch');
  }
  if (typeof manifest.approvalRef !== 'string'
    || !APPROVAL_REF.test(manifest.approvalRef)) {
    throw configError('Invalid Scout coverage approval reference');
  }
  if (!Array.isArray(manifest.rootQueryKeys) || !manifest.rootQueryKeys.length
    || manifest.rootQueryKeys.length > MAX_ROOTS) {
    throw configError('Invalid Scout coverage root allowlist');
  }
  // Copy before validation so the frozen result cannot retain mutable caller data.
  const rootQueryKeys = Array.from(manifest.rootQueryKeys);
  if (!rootQueryKeys.length || rootQueryKeys.length > MAX_ROOTS
    || rootQueryKeys.some(key => typeof key !== 'string' || !SHA256.test(key))
    || new Set(rootQueryKeys).size !== rootQueryKeys.length) {
    throw configError('Invalid Scout coverage root allowlist');
  }
  for (const field of ['notBefore', 'expiresAt']) {
    if (typeof manifest[field] !== 'string' || !TIMESTAMP.test(manifest[field])
      || !Number.isFinite(Date.parse(manifest[field]))
      || new Date(manifest[field]).toISOString() !== manifest[field]) {
      throw configError('Invalid Scout coverage approval window');
    }
  }
  if (Date.parse(manifest.expiresAt) <= Date.parse(manifest.notBefore)) {
    throw configError('Invalid Scout coverage approval window');
  }
  if (!Number.isInteger(manifest.maxNewPages) || manifest.maxNewPages < 1
    || manifest.maxNewPages > rootQueryKeys.length * MAX_PAGES_PER_ROOT) {
    throw configError('Invalid Scout coverage grant page cap');
  }
  manifest.rootQueryKeys = Object.freeze(rootQueryKeys.sort());
  const result = Object.freeze({
    ...manifest,
    manifestHash: digest(manifest),
    allocationHash: allocationIdentity(manifest.tenantId, manifest.fingerprint),
    allocation: ALLOCATION,
  });
  validated.add(result);
  return result;
}

function validateCoverageGrantConfig(value, scope) {
  try {
    return validateCoverageGrantConfigInner(value, scope);
  } catch (error) {
    if (error && error.code === ERROR_CODE) throw error;
    throw configError('Invalid Scout coverage grant manifest');
  }
}

module.exports = { MAX_ROOTS, allocationIdentity, validateCoverageGrantConfig };
