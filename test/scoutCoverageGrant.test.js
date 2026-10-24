'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createHash } = require('node:crypto');
const { MAX_ROOTS, allocationIdentity, validateCoverageGrantConfig } = require('../services/scoutCoverageGrant');

const hash = text => createHash('sha256').update(text).digest('hex');
const scope = { tenantId: '13', fingerprint: hash('synthetic unchanged mission') };
const invalid = { code: 'SCOUT_COVERAGE_EPOCH_CONFIG_INVALID' };
const manifest = () => ({
  version: 2,
  epoch: 1,
  approvalRef: 'synthetic-review-only',
  ...scope,
  rootQueryKeys: [hash('synthetic root b'), hash('synthetic root a')],
  notBefore: '2026-10-20T01:00:00.000Z',
  expiresAt: '2026-10-20T02:00:00.000Z',
  maxNewPages: 1,
});
const validate = value => validateCoverageGrantConfig(value, scope);

test('v2 grants are absent without a manifest and require exact own data fields', () => {
  assert.equal(validate(null), null);
  assert.equal(validate(undefined), null);
  for (const value of [true, false, 2, '', 'null', [], {}, new Date(), () => manifest()]) {
    assert.throws(() => validate(value), invalid);
  }
  for (const field of Object.keys(manifest())) {
    const missing = manifest();
    delete missing[field];
    assert.throws(() => validate(missing), invalid, `missing ${field}`);
  }
  for (const field of ['reset', 'manifestHash', 'allocationHash', 'allocation', 'maxTotalPages', 'grantHash']) {
    assert.throws(() => validate({ ...manifest(), [field]: true }), invalid, `extra ${field}`);
  }
  const symbol = manifest();
  symbol[Symbol('extra')] = true;
  assert.throws(() => validate(symbol), invalid);
  const hidden = Object.defineProperty(manifest(), 'hidden', { value: true });
  assert.throws(() => validate(hidden), invalid);
  const nonEnumerable = Object.defineProperty(manifest(), 'approvalRef', { enumerable: false });
  assert.throws(() => validate(nonEnumerable), invalid);
  const getter = Object.defineProperty(manifest(), 'approvalRef', {
    enumerable: true,
    get() { throw new Error('Must not execute a manifest getter'); },
  });
  assert.throws(() => validate(getter), invalid);
  assert.throws(() => validate(Object.create(manifest())), invalid);
  assert.equal(validate(Object.assign(Object.create(null), manifest())).version, 2);
});

test('version, epoch, approval reference, and manifest scope are strict', () => {
  for (const change of [
    { version: 1 }, { version: '2' }, { version: 3 },
    { epoch: 0 }, { epoch: '1' }, { epoch: 2 },
    { tenantId: 13 }, { tenantId: '10' }, { tenantId: 'synthetic-tenant' },
    { fingerprint: hash('synthetic different mission') }, { fingerprint: 'private mission' },
    { fingerprint: 'A'.repeat(64) }, { fingerprint: new String(scope.fingerprint) },
    { fingerprint: `${scope.fingerprint}\n` },
    { approvalRef: '' }, { approvalRef: 'has spaces' }, { approvalRef: '../relative' },
    { approvalRef: 'x'.repeat(129) }, { approvalRef: 1 },
    ...['\n', '\r', '\u2028', '\u2029'].map(ending => ({ approvalRef: `synthetic-review${ending}` })),
  ]) assert.throws(() => validate({ ...manifest(), ...change }), invalid);
  assert.equal(validate({ ...manifest(), approvalRef: 'x'.repeat(128) }).approvalRef.length, 128);
  assert.equal(validate({ ...manifest(), approvalRef: 'a._:-0' }).approvalRef, 'a._:-0');
  const tenant10 = { ...scope, tenantId: '10' };
  assert.equal(validateCoverageGrantConfig({ ...manifest(), ...tenant10 }, tenant10).tenantId, '10');
  assert.equal(validateCoverageGrantConfig(manifest(), { ...scope, tenantId: 13 }).tenantId, '13');
  for (const badScope of [undefined, null, {}, { ...scope, tenantId: [] },
    { ...scope, tenantId: { toString: () => '13' } }, { ...scope, fingerprint: undefined }]) {
    assert.throws(() => validateCoverageGrantConfig(manifest(), badScope), invalid);
  }
});

test('root allowlist is bounded, dense, distinct, and contains lowercase SHA-256 identities only', () => {
  assert.equal(MAX_ROOTS, 54);
  for (const rootQueryKeys of [
    [], null, 'query', [hash('same synthetic root'), hash('same synthetic root')],
    ['private raw query'], ['A'.repeat(64)], ['a'.repeat(63)], ['a'.repeat(65)], [null],
    [`${hash('synthetic root')}\n`],
    [new String(hash('synthetic root'))], new Array(1),
    Array.from({ length: 55 }, (_, i) => hash(`synthetic root ${i}`)),
  ]) assert.throws(() => validate({ ...manifest(), rootQueryKeys }), invalid);
  for (const count of [1, MAX_ROOTS]) {
    const rootQueryKeys = Array.from({ length: count }, (_, i) => hash(`synthetic root ${i}`));
    const result = validate({ ...manifest(), rootQueryKeys, maxNewPages: count * 3 });
    assert.equal(result.rootQueryKeys.length, count);
    assert.equal(result.maxNewPages, count * 3);
  }
  for (const count of [0, 55]) {
    const rootQueryKeys = [hash('synthetic apparent root')];
    rootQueryKeys[Symbol.iterator] = function* () {
      for (let i = 0; i < count; i += 1) yield hash(`synthetic iterator root ${i}`);
    };
    assert.throws(() => validate({ ...manifest(), rootQueryKeys }), invalid);
  }
});

test('approval windows require canonical UTC timestamps and a strictly positive interval', () => {
  for (const value of [
    '2026-10-20', '2026-10-20T01:00:00Z', '2026-10-20T01:00:00.000+00:00',
    '2026-10-20T01:00:00.000z', '2026-02-30T01:00:00.000Z',
    '2026-13-20T01:00:00.000Z', '2026-10-20T24:00:00.000Z',
    '2026-10-20T01:00:60.000Z', '2026-10-20T01:00:00.0000Z',
    '', null, Date.parse(manifest().notBefore), new Date(manifest().notBefore),
  ]) {
    assert.throws(() => validate({ ...manifest(), notBefore: value }), invalid);
    assert.throws(() => validate({ ...manifest(), expiresAt: value }), invalid);
  }
  assert.throws(() => validate({ ...manifest(), expiresAt: manifest().notBefore }), invalid);
  assert.throws(() => validate({ ...manifest(), notBefore: manifest().expiresAt }), invalid);
  assert.equal(validate({ ...manifest(), expiresAt: '2026-10-20T01:00:00.001Z' }).maxNewPages, 1);
});

test('maxNewPages is an explicit positive integer bounded by this grant root set', () => {
  for (const maxNewPages of [undefined, null, true, '1', 0, -1, -0, 1.5, NaN, Infinity, 7, 162]) {
    assert.throws(() => validate({ ...manifest(), maxNewPages }), invalid);
  }
  for (const maxNewPages of [1, 6]) {
    assert.equal(validate({ ...manifest(), maxNewPages }).maxNewPages, maxNewPages);
  }
  assert.throws(() => validate({ ...manifest(), rootQueryKeys: [hash('synthetic root')], maxNewPages: 4 }), invalid);
});

test('allocation is stable across grants and always retains the cumulative 54-root structural ceiling', () => {
  const a = validate(manifest());
  assert.equal(a.allocationHash, hash(JSON.stringify([
    'places-coverage-allocation-v2', scope.tenantId, scope.fingerprint, 1, 54, 3,
  ])));
  assert.equal(a.allocationHash, allocationIdentity(scope.tenantId, scope.fingerprint));
  assert.equal(a.allocationHash, allocationIdentity(13, scope.fingerprint));
  assert.deepEqual(a.allocation, {
    maxInitialReacquisitions: 54,
    maxTotalPages: 162,
    maxSearchHttpAttempts: 486,
    maxDetailsHttpAttempts: 3240,
    maxTotalHttpAttempts: 3726,
  });
  for (const change of [
    { approvalRef: 'synthetic-later-review' },
    { notBefore: '2026-10-20T00:59:59.999Z' },
    { expiresAt: '2026-10-20T03:00:00.000Z' },
    { rootQueryKeys: [hash('synthetic fresh root')] },
    { rootQueryKeys: [manifest().rootQueryKeys[0]] },
    { maxNewPages: 6 },
  ]) {
    const b = validate({ ...manifest(), ...change });
    assert.equal(b.allocationHash, a.allocationHash);
    assert.deepEqual(b.allocation, a.allocation);
    assert.notEqual(b.manifestHash, a.manifestHash);
  }
  assert.notEqual(allocationIdentity('10', scope.fingerprint), a.allocationHash);
  assert.notEqual(allocationIdentity('13', hash('synthetic different mission')), a.allocationHash);
  for (const [tenantId, fingerprint] of [[null, scope.fingerprint], ['synthetic-tenant', scope.fingerprint],
    [{ toString: () => '13' }, scope.fingerprint], ['13', 'raw mission'], ['13', null],
    ['13', new String(scope.fingerprint)], ['13', `${scope.fingerprint}\n`]]) {
    assert.throws(() => allocationIdentity(tenantId, fingerprint), invalid);
  }
});

test('canonical grant hashes disregard input field and root order, but do not include derived allocation', () => {
  const input = manifest();
  const a = validate(input);
  const reversed = Object.fromEntries(Object.entries(input).reverse());
  reversed.rootQueryKeys = [...input.rootQueryKeys].reverse();
  const b = validate(reversed);
  assert.deepEqual(a.rootQueryKeys, [...input.rootQueryKeys].sort());
  assert.equal(a.manifestHash, b.manifestHash);
  assert.equal(a.manifestHash, hash(JSON.stringify({ ...input, rootQueryKeys: [...input.rootQueryKeys].sort() })));
  assert.notEqual(a.manifestHash, a.allocationHash);
});

test('validated configurations are detached, deeply frozen, and cannot be forged or reused across scopes', () => {
  const input = manifest();
  const a = validate(input);
  const originalRoots = [...a.rootQueryKeys];
  const originalHash = a.manifestHash;
  assert.ok(Object.isFrozen(a));
  assert.ok(Object.isFrozen(a.rootQueryKeys));
  assert.ok(Object.isFrozen(a.allocation));
  assert.equal(validate(a), a);
  input.approvalRef = 'changed-original';
  input.rootQueryKeys.push(hash('synthetic later root'));
  assert.deepEqual(a.rootQueryKeys, originalRoots);
  assert.equal(a.manifestHash, originalHash);
  assert.equal(a.approvalRef, 'synthetic-review-only');
  assert.throws(() => { a.maxNewPages = 162; }, TypeError);
  assert.throws(() => { a.allocationHash = hash('synthetic forged allocation'); }, TypeError);
  assert.throws(() => a.rootQueryKeys.push(hash('synthetic extra root')), TypeError);
  assert.throws(() => { a.rootQueryKeys[0] = hash('synthetic replaced root'); }, TypeError);
  assert.throws(() => { a.allocation.maxTotalPages = 999; }, TypeError);
  assert.throws(() => validate({ ...a }), invalid);
  assert.throws(() => validate(JSON.parse(JSON.stringify(a))), invalid);
  for (const badScope of [{ ...scope, tenantId: '10' },
    { ...scope, fingerprint: hash('synthetic different mission') }, {}, null]) {
    assert.throws(() => validateCoverageGrantConfig(a, badScope), invalid);
  }
});
