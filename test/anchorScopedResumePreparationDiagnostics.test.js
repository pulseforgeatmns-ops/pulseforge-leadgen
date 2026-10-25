'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { createPreparationTrace } = require('../services/anchorCrmPreparationDiagnostics');

test('preparation diagnostics contain only bounded allowlisted observations', async t => {
  let lines = [];
  t.mock.method(console, 'error', (...args) => lines.push(args));
  const read = () => JSON.parse(lines.at(-1)[1]);
  await t.test('one terminal event contains no raw error, identity or SQL', () => {
    lines = [];
    const secret = 'private-email@example.test password session-cookie SQL customer copy';
    const trace = createPreparationTrace();
    trace.operation('a'.repeat(64)); trace.claimStarted(); trace.claimCommitted();
    trace.enter('canonical_hydration');
    const error = Object.assign(new Error(secret), { code: 'MISSION_STATE_INCONSISTENT', details: secret, sql: secret, sessionId: secret });
    trace.failure(error); trace.failure(error);
    assert.equal(lines.length, 1);
    assert.equal(read().phase, 'canonical_hydration');
    assert.equal(read().errorCode, 'MISSION_STATE_INCONSISTENT');
    assert.equal(read().claimOutcome, 'committed');
    assert.equal(read().operationId, 'a'.repeat(64));
    assert.equal(read().retryAllowed, false);
    assert.ok(!JSON.stringify(lines).includes(secret));
    assert.deepEqual(Object.keys(read()).sort(), ['event', 'version', 'phase', 'claimOutcome', 'operationId', 'step',
      'intent', 'errorCode', 'causeCodes', 'executionErrorClass', 'retryAllowed'].sort());
  });
  await t.test('lost claim result is unknown, never labeled unconsumed', () => {
    const trace = createPreparationTrace(); trace.enter('claim'); trace.claimStarted(); trace.failure(new Error('lost response'));
    assert.equal(read().claimOutcome, 'unknown'); assert.equal(read().errorCode, 'unknown');
  });
  await t.test('stage persistence class and known SQL cause retain no detail', () => {
    const trace = createPreparationTrace(); trace.enter('stage_execution'); trace.stage(0, 'APPROVE_DISCOVERY');
    trace.failure(Object.assign(new Error('private SQL'), { code: '23514', tmeClass: 'persistence', cause: { code: '23514', detail: 'private row' } }));
    assert.equal(read().executionErrorClass, 'persistence'); assert.deepEqual(read().causeCodes, ['23514']);
    assert.equal(read().step, 0); assert.equal(read().intent, 'APPROVE_DISCOVERY');
    assert.doesNotMatch(JSON.stringify(read()), /private/);
  });
  await t.test('arbitrary labels, codes, classes and IDs cannot enter logs', () => {
    const trace = createPreparationTrace(); trace.enter('secret-phase'); trace.operation('secret-session'); trace.stage(100, 'secret-intent');
    trace.failure({ code: 'secret-code', tmeClass: 'secret-class', cause: { code: 'secret-nested' } });
    assert.equal(read().phase, 'unknown'); assert.equal(read().operationId, null); assert.equal(read().step, null);
    assert.equal(read().intent, null); assert.equal(read().errorCode, 'unknown'); assert.equal(read().executionErrorClass, null);
    assert.deepEqual(read().causeCodes, []); assert.doesNotMatch(JSON.stringify(read()), /secret/);
  });
  await t.test('cyclic or excessively deep causes are bounded', () => {
    const cyclic = { code: '23514' }; cyclic.cause = cyclic;
    createPreparationTrace().failure(cyclic); assert.deepEqual(read().causeCodes, []);
    const deep = { code: '23505', cause: { code: '23503', cause: { code: '23502', cause: { code: '57014', cause: { code: '55P03' } } } } };
    createPreparationTrace().failure(deep); assert.deepEqual(read().causeCodes, ['23503', '23502', '57014']);
  });
  await t.test('changing getters cannot bypass code allowlist', () => {
    let gets = 0; const error = { get code() { return ++gets === 1 ? '23514' : 'secret-value'; } };
    createPreparationTrace().failure(error); assert.equal(gets, 1); assert.equal(read().errorCode, '23514');
  });
  await t.test('hostile getters and failed logging never throw', () => {
    assert.doesNotThrow(() => createPreparationTrace().failure({ get cause() { throw Error('do not disclose'); } }));
    const before = console.error;
    console.error = () => { throw Error('sink unavailable'); };
    try { assert.doesNotThrow(() => createPreparationTrace().failure(new Error('original failure'))); }
    finally { console.error = before; }
  });
});
