'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { candidateKey, canonicalJson, canonicalJsonText, createCandidateTicketStore, CRASH_RETRY_MS, LEASE_MS } = require('../services/scoutCandidateTickets');

const pool = { query: async () => ({ rows: [] }), connect: async () => { throw Error('must not connect'); } };
const store = () => createCandidateTicketStore({ pool, tenantId: 't1', fingerprint: 'f1' });

test('candidate identity uses exact provider ID; canonical raw fallback preserves contradictions', () => {
  assert.equal(candidateKey({ placeId: 'P', name: 'First', website: 'a.test' }), candidateKey({ place_id: 'P', name: 'Second', website: 'b.test' }));
  assert.notEqual(candidateKey({ placeId: 'P' }), candidateKey({ placeId: 'p' }));
  assert.notEqual(candidateKey({ placeId: 'P' }), candidateKey({ placeId: ' P' }));
  assert.notEqual(candidateKey({ name: 'Same', website: 'shared.test' }), candidateKey({ name: 'Different', website: 'shared.test' }));
  assert.equal(candidateKey({ a: 1, b: { c: 2, d: 3 } }), candidateKey({ b: { d: 3, c: 2 }, a: 1 }));
  assert.notEqual(candidateKey({ a: [1, 2] }), candidateKey({ a: [2, 1] }));
  for (const value of [null, 1, 'raw', []]) assert.throws(() => candidateKey(value), /raw JSON object/);
  for (const value of [{ a: undefined }, { a: NaN }, { a: Infinity }]) assert.throws(() => canonicalJson(value), /only JSON/);
});

test('store validates scope and limits before touching PostgreSQL', async () => {
  assert.throws(() => createCandidateTicketStore({ pool, tenantId: null, fingerprint: 'f' }), /tenant/);
  assert.throws(() => createCandidateTicketStore({ pool, tenantId: 't', fingerprint: ' f' }), /fingerprint/);
  assert.equal(LEASE_MS, 15 * 60 * 1000);
  assert.equal(CRASH_RETRY_MS, 60 * 60 * 1000);
  for (const limit of [0, -1, 101, 1.5]) await assert.rejects(store().claimDue({ dependencyKey: 'd', limit }), /claim limit/);
  await assert.rejects(store().claimDue(), /dependency key/);
  await assert.rejects(store().finish([], { state: 'handled', reason: 'queued', dependencyKey: 'd' }), /references/);
  await assert.rejects(store().renew([{ tenantId: 'other' }]), /scope or claim/);
});

test('schema failure fails closed; store contains no runtime DDL or external dependencies', async () => {
  let connected = 0;
  const unavailable = createCandidateTicketStore({ tenantId: 't', fingerprint: 'f', pool: {
    query: async () => { throw Object.assign(Error('relation does not exist'), { code: '42P01' }); },
    connect: async () => { connected += 1; },
  } });
  await assert.rejects(unavailable.transferQuery('q'), error => error.ticketCode === 'schema_not_ready');
  assert.equal(connected, 0);
  const source = fs.readFileSync(require.resolve('../services/scoutCandidateTickets'), 'utf8');
  assert.doesNotMatch(source, /CREATE TABLE|ALTER TABLE|DROP TABLE|require\(['"](?:\.\.\/db|axios|node:https|node:http)['"]\)|fetch\(/);
  assert.match(source, /FOR UPDATE SKIP LOCKED/);
  assert.match(source, /clock_timestamp\(\)/);
  assert.doesNotMatch(source, /acquisition_outbound_events/);
});

test('authoritative JSON text hashing preserves exact large numeric observations', () => {
  assert.equal(canonicalJsonText(' { "b": [ 9007199254740993, "escaped\\\"quote" ], "a": 1 } '), '{"a":1,"b":[9007199254740993,"escaped\\\"quote"]}');
  assert.equal(canonicalJsonText(JSON.stringify({ longKey: { z: 2 }, a: true })), canonicalJson({ longKey: { z: 2 }, a: true }));
  assert.notEqual(candidateKey({ value: 9007199254740992 }, '{"value":9007199254740992}'), candidateKey({ value: 9007199254740992 }, '{"value":9007199254740993}'));
  for (const invalid of ['{', '[1,]', '{"a":}', '123x']) assert.throws(() => canonicalJsonText(invalid), /Invalid/);
});
