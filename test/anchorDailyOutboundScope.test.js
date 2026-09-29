'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');

const legacySource = {
  tenantId: '10',
  objective: 'Acquire one recurring cleaning client from a short-term rental operator.',
  targetSegment: 'Short-Term Rental Operators',
  structuredMission: {
    immutable: true,
    market: { segment: 'short_term_rental' },
    geography: { cities: ['Manchester'], region: 'Greater Manchester' },
    success: { type: 'recurring_clients', target: 1 },
  },
  constraints: ['Commercial operators only'],
};

test('daily child null resolvedObjective preserves the legacy approved scope hash', () => {
  const child = { ...structuredClone(legacySource), resolvedObjective: null };
  const originalChild = structuredClone(child);
  const approvedHash = hash(legacySource);
  assert.equal(hash(missionScope(legacySource)), approvedHash);
  assert.equal(hash(missionScope(child)), approvedHash);
  assert.deepEqual(child, originalChild, 'Scope projection must not change mission data');
});

test('normalizing the absent optional objective still binds every substantive scope field', () => {
  const approvedHash = hash(missionScope(legacySource));
  const changes = [
    { tenantId: '11' },
    { objective: 'Acquire restaurant clients.' },
    { targetSegment: 'Restaurants' },
    { structuredMission: { ...legacySource.structuredMission, geography: { cities: ['Boston'] } } },
    { constraints: [] },
    { resolvedObjective: { objective: 'Acquire restaurant clients.' } },
    { resolvedObjective: false },
    { resolvedObjective: '' },
  ];
  for (const change of changes) {
    assert.notEqual(hash(missionScope({ ...legacySource, resolvedObjective: null, ...change })),
      approvedHash, `Changed scope must fail comparison: ${JSON.stringify(change)}`);
  }
});

test('non-null resolved objectives retain their existing hash and nested bindings', () => {
  const source = { ...legacySource, resolvedObjective: {
    objective: legacySource.objective, executionPolicy: { maxSends: 1 },
  } };
  assert.equal(hash(missionScope(source)), hash(source));
  const changed = structuredClone(source);
  changed.resolvedObjective.executionPolicy.maxSends = 2;
  assert.notEqual(hash(missionScope(changed)), hash(missionScope(source)));
});

test('null normalization is limited to resolvedObjective', () => {
  const { constraints, ...withoutConstraints } = legacySource;
  assert.notEqual(hash(missionScope(withoutConstraints)),
    hash(missionScope({ ...withoutConstraints, constraints: null })));
  assert.deepEqual(missionScope(legacySource).constraints, constraints);
});
