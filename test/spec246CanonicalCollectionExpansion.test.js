'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const { CIECanonicalAdapter, UNREPRESENTABLE_FIELDS } = require('../lib/cieCanonicalAdapter');
const seed = require('../migrations/2026-09-02-spec-224-production-registry-artifact');

function build(normalizedFacts, definitions = seed.PREDICATE_DEFINITIONS) {
  return CIECanonicalAdapter.buildBatch({
    tenant_id: 'tenant:spec246', client_id: 246,
    blueprint: { normalizedFacts }, blueprint_id: 'bp-spec246', blueprint_version: '1.0',
    interpreter_id: 'spec246-test', interpreter_version: '1.0',
    registry_artifact: { id: 'registry', registry_version: seed.REGISTRY_VERSION,
      content_digest: 'a'.repeat(64), entity_vocabulary: seed.ENTITY_VOCABULARY,
      predicate_definitions: definitions },
    cie_evidence_records: [{ id: 'evidence', category: 'customer', statement: 'Frozen evidence.',
      source_text_sha256: 'b'.repeat(64) }],
  });
}
const relationships = batch => batch.semantic_facts.filter(f => f.predicate === 'targets_customer_profile');

test('AUDIT-133: two ideal customers expand into two labeled entities and relationships', () => {
  const values = ['existing operating small business', 'cleaning/home services'];
  const batch = build({ ideal_customers: values });
  assert.deepEqual(batch.label_assertions.map(l => l.label), values);
  assert.equal(batch.semantic_entities.filter(e => e.entity_type === 'CUSTOMER_PROFILE').length, 2);
  assert.equal(relationships(batch).length, 2);
  for (const [index, fact] of relationships(batch).entries()) {
    assert.equal(fact.object_value.type, 'ENTITY_REF');
    assert.equal(fact.object_value.value, batch.label_assertions[index].entity_identity_key);
    assert.equal(batch.fact_evidence_links[index].fact_index, index);
  }
});

test('three customers produce three relationships; duplicates preserve first occurrence order', () => {
  const batch = build({ ideal_customers: ['B', 'A', 'B', 'C', 'A'] });
  assert.deepEqual(batch.label_assertions.map(l => l.label), ['B', 'A', 'C']);
  assert.equal(relationships(batch).length, 3);
  const duplicate = build({ ideal_customers: ['property managers', 'property managers'] });
  assert.equal(duplicate.label_assertions.length, 1);
  assert.equal(relationships(duplicate).length, 1);
});

test('single-element collection has the same output as a scalar; empty emits nothing', () => {
  assert.deepEqual(build({ ideal_customers: ['Founders'] }), build({ ideal_customers: 'Founders' }));
  const batch = build({ ideal_customers: [], avoid_customers: [], services: [], target_markets: [] });
  assert.deepEqual(batch.label_assertions, []);
  assert.deepEqual(batch.semantic_facts, []);
  assert.equal(batch.semantic_entities.length, 1);
});

test('all existing SET mappings expand, deduplicate, and retain scoped qualifiers and evidence', () => {
  const batch = build({ services: [{ name: 'Coaching', variants: [{ name: 'Group' }, { name: 'Group' }] },
      { name: 'Coaching', variants: [{ name: 'Group' }, { name: 'Group' }] }],
    ideal_customers: ['A', 'B'], ideal_customers_role: ['owner', 'owner', 'founder'],
    ideal_customers_geography: ['US', 'UK', 'US'], avoid_customers: ['C', 'C', 'D'],
    target_markets: ['US', 'UK', 'US'], differentiation: ['X', 'X', 'Y'],
    ninety_day_outcomes: [{ name: 'Growth' }, { name: 'Growth' }] });
  const counts = Object.fromEntries([...new Set(batch.semantic_facts.map(f => f.predicate))]
    .map(p => [p, batch.semantic_facts.filter(f => f.predicate === p).length]));
  assert.deepEqual(counts, { offers: 1, contains_program: 1, targets_customer_profile: 2,
    has_role: 4, has_geography: 6, excludes_customer_profile: 2, has_buying_reason: 2, targets_outcome: 1 });
  assert.ok(batch.label_assertions.every(l => typeof l.label === 'string'));
  assert.ok(batch.semantic_facts.every(f => typeof f.object_value.value === 'string'));
  assert.deepEqual(batch.fact_evidence_links.map(l => l.fact_index), batch.semantic_facts.map((_, i) => i));
  assert.ok(batch.semantic_facts.filter(f => f.predicate === 'excludes_customer_profile')
    .every(f => f.qualifiers.strength === 'LOW_PRIORITY'));
});

test('normalized string services, variants, and outcomes receive scalar labels', () => {
  assert.deepEqual(build({ services: ['Coaching', 'Coaching'], ninety_day_outcomes: ['Growth'] })
    .label_assertions.map(l => l.label), ['Coaching', 'Growth']);
  assert.deepEqual(build({ services: [{ name: 'Coaching', variants: ['Group', 'Group'] }] })
    .label_assertions.map(l => l.label), ['Coaching', 'Group']);
});

test('no array, object, Set, or Map can reach a canonical label or scalar literal', () => {
  for (const invalid of [['nested'], { label: 'nested' }, new Set(['nested']), new Map([['a', 'b']])]) {
    for (const field of ['ideal_customers', 'avoid_customers']) {
      assert.throws(() => build({ [field]: [invalid] }), /scalar string/);
    }
    assert.throws(() => build({ business_name: invalid }), /scalar/);
    assert.throws(() => build({ services: [{ name: invalid }] }), /scalar string/);
    assert.throws(() => build({ target_markets: [invalid] }), /scalar string/);
    assert.throws(() => build({ services: [{ name: 'A', variants: [{ name: invalid }] }] }), /scalar string/);
    assert.throws(() => build({ ninety_day_outcomes: [{ name: invalid }] }), /scalar string/);
  }
});

test('pinned metadata governs cardinality, value kind, and entity type', () => {
  const defs = structuredClone(seed.PREDICATE_DEFINITIONS);
  defs.targets_customer_profile.cardinality = 'SINGLE';
  assert.throws(() => build({ ideal_customers: ['A', 'B'] }, defs), /SINGLE requires a scalar/);
  defs.targets_customer_profile.cardinality = 'ORDERED_SET';
  assert.deepEqual(build({ ideal_customers: ['B', 'A', 'B'] }, defs).label_assertions.map(l => l.label), ['B', 'A']);
  defs.targets_customer_profile.range.entity_types = ['OUTCOME'];
  assert.equal(build({ ideal_customers: ['A'] }, defs).semantic_entities[1].entity_type, 'OUTCOME');
  defs.has_geography.range.literal_types = ['CONCEPT'];
  assert.throws(() => build({ target_markets: 'US' }, defs), /incompatible value kind/);
  assert.throws(() => build({ ideal_customers: 'A' }, {}), /registry metadata/);
  assert.throws(() => build({ ideal_customers: 'A', ideal_customers_stage: ['early', 'late'] }), /SINGLE/);
});

test('existing scalar assertions and structured employee range remain unchanged', () => {
  const batch = build({ business_name: 'Acme', ideal_customers: 'Founders', ideal_customers_role: 'owner',
    ideal_customers_stage: 'operating', ideal_customers_employee_range: '1-10',
    target_markets: 'US', differentiation: 'Practical', brand_voice: 'Friendly', pricing_model: 'Fixed' });
  assert.deepEqual(batch.label_assertions.map(l => l.label), ['Acme', 'Founders']);
  assert.equal(batch.semantic_facts.length, 7);
  assert.deepEqual(batch.semantic_facts.find(f => f.predicate === 'has_employee_range').object_value,
    { type: 'INTEGER_RANGE', value: { min: 1, max: 10, unit: 'employees' } });
  // These fields had no adapter mapping before SPEC-246; do not invent one.
  assert.deepEqual(build({ brand_voice: 'Friendly', pricing_model: 'Fixed' }).semantic_facts, []);
});

test('surveyed unsupported collections remain unrepresented and are recorded', () => {
  const batch = build(Object.fromEntries(UNREPRESENTABLE_FIELDS.map(f => [f, ['A', 'B']])));
  assert.deepEqual(batch.semantic_facts, []);
  assert.deepEqual(batch.snapshot_metadata.unrepresentable_fields, UNREPRESENTABLE_FIELDS);
});
