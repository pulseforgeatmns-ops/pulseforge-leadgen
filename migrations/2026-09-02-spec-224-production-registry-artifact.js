/**
 * SPEC-224 -- Production Registry Artifact Seed
 *
 * Seeds the first production canonical_registry_artifacts row using the
 * SPEC-222 v1 entity vocabulary and predicate registry. Wire format matches
 * lib/canonicalSemanticWrite.js exactly: predicate domain is an array of
 * entity types, range is {kind:'ENTITY',entity_types:[...]} or
 * {kind:'LITERAL',literal_types:[...]}, cardinality is one of
 * SINGLE|OPTIONAL_SINGLE|SET|ORDERED_SET, and slot_qualifiers lists any
 * qualifier keys required for conflict-slot derivation.
 *
 * content_digest MUST be computed by Postgres itself
 * (digest(jsonb_build_object(...)::text,'sha256')) -- the same expression the
 * canonical_registry_digest_trigger re-derives on insert. Computing it in
 * Node from JSON.stringify() would not reliably match Postgres's own jsonb
 * text serialization.
 */

const ENTITY_TYPES = ['BUSINESS', 'OFFER', 'PROGRAM', 'CUSTOMER_PROFILE', 'PAIN',
  'CAPABILITY', 'OUTCOME', 'OBJECTIVE', 'METRIC'];

function entityPredicate(domain, entityTypes, cardinality, slotQualifiers) {
  const def = { domain, range: { kind: 'ENTITY', entity_types: entityTypes }, cardinality };
  if (slotQualifiers) def.slot_qualifiers = slotQualifiers;
  return def;
}

function literalPredicate(domain, literalTypes, cardinality, slotQualifiers) {
  const def = { domain, range: { kind: 'LITERAL', literal_types: literalTypes }, cardinality };
  if (slotQualifiers) def.slot_qualifiers = slotQualifiers;
  return def;
}

const ENTITY_VOCABULARY = ENTITY_TYPES;

const PREDICATE_DEFINITIONS = {
  offers: entityPredicate(['BUSINESS'], ['OFFER'], 'SET'),
  contains_program: entityPredicate(['OFFER'], ['PROGRAM'], 'SET'),
  has_delivery_mode: literalPredicate(['OFFER', 'PROGRAM'], ['DELIVERY_MODE'], 'SET'),
  targets_customer_profile: entityPredicate(['BUSINESS', 'OFFER', 'PROGRAM'], ['CUSTOMER_PROFILE'], 'SET'),
  excludes_customer_profile: entityPredicate(['BUSINESS', 'OFFER', 'PROGRAM'], ['CUSTOMER_PROFILE'], 'SET', ['strength']),
  teaches_capability: entityPredicate(['PROGRAM', 'OFFER'], ['CAPABILITY'], 'SET'),
  targets_outcome: entityPredicate(['PROGRAM', 'OFFER', 'OBJECTIVE'], ['OUTCOME'], 'SET'),
  addresses_pain: entityPredicate(['PROGRAM', 'OFFER', 'CUSTOMER_PROFILE'], ['PAIN'], 'SET'),
  has_role: literalPredicate(['CUSTOMER_PROFILE'], ['ROLE'], 'SET'),
  has_business_stage: literalPredicate(['CUSTOMER_PROFILE', 'BUSINESS'], ['STAGE'], 'SINGLE'),
  has_characteristic: literalPredicate(['CUSTOMER_PROFILE', 'BUSINESS'], ['CONCEPT'], 'SET'),
  has_geography: literalPredicate(['CUSTOMER_PROFILE', 'BUSINESS', 'OBJECTIVE'], ['GEOGRAPHY'], 'SET', ['scope']),
  has_employee_range: literalPredicate(['CUSTOMER_PROFILE', 'BUSINESS'], ['INTEGER_RANGE'], 'SINGLE'),
  has_vertical: literalPredicate(['CUSTOMER_PROFILE', 'BUSINESS', 'OBJECTIVE'], ['VERTICAL'], 'SET', ['validation_scope']),
  has_description: literalPredicate(ENTITY_TYPES, ['SEMANTIC_TEXT'], 'SINGLE', ['language']),
  measures_objective: entityPredicate(['METRIC'], ['OBJECTIVE'], 'SET'),
  depends_on: literalPredicate(ENTITY_TYPES, ['FACT_REF'], 'SET'),
  has_buying_reason: literalPredicate(['BUSINESS', 'OFFER'], ['CONCEPT'], 'SET'),
  has_brand_voice: literalPredicate(['BUSINESS'], ['BRAND_DIRECTION'], 'SET'),
  avoids_brand_trait: literalPredicate(['BUSINESS'], ['BRAND_TRAIT'], 'SET'),
  has_validation_status: literalPredicate(['BUSINESS', 'OFFER', 'CUSTOMER_PROFILE', 'OBJECTIVE'], ['VALIDATION_STATUS'], 'SINGLE', ['validation_scope']),
};

const REGISTRY_VERSION = '1.0.0-spec-222-canonical';

async function up(pool) {
  const existing = await pool.query(
    `SELECT id, content_digest FROM canonical_registry_artifacts WHERE registry_version = $1`,
    [REGISTRY_VERSION]
  );
  if (existing.rows.length > 0) {
    console.log(`[SPEC-224] Registry artifact already seeded: ${existing.rows[0].id}`);
    return existing.rows[0];
  }

  const vocabularyJson = JSON.stringify(ENTITY_VOCABULARY);
  const predicatesJson = JSON.stringify(PREDICATE_DEFINITIONS);

  const digestRow = (await pool.query(
    `SELECT encode(digest(jsonb_build_object(
       'entity_vocabulary', $1::jsonb,
       'predicate_definitions', $2::jsonb,
       'registry_version', $3::text)::text, 'sha256'), 'hex') AS digest`,
    [vocabularyJson, predicatesJson, REGISTRY_VERSION]
  )).rows[0];

  const result = await pool.query(
    `INSERT INTO canonical_registry_artifacts
      (registry_version, entity_vocabulary, predicate_definitions, content_digest)
      VALUES ($1, $2::jsonb, $3::jsonb, $4)
      RETURNING id, registry_version, content_digest`,
    [REGISTRY_VERSION, vocabularyJson, predicatesJson, digestRow.digest]
  );

  const registry = result.rows[0];
  console.log(`[SPEC-224] Production registry artifact created: ${registry.id} (${registry.registry_version}, digest ${registry.content_digest})`);
  return registry;
}

async function down(pool) {
  console.log('[SPEC-224] Keeping production registry artifact (immutable production data; no destructive down)');
}

module.exports = { up, down, REGISTRY_VERSION, ENTITY_VOCABULARY, PREDICATE_DEFINITIONS };
