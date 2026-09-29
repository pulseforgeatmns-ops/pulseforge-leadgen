/**
 * SPEC-250 -- Canonical Operator Judgment registry artifact
 *
 * Additive SPEC-222 vocabulary extension. Does not mutate the frozen
 * 1.0.0-spec-222-canonical artifact (historical snapshots pin its digest).
 *
 * Primitive chosen: first-class entity type OPERATOR_JUDGMENT plus the
 * minimum predicates needed to bind, type, express, and optionally associate
 * a durable operator judgment. Epistemic state stays on facts (SPEC-221).
 * Hypothesis remains epistemic_state=HYPOTHESIS. Ordinary business facts keep
 * BUSINESS/OFFER/… subjects. Operator judgment is machine-inspectable as
 * entity_type=OPERATOR_JUDGMENT / predicate=expresses_judgment.
 */

const v1 = require('./2026-09-02-spec-224-production-registry-artifact');

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

const ENTITY_VOCABULARY = [...v1.ENTITY_VOCABULARY, 'OPERATOR_JUDGMENT'];

const PREDICATE_DEFINITIONS = {
  ...v1.PREDICATE_DEFINITIONS,
  has_description: literalPredicate(ENTITY_VOCABULARY, ['SEMANTIC_TEXT'], 'SINGLE', ['language']),
  depends_on: literalPredicate(ENTITY_VOCABULARY, ['FACT_REF'], 'SET'),
  has_operator_judgment: entityPredicate(['BUSINESS'], ['OPERATOR_JUDGMENT'], 'SET'),
  has_judgment_kind: literalPredicate(['OPERATOR_JUDGMENT'], ['JUDGMENT_KIND'], 'SINGLE'),
  expresses_judgment: literalPredicate(['OPERATOR_JUDGMENT'], ['SEMANTIC_TEXT'], 'SINGLE', ['judgment_slot']),
  associated_with_objective: entityPredicate(['OPERATOR_JUDGMENT'], ['OBJECTIVE'], 'SET'),
};

const REGISTRY_VERSION = '1.1.0-spec-250-operator-judgment';

const CLOSED_ENTITY_TYPES = ENTITY_VOCABULARY;

async function extendEntityTypeConstraint(pool) {
  await pool.query(`
    DO $$
    DECLARE
      constraint_name TEXT;
    BEGIN
      FOR constraint_name IN
        SELECT con.conname
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        WHERE nsp.nspname = 'public'
          AND rel.relname = 'canonical_business_entities'
          AND con.contype = 'c'
          AND (
            pg_get_constraintdef(con.oid) ILIKE '%entity_type IN (%'
            OR pg_get_constraintdef(con.oid) ILIKE '%entity_type = ANY%'
          )
          AND pg_get_constraintdef(con.oid) NOT ILIKE '%domain_client_id%'
      LOOP
        EXECUTE format('ALTER TABLE canonical_business_entities DROP CONSTRAINT %I', constraint_name);
      END LOOP;
      ALTER TABLE canonical_business_entities
        ADD CONSTRAINT canonical_business_entities_entity_type_check
        CHECK (entity_type IN (
          'BUSINESS', 'OFFER', 'PROGRAM', 'CUSTOMER_PROFILE', 'PAIN',
          'CAPABILITY', 'OUTCOME', 'OBJECTIVE', 'METRIC', 'OPERATOR_JUDGMENT'
        ));
    END;
    $$;
  `);
}

async function up(pool) {
  await extendEntityTypeConstraint(pool);

  const existing = await pool.query(
    `SELECT id, content_digest FROM canonical_registry_artifacts WHERE registry_version = $1`,
    [REGISTRY_VERSION]
  );
  if (existing.rows.length > 0) {
    console.log(`[SPEC-250] Registry artifact already seeded: ${existing.rows[0].id}`);
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
  console.log(`[SPEC-250] Operator-judgment registry artifact created: ${registry.id} (${registry.registry_version}, digest ${registry.content_digest})`);
  return registry;
}

async function down(pool) {
  console.log('[SPEC-250] Keeping operator-judgment registry artifact (immutable production data; no destructive down)');
}

module.exports = {
  up,
  down,
  REGISTRY_VERSION,
  ENTITY_VOCABULARY,
  PREDICATE_DEFINITIONS,
  CLOSED_ENTITY_TYPES,
  extendEntityTypeConstraint,
};
