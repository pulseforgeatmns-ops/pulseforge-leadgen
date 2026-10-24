'use strict';

const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const { after, before, beforeEach, describe, it } = require('node:test');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

const { commitCanonicalSemanticBatch, deriveInterpretationBatchKey } = require('../lib/canonicalSemanticWrite');
const { reconstructCanonicalSemanticProjection } = require('../lib/canonicalSemanticProjection');
const {
  OperatorJudgmentCanonicalAdapter,
  judgmentIdentityKey,
  isOperatorJudgmentFact,
  isOrdinaryBusinessFact,
  INTERPRETER_ID,
} = require('../lib/operatorJudgmentCanonicalAdapter');
const { commitOperatorJudgment } = require('../services/operatorJudgmentCanonical');
const v1Seed = require('../migrations/2026-09-02-spec-224-production-registry-artifact');
const spec250Seed = require('../migrations/2026-09-11-spec-250-operator-judgment-registry');
const { ANCHOR_AO_ALLOCATION_FIXTURE } = require('./fixtures/spec250-anchor-ao-allocation');

const spec223aMigration = fs.readFileSync(
  path.join(__dirname, '..', 'migrations', '2026-09-01-spec-223a-canonical-semantic-persistence.sql'),
  'utf8'
);
const cieMigration = fs.readFileSync(
  path.join(__dirname, '..', 'migrations', '2026-08-06-client-intelligence-engine.sql'),
  'utf8'
);

function cloneFixture(overrides = {}) {
  return {
    ...ANCHOR_AO_ALLOCATION_FIXTURE,
    ...overrides,
    provenance: { ...ANCHOR_AO_ALLOCATION_FIXTURE.provenance, ...(overrides.provenance || {}) },
    operator: { ...ANCHOR_AO_ALLOCATION_FIXTURE.operator, ...(overrides.operator || {}) },
    propositions: (overrides.propositions || ANCHOR_AO_ALLOCATION_FIXTURE.propositions).map(item => ({ ...item })),
  };
}

describe('SPEC-250 canonical operator judgment producer', () => {
  it('rejects unstructured conversational text at the typed write boundary', () => {
    assert.throws(
      () => OperatorJudgmentCanonicalAdapter.buildBatch({
        tenant_id: 'tenant:anchor',
        client_id: 10,
        text: 'Zack Bunker should get the larger property-management accounts.',
      }),
      error => error.code === 'JUDGMENT_KIND_REQUIRED'
    );
    assert.throws(
      () => OperatorJudgmentCanonicalAdapter.buildBatch('Zack should get PM accounts'),
      error => error.code === 'JUDGMENT_INPUT_INVALID' || error.code === 'JUDGMENT_KIND_REQUIRED'
    );
  });

  it('refuses to label model inference as operator judgment', () => {
    const registry = {
      id: '11111111-1111-4111-8111-111111111111',
      registry_version: spec250Seed.REGISTRY_VERSION,
      content_digest: 'a'.repeat(64),
      entity_vocabulary: spec250Seed.ENTITY_VOCABULARY,
      predicate_definitions: spec250Seed.PREDICATE_DEFINITIONS,
    };
    const base = {
      kind: 'OPERATOR_JUDGMENT',
      tenant_id: 'tenant:anchor',
      client_id: 10,
      judgment_key: 'ao-initial-prospect-allocation',
      judgment_kind: 'ALLOCATION_DECISION',
      registry_artifact: registry,
      evidence_records: [{
        id: '22222222-2222-4222-8222-222222222222',
        role: 'canonical_judgment',
        statement: 'placeholder',
        source_text_sha256: 'b'.repeat(64),
      }],
      propositions: [{ judgment_slot: 'allocation:zack_bunker', statement: 'Zack gets larger PM accounts.' }],
    };
    assert.throws(
      () => OperatorJudgmentCanonicalAdapter.buildBatch({
        ...base,
        provenance: { origin: 'MODEL', origin_kind: 'model_inference' },
      }),
      error => error.code === 'OPERATOR_PROVENANCE_INVALID'
    );
    assert.throws(
      () => OperatorJudgmentCanonicalAdapter.buildBatch({
        ...base,
        provenance: { origin: 'SPECIALIST', origin_kind: 'specialist_recommendation' },
      }),
      error => error.code === 'OPERATOR_PROVENANCE_INVALID'
    );
  });

  it('fails closed when the pinned registry lacks OPERATOR_JUDGMENT', () => {
    assert.throws(
      () => OperatorJudgmentCanonicalAdapter.buildBatch({
        kind: 'OPERATOR_JUDGMENT',
        tenant_id: 'tenant:anchor',
        client_id: 10,
        judgment_key: 'ao-initial-prospect-allocation',
        judgment_kind: 'ALLOCATION_DECISION',
        provenance: { origin: 'OPERATOR', origin_kind: 'operator_authored' },
        registry_artifact: {
          id: '33333333-3333-4333-8333-333333333333',
          registry_version: v1Seed.REGISTRY_VERSION,
          content_digest: 'c'.repeat(64),
          entity_vocabulary: v1Seed.ENTITY_VOCABULARY,
          predicate_definitions: v1Seed.PREDICATE_DEFINITIONS,
        },
        evidence_records: [{
          id: '44444444-4444-4444-8444-444444444444',
          statement: 'placeholder',
          source_text_sha256: 'd'.repeat(64),
        }],
        propositions: [{ judgment_slot: 'allocation:zack_bunker', statement: 'Zack gets larger PM accounts.' }],
      }),
      error => error.code === 'UNSUPPORTED_SEMANTIC_PRIMITIVE'
    );
  });

  it('fails closed rather than guessing an active mission', () => {
    assert.throws(
      () => OperatorJudgmentCanonicalAdapter.buildBatch({
        kind: 'OPERATOR_JUDGMENT',
        tenant_id: 'tenant:anchor',
        client_id: 10,
        judgment_key: 'ao-initial-prospect-allocation',
        judgment_kind: 'ALLOCATION_DECISION',
        provenance: { origin: 'OPERATOR', origin_kind: 'operator_authored' },
        resolve_mission: true,
        registry_artifact: {
          id: '55555555-5555-4555-8555-555555555555',
          registry_version: spec250Seed.REGISTRY_VERSION,
          content_digest: 'e'.repeat(64),
          entity_vocabulary: spec250Seed.ENTITY_VOCABULARY,
          predicate_definitions: spec250Seed.PREDICATE_DEFINITIONS,
        },
        evidence_records: [{
          id: '66666666-6666-4666-8666-666666666666',
          statement: 'placeholder',
          source_text_sha256: 'f'.repeat(64),
        }],
        propositions: [{ judgment_slot: 'allocation:zack_bunker', statement: 'Zack gets larger PM accounts.' }],
      }),
      error => error.code === 'MISSION_BINDING_AMBIGUOUS'
    );
  });
});

describe('SPEC-250 canonical operator judgment persistence', () => {
  let postgres;
  let pool;

  before(async () => {
    postgres = await startDisposablePostgres('spec-250-pg-');
    pool = new Pool({ connectionString: postgres.connectionString });
    await pool.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto;
      CREATE TABLE clients (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
      CREATE TABLE tenant_workspaces (client_id INTEGER PRIMARY KEY REFERENCES clients(id), tenant_key TEXT NOT NULL UNIQUE);
      INSERT INTO clients VALUES (10,'Anchor Cleaning'),(11,'NoTenantClient');
      INSERT INTO tenant_workspaces VALUES (10,'tenant:anchor');`);
    await pool.query(cieMigration);
    await pool.query(spec223aMigration);
    await v1Seed.up(pool);
    await spec250Seed.up(pool);
  });

  after(async () => {
    await pool.end();
    await postgres.stop();
  });

  beforeEach(async () => {
    await pool.query('TRUNCATE canonical_interpretation_batches CASCADE');
    await pool.query('TRUNCATE cie_evidence CASCADE');
    await pool.query('TRUNCATE cie_interview_sessions CASCADE');
  });

  function entitiesById(projection) {
    return new Map((projection.entities || []).map(entity => [entity.id, entity]));
  }

  function judgmentFacts(projection) {
    const entities = entitiesById(projection);
    return projection.facts.filter(fact => isOperatorJudgmentFact(fact, entities));
  }

  function factForSlot(projection, slot, options = {}) {
    const matches = judgmentFacts(projection).filter(fact => fact.qualifiers?.judgment_slot === slot);
    if (options.selected) return matches.find(fact => fact.selected_in_conflict);
    return matches[0];
  }

  it('1-7. Anchor AO allocation persists, projects, and preserves provenance/rationale/mission', async () => {
    const result = await commitOperatorJudgment(pool, cloneFixture());
    assert.equal(result.newly_committed, true);
    assert.equal(result.replayed, false);
    assert.equal(result.interpreter_id, INTERPRETER_ID);
    assert.ok(result.snapshot_id);

    const projection = await reconstructCanonicalSemanticProjection(pool, {
      tenant_id: 'tenant:anchor',
      snapshot_id: result.snapshot_id,
    });
    const entities = entitiesById(projection);
    const judgmentEntity = projection.entities.find(entity => entity.entity_type === 'OPERATOR_JUDGMENT');
    assert.ok(judgmentEntity);
    assert.equal(judgmentEntity.identity_key, judgmentIdentityKey('ao-initial-prospect-allocation'));
    assert.equal(judgmentEntity.canonical_label, ANCHOR_AO_ALLOCATION_FIXTURE.label);

    const zack = factForSlot(projection, 'allocation:zack_bunker');
    const tony = factForSlot(projection, 'allocation:tony_jackson');
    const rory = factForSlot(projection, 'allocation:rory_matthews');
    const method = factForSlot(projection, 'allocation_method');
    const learning = factForSlot(projection, 'learning_hypothesis:ao_segment_fit');
    assert.match(zack.object_value.value, /Zack Bunker/);
    assert.match(zack.object_value.value, /property-management/);
    assert.match(tony.object_value.value, /facilities-heavy/);
    assert.match(rory.object_value.value, /professional offices/);
    assert.match(method.object_value.value, /not round-robin/);
    assert.equal(zack.epistemic_state, 'KNOWN');
    assert.equal(tony.epistemic_state, 'KNOWN');
    assert.equal(learning.epistemic_state, 'HYPOTHESIS');
    assert.notEqual(learning.epistemic_state, 'KNOWN');
    assert.equal(zack.qualifiers.provenance.origin, 'OPERATOR');
    assert.equal(zack.qualifiers.provenance.origin_kind, 'operator_authored');
    assert.equal(zack.qualifiers.provenance.actor_id, 'operator:authenticated');
    assert.equal(zack.qualifiers.associated_mission_id, ANCHOR_AO_ALLOCATION_FIXTURE.associated_mission_id);
    assert.equal(zack.temporal_status, 'CURRENT');
    assert.equal(zack.modality, 'INTENDED');

    const rationaleRefs = projection.evidence_references.filter(ref => ref.fact_id === tony.id);
    assert.ok(rationaleRefs.length >= 2);
    assert.ok(rationaleRefs.every(ref => ref.support_type === 'OPERATOR_CONFIRMED'));
    const rationaleEvidence = await pool.query(
      `SELECT statement FROM cie_evidence WHERE id = ANY($1::uuid[])`,
      [rationaleRefs.map(ref => ref.evidence_id)]
    );
    assert.ok(rationaleEvidence.rows.some(row => /ABM\/UNH housekeeping/.test(row.statement)));

    const objectiveFact = projection.facts.find(fact => fact.predicate === 'associated_with_objective');
    assert.ok(objectiveFact);
    assert.equal(entities.get(objectiveFact.subject_entity_id).entity_type, 'OPERATOR_JUDGMENT');
    assert.equal(entities.get(objectiveFact.object_value.value).entity_type, 'OBJECTIVE');

    assert.equal(isOrdinaryBusinessFact(zack, entities), false);
    assert.equal(isOperatorJudgmentFact(zack, entities), true);
    const binding = projection.facts.find(fact => fact.predicate === 'has_operator_judgment');
    assert.equal(entities.get(binding.subject_entity_id).entity_type, 'BUSINESS');
    assert.ok(judgmentFacts(projection).every(fact => fact.predicate === 'expresses_judgment'));
    assert.ok(projection.facts.some(fact => fact.epistemic_state === 'HYPOTHESIS'
      && fact.qualifiers.judgment_slot === 'learning_hypothesis:ao_segment_fit'));
  });

  it('5/6. hypothesis stays HYPOTHESIS and is distinct from a KNOWN business fact', async () => {
    const first = await commitOperatorJudgment(pool, cloneFixture());
    const evidenceId = first.evidence_ids[0];
    const evidence = (await pool.query('SELECT id, statement, source_text_sha256 FROM cie_evidence WHERE id=$1', [evidenceId])).rows[0];
    const registry = (await pool.query(
      `SELECT * FROM canonical_registry_artifacts WHERE registry_version=$1`,
      [spec250Seed.REGISTRY_VERSION]
    )).rows[0];
    const businessFact = {
      tenant_id: 'tenant:anchor',
      registry_artifact_id: registry.id,
      registry_version: registry.registry_version,
      registry_content_digest: registry.content_digest,
      interpreter_id: 'spec-250-business-fact-control',
      interpreter_version: '1.0.0',
      semantic_model_version: 1,
      ordered_evidence_input_ids: [evidence.id],
      semantic_entities: [{ entity_type: 'BUSINESS', identity_key: 'client:10', domain_client_id: 10 }],
      label_assertions: [],
      semantic_facts: [{
        subject_entity_identity_key: 'client:10',
        predicate: 'has_description',
        object_value: { type: 'SEMANTIC_TEXT', value: 'Anchor Cleaning is a commercial cleaning company.' },
        epistemic_state: 'KNOWN',
        interpretation_confidence: 0.95,
        interpretation_calibration_version: 'test-v1',
        temporal_status: 'CURRENT',
        modality: 'ACTUAL',
        qualifiers: { language: 'en' },
      }],
      fact_evidence_links: [{
        fact_index: 0,
        evidence_id: evidence.id,
        source_text_sha256: evidence.source_text_sha256,
        span_start_utf16: 0,
        span_end_utf16: evidence.statement.length,
        support_type: 'DIRECT',
      }],
      fact_relations: [],
      entity_merge_events: [],
      conflict_set_resolutions: [],
      snapshot_metadata: {},
    };
    businessFact.idempotency_key = deriveInterpretationBatchKey(
      businessFact,
      new Map([[evidence.id, evidence]])
    );
    const business = await commitCanonicalSemanticBatch(pool, businessFact);
    const projection = await reconstructCanonicalSemanticProjection(pool, {
      tenant_id: 'tenant:anchor',
      snapshot_id: business.snapshot_id,
    });
    const entities = entitiesById(projection);
    const learning = factForSlot(projection, 'learning_hypothesis:ao_segment_fit');
    const description = projection.facts.find(fact => fact.predicate === 'has_description');
    assert.equal(learning.epistemic_state, 'HYPOTHESIS');
    assert.equal(description.epistemic_state, 'KNOWN');
    assert.equal(isOperatorJudgmentFact(learning, entities), true);
    assert.equal(isOrdinaryBusinessFact(description, entities), true);
    assert.notEqual(entities.get(learning.subject_entity_id).entity_type, entities.get(description.subject_entity_id).entity_type);
  });

  it('8. missing or unbound tenant identity fails closed and writes nothing', async () => {
    await assert.rejects(
      commitOperatorJudgment(pool, cloneFixture({ tenant_id: '' })),
      error => error.code === 'TENANT_IDENTITY_REQUIRED'
    );
    await assert.rejects(
      commitOperatorJudgment(pool, cloneFixture({ tenant_id: 'tenant:missing', client_id: 10 })),
      error => error.code === 'TENANT_IDENTITY_REQUIRED'
    );
    await assert.rejects(
      commitOperatorJudgment(pool, cloneFixture({ tenant_id: 'tenant:anchor', client_id: 11 })),
      error => error.code === 'TENANT_IDENTITY_REQUIRED'
    );
    assert.equal((await pool.query('SELECT count(*)::int AS count FROM canonical_business_snapshots')).rows[0].count, 0);
    assert.equal((await pool.query('SELECT count(*)::int AS count FROM canonical_business_facts')).rows[0].count, 0);
  });

  it('9. unsupported semantic primitive fails closed', async () => {
    const v1 = (await pool.query(
      `SELECT * FROM canonical_registry_artifacts WHERE registry_version=$1`,
      [v1Seed.REGISTRY_VERSION]
    )).rows[0];
    await assert.rejects(
      commitOperatorJudgment(pool, cloneFixture({ registry_artifact: v1 })),
      error => error.code === 'UNSUPPORTED_SEMANTIC_PRIMITIVE'
    );
    assert.equal((await pool.query('SELECT count(*)::int AS count FROM canonical_business_snapshots')).rows[0].count, 0);
  });

  it('10. duplicate submission is idempotent', async () => {
    const first = await commitOperatorJudgment(pool, cloneFixture());
    const second = await commitOperatorJudgment(pool, cloneFixture());
    assert.equal(second.replayed, true);
    assert.equal(second.newly_committed, false);
    assert.equal(second.snapshot_id, first.snapshot_id);
    assert.equal((await pool.query('SELECT count(*)::int AS count FROM canonical_business_snapshots')).rows[0].count, 1);
    const facts = (await pool.query('SELECT count(*)::int AS count FROM canonical_business_facts')).rows[0].count;
    assert.equal(facts, first.fact_ids_created.length);
  });

  it('11. revised judgment preserves history and supersedes the prior slot', async () => {
    const first = await commitOperatorJudgment(pool, cloneFixture());
    const revised = cloneFixture();
    const rory = revised.propositions.find(item => item.judgment_slot === 'allocation:rory_matthews');
    rory.statement = 'Rory Matthews should now focus primarily on property managers.';
    rory.rationale_points = ['Later operator decision: shift Rory toward property managers.'];
    const second = await commitOperatorJudgment(pool, revised);
    assert.equal(second.newly_committed, true);
    assert.notEqual(second.snapshot_id, first.snapshot_id);

    const original = await reconstructCanonicalSemanticProjection(pool, {
      tenant_id: 'tenant:anchor',
      snapshot_id: first.snapshot_id,
    });
    const latest = await reconstructCanonicalSemanticProjection(pool, {
      tenant_id: 'tenant:anchor',
      snapshot_id: second.snapshot_id,
    });
    const originalRory = factForSlot(original, 'allocation:rory_matthews', { selected: true });
    const historicalRory = latest.facts.find(fact => fact.id === originalRory.id);
    const currentRory = factForSlot(latest, 'allocation:rory_matthews', { selected: true });
    assert.match(originalRory.object_value.value, /professional offices/);
    assert.equal(originalRory.selected_in_conflict, true);
    assert.equal(historicalRory.selected_in_conflict, false);
    assert.match(currentRory.object_value.value, /now focus primarily on property managers/);
    assert.equal(currentRory.selected_in_conflict, true);
    assert.notEqual(currentRory.id, originalRory.id);
    assert.ok(latest.relations.some(relation => relation.relation_type === 'SUPERSEDES'
      && relation.from_fact_id === currentRory.id && relation.to_fact_id === originalRory.id));
    assert.ok(latest.relations.some(relation => relation.relation_type === 'CORRECTION_OF'
      && relation.from_fact_id === currentRory.id && relation.to_fact_id === originalRory.id));
    assert.match(factForSlot(latest, 'allocation:zack_bunker').object_value.value, /Zack Bunker/);
  });

  it('12. ordinary Max conversational text does not become canonical judgment', async () => {
    await assert.rejects(
      commitOperatorJudgment(pool, {
        tenant_id: 'tenant:anchor',
        client_id: 10,
        text: 'Zack should get the bigger PM accounts and Tony can take facilities.',
      }),
      error => error.code === 'JUDGMENT_KIND_REQUIRED'
    );
    const engineSource = fs.readFileSync(path.join(__dirname, '../packages/max/workspace/WorkspaceEngine.js'), 'utf8');
    assert.equal(engineSource.includes('operatorJudgmentCanonical'), false);
    assert.equal(engineSource.includes('commitOperatorJudgment'), false);
    assert.equal((await pool.query('SELECT count(*)::int AS count FROM canonical_business_facts')).rows[0].count, 0);
  });

  it('13. no parallel operator_decisions storage is introduced', async () => {
    await commitOperatorJudgment(pool, cloneFixture());
    const tables = (await pool.query(`
      SELECT tablename FROM pg_tables
      WHERE schemaname = 'public'
        AND tablename IN ('operator_decisions', 'operator_memory', 'operator_judgment_store')
    `)).rows;
    assert.deepEqual(tables, []);
    const facts = (await pool.query('SELECT count(*)::int AS count FROM canonical_business_facts')).rows[0].count;
    assert.ok(facts > 0);
  });
});
