'use strict';

const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const { randomUUID } = require('crypto');
const { after, before, beforeEach, describe, it } = require('node:test');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const {
  commitCanonicalSemanticBatch,
  deriveInterpretationBatchKey,
} = require('../lib/canonicalSemanticWrite');
const { reconstructCanonicalSemanticProjection } = require('../lib/canonicalSemanticProjection');

const migration = fs.readFileSync(path.join(__dirname, '..', 'migrations',
  '2026-09-01-spec-223a-canonical-semantic-persistence.sql'), 'utf8');

describe('SPEC-223B canonical semantic write boundary', () => {
  let postgres;
  let pool;
  let registryV1;
  let registryV2;
  const sessions = new Map();

  async function registryDigest(version, vocabulary, predicates) {
    return (await pool.query(`SELECT encode(digest(jsonb_build_object(
      'entity_vocabulary',$1::jsonb,'predicate_definitions',$2::jsonb,
      'registry_version',$3::text)::text,'sha256'),'hex') AS digest`,
    [JSON.stringify(vocabulary), JSON.stringify(predicates), version])).rows[0].digest;
  }

  async function addRegistry(version, predicates) {
    const vocabulary = ['BUSINESS', 'OFFER', 'OUTCOME'];
    const digest = await registryDigest(version, vocabulary, predicates);
    return (await pool.query(`INSERT INTO canonical_registry_artifacts
      (registry_version,entity_vocabulary,predicate_definitions,content_digest)
      VALUES ($1,$2::jsonb,$3::jsonb,$4) RETURNING *`,
    [version, JSON.stringify(vocabulary), JSON.stringify(predicates), digest])).rows[0];
  }

  async function evidence(clientId, statement) {
    return (await pool.query(`INSERT INTO cie_evidence
      (client_id,session_id,category,statement,source_text_sha256,immutable_at)
      VALUES ($1,$2,'identity',$3,encode(digest($3,'sha256'),'hex'),NOW())
      RETURNING id,source_text_sha256,statement`, [clientId, sessions.get(clientId), statement])).rows[0];
  }

  function entity(type, identityKey, clientId = null, extra = {}) {
    return { entity_type: type, identity_key: identityKey,
      ...(type === 'BUSINESS' ? { domain_client_id: clientId } : {}), ...extra };
  }

  function fact(predicate, objectValue, extra = {}) {
    return { subject_entity_identity_key: 'client:1', predicate, object_value: objectValue,
      qualifiers: {}, epistemic_state: 'KNOWN', interpretation_confidence: 0.95,
      interpretation_calibration_version: 'test-v1', temporal_status: 'CURRENT',
      modality: 'ACTUAL', ...extra };
  }

  function batch({ registry = registryV1, clientId = 1, tenant = 'tenant:babrun',
    evidenceInputs = [], entities = [], facts = [], links, labels = [], relations = [],
    merges = [], conflicts = [], snapshotMetadata = {} }) {
    const input = {
      tenant_id: tenant, registry_artifact_id: registry.id,
      registry_version: registry.registry_version,
      registry_content_digest: registry.content_digest,
      interpreter_id: 'spec-223b-test', interpreter_version: '1.0.0',
      semantic_model_version: 1,
      ordered_evidence_input_ids: evidenceInputs.map(item => item.id),
      semantic_entities: [entity('BUSINESS', `client:${clientId}`, clientId), ...entities],
      label_assertions: labels, semantic_facts: facts,
      fact_evidence_links: links === undefined ? facts.map((_, index) => ({
        fact_index: index, evidence_id: evidenceInputs[index].id,
        source_text_sha256: evidenceInputs[index].source_text_sha256,
        span_start_utf16: 0, span_end_utf16: evidenceInputs[index].statement.length,
        support_type: 'DIRECT',
      })) : links,
      fact_relations: relations, entity_merge_events: merges,
      conflict_set_resolutions: conflicts, snapshot_metadata: snapshotMetadata,
    };
    input.idempotency_key = deriveInterpretationBatchKey(
      input,
      new Map(evidenceInputs.map(item => [item.id, item]))
    );
    return input;
  }

  async function counts() {
    const tables = ['canonical_interpretation_batches', 'canonical_business_entities',
      'canonical_entity_label_assertions', 'canonical_business_facts',
      'canonical_fact_evidence_refs', 'canonical_fact_relations',
      'canonical_entity_merge_events', 'canonical_business_snapshots'];
    return Object.fromEntries(await Promise.all(tables.map(async table => [table,
      (await pool.query(`SELECT count(*)::int AS count FROM ${table}`)).rows[0].count])));
  }

  before(async () => {
    postgres = await startDisposablePostgres('spec-223b-pg-');
    pool = new Pool({ connectionString: postgres.connectionString });
    await pool.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto;
      CREATE TABLE clients (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
      CREATE TABLE tenant_workspaces (client_id INTEGER PRIMARY KEY REFERENCES clients(id), tenant_key TEXT NOT NULL UNIQUE);
      CREATE TABLE cie_interview_sessions (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL);
      CREATE TABLE cie_evidence (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER NOT NULL,
        session_id UUID NOT NULL REFERENCES cie_interview_sessions(id), category TEXT NOT NULL, statement TEXT NOT NULL);
      CREATE TABLE "normalizedFacts" (id UUID PRIMARY KEY DEFAULT gen_random_uuid());
      CREATE TABLE business_facts (id UUID PRIMARY KEY DEFAULT gen_random_uuid());
      CREATE TABLE knowledge_nodes (id UUID PRIMARY KEY DEFAULT gen_random_uuid());
      INSERT INTO clients VALUES (1,'Babrun'),(2,'Other');
      INSERT INTO tenant_workspaces VALUES (1,'tenant:babrun'),(2,'tenant:other');`);
    for (const clientId of [1, 2]) sessions.set(clientId,
      (await pool.query('INSERT INTO cie_interview_sessions(client_id) VALUES ($1) RETURNING id', [clientId])).rows[0].id);
    await pool.query(migration);
    registryV1 = await addRegistry('v1', {
      offers: { domain: ['BUSINESS'], range: { kind: 'ENTITY', entity_types: ['OFFER'] }, cardinality: 'SET' },
      legal_name: { domain: ['BUSINESS'], range: { kind: 'LITERAL', literal_types: ['STRING'] }, cardinality: 'SINGLE' },
      targets_outcome: { domain: ['OFFER'], range: { kind: 'ENTITY', entity_types: ['OUTCOME'] }, cardinality: 'SET' },
    });
    registryV2 = await addRegistry('v2', {
      offers: { domain: ['BUSINESS'], range: { kind: 'ENTITY', entity_types: ['OFFER'] }, cardinality: 'SET' },
    });
  });

  beforeEach(async () => {
    await pool.query(`TRUNCATE canonical_interpretation_batches CASCADE`);
  });

  after(async () => { await pool.end(); await postgres.stop(); });

  it('A. commits a complete structured Babrun batch', async () => {
    const source = await evidence(1, 'Babrun offers leadership coaching.');
    const nameSource = await evidence(1, 'The legal name is Babrun LLC.');
    const result = await commitCanonicalSemanticBatch(pool, batch({ key: 'A', evidenceInputs: [source, nameSource],
      entities: [entity('OFFER', 'offer:leadership')],
      labels: [{ entity_identity_key: 'client:1', label: 'Babrun', assertion_kind: 'CANONICAL', evidence_id: source.id }],
      facts: [fact('offers', { type: 'ENTITY_REF', value: 'offer:leadership' }),
        fact('legal_name', { type: 'STRING', value: 'Babrun LLC' })],
      relations: [{ from_fact_index: 0, to_fact_index: 1, relation_type: 'DEPENDS_ON' }] }));
    assert.equal(result.newly_committed, true);
    assert.ok(result.canonical_business_entity_id && result.snapshot_id && result.manifest_digest);
    assert.equal(result.relations_created.length, 1);
    assert.deepEqual((await pool.query('SELECT input_ordinal FROM canonical_batch_evidence_inputs ORDER BY input_ordinal')).rows,
      [{ input_ordinal: 0 }, { input_ordinal: 1 }]);
  });

  it('B. exact replay creates no duplicate canonical state', async () => {
    const source = await evidence(1, 'Babrun offers leadership coaching.');
    const input = batch({ key: 'B', evidenceInputs: [source], entities: [entity('OFFER', 'offer:b')],
      facts: [fact('offers', { type: 'ENTITY_REF', value: 'offer:b' })] });
    const first = await commitCanonicalSemanticBatch(pool, input);
    const beforeReplay = await counts();
    const replay = await commitCanonicalSemanticBatch(pool, input);
    assert.equal(replay.replayed, true);
    assert.equal(replay.snapshot_id, first.snapshot_id);
    assert.equal(replay.manifest_digest, first.manifest_digest);
    assert.deepEqual(await counts(), beforeReplay);
  });

  it('C. later evidence reuses proposition identity and adds evidence', async () => {
    const firstEvidence = await evidence(1, 'Babrun offers coaching.');
    const semanticFact = fact('offers', { type: 'ENTITY_REF', value: 'offer:c' });
    const first = await commitCanonicalSemanticBatch(pool, batch({ key: 'C1', evidenceInputs: [firstEvidence],
      entities: [entity('OFFER', 'offer:c')], facts: [semanticFact] }));
    const laterEvidence = await evidence(1, 'Coaching remains an active Babrun offer.');
    const later = await commitCanonicalSemanticBatch(pool, batch({ key: 'C2', evidenceInputs: [laterEvidence],
      entities: [entity('OFFER', 'offer:c')], facts: [semanticFact] }));
    assert.deepEqual(later.fact_ids_reused, first.fact_ids_created);
    assert.equal(later.evidence_links_created.length, 1);
    assert.equal((await pool.query('SELECT count(*)::int AS count FROM canonical_business_facts')).rows[0].count, 1);
  });

  it('D. cross-tenant evidence rejects and rolls back the batch', async () => {
    const foreign = await evidence(2, 'Foreign evidence.');
    await assert.rejects(commitCanonicalSemanticBatch(pool, batch({ key: 'D', evidenceInputs: [foreign],
      entities: [entity('OFFER', 'offer:d')], facts: [fact('offers', { type: 'ENTITY_REF', value: 'offer:d' })] })),
    error => error.code === 'EVIDENCE_TENANT_MISMATCH');
    assert.deepEqual(await counts(), Object.fromEntries(Object.keys(await counts()).map(key => [key, 0])));
  });

  it('E. cross-tenant entity reference rejects and rolls back', async () => {
    const foreignEvidence = await evidence(2, 'Other sells consulting.');
    const foreign = await commitCanonicalSemanticBatch(pool, batch({ key: 'E-other', clientId: 2, tenant: 'tenant:other',
      evidenceInputs: [foreignEvidence], entities: [entity('OFFER', 'offer:foreign')],
      facts: [fact('offers', { type: 'ENTITY_REF', value: 'offer:foreign' }, { subject_entity_identity_key: 'client:2' })] }));
    const foreignEntityId = foreign.entity_ids_created.find(id => id !== foreign.canonical_business_entity_id);
    const source = await evidence(1, 'Babrun evidence.');
    const before = await counts();
    await assert.rejects(commitCanonicalSemanticBatch(pool, batch({ key: 'E', evidenceInputs: [source],
      entities: [entity('OFFER', 'offer:foreign', null, { existing_entity_id: foreignEntityId })],
      facts: [fact('offers', { type: 'ENTITY_REF', value: 'offer:foreign' })] })),
    error => error.code === 'ENTITY_TENANT_MISMATCH');
    assert.deepEqual(await counts(), before);
  });

  it('F. invalid predicate, domain, and range each reject', async t => {
    const source = await evidence(1, 'Invalid semantic input.');
    await t.test('predicate', async () => assert.rejects(commitCanonicalSemanticBatch(pool,
      batch({ key: 'F1', evidenceInputs: [source], facts: [fact('invented', { type: 'STRING', value: 'x' })] })),
    error => error.code === 'PREDICATE_INVALID'));
    await t.test('domain', async () => assert.rejects(commitCanonicalSemanticBatch(pool,
      batch({ key: 'F2', evidenceInputs: [source], entities: [entity('OFFER', 'offer:f')],
        facts: [fact('legal_name', { type: 'STRING', value: 'x' }, { subject_entity_identity_key: 'offer:f' })] })),
    error => error.code === 'PREDICATE_DOMAIN_INVALID'));
    await t.test('range', async () => assert.rejects(commitCanonicalSemanticBatch(pool,
      batch({ key: 'F3', evidenceInputs: [source], facts: [fact('offers', { type: 'STRING', value: 'x' })] })),
    error => error.code === 'PREDICATE_RANGE_INVALID'));
  });

  it('G. SINGLE slot rejects simultaneously active incompatible facts', async () => {
    const one = await evidence(1, 'The legal name is Babrun LLC.');
    const two = await evidence(1, 'The legal name is Babrun Inc.');
    await assert.rejects(commitCanonicalSemanticBatch(pool, batch({ key: 'G', evidenceInputs: [one, two],
      facts: [fact('legal_name', { type: 'STRING', value: 'Babrun LLC' }),
        fact('legal_name', { type: 'STRING', value: 'Babrun Inc.' })] })),
    error => error.code === 'CONFLICT_CARDINALITY_VIOLATED');
    assert.equal((await pool.query('SELECT count(*)::int AS count FROM canonical_business_facts')).rows[0].count, 0);
  });

  it('H. SET slot accepts multiple compatible facts', async () => {
    const one = await evidence(1, 'Babrun offers coaching.');
    const two = await evidence(1, 'Babrun offers workshops.');
    const result = await commitCanonicalSemanticBatch(pool, batch({ key: 'H', evidenceInputs: [one, two],
      entities: [entity('OFFER', 'offer:coaching'), entity('OFFER', 'offer:workshops')],
      facts: [fact('offers', { type: 'ENTITY_REF', value: 'offer:coaching' }),
        fact('offers', { type: 'ENTITY_REF', value: 'offer:workshops' })] }));
    assert.equal(result.fact_ids_created.length, 2);
    assert.equal(result.conflict_results.length, 2);
  });

  it('I. ambiguous entity identity fails closed', async () => {
    await assert.rejects(commitCanonicalSemanticBatch(pool, batch({ key: 'I', entities: [
      entity('OFFER', 'offer:i', null, { ambiguous_candidates: ['offer:one', 'offer:two'] })] })),
    error => error.code === 'ENTITY_IDENTITY_AMBIGUOUS');
    assert.equal((await pool.query('SELECT count(*)::int AS count FROM canonical_business_entities')).rows[0].count, 0);
  });

  it('J. failed relation write leaves no partial entities or facts', async () => {
    const source = await evidence(1, 'Babrun offers coaching.');
    await assert.rejects(commitCanonicalSemanticBatch(pool, batch({ key: 'J', evidenceInputs: [source],
      entities: [entity('OFFER', 'offer:j')], facts: [fact('offers', { type: 'ENTITY_REF', value: 'offer:j' })],
      relations: [{ from_fact_index: 0, to_fact_id: randomUUID(), relation_type: 'DEPENDS_ON' }] })),
    error => error.code === 'FACT_REFERENCE_UNRESOLVED');
    const state = await counts();
    assert.equal(state.canonical_business_entities, 0);
    assert.equal(state.canonical_business_facts, 0);
  });

  it('K. invalid manifest conflict reference prevents snapshot and rolls back', async () => {
    const source = await evidence(1, 'Babrun legal name is Babrun LLC.');
    await assert.rejects(commitCanonicalSemanticBatch(pool, batch({ key: 'K', evidenceInputs: [source],
      facts: [fact('legal_name', { type: 'STRING', value: 'Babrun LLC' })],
      conflicts: [{ conflict_set_id: randomUUID(), active_fact_ids: [randomUUID()], resolution_state: 'RESOLVED' }] })),
    error => ['FACT_REFERENCE_UNRESOLVED', 'CONFLICT_SET_UNRESOLVED'].includes(error.code));
    assert.equal((await pool.query('SELECT count(*)::int AS count FROM canonical_business_snapshots')).rows[0].count, 0);
  });

  it('L. snapshot manifest is the exact committed contribution membership', async () => {
    const source = await evidence(1, 'Babrun offers coaching.');
    const result = await commitCanonicalSemanticBatch(pool, batch({ key: 'L', evidenceInputs: [source],
      entities: [entity('OFFER', 'offer:l')], labels: [{ entity_identity_key: 'client:1', label: 'Babrun', assertion_kind: 'CANONICAL' }],
      facts: [fact('offers', { type: 'ENTITY_REF', value: 'offer:l' })] }));
    const manifest = (await pool.query('SELECT manifest FROM canonical_business_snapshots WHERE id=$1', [result.snapshot_id])).rows[0].manifest;
    for (const [field, table] of [['entity_ids', 'canonical_business_entities'], ['label_assertion_ids', 'canonical_entity_label_assertions'],
      ['fact_ids', 'canonical_business_facts'], ['fact_evidence_ref_ids', 'canonical_fact_evidence_refs']]) {
      const ids = (await pool.query(`SELECT id FROM ${table} WHERE tenant_id='tenant:babrun' ORDER BY id`)).rows.map(row => row.id);
      assert.deepEqual(manifest[field], ids);
    }
  });

  it('M. v1 batch validates against pinned v1 while v2 exists', async () => {
    const source = await evidence(1, 'Babrun LLC is the legal name.');
    const result = await commitCanonicalSemanticBatch(pool, batch({ key: 'M', registry: registryV1,
      evidenceInputs: [source], facts: [fact('legal_name', { type: 'STRING', value: 'Babrun LLC' })] }));
    assert.equal(result.registry_artifact_id, registryV1.id);
    assert.notEqual(result.registry_artifact_id, registryV2.id);
  });

  it('N. label evolution appends instead of mutating', async () => {
    const firstEvidence = await evidence(1, 'The business is Babrun.');
    const first = await commitCanonicalSemanticBatch(pool, batch({ key: 'N1', evidenceInputs: [firstEvidence],
      labels: [{ entity_identity_key: 'client:1', label: 'Babrun', assertion_kind: 'CANONICAL', evidence_id: firstEvidence.id }] }));
    const laterEvidence = await evidence(1, 'The current legal label is Babrun LLC.');
    const later = await commitCanonicalSemanticBatch(pool, batch({ key: 'N2', evidenceInputs: [laterEvidence],
      labels: [{ entity_identity_key: 'client:1', label: 'Babrun LLC', assertion_kind: 'CANONICAL', evidence_id: laterEvidence.id }] }));
    assert.equal((await pool.query('SELECT count(*)::int AS count FROM canonical_entity_label_assertions')).rows[0].count, 2);
    assert.notEqual(first.snapshot_id, later.snapshot_id);
    assert.equal(later.label_assertions_created.length, 1);
  });

  it('O. merge and revocation events commit append-only', async () => {
    const mergeEvidence = await evidence(1, 'Offer A and Offer B are the same offer.');
    await commitCanonicalSemanticBatch(pool, batch({ key: 'O1', evidenceInputs: [mergeEvidence],
      entities: [entity('OFFER', 'offer:o-a'), entity('OFFER', 'offer:o-b')],
      merges: [{ source_entity_identity_key: 'offer:o-a', target_entity_identity_key: 'offer:o-b', event_kind: 'MERGED', evidence_id: mergeEvidence.id }] }));
    const revokeEvidence = await evidence(1, 'Offer A and B are distinct.');
    await commitCanonicalSemanticBatch(pool, batch({ key: 'O2', evidenceInputs: [revokeEvidence],
      entities: [entity('OFFER', 'offer:o-a'), entity('OFFER', 'offer:o-b')],
      merges: [{ source_entity_identity_key: 'offer:o-a', target_entity_identity_key: 'offer:o-b', event_kind: 'MERGE_REVOKED', evidence_id: revokeEvidence.id }] }));
    assert.deepEqual((await pool.query('SELECT event_kind FROM canonical_entity_merge_events ORDER BY created_at')).rows,
      [{ event_kind: 'MERGED' }, { event_kind: 'MERGE_REVOKED' }]);
  });

  it('P. canonical path does not write legacy semantic stores', async () => {
    const source = await evidence(1, 'Babrun offers coaching.');
    await commitCanonicalSemanticBatch(pool, batch({ key: 'P', evidenceInputs: [source],
      entities: [entity('OFFER', 'offer:p')], facts: [fact('offers', { type: 'ENTITY_REF', value: 'offer:p' })] }));
    assert.equal((await pool.query('SELECT count(*)::int AS count FROM "normalizedFacts"')).rows[0].count, 0);
    assert.equal((await pool.query('SELECT count(*)::int AS count FROM business_facts')).rows[0].count, 0);
    assert.equal((await pool.query('SELECT count(*)::int AS count FROM knowledge_nodes')).rows[0].count, 0);
  });

  it('223C A/B/C/E/F/H/I/J/K/Q/R reconstructs immutable relationship-closed state', async () => {
    const firstEvidence = await evidence(1, 'Babrun offers current coaching.');
    const plannedEvidence = await evidence(1, 'Babrun intends to offer group coaching.');
    const outcomeEvidence = await evidence(1, 'Current coaching targets growth.');
    const first = await commitCanonicalSemanticBatch(pool, batch({ key: '223C-1', evidenceInputs: [firstEvidence, plannedEvidence, outcomeEvidence],
      entities: [entity('OFFER', 'offer:current'), entity('OFFER', 'offer:planned'), entity('OUTCOME', 'outcome:growth')],
      labels: [{ entity_identity_key: 'client:1', label: 'Babrun', assertion_kind: 'CANONICAL', evidence_id: firstEvidence.id }],
      facts: [fact('offers', { type: 'ENTITY_REF', value: 'offer:current' }),
        fact('offers', { type: 'ENTITY_REF', value: 'offer:planned' }, { temporal_status: 'PLANNED', modality: 'INTENDED' }),
        fact('targets_outcome', { type: 'ENTITY_REF', value: 'outcome:growth' }, { subject_entity_identity_key: 'offer:current' })] }));
    const laterEvidence = await evidence(1, 'Babrun LLC is the current label and still offers coaching.');
    const later = await commitCanonicalSemanticBatch(pool, batch({ key: '223C-2', evidenceInputs: [laterEvidence],
      entities: [entity('OFFER', 'offer:current')],
      labels: [{ entity_identity_key: 'client:1', label: 'Babrun LLC', assertion_kind: 'CANONICAL', evidence_id: laterEvidence.id }],
      facts: [fact('offers', { type: 'ENTITY_REF', value: 'offer:current' })] }));
    const firstProjection = await reconstructCanonicalSemanticProjection(pool, { tenant_id: 'tenant:babrun', snapshot_id: first.snapshot_id });
    const repeated = await reconstructCanonicalSemanticProjection(pool, { tenant_id: 'tenant:babrun', snapshot_id: first.snapshot_id });
    const laterProjection = await reconstructCanonicalSemanticProjection(pool, { tenant_id: 'tenant:babrun', snapshot_id: later.snapshot_id });
    assert.equal(firstProjection.domain_business_id, 1);
    assert.equal(firstProjection.registry_version, 'v1');
    assert.equal(firstProjection.entities.find(row => row.entity_type === 'BUSINESS').canonical_label, 'Babrun');
    assert.equal(laterProjection.entities.find(row => row.entity_type === 'BUSINESS').canonical_label, 'Babrun LLC');
    assert.equal(firstProjection.evidence_references.length, 3);
    assert.equal(laterProjection.evidence_references.length, 4);
    assert.ok(firstProjection.facts.every(row => row.subject_entity_id));
    assert.equal(firstProjection.facts.filter(row => row.active_at_evaluation).length, 3);
    assert.equal(firstProjection.facts.find(row => row.temporal_status === 'PLANNED').modality, 'INTENDED');
    assert.equal(firstProjection.projection_digest, repeated.projection_digest);
  });

  it('223C orders equal-time canonical labels by snapshot lineage instead of adversarial UUIDs', async () => {
    const fixedTime = '2026-09-01T12:00:00.000Z';
    const labels = [
      ['ffffffff-ffff-4fff-8fff-ffffffffffff', 'Babrun'],
      ['00000000-0000-4000-8000-000000000001', 'Babrun LLC'],
      ['11111111-1111-4111-8111-111111111111', 'Babrun Group'],
    ];
    const snapshots = [];
    try {
      await pool.query(`ALTER TABLE canonical_entity_label_assertions
        ALTER COLUMN created_at SET DEFAULT '${fixedTime}'::timestamptz`);
      for (let index = 0; index < labels.length; index += 1) {
        const [id, label] = labels[index];
        await pool.query(`ALTER TABLE canonical_entity_label_assertions
          ALTER COLUMN id SET DEFAULT '${id}'::uuid`);
        const source = await evidence(1, `${label} is the current canonical label.`);
        snapshots.push(await commitCanonicalSemanticBatch(pool, batch({ key: `223C-label-${index}`,
          evidenceInputs: [source], labels: [{ entity_identity_key: 'client:1', label,
            assertion_kind: 'CANONICAL', evidence_id: source.id }] })));
      }
    } finally {
      await pool.query(`ALTER TABLE canonical_entity_label_assertions
        ALTER COLUMN id SET DEFAULT gen_random_uuid(),
        ALTER COLUMN created_at SET DEFAULT NOW()`);
    }
    for (let index = 0; index < snapshots.length; index += 1) {
      const projection = await reconstructCanonicalSemanticProjection(pool, {
        tenant_id: 'tenant:babrun', snapshot_id: snapshots[index].snapshot_id,
      });
      assert.equal(projection.entities.find(row => row.entity_type === 'BUSINESS').canonical_label,
        labels[index][1]);
    }
  });

  it('223C L/M/N/O resolves snapshot conflict, merge, and temporal history', async () => {
    const one = await evidence(1, 'Babrun LLC is the legal name.');
    const single = await commitCanonicalSemanticBatch(pool, batch({ key: '223C-single', evidenceInputs: [one],
      facts: [fact('legal_name', { type: 'STRING', value: 'Babrun LLC' })] }));
    const projection = await reconstructCanonicalSemanticProjection(pool, { tenant_id: 'tenant:babrun', snapshot_id: single.snapshot_id });
    assert.equal(projection.conflict_resolutions[0].active_fact_ids.length, 1);
    const mergeEvidence = await evidence(1, 'Two offers are one.');
    const merged = await commitCanonicalSemanticBatch(pool, batch({ key: '223C-merge', evidenceInputs: [mergeEvidence],
      entities: [entity('OFFER', 'offer:merge-a'), entity('OFFER', 'offer:merge-b')],
      merges: [{ source_entity_identity_key: 'offer:merge-a', target_entity_identity_key: 'offer:merge-b', event_kind: 'MERGED', evidence_id: mergeEvidence.id }] }));
    const revokeEvidence = await evidence(1, 'The offers are distinct.');
    const revoked = await commitCanonicalSemanticBatch(pool, batch({ key: '223C-revoke', evidenceInputs: [revokeEvidence],
      entities: [entity('OFFER', 'offer:merge-a'), entity('OFFER', 'offer:merge-b')],
      merges: [{ source_entity_identity_key: 'offer:merge-a', target_entity_identity_key: 'offer:merge-b', event_kind: 'MERGE_REVOKED', evidence_id: revokeEvidence.id }] }));
    assert.equal((await reconstructCanonicalSemanticProjection(pool, { tenant_id: 'tenant:babrun', snapshot_id: merged.snapshot_id })).resolved_merges.length, 1);
    assert.equal((await reconstructCanonicalSemanticProjection(pool, { tenant_id: 'tenant:babrun', snapshot_id: revoked.snapshot_id })).resolved_merges.length, 0);
  });

  it('223C fail-closes corrupted manifests and tenant mismatches', async () => {
    const source = await evidence(1, 'Babrun offers coaching.');
    const result = await commitCanonicalSemanticBatch(pool, batch({ key: '223C-corrupt', evidenceInputs: [source],
      entities: [entity('OFFER', 'offer:corrupt')], facts: [fact('offers', { type: 'ENTITY_REF', value: 'offer:corrupt' })] }));
    await assert.rejects(reconstructCanonicalSemanticProjection(pool, { tenant_id: 'tenant:other', snapshot_id: result.snapshot_id }),
      error => error.code === 'SNAPSHOT_NOT_FOUND');
    await pool.query('ALTER TABLE canonical_business_snapshots DISABLE TRIGGER canonical_business_snapshots_append_only');
    await pool.query('ALTER TABLE canonical_business_snapshots DISABLE TRIGGER canonical_snapshot_digest_trigger');
    await pool.query(`UPDATE canonical_business_snapshots SET manifest='{}'::jsonb WHERE tenant_id='tenant:babrun' AND id=$1`, [result.snapshot_id]);
    await assert.rejects(reconstructCanonicalSemanticProjection(pool, { tenant_id: 'tenant:babrun', snapshot_id: result.snapshot_id }),
      error => error.code === 'MANIFEST_DIGEST_INVALID');
  });
});