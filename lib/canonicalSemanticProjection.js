'use strict';

const { CanonicalSemanticError, canonicalJsonString, hash } = require('./canonicalSemanticWrite');

const MANIFEST_MEMBERS = {
  entity_ids: 'canonical_business_entities',
  label_assertion_ids: 'canonical_entity_label_assertions',
  fact_ids: 'canonical_business_facts',
  fact_evidence_ref_ids: 'canonical_fact_evidence_refs',
  fact_relation_ids: 'canonical_fact_relations',
  merge_event_ids: 'canonical_entity_merge_events',
};

function fail(code, message, details = {}) {
  throw new CanonicalSemanticError(code, message, details);
}

function sorted(values, key = value => value.id) {
  return [...values].sort((left, right) => String(key(left)).localeCompare(String(key(right))));
}

function parseEvaluationContext(input, snapshot) {
  const context = input || {};
  const point = context.at || context.evaluation_time || snapshot.created_at;
  const from = context.from || context.valid_from || point;
  const to = context.to || context.valid_to || point;
  const fromDate = new Date(from);
  const toDate = new Date(to);
  if (Number.isNaN(fromDate.valueOf()) || Number.isNaN(toDate.valueOf()) || fromDate > toDate) {
    fail('EVALUATION_CONTEXT_INVALID', 'evaluation context must contain a valid instant or interval');
  }
  return { from: fromDate.toISOString(), to: toDate.toISOString() };
}

async function rowsForManifest(client, tenantId, manifest, field, columns = '*') {
  const ids = manifest[field] || [];
  if (!Array.isArray(ids) || new Set(ids).size !== ids.length) fail('MANIFEST_INVALID', `${field} must contain unique IDs`);
  if (!ids.length) return [];
  const rows = (await client.query(`SELECT ${columns} FROM ${MANIFEST_MEMBERS[field]}
    WHERE tenant_id=$1 AND id=ANY($2::uuid[])`, [tenantId, ids])).rows;
  if (rows.length !== ids.length) fail('MANIFEST_REFERENCE_INVALID', `${field} has a missing or cross-tenant member`);
  return rows;
}

function containsCycle(edges) {
  const graph = new Map();
  for (const [from, to] of edges) graph.set(from, [...(graph.get(from) || []), to]);
  const visiting = new Set();
  const visited = new Set();
  const visit = node => {
    if (visiting.has(node)) return true;
    if (visited.has(node)) return false;
    visiting.add(node);
    for (const next of graph.get(node) || []) if (visit(next)) return true;
    visiting.delete(node);
    visited.add(node);
    return false;
  };
  return [...graph.keys()].some(visit);
}

function temporalActive(fact, context) {
  const from = new Date(context.from);
  const to = new Date(context.to);
  const validFrom = fact.valid_from ? new Date(fact.valid_from) : null;
  const validTo = fact.valid_to ? new Date(fact.valid_to) : null;
  return fact.temporal_status !== 'RETIRED'
    && (!validFrom || validFrom <= to)
    && (!validTo || validTo >= from);
}

function objectEntityId(value) {
  return value?.type === 'ENTITY_REF' ? value.value : null;
}

async function reconstructCanonicalSemanticProjection(pool, input = {}) {
  const { tenant_id: tenantId, snapshot_id: snapshotId } = input;
  if (!tenantId || !snapshotId) fail('SNAPSHOT_ID_REQUIRED', 'tenant_id and explicit snapshot_id are required');
  const client = await pool.connect();
  try {
    await client.query('BEGIN READ ONLY');
    const snapshot = (await client.query(`SELECT * FROM canonical_business_snapshots
      WHERE tenant_id=$1 AND id=$2`, [tenantId, snapshotId])).rows[0];
    if (!snapshot) fail('SNAPSHOT_NOT_FOUND', 'snapshot does not exist for this tenant');
    const computedManifestDigest = (await client.query(
      `SELECT encode(digest(manifest::text,'sha256'),'hex') AS digest FROM canonical_business_snapshots
       WHERE tenant_id=$1 AND id=$2`, [tenantId, snapshotId])).rows[0].digest;
    if (computedManifestDigest !== snapshot.manifest_digest) fail('MANIFEST_DIGEST_INVALID', 'snapshot manifest digest is invalid');
    const manifest = snapshot.manifest;
    if (!manifest || typeof manifest !== 'object') fail('MANIFEST_INVALID', 'snapshot manifest is invalid');
    const registry = (await client.query(`SELECT *, encode(digest(jsonb_build_object(
      'entity_vocabulary',entity_vocabulary,'predicate_definitions',predicate_definitions,
      'registry_version',registry_version)::text,'sha256'),'hex') AS computed_digest
      FROM canonical_registry_artifacts WHERE id=$1`, [snapshot.registry_artifact_id])).rows[0];
    if (!registry || registry.registry_version !== snapshot.registry_version
      || registry.content_digest !== snapshot.registry_content_digest || registry.computed_digest !== registry.content_digest) {
      fail('REGISTRY_MISMATCH', 'snapshot pinned registry is missing or has an invalid digest');
    }
    if (manifest.registry_artifact_id !== snapshot.registry_artifact_id
      || manifest.registry_version !== snapshot.registry_version
      || manifest.registry_content_digest !== snapshot.registry_content_digest) {
      fail('MANIFEST_REGISTRY_MISMATCH', 'manifest registry pin differs from snapshot');
    }
    const [entities, labels, facts, evidence, relations, merges] = await Promise.all([
      rowsForManifest(client, tenantId, manifest, 'entity_ids'),
      rowsForManifest(client, tenantId, manifest, 'label_assertion_ids'),
      rowsForManifest(client, tenantId, manifest, 'fact_ids'),
      rowsForManifest(client, tenantId, manifest, 'fact_evidence_ref_ids'),
      rowsForManifest(client, tenantId, manifest, 'fact_relation_ids'),
      rowsForManifest(client, tenantId, manifest, 'merge_event_ids'),
    ]);
    const lineage = (await client.query(`WITH RECURSIVE chain AS (
      SELECT id, committed_batch_id, supersedes_snapshot_id, 0 AS depth
      FROM canonical_business_snapshots WHERE tenant_id=$1 AND id=$2
      UNION ALL
      SELECT previous.id, previous.committed_batch_id, previous.supersedes_snapshot_id, chain.depth + 1
      FROM canonical_business_snapshots previous JOIN chain ON previous.id=chain.supersedes_snapshot_id
      WHERE previous.tenant_id=$1
    ) SELECT committed_batch_id, depth FROM chain`, [tenantId, snapshotId])).rows;
    const batchOrder = new Map(lineage.map(row => [row.committed_batch_id, -row.depth]));
    const entityIds = new Set(entities.map(row => row.id));
    const factIds = new Set(facts.map(row => row.id));
    const vocabulary = new Set(Array.isArray(registry.entity_vocabulary)
      ? registry.entity_vocabulary : Object.keys(registry.entity_vocabulary || {}));
    const business = entities.filter(row => row.entity_type === 'BUSINESS');
    if (business.length !== 1 || !Number.isInteger(Number(business[0].domain_client_id))) {
      fail('BUSINESS_BINDING_INVALID', 'snapshot must contain exactly one bound BUSINESS entity');
    }
    for (const entity of entities) if (!vocabulary.has(entity.entity_type)) fail('ENTITY_TYPE_INVALID', 'entity type is unknown to pinned registry');
    for (const fact of facts) {
      if (!entityIds.has(fact.subject_entity_id)) fail('DANGLING_REFERENCE', 'fact subject is absent from manifest');
      if (!registry.predicate_definitions?.[fact.predicate]) fail('PREDICATE_INVALID', 'fact predicate is unknown to pinned registry');
      const objectId = objectEntityId(fact.object_value);
      if (objectId && !entityIds.has(objectId)) fail('DANGLING_REFERENCE', 'fact object is absent from manifest');
      for (const condition of fact.qualifiers?.condition_refs || []) {
        if (!factIds.has(condition.fact_id)) fail('DANGLING_REFERENCE', 'fact condition is absent from manifest');
      }
    }
    for (const row of labels) if (!entityIds.has(row.entity_id)) fail('DANGLING_REFERENCE', 'label entity is absent from manifest');
    for (const row of evidence) if (!factIds.has(row.fact_id)) fail('DANGLING_REFERENCE', 'evidence fact is absent from manifest');
    for (const row of relations) if (!factIds.has(row.from_fact_id) || !factIds.has(row.to_fact_id)) fail('DANGLING_REFERENCE', 'relation endpoint is absent from manifest');
    if (containsCycle(relations.filter(row => ['SUPERSEDES', 'CORRECTION_OF'].includes(row.relation_type))
      .map(row => [row.from_fact_id, row.to_fact_id]))) fail('SUPERSESSION_CYCLE', 'snapshot contains a revision cycle');

    const activeMerges = new Map();
    for (const row of [...merges].sort((a, b) => (batchOrder.get(a.created_by_batch_id) - batchOrder.get(b.created_by_batch_id))
      || `${a.created_at}:${a.id}`.localeCompare(`${b.created_at}:${b.id}`))) {
      if (!entityIds.has(row.source_entity_id) || !entityIds.has(row.target_entity_id)) fail('DANGLING_REFERENCE', 'merge endpoint is absent from manifest');
      const key = `${row.source_entity_id}:${row.target_entity_id}`;
      if (row.event_kind === 'MERGED') activeMerges.set(key, [row.source_entity_id, row.target_entity_id]);
      if (row.event_kind === 'MERGE_REVOKED') activeMerges.delete(key);
    }
    if (containsCycle([...activeMerges.values()])) fail('MERGE_CYCLE', 'snapshot contains a merge cycle');
    const resolvedEntityId = id => {
      const edge = [...activeMerges.values()].find(([source]) => source === id);
      return edge ? resolvedEntityId(edge[1]) : id;
    };
    const conflictIds = [...new Set((manifest.conflict_set_resolutions || []).map(row => row.conflict_set_id))];
    if (conflictIds.length !== (manifest.conflict_set_resolutions || []).length) fail('CONFLICT_RESOLUTION_INVALID', 'duplicate conflict resolution');
    const conflictRows = conflictIds.length ? (await client.query(`SELECT * FROM canonical_conflict_sets
      WHERE tenant_id=$1 AND id=ANY($2::uuid[])`, [tenantId, conflictIds])).rows : [];
    if (conflictRows.length !== conflictIds.length) fail('CONFLICT_RESOLUTION_INVALID', 'conflict set is missing or cross-tenant');
    const conflictById = new Map(conflictRows.map(row => [row.id, row]));
    const activeFactIds = new Set();
    const conflictResolutions = sorted(manifest.conflict_set_resolutions || [], row => row.conflict_set_id).map(resolution => {
      const conflict = conflictById.get(resolution.conflict_set_id);
      const selected = [...new Set(resolution.active_fact_ids || [])].sort();
      if (!conflict || !['RESOLVED', 'UNRESOLVED'].includes(resolution.resolution_state)
        || selected.some(id => !factIds.has(id) || facts.find(fact => fact.id === id).conflict_set_id !== conflict.id)) {
        fail('CONFLICT_RESOLUTION_INVALID', 'conflict selection is impossible');
      }
      if (resolution.resolution_state === 'UNRESOLVED' && selected.length) fail('CONFLICT_RESOLUTION_INVALID', 'unresolved conflict selects facts');
      if (['SINGLE', 'OPTIONAL_SINGLE'].includes(conflict.slot_cardinality) && selected.length > 1) fail('CONFLICT_CARDINALITY_VIOLATED', 'exclusive slot has multiple selected facts');
      if (conflict.slot_cardinality === 'SINGLE' && resolution.resolution_state === 'RESOLVED' && selected.length !== 1) fail('CONFLICT_CARDINALITY_VIOLATED', 'SINGLE slot requires one selected fact');
      selected.forEach(id => activeFactIds.add(id));
      return { conflict_set_id: conflict.id, resolution_state: resolution.resolution_state, active_fact_ids: selected };
    });
    const evaluationContext = parseEvaluationContext(input.evaluation_context, snapshot);
    const labelsByEntity = new Map();
    for (const label of labels) labelsByEntity.set(label.entity_id, [...(labelsByEntity.get(label.entity_id) || []), label]);
    const projectedEntities = sorted(entities).map(entity => {
      const assertions = labelsByEntity.get(entity.id) || [];
      if (assertions.some(row => !batchOrder.has(row.created_by_batch_id))) {
        fail('LABEL_ASSERTION_ORDER_INVALID', 'label assertion batch is absent from snapshot lineage');
      }
      const canonicalAssertions = assertions.filter(row => row.assertion_kind === 'CANONICAL');
      const latestCanonicalOrder = canonicalAssertions.length
        ? Math.max(...canonicalAssertions.map(row => batchOrder.get(row.created_by_batch_id))) : null;
      const latestCanonical = canonicalAssertions.filter(row =>
        batchOrder.get(row.created_by_batch_id) === latestCanonicalOrder);
      if (latestCanonical.length > 1) {
        fail('LABEL_ASSERTION_ORDER_AMBIGUOUS', 'multiple canonical labels share the latest interpretation batch');
      }
      const canonical = latestCanonical[0];
      return { id: entity.id, entity_type: entity.entity_type, identity_key: entity.identity_key,
        domain_client_id: entity.domain_client_id, canonical_label: canonical?.label || null,
        aliases: assertions.filter(row => row.assertion_kind === 'ALIAS').map(row => row.label).sort(),
        resolved_entity_id: resolvedEntityId(entity.id) };
    });
    const projectedFacts = sorted(facts).map(fact => ({ id: fact.id, subject_entity_id: fact.subject_entity_id,
      predicate: fact.predicate, object_value: fact.object_value, qualifiers: fact.qualifiers,
      epistemic_state: fact.epistemic_state, epistemic_confidence: fact.epistemic_confidence,
      interpretation_confidence: fact.interpretation_confidence, temporal_status: fact.temporal_status,
      valid_from: fact.valid_from, valid_to: fact.valid_to, modality: fact.modality,
      conflict_set_id: fact.conflict_set_id, selected_in_conflict: activeFactIds.has(fact.id),
      active_at_evaluation: activeFactIds.has(fact.id) && temporalActive(fact, evaluationContext) }));
    const projection = { snapshot_id: snapshot.id, tenant_id: tenantId, domain_business_id: business[0].domain_client_id,
      registry_artifact_id: registry.id, registry_version: registry.registry_version, evaluation_context: evaluationContext,
      entities: projectedEntities, facts: projectedFacts,
      evidence_references: sorted(evidence), relations: sorted(relations),
      resolved_merges: sorted([...activeMerges.values()].map(([source_entity_id, target_entity_id]) => ({ source_entity_id, target_entity_id })), row => `${row.source_entity_id}:${row.target_entity_id}`),
      conflict_resolutions: conflictResolutions, warnings: [] };
    projection.projection_digest = hash(projection);
    await client.query('COMMIT');
    return Object.freeze(projection);
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

module.exports = { reconstructCanonicalSemanticProjection };