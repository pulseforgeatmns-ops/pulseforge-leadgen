'use strict';

const crypto = require('crypto');

const HEX_64 = /^[0-9a-f]{64}$/;
const ENTITY_TYPES = new Set(['BUSINESS', 'OFFER', 'PROGRAM', 'CUSTOMER_PROFILE', 'PAIN', 'CAPABILITY', 'OUTCOME', 'OBJECTIVE', 'METRIC']);
const EPISTEMIC_STATES = new Set(['KNOWN', 'HYPOTHESIS', 'UNKNOWN', 'UNRESOLVED', 'NOT_APPLICABLE']);
const TEMPORAL_STATUSES = new Set(['CURRENT', 'PLANNED', 'HISTORICAL', 'RETIRED']);
const MODALITIES = new Set(['ACTUAL', 'INTENDED', 'CONDITIONAL']);
const RELATION_TYPES = new Set(['SUPERSEDES', 'CORRECTION_OF', 'CONTRADICTS', 'DEPENDS_ON']);
const MERGE_TYPES = new Set(['MERGED', 'MERGE_REVOKED', 'SPLIT_SUCCESSOR']);
const SUPPORT_TYPES = new Set(['DIRECT', 'INFERRED', 'OPERATOR_CONFIRMED']);

function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonicalize(value[key])]));
  }
  return value;
}

const canonicalJsonString = value => JSON.stringify(canonicalize(value));
const hash = value => crypto.createHash('sha256')
  .update(typeof value === 'string' ? value : canonicalJsonString(value)).digest('hex');
const asArray = value => value == null ? [] : (Array.isArray(value) ? value : [value]);
const uniqueSorted = values => [...new Set((values || []).filter(Boolean))].sort();

function deriveInterpretationBatchKey(batch, evidenceById) {
  return hash({
    tenant_id: batch.tenant_id,
    evidence_inputs: batch.ordered_evidence_input_ids.map((evidenceId, inputOrdinal) => ({
      evidence_id: evidenceId,
      source_text_sha256: evidenceById.get(evidenceId)?.source_text_sha256,
      input_ordinal: inputOrdinal,
    })),
    interpreter_id: batch.interpreter_id,
    interpreter_version: batch.interpreter_version,
    semantic_model_version: batch.semantic_model_version,
    registry_artifact_id: batch.registry_artifact_id,
    registry_content_digest: batch.registry_content_digest,
  });
}

class CanonicalSemanticError extends Error {
  constructor(code, message, details = {}) {
    super(`${code}: ${message}`);
    this.name = 'CanonicalSemanticError';
    this.code = code;
    this.details = details;
  }
}

function fail(code, message, details) {
  throw new CanonicalSemanticError(code, message, details);
}

class CanonicalSemanticBatch {
  constructor(input = {}) {
    for (const field of ['tenant_id', 'registry_artifact_id', 'registry_version', 'registry_content_digest',
      'interpreter_id', 'interpreter_version', 'semantic_model_version', 'idempotency_key']) this[field] = input[field];
    for (const field of ['ordered_evidence_input_ids', 'semantic_entities', 'label_assertions', 'semantic_facts',
      'fact_evidence_links', 'fact_relations', 'entity_merge_events', 'conflict_set_resolutions']) {
      this[field] = input[field] || [];
    }
    this.snapshot_metadata = input.snapshot_metadata || {};
  }

  validate() {
    const errors = [];
    for (const field of ['tenant_id', 'registry_artifact_id', 'registry_version', 'interpreter_id', 'interpreter_version']) {
      if (typeof this[field] !== 'string' || !this[field].trim()) errors.push(`${field} is required`);
    }
    if (!HEX_64.test(this.registry_content_digest || '')) errors.push('registry_content_digest must be a SHA-256 digest');
    if (!HEX_64.test(this.idempotency_key || '')) errors.push('idempotency_key must be a SHA-256 digest');
    if (!Number.isInteger(this.semantic_model_version) || this.semantic_model_version < 1) errors.push('semantic_model_version must be positive');
    for (const field of ['ordered_evidence_input_ids', 'semantic_entities', 'label_assertions', 'semantic_facts',
      'fact_evidence_links', 'fact_relations', 'entity_merge_events', 'conflict_set_resolutions']) {
      if (!Array.isArray(this[field])) errors.push(`${field} must be an array`);
    }
    return { valid: !errors.length, errors };
  }
}

class CanonicalCommitResult {
  constructor(values) {
    Object.assign(this, { entity_ids_created: [], entity_ids_reused: [], fact_ids_created: [], fact_ids_reused: [],
      evidence_links_created: [], evidence_links_reused: [], relations_created: [], label_assertions_created: [],
      label_assertions_reused: [], merge_events_created: [], conflict_results: [] }, values);
  }
}

function vocabularyTypes(value) {
  return new Set(Array.isArray(value) ? value : Object.keys(value || {}));
}

async function loadRegistry(client, batch) {
  const row = (await client.query(`SELECT *, encode(digest(jsonb_build_object(
      'entity_vocabulary',entity_vocabulary,'predicate_definitions',predicate_definitions,
      'registry_version',registry_version)::text,'sha256'),'hex') AS computed_digest
    FROM canonical_registry_artifacts WHERE id=$1`, [batch.registry_artifact_id])).rows[0];
  if (!row) fail('REGISTRY_NOT_FOUND', 'pinned registry artifact does not exist');
  if (row.registry_version !== batch.registry_version || row.content_digest !== batch.registry_content_digest
      || row.computed_digest !== row.content_digest) fail('REGISTRY_MISMATCH', 'pinned registry identity or digest is invalid');
  if (!vocabularyTypes(row.entity_vocabulary).size || !row.predicate_definitions || Array.isArray(row.predicate_definitions)) {
    fail('REGISTRY_INVALID', 'registry must contain a closed vocabulary and predicate map');
  }
  return row;
}

async function loadEvidence(client, batch, clientId) {
  if (new Set(batch.ordered_evidence_input_ids).size !== batch.ordered_evidence_input_ids.length) fail('EVIDENCE_DUPLICATE', 'duplicate ordered evidence input');
  if (!batch.ordered_evidence_input_ids.length) return new Map();
  const rows = (await client.query(`SELECT id,client_id,statement,source_text_sha256,
      encode(digest(statement,'sha256'),'hex') AS computed_digest
    FROM cie_evidence WHERE id=ANY($1::uuid[])`, [batch.ordered_evidence_input_ids])).rows;
  if (rows.length !== batch.ordered_evidence_input_ids.length) fail('EVIDENCE_NOT_FOUND', 'an ordered evidence input does not exist');
  const evidence = new Map(rows.map(row => [row.id, row]));
  for (const id of batch.ordered_evidence_input_ids) {
    const row = evidence.get(id);
    if (Number(row.client_id) !== clientId) fail('EVIDENCE_TENANT_MISMATCH', `evidence ${id} belongs to another client`);
    if (!row.source_text_sha256 || row.source_text_sha256 !== row.computed_digest) fail('EVIDENCE_DIGEST_INVALID', `evidence ${id} is not immutable`);
  }
  return evidence;
}

async function insertBatch(client, batch, evidence) {
  const id = (await client.query(`INSERT INTO canonical_interpretation_batches
      (tenant_id,registry_artifact_id,registry_version,registry_content_digest,interpreter_id,
       interpreter_version,semantic_model_version,idempotency_key,status,committed_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'COMMITTED',NOW()) RETURNING id`,
  [batch.tenant_id, batch.registry_artifact_id, batch.registry_version, batch.registry_content_digest,
    batch.interpreter_id, batch.interpreter_version, batch.semantic_model_version, batch.idempotency_key])).rows[0].id;
  for (let ordinal = 0; ordinal < batch.ordered_evidence_input_ids.length; ordinal += 1) {
    const evidenceId = batch.ordered_evidence_input_ids[ordinal];
    await client.query(`INSERT INTO canonical_batch_evidence_inputs
      (tenant_id,batch_id,evidence_id,source_text_sha256,input_ordinal) VALUES ($1,$2,$3,$4,$5)`,
    [batch.tenant_id, id, evidenceId, evidence.get(evidenceId).source_text_sha256, ordinal]);
  }
  return id;
}

async function resolveEntities(client, batch, batchId, registry) {
  const businesses = batch.semantic_entities.filter(entity => entity.entity_type === 'BUSINESS');
  if (businesses.length !== 1 || !Number.isInteger(Number(businesses[0].domain_client_id))) fail('BUSINESS_ENTITY_REQUIRED', 'exactly one BUSINESS binding is required');
  const allowed = vocabularyTypes(registry.entity_vocabulary);
  const refs = new Map();
  const created = [];
  const reused = [];
  for (const entity of batch.semantic_entities) {
    if (!allowed.has(entity.entity_type) || !ENTITY_TYPES.has(entity.entity_type)) fail('ENTITY_TYPE_INVALID', `${entity.entity_type} is not registered`);
    if (entity.ambiguous_candidates?.length > 1) fail('ENTITY_IDENTITY_AMBIGUOUS', 'multiple entity candidates supplied');
    if (!entity.identity_key?.trim()) fail('ENTITY_IDENTITY_MISSING', 'canonical identity_key is required');
    if (refs.has(entity.identity_key) || (entity.input_id && refs.has(entity.input_id))) fail('ENTITY_REFERENCE_DUPLICATE', `duplicate reference ${entity.identity_key}`);
    if (entity.entity_type !== 'BUSINESS' && entity.domain_client_id != null) fail('ENTITY_DOMAIN_INVALID', 'only BUSINESS can bind a client');
    let row;
    if (entity.existing_entity_id) {
      row = (await client.query('SELECT * FROM canonical_business_entities WHERE id=$1', [entity.existing_entity_id])).rows[0];
      if (!row || row.tenant_id !== batch.tenant_id) fail('ENTITY_TENANT_MISMATCH', 'existing entity is not tenant-local');
      if (row.entity_type !== entity.entity_type || row.identity_key !== entity.identity_key) fail('ENTITY_IDENTITY_MISMATCH', 'existing entity identity differs');
      reused.push(row.id);
    } else {
      row = (await client.query(`SELECT * FROM canonical_business_entities
        WHERE tenant_id=$1 AND entity_type=$2 AND identity_key=$3`, [batch.tenant_id, entity.entity_type, entity.identity_key])).rows[0];
      if (row) reused.push(row.id);
      else {
        row = (await client.query(`INSERT INTO canonical_business_entities
          (tenant_id,entity_type,identity_key,domain_client_id,created_by_batch_id)
          VALUES ($1,$2,$3,$4,$5) RETURNING *`, [batch.tenant_id, entity.entity_type, entity.identity_key,
          entity.entity_type === 'BUSINESS' ? Number(entity.domain_client_id) : null, batchId])).rows[0];
        created.push(row.id);
      }
    }
    if (entity.entity_type === 'BUSINESS' && Number(row.domain_client_id) !== Number(entity.domain_client_id)) fail('BUSINESS_BINDING_INVALID', 'BUSINESS is bound to another client');
    refs.set(entity.identity_key, row);
    if (entity.input_id) refs.set(entity.input_id, row);
  }
  return { refs, created, reused, business: refs.get(businesses[0].identity_key) };
}

function resolveObject(value, refs) {
  if (value == null) return null;
  if (typeof value !== 'object' || Array.isArray(value) || !value.type) fail('FACT_OBJECT_INVALID', 'object_value must be typed or null');
  if (String(value.type).toUpperCase() !== 'ENTITY_REF') return canonicalize(value);
  const entity = refs.get(value.entity_identity_key || value.value);
  if (!entity) fail('ENTITY_REFERENCE_UNRESOLVED', 'object entity was not resolved');
  return { type: 'ENTITY_REF', value: entity.id, entity_type: entity.entity_type, tenant_id: entity.tenant_id };
}

function validateFact(fact, subject, object, definition) {
  if (!definition) fail('PREDICATE_INVALID', `${fact.predicate} is not registered`);
  const domains = asArray(definition.domain || definition.subject_types);
  if (!domains.length || (!domains.includes('*') && !domains.includes(subject.entity_type))) fail('PREDICATE_DOMAIN_INVALID', `${fact.predicate} rejects ${subject.entity_type}`);
  if (!EPISTEMIC_STATES.has(fact.epistemic_state)) fail('EPISTEMIC_STATE_INVALID', 'invalid epistemic_state');
  if (['UNKNOWN', 'UNRESOLVED', 'NOT_APPLICABLE'].includes(fact.epistemic_state) && object !== null) fail('FACT_OBJECT_INVALID', `${fact.epistemic_state} requires null`);
  if (!TEMPORAL_STATUSES.has(fact.temporal_status) || !MODALITIES.has(fact.modality)) fail('TEMPORAL_MODAL_INVALID', 'invalid temporal/modal state');
  if (fact.temporal_status === 'PLANNED' && fact.modality === 'ACTUAL') fail('TEMPORAL_MODAL_INVALID', 'PLANNED cannot be ACTUAL');
  const allowedTemporal = asArray(definition.temporal_statuses || definition.allowed_temporal_statuses);
  const allowedModalities = asArray(definition.modalities || definition.allowed_modalities);
  if (allowedTemporal.length && !allowedTemporal.includes(fact.temporal_status)) fail('TEMPORAL_STATUS_INVALID', `${fact.predicate} rejects ${fact.temporal_status}`);
  if (allowedModalities.length && !allowedModalities.includes(fact.modality)) fail('MODALITY_INVALID', `${fact.predicate} rejects ${fact.modality}`);
  if (!Number.isFinite(fact.interpretation_confidence) || fact.interpretation_confidence < 0 || fact.interpretation_confidence > 1) fail('INTERPRETATION_CONFIDENCE_INVALID', 'confidence must be 0..1');
  const entityRange = definition.range?.kind === 'ENTITY' ? asArray(definition.range.entity_types)
    : (typeof definition.range === 'string' && ENTITY_TYPES.has(definition.range) ? [definition.range] : []);
  if (entityRange.length && (object?.type !== 'ENTITY_REF' || !entityRange.includes(object.entity_type))) fail('PREDICATE_RANGE_INVALID', `${fact.predicate} has invalid range`);
  if (definition.range?.kind === 'LITERAL') {
    const types = asArray(definition.range.literal_types).map(type => String(type).toUpperCase());
    if (object !== null && !types.includes(String(object.type).toUpperCase())) fail('PREDICATE_RANGE_INVALID', `${fact.predicate} has invalid literal range`);
  }
  if (fact.valid_from && fact.valid_to && new Date(fact.valid_from) > new Date(fact.valid_to)) fail('TEMPORAL_RANGE_INVALID', 'invalid validity interval');
}

function slotKey(batch, subject, fact, object, definition) {
  if (!['SINGLE', 'OPTIONAL_SINGLE', 'SET', 'ORDERED_SET'].includes(definition.cardinality)) fail('REGISTRY_CARDINALITY_INVALID', 'invalid cardinality');
  const scope = {};
  for (const key of asArray(definition.slot_qualifiers)) {
    if (!(key in (fact.qualifiers || {}))) fail('SEMANTIC_SLOT_INVALID', `missing ${key}`);
    scope[key] = fact.qualifiers[key];
  }
  return hash({ tenant_id: batch.tenant_id, subject_entity_id: subject.id, predicate: fact.predicate,
    object_identity: ['SET', 'ORDERED_SET'].includes(definition.cardinality) ? object : null,
    scope, valid_from: fact.valid_from || null, valid_to: fact.valid_to || null });
}

async function resolveFacts(client, batch, batchId, registry, entityRefs) {
  const created = [], reused = [], byIndex = new Map(), byId = new Map();
  for (let index = 0; index < batch.semantic_facts.length; index += 1) {
    const fact = batch.semantic_facts[index];
    const subject = entityRefs.get(fact.subject_entity_identity_key || fact.subject_ref);
    if (!subject) fail('ENTITY_REFERENCE_UNRESOLVED', `fact ${index} subject unresolved`);
    const definition = registry.predicate_definitions[fact.predicate];
    const object = resolveObject(fact.object_value, entityRefs);
    validateFact(fact, subject, object, definition);
    const semanticSlotKey = slotKey(batch, subject, fact, object, definition);
    if (fact.semantic_slot_key && fact.semantic_slot_key !== semanticSlotKey) fail('SEMANTIC_SLOT_MISMATCH', 'supplied slot differs from registry derivation');
    let conflict = (await client.query(`SELECT id,slot_cardinality FROM canonical_conflict_sets
      WHERE tenant_id=$1 AND semantic_slot_key=$2`, [batch.tenant_id, semanticSlotKey])).rows[0];
    if (!conflict) conflict = (await client.query(`INSERT INTO canonical_conflict_sets
      (tenant_id,semantic_slot_key,predicate,subject_entity_id,slot_cardinality,created_by_batch_id)
      VALUES ($1,$2,$3,$4,$5,$6) RETURNING id,slot_cardinality`,
    [batch.tenant_id, semanticSlotKey, fact.predicate, subject.id, definition.cardinality, batchId])).rows[0];
    if (conflict.slot_cardinality !== definition.cardinality) fail('CONFLICT_REGISTRY_MISMATCH', 'slot cardinality differs from pinned registry');
    const conditions = asArray(fact.condition_refs).map(ref => {
      const target = Number.isInteger(ref.fact_index) ? byIndex.get(ref.fact_index) : byId.get(ref.fact_id);
      if (!target) fail('FACT_REFERENCE_UNRESOLVED', `fact ${index} condition unresolved`);
      if (!['EXACT', 'CURRENT_IN_CONFLICT_SET'].includes(ref.resolution)) fail('FACT_REFERENCE_INVALID', 'invalid condition resolution');
      return { fact_id: target.id, resolution: ref.resolution };
    });
    const qualifiers = canonicalize({ ...(fact.qualifiers || {}), condition_refs: conditions });
    const propositionKey = hash({ tenant_id: batch.tenant_id, subject_entity_id: subject.id, predicate: fact.predicate,
      object_value: object, qualifiers, semantic_slot_key: semanticSlotKey, valid_from: fact.valid_from || null,
      valid_to: fact.valid_to || null, temporal_status: fact.temporal_status, modality: fact.modality,
      condition_refs: conditions });
    const assertionKey = hash({ proposition_key: propositionKey, epistemic_state: fact.epistemic_state,
      assertion_qualifiers: fact.assertion_qualifiers || {} });
    let row = (await client.query(`SELECT id,proposition_key,assertion_key,conflict_set_id
      FROM canonical_business_facts WHERE tenant_id=$1 AND assertion_key=$2`, [batch.tenant_id, assertionKey])).rows[0];
    if (row) reused.push(row.id);
    else {
      row = (await client.query(`INSERT INTO canonical_business_facts
        (tenant_id,proposition_key,assertion_key,subject_entity_id,predicate,object_value,qualifiers,
         epistemic_state,epistemic_confidence,epistemic_calibration_version,interpretation_confidence,
         interpretation_calibration_version,temporal_status,valid_from,valid_to,modality,conflict_set_id,created_by_batch_id)
        VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
        RETURNING id,proposition_key,assertion_key,conflict_set_id`, [batch.tenant_id, propositionKey, assertionKey,
        subject.id, fact.predicate, canonicalJsonString(object), canonicalJsonString(qualifiers), fact.epistemic_state,
        fact.epistemic_confidence ?? null, fact.epistemic_calibration_version || null, fact.interpretation_confidence,
        fact.interpretation_calibration_version, fact.temporal_status, fact.valid_from || null, fact.valid_to || null,
        fact.modality, conflict.id, batchId])).rows[0];
      created.push(row.id);
    }
    byIndex.set(index, row); byId.set(row.id, row);
  }
  return { created, reused, byIndex, byId };
}

async function appendLabels(client, batch, batchId, refs, evidence) {
  const created = [], reused = [];
  for (const label of batch.label_assertions) {
    const entity = refs.get(label.entity_identity_key || label.entity_ref);
    if (!entity || !label.label?.trim() || !['CANONICAL', 'ALIAS'].includes(label.assertion_kind)) fail('LABEL_ASSERTION_INVALID', 'invalid label assertion');
    if (label.evidence_id && !evidence.has(label.evidence_id)) fail('LABEL_EVIDENCE_INVALID', 'label evidence is not a batch input');
    let row = (await client.query(`SELECT id FROM canonical_entity_label_assertions
      WHERE tenant_id=$1 AND entity_id=$2 AND label=$3 AND assertion_kind=$4`,
    [batch.tenant_id, entity.id, label.label, label.assertion_kind])).rows[0];
    if (row) reused.push(row.id);
    else { row = (await client.query(`INSERT INTO canonical_entity_label_assertions
      (tenant_id,entity_id,label,assertion_kind,evidence_id,created_by_batch_id) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
    [batch.tenant_id, entity.id, label.label, label.assertion_kind, label.evidence_id || null, batchId])).rows[0]; created.push(row.id); }
  }
  return { created, reused };
}

async function attachEvidence(client, batch, batchId, facts, evidence) {
  const created = [], reused = [], linked = new Set();
  for (const link of batch.fact_evidence_links) {
    const fact = Number.isInteger(link.fact_index) ? facts.byIndex.get(link.fact_index) : facts.byId.get(link.fact_id);
    const source = evidence.get(link.evidence_id);
    if (!fact) fail('EVIDENCE_FACT_UNRESOLVED', 'evidence fact reference unresolved');
    if (!source) fail('EVIDENCE_LINK_INVALID', 'evidence is not an ordered input');
    if (source.source_text_sha256 !== link.source_text_sha256) fail('EVIDENCE_DIGEST_MISMATCH', 'evidence digest mismatch');
    if (!Number.isInteger(link.span_start_utf16) || !Number.isInteger(link.span_end_utf16)
      || link.span_start_utf16 < 0 || link.span_end_utf16 <= link.span_start_utf16 || link.span_end_utf16 > source.statement.length) fail('EVIDENCE_SPAN_INVALID', 'invalid UTF-16 span');
    if (!SUPPORT_TYPES.has(link.support_type)) fail('EVIDENCE_SUPPORT_INVALID', 'invalid support_type');
    let row = (await client.query(`SELECT id FROM canonical_fact_evidence_refs WHERE tenant_id=$1 AND fact_id=$2
      AND evidence_id=$3 AND span_start_utf16=$4 AND span_end_utf16=$5`,
    [batch.tenant_id, fact.id, link.evidence_id, link.span_start_utf16, link.span_end_utf16])).rows[0];
    if (row) reused.push(row.id);
    else { row = (await client.query(`INSERT INTO canonical_fact_evidence_refs
      (tenant_id,fact_id,evidence_id,source_text_sha256,span_start_utf16,span_end_utf16,support_type,created_by_batch_id)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id`, [batch.tenant_id, fact.id, link.evidence_id,
      link.source_text_sha256, link.span_start_utf16, link.span_end_utf16, link.support_type, batchId])).rows[0]; created.push(row.id); }
    if (Number.isInteger(link.fact_index)) linked.add(link.fact_index);
  }
  for (let index = 0; index < batch.semantic_facts.length; index += 1) if (!linked.has(index)) fail('FACT_EVIDENCE_REQUIRED', `fact ${index} lacks evidence`);
  return { created, reused };
}

async function getFact(client, tenantId, facts, index, id) {
  if (Number.isInteger(index)) return facts.byIndex.get(index);
  if (facts.byId.has(id)) return facts.byId.get(id);
  const row = (await client.query('SELECT id,conflict_set_id FROM canonical_business_facts WHERE tenant_id=$1 AND id=$2', [tenantId, id])).rows[0];
  if (!row) fail('FACT_REFERENCE_UNRESOLVED', `fact ${id} is not tenant-local`);
  facts.byId.set(id, row); return row;
}

async function appendRelations(client, batch, batchId, facts) {
  const created = [], reused = [], rows = [];
  for (const relation of batch.fact_relations) {
    if (!RELATION_TYPES.has(relation.relation_type)) fail('RELATION_TYPE_INVALID', 'invalid relation type');
    const from = await getFact(client, batch.tenant_id, facts, relation.from_fact_index, relation.from_fact_id);
    const to = await getFact(client, batch.tenant_id, facts, relation.to_fact_index, relation.to_fact_id);
    if (!from || !to || from.id === to.id) fail('RELATION_FACT_UNRESOLVED', 'invalid relation endpoints');
    if (relation.relation_type !== 'DEPENDS_ON' && from.conflict_set_id !== to.conflict_set_id) fail('RELATION_CONFLICT_SET_INVALID', 'revision relation crosses slots');
    let row = (await client.query(`SELECT id FROM canonical_fact_relations WHERE tenant_id=$1 AND from_fact_id=$2
      AND relation_type=$3 AND to_fact_id=$4`, [batch.tenant_id, from.id, relation.relation_type, to.id])).rows[0];
    if (row) reused.push(row.id);
    else { row = (await client.query(`INSERT INTO canonical_fact_relations
      (tenant_id,from_fact_id,relation_type,to_fact_id,created_by_batch_id) VALUES ($1,$2,$3,$4,$5) RETURNING id`,
    [batch.tenant_id, from.id, relation.relation_type, to.id, batchId])).rows[0]; created.push(row.id); }
    rows.push({ id: row.id, from_fact_id: from.id, to_fact_id: to.id, relation_type: relation.relation_type });
  }
  return { created, reused, rows };
}

function cyclic(edges) {
  const graph = new Map(), visiting = new Set(), visited = new Set();
  for (const [from, to] of edges) graph.set(from, [...(graph.get(from) || []), to]);
  const visit = node => { if (visiting.has(node)) return true; if (visited.has(node)) return false;
    visiting.add(node); for (const next of graph.get(node) || []) if (visit(next)) return true;
    visiting.delete(node); visited.add(node); return false; };
  return [...graph.keys()].some(visit);
}

async function appendMerges(client, batch, batchId, refs, evidence, prior) {
  const created = [], reused = [], active = new Map();
  const priorIds = prior?.merge_event_ids || [];
  const priorRows = priorIds.length ? (await client.query(`SELECT * FROM canonical_entity_merge_events
    WHERE tenant_id=$1 AND id=ANY($2::uuid[]) ORDER BY created_at,id`, [batch.tenant_id, priorIds])).rows : [];
  for (const event of priorRows) { const key = `${event.source_entity_id}:${event.target_entity_id}`;
    if (event.event_kind === 'MERGED') active.set(key, [event.source_entity_id, event.target_entity_id]);
    if (event.event_kind === 'MERGE_REVOKED') active.delete(key); }
  for (const event of batch.entity_merge_events) {
    if (!MERGE_TYPES.has(event.event_kind)) fail('MERGE_EVENT_INVALID', 'invalid merge event');
    const source = refs.get(event.source_entity_identity_key), target = refs.get(event.target_entity_identity_key);
    if (!source || !target || source.id === target.id || source.entity_type !== target.entity_type) fail('MERGE_ENTITY_INVALID', 'merge endpoints invalid');
    if (event.evidence_id && !evidence.has(event.evidence_id)) fail('MERGE_EVIDENCE_INVALID', 'merge evidence not in batch');
    const key = `${source.id}:${target.id}`;
    if (event.event_kind === 'MERGED') active.set(key, [source.id, target.id]);
    if (event.event_kind === 'MERGE_REVOKED') { if (!active.has(key)) fail('MERGE_REVOCATION_INVALID', 'no merge to revoke'); active.delete(key); }
    if (cyclic([...active.values()])) fail('MERGE_CYCLE', 'merge cycle detected');
    let row = (await client.query(`SELECT id FROM canonical_entity_merge_events WHERE tenant_id=$1 AND source_entity_id=$2
      AND target_entity_id=$3 AND event_kind=$4 AND evidence_id IS NOT DISTINCT FROM $5`,
    [batch.tenant_id, source.id, target.id, event.event_kind, event.evidence_id || null])).rows[0];
    if (row) reused.push(row.id);
    else { row = (await client.query(`INSERT INTO canonical_entity_merge_events
      (tenant_id,source_entity_id,target_entity_id,event_kind,evidence_id,created_by_batch_id) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
    [batch.tenant_id, source.id, target.id, event.event_kind, event.evidence_id || null, batchId])).rows[0]; created.push(row.id); }
  }
  return { created, reused };
}

async function resolveConflicts(client, batch, facts, relations, prior) {
  const results = new Map((prior?.conflict_set_resolutions || []).map(value => [value.conflict_set_id, value]));
  const grouped = new Map();
  for (const fact of facts.byIndex.values()) grouped.set(fact.conflict_set_id, [...(grouped.get(fact.conflict_set_id) || []), fact.id]);
  const explicit = new Map();
  for (const request of batch.conflict_set_resolutions) {
    let conflictSetId = request.conflict_set_id;
    if (!conflictSetId && request.semantic_slot_key) {
      conflictSetId = (await client.query(`SELECT id FROM canonical_conflict_sets
        WHERE tenant_id=$1 AND semantic_slot_key=$2`, [batch.tenant_id, request.semantic_slot_key])).rows[0]?.id;
    }
    if (!conflictSetId || explicit.has(conflictSetId)) fail('CONFLICT_RESOLUTION_INVALID', 'one unique tenant-local conflict set is required');
    const active = [];
    for (const index of request.active_fact_indices || []) { const fact = facts.byIndex.get(index); if (!fact) fail('CONFLICT_FACT_UNRESOLVED', 'active index invalid'); active.push(fact.id); }
    for (const id of request.active_fact_ids || []) active.push((await getFact(client, batch.tenant_id, facts, null, id)).id);
    explicit.set(conflictSetId, { conflict_set_id: conflictSetId, active_fact_ids: uniqueSorted(active), resolution_state: request.resolution_state });
  }
  for (const conflictId of new Set([...grouped.keys(), ...explicit.keys()])) {
    const set = (await client.query('SELECT slot_cardinality FROM canonical_conflict_sets WHERE tenant_id=$1 AND id=$2', [batch.tenant_id, conflictId])).rows[0];
    if (!set) fail('CONFLICT_SET_UNRESOLVED', 'conflict set is not tenant-local');
    const members = new Set((await client.query('SELECT id FROM canonical_business_facts WHERE tenant_id=$1 AND conflict_set_id=$2', [batch.tenant_id, conflictId])).rows.map(row => row.id));
    let resolution = explicit.get(conflictId);
    if (!resolution) {
      let active = uniqueSorted([...(results.get(conflictId)?.active_fact_ids || []), ...(grouped.get(conflictId) || [])]);
      for (const relation of relations.rows) if (['SUPERSEDES', 'CORRECTION_OF'].includes(relation.relation_type)) active = active.filter(id => id !== relation.to_fact_id);
      resolution = { conflict_set_id: conflictId, active_fact_ids: active, resolution_state: 'RESOLVED' };
    }
    if (!['RESOLVED', 'UNRESOLVED'].includes(resolution.resolution_state) || resolution.active_fact_ids.some(id => !members.has(id))) fail('CONFLICT_RESOLUTION_INVALID', 'invalid conflict membership');
    if (resolution.resolution_state === 'UNRESOLVED' && resolution.active_fact_ids.length) fail('CONFLICT_RESOLUTION_INVALID', 'UNRESOLVED must select none');
    if (['SINGLE', 'OPTIONAL_SINGLE'].includes(set.slot_cardinality) && resolution.active_fact_ids.length > 1) fail('CONFLICT_CARDINALITY_VIOLATED', 'exclusive slot has multiple active facts');
    if (set.slot_cardinality === 'SINGLE' && resolution.resolution_state === 'RESOLVED' && resolution.active_fact_ids.length !== 1) fail('CONFLICT_CARDINALITY_VIOLATED', 'SINGLE must select one fact');
    results.set(conflictId, resolution);
  }
  return [...results.values()].map(value => ({ ...value, active_fact_ids: uniqueSorted(value.active_fact_ids) })).sort((a, b) => a.conflict_set_id.localeCompare(b.conflict_set_id));
}

async function validateManifest(client, batch, manifest) {
  for (const [field, table] of [['entity_ids', 'canonical_business_entities'], ['label_assertion_ids', 'canonical_entity_label_assertions'],
    ['fact_ids', 'canonical_business_facts'], ['fact_evidence_ref_ids', 'canonical_fact_evidence_refs'],
    ['fact_relation_ids', 'canonical_fact_relations'], ['merge_event_ids', 'canonical_entity_merge_events']]) {
    if (!manifest[field].length) continue;
    const count = (await client.query(`SELECT count(*)::int AS count FROM ${table} WHERE tenant_id=$1 AND id=ANY($2::uuid[])`, [batch.tenant_id, manifest[field]])).rows[0].count;
    if (count !== manifest[field].length) fail('MANIFEST_REFERENCE_INVALID', `${field} contains an invalid reference`);
  }
  const facts = new Set(manifest.fact_ids), entities = new Set(manifest.entity_ids);
  if (manifest.fact_ids.length) {
    const rows = (await client.query('SELECT subject_entity_id FROM canonical_business_facts WHERE tenant_id=$1 AND id=ANY($2::uuid[])', [batch.tenant_id, manifest.fact_ids])).rows;
    if (rows.some(row => !entities.has(row.subject_entity_id))) fail('MANIFEST_GRAPH_INVALID', 'fact subject absent from manifest');
  }
  if (manifest.fact_relation_ids.length) {
    const rows = (await client.query('SELECT from_fact_id,to_fact_id FROM canonical_fact_relations WHERE tenant_id=$1 AND id=ANY($2::uuid[])', [batch.tenant_id, manifest.fact_relation_ids])).rows;
    if (rows.some(row => !facts.has(row.from_fact_id) || !facts.has(row.to_fact_id))) fail('MANIFEST_GRAPH_INVALID', 'relation endpoint absent');
  }
  if (manifest.label_assertion_ids.length) {
    const rows = (await client.query('SELECT entity_id FROM canonical_entity_label_assertions WHERE tenant_id=$1 AND id=ANY($2::uuid[])', [batch.tenant_id, manifest.label_assertion_ids])).rows;
    if (rows.some(row => !entities.has(row.entity_id))) fail('MANIFEST_GRAPH_INVALID', 'label entity absent');
  }
  if (manifest.fact_evidence_ref_ids.length) {
    const rows = (await client.query('SELECT fact_id FROM canonical_fact_evidence_refs WHERE tenant_id=$1 AND id=ANY($2::uuid[])', [batch.tenant_id, manifest.fact_evidence_ref_ids])).rows;
    if (rows.some(row => !facts.has(row.fact_id))) fail('MANIFEST_GRAPH_INVALID', 'evidence fact absent');
  }
  if (manifest.merge_event_ids.length) {
    const rows = (await client.query('SELECT source_entity_id,target_entity_id FROM canonical_entity_merge_events WHERE tenant_id=$1 AND id=ANY($2::uuid[])', [batch.tenant_id, manifest.merge_event_ids])).rows;
    if (rows.some(row => !entities.has(row.source_entity_id) || !entities.has(row.target_entity_id))) fail('MANIFEST_GRAPH_INVALID', 'merge endpoint absent');
  }
  for (const resolution of manifest.conflict_set_resolutions) {
    const exists = (await client.query('SELECT 1 FROM canonical_conflict_sets WHERE tenant_id=$1 AND id=$2', [batch.tenant_id, resolution.conflict_set_id])).rows[0];
    if (!exists || resolution.active_fact_ids.some(id => !facts.has(id))) fail('MANIFEST_CONFLICT_INVALID', 'invalid conflict reference');
  }
}

async function replayResult(client, batch, batchId) {
  const snapshot = (await client.query(`SELECT * FROM canonical_business_snapshots WHERE tenant_id=$1 AND committed_batch_id=$2`, [batch.tenant_id, batchId])).rows[0];
  if (!snapshot) fail('IDEMPOTENCY_REPLAY_FAILED', 'committed batch has no snapshot');
  const created = async table => (await client.query(`SELECT id FROM ${table} WHERE tenant_id=$1 AND created_by_batch_id=$2 ORDER BY id`, [batch.tenant_id, batchId])).rows.map(row => row.id);
  const entityIds = await created('canonical_business_entities'), factIds = await created('canonical_business_facts'), evidenceIds = await created('canonical_fact_evidence_refs'), labelIds = await created('canonical_entity_label_assertions');
  const relationIds = await created('canonical_fact_relations');
  const mergeIds = await created('canonical_entity_merge_events');
  const business = (await client.query(`SELECT id FROM canonical_business_entities WHERE tenant_id=$1 AND entity_type='BUSINESS' AND id=ANY($2::uuid[]) LIMIT 1`, [batch.tenant_id, snapshot.manifest.entity_ids])).rows[0];
  return new CanonicalCommitResult({ interpretation_batch_id: batchId, replayed: true, newly_committed: false,
    canonical_business_entity_id: business.id, entity_ids_created: entityIds, entity_ids_reused: snapshot.manifest.entity_ids.filter(id => !entityIds.includes(id)),
    fact_ids_created: factIds, fact_ids_reused: snapshot.manifest.fact_ids.filter(id => !factIds.includes(id)), evidence_links_created: evidenceIds,
    evidence_links_reused: snapshot.manifest.fact_evidence_ref_ids.filter(id => !evidenceIds.includes(id)),
    relations_created: relationIds, label_assertions_created: labelIds,
    label_assertions_reused: snapshot.manifest.label_assertion_ids.filter(id => !labelIds.includes(id)),
    merge_events_created: mergeIds, conflict_results: snapshot.manifest.conflict_set_resolutions,
    snapshot_id: snapshot.id, manifest_digest: snapshot.manifest_digest, registry_artifact_id: snapshot.registry_artifact_id, registry_version: snapshot.registry_version });
}

async function commitCanonicalSemanticBatch(pool, input) {
  const batch = input instanceof CanonicalSemanticBatch ? input : new CanonicalSemanticBatch(input);
  const validation = batch.validate();
  if (!validation.valid) fail('INPUT_VALIDATION_FAILED', validation.errors.join('; '));
  const business = batch.semantic_entities.filter(entity => entity.entity_type === 'BUSINESS');
  if (business.length !== 1) fail('BUSINESS_ENTITY_REQUIRED', 'exactly one BUSINESS is required');
  const clientId = Number(business[0].domain_client_id), client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [batch.tenant_id]);
    const authority = (await client.query(`SELECT 1 FROM tenant_workspaces tw JOIN clients c ON c.id=tw.client_id
      WHERE tw.tenant_key=$1 AND tw.client_id=$2`, [batch.tenant_id, clientId])).rows[0];
    if (!authority) fail('TENANT_AUTHORITY_FAILED', 'tenant/client workspace binding is invalid');
    const registry = await loadRegistry(client, batch);
    const evidenceInputs = await loadEvidence(client, batch, clientId);
    if (deriveInterpretationBatchKey(batch, evidenceInputs) !== batch.idempotency_key) {
      fail('IDEMPOTENCY_KEY_INVALID', 'idempotency_key does not match ordered canonical batch provenance');
    }
    const replay = (await client.query(`SELECT id,status FROM canonical_interpretation_batches WHERE tenant_id=$1 AND idempotency_key=$2`, [batch.tenant_id, batch.idempotency_key])).rows[0];
    if (replay) {
      if (replay.status !== 'COMMITTED') fail('IDEMPOTENCY_REPLAY_FAILED', 'existing batch is not committed');
      const result = await replayResult(client, batch, replay.id); await client.query('COMMIT'); return result;
    }
    const prior = (await client.query(`SELECT id,registry_artifact_id,manifest FROM canonical_business_snapshots WHERE tenant_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1`, [batch.tenant_id])).rows[0] || null;
    let registryTransition = null;
    if (prior && prior.registry_artifact_id !== batch.registry_artifact_id) {
      const priorRegistry = await loadPriorRegistry(client, prior.id);
      if (!priorRegistry) fail('PRIOR_REGISTRY_NOT_FOUND', 'prior registry artifact missing');
      registryTransition = await createRegistryTransitionPlan(client, batch.tenant_id, prior, priorRegistry, registry);
    }
    const batchId = await insertBatch(client, batch, evidenceInputs);
    const entities = await resolveEntities(client, batch, batchId, registry);
    const labels = await appendLabels(client, batch, batchId, entities.refs, evidenceInputs);
    const facts = await resolveFacts(client, batch, batchId, registry, entities.refs);
    const evidence = await attachEvidence(client, batch, batchId, facts, evidenceInputs);
    const relations = await appendRelations(client, batch, batchId, facts);
    const priorManifestForMerges = (prior && prior.registry_artifact_id === batch.registry_artifact_id) ? prior.manifest : null;
    const merges = await appendMerges(client, batch, batchId, entities.refs, evidenceInputs, priorManifestForMerges);
    const conflicts = await resolveConflicts(client, batch, facts, relations, priorManifestForMerges);
    let base = {};
    if (prior && prior.registry_artifact_id === batch.registry_artifact_id) {
      base = prior.manifest;
    }
    // Note: On registry transition, base starts empty. The transition plan validates
    // that facts CAN survive, but doesn't automatically carry them over. The current
    // batch determines what goes into the new manifest. Prior snapshots remain unchanged.
    const manifest = { entity_ids: uniqueSorted([...(base.entity_ids || []), ...entities.created, ...entities.reused]),
      label_assertion_ids: uniqueSorted([...(base.label_assertion_ids || []), ...labels.created, ...labels.reused]),
      fact_ids: uniqueSorted([...(base.fact_ids || []), ...facts.created, ...facts.reused]),
      fact_evidence_ref_ids: uniqueSorted([...(base.fact_evidence_ref_ids || []), ...evidence.created, ...evidence.reused]),
      fact_relation_ids: uniqueSorted([...(base.fact_relation_ids || []), ...relations.created, ...relations.reused]),
      merge_event_ids: uniqueSorted([...(base.merge_event_ids || []), ...merges.created, ...merges.reused]),
      conflict_set_resolutions: conflicts, registry_artifact_id: batch.registry_artifact_id,
      registry_version: batch.registry_version, registry_content_digest: batch.registry_content_digest };
    await validateManifest(client, batch, manifest);
    if (batch.snapshot_metadata.supersedes_snapshot_id && batch.snapshot_metadata.supersedes_snapshot_id !== prior?.id) fail('SNAPSHOT_SUPERSESSION_INVALID', 'superseded snapshot is not latest');
    const snapshot = (await client.query(`INSERT INTO canonical_business_snapshots
      (tenant_id,committed_batch_id,registry_artifact_id,registry_version,registry_content_digest,supersedes_snapshot_id,manifest,manifest_digest)
      VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,encode(digest($7::jsonb::text,'sha256'),'hex')) RETURNING id,manifest_digest`,
    [batch.tenant_id, batchId, batch.registry_artifact_id, batch.registry_version, batch.registry_content_digest, prior?.id || null, canonicalJsonString(manifest)])).rows[0];
    await client.query('COMMIT');
    return new CanonicalCommitResult({ interpretation_batch_id: batchId, replayed: false, newly_committed: true,
      canonical_business_entity_id: entities.business.id, entity_ids_created: entities.created, entity_ids_reused: entities.reused,
      fact_ids_created: facts.created, fact_ids_reused: facts.reused, evidence_links_created: evidence.created,
      evidence_links_reused: evidence.reused, relations_created: relations.created, label_assertions_created: labels.created,
      label_assertions_reused: labels.reused, merge_events_created: merges.created, conflict_results: conflicts,
      snapshot_id: snapshot.id, manifest_digest: snapshot.manifest_digest, registry_artifact_id: batch.registry_artifact_id,
      registry_version: batch.registry_version });
  } catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error; }
  finally { client.release(); }
}

// ============================================================================
// SPEC-223B Registry Transition Implementation
// ============================================================================

async function loadPriorRegistry(client, priorSnapshotId) {
  if (!priorSnapshotId) return null;
  const snapshot = (await client.query(`SELECT registry_artifact_id,registry_version,registry_content_digest
    FROM canonical_business_snapshots WHERE id=$1`, [priorSnapshotId])).rows[0];
  if (!snapshot) return null;
  const registry = (await client.query(`SELECT * FROM canonical_registry_artifacts WHERE id=$1`, [snapshot.registry_artifact_id])).rows[0];
  return registry;
}

class RegistryTransitionPlan {
  constructor() {
    this.entities = { retain: [], reject: [] };
    this.facts = { retain: [], reclassify: [], replace: [], reject: [] };
    this.relations = { retain: [], rebuild: [], reject: [] };
    this.conflicts = { retain: [], rebuild: [] };
    this.prior_snapshot_id = null;
    this.prior_registry_artifact_id = null;
    this.target_registry_artifact_id = null;
    this.validation_results = [];
    this.errors = [];
  }

  fail(code, message) {
    this.errors.push({ code, message });
    throw new CanonicalSemanticError(code, message, { transition_plan: this });
  }
}

function classifyCardinalityChange(priorCardinality, targetCardinality) {
  const singleTypes = ['SINGLE', 'OPTIONAL_SINGLE'];
  const setTypes = ['SET', 'ORDERED_SET'];
  const priorIsSingle = singleTypes.includes(priorCardinality);
  const targetIsSingle = setTypes.includes(targetCardinality);
  if (priorIsSingle && !targetIsSingle) return 'SINGLE_TO_SET';
  if (!priorIsSingle && targetIsSingle) return 'SET_TO_SINGLE';
  if (priorCardinality === targetCardinality) return 'UNCHANGED';
  return 'OPTIONAL_CHANGE';
}

async function transitionEntities(client, tenantId, priorManifest, priorRegistry, targetRegistry, plan) {
  const targetVocabulary = vocabularyTypes(targetRegistry.entity_vocabulary);
  if (!priorManifest.entity_ids?.length) return;
  const entities = (await client.query(
    `SELECT id,entity_type,identity_key,domain_client_id FROM canonical_business_entities
     WHERE tenant_id=$1 AND id=ANY($2::uuid[])`,
    [tenantId, priorManifest.entity_ids]
  )).rows;
  for (const entity of entities) {
    if (!targetVocabulary.has(entity.entity_type)) {
      plan.entities.reject.push({
        id: entity.id, entity_type: entity.entity_type, reason: 'ENTITY_TYPE_REMOVED_FROM_REGISTRY'
      });
    } else if (entity.entity_type === 'BUSINESS') {
      plan.entities.retain.push({ id: entity.id, entity_type: entity.entity_type, domain_client_id: entity.domain_client_id });
    } else {
      plan.entities.retain.push({ id: entity.id, entity_type: entity.entity_type });
    }
  }
}

async function validateFactAgainstRegistry(fact, subject, definition, targetRegistry) {
  const errors = [];
  const domains = asArray(definition.domain || definition.subject_types);
  if (!domains.length || (!domains.includes('*') && !domains.includes(subject.entity_type))) {
    errors.push('PREDICATE_DOMAIN_MISMATCH');
  }
  const entityRange = definition.range?.kind === 'ENTITY' ? asArray(definition.range.entity_types)
    : (typeof definition.range === 'string' && ENTITY_TYPES.has(definition.range) ? [definition.range] : []);
  if (entityRange.length && fact.object_value?.type === 'ENTITY_REF') {
    if (!entityRange.includes(fact.object_value.entity_type)) errors.push('PREDICATE_RANGE_MISMATCH');
  } else if (definition.range?.kind === 'LITERAL') {
    const types = asArray(definition.range.literal_types).map(type => String(type).toUpperCase());
    if (fact.object_value?.type && !types.includes(String(fact.object_value.type).toUpperCase())) {
      errors.push('PREDICATE_RANGE_MISMATCH');
    }
  }
  const allowedTemporal = asArray(definition.temporal_statuses || definition.allowed_temporal_statuses);
  if (allowedTemporal.length && !allowedTemporal.includes(fact.temporal_status)) errors.push('TEMPORAL_STATUS_INVALID');
  const allowedModalities = asArray(definition.modalities || definition.allowed_modalities);
  if (allowedModalities.length && !allowedModalities.includes(fact.modality)) errors.push('MODALITY_INVALID');
  if (fact.temporal_status === 'PLANNED' && fact.modality === 'ACTUAL') errors.push('TEMPORAL_MODAL_INVALID');
  return errors;
}

async function transitionFacts(client, tenantId, priorManifest, priorRegistry, targetRegistry, retainedEntityIds, plan) {
  if (!priorManifest.fact_ids?.length) return;
  const facts = (await client.query(
    `SELECT id,proposition_key,assertion_key,subject_entity_id,predicate,object_value,
            qualifiers,epistemic_state,temporal_status,modality,conflict_set_id
     FROM canonical_business_facts WHERE tenant_id=$1 AND id=ANY($2::uuid[])`,
    [tenantId, priorManifest.fact_ids]
  )).rows;
  const retainedSet = new Set(retainedEntityIds);
  for (const fact of facts) {
    if (!retainedSet.has(fact.subject_entity_id)) {
      plan.facts.reject.push({ id: fact.id, reason: 'SUBJECT_ENTITY_REJECTED' });
      continue;
    }
    const priorDefinition = priorRegistry.predicate_definitions[fact.predicate];
    const targetDefinition = targetRegistry.predicate_definitions[fact.predicate];
    if (!targetDefinition) {
      plan.facts.reject.push({ id: fact.id, predicate: fact.predicate, reason: 'PREDICATE_REMOVED' });
      continue;
    }
    const subject = (await client.query('SELECT entity_type FROM canonical_business_entities WHERE tenant_id=$1 AND id=$2',
      [tenantId, fact.subject_entity_id])).rows[0];
    const validationErrors = await validateFactAgainstRegistry(fact, subject, targetDefinition, targetRegistry);
    if (validationErrors.length) {
      plan.facts.reject.push({ id: fact.id, predicate: fact.predicate, errors: validationErrors, reason: 'REGISTRY_VALIDATION_FAILED' });
      continue;
    }
    const cardinalityChange = classifyCardinalityChange(
      priorDefinition.cardinality, targetDefinition.cardinality
    );
    if (cardinalityChange === 'UNCHANGED') {
      plan.facts.retain.push({ id: fact.id, predicate: fact.predicate });
    } else if (cardinalityChange === 'SINGLE_TO_SET') {
      plan.facts.reclassify.push({
        id: fact.id, predicate: fact.predicate, change: cardinalityChange,
        prior_cardinality: priorDefinition.cardinality, target_cardinality: targetDefinition.cardinality
      });
    } else if (cardinalityChange === 'SET_TO_SINGLE') {
      plan.facts.reclassify.push({
        id: fact.id, predicate: fact.predicate, change: cardinalityChange,
        prior_cardinality: priorDefinition.cardinality, target_cardinality: targetDefinition.cardinality,
        warning: 'Multiple prior facts must be resolved to single active fact'
      });
    } else {
      plan.facts.reclassify.push({ id: fact.id, predicate: fact.predicate, change: cardinalityChange });
    }
  }
}

async function transitionRelations(client, tenantId, priorManifest, retainedFactIds, plan) {
  if (!priorManifest.fact_relation_ids?.length) return;
  const relations = (await client.query(
    `SELECT id,from_fact_id,to_fact_id,relation_type FROM canonical_fact_relations
     WHERE tenant_id=$1 AND id=ANY($2::uuid[])`,
    [tenantId, priorManifest.fact_relation_ids]
  )).rows;
  const retainedSet = new Set(retainedFactIds);
  for (const relation of relations) {
    if (!retainedSet.has(relation.from_fact_id) || !retainedSet.has(relation.to_fact_id)) {
      plan.relations.reject.push({
        id: relation.id, from_fact_id: relation.from_fact_id, to_fact_id: relation.to_fact_id, reason: 'ENDPOINT_NOT_RETAINED'
      });
    } else {
      plan.relations.retain.push({
        id: relation.id, from_fact_id: relation.from_fact_id, to_fact_id: relation.to_fact_id, relation_type: relation.relation_type
      });
    }
  }
}

async function transitionConflicts(client, tenantId, priorManifest, retainedFactIds, plan) {
  if (!priorManifest.conflict_set_resolutions?.length) return;
  const retainedSet = new Set(retainedFactIds);
  for (const resolution of priorManifest.conflict_set_resolutions) {
    const activeInTarget = resolution.active_fact_ids.filter(id => retainedSet.has(id));
    if (activeInTarget.length === 0 && resolution.resolution_state === 'UNRESOLVED') {
      plan.conflicts.retain.push({
        conflict_set_id: resolution.conflict_set_id, active_fact_ids: [], resolution_state: 'UNRESOLVED'
      });
    } else if (activeInTarget.length) {
      plan.conflicts.rebuild.push({
        conflict_set_id: resolution.conflict_set_id, prior_active_fact_ids: resolution.active_fact_ids,
        target_active_fact_ids: activeInTarget, requires_validation: true
      });
    }
  }
}

async function createRegistryTransitionPlan(client, tenantId, priorSnapshot, priorRegistry, targetRegistry) {
  const plan = new RegistryTransitionPlan();
  plan.prior_snapshot_id = priorSnapshot.id;
  plan.prior_registry_artifact_id = priorRegistry.id;
  plan.target_registry_artifact_id = targetRegistry.id;
  const priorManifest = priorSnapshot.manifest;
  await transitionEntities(client, tenantId, priorManifest, priorRegistry, targetRegistry, plan);
  const retainedEntityIds = plan.entities.retain.map(e => e.id);
  await transitionFacts(client, tenantId, priorManifest, priorRegistry, targetRegistry, retainedEntityIds, plan);
  const retainedFactIds = plan.facts.retain.map(f => f.id).concat(plan.facts.reclassify.map(f => f.id));
  await transitionRelations(client, tenantId, priorManifest, retainedFactIds, plan);
  await transitionConflicts(client, tenantId, priorManifest, retainedFactIds, plan);
  plan.validation_results.push({
    entities_retained: plan.entities.retain.length, entities_rejected: plan.entities.reject.length,
    facts_retained: plan.facts.retain.length, facts_reclassified: plan.facts.reclassify.length,
    facts_replace_required: plan.facts.replace.length, facts_rejected: plan.facts.reject.length,
    relations_retained: plan.relations.retain.length, relations_rebuild_required: plan.relations.rebuild.length,
    relations_rejected: plan.relations.reject.length,
    conflicts_retained: plan.conflicts.retain.length, conflicts_rebuild_required: plan.conflicts.rebuild.length
  });
  return plan;
}

module.exports = { CanonicalSemanticBatch, CanonicalCommitResult, CanonicalSemanticError,
  commitCanonicalSemanticBatch, deriveInterpretationBatchKey, canonicalJsonString, hash,
  RegistryTransitionPlan, createRegistryTransitionPlan, loadPriorRegistry };