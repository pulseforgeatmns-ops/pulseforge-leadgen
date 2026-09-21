'use strict';

/**
 * SPEC-250 bounded write service.
 *
 * Explicit operator-judgment persistence only. No Max conversation classification.
 * Evidence is stored in the existing cie_evidence substrate (SPEC-223 write path)
 * under a dedicated holder session — not through CIE interview canonicalization.
 */

const crypto = require('crypto');
const {
  commitCanonicalSemanticBatch,
  CanonicalSemanticError,
} = require('../lib/canonicalSemanticWrite');
const { reconstructCanonicalSemanticProjection } = require('../lib/canonicalSemanticProjection');
const {
  OperatorJudgmentCanonicalAdapter,
  INTERPRETER_ID,
  judgmentIdentityKey,
  buildCanonicalJudgmentEvidenceText,
} = require('../lib/operatorJudgmentCanonicalAdapter');
const registrySeed = require('../migrations/2026-09-11-spec-250-operator-judgment-registry');

function fail(code, message, details) {
  throw new CanonicalSemanticError(code, message, details);
}

function sha256(text) {
  return crypto.createHash('sha256').update(String(text)).digest('hex');
}

async function loadSpec250Registry(pool) {
  const row = (await pool.query(
    `SELECT * FROM canonical_registry_artifacts WHERE registry_version = $1`,
    [registrySeed.REGISTRY_VERSION]
  )).rows[0];
  if (!row) fail('UNSUPPORTED_SEMANTIC_PRIMITIVE', 'SPEC-250 operator-judgment registry artifact is not seeded');
  return row;
}

async function assertTenantBinding(pool, tenantId, clientId) {
  if (!tenantId || !Number.isInteger(Number(clientId))) {
    fail('TENANT_IDENTITY_REQUIRED', 'tenant_id and client_id are required');
  }
  const row = (await pool.query(
    `SELECT 1 FROM tenant_workspaces tw JOIN clients c ON c.id = tw.client_id
     WHERE tw.tenant_key = $1 AND tw.client_id = $2`,
    [tenantId, Number(clientId)]
  )).rows[0];
  if (!row) fail('TENANT_IDENTITY_REQUIRED', 'tenant/client workspace binding is invalid or ambiguous');
}

async function loadLatestSnapshot(pool, tenantId) {
  return (await pool.query(
    `SELECT id, registry_artifact_id, registry_version FROM canonical_business_snapshots
     WHERE tenant_id = $1 ORDER BY created_at DESC, id DESC LIMIT 1`,
    [tenantId]
  )).rows[0] || null;
}

async function ensureEvidenceHolderSession(pool, clientId) {
  const existing = (await pool.query(
    `SELECT id FROM cie_interview_sessions
     WHERE client_id = $1 AND interview_state->>'canonical_purpose' = 'operator_judgment'
     ORDER BY created_at ASC LIMIT 1`,
    [clientId]
  )).rows[0];
  if (existing) return existing.id;
  return (await pool.query(
    `INSERT INTO cie_interview_sessions (client_id, status, current_stage, interview_state)
     VALUES ($1, 'NEW', 'Identity', '{"canonical_purpose":"operator_judgment"}'::jsonb)
     RETURNING id`,
    [clientId]
  )).rows[0].id;
}

async function reuseOrInsertEvidence(pool, { clientId, sessionId, role, statement, category }) {
  const digest = sha256(statement);
  const existing = (await pool.query(
    `SELECT id, statement, source_text_sha256, category, source
     FROM cie_evidence
     WHERE client_id = $1 AND source = 'operator_judgment' AND source_text_sha256 = $2
     ORDER BY created_at ASC LIMIT 1`,
    [clientId, digest]
  )).rows[0];
  if (existing) return { ...existing, role };
  const inserted = (await pool.query(
    `INSERT INTO cie_evidence
      (id, client_id, session_id, source, category, statement, confidence, type, source_text_sha256, immutable_at)
     VALUES ($1, $2, $3, 'operator_judgment', $4, $5, 1, 'EXPLICIT', $6, NOW())
     RETURNING id, statement, source_text_sha256, category, source`,
    [crypto.randomUUID(), clientId, sessionId, category, statement, digest]
  )).rows[0];
  return { ...inserted, role };
}

function propositionRationale(proposition) {
  if (typeof proposition.rationale === 'string' && proposition.rationale.trim()) return proposition.rationale.trim();
  if (Array.isArray(proposition.rationale_points) && proposition.rationale_points.length) {
    return proposition.rationale_points.map(point => String(point).trim()).filter(Boolean).join('\n');
  }
  return '';
}

function selectedJudgmentFacts(projection, identityKey) {
  const entity = (projection.entities || []).find(row => row.identity_key === identityKey
    && row.entity_type === 'OPERATOR_JUDGMENT');
  if (!entity) return [];
  return (projection.facts || []).filter(fact => fact.subject_entity_id === entity.id
    && fact.predicate === 'expresses_judgment'
    && fact.selected_in_conflict);
}

async function revisionRelations(pool, tenantId, batch, identityKey, prior) {
  if (!prior) return [];
  const projection = await reconstructCanonicalSemanticProjection(pool, {
    tenant_id: tenantId,
    snapshot_id: prior.id,
  });
  const priorBySlot = new Map();
  for (const fact of selectedJudgmentFacts(projection, identityKey)) {
    const slot = fact.qualifiers?.judgment_slot;
    if (slot) priorBySlot.set(slot, fact);
  }
  const relations = [];
  batch.semantic_facts.forEach((fact, index) => {
    if (fact.predicate !== 'expresses_judgment') return;
    const slot = fact.qualifiers?.judgment_slot;
    const previous = priorBySlot.get(slot);
    if (!previous) return;
    const previousValue = previous.object_value?.value;
    const nextValue = fact.object_value?.value;
    if (previousValue === nextValue && previous.epistemic_state === fact.epistemic_state) return;
    relations.push({ from_fact_index: index, to_fact_id: previous.id, relation_type: 'CORRECTION_OF' });
    relations.push({ from_fact_index: index, to_fact_id: previous.id, relation_type: 'SUPERSEDES' });
  });
  return relations;
}

async function persistOperatorJudgmentEvidence(pool, input, provenanceTextInput) {
  const clientId = Number(input.client_id);
  const sessionId = await ensureEvidenceHolderSession(pool, clientId);
  const canonicalStatement = buildCanonicalJudgmentEvidenceText(input, provenanceTextInput);
  const records = [
    await reuseOrInsertEvidence(pool, {
      clientId,
      sessionId,
      role: 'canonical_judgment',
      category: 'operator_judgment',
      statement: canonicalStatement,
    }),
  ];
  const propositions = (input.propositions || []).map(proposition => ({ ...proposition }));
  for (const proposition of propositions) {
    const rationale = propositionRationale(proposition);
    if (!rationale) continue;
    const role = `rationale:${proposition.judgment_slot}`;
    records.push(await reuseOrInsertEvidence(pool, {
      clientId,
      sessionId,
      role,
      category: 'operator_judgment_rationale',
      statement: rationale,
    }));
    proposition.rationale_evidence_role = role;
  }
  return { records, propositions };
}

/**
 * Persist an explicit typed operator judgment into canonical semantic storage.
 * Does not accept ordinary conversational text.
 */
async function commitOperatorJudgment(pool, input = {}) {
  if (!pool) fail('JUDGMENT_INPUT_INVALID', 'database pool is required');
  if (!input || typeof input !== 'object' || Array.isArray(input) || typeof input === 'string') {
    fail('JUDGMENT_INPUT_INVALID', 'operator judgment input must be a typed object');
  }
  if (typeof input.text === 'string' && input.kind !== 'OPERATOR_JUDGMENT') {
    fail('JUDGMENT_KIND_REQUIRED', 'ordinary conversational text is not a canonical operator judgment');
  }

  const tenantId = String(input.tenant_id || '').trim();
  const clientId = Number(input.client_id);
  await assertTenantBinding(pool, tenantId, clientId);

  const registry = input.registry_artifact || await loadSpec250Registry(pool);
  const prior = await loadLatestSnapshot(pool, tenantId);
  if (prior && prior.registry_artifact_id !== registry.id) {
    fail('REGISTRY_TRANSITION_UNSUPPORTED',
      'refusing to write operator judgment over a different pinned registry (would drop prior canonical facts)');
  }

  const adapterInput = {
    ...input,
    kind: input.kind,
    registry_artifact: registry,
  };
  // Validate typed shape before writing evidence so malformed conversation cannot persist.
  OperatorJudgmentCanonicalAdapter.buildBatch({
    ...adapterInput,
    evidence_records: [{
      id: '00000000-0000-4000-8000-000000000000',
      role: 'canonical_judgment',
      statement: 'placeholder',
      source_text_sha256: sha256('placeholder'),
    }],
  });

  const persisted = await persistOperatorJudgmentEvidence(pool, input, {
    origin: 'OPERATOR',
    origin_kind: String(input.provenance?.origin_kind || '').trim(),
    actor_id: (input.operator?.id || input.provenance?.actor_id) != null
      ? String(input.operator?.id || input.provenance?.actor_id).trim() : '',
  });

  const batch = OperatorJudgmentCanonicalAdapter.buildBatch({
    ...adapterInput,
    propositions: persisted.propositions,
    evidence_records: persisted.records,
  });
  batch.fact_relations = await revisionRelations(
    pool,
    tenantId,
    batch,
    judgmentIdentityKey(String(input.judgment_key).trim()),
    prior
  );
  // Relations are not part of the interpretation idempotency key; leave key as derived.

  const result = await commitCanonicalSemanticBatch(pool, batch);
  return {
    ...result,
    interpreter_id: INTERPRETER_ID,
    judgment_identity_key: judgmentIdentityKey(String(input.judgment_key).trim()),
    evidence_ids: persisted.records.map(record => record.id),
  };
}

module.exports = {
  commitOperatorJudgment,
  loadSpec250Registry,
  persistOperatorJudgmentEvidence,
};
