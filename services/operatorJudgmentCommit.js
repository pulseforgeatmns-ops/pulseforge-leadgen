'use strict';

/**
 * SPEC-251 — Typed operator judgment commit boundary.
 *
 * Authenticated operators submit structured OPERATOR_JUDGMENT payloads only.
 * No conversational classification. Server binds tenant, client, and provenance.
 */

const { CanonicalSemanticError } = require('../lib/canonicalSemanticWrite');
const { commitOperatorJudgment } = require('./operatorJudgmentCanonical');
const { resolveActiveTenantId } = require('../packages/max/workspace/TenantContextResolver');

const KIND = 'OPERATOR_JUDGMENT';
const OBJECTIVE_IDENTITY_RE = /^objective:[a-z0-9][a-z0-9._-]*$/i;
const JUDGMENT_KEY_RE = /^[a-z0-9][a-z0-9._-]*$/i;

const FORBIDDEN_REQUEST_KEYS = new Set([
  'tenant_id',
  'client_id',
  'operator',
  'provenance',
  'registry_artifact',
  'evidence_records',
  'text',
  'message',
  'resolve_mission',
  'resolve_objective',
]);

class OperatorJudgmentCommitError extends Error {
  constructor(code, message, status = 400, details = {}) {
    super(message);
    this.name = 'OperatorJudgmentCommitError';
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

function fail(code, message, status = 400, details = {}) {
  throw new OperatorJudgmentCommitError(code, message, status, details);
}

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function resolveClientIdFromRequest(req) {
  const user = req?.user || req?.session?.user || null;
  if (user?.role === 'client') {
    const id = Number(user.client_id);
    return Number.isInteger(id) && id > 0 ? id : null;
  }
  return resolveActiveTenantId(req);
}

function resolveActor(req) {
  const user = req?.user || req?.session?.user || {};
  const id = user.id != null ? String(user.id).trim() : '';
  const email = user.email != null ? String(user.email).trim() : '';
  const role = user.role != null ? String(user.role).trim() : '';
  return {
    id: id || email || 'operator',
    email: email || null,
    name: user.name != null ? String(user.name).trim() : (email || 'operator'),
    role: role || 'operator',
  };
}

async function loadTenantKey(pool, clientId) {
  const row = (await pool.query(
    `SELECT tenant_key FROM tenant_workspaces WHERE client_id = $1 LIMIT 1`,
    [clientId]
  )).rows[0];
  if (!row?.tenant_key) {
    fail('TENANT_WORKSPACE_NOT_FOUND', 'No tenant workspace binding for active client', 400);
  }
  return String(row.tenant_key).trim();
}

function assertPlainObject(body, code = 'JUDGMENT_BODY_INVALID') {
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    fail(code, 'request body must be a structured judgment object');
  }
}

function rejectForbiddenOverrides(body) {
  for (const key of Object.keys(body)) {
    if (FORBIDDEN_REQUEST_KEYS.has(key)) {
      fail('JUDGMENT_OVERRIDE_FORBIDDEN', `client may not supply ${key}`);
    }
  }
}

function validateObjectiveIdentityKey(value) {
  const key = asText(value);
  if (!key) return null;
  if (!OBJECTIVE_IDENTITY_RE.test(key)) {
    fail('OBJECTIVE_BINDING_INVALID', 'associated_objective_identity_key is invalid');
  }
  return key;
}

function validatePropositions(propositions) {
  if (!Array.isArray(propositions) || !propositions.length) {
    fail('JUDGMENT_PROPOSITIONS_REQUIRED', 'at least one typed proposition is required');
  }
  const slots = new Set();
  return propositions.map((proposition, index) => {
    if (!proposition || typeof proposition !== 'object' || Array.isArray(proposition)) {
      fail('JUDGMENT_PROPOSITION_INVALID', `proposition at index ${index} must be an object`);
    }
    const judgment_slot = asText(proposition.judgment_slot);
    const statement = asText(proposition.statement);
    if (!judgment_slot) fail('JUDGMENT_SLOT_REQUIRED', `proposition at index ${index} requires judgment_slot`);
    if (!statement) fail('JUDGMENT_STATEMENT_REQUIRED', `proposition at index ${index} requires statement`);
    if (slots.has(judgment_slot)) {
      fail('JUDGMENT_SLOT_DUPLICATE', `duplicate judgment_slot ${judgment_slot}`);
    }
    slots.add(judgment_slot);
    const normalized = {
      judgment_slot,
      statement,
    };
    if (proposition.epistemic_state != null) normalized.epistemic_state = String(proposition.epistemic_state).trim();
    if (proposition.temporal_status != null) normalized.temporal_status = String(proposition.temporal_status).trim();
    if (proposition.modality != null) normalized.modality = String(proposition.modality).trim();
    if (typeof proposition.rationale === 'string' && proposition.rationale.trim()) {
      normalized.rationale = proposition.rationale.trim();
    }
    if (Array.isArray(proposition.rationale_points)) {
      normalized.rationale_points = proposition.rationale_points
        .map(point => String(point).trim())
        .filter(Boolean);
    }
    return normalized;
  });
}

function parseRequestBody(body) {
  assertPlainObject(body);
  rejectForbiddenOverrides(body);

  const kind = asText(body.kind);
  if (!kind) fail('JUDGMENT_KIND_REQUIRED', 'kind OPERATOR_JUDGMENT is required');
  if (kind !== KIND) fail('JUDGMENT_KIND_INVALID', 'kind must be OPERATOR_JUDGMENT');

  const judgment_key = asText(body.judgment_key);
  const judgment_kind = asText(body.judgment_kind);
  const label = asText(body.label);
  if (!judgment_key) fail('JUDGMENT_IDENTITY_REQUIRED', 'judgment_key is required');
  if (!JUDGMENT_KEY_RE.test(judgment_key)) fail('JUDGMENT_IDENTITY_INVALID', 'judgment_key format is invalid');
  if (!judgment_kind) fail('JUDGMENT_KIND_INVALID', 'judgment_kind is required');
  if (!label) fail('JUDGMENT_LABEL_REQUIRED', 'label is required');

  const associated_mission_id = asText(body.associated_mission_id) || null;
  const associated_objective_identity_key = validateObjectiveIdentityKey(body.associated_objective_identity_key);
  const propositions = validatePropositions(body.propositions);
  const confirmation = body.confirmation === true || body.confirmed === true;

  return {
    judgment_key,
    judgment_kind,
    label,
    propositions,
    associated_mission_id,
    associated_objective_identity_key,
    confirmation,
  };
}

async function validateMissionBinding(missionId, clientId, deps = {}) {
  const inspectMission = deps.inspectMission;
  if (typeof inspectMission !== 'function') {
    fail('MISSION_VALIDATION_UNAVAILABLE', 'mission binding validation is unavailable', 503);
  }
  let snapshot;
  try {
    snapshot = await inspectMission(missionId, {
      tenantId: String(clientId),
      pool: deps.pool,
      persist: deps.persist,
      acquisitionMissionRuntime: deps.acquisitionMissionRuntime,
    });
  } catch (err) {
    if (err && err.code === 'amo_mission_not_found') {
      fail('MISSION_BINDING_INVALID', `Unknown mission: ${missionId}`, 404);
    }
    throw err;
  }
  const mission = snapshot?.mission || snapshot;
  if (!mission || String(mission.id) !== String(missionId)) {
    fail('MISSION_BINDING_INVALID', `Unknown mission: ${missionId}`, 404);
  }
  const missionTenant = mission.tenantId != null ? String(mission.tenantId) : null;
  if (missionTenant && missionTenant !== String(clientId)) {
    fail('MISSION_TENANT_MISMATCH', 'mission does not belong to the authenticated tenant', 403);
  }
  return mission;
}

function buildServerProvenance(actor, confirmation) {
  return {
    origin: 'OPERATOR',
    origin_kind: confirmation ? 'operator_confirmed' : 'operator_authored',
    actor_kind: 'authenticated_operator',
    actor_id: actor.id,
    ...(actor.role ? { actor_role: actor.role } : {}),
  };
}

function mapCanonicalError(err) {
  if (!(err instanceof CanonicalSemanticError)) return err;
  const status =
    err.code === 'UNSUPPORTED_SEMANTIC_PRIMITIVE' ? 503
      : err.code === 'TENANT_IDENTITY_REQUIRED' || err.code === 'TENANT_AUTHORITY_FAILED' ? 403
        : err.code === 'REGISTRY_TRANSITION_UNSUPPORTED' ? 409
          : 400;
  return new OperatorJudgmentCommitError(err.code, err.message, status, err.details || {});
}

/**
 * Commit a typed operator judgment using authenticated request context.
 *
 * @param {object} input
 * @param {import('express').Request} input.req
 * @param {object} input.body - structured judgment payload (without authority overrides)
 * @param {object} [input.pool]
 * @param {object} [deps]
 */
async function commitTypedOperatorJudgment(input = {}, deps = {}) {
  const req = input.req;
  const pool = input.pool || deps.pool;
  if (!pool) fail('DATABASE_UNAVAILABLE', 'database pool is required', 503);

  const clientId = resolveClientIdFromRequest(req);
  if (!Number.isInteger(clientId) || clientId < 1) {
    fail('no_tenant', 'No active client selected.', 400);
  }

  const parsed = parseRequestBody(input.body || {});
  const tenant_id = await loadTenantKey(pool, clientId);
  const actor = resolveActor(req);

  if (parsed.associated_mission_id) {
    await validateMissionBinding(parsed.associated_mission_id, clientId, {
      ...deps,
      pool,
    });
  }

  const provenance = buildServerProvenance(actor, parsed.confirmation);
  const commitInput = {
    kind: KIND,
    tenant_id,
    client_id: clientId,
    judgment_key: parsed.judgment_key,
    judgment_kind: parsed.judgment_kind,
    label: parsed.label,
    propositions: parsed.propositions,
    provenance,
    operator: {
      id: actor.id,
      role: actor.role,
      ...(actor.email ? { email: actor.email } : {}),
    },
    ...(parsed.associated_mission_id ? { associated_mission_id: parsed.associated_mission_id } : {}),
    ...(parsed.associated_objective_identity_key
      ? { associated_objective_identity_key: parsed.associated_objective_identity_key }
      : {}),
  };

  const commit = deps.commitOperatorJudgment || commitOperatorJudgment;
  try {
    const result = await commit(pool, commitInput);
    return {
      spec: 'SPEC-251',
      tenant_id,
      client_id: clientId,
      judgment_key: parsed.judgment_key,
      judgment_identity_key: result.judgment_identity_key,
      snapshot_id: result.snapshot_id,
      newly_committed: result.newly_committed === true,
      replayed: result.replayed === true,
      interpreter_id: result.interpreter_id,
      evidence_ids: result.evidence_ids || [],
    };
  } catch (err) {
    throw mapCanonicalError(err);
  }
}

module.exports = {
  KIND,
  OperatorJudgmentCommitError,
  commitTypedOperatorJudgment,
  parseRequestBody,
  resolveClientIdFromRequest,
  resolveActor,
  buildServerProvenance,
  validateMissionBinding,
};
