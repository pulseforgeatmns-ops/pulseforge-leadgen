'use strict';

/**
 * SPEC-098 — durable stores for delegations, results, and Max evaluations.
 * Memory store is snapshot-serializable so tests can prove restart survival.
 * Postgres store is the production trail (Railway restart safe).
 */

const crypto = require('crypto');
const { clone } = require('./Types');

function newId() {
  return crypto.randomUUID();
}

function createMemoryStore(snapshot = null) {
  /** @type {Map<string, object>} */
  const delegations = new Map();
  /** @type {Map<string, object>} */
  const results = new Map();
  /** @type {Map<string, object>} */
  const evaluations = new Map();

  if (snapshot && typeof snapshot === 'object') {
    for (const row of snapshot.delegations || []) {
      delegations.set(row.id, clone(row));
    }
    for (const row of snapshot.results || []) {
      results.set(row.id, clone(row));
    }
    for (const row of snapshot.evaluations || []) {
      evaluations.set(row.id, clone(row));
    }
  }

  return {
    kind: 'memory',
    async insertDelegation(row) {
      delegations.set(row.id, clone(row));
      return clone(row);
    },
    async updateDelegation(row) {
      if (!delegations.has(row.id)) return null;
      delegations.set(row.id, clone(row));
      return clone(row);
    },
    async getDelegation(id, tenantId) {
      const row = delegations.get(id);
      if (!row) return null;
      if (tenantId != null && String(row.tenantId) !== String(tenantId)) return null;
      return clone(row);
    },
    async listDelegations(filter = {}) {
      let list = [...delegations.values()];
      if (filter.tenantId != null) {
        list = list.filter((r) => String(r.tenantId) === String(filter.tenantId));
      }
      if (filter.specialist) {
        list = list.filter((r) => r.specialist === filter.specialist);
      }
      if (filter.status) {
        list = list.filter((r) => r.status === filter.status);
      }
      list.sort(
        (a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
      );
      if (filter.limit != null) list = list.slice(0, filter.limit);
      return list.map(clone);
    },
    async insertResult(row) {
      results.set(row.id, clone(row));
      return clone(row);
    },
    async getResult(id, tenantId) {
      const row = results.get(id);
      if (!row) return null;
      if (tenantId != null && String(row.tenantId) !== String(tenantId)) return null;
      return clone(row);
    },
    async getResultByDelegation(delegationId, tenantId) {
      const matches = [...results.values()].filter((r) => r.delegationId === delegationId);
      const row = matches.sort(
        (a, b) => new Date(b.completedAt || b.startedAt || 0) - new Date(a.completedAt || a.startedAt || 0)
      )[0];
      if (!row) return null;
      if (tenantId != null && String(row.tenantId) !== String(tenantId)) return null;
      return clone(row);
    },
    async listResults(filter = {}) {
      let list = [...results.values()];
      if (filter.tenantId != null) {
        list = list.filter((r) => String(r.tenantId) === String(filter.tenantId));
      }
      if (filter.delegationId) {
        list = list.filter((r) => r.delegationId === filter.delegationId);
      }
      list.sort(
        (a, b) =>
          new Date(b.completedAt || b.startedAt || 0).getTime() -
          new Date(a.completedAt || a.startedAt || 0).getTime()
      );
      return list.map(clone);
    },
    async insertEvaluation(row) {
      evaluations.set(row.id, clone(row));
      return clone(row);
    },
    async getEvaluation(id, tenantId) {
      const row = evaluations.get(id);
      if (!row) return null;
      if (tenantId != null && String(row.tenantId) !== String(tenantId)) return null;
      return clone(row);
    },
    async listEvaluations(filter = {}) {
      let list = [...evaluations.values()];
      if (filter.tenantId != null) {
        list = list.filter((r) => String(r.tenantId) === String(filter.tenantId));
      }
      if (filter.delegationId) {
        list = list.filter((r) => r.delegationId === filter.delegationId);
      }
      if (filter.resultId) {
        list = list.filter((r) => r.resultId === filter.resultId);
      }
      list.sort(
        (a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
      );
      return list.map(clone);
    },
    serialize() {
      return {
        delegations: [...delegations.values()].map(clone),
        results: [...results.values()].map(clone),
        evaluations: [...evaluations.values()].map(clone),
      };
    },
    clear() {
      delegations.clear();
      results.clear();
      evaluations.clear();
    },
  };
}

function mapDelegationRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: row.tenant_id,
    specialist: row.specialist,
    capability: row.capability,
    objective: row.objective,
    reason: row.reason,
    businessContext: row.business_context || {},
    targetContext: row.target_context || null,
    evidenceRefs: row.evidence_refs || [],
    constraints: row.constraints || {},
    authority: row.authority,
    expectedReturn: row.expected_return || {},
    requestedBy: row.requested_by,
    createdAt:
      row.created_at instanceof Date
        ? row.created_at.toISOString()
        : String(row.created_at),
    status: row.status,
    policyEvents: row.policy_events || [],
    updatedAt:
      row.updated_at instanceof Date
        ? row.updated_at.toISOString()
        : String(row.updated_at),
  };
}

function mapResultRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    delegationId: row.delegation_id,
    tenantId: row.tenant_id,
    specialist: row.specialist,
    capability: row.capability,
    status: row.status,
    summary: row.summary || null,
    observations: row.observations || [],
    actionsTaken: row.actions_taken || [],
    evidenceRefs: row.evidence_refs || [],
    artifactRefs: row.artifact_refs || [],
    confidence: row.confidence == null ? null : Number(row.confidence),
    uncertainties: row.uncertainties || [],
    recommendedNextAction: row.recommended_next_action || null,
    policyEvents: row.policy_events || [],
    errors: row.errors || [],
    startedAt:
      row.started_at instanceof Date
        ? row.started_at.toISOString()
        : row.started_at
          ? String(row.started_at)
          : null,
    completedAt:
      row.completed_at instanceof Date
        ? row.completed_at.toISOString()
        : row.completed_at
          ? String(row.completed_at)
          : null,
  };
}

function mapEvaluationRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: row.tenant_id,
    delegationId: row.delegation_id,
    resultId: row.result_id,
    objectiveSatisfied: row.objective_satisfied === true,
    materialChange: row.material_change === true,
    warrantsOperatorAttention: row.warrants_operator_attention === true,
    warrantsAnotherDelegation: row.warrants_another_delegation === true,
    suggestedPriorityChange: row.suggested_priority_change || null,
    priorityApplied: row.priority_applied === true,
    operatorDirectionHonored: row.operator_direction_honored !== false,
    acceptedAsGroundTruth: row.accepted_as_ground_truth === true,
    explanation: row.explanation,
    provenance: row.provenance || {},
    payload: row.payload || {},
    createdAt:
      row.created_at instanceof Date
        ? row.created_at.toISOString()
        : String(row.created_at),
  };
}

function createPostgresStore(pool) {
  const db = pool || require('../../../db');
  return {
    kind: 'postgres',
    async insertDelegation(row) {
      const result = await db.query(
        `INSERT INTO specialist_delegations (
          id, tenant_id, specialist, capability, objective, reason,
          business_context, target_context, evidence_refs, constraints,
          authority, expected_return, requested_by, created_at, status,
          policy_events, updated_at
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb,
          $11,$12::jsonb,$13,$14,$15,$16::jsonb,$17
        ) RETURNING *`,
        [
          row.id,
          row.tenantId,
          row.specialist,
          row.capability,
          row.objective,
          row.reason,
          JSON.stringify(row.businessContext || {}),
          JSON.stringify(row.targetContext || null),
          JSON.stringify(row.evidenceRefs || []),
          JSON.stringify(row.constraints || {}),
          row.authority,
          JSON.stringify(row.expectedReturn || {}),
          row.requestedBy,
          row.createdAt,
          row.status,
          JSON.stringify(row.policyEvents || []),
          row.updatedAt,
        ]
      );
      return mapDelegationRow(result.rows[0]);
    },
    async updateDelegation(row) {
      const result = await db.query(
        `UPDATE specialist_delegations SET
          status = $2,
          policy_events = $3::jsonb,
          updated_at = $4
        WHERE id = $1 AND tenant_id = $5
        RETURNING *`,
        [
          row.id,
          row.status,
          JSON.stringify(row.policyEvents || []),
          row.updatedAt,
          row.tenantId,
        ]
      );
      return mapDelegationRow(result.rows[0] || null);
    },
    async getDelegation(id, tenantId) {
      const result = await db.query(
        `SELECT * FROM specialist_delegations WHERE id = $1 AND tenant_id = $2`,
        [id, String(tenantId)]
      );
      return mapDelegationRow(result.rows[0] || null);
    },
    async listDelegations(filter = {}) {
      const params = [];
      const clauses = [];
      if (filter.tenantId != null) {
        params.push(String(filter.tenantId));
        clauses.push(`tenant_id = $${params.length}`);
      }
      if (filter.specialist) {
        params.push(filter.specialist);
        clauses.push(`specialist = $${params.length}`);
      }
      if (filter.status) {
        params.push(filter.status);
        clauses.push(`status = $${params.length}`);
      }
      const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
      const limit = filter.limit != null ? `LIMIT ${Number(filter.limit)}` : '';
      const result = await db.query(
        `SELECT * FROM specialist_delegations ${where} ORDER BY created_at DESC ${limit}`,
        params
      );
      return result.rows.map(mapDelegationRow);
    },
    async insertResult(row) {
      const result = await db.query(
        `INSERT INTO specialist_results (
          id, delegation_id, tenant_id, specialist, capability, status,
          summary, observations, actions_taken, evidence_refs, artifact_refs,
          confidence, uncertainties, recommended_next_action, policy_events,
          errors, started_at, completed_at
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb,$11::jsonb,
          $12,$13::jsonb,$14::jsonb,$15::jsonb,$16::jsonb,$17,$18
        ) RETURNING *`,
        [
          row.id,
          row.delegationId,
          row.tenantId,
          row.specialist,
          row.capability,
          row.status,
          row.summary,
          JSON.stringify(row.observations || []),
          JSON.stringify(row.actionsTaken || []),
          JSON.stringify(row.evidenceRefs || []),
          JSON.stringify(row.artifactRefs || []),
          row.confidence,
          JSON.stringify(row.uncertainties || []),
          JSON.stringify(row.recommendedNextAction || null),
          JSON.stringify(row.policyEvents || []),
          JSON.stringify(row.errors || []),
          row.startedAt,
          row.completedAt,
        ]
      );
      return mapResultRow(result.rows[0]);
    },
    async getResult(id, tenantId) {
      const result = await db.query(
        `SELECT * FROM specialist_results WHERE id = $1 AND tenant_id = $2`,
        [id, String(tenantId)]
      );
      return mapResultRow(result.rows[0] || null);
    },
    async getResultByDelegation(delegationId, tenantId) {
      const result = await db.query(
        `SELECT * FROM specialist_results
         WHERE delegation_id = $1 AND tenant_id = $2
         ORDER BY completed_at DESC NULLS LAST, started_at DESC
         LIMIT 1`,
        [delegationId, String(tenantId)]
      );
      return mapResultRow(result.rows[0] || null);
    },
    async listResults(filter = {}) {
      const params = [];
      const clauses = [];
      if (filter.tenantId != null) {
        params.push(String(filter.tenantId));
        clauses.push(`tenant_id = $${params.length}`);
      }
      if (filter.delegationId) {
        params.push(filter.delegationId);
        clauses.push(`delegation_id = $${params.length}`);
      }
      const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
      const result = await db.query(
        `SELECT * FROM specialist_results ${where}
         ORDER BY completed_at DESC NULLS LAST`,
        params
      );
      return result.rows.map(mapResultRow);
    },
    async insertEvaluation(row) {
      const result = await db.query(
        `INSERT INTO specialist_evaluations (
          id, tenant_id, delegation_id, result_id, objective_satisfied,
          material_change, warrants_operator_attention, warrants_another_delegation,
          suggested_priority_change, priority_applied, operator_direction_honored,
          accepted_as_ground_truth, explanation, provenance, payload, created_at
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14::jsonb,$15::jsonb,$16
        ) RETURNING *`,
        [
          row.id,
          row.tenantId,
          row.delegationId,
          row.resultId,
          row.objectiveSatisfied,
          row.materialChange,
          row.warrantsOperatorAttention,
          row.warrantsAnotherDelegation,
          JSON.stringify(row.suggestedPriorityChange || null),
          row.priorityApplied,
          row.operatorDirectionHonored,
          row.acceptedAsGroundTruth,
          row.explanation,
          JSON.stringify(row.provenance || {}),
          JSON.stringify(row.payload || {}),
          row.createdAt,
        ]
      );
      return mapEvaluationRow(result.rows[0]);
    },
    async getEvaluation(id, tenantId) {
      const result = await db.query(
        `SELECT * FROM specialist_evaluations WHERE id = $1 AND tenant_id = $2`,
        [id, String(tenantId)]
      );
      return mapEvaluationRow(result.rows[0] || null);
    },
    async listEvaluations(filter = {}) {
      const params = [];
      const clauses = [];
      if (filter.tenantId != null) {
        params.push(String(filter.tenantId));
        clauses.push(`tenant_id = $${params.length}`);
      }
      if (filter.delegationId) {
        params.push(filter.delegationId);
        clauses.push(`delegation_id = $${params.length}`);
      }
      if (filter.resultId) {
        params.push(filter.resultId);
        clauses.push(`result_id = $${params.length}`);
      }
      const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
      const result = await db.query(
        `SELECT * FROM specialist_evaluations ${where} ORDER BY created_at DESC`,
        params
      );
      return result.rows.map(mapEvaluationRow);
    },
  };
}

function resolveStore(opts = {}) {
  if (opts.store) return opts.store;
  if (opts.pool) return createPostgresStore(opts.pool);
  return createMemoryStore();
}

module.exports = {
  newId,
  createMemoryStore,
  createPostgresStore,
  resolveStore,
  mapDelegationRow,
  mapResultRow,
  mapEvaluationRow,
};
