'use strict';

const { EXECUTION_STATUS } = require('../types');

class PostgresDecisionStore {
  constructor(db, { clientId }) {
    this.db = db;
    this.clientId = clientId;
  }

  async init() {
    const { ensureMaxDecisionExecutionSchema } = require('../../../../utils/maxDecisionExecutionSchema');
    await ensureMaxDecisionExecutionSchema(this.db);
  }

  async findDecisionByIdempotency(clientId, idempotencyKey) {
    const { rows } = await this.db.query(
      `SELECT * FROM max_operational_decisions WHERE client_id = $1 AND idempotency_key = $2`,
      [clientId, idempotencyKey]
    );
    return rows[0] ? this._hydrateDecision(rows[0]) : null;
  }

  async findActiveDecision({ clientId, subjectType, subjectId, actionType }) {
    const { rows } = await this.db.query(`
      SELECT * FROM max_operational_decisions
      WHERE client_id = $1 AND subject_type = $2 AND subject_id = $3
        AND selected_action->>'action_type' = $4
        AND execution_status NOT IN ('SUPERSEDED', 'EXECUTION_FAILURE')
      ORDER BY created_at DESC LIMIT 1
    `, [clientId, subjectType, subjectId, actionType]);
    return rows[0] ? this._hydrateDecision(rows[0]) : null;
  }

  _hydrateDecision(row) {
    return {
      ...row,
      trigger: { type: row.trigger_type, payload: row.trigger_payload || {} },
      selected_action: row.selected_action,
    };
  }

  async persistDecision(decision) {
    await this.db.query(`
      INSERT INTO max_operational_decisions (
        id, client_id, trigger_type, trigger_payload, triggering_evidence,
        canonical_state_snapshot, subject_type, subject_id, decision_type,
        candidate_actions, selected_action, not_selected, rationale,
        supporting_evidence, assumptions, confidence, priority, owner,
        authority_class, execution_status, verification_status, idempotency_key,
        reevaluate_after, receipt_summary, superseded_by, telemetry, created_at, resolved_at
      ) VALUES (
        $1,$2,$3,$4::jsonb,$5::jsonb,$6::jsonb,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,$13,$14::jsonb,$15::jsonb,$16,$17::jsonb,$18,$19,$20,$21,$22,$23,$24,$25::jsonb,$26::jsonb,$27,$28
      )
      ON CONFLICT (id) DO UPDATE SET
        execution_status = EXCLUDED.execution_status,
        verification_status = EXCLUDED.verification_status,
        resolved_at = EXCLUDED.resolved_at,
        superseded_by = EXCLUDED.superseded_by,
        telemetry = EXCLUDED.telemetry
    `, [
      decision.id, decision.client_id, decision.trigger?.type || decision.decision_type,
      JSON.stringify(decision.trigger?.payload || {}),
      JSON.stringify(decision.triggering_evidence || []),
      JSON.stringify(decision.canonical_state_snapshot || {}),
      decision.subject_type, decision.subject_id, decision.decision_type,
      JSON.stringify(decision.candidate_actions || []),
      JSON.stringify(decision.selected_action || null),
      JSON.stringify(decision.not_selected || []),
      decision.rationale,
      JSON.stringify(decision.supporting_evidence || []),
      JSON.stringify(decision.assumptions || []),
      decision.confidence,
      JSON.stringify(decision.priority || {}),
      decision.owner,
      decision.authority_class,
      decision.execution_status,
      decision.verification_status,
      decision.idempotency_key,
      decision.reevaluate_after || null,
      decision.receipt_summary || null,
      decision.superseded_by || null,
      JSON.stringify(decision.telemetry || {}),
      decision.created_at || new Date().toISOString(),
      decision.resolved_at || null,
    ]);
    return decision;
  }

  async persistActionIntent(intent) {
    await this.db.query(`
      INSERT INTO max_operational_action_intents (
        id, decision_id, client_id, action_type, action_payload, authority_class,
        execution_status, verification_status, idempotency_key, output_payload, error_code, completed_at
      ) VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10::jsonb,$11,$12)
      ON CONFLICT (id) DO UPDATE SET
        execution_status = EXCLUDED.execution_status,
        verification_status = EXCLUDED.verification_status,
        output_payload = EXCLUDED.output_payload,
        completed_at = EXCLUDED.completed_at
    `, [
      intent.id, intent.decision_id, intent.client_id, intent.action_type,
      JSON.stringify(intent.action_payload || {}), intent.authority_class,
      intent.execution_status, intent.verification_status, intent.idempotency_key,
      JSON.stringify(intent.output_payload || {}), intent.error_code || null,
      intent.execution_status === 'EXECUTED' ? new Date().toISOString() : null,
    ]);
    return intent;
  }

  async createAoTask(task) {
    const { rows: existing } = await this.db.query(`
      SELECT * FROM max_ao_follow_up_tasks
      WHERE decision_id = $1 AND status = 'open' LIMIT 1
    `, [task.decision_id]);
    if (existing[0]) return existing[0];
    await this.db.query(`
      INSERT INTO max_ao_follow_up_tasks
        (id, client_id, decision_id, prospect_id, owner_id, owner_name, account_name, prompt, status, expectation_id)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    `, [
      task.id, task.client_id, task.decision_id, task.prospect_id,
      task.owner_id, task.owner_name, task.account_name, task.prompt,
      task.status || 'open', task.expectation_id || null,
    ]);
    return task;
  }

  async findAoTask(id) {
    const { rows } = await this.db.query(`SELECT * FROM max_ao_follow_up_tasks WHERE id = $1`, [id]);
    return rows[0] || null;
  }

  async supersedeActiveDecisions({ clientId, subjectType, subjectId, supersededBy }) {
    const { rows } = await this.db.query(`
      UPDATE max_operational_decisions
      SET execution_status = $5, superseded_by = $6, resolved_at = NOW()
      WHERE client_id = $1 AND subject_type = $2 AND subject_id = $3
        AND id <> $4 AND execution_status <> $5
      RETURNING *
    `, [clientId, subjectType, subjectId, supersededBy, EXECUTION_STATUS.SUPERSEDED, supersededBy]);
    await this.db.query(`
      UPDATE max_ao_follow_up_tasks t
      SET status = 'superseded'
      FROM max_operational_decisions d
      WHERE t.decision_id = d.id AND d.superseded_by = $1 AND t.status = 'open'
    `, [supersededBy]);
    return rows.map(r => this._hydrateDecision(r));
  }

  async createDelegation(entry) {
    return entry;
  }

  async createEscalation(entry) {
    return entry;
  }

  async listDecisions({ clientId = this.clientId } = {}) {
    const { rows } = await this.db.query(
      `SELECT * FROM max_operational_decisions WHERE client_id = $1 ORDER BY created_at DESC`,
      [clientId]
    );
    return rows.map(r => this._hydrateDecision(r));
  }
}

module.exports = {
  PostgresDecisionStore,
};
