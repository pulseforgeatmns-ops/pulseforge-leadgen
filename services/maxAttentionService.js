'use strict';

const pool = require('../db');
const {
  runAttentionCycle,
  wakeAttentionForIngestion,
  syncAttentionFromDecision,
  operatorVisibleItems,
  MemoryAttentionStore,
  PostgresAttentionStore,
} = require('../packages/max/attention');
const { PostgresStateStore } = require('../packages/max/stateIngestion');
const { PostgresDecisionStore } = require('../packages/max/decisionExecution');

// Scheduler startup must never migrate schema, seed clients or backfill users.
// Explicit projections fail closed on absent tables/columns before any run write.
async function assertSchedulerReady(db) {
  const checks = [
    'SELECT id, client_id, prospect_id, company_id, ao_id, expectation_type, description, expected_window, status, source_ingestion_id, source_claim_id, source_evidence, resolved_at, created_at, updated_at FROM max_open_expectations LIMIT 0',
    'SELECT id, client_id, trigger_type, trigger_payload, triggering_evidence, canonical_state_snapshot, subject_type, subject_id, decision_type, candidate_actions, selected_action, not_selected, rationale, supporting_evidence, assumptions, confidence, priority, owner, authority_class, execution_status, verification_status, idempotency_key, reevaluate_after, receipt_summary, superseded_by, telemetry, created_at, resolved_at FROM max_operational_decisions LIMIT 0',
    'SELECT id, decision_id, client_id, action_type, action_payload, authority_class, execution_status, verification_status, idempotency_key, output_payload, error_code, created_at, completed_at FROM max_operational_action_intents LIMIT 0',
    'SELECT id, client_id, decision_id, prospect_id, owner_id, owner_name, account_name, prompt, status, expectation_id, created_at, completed_at FROM max_ao_follow_up_tasks LIMIT 0',
    'SELECT id, client_id, subject_type, subject_id, reason, source_decision_id, supporting_evidence, status, priority, next_review_at, review_trigger, owner, audience_tier, parent_attention_id, dedup_fingerprint, resolution_evidence, last_evaluation_id, claim_token, claimed_at, execution_failures, created_at, last_reviewed_at, resolved_at FROM max_attention_items LIMIT 0',
    'SELECT id, client_id, started_at, completed_at, status, items_claimed, items_evaluated, items_failed, error_message, telemetry FROM max_attention_scheduler_runs LIMIT 0',
    'SELECT id, last_successful_cycle_at, last_run_id, consecutive_failures, updated_at FROM max_attention_heartbeats LIMIT 0',
    'SELECT id, client_id, company_id, assigned_ao_id, status, acquisition_metadata FROM prospects LIMIT 0',
    'SELECT id, name FROM companies LIMIT 0',
    'SELECT id, tenant_id, prospect_id, ao_id, activity_type, outcome, metadata, created_at FROM ao_prospect_activity LIMIT 0',
  ];
  try {
    for (const sql of checks) await db.query(sql);
    const { rows } = await db.query("SELECT id FROM max_attention_heartbeats WHERE id = 'global'");
    if (rows.length !== 1) throw new Error('Missing scheduler heartbeat');
  } catch {
    throw Object.assign(new Error('Max attention schema is not ready; apply reviewed migrations separately'), {
      code: 'MAX_ATTENTION_SCHEMA_NOT_READY',
    });
  }
}

async function createStores(clientId, db = pool) {
  await assertSchedulerReady(db);
  return {
    stateStore: new PostgresStateStore(db, { clientId }),
    decisionStore: new PostgresDecisionStore(db, { clientId }),
    attentionStore: new PostgresAttentionStore(db, { clientId }),
  };
}

async function runAttentionScheduler(clientId, { db = pool, now = new Date(), limit = 20 } = {}) {
  const { stateStore, decisionStore, attentionStore } = await createStores(clientId, db);
  return runAttentionCycle({
    clientId,
    stateStore,
    decisionStore,
    attentionStore,
    now,
    limit,
  });
}

async function runAttentionSchedulerAllClients({ db = pool, now = new Date(), limit = 20 } = {}) {
  const { rows } = await db.query(`SELECT id FROM clients WHERE active = true ORDER BY id`);
  const results = [];
  for (const row of rows) {
    try {
      results.push({
        client_id: row.id,
        ...(await runAttentionScheduler(row.id, { db, now, limit })),
      });
    } catch (err) {
      results.push({ client_id: row.id, error: err.message });
    }
  }
  return results;
}

async function afterIngestionAttention(clientId, ingestionResult, { db = pool, now = new Date() } = {}) {
  const attentionStore = new PostgresAttentionStore(db, { clientId });
  await attentionStore.init();
  const stateStore = new PostgresStateStore(db, { clientId });
  await stateStore.init();
  return wakeAttentionForIngestion({
    attentionStore,
    clientId,
    ingestionResult,
    stateStore,
    now,
  });
}

async function afterDecisionAttention(clientId, decisionResult, { db = pool, now = new Date() } = {}) {
  if (!decisionResult?.decision) return null;
  const attentionStore = new PostgresAttentionStore(db, { clientId });
  await attentionStore.init();
  return syncAttentionFromDecision({
    attentionStore,
    decision: decisionResult.decision,
    snapshot: decisionResult.snapshot,
    now,
  });
}

async function listOperatorAttention(clientId, { db = pool, limit = 10 } = {}) {
  const attentionStore = new PostgresAttentionStore(db, { clientId });
  await attentionStore.init();
  const items = await attentionStore.listUnresolved({ clientId });
  return operatorVisibleItems(items, { limit });
}

async function getAttentionHealth({ db = pool } = {}) {
  const attentionStore = new PostgresAttentionStore(db, {});
  await attentionStore.init();
  return attentionStore.getHeartbeat();
}

module.exports = {
  runAttentionScheduler,
  runAttentionSchedulerAllClients,
  afterIngestionAttention,
  afterDecisionAttention,
  listOperatorAttention,
  getAttentionHealth,
  MemoryAttentionStore,
};
