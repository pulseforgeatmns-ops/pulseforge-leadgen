'use strict';

// Deliberately not an ingestion engine or a live AO hook. This one-event adapter
// only records an operator review; it never interprets a note as permission to act.
// Callers must provide a pool of dedicated connections. No shared db/init imports.
const { createHash } = require('node:crypto');
const { PostgresStateStore } = require('../packages/max/stateIngestion/store/postgresStore');
const { PostgresDecisionStore } = require('../packages/max/decisionExecution/store/postgresStore');
const { PostgresAttentionStore } = require('../packages/max/attention/store/postgresStore');
const { MANUAL_REVIEW_PREFIX } = require('../packages/max/attention/manualReviewOnly');

const SCOPE = Object.freeze({
  clientId: 10,
  prospectId: 'c36194b0-2203-4dc5-9f7f-40292b264ba1',
  aoId: 24,
  activityId: 'a299ce4e-4f43-4c62-b3d4-c962e890350c',
  companionActivityId: '3cdf94e5-915a-4d75-8dee-dba2d5d3643a',
});
const ADAPTER_VERSION = 'ao-help-review-v1';
const BUSINESS_KEY = `${MANUAL_REVIEW_PREFIX}${SCOPE.clientId}:${SCOPE.activityId}`;
const IDS = Object.freeze({ ingestion: `${BUSINESS_KEY}:ingestion`, decision: `${BUSINESS_KEY}:decision`, attention: `${BUSINESS_KEY}:attention` });
const MAX_PREVIEW_AGE_MS = 15 * 60 * 1000;
const MAX_EVIDENCE_ROWS = 1000;
const UNCERTAINTY = 'No completion of this request is recorded in the checked Pulseforge sources. Contact or completion outside this system is unknown; confirm with the operator before any follow-up.';

function failure(code, message = code) { return Object.assign(new Error(message), { code }); }
function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  return JSON.stringify(value);
}
function sourceVersion(snapshot) { return createHash('sha256').update(canonicalJson(snapshot)).digest('hex'); }
function validateScope(input) {
  for (const key of ['clientId', 'prospectId', 'aoId', 'activityId']) {
    if (!['string', 'number'].includes(typeof input[key]) || String(input[key]) !== String(SCOPE[key])) throw failure('AO_HELP_REVIEW_SCOPE_MISMATCH');
  }
}

// Full precision PostgreSQL timestamps and explicit row-version tokens prevent
// source edits within one millisecond from disappearing in JS Date conversion.
async function readSnapshot(db, { lock = false } = {}) {
  const args = [SCOPE.clientId, SCOPE.prospectId];
  const tenant = (await db.query(`SELECT id, active, xmin::text AS row_version FROM clients
    WHERE id=$1 ${lock ? 'FOR SHARE' : ''}`, [SCOPE.clientId])).rows[0];
  if (!tenant || tenant.active !== true) throw failure('AO_HELP_REVIEW_TENANT_UNAVAILABLE');
  const p = (await db.query(`SELECT p.id::text, p.client_id, p.assigned_ao_id,
    p.help_requested, p.help_resolved_at::text, p.help_requested_at::text,
    p.help_reason, p.help_manager_note, p.ao_current_status, p.ao_next_action,
    p.ao_last_touch_at::text, p.ao_paused, p.status, p.xmin::text AS row_version,
    c.name AS account_name
    FROM prospects p LEFT JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
    WHERE p.client_id=$1 AND p.id=$2::uuid ${lock ? 'FOR SHARE OF p' : ''}`, args)).rows[0];
  if (!p || p.assigned_ao_id !== SCOPE.aoId) throw failure('AO_HELP_REVIEW_OWNER_MISMATCH');
  if (p.help_requested !== true || p.help_resolved_at) throw failure('AO_HELP_REVIEW_HELP_NOT_OPEN');
  const owner = (await db.query(`SELECT id, client_id, name, role, active, xmin::text AS row_version
    FROM users WHERE client_id=$1 AND id=$2 ${lock ? 'FOR SHARE' : ''}`, [SCOPE.clientId, SCOPE.aoId])).rows[0];
  if (!owner || owner.active !== true) throw failure('AO_HELP_REVIEW_OWNER_UNAVAILABLE');
  const activityRows = (await db.query(`SELECT id::text, tenant_id, prospect_id::text, ao_id,
    activity_type, outcome, notes, metadata, created_at::text, xmin::text AS row_version
    FROM ao_prospect_activity WHERE tenant_id=$1 AND prospect_id=$2::uuid
      AND id=ANY($3::uuid[]) ORDER BY created_at,id ${lock ? 'FOR SHARE' : ''}`,
  [...args, [SCOPE.activityId, SCOPE.companionActivityId]])).rows;
  const source = activityRows.find(row => row.id === SCOPE.activityId);
  const companion = activityRows.find(row => row.id === SCOPE.companionActivityId);
  if (!source || source.ao_id !== SCOPE.aoId || source.activity_type !== 'help_requested' || source.outcome !== 'needs_jake'
    || !companion || companion.ao_id !== SCOPE.aoId || companion.activity_type !== 'help_requested'
    || companion.metadata?.source !== 'ao_crm_outcome_form') throw failure('AO_HELP_REVIEW_SOURCE_UNSUPPORTED');
  const bounded = async (name, sql, params = args) => {
    const { rows } = await db.query(`${sql} LIMIT ${MAX_EVIDENCE_ROWS + 1}`, params);
    if (rows.length > MAX_EVIDENCE_ROWS) throw failure('AO_HELP_REVIEW_EVIDENCE_TRUNCATED', `${name} exceeds bounded evidence coverage`);
    return rows;
  };
  const evidence = {};
  evidence.ao_activity = await bounded('ao_activity', `SELECT id::text, ao_id, activity_type, outcome,
    notes, metadata, created_at::text, xmin::text AS row_version, created_at >= $3::timestamptz AS at_or_after_source
    FROM ao_prospect_activity WHERE tenant_id=$1 AND prospect_id=$2::uuid ORDER BY created_at,id`, [...args, source.created_at]);
  evidence.touchpoints = await bounded('touchpoints', `SELECT id::text, channel, action_type, outcome,
    created_at::text, xmin::text AS row_version, created_at >= $3::timestamptz AS at_or_after_source
    FROM touchpoints WHERE client_id=$1 AND prospect_id=$2::uuid ORDER BY created_at,id`, [...args, source.created_at]);
  evidence.assignment_tasks = await bounded('assignment_tasks', `SELECT id::text, assigned_ao_id, status,
    created_at::text, completed_at::text, xmin::text AS row_version FROM ao_prospect_tasks
    WHERE client_id=$1 AND prospect_id=$2::uuid ORDER BY id`);
  evidence.email_events = await bounded('email_events', `SELECT event_id::text, event_type, event_at::text,
    xmin::text AS row_version FROM email_events WHERE client_id=$1 AND prospect_id=$2::uuid ORDER BY event_id`);
  evidence.drafts = await bounded('drafts', `SELECT id::text, assigned_ao_id, status, created_at::text, updated_at::text,
    xmin::text AS row_version FROM ao_followup_drafts WHERE tenant_id=$1 AND account_id=$2::uuid ORDER BY id`);
  evidence.account_flags = await bounded('account_flags', `SELECT id::text, ao_id, status, created_at::text,
    resolved_at::text, xmin::text AS row_version FROM ao_account_flags WHERE client_id=$1 AND account_id=$2::uuid ORDER BY id`);
  const outboundArgs = [String(SCOPE.clientId), SCOPE.prospectId];
  evidence.outbound_executions = await bounded('outbound_executions', `SELECT id, status, attempted_at::text,
    sent_at::text, updated_at::text, xmin::text AS row_version FROM acquisition_mission_outbound_executions
    WHERE tenant_id=$1 AND prospect_id=$2 ORDER BY id`, outboundArgs);
  evidence.provider_events = await bounded('provider_events', `SELECT id, execution_record_id, event_type,
    event_category, occurred_at::text, xmin::text AS row_version FROM acquisition_mission_provider_events
    WHERE tenant_id=$1 AND prospect_id=$2 ORDER BY id`, outboundArgs);
  evidence.outbound_items = await bounded('outbound_items', `SELECT id, status, attempted_at::text,
    xmin::text AS row_version FROM acquisition_outbound_items WHERE tenant_id=$1 AND prospect_id=$2 ORDER BY id`, outboundArgs);
  evidence.replies = await bounded('replies', `SELECT id, classification, created_at::text, classified_at::text,
    xmin::text AS row_version FROM acquisition_outbound_replies WHERE tenant_id=$1 AND prospect_id=$2 ORDER BY id`, outboundArgs);
  return { adapter_version: ADAPTER_VERSION, scope: SCOPE, tenant, source, companion_evidence: companion, prospect: p, owner, evidence };
}
function eligibility(snapshot) {
  // This first proof supports only the reconciled zero-later-evidence case.
  // Any later evidence requires human reconciliation, not guessed completion.
  const nonempty = Object.entries(snapshot.evidence).filter(([kind, rows]) => !['assignment_tasks', 'ao_activity', 'touchpoints'].includes(kind) && rows.length).map(([kind]) => kind);
  // Hash the entire bounded account history, including equal/backdated records.
  // Eligibility is separately request-relative; old assignment/contact work must
  // not be misrepresented as completion of this later help request.
  if (snapshot.evidence.ao_activity.some(row => ![SCOPE.activityId, SCOPE.companionActivityId].includes(row.id) && row.at_or_after_source !== false)) nonempty.push('ao_activity_at_or_after_request');
  if (snapshot.evidence.touchpoints.some(row => row.at_or_after_source !== false)) nonempty.push('touchpoints_at_or_after_request');
  if (snapshot.evidence.assignment_tasks.some(task => !['completed', 'cancelled'].includes(task.status))) nonempty.push('active_assignment_tasks');
  if (nonempty.length) throw failure('AO_HELP_REVIEW_EVIDENCE_REQUIRES_REVIEW', `Reconcile existing evidence first: ${nonempty.join(', ')}`);
}
async function assertReady(db) {
  await db.query('SELECT id, client_id, source_type, source_actor, raw_source, received_at, receipt_summary, telemetry, pipeline_audit, created_at FROM max_operational_ingestions LIMIT 0');
  await db.query('SELECT id, client_id, trigger_type, trigger_payload, triggering_evidence, canonical_state_snapshot, subject_type, subject_id, decision_type, candidate_actions, selected_action, not_selected, rationale, supporting_evidence, assumptions, confidence, priority, owner, authority_class, execution_status, verification_status, idempotency_key, reevaluate_after, receipt_summary, superseded_by, telemetry, created_at, resolved_at FROM max_operational_decisions LIMIT 0');
  await db.query('SELECT id, client_id, subject_type, subject_id, reason, source_decision_id, supporting_evidence, status, priority, next_review_at, review_trigger, owner, audience_tier, parent_attention_id, dedup_fingerprint, resolution_evidence, last_evaluation_id, claim_token, claimed_at, execution_failures, created_at, last_reviewed_at, resolved_at FROM max_attention_items LIMIT 0');
  const { rows } = await db.query(`SELECT c.relname AS table_name,
    array_agg(a.attname::text ORDER BY k.ordinality) AS columns
    FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid
    CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,ordinality)
    JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum
    WHERE i.indrelid=ANY(ARRAY[to_regclass('max_operational_ingestions'),
      to_regclass('max_operational_decisions'),to_regclass('max_attention_items')])
      AND i.indisunique AND i.indisvalid AND i.indisready AND i.indpred IS NULL AND i.indexprs IS NULL
    GROUP BY c.relname,i.indexrelid`);
  const required = { max_operational_ingestions: [['id']], max_operational_decisions: [['id'], ['client_id', 'idempotency_key']],
    max_attention_items: [['id'], ['client_id', 'dedup_fingerprint']] };
  for (const [table, indexes] of Object.entries(required)) {
    if (indexes.some(columns => !rows.some(row => row.table_name === table && canonicalJson(row.columns) === canonicalJson(columns)))) {
      throw failure('AO_HELP_REVIEW_SCHEMA_NOT_READY', 'Required unique constraints are absent; no automatic initialization is allowed');
    }
  }
}
async function readChain(db) {
  const i = await db.query('SELECT *, received_at::text AS received_at_exact, created_at::text AS created_at_exact FROM max_operational_ingestions WHERE client_id=$1 AND id=$2', [SCOPE.clientId, IDS.ingestion]);
  const d = await db.query('SELECT *, created_at::text AS created_at_exact FROM max_operational_decisions WHERE client_id=$1 AND (id=$2 OR idempotency_key=$3)', [SCOPE.clientId, IDS.decision, BUSINESS_KEY]);
  const a = await db.query('SELECT *, created_at::text AS created_at_exact FROM max_attention_items WHERE client_id=$1 AND (id=$2 OR dedup_fingerprint=$3)', [SCOPE.clientId, IDS.attention, BUSINESS_KEY]);
  if (!i.rows.length && !d.rows.length && !a.rows.length) return null;
  const ingestion = i.rows[0], decision = d.rows[0], attention = a.rows[0];
  if (i.rows.length !== 1 || d.rows.length !== 1 || a.rows.length !== 1
    || decision.id !== IDS.decision || attention.id !== IDS.attention
    || decision.trigger_payload?.ingestion_id !== IDS.ingestion || attention.source_decision_id !== IDS.decision
    || ingestion.pipeline_audit?.linked_ids?.attention !== IDS.attention
    || ingestion.pipeline_audit?.linked_ids?.decision !== IDS.decision
    || decision.idempotency_key !== BUSINESS_KEY || attention.dedup_fingerprint !== BUSINESS_KEY
    || decision.selected_action?.action_type !== 'ESCALATE_OPERATOR' || decision.authority_class !== 'C'
    || decision.execution_status !== 'EXECUTION_BLOCKED' || !ingestion.receipt_summary || !decision.receipt_summary
    || attention.priority?.manual_review_only !== true || attention.next_review_at !== null
    || attention.review_trigger !== 'MANUAL' || attention.audience_tier !== 'OPERATOR_VISIBLE'
    || ingestion.raw_source?.source_version !== decision.trigger_payload?.source_version
    || ingestion.raw_source?.adapter_version !== ADAPTER_VERSION || ingestion.raw_source?.business_key !== BUSINESS_KEY
    || sourceVersion(ingestion.raw_source?.snapshot || {}) !== ingestion.raw_source?.source_version
    || canonicalJson(ingestion.raw_source?.snapshot?.scope) !== canonicalJson(SCOPE)
    || ingestion.raw_source?.snapshot?.source?.id !== SCOPE.activityId
    || ingestion.pipeline_audit?.linked_ids?.ingestion !== IDS.ingestion
    || decision.trigger_payload?.source_activity_id !== SCOPE.activityId
    || decision.subject_type !== 'prospect' || decision.subject_id !== SCOPE.prospectId
    || attention.subject_type !== 'prospect' || attention.subject_id !== SCOPE.prospectId
    || decision.trigger_payload?.manual_review_only !== true || decision.reevaluate_after !== null
    || attention.claim_token !== null || attention.resolved_at && !['RESOLVED', 'SUPERSEDED'].includes(attention.status)) {
    throw failure('AO_HELP_REVIEW_INCOMPLETE_OR_CHANGED_CHAIN');
  }
  const expected = buildRecords(ingestion.raw_source.snapshot, ingestion.raw_source.source_version, ingestion.raw_source.observed_at);
  const fieldsMatch = (actual, wanted, excluded = []) => Object.entries(wanted).every(([key, value]) =>
    excluded.includes(key) || canonicalJson(actual[key]) === canonicalJson(value));
  const closed = ['RESOLVED', 'SUPERSEDED'].includes(attention.status);
  if (!fieldsMatch(ingestion, expected.ingestion, ['received_at'])
    || ingestion.received_at_exact !== expected.ingestion.received_at || ingestion.created_at_exact !== expected.ingestion.received_at
    || !fieldsMatch(decision, expected.decision, ['trigger', 'created_at', 'confidence'])
    || decision.trigger_type !== expected.decision.trigger.type
    || canonicalJson(decision.trigger_payload) !== canonicalJson(expected.decision.trigger.payload)
    || Number(decision.confidence) !== expected.decision.confidence || decision.created_at_exact !== expected.decision.created_at
    || decision.superseded_by !== null || decision.resolved_at !== null
    || !fieldsMatch(attention, expected.attention, ['created_at', 'status'])
    || attention.created_at_exact !== expected.attention.created_at
    || attention.parent_attention_id !== null || attention.claim_token !== null || attention.claimed_at !== null
    || attention.execution_failures !== 0
    || (!closed && (attention.status !== 'ACTIVE' || attention.resolved_at !== null || attention.last_reviewed_at !== null
      || canonicalJson(attention.resolution_evidence) !== '[]'))
    || (closed && !attention.resolved_at)) {
    throw failure('AO_HELP_REVIEW_INCOMPLETE_OR_CHANGED_CHAIN');
  }
  return { ingestion, decision, attention };
}
function resultFor(chain, status) {
  return { status, business_key: BUSINESS_KEY, ids: IDS, source_version: chain.ingestion.raw_source.source_version,
    receipt_summary: chain.ingestion.receipt_summary, attention_status: chain.attention.status,
    observed_at: chain.ingestion.raw_source.observed_at, freshness_boundary: 'database_transaction_snapshot',
    manual_review_only: true, outside_contact: 'unknown', readback_verified: true };
}
function buildRecords(snapshot, version, observedAt) {
  const reason = `As of the database transaction snapshot at ${observedAt}, ${snapshot.owner.name || 'the AO'}'s recorded help request for ${snapshot.prospect.account_name || 'this account'} remains open. ${UNCERTAINTY}`;
  const evidence = [{ kind: 'ao_help_review', review_only: true, source_table: 'ao_prospect_activity', source_id: SCOPE.activityId,
    occurred_at: snapshot.source.created_at, source_version: version, observed_at: observedAt, outside_contact: 'unknown' }];
  const receipt = `${reason} Recorded operator-review attention only; no contact, task, expectation or CRM state was changed.`;
  const priority = { urgency: 0, importance: 1, mission_relevance: 0, manual_review_only: true };
  return {
    ingestion: { id: IDS.ingestion, client_id: SCOPE.clientId, source_type: 'AO_REPORTED', source_actor: String(SCOPE.aoId),
      raw_source: { adapter_version: ADAPTER_VERSION, business_key: BUSINESS_KEY, source_version: version, observed_at: observedAt, snapshot,
        checked_sources: Object.fromEntries(Object.entries(snapshot.evidence).map(([key, rows]) => [key, { status: 'read', count: rows.length }])),
        outside_contact: 'unknown', freshness_boundary: 'database_transaction_snapshot', assignment_task_completion_is_not_contact_proof: true },
      received_at: observedAt, receipt_summary: receipt,
      telemetry: { review_only: true, ingestion_rows: 1, decision_rows: 1, attention_rows: 1, actions_executed: 0 },
      pipeline_audit: { adapter_version: ADAPTER_VERSION, status: 'review_recorded', linked_ids: IDS, source_version: version } },
    decision: { id: IDS.decision, client_id: SCOPE.clientId, trigger: { type: 'AO_UPDATE', payload: { subtype: 'ao_help_review',
      ingestion_id: IDS.ingestion, source_activity_id: SCOPE.activityId, source_version: version, manual_review_only: true } },
      triggering_evidence: evidence, canonical_state_snapshot: snapshot, subject_type: 'prospect', subject_id: SCOPE.prospectId,
      decision_type: 'ao_help_review', candidate_actions: [{ action_type: 'ESCALATE_OPERATOR' }],
      selected_action: { action_type: 'ESCALATE_OPERATOR', review_only: true }, not_selected: [], rationale: reason,
      supporting_evidence: evidence, assumptions: [UNCERTAINTY], confidence: 1, priority, owner: 'operator', authority_class: 'C',
      execution_status: 'EXECUTION_BLOCKED', verification_status: null, idempotency_key: BUSINESS_KEY,
      reevaluate_after: null, receipt_summary: receipt, telemetry: { review_only: true, actions_executed: 0 }, created_at: observedAt },
    attention: { id: IDS.attention, client_id: SCOPE.clientId, subject_type: 'prospect', subject_id: SCOPE.prospectId, reason,
      source_decision_id: IDS.decision, supporting_evidence: evidence, status: 'ACTIVE', priority, next_review_at: null,
      review_trigger: 'MANUAL', owner: 'operator', audience_tier: 'OPERATOR_VISIBLE', dedup_fingerprint: BUSINESS_KEY,
      last_evaluation_id: IDS.decision, created_at: observedAt },
  };
}
async function inReadOnly(pool, callback) {
  const db = await pool.connect();
  try {
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    await db.query("SET LOCAL statement_timeout='5000ms'");
    await db.query("SET LOCAL lock_timeout='1000ms'");
    await db.query("SET LOCAL timezone='UTC'");
    return await callback(db);
  } finally { try { await db.query('ROLLBACK'); } finally { db.release(); } }
}
async function reconcileAoHelpReview({ pool, ...scope }) {
  validateScope(scope);
  return inReadOnly(pool, async db => {
    await assertReady(db);
    const chain = await readChain(db);
    return chain ? resultFor(chain, 'recorded') : { status: 'not_recorded', business_key: BUSINESS_KEY, ids: IDS };
  });
}
async function previewAoHelpReview({ pool, ...scope }) {
  validateScope(scope);
  return inReadOnly(pool, async db => {
    await assertReady(db);
    const snapshot = await readSnapshot(db);
    eligibility(snapshot);
    const version = sourceVersion(snapshot);
    const observedAt = (await db.query('SELECT transaction_timestamp()::text AS observed_at')).rows[0].observed_at;
    const chain = await readChain(db);
    if (chain && chain.ingestion.raw_source.source_version !== version) throw failure('AO_HELP_REVIEW_SOURCE_CHANGED');
    return { mode: 'preview', business_key: BUSINESS_KEY, ids: IDS, source_version: version, observed_at: observedAt,
      expires_in_ms: MAX_PREVIEW_AGE_MS, scope: SCOPE, snapshot, reason: buildRecords(snapshot, version, observedAt).attention.reason,
      existing: chain ? resultFor(chain, 'already_recorded') : null, proposed_rows: chain ? 0 : 3, outside_contact: 'unknown', freshness_boundary: 'database_transaction_snapshot', manual_review_only: true };
  });
}
async function applyAoHelpReview({ pool, expectedSourceVersion, previewObservedAt, ...scope }) {
  validateScope(scope);
  if (!/^[a-f0-9]{64}$/.test(expectedSourceVersion || '') || !Number.isFinite(Date.parse(previewObservedAt))) {
    throw failure('AO_HELP_REVIEW_PREVIEW_REQUIRED');
  }
  const db = await pool.connect();
  let committing = false;
  let committed = false;
  try {
    await db.query('BEGIN ISOLATION LEVEL SERIALIZABLE');
    await db.query("SET LOCAL statement_timeout='5000ms'");
    await db.query("SET LOCAL lock_timeout='1000ms'");
    await db.query("SET LOCAL idle_in_transaction_session_timeout='15000ms'");
    await db.query("SET LOCAL timezone='UTC'");
    // This lock serializes adapter writers. PostgreSQL takes the serializable
    // snapshot before the lock is acquired, so a concurrent loser can get 40001.
    // It must reconcile/read or obtain a fresh preview, never auto-retry a write.
    await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [BUSINESS_KEY]);
    await assertReady(db);
    const observedAt = (await db.query('SELECT transaction_timestamp()::text AS observed_at')).rows[0].observed_at;
    const age = Date.parse(observedAt) - Date.parse(previewObservedAt);
    if (age < 0 || age > MAX_PREVIEW_AGE_MS) throw failure('AO_HELP_REVIEW_PREVIEW_EXPIRED');
    const snapshot = await readSnapshot(db, { lock: true });
    if (sourceVersion(snapshot) !== expectedSourceVersion) throw failure('AO_HELP_REVIEW_SOURCE_CHANGED');
    eligibility(snapshot);
    const existing = await readChain(db);
    if (existing) {
      if (existing.ingestion.raw_source.source_version !== expectedSourceVersion) throw failure('AO_HELP_REVIEW_SOURCE_CHANGED');
      await db.query('ROLLBACK');
      return resultFor(existing, ['RESOLVED', 'SUPERSEDED'].includes(existing.attention.status) ? 'already_closed' : 'already_recorded');
    }
    const records = buildRecords(snapshot, expectedSourceVersion, observedAt);
    await new PostgresStateStore(db, { clientId: SCOPE.clientId }).persistIngestion(records.ingestion);
    await new PostgresDecisionStore(db, { clientId: SCOPE.clientId }).persistDecision(records.decision);
    await new PostgresAttentionStore(db, { clientId: SCOPE.clientId }).createAttention(records.attention);
    if (!await readChain(db)) throw failure('AO_HELP_REVIEW_READBACK_MISSING'); // Never commit an incomplete receipt.
    committing = true;
    await db.query('COMMIT');
    committed = true;
  } catch (error) {
    try { await db.query('ROLLBACK'); } catch { /* Disconnect also rolls back uncommitted work. */ }
    if (committing) throw failure('AO_HELP_REVIEW_COMMIT_UNCERTAIN', 'Commit outcome is uncertain. Use read-only reconcile; do not retry apply.');
    throw error;
  } finally { db.release(); }
  if (committed) {
    // Fresh lease/transaction after COMMIT, not a retained in-memory object or snapshot.
    try {
      const result = await reconcileAoHelpReview({ pool, ...scope });
      if (result.status !== 'recorded') throw failure('AO_HELP_REVIEW_READBACK_MISSING');
      return { ...result, status: 'applied' };
    } catch { throw failure('AO_HELP_REVIEW_READBACK_REQUIRED', 'Commit acknowledged but readback failed. Use read-only reconcile; do not retry apply.'); }
  }
}

module.exports = { SCOPE, ADAPTER_VERSION, BUSINESS_KEY, IDS, MAX_PREVIEW_AGE_MS, previewAoHelpReview, applyAoHelpReview, reconcileAoHelpReview };
