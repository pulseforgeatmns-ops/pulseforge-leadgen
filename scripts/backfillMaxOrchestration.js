require('dotenv').config();

const crypto = require('crypto');
const pool = require('../db');
const { loadMaxOrchestrationConfig, withProspectTier } = require('../config/maxOrchestration');
const { calculateWarmthScore } = require('../utils/maxWarmthScoring');
const { loadProspectContext, mapLegacyStatus } = require('../utils/maxOrchestration');
const { assertAllowed, boundedInteger, optionalPositiveInteger, optionalUuid, tokenizeArgs } = require('../utils/maxCli');

const MIGRATION_VERSION = 'max-orchestration-v1-backfill';

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, { values: ['--client-id','--after-id','--limit'], flags: ['--apply'] });
  return {
    apply: parsed.flags.has('--apply'),
    clientId: optionalPositiveInteger(parsed.values.get('--client-id'), '--client-id'),
    afterId: optionalUuid(parsed.values.get('--after-id'), '--after-id'),
    limit: boundedInteger(parsed.values.get('--limit'), '--limit', { defaultValue: 500, max: 5000 }),
  };
}

function migrationDecisionId(prospectId) {
  return crypto.createHash('sha256').update(`${MIGRATION_VERSION}:${prospectId}`).digest('hex');
}

async function fetchBatch(db, { clientId, afterId, limit }) {
  const params = [clientId, afterId, limit];
  return db.query(`
    SELECT p.id, p.client_id, p.status, p.do_not_contact, p.lifecycle_state,
           c.max_orchestration_config, c.vertical_tiers
    FROM prospects p
    JOIN clients c ON c.id = p.client_id
    WHERE ($1::int IS NULL OR p.client_id = $1)
      AND ($2::uuid IS NULL OR p.id > $2::uuid)
    ORDER BY p.id
    LIMIT $3
  `, params);
}

async function applyBackfill(db, { prospect, mappedState, scoreResult, now }) {
  const decisionId = migrationDecisionId(prospect.id);
  await db.query('BEGIN');
  try {
    await db.query('SELECT pg_advisory_xact_lock(hashtext($1))', [String(prospect.id)]);
    await db.query(`
      INSERT INTO max_decisions (
        id, client_id, prospect_id, company_id, trigger_event_type, idempotency_key,
        decision_version, score_version, current_state, recommended_state, warmth_score,
        score_components, reason_codes, reason_summary, next_best_action, actions,
        operator_required, operator_priority, is_shadow, config_snapshot, created_at
      ) VALUES ($1,$2,$3,$4,'migration',$5,$6,$7,$8,$8,$9,$10::jsonb,'["MIGRATION_BACKFILL"]'::jsonb,
        $11,NULL,'[]'::jsonb,FALSE,'low',TRUE,'{}'::jsonb,$12)
      ON CONFLICT (client_id, idempotency_key) DO NOTHING
    `, [
      decisionId, prospect.client_id, prospect.id, prospect.company_id || null,
      `${MIGRATION_VERSION}:${prospect.id}`, MIGRATION_VERSION, scoreResult.score_version,
      mappedState, scoreResult.score, JSON.stringify(scoreResult.components),
      `Initialized lifecycle state ${mappedState} from legacy status ${prospect.status || 'empty'}.`, now,
    ]);
    await db.query(`
      INSERT INTO prospect_state_transitions (
        client_id, prospect_id, decision_id, from_state, to_state, warmth_score,
        reason_codes, reason_summary, trigger_event_type, decision_source,
        operator_required, is_shadow, applied, created_at
      ) VALUES ($1,$2,$3,$4,$4,$5,'["MIGRATION_BACKFILL"]'::jsonb,$6,'migration','migration',FALSE,FALSE,TRUE,$7)
      ON CONFLICT (decision_id) DO NOTHING
    `, [
      prospect.client_id, prospect.id, decisionId, mappedState, scoreResult.score,
      `Initialized lifecycle state ${mappedState} from legacy status ${prospect.status || 'empty'}.`, now,
    ]);
    await db.query(`
      UPDATE prospects
      SET lifecycle_state = COALESCE(lifecycle_state, $1),
          state_changed_at = COALESCE(state_changed_at, $2),
          warmth_score = $3,
          warmth_score_updated_at = $2,
          warmth_score_version = $4,
          state_reason_codes = CASE WHEN lifecycle_state IS NULL THEN '["MIGRATION_BACKFILL"]'::jsonb ELSE state_reason_codes END,
          state_reason_summary = CASE WHEN lifecycle_state IS NULL THEN $5 ELSE state_reason_summary END,
          last_meaningful_signal_at = COALESCE($6::timestamptz, last_meaningful_signal_at),
          last_human_open_at = COALESCE($7::timestamptz, last_human_open_at),
          last_reply_at = COALESCE($8::timestamptz, last_reply_at),
          last_positive_reply_at = COALESCE($9::timestamptz, last_positive_reply_at)
      WHERE id = $10 AND client_id = $11
    `, [
      mappedState, now, scoreResult.score, scoreResult.score_version,
      `Initialized from legacy status ${prospect.status || 'empty'}.`, scoreResult.last_meaningful_signal_at,
      scoreResult.last_human_open_at, scoreResult.last_reply_at, scoreResult.last_positive_reply_at,
      prospect.id, prospect.client_id,
    ]);
    await db.query('COMMIT');
  } catch (error) {
    await db.query('ROLLBACK').catch(() => {});
    throw error;
  }
}

async function run(options = parseArgs(), db = pool) {
  const report = {
    mode: options.apply ? 'apply' : 'dry-run',
    migration_version: MIGRATION_VERSION,
    scanned: 0,
    would_initialize: 0,
    initialized: 0,
    rescored: 0,
    by_state: {},
    errors: [],
    last_prospect_id: options.afterId || null,
    side_effects: { emails: 0, sequence_changes: 0, operator_tasks: 0, enrichment_retries: 0 },
  };
  const batch = await fetchBatch(db, options);
  for (const row of batch.rows) {
    report.scanned++;
    report.last_prospect_id = row.id;
    try {
      const context = await loadProspectContext(db, row.id, row.client_id, { includeLegacySignals: true });
      const mappedState = row.lifecycle_state || mapLegacyStatus(row.status, row.do_not_contact);
      const config = loadMaxOrchestrationConfig({ env: {}, clientOverrides: row.max_orchestration_config });
      const prospect = withProspectTier({ ...context.prospect, lifecycle_state: mappedState }, {
        vertical_tiers: row.vertical_tiers || {},
      });
      const now = new Date();
      const scoreResult = calculateWarmthScore({ prospect, signals: context.signals, config, now });
      report.by_state[mappedState] = (report.by_state[mappedState] || 0) + 1;
      if (!row.lifecycle_state) report.would_initialize++;
      if (options.apply) {
        await applyBackfill(db, { prospect, mappedState, scoreResult, now });
        if (!row.lifecycle_state) report.initialized++;
        report.rescored++;
      }
    } catch (error) {
      report.errors.push({ prospect_id: row.id, error: error.message });
    }
  }
  return report;
}

module.exports = { MIGRATION_VERSION, applyBackfill, fetchBatch, migrationDecisionId, parseArgs, run };

if (require.main === module) {
  run().then(report => {
    console.log(JSON.stringify(report, null, 2));
    process.exitCode = report.errors.length ? 1 : 0;
  }).catch(error => {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  }).finally(() => pool.end());
}
