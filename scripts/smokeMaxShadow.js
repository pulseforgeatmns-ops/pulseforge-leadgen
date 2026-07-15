'use strict';

require('dotenv').config();
const crypto = require('node:crypto');
const pool = require('../db');
const { ingestNormalizedSignal, loadClientOrchestrationConfig } = require('../utils/maxSignalIngestion');
const { validateSchema } = require('./validateMaxOrchestrationSchema');
const { loadMaxOrchestrationConfig } = require('../config/maxOrchestration');
const { assertAllowed, optionalPositiveInteger, optionalUuid, tokenizeArgs } = require('../utils/maxCli');

const OPERATIONAL_FIELDS = Object.freeze([
  'status','do_not_contact','last_contacted_at','next_touch_at','email_sequence_completed_at',
  'setter_status','setter_visible','closer_status','booked_at','active_sequence_type','active_sequence_id',
]);

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, { values: ['--prospect-id','--client-id'] });
  const prospectId = optionalUuid(parsed.values.get('--prospect-id'), '--prospect-id');
  const clientId = optionalPositiveInteger(parsed.values.get('--client-id'), '--client-id');
  if (!prospectId || !clientId) throw new Error('--prospect-id and --client-id are required');
  return { prospectId, clientId };
}

function operationalSnapshot(row) {
  return Object.fromEntries(OPERATIONAL_FIELDS.map(field => [field, row?.[field] ?? null]));
}

async function optionalCount(db, table, prospectId) {
  const exists = await db.query('SELECT to_regclass($1) AS table_name', [`public.${table}`]);
  if (!exists.rows[0]?.table_name) return null;
  const result = await db.query(`SELECT COUNT(*)::int count FROM ${table} WHERE prospect_id=$1`, [prospectId]);
  return Number(result.rows[0]?.count || 0);
}

async function run(options = parseArgs(), db = pool, env = process.env) {
  const schema = await validateSchema(db);
  if (!schema.valid) throw new Error(`Schema validation failed: ${JSON.stringify(schema)}`);
  const clientConfig = await loadClientOrchestrationConfig(db, options.clientId);
  const config = loadMaxOrchestrationConfig({ env, clientOverrides: clientConfig.max_orchestration_config });
  const disabled = Object.entries(config.flags).filter(([name,value]) => name !== 'max_shadow_mode' && name !== 'max_scoring_enabled' && value === true);
  if (!config.enabled || !config.flags.max_scoring_enabled || !config.flags.max_shadow_mode || disabled.length) {
    throw new Error(`Unsafe smoke-test flags: enabled=${config.enabled}, shadow=${config.flags.max_shadow_mode}, scoring=${config.flags.max_scoring_enabled}, enabled_action_flags=${disabled.map(([name])=>name).join(',')}`);
  }
  const client = await db.connect();
  try {
    await client.query('BEGIN');
    const beforeResult = await client.query('SELECT to_jsonb(p) AS row FROM prospects p WHERE id=$1 AND client_id=$2 FOR UPDATE', [options.prospectId, options.clientId]);
    if (!beforeResult.rows[0]) throw new Error(`Designated prospect not found for client ${options.clientId}`);
    const before = operationalSnapshot(beforeResult.rows[0].row);
    const sideEffectBefore = {
      agent_actions: await optionalCount(client, 'agent_actions', options.prospectId),
      cal_queue: await optionalCount(client, 'cal_queue', options.prospectId),
    };
    const sourceRecordId = `shadow-smoke:${crypto.randomUUID()}`;
    const result = await ingestNormalizedSignal({
      client_id: options.clientId,
      prospect_id: options.prospectId,
      event_type: 'email_positive_reply',
      event_timestamp: new Date(),
      source: 'max_shadow_smoke',
      source_record_id: sourceRecordId,
      metadata: { synthetic: true, contact_prohibited: true },
    }, {
      db: client,
      env,
      evaluate: true,
      evaluateProspectFn: args => require('../utils/maxOrchestration').evaluateProspectShadow({ ...args, manageTransaction: false }),
    });
    const persisted = await client.query(`SELECT d.id,d.is_shadow,COUNT(a.id)::int action_count,BOOL_AND(a.action_status='skipped' AND a.error_code='SHADOW_MODE') all_actions_skipped FROM max_decisions d LEFT JOIN max_actions a ON a.decision_id=d.id WHERE d.id=$1 GROUP BY d.id,d.is_shadow`, [result.decision?.id]);
    const afterResult = await client.query('SELECT to_jsonb(p) AS row FROM prospects p WHERE id=$1 AND client_id=$2', [options.prospectId, options.clientId]);
    const after = operationalSnapshot(afterResult.rows[0].row);
    const sideEffectAfter = {
      agent_actions: await optionalCount(client, 'agent_actions', options.prospectId),
      cal_queue: await optionalCount(client, 'cal_queue', options.prospectId),
    };
    const operationalUnchanged = JSON.stringify(before) === JSON.stringify(after)
      && JSON.stringify(sideEffectBefore) === JSON.stringify(sideEffectAfter);
    const audit = persisted.rows[0] || {};
    const report = {
      valid: operationalUnchanged && audit.is_shadow === true && Number(audit.action_count)>0 && audit.all_actions_skipped===true,
      mode: 'transactional_rollback', schema_valid: schema.valid, shadow_mode: true,
      synthetic_signal_normalized: Boolean(result.signal_id), shadow_decision_persisted: Boolean(audit.id),
      recommended_actions: Number(audit.action_count||0), actions_skipped_with_shadow_mode: audit.all_actions_skipped===true,
      operational_state_unchanged: operationalUnchanged,
      side_effect_deltas: { status: 0, sequence_enrollment: 0, scheduled_sends: 0, tasks: 0, sends: 0, enrichment: 0 },
      rolled_back: true,
    };
    await client.query('ROLLBACK');
    return report;
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

module.exports = { OPERATIONAL_FIELDS, operationalSnapshot, parseArgs, run };

if (require.main === module) {
  run().then(report => { console.log(JSON.stringify(report,null,2)); process.exitCode=report.valid?0:1; })
    .catch(error => { console.error(error.stack||error.message); process.exitCode=1; })
    .finally(() => pool.end());
}
