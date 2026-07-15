require('dotenv').config();

const pool = require('./db');
const { calculateProspectShadow, evaluateProspectShadow } = require('./utils/maxOrchestration');
const { loadClientOrchestrationConfig } = require('./utils/maxSignalIngestion');
const { logMaxOrchestrationFailure, recordMaxMetric } = require('./utils/maxOrchestrationObservability');
const { assertAllowed, boundedInteger, optionalPositiveInteger, optionalUuid, tokenizeArgs } = require('./utils/maxCli');

function boundedLimit(value, fallback = 250) {
  const parsed = Number(value || fallback);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 2000) throw new Error('limit must be an integer from 1 to 2000');
  return parsed;
}

function booleanValue(value, fallback = false) {
  if (value === undefined || value === null || value === '') return fallback;
  if (typeof value === 'boolean') return value;
  return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
}

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, { values: ['--client-id','--after-id','--limit'], flags: ['--dry-run','--apply'] });
  if (parsed.flags.has('--dry-run') && parsed.flags.has('--apply')) throw new Error('--dry-run and --apply are mutually exclusive');
  return {
    client_id: optionalPositiveInteger(parsed.values.get('--client-id'), '--client-id'),
    after_id: optionalUuid(parsed.values.get('--after-id'), '--after-id'),
    limit: boundedInteger(parsed.values.get('--limit'), '--limit', { defaultValue: 250, max: 2000 }),
    dry_run: !parsed.flags.has('--apply'),
  };
}

async function findDecayCandidates(db, { clientId = null, afterId = null, limit = 250 } = {}) {
  return db.query(`
    SELECT p.id, p.client_id
    FROM prospects p
    WHERE ($1::int IS NULL OR p.client_id = $1)
      AND ($2::uuid IS NULL OR p.id > $2::uuid)
      AND COALESCE(p.do_not_contact, false) = false
      AND (
        p.downgrade_candidate_since IS NOT NULL
        OR (
          p.warmth_score_updated_at < NOW() - INTERVAL '23 hours'
          AND p.last_meaningful_signal_at >= NOW() - INTERVAL '16 days'
        )
        OR EXISTS (
          SELECT 1 FROM prospect_signal_events event
          WHERE event.client_id = p.client_id
            AND event.prospect_id = p.id
            AND event.event_type IN (
              'email_human_opened','email_clicked','company_signal_detected','icp_score_changed',
              'enrichment_failed','email_soft_bounced'
            )
            AND event.event_timestamp >= NOW() - INTERVAL '16 days'
            AND event.event_timestamp <= NOW() - INTERVAL '6 days'
        )
      )
    ORDER BY p.id
    LIMIT $3
  `, [clientId, afterId, limit]);
}

async function run(params = {}, db = pool, dependencies = {}) {
  const started = Date.now();
  const options = {
    clientId: params.client_id ?? params.clientId ?? null,
    afterId: params.after_id ?? params.afterId ?? null,
    limit: boundedLimit(params.limit),
    dryRun: booleanValue(params.dry_run ?? params.dryRun, true),
  };
  const report = {
    mode: options.dryRun ? 'dry-run' : 'shadow-write',
    client_id: options.clientId == null ? null : Number(options.clientId),
    scanned: 0,
    evaluated: 0,
    decisions_created: 0,
    duplicates: 0,
    downgrade_candidates: 0,
    downgrade_recommendations: 0,
    skipped: 0,
    errors: [],
    last_prospect_id: options.afterId,
    side_effects: { status_updates: 0, messages: 0, sequence_changes: 0, enrichment_retries: 0, tasks: 0 },
  };
  const candidates = await findDecayCandidates(db, options);
  const calculateFn = dependencies.calculateProspectShadow || calculateProspectShadow;
  const evaluateFn = dependencies.evaluateProspectShadow || evaluateProspectShadow;
  const loadConfigFn = dependencies.loadClientOrchestrationConfig || loadClientOrchestrationConfig;
  const clientConfigs = new Map();
  for (const row of candidates.rows) {
    report.scanned++;
    report.last_prospect_id = row.id;
    try {
      if (!clientConfigs.has(row.client_id)) {
        clientConfigs.set(row.client_id, await loadConfigFn(db, row.client_id));
      }
      const args = {
        db,
        prospectId: row.id,
        clientId: row.client_id,
        clientConfig: clientConfigs.get(row.client_id),
        env: process.env,
        now: new Date(),
        ignoreFeatureFlags: options.dryRun,
      };
      const result = options.dryRun
        ? await calculateFn(args)
        : await evaluateFn(args);
      if (result.skipped) {
        report.skipped++;
        continue;
      }
      report.evaluated++;
      const decision = options.dryRun ? result.decision : result.decision;
      if (!options.dryRun) {
        if (result.duplicate) report.duplicates++;
        else report.decisions_created++;
        await recordMaxMetric('max_decay_evaluations_total', {
          db, clientId: row.client_id, prospectId: row.id, decisionId: decision?.id || null,
        }).catch(() => {});
      }
      if (decision?.reason_codes?.includes('DOWNGRADE_STABILIZING')) report.downgrade_candidates++;
      if (decision?.reason_codes?.includes('DOWNGRADE_STABILIZED')) report.downgrade_recommendations++;
    } catch (error) {
      report.errors.push({ prospect_id: row.id, error: error.message });
      await logMaxOrchestrationFailure({
        db, clientId: row.client_id, prospectId: row.id, action: 'decay_evaluation_failed', error,
      }).catch(() => {});
    }
  }
  report.duration_ms = Date.now() - started;
  if (!options.dryRun) {
    await recordMaxMetric('decay_batch_duration', {
      db, clientId: options.clientId, value: report.duration_ms,
      dimensions: { scanned: report.scanned, evaluated: report.evaluated, errors: report.errors.length },
    }).catch(() => {});
    await db.query(`
      INSERT INTO agent_log (agent_name, action, payload, status, error_msg, ran_at, client_id)
      VALUES ('max_orchestration','decay_summary',$1::jsonb,$2,$3,NOW(),$4)
    `, [
      JSON.stringify(report), report.errors.length ? 'failed' : 'success',
      report.errors.length ? `${report.errors.length} decay evaluation(s) failed` : null,
      options.clientId,
    ]).catch(() => {});
  }
  return report;
}

module.exports = { booleanValue, boundedLimit, findDecayCandidates, parseArgs, run };

if (require.main === module) {
  run(parseArgs()).then(result => console.log(JSON.stringify(result, null, 2)))
    .catch(error => { console.error(error.stack || error.message); process.exitCode = 1; })
    .finally(() => pool.end());
}
