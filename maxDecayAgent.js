require('dotenv').config();

const crypto = require('crypto');
const pool = require('./db');
const { calculateProspectShadow, evaluateProspectShadow } = require('./utils/maxOrchestration');
const { loadClientOrchestrationConfig } = require('./utils/maxSignalIngestion');
const { logMaxOrchestrationFailure, recordMaxMetric } = require('./utils/maxOrchestrationObservability');
const { assertAllowed, boundedInteger, optionalPositiveInteger, optionalUuid, tokenizeArgs } = require('./utils/maxCli');

const DECAY_LOCK_NAMESPACE = 194836421;
const DECAY_LOCK_KEY = 1;

function sanitizedError(error) {
  return String(error?.message || error || 'Unknown decay failure').replace(/[\r\n]+/g, ' ').slice(0, 1000);
}

async function recordDecayRunEvent(db, event) {
  await db.query(`
    INSERT INTO max_decay_run_events (
      run_id,mode,status,started_at,completed_at,lock_acquired,client_scope,batch_limit,
      start_cursor,end_cursor,candidates_found,prospects_evaluated,scores_changed,
      downgrade_candidates,recommendations_created,decisions_created,errors,error_stage,
      error_code,error_summary,retryable,operational_effects,deployment_commit,details
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22::jsonb,$23,$24::jsonb)
  `, [
    event.run_id, event.mode, event.status, event.started_at, event.completed_at || null,
    event.lock_acquired === true, event.client_scope ?? null, event.batch_limit,
    event.start_cursor || null, event.end_cursor || null, Number(event.candidates_found || 0),
    Number(event.prospects_evaluated || 0), Number(event.scores_changed || 0),
    Number(event.downgrade_candidates || 0), Number(event.recommendations_created || 0),
    Number(event.decisions_created || 0), Number(event.errors || 0), event.error_stage || null,
    event.error_code || null, event.error_summary || null, event.retryable ?? null,
    JSON.stringify(event.operational_effects || {}), event.deployment_commit || process.env.RAILWAY_GIT_COMMIT_SHA || null,
    JSON.stringify(event.details || {}),
  ]);
}

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
  assertAllowed(parsed, { values: ['--client-id','--after-id','--limit'], flags: ['--dry-run','--apply','--resume'] });
  if (parsed.flags.has('--dry-run') && parsed.flags.has('--apply')) throw new Error('--dry-run and --apply are mutually exclusive');
  return {
    client_id: optionalPositiveInteger(parsed.values.get('--client-id'), '--client-id'),
    after_id: optionalUuid(parsed.values.get('--after-id'), '--after-id'),
    limit: boundedInteger(parsed.values.get('--limit'), '--limit', { defaultValue: 250, max: 2000 }),
    dry_run: !parsed.flags.has('--apply'),
    resume: parsed.flags.has('--resume'),
  };
}

async function latestResumeCursor(db, { clientId = null, mode = 'shadow-write' } = {}) {
  const result = await db.query(`
    SELECT end_cursor
    FROM max_decay_run_events
    WHERE status='completed' AND mode=$2
      AND client_scope IS NOT DISTINCT FROM $1::int
    ORDER BY recorded_at DESC LIMIT 1
  `, [clientId, mode]);
  return result.rows[0]?.end_cursor || null;
}

async function findDecayCandidates(db, { clientId = null, afterId = null, limit = 250 } = {}) {
  return db.query(`
    SELECT p.id, p.client_id, p.warmth_score
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
  const startedAt = new Date();
  const runId = dependencies.runId || crypto.randomUUID();
  const options = {
    clientId: params.client_id ?? params.clientId ?? null,
    afterId: params.after_id ?? params.afterId ?? null,
    limit: boundedLimit(params.limit),
    dryRun: booleanValue(params.dry_run ?? params.dryRun, true),
    resume: booleanValue(params.resume, false),
  };
  const report = {
    run_id: runId,
    mode: options.dryRun ? 'dry-run' : 'shadow-write',
    status: 'running',
    lock_acquired: false,
    client_id: options.clientId == null ? null : Number(options.clientId),
    scanned: 0,
    evaluated: 0,
    decisions_created: 0,
    duplicates: 0,
    downgrade_candidates: 0,
    downgrade_recommendations: 0,
    recommendations_created: 0,
    transitions_recommended: 0,
    actions_skipped: 0,
    scores_changed: 0,
    skipped: 0,
    errors: [],
    last_prospect_id: options.afterId,
    start_cursor: options.afterId,
    cursor_wrapped: false,
    side_effects: { status_updates: 0, messages: 0, sequence_changes: 0, enrichment_retries: 0, tasks: 0 },
  };
  const runEventFn = dependencies.recordDecayRunEvent || recordDecayRunEvent;
  const lockClient = dependencies.lockClient || (typeof db.connect === 'function' ? await db.connect() : db);
  const ownsLockClient = !dependencies.lockClient && lockClient !== db;
  let lockHeld = false;
  const eventBase = () => ({
    run_id: runId, mode: report.mode, started_at: startedAt, lock_acquired: report.lock_acquired,
    client_scope: options.clientId, batch_limit: options.limit, start_cursor: options.afterId,
    end_cursor: report.last_prospect_id, candidates_found: report.scanned,
    prospects_evaluated: report.evaluated, scores_changed: report.scores_changed,
    downgrade_candidates: report.downgrade_candidates,
    recommendations_created: report.recommendations_created,
    decisions_created: report.decisions_created, errors: report.errors.length,
    operational_effects: report.side_effects,
  });
  const releaseLock = async () => {
    if (!lockHeld) return true;
    const unlocked = await lockClient.query(
      'SELECT pg_advisory_unlock($1,$2) AS unlocked',
      [DECAY_LOCK_NAMESPACE, DECAY_LOCK_KEY]
    );
    lockHeld = false;
    return unlocked.rows[0]?.unlocked === true;
  };
  const calculateFn = dependencies.calculateProspectShadow || calculateProspectShadow;
  const evaluateFn = dependencies.evaluateProspectShadow || evaluateProspectShadow;
  const loadConfigFn = dependencies.loadClientOrchestrationConfig || loadClientOrchestrationConfig;
  const clientConfigs = new Map();
  try {
    const lock = await lockClient.query(
      'SELECT pg_try_advisory_lock($1,$2) AS locked',
      [DECAY_LOCK_NAMESPACE, DECAY_LOCK_KEY]
    );
    lockHeld = lock.rows[0]?.locked === true;
    report.lock_acquired = lockHeld;
    if (!lockHeld) {
      report.status = 'skipped_overlap';
      report.duration_ms = Date.now() - started;
      await runEventFn(db, { ...eventBase(), status: report.status, completed_at: new Date(), details: { reason: 'active_decay_run' } });
      return report;
    }
    if (options.resume && !options.afterId) {
      options.afterId = await latestResumeCursor(db, { clientId: options.clientId, mode: report.mode });
      report.start_cursor = options.afterId;
      report.last_prospect_id = options.afterId;
    }
    await runEventFn(db, { ...eventBase(), status: 'running', details: { lock_namespace: DECAY_LOCK_NAMESPACE, lock_key: DECAY_LOCK_KEY } });
    let candidates = await findDecayCandidates(db, options);
    if (options.resume && options.afterId && candidates.rows.length === 0) {
      report.cursor_wrapped = true;
      candidates = await findDecayCandidates(db, { ...options, afterId: null });
    }
    report.scanned = candidates.rows.length;
    for (const row of candidates.rows) {
      report.last_prospect_id = row.id;
      const prospectStarted = Date.now();
      try {
        if (!clientConfigs.has(row.client_id)) {
          clientConfigs.set(row.client_id, await loadConfigFn(db, row.client_id));
        }
        const args = {
          db, prospectId: row.id, clientId: row.client_id,
          clientConfig: clientConfigs.get(row.client_id), env: process.env,
          now: new Date(), ignoreFeatureFlags: options.dryRun,
        };
        const result = options.dryRun ? await calculateFn(args) : await evaluateFn(args);
        if (result.skipped) { report.skipped++; continue; }
        report.evaluated++;
        const decision = result.decision;
        const score = options.dryRun ? result.scoreResult?.score : result.score?.score;
        if (Number(score) !== Number(row.warmth_score || 0)) report.scores_changed++;
        if (!options.dryRun) {
          if (result.duplicate) report.duplicates++;
          else report.decisions_created++;
          if (!result.duplicate && decision?.transition_recommended) {
            report.transitions_recommended++;
            report.recommendations_created++;
          }
          if (!result.duplicate && Array.isArray(decision?.actions)) {
            report.actions_skipped += decision.actions.length;
            report.recommendations_created += decision.actions.length;
          }
          await Promise.allSettled([
            recordMaxMetric('max_decay_evaluations_total', {
              db, clientId: row.client_id, prospectId: row.id, decisionId: decision?.id || null,
              dimensions: { provenance: 'daily_decay', run_id: runId },
            }),
            recordMaxMetric('decay_processing_latency', {
              db, clientId: row.client_id, prospectId: row.id, decisionId: decision?.id || null,
              value: Date.now() - prospectStarted,
              dimensions: { provenance: 'daily_decay', run_id: runId },
            }),
          ]);
        }
        if (decision?.reason_codes?.includes('DOWNGRADE_STABILIZING')) report.downgrade_candidates++;
        if (decision?.reason_codes?.includes('DOWNGRADE_STABILIZED')) report.downgrade_recommendations++;
      } catch (error) {
        report.errors.push({ prospect_id: row.id, code: error.code || null, error: sanitizedError(error) });
        await logMaxOrchestrationFailure({
          db, clientId: row.client_id, prospectId: row.id, action: 'decay_evaluation_failed', error,
          payload: { run_id: runId, failure_stage: 'prospect_evaluation', cursor: row.id, retryable: false },
        }).catch(() => {});
      }
    }
    report.duration_ms = Date.now() - started;
    report.status = report.errors.length ? 'failed' : 'completed';
    if (!options.dryRun) {
      await recordMaxMetric('decay_batch_duration', {
        db, clientId: options.clientId, value: report.duration_ms,
        dimensions: { provenance: 'daily_decay', run_id: runId, scanned: report.scanned, evaluated: report.evaluated, errors: report.errors.length },
      }).catch(() => {});
    }
    const released = await releaseLock();
    await runEventFn(db, {
      ...eventBase(), status: report.status, completed_at: new Date(),
      error_stage: report.errors.length ? 'prospect_evaluation' : null,
      error_code: report.errors[0]?.code || null,
      error_summary: report.errors.length ? `${report.errors.length} decay evaluation(s) failed` : null,
      retryable: report.errors.length ? false : null,
      details: {
        lock_released: released, cursor_wrapped: report.cursor_wrapped,
        transitions_recommended: report.transitions_recommended, actions_skipped: report.actions_skipped,
      },
    });
    return report;
  } catch (error) {
    report.status = 'failed';
    report.duration_ms = Date.now() - started;
    const released = await releaseLock().catch(() => false);
    await runEventFn(db, {
      ...eventBase(), status: 'failed', completed_at: new Date(), errors: Math.max(1, report.errors.length),
      error_stage: 'run', error_code: error.code || null, error_summary: sanitizedError(error), retryable: false,
      details: { lock_released: released },
    }).catch(() => {});
    await logMaxOrchestrationFailure({
      db, clientId: options.clientId, action: 'decay_run_failed', error,
      payload: { run_id: runId, failure_stage: 'run', cursor: report.last_prospect_id, retryable: false },
    }).catch(() => {});
    throw error;
  } finally {
    if (lockHeld) await releaseLock().catch(() => {});
    if (ownsLockClient) lockClient.release();
  }
}

module.exports = {
  DECAY_LOCK_KEY, DECAY_LOCK_NAMESPACE, booleanValue, boundedLimit, findDecayCandidates,
  latestResumeCursor, parseArgs, recordDecayRunEvent, run, sanitizedError,
};

if (require.main === module) {
  run(parseArgs()).then(result => {
    console.log(JSON.stringify(result, null, 2));
    if (result.status === 'failed') process.exitCode = 1;
  })
    .catch(error => { console.error(error.stack || error.message); process.exitCode = 1; })
    .finally(() => pool.end());
}
