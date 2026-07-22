const pool = require('./db');
const { ensureScoutUnenrichedTable } = require('./utils/scoutUnenrichedSchema');
const { ensureEmailVerificationColumns } = require('./utils/emailVerificationSchema');
const { promoteRecord } = require('./scripts/promoteUnenriched');

const AGENT_NAME = 'scout_unenriched_enrichment';
const ANCHOR_CLIENT_ID = 10;
const DEFAULT_LIMIT = 5;
const DEFAULT_RETRY_HOURS = 7 * 24;
const DEFAULT_MAX_ATTEMPTS = 3;
const SCHEDULED_HOUR_ET = 17;
const ANCHOR_PRIORITY_VERTICALS = ['property_manager', 'str_manager', 'commercial_office'];

function easternParts(now = new Date()) {
  const values = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    weekday: 'short',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(now);
  const get = type => values.find(value => value.type === type)?.value;
  return {
    weekday: get('weekday'),
    date: `${get('year')}-${get('month')}-${get('day')}`,
    hour: Number(get('hour')),
  };
}

function scheduledWindowOpen(now = new Date()) {
  const local = easternParts(now);
  return ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].includes(local.weekday) && local.hour >= SCHEDULED_HOUR_ET;
}

async function logRun(clientId, payload, status = 'success', error = null, db = pool) {
  await db.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, error_msg, ran_at, client_id)
    VALUES ($1, 'retry_unenriched', $2::jsonb, $3, $4, NOW(), $5)
  `, [AGENT_NAME, JSON.stringify(payload), status, error, clientId]);
}

async function run(params = {}) {
  const clientId = Number(params.client_id || params.clientId || ANCHOR_CLIENT_ID);
  const limit = Math.max(1, Math.min(Number(params.limit || DEFAULT_LIMIT), DEFAULT_LIMIT));
  const retryHours = Math.max(1, Number(params.retryHours || DEFAULT_RETRY_HOURS));
  const maxAttempts = Math.max(1, Number(params.maxAttempts || DEFAULT_MAX_ATTEMPTS));
  const verticals = Array.isArray(params.verticals)
    ? params.verticals
    : clientId === ANCHOR_CLIENT_ID ? ANCHOR_PRIORITY_VERTICALS : null;
  const db = params.db || pool;
  const promote = params.promote || promoteRecord;

  await ensureScoutUnenrichedTable();
  await ensureEmailVerificationColumns();

  const rows = await db.query(`
    SELECT *
    FROM scout_unenriched
    WHERE client_id = $1
      AND COALESCE(enrichment_attempts, 0) < $2
      AND COALESCE(last_attempt_at, NOW() - INTERVAL '100 years') <= NOW() - ($3::numeric * INTERVAL '1 hour')
      AND ($5::text[] IS NULL OR vertical = ANY($5::text[]))
    ORDER BY last_attempt_at ASC, id ASC
    LIMIT $4
  `, [clientId, maxAttempts, retryHours, limit, verticals]);

  const summary = { client_id: clientId, considered: rows.rows.length, promoted: 0, unresolved: 0, failed: 0, limit, retry_hours: retryHours, verticals };
  for (const record of rows.rows) {
    try {
      const promoted = await promote(record, { db });
      if (promoted) summary.promoted++;
      else summary.unresolved++;
    } catch (error) {
      summary.failed++;
      await db.query(`
        UPDATE scout_unenriched
        SET enrichment_attempts = enrichment_attempts + 1,
            last_attempt_at = NOW(),
            notes = COALESCE(notes, '') || ' | automatic retry failed'
        WHERE id = $1 AND client_id = $2
      `, [record.id, clientId]);
      console.error(`[${AGENT_NAME}] ${record.domain || record.company || record.id}: ${error.message}`);
    }
  }

  await logRun(clientId, summary, summary.failed ? 'partial' : 'success', null, db);
  console.log(`[${AGENT_NAME}] ${JSON.stringify(summary)}`);
  return summary;
}

function startAnchorUnenrichedEnrichmentScheduler() {
  let lastRunDate = null;
  const tick = () => {
    const local = easternParts();
    if (!scheduledWindowOpen() || lastRunDate === local.date) return;
    lastRunDate = local.date;
    run({ client_id: ANCHOR_CLIENT_ID }).catch(error =>
      console.error(`[${AGENT_NAME}] scheduled run failed: ${error.message}`)
    );
  };
  setTimeout(tick, 30_000).unref();
  setInterval(tick, 60 * 60 * 1000).unref();
}

module.exports = {
  run,
  startAnchorUnenrichedEnrichmentScheduler,
  _test: { easternParts, scheduledWindowOpen, ANCHOR_PRIORITY_VERTICALS },
};

if (require.main === module) {
  run({ client_id: process.env.ACTIVE_CLIENT_ID || ANCHOR_CLIENT_ID }).catch(error => {
    console.error(`[${AGENT_NAME}] Fatal: ${error.stack || error.message}`);
    process.exit(1);
  });
}
