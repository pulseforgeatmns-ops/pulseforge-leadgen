const pool = require('./db');
const { withResearchProviderScope, isResearchProviderControlError } = require('./utils/anchorResearchProviderPause');
const { ensureScoutUnenrichedTable } = require('./utils/scoutUnenrichedSchema');
const { ensureEmailVerificationColumns } = require('./utils/emailVerificationSchema');
const { promoteRecord } = require('./scripts/promoteUnenriched');
const { ENRICHABLE_SCOUT_VERTICALS } = require('./utils/replenishmentVertical');

const { buildEnrichmentEligibilitySql, DAILY_RETRY_AFTER_ATTEMPT, LONG_HOLD_AFTER_ATTEMPT } = require('./utils/scoutEnrichmentEligibility');

const AGENT_NAME = 'scout_unenriched_enrichment';
const ANCHOR_CLIENT_ID = 10;
const DEFAULT_LIMIT = 5;
const DEFAULT_RETRY_HOURS = 7 * 24;
const DEFAULT_MAX_ATTEMPTS = LONG_HOLD_AFTER_ATTEMPT;
const DAILY_RETRY_HOURS = 24;
const LONG_HOLD_RETRY_HOURS = 7 * 24;
const SCHEDULED_HOUR_ET = 17;
const ANCHOR_PRIORITY_VERTICALS = ENRICHABLE_SCOUT_VERTICALS;

const ENRICHMENT_ELIGIBILITY_SQL = buildEnrichmentEligibilitySql({
  unrestrictedVerticals: true,
  maxAttempts: '$2', retryInterval: "($3::numeric * INTERVAL '1 hour')",
  dailyInterval: "($6::numeric * INTERVAL '1 hour')",
  longInterval: "($7::numeric * INTERVAL '1 hour')",
});

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
  const tenantId = params.client_id ?? params.clientId ?? ANCHOR_CLIENT_ID;
  return withResearchProviderScope(tenantId, () => params.client_id != null && params.clientId != null
    ? withResearchProviderScope(params.clientId, () => runScoped(params))
    : runScoped(params));
}

async function runScoped(params = {}) {
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
      AND (${ENRICHMENT_ELIGIBILITY_SQL}) = 'eligible_now'
    ORDER BY last_attempt_at ASC, id ASC
    LIMIT $4
  `, [clientId, maxAttempts, retryHours, limit, verticals,
    DAILY_RETRY_HOURS, LONG_HOLD_RETRY_HOURS]);

  const summary = {
    client_id: clientId,
    considered: rows.rows.length,
    promoted: 0,
    recovered: 0,
    unresolved: 0,
    failed: 0,
    emailResolved: 0,
    emailVerified: 0,
    outcomes: [],
    limit,
    retry_hours: retryHours,
    daily_retry_after_attempt: DAILY_RETRY_AFTER_ATTEMPT,
    daily_retry_hours: DAILY_RETRY_HOURS,
    long_hold_after_attempt: maxAttempts,
    long_hold_retry_hours: LONG_HOLD_RETRY_HOURS,
    verticals,
  };
  const promotionIds = [];
  require('./services/scoutPromotionCohort').retainPromotionIds(summary, promotionIds);
  for (const record of rows.rows) {
    let controlRejected = false;
    try {
      const result = await withResearchProviderScope(record.client_id, () => promote(record, { db }));
      summary.outcomes.push({ company: record.company, domain: record.domain,
        emailResolved: result?.emailResolved === true, emailVerified: result?.emailVerified === true,
        promoted: result?.promoted === true, recovered: result?.recovered === true, reason: result?.reason || null });
      const promoted = result === true || result?.promoted === true;
      const recovered = result?.recovered === true;
      if (result?.emailResolved) summary.emailResolved++;
      if (result?.emailVerified) summary.emailVerified++;
      if (promoted || recovered) promotionIds.push(result?.prospectId ?? null);
      if (promoted) summary.promoted++;
      else if (recovered) summary.recovered++;
      else summary.unresolved++;
    } catch (error) {
      controlRejected = isResearchProviderControlError(error);
      if (controlRejected) throw error;
      summary.failed++;
      await db.query(`
        UPDATE scout_unenriched
        SET enrichment_attempts = enrichment_attempts + 1,
            last_attempt_at = NOW(),
            notes = COALESCE(notes, '') || ' | automatic retry failed'
        WHERE id = $1 AND client_id = $2
      `, [record.id, clientId]);
      console.error(`[${AGENT_NAME}] ${record.domain || record.company || record.id}: ${error.message}`);
    } finally {
      // Verification and binding rejections also consume an attempt. Otherwise
      // the next batch immediately selects the same permanently invalid row.
      // A pause is not a failed candidate or a consumed enrichment attempt.
      if (!controlRejected) {
        await db.query(`UPDATE scout_unenriched SET enrichment_attempts = GREATEST(COALESCE(enrichment_attempts, 0), $3),
          last_attempt_at = NOW() WHERE id = $1 AND client_id = $2`,
        [record.id, clientId, Number(record.enrichment_attempts || 0) + 1]);
      }
    }
  }

  await logRun(clientId, summary, summary.failed ? 'partial' : 'success', null, db);
  console.log(`[${AGENT_NAME}] ${JSON.stringify(summary)}`);
  return summary;
}

function startAnchorUnenrichedEnrichmentScheduler() {
  let lastRunDate = null;
  let running = false;
  const tick = () => {
    const local = easternParts();
    if (!scheduledWindowOpen() || running || lastRunDate === local.date) return;
    running = true;
    lastRunDate = local.date;
    return run({ client_id: ANCHOR_CLIENT_ID }).catch(error => {
      // A control rejection may retry after unpausing in the same daily window.
      // Ordinary failures retain the existing once-per-day scheduling behavior.
      if (isResearchProviderControlError(error)) lastRunDate = null;
      console.error(`[${AGENT_NAME}] scheduled run failed: ${error.message}`);
    }).finally(() => { running = false; });
  };
  setTimeout(tick, 30_000).unref();
  setInterval(tick, 60 * 60 * 1000).unref();
}

module.exports = {
  run,
  startAnchorUnenrichedEnrichmentScheduler,
  _test: { ENRICHMENT_ELIGIBILITY_SQL, easternParts, scheduledWindowOpen, ANCHOR_PRIORITY_VERTICALS,
    DEFAULT_MAX_ATTEMPTS, DAILY_RETRY_AFTER_ATTEMPT, DAILY_RETRY_HOURS, LONG_HOLD_RETRY_HOURS },
};

if (require.main === module) {
  run({ client_id: process.env.ACTIVE_CLIENT_ID || ANCHOR_CLIENT_ID }).catch(error => {
    console.error(`[${AGENT_NAME}] Fatal: ${error.stack || error.message}`);
    process.exit(1);
  });
}
