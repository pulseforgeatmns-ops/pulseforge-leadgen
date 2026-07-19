const { spawn } = require('child_process');
const path = require('path');
const pool = require('../db');

const LOCK_NAMESPACE = 701102;
const DEFAULT_TIME_ZONE = 'America/New_York';
const DEFAULT_PER_SLICE = 3;
const DEFAULT_WINDOW_START = 9;
const DEFAULT_WINDOW_END = 16;
const DEFAULT_BOUNCE_THRESHOLD = 0.05;
const DEFAULT_BOUNCE_MIN_SENDS = 10;
let schemaPromise;

function envEnabled(value) {
  return ['1', 'true', 'yes', 'on'].includes(String(value || '').trim().toLowerCase());
}

function localDateParts(date = new Date(), timeZone = DEFAULT_TIME_ZONE) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    weekday: 'short',
    hour: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(date);
  const get = type => parts.find(part => part.type === type)?.value;
  return {
    date: `${get('year')}-${get('month')}-${get('day')}`,
    weekday: get('weekday'),
    hour: Number(get('hour')),
  };
}

function isBusinessWindow(date = new Date(), options = {}) {
  const local = localDateParts(date, options.timeZone || DEFAULT_TIME_ZONE);
  const startHour = Number(options.startHour ?? DEFAULT_WINDOW_START);
  const endHour = Number(options.endHour ?? DEFAULT_WINDOW_END);
  return ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].includes(local.weekday) &&
    local.hour >= startHour && local.hour < endHour;
}

function parseIsoDate(value) {
  const match = String(value || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) throw new TypeError(`Invalid ISO date: ${value}`);
  return new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
}

function businessDaysInclusive(startDate, endDate) {
  const start = parseIsoDate(startDate);
  const end = parseIsoDate(endDate);
  if (end < start) return 0;
  let total = 0;
  for (let day = new Date(start); day <= end; day.setUTCDate(day.getUTCDate() + 1)) {
    const weekday = day.getUTCDay();
    if (weekday !== 0 && weekday !== 6) total++;
  }
  return total;
}

function rampCap(stages, businessDay) {
  const normalized = (stages || [])
    .map(stage => ({
      business_day_start: Number(stage.business_day_start),
      daily_cap: Number(stage.daily_cap),
    }))
    .filter(stage => stage.business_day_start > 0 && stage.daily_cap > 0)
    .sort((a, b) => a.business_day_start - b.business_day_start);
  if (!normalized.length) throw new Error('Emmett warmup ramp has no configured stages');
  const day = Math.max(1, Number(businessDay || 1));
  return normalized.reduce(
    (cap, stage) => day >= stage.business_day_start ? stage.daily_cap : cap,
    normalized[0].daily_cap
  );
}

async function applyEmmettAutosendSchema(query) {
  await query(`
    ALTER TABLE clients
      ADD COLUMN IF NOT EXISTS warmup_start_date DATE,
      ADD COLUMN IF NOT EXISTS autosend_enabled BOOLEAN NOT NULL DEFAULT FALSE
  `);
  await query(`
    ALTER TABLE prospects
      ADD COLUMN IF NOT EXISTS next_touch_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS email_sequence_completed_at TIMESTAMPTZ
  `);
  await query(`
    CREATE TABLE IF NOT EXISTS emmett_warmup_config (
      business_day_start INTEGER PRIMARY KEY CHECK (business_day_start > 0),
      daily_cap INTEGER NOT NULL CHECK (daily_cap > 0),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await query(`
    INSERT INTO emmett_warmup_config (business_day_start, daily_cap)
    VALUES (1, 5), (6, 10), (11, 20), (16, 35), (21, 50)
    ON CONFLICT (business_day_start) DO NOTHING
  `);
  await query(`
    CREATE TABLE IF NOT EXISTS emmett_schema_migrations (
      name TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await query(`
    INSERT INTO emmett_schema_migrations (name)
    VALUES ('2026-07-02-enable-anchor-autosend')
    ON CONFLICT (name) DO NOTHING
  `);
}

function ensureEmmettAutosendSchema(query) {
  if (query) return applyEmmettAutosendSchema(query);
  if (!schemaPromise) {
    schemaPromise = applyEmmettAutosendSchema(pool.query.bind(pool)).catch(err => {
      schemaPromise = null;
      throw err;
    });
  }
  return schemaPromise;
}

async function runEmmettChild(context, options = {}) {
  const script = options.script || path.join(__dirname, '..', 'scripts', 'runEmmettAutorunSlice.js');
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [script], {
      cwd: path.join(__dirname, '..'),
      env: { ...process.env, ACTIVE_CLIENT_ID: String(context.client_id) },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    child.on('error', reject);
    child.on('close', code => {
      const marker = stdout.split('\n').find(line => line.startsWith('EMMETT_AUTORUN_RESULT='));
      if (code !== 0 || !marker) {
        return reject(new Error(`Emmett autorun child failed (${code}): ${stderr.trim() || stdout.trim() || 'no result'}`));
      }
      try {
        resolve(JSON.parse(marker.slice('EMMETT_AUTORUN_RESULT='.length)));
      } catch (err) {
        reject(new Error(`Invalid Emmett autorun child result: ${err.message}`));
      }
    });
    child.stdin.end(JSON.stringify(context));
  });
}

async function logAutorun(query, clientId, result, status = 'success', errorMsg = null) {
  await query(`
    INSERT INTO agent_log (agent_name, action, payload, status, error_msg, ran_at, client_id)
    VALUES ('emmett', 'autorun', $1, $2, $3, NOW(), $4)
  `, [JSON.stringify(result), status, errorMsg, clientId]);
}

async function autorun(clientId, options = {}) {
  const id = Number(clientId);
  if (!Number.isInteger(id) || id <= 0) throw Object.assign(new Error('client_id must be a positive integer'), { statusCode: 400 });
  const now = options.now instanceof Date ? options.now : new Date(options.now || Date.now());
  const dbPool = options.pool || pool;
  const query = options.query || dbPool.query.bind(dbPool);
  const runChild = options.runChild || runEmmettChild;
  const base = { client_id: id, sent: 0, skipped: 0, cap: 0, sent_today_after: 0, halted_reason: null };

  await (options.query ? ensureEmmettAutosendSchema(query) : ensureEmmettAutosendSchema());
  const exists = await query('SELECT id FROM clients WHERE id = $1', [id]);
  if (!exists.rows[0]) throw Object.assign(new Error(`Client not found: ${id}`), { statusCode: 404 });
  if (!envEnabled(options.globalEnabled ?? process.env.EMMETT_AUTOSEND_ENABLED)) {
    const result = { ...base, halted_reason: 'disabled' };
    await logAutorun(query, id, result, 'skipped');
    return result;
  }

  const lockClient = options.lockClient || await dbPool.connect();
  const releaseClient = !options.lockClient;
  let locked = false;
  try {
    const lock = await lockClient.query('SELECT pg_try_advisory_lock($1, $2) AS locked', [LOCK_NAMESPACE, id]);
    locked = lock.rows[0]?.locked === true;
    if (!locked) {
      const result = { ...base, halted_reason: 'overlap' };
      await logAutorun(query, id, result, 'skipped');
      return result;
    }

    const clientResult = await query(`
      SELECT id, active, autosend_enabled, warmup_start_date, sender_email, sending_domain
      FROM clients
      WHERE id = $1
    `, [id]);
    const client = clientResult.rows[0];
    if (!client || client.active !== true || client.autosend_enabled !== true) {
      const result = { ...base, halted_reason: 'disabled' };
      await logAutorun(query, id, result, 'skipped');
      return result;
    }

    const local = localDateParts(now, options.timeZone || DEFAULT_TIME_ZONE);
    const timeZone = options.timeZone || DEFAULT_TIME_ZONE;
    if (!client.warmup_start_date) {
      const updated = await query(`
        UPDATE clients
        SET warmup_start_date = $2::date
        WHERE id = $1 AND warmup_start_date IS NULL
        RETURNING warmup_start_date
      `, [id, local.date]);
      client.warmup_start_date = updated.rows[0]?.warmup_start_date || local.date;
    }

    if (!isBusinessWindow(now, options)) {
      const result = { ...base, halted_reason: 'outside_window' };
      await logAutorun(query, id, result, 'skipped');
      return result;
    }

    const stageResult = await query(`
      SELECT business_day_start, daily_cap
      FROM emmett_warmup_config
      ORDER BY business_day_start
    `);
    const startDate = client.warmup_start_date instanceof Date
      ? client.warmup_start_date.toISOString().slice(0, 10)
      : String(client.warmup_start_date).slice(0, 10);
    const businessDay = businessDaysInclusive(startDate, local.date);
    const cap = rampCap(stageResult.rows, businessDay);

    const statsResult = await query(`
      SELECT
        COUNT(*) FILTER (
          WHERE action = 'email_sent'
            AND (ran_at AT TIME ZONE $3)::date = $2::date
        )::int AS sent_today,
        COUNT(*) FILTER (
          WHERE action = 'email_failed'
            AND (ran_at AT TIME ZONE $3)::date = $2::date
        )::int AS failed_today
      FROM agent_log
      WHERE agent_name = 'emmett'
        AND client_id = $1
        AND (ran_at AT TIME ZONE $3)::date = $2::date
    `, [id, local.date, timeZone]);
    const sentToday = Number(statsResult.rows[0]?.sent_today || 0);

    const bounceResult = await query(`
      SELECT COUNT(*)::int AS bounced_today
      FROM email_events
      WHERE client_id = $1
        AND event_type IN ('hard_bounce', 'soft_bounce', 'blocked')
        AND (event_at AT TIME ZONE $3)::date = $2::date
    `, [id, local.date, timeZone]).catch(err => {
      if (err.code === '42P01') return { rows: [{ bounced_today: 0 }] };
      throw err;
    });
    const bouncedToday = Number(bounceResult.rows[0]?.bounced_today || 0);
    const bounceThreshold = Number(options.bounceThreshold ?? DEFAULT_BOUNCE_THRESHOLD);
    const bounceMinSends = Number(options.bounceMinSends ?? DEFAULT_BOUNCE_MIN_SENDS);
    if (sentToday >= bounceMinSends && bouncedToday / sentToday > bounceThreshold) {
      const result = { ...base, cap, sent_today_after: sentToday, halted_reason: 'bounce_breaker' };
      await logAutorun(query, id, { ...result, bounced_today: bouncedToday }, 'failed');
      return result;
    }

    const remaining = Math.max(0, cap - sentToday);
    if (remaining <= 0) {
      const result = { ...base, cap, sent_today_after: sentToday, halted_reason: 'cap_reached' };
      await logAutorun(query, id, result, 'skipped');
      return result;
    }

    const candidates = await query(`
      SELECT p.email
      FROM prospects p
      WHERE p.client_id = $1
        AND p.status IN ('cold', 'contacted', 'warm')
        AND COALESCE(p.do_not_contact, FALSE) = FALSE
        AND COALESCE(p.is_synthetic, FALSE) = FALSE
        AND p.email IS NOT NULL
        AND p.email <> ''
        AND p.email_sequence_completed_at IS NULL
        AND (
          p.last_contacted_at IS NULL
          OR p.next_touch_at <= NOW()
          OR (p.next_touch_at IS NULL AND p.last_contacted_at IS NOT NULL)
        )
        AND NOT EXISTS (
          SELECT 1 FROM touchpoints reply
          WHERE reply.prospect_id = p.id
            AND reply.client_id = p.client_id
            AND reply.action_type IN ('inbound_reply', 'inbound', 'reply', 'email_reply')
        )
        AND NOT EXISTS (
          SELECT 1 FROM touchpoints bounced
          WHERE bounced.prospect_id = p.id
            AND bounced.client_id = p.client_id
            AND bounced.action_type IN ('email_bounced', 'email_soft_bounce', 'email_hard_bounce', 'email_unsubscribed', 'email_spam')
        )
        AND NOT EXISTS (
          SELECT 1 FROM agent_log pending
          WHERE pending.agent_name = 'emmett'
            AND pending.action = 'email_pending'
            AND pending.status = 'pending'
            AND pending.prospect_id = p.id
            AND pending.client_id = p.client_id
        )
      ORDER BY
        EXISTS (
          SELECT 1 FROM touchpoints warm
          WHERE warm.prospect_id = p.id
            AND warm.client_id = p.client_id
            AND warm.action_type IN ('open', 'email_opened', 'click', 'email_clicked', 'hot_flag')
        ) DESC,
        p.icp_score DESC NULLS LAST,
        p.next_touch_at ASC NULLS FIRST
    `, [id]);

    const perSlice = Math.max(1, Number(options.perSlice ?? process.env.EMMETT_AUTOSEND_PER_SLICE ?? DEFAULT_PER_SLICE));
    const slice = Math.min(perSlice, remaining);
    if (!candidates.rows.length) {
      const result = { ...base, cap, sent_today_after: sentToday, halted_reason: 'no_eligible_prospects' };
      await logAutorun(query, id, result, 'success');
      return result;
    }

    const childResult = await runChild({
      client_id: id,
      maxSends: slice,
      dailyCapOverride: cap,
      targetEmails: candidates.rows.map(row => row.email),
      autorunPrechecked: true,
      autorun: true,
      stopOnSendError: false,
    });
    const sent = Number(childResult.successes ?? childResult.sent ?? 0);
    const skipped = Number(childResult.skipped || 0);
    const sentAfterResult = await query(`
      SELECT COUNT(*)::int AS count
      FROM agent_log
      WHERE agent_name = 'emmett'
        AND action = 'email_sent'
        AND client_id = $1
        AND (ran_at AT TIME ZONE $3)::date = $2::date
    `, [id, local.date, timeZone]);
    const result = {
      ...base,
      sent,
      skipped,
      cap,
      sent_today_after: Number(sentAfterResult.rows[0]?.count || sentToday + sent),
      halted_reason: childResult.failed ? 'send_error' : null,
    };
    await logAutorun(query, id, result, childResult.failed ? 'failed' : 'success', childResult.errorSample?.error || null);
    return result;
  } finally {
    if (locked) await lockClient.query('SELECT pg_advisory_unlock($1, $2)', [LOCK_NAMESPACE, id]).catch(() => {});
    if (releaseClient) lockClient.release();
  }
}

module.exports = {
  DEFAULT_TIME_ZONE,
  autorun,
  businessDaysInclusive,
  ensureEmmettAutosendSchema,
  envEnabled,
  isBusinessWindow,
  localDateParts,
  rampCap,
  runEmmettChild,
};
