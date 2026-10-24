require('dotenv').config();

const axios = require('axios');
const pool = require('../db');
const {
  BATCH_OPEN_DISTINCT_PROSPECT_THRESHOLD,
  BATCH_OPEN_WINDOW_SECONDS,
  DELIVERY_COINCIDENT_THRESHOLD_SECONDS,
  OPEN_SOURCE,
  ensureOpenSignalSchema,
} = require('../utils/openSignalGate');

const DAYS = Number(process.env.OPEN_SOURCE_BACKFILL_DAYS || 30);
const TODOIST_API_BASE = 'https://api.todoist.com/api/v1';

function todoistToken() {
  return process.env.TODOIST_API_TOKEN || process.env.TODOIST_TOKEN || null;
}

async function query(sql, params = []) {
  const res = await pool.query(sql, params);
  return res.rowCount || Number(res.rows[0]?.count || 0);
}

function sendExistsSql(alias = 'ee') {
  return `
    (
      EXISTS (
        SELECT 1
        FROM email_events sent
        WHERE sent.client_id = ${alias}.client_id
          AND sent.prospect_id = ${alias}.prospect_id
          AND sent.event_type IN ('sent', 'delivered')
          AND (
            (${alias}.brevo_message_id IS NOT NULL AND sent.brevo_message_id = ${alias}.brevo_message_id)
            OR (
              ${alias}.brevo_message_id IS NULL
              AND LOWER(sent.recipient_email) = LOWER(${alias}.recipient_email)
              AND sent.subject_line IS NOT DISTINCT FROM ${alias}.subject_line
              AND sent.event_at <= ${alias}.event_at
            )
          )
      )
      OR EXISTS (
        SELECT 1
        FROM agent_log al
        WHERE al.client_id = ${alias}.client_id
          AND al.prospect_id = ${alias}.prospect_id
          AND al.agent_name = 'emmett'
          AND al.action = 'email_sent'
          AND (
            (${alias}.brevo_message_id IS NOT NULL AND al.payload->>'message_id' = ${alias}.brevo_message_id)
            OR (
              ${alias}.brevo_message_id IS NULL
              AND al.payload->>'subject' IS NOT DISTINCT FROM ${alias}.subject_line
              AND al.ran_at <= ${alias}.event_at
            )
          )
      )
    )
  `;
}

async function reclassifyRecentOpens() {
  await ensureOpenSignalSchema(pool);

  await query(`
    UPDATE email_events
    SET user_agent = COALESCE(
          NULLIF(user_agent, ''),
          NULLIF(raw_payload->>'user_agent', ''),
          NULLIF(raw_payload->>'userAgent', ''),
          NULLIF(raw_payload->>'user-agent', ''),
          NULLIF(raw_payload->>'User-Agent', '')
        ),
        ip_address = COALESCE(
          NULLIF(ip_address, ''),
          NULLIF(raw_payload->>'ip', ''),
          NULLIF(raw_payload->>'ip_address', ''),
          NULLIF(raw_payload->>'remote_ip', ''),
          NULLIF(raw_payload->>'client_ip', '')
        )
    WHERE event_at >= NOW() - ($1::int * INTERVAL '1 day')
      AND event_type IN ('opened', 'open', 'opened_proxy', 'clicked', 'click')
  `, [DAYS]);

  await query(`
    UPDATE email_events
    SET open_source = $2::open_source,
        open_source_reason = 'backfill_pending',
        open_source_classified_at = NOW()
    WHERE event_at >= NOW() - ($1::int * INTERVAL '1 day')
      AND event_type IN ('opened', 'open', 'opened_proxy')
  `, [DAYS, OPEN_SOURCE.UNKNOWN]);

  const explicitProxy = await query(`
    UPDATE email_events
    SET open_source = $2::open_source,
        open_source_reason = 'brevo_proxy_event',
        open_source_classified_at = NOW()
    WHERE event_at >= NOW() - ($1::int * INTERVAL '1 day')
      AND (
        event_type = 'opened_proxy'
        OR COALESCE(raw_payload->>'event', '') ~* '^(loaded_by_proxy|loadedbyproxy|proxyopen|proxy_open|unique_loaded_by_proxy|uniqueloadedbyproxy|uniqueproxyopen|unique_proxy_open)$'
      )
  `, [DAYS, OPEN_SOURCE.PROXY]);

  const uaProxy = await query(`
    UPDATE email_events
    SET open_source = $2::open_source,
        open_source_reason = 'known_proxy_user_agent',
        open_source_classified_at = NOW()
    WHERE event_at >= NOW() - ($1::int * INTERVAL '1 day')
      AND event_type IN ('opened', 'open')
      AND open_source <> $2::open_source
      AND COALESCE(user_agent, '') ~* '(googleimageproxy|mailprivacy|mail privacy|apple.*mail.*proxy|duckduckgo.*email|yahoo.*proxy|proxy)'
  `, [DAYS, OPEN_SOURCE.PROXY]);

  const deliveryCoincident = await query(`
    WITH open_rows AS (
      SELECT ee.id, MIN(sent.event_at) AS delivered_at
      FROM email_events ee
      JOIN email_events sent
        ON sent.client_id = ee.client_id
       AND sent.prospect_id = ee.prospect_id
       AND sent.event_type IN ('sent', 'delivered')
       AND (
         (ee.brevo_message_id IS NOT NULL AND sent.brevo_message_id = ee.brevo_message_id)
         OR (
           ee.brevo_message_id IS NULL
           AND LOWER(sent.recipient_email) = LOWER(ee.recipient_email)
           AND sent.subject_line IS NOT DISTINCT FROM ee.subject_line
           AND sent.event_at <= ee.event_at
         )
       )
      WHERE ee.event_at >= NOW() - ($1::int * INTERVAL '1 day')
        AND ee.event_type IN ('opened', 'open')
        AND ee.open_source <> $3::open_source
      GROUP BY ee.id, ee.event_at
      HAVING ABS(EXTRACT(EPOCH FROM (ee.event_at - MIN(sent.event_at)))) <= $2
    )
    UPDATE email_events ee
    SET open_source = $3::open_source,
        open_source_reason = 'delivery_coincident',
        open_source_classified_at = NOW()
    FROM open_rows
    WHERE ee.id = open_rows.id
  `, [DAYS, DELIVERY_COINCIDENT_THRESHOLD_SECONDS, OPEN_SOURCE.PROXY]);

  const batchRes = await pool.query(`
    WITH candidates AS (
      SELECT
        id,
        prospect_id,
        CASE
          WHEN ip_address ~ '^([0-9]{1,3}\\.){3}[0-9]{1,3}$'
            THEN 'ip:' || split_part(ip_address, '.', 1) || '.' || split_part(ip_address, '.', 2) || '.' || split_part(ip_address, '.', 3) || '.0/24'
          WHEN COALESCE(user_agent, '') <> ''
            THEN 'ua:' || LEFT(LOWER(regexp_replace(user_agent, '\\s+', ' ', 'g')), 180)
          ELSE NULL
        END AS source_key,
        FLOOR(EXTRACT(EPOCH FROM event_at) / $2::int)::bigint AS bucket
      FROM email_events
      WHERE event_at >= NOW() - ($1::int * INTERVAL '1 day')
        AND event_type IN ('opened', 'open')
        AND open_source <> $4::open_source
    ), hot_buckets AS (
      SELECT source_key, bucket
      FROM candidates
      WHERE source_key IS NOT NULL
      GROUP BY source_key, bucket
      HAVING COUNT(DISTINCT prospect_id) >= $3
    )
    UPDATE email_events ee
    SET open_source = $4::open_source,
        open_source_reason = 'batch_fire',
        open_source_classified_at = NOW()
    FROM candidates c
    JOIN hot_buckets b ON b.source_key = c.source_key AND b.bucket = c.bucket
    WHERE ee.id = c.id
  `, [
    DAYS,
    BATCH_OPEN_WINDOW_SECONDS,
    BATCH_OPEN_DISTINCT_PROSPECT_THRESHOLD,
    OPEN_SOURCE.PROXY,
  ]);
  const batchProxy = Number(batchRes.rowCount || 0);

  const human = await query(`
    UPDATE email_events ee
    SET open_source = $2::open_source,
        open_source_reason = 'sent_non_proxy_open',
        open_source_classified_at = NOW()
    WHERE ee.event_at >= NOW() - ($1::int * INTERVAL '1 day')
      AND ee.event_type IN ('opened', 'open')
      AND ee.open_source = 'unknown'::open_source
      AND ${sendExistsSql('ee')}
  `, [DAYS, OPEN_SOURCE.HUMAN]);

  const proxyTotal = Number(explicitProxy) + Number(uaProxy) + Number(deliveryCoincident) + Number(batchProxy);
  return { explicitProxy, uaProxy, deliveryCoincident, batchProxy, proxyTotal, human };
}

async function logZeroSendRejections() {
  const { rows } = await pool.query(`
    WITH rejected AS (
      SELECT ee.*
      FROM email_events ee
      WHERE ee.event_at >= NOW() - ($1::int * INTERVAL '1 day')
        AND ee.event_type IN ('opened', 'open', 'clicked', 'click')
        AND NOT ${sendExistsSql('ee')}
        AND NOT EXISTS (
          SELECT 1
          FROM agent_log al
          WHERE al.agent_name = 'riley'
            AND al.action = 'signal_dropped_zero_send'
            AND al.payload->>'event_id' = ee.event_id
        )
    ), inserted AS (
      INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
      SELECT 'riley', 'signal_dropped_zero_send', prospect_id,
        jsonb_build_object(
          'event_id', event_id,
          'event_type', event_type,
          'recipient_email', recipient_email,
          'brevo_message_id', brevo_message_id,
          'subject', subject_line,
          'reason', 'no_corresponding_send',
          'prospect_id', prospect_id,
          'client_id', client_id,
          'backfill', true
        ),
        'skipped', NOW(), client_id
      FROM rejected
      RETURNING id
    )
    SELECT COUNT(*)::int AS count FROM inserted
  `, [DAYS]);
  return Number(rows[0]?.count || 0);
}

async function suppressWarmSignalEvents() {
  const { rows } = await pool.query(`
    WITH bad_events AS (
      SELECT ee.id
      FROM email_events ee
      WHERE ee.event_at >= NOW() - ($1::int * INTERVAL '1 day')
        AND (
          (ee.event_type IN ('opened', 'open') AND ee.open_source <> $2::open_source)
          OR (ee.event_type IN ('opened', 'open', 'clicked', 'click') AND NOT ${sendExistsSql('ee')})
        )
    ), updated AS (
      UPDATE warm_signal_events wse
      SET status = 'failed',
          evidence = COALESCE(wse.evidence, '{}'::jsonb) || jsonb_build_object(
            'suppressed_reason', 'proxy_or_zero_send',
            'suppressed_at', NOW()
          )
      FROM bad_events bad
      WHERE wse.event_key = 'email_event:' || bad.id::text
        AND wse.status <> 'failed'
      RETURNING wse.id
    )
    SELECT COUNT(*)::int AS count FROM updated
  `, [DAYS, OPEN_SOURCE.HUMAN]);
  return Number(rows[0]?.count || 0);
}

async function closeTodoistTask(taskId) {
  const token = todoistToken();
  if (!token || !taskId) return { closed: false, reason: token ? 'missing_task_id' : 'missing_token' };
  await axios.post(`${TODOIST_API_BASE}/tasks/${taskId}/close`, {}, {
    headers: { Authorization: `Bearer ${token}` },
    timeout: 8000,
  });
  return { closed: true };
}

async function closePhantomTouchTasks() {
  const { rows } = await pool.query(`
    WITH bad_events AS (
      SELECT ee.id, ee.prospect_id, ee.client_id, ee.event_at
      FROM email_events ee
      WHERE ee.event_at >= NOW() - ($1::int * INTERVAL '1 day')
        AND (
          (ee.event_type IN ('opened', 'open') AND ee.open_source <> $2::open_source)
          OR (ee.event_type IN ('opened', 'open', 'clicked', 'click') AND NOT ${sendExistsSql('ee')})
        )
    ), bad_fires AS (
      SELECT DISTINCT fire.id
      FROM warm_trigger_fires fire
      LEFT JOIN LATERAL jsonb_array_elements(COALESCE(fire.trigger_payload->'evidence', '[]'::jsonb)) evidence ON TRUE
      LEFT JOIN bad_events direct_bad
        ON evidence->>'event_key' = 'email_event:' || direct_bad.id::text
      LEFT JOIN bad_events score_bad
        ON score_bad.client_id = fire.client_id
       AND score_bad.prospect_id = fire.prospect_id
       AND score_bad.event_at <= fire.fired_at
       AND score_bad.event_at >= fire.fired_at - INTERVAL '24 hours'
      WHERE fire.fired_at >= NOW() - ($1::int * INTERVAL '1 day')
        AND (
          direct_bad.id IS NOT NULL
          OR (
            fire.trigger_reason IN ('ICP_JUMP_15', 'ICP_CROSS_90', 'ICP_CROSS_80_RECENT', 'ENGAGEMENT_CLUSTER')
            AND score_bad.id IS NOT NULL
            AND NOT EXISTS (
              SELECT 1
              FROM email_events good
              WHERE good.client_id = fire.client_id
                AND good.prospect_id = fire.prospect_id
                AND good.event_type IN ('opened', 'open')
                AND good.open_source = $2::open_source
                AND good.event_at <= fire.fired_at
                AND good.event_at >= fire.fired_at - INTERVAL '24 hours'
                AND ${sendExistsSql('good')}
            )
          )
        )
    )
    SELECT fire.id, fire.client_id, fire.prospect_id, fire.todoist_task_id, fire.resolved_action
    FROM warm_trigger_fires fire
    JOIN bad_fires bad ON bad.id = fire.id
    WHERE COALESCE(fire.trigger_payload->>'closed_reason_code', '') <> 'proxy_or_zero_send'
  `, [DAYS, OPEN_SOURCE.HUMAN]);

  let todoistClosed = 0;
  let todoistSkipped = 0;
  for (const row of rows) {
    let closeResult = { closed: false, reason: 'not_auto_task' };
    if (row.todoist_task_id && row.resolved_action === 'auto_escalated') {
      try {
        closeResult = await closeTodoistTask(row.todoist_task_id);
      } catch (err) {
        closeResult = { closed: false, reason: err.message };
      }
    }
    if (closeResult.closed) todoistClosed++;
    else todoistSkipped++;

    await pool.query(`
      UPDATE warm_trigger_fires
      SET resolved_action = 'closed_phantom_signal',
          resolved_at = NOW(),
          trigger_payload = COALESCE(trigger_payload, '{}'::jsonb) || $2::jsonb
      WHERE id = $1
    `, [
      row.id,
      JSON.stringify({
        closed_reason_code: 'proxy_or_zero_send',
        closed_by: 'backfillOpenSourceGate',
        closed_at: new Date().toISOString(),
        todoist_close: closeResult,
      }),
    ]);
  }

  return { firesClosed: rows.length, todoistClosed, todoistSkipped };
}

async function main() {
  const classification = await reclassifyRecentOpens();
  const zeroSendLogged = await logZeroSendRejections();
  const warmSignalsSuppressed = await suppressWarmSignalEvents();
  const touchTasks = await closePhantomTouchTasks();

  console.log(JSON.stringify({
    days: DAYS,
    opens_reclassified_as_proxy: classification.proxyTotal,
    classification,
    zero_send_rejections_logged: zeroSendLogged,
    warm_signals_suppressed: warmSignalsSuppressed,
    touch_tasks_closed: touchTasks.firesClosed,
    todoist_tasks_closed: touchTasks.todoistClosed,
    todoist_tasks_skipped: touchTasks.todoistSkipped,
  }, null, 2));
}

main()
  .catch(err => {
    console.error(err.stack || err.message);
    process.exitCode = 1;
  })
  .finally(() => pool.end());
