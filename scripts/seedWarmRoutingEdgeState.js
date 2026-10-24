require('dotenv').config();

const pool = require('../db');

const SEED_VERSION = '2026-07-04-edge-v1';
const CLIENT_ID = Number(process.env.CLIENT_ID || 1);

function assertSafeToSeed() {
  if (String(process.env.WARM_ROUTING_ENABLED || '').toLowerCase() === 'true') {
    throw new Error('Refusing to seed while WARM_ROUTING_ENABLED=true');
  }
  if (process.env.WARM_ROUTING_SEED_CONFIRM !== SEED_VERSION) {
    throw new Error(`Set WARM_ROUTING_SEED_CONFIRM=${SEED_VERSION} to run this explicit write`);
  }
}

async function seedWarmRoutingEdgeState() {
  assertSafeToSeed();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock($1)', [91720260617]);

    // Existing fires become immutable consumed legacy events. This prevents the
    // migration itself from making historical evidence look new.
    await client.query(`
      UPDATE warm_trigger_fires
      SET event_key = 'legacy_fire:' || id::text
      WHERE client_id = $1 AND event_key IS NULL
    `, [CLIENT_ID]);
    await client.query(`
      INSERT INTO warm_signal_events (
        client_id, prospect_id, signal_type, event_key, observed_at,
        evidence, status, routed_fire_id, consumed_at
      )
      SELECT client_id, prospect_id, 'LEGACY_FIRE', event_key, fired_at,
        jsonb_build_object('trigger_reason', trigger_reason, 'seeded_from_fire', true),
        'consumed', id, NOW()
      FROM warm_trigger_fires
      WHERE client_id = $1
      ON CONFLICT (client_id, event_key) DO UPDATE SET
        status = 'consumed', routed_fire_id = EXCLUDED.routed_fire_id,
        consumed_at = COALESCE(warm_signal_events.consumed_at, NOW())
    `, [CLIENT_ID]);

    // Seed ICP state to the latest known history row and preserve score high-water.
    await client.query(`
      INSERT INTO warm_signal_state (
        client_id, prospect_id, signal_type, is_active, last_observed_value,
        last_source_event_key, last_fired_value, last_fired_at
      )
      SELECT p.client_id, p.id, 'ICP_SCORE', COALESCE(p.icp_score, 0) >= 80,
        jsonb_build_object(
          'current_score', COALESCE(p.icp_score, 0),
          'high_water_score', GREATEST(COALESCE(p.icp_score, 0), COALESCE(hist.high_water, 0))
        ),
        hist.latest_id::text,
        CASE WHEN fire.id IS NULL THEN NULL ELSE jsonb_build_object('seeded_from_fire_id', fire.id) END,
        fire.fired_at
      FROM prospects p
      LEFT JOIN LATERAL (
        SELECT MAX(id) AS latest_id, MAX(new_score) AS high_water
        FROM icp_score_history h WHERE h.prospect_id = p.id
      ) hist ON TRUE
      LEFT JOIN LATERAL (
        SELECT id, fired_at FROM warm_trigger_fires f
        WHERE f.client_id = p.client_id AND f.prospect_id = p.id
          AND f.trigger_reason IN ('ICP_JUMP_15', 'ICP_CROSS_90', 'ICP_CROSS_80_RECENT')
        ORDER BY fired_at DESC LIMIT 1
      ) fire ON TRUE
      WHERE p.client_id = $1
      ON CONFLICT (client_id, prospect_id, signal_type) DO UPDATE SET
        is_active = EXCLUDED.is_active,
        last_observed_value = EXCLUDED.last_observed_value,
        last_source_event_key = EXCLUDED.last_source_event_key,
        last_fired_value = EXCLUDED.last_fired_value,
        last_fired_at = EXCLUDED.last_fired_at,
        updated_at = NOW()
    `, [CLIENT_ID]);

    // A prospect already at >=80 with a recent email touch is seeded active.
    await client.query(`
      INSERT INTO warm_signal_state (
        client_id, prospect_id, signal_type, is_active, last_observed_value,
        last_source_event_key, last_fired_value, last_fired_at
      )
      SELECT p.client_id, p.id, 'ICP_ELIGIBILITY',
        COALESCE(
          COALESCE(p.icp_score, 0) >= 80
            AND touch.latest_touch >= NOW() - INTERVAL '14 days',
          FALSE
        ),
        jsonb_build_object('icp_score', COALESCE(p.icp_score, 0), 'email_touched_at', touch.latest_touch),
        CASE WHEN touch.latest_touch IS NULL THEN NULL ELSE 'email_touch:' || touch.latest_touch::text END,
        CASE WHEN fire.id IS NULL THEN NULL ELSE jsonb_build_object('seeded_from_fire_id', fire.id) END,
        fire.fired_at
      FROM prospects p
      LEFT JOIN LATERAL (
        SELECT GREATEST(p.email_touched_at, latest.max_touch) AS latest_touch
        FROM (
          SELECT MAX(t.created_at) AS max_touch
          FROM touchpoints t
          WHERE t.client_id = p.client_id AND t.prospect_id = p.id AND t.channel = 'email'
        ) latest
      ) touch ON TRUE
      LEFT JOIN LATERAL (
        SELECT id, fired_at FROM warm_trigger_fires f
        WHERE f.client_id = p.client_id AND f.prospect_id = p.id
          AND f.trigger_reason = 'ICP_CROSS_80_RECENT'
        ORDER BY fired_at DESC LIMIT 1
      ) fire ON TRUE
      WHERE p.client_id = $1
      ON CONFLICT (client_id, prospect_id, signal_type) DO UPDATE SET
        is_active = EXCLUDED.is_active,
        last_observed_value = EXCLUDED.last_observed_value,
        last_source_event_key = EXCLUDED.last_source_event_key,
        last_fired_value = EXCLUDED.last_fired_value,
        last_fired_at = EXCLUDED.last_fired_at,
        updated_at = NOW()
    `, [CLIENT_ID]);

    // Canonical engagement counts email_events once and excludes mirrored touchpoints.
    await client.query(`
      WITH canonical AS (
        SELECT p.id AS prospect_id, p.client_id,
          COUNT(*) FILTER (WHERE event.kind = 'open')::int AS opens_24h,
          COUNT(*) FILTER (WHERE event.kind = 'click')::int AS clicks_24h,
          (ARRAY_AGG(event.event_key ORDER BY event.occurred_at DESC, event.event_key DESC))[1] AS latest_key
        FROM prospects p
        LEFT JOIN LATERAL (
          SELECT DISTINCT ON (kind, message_key, DATE_TRUNC('second', occurred_at))
            kind, event_key, message_key, occurred_at
          FROM (
            SELECT CASE WHEN ee.event_type IN ('opened','open') THEN 'open' ELSE 'click' END kind,
              'email_event:' || ee.id::text event_key,
              COALESCE(NULLIF(ee.brevo_message_id,''), LOWER(ee.recipient_email)) message_key,
              ee.event_at occurred_at
            FROM email_events ee
            WHERE ee.client_id = p.client_id AND ee.prospect_id = p.id
              AND ee.event_type IN ('opened','open','clicked','click')
              AND ee.event_at >= NOW() - INTERVAL '24 hours'
            UNION ALL
            SELECT CASE WHEN t.action_type IN ('open','email_opened') THEN 'open' ELSE 'click' END,
              'touchpoint:' || t.id::text,
              COALESCE(NULLIF(t.external_ref,''), t.id::text), t.created_at
            FROM touchpoints t
            WHERE t.client_id = p.client_id AND t.prospect_id = p.id
              AND t.action_type IN ('open','email_opened','click','email_clicked')
              AND t.created_at >= NOW() - INTERVAL '24 hours'
              AND NOT EXISTS (
                SELECT 1 FROM email_events ee
                WHERE ee.client_id=t.client_id AND ee.prospect_id=t.prospect_id
                  AND COALESCE(ee.brevo_message_id,'')=COALESCE(t.external_ref,'')
                  AND ee.event_type IN ('opened','open','clicked','click')
                  AND ee.event_at >= NOW() - INTERVAL '24 hours'
              )
          ) raw
          ORDER BY kind, message_key, DATE_TRUNC('second', occurred_at), occurred_at, event_key
        ) event ON TRUE
        WHERE p.client_id = $1
        GROUP BY p.id, p.client_id
      )
      INSERT INTO warm_signal_state (
        client_id, prospect_id, signal_type, is_active, last_observed_value, last_source_event_key
      )
      SELECT client_id, prospect_id, 'ENGAGEMENT_CLUSTER', opens_24h >= 3 OR clicks_24h > 0,
        jsonb_build_object('opens_24h', opens_24h, 'clicks_24h', clicks_24h), latest_key
      FROM canonical
      ON CONFLICT (client_id, prospect_id, signal_type) DO UPDATE SET
        is_active = EXCLUDED.is_active,
        last_observed_value = EXCLUDED.last_observed_value,
        last_source_event_key = EXCLUDED.last_source_event_key,
        updated_at = NOW()
    `, [CLIENT_ID]);

    // Cursor replies to the newest existing source event; old replies are never replayed.
    await client.query(`
      WITH latest_reply AS (
        SELECT DISTINCT ON (source.prospect_id)
          source.prospect_id, source.client_id, source.event_key, source.observed_at
        FROM (
          SELECT ee.prospect_id, ee.client_id, 'email_event:' || ee.id::text event_key, ee.event_at observed_at
          FROM email_events ee
          WHERE ee.client_id=$1 AND ee.event_type IN ('replied','reply')
          UNION ALL
          SELECT t.prospect_id, t.client_id, 'touchpoint:' || t.id::text, t.created_at
          FROM touchpoints t
          WHERE t.client_id=$1 AND t.action_type IN ('inbound','reply','email_reply','inbound_reply')
        ) source
        ORDER BY source.prospect_id, source.observed_at DESC, source.event_key DESC
      )
      INSERT INTO warm_signal_state (
        client_id, prospect_id, signal_type, is_active, last_observed_value, last_source_event_key
      )
      SELECT p.client_id, p.id, 'REPLY', FALSE,
        jsonb_build_object('cursor_at', reply.observed_at), reply.event_key
      FROM prospects p
      LEFT JOIN latest_reply reply ON reply.prospect_id=p.id AND reply.client_id=p.client_id
      WHERE p.client_id=$1
      ON CONFLICT (client_id, prospect_id, signal_type) DO UPDATE SET
        is_active=FALSE,
        last_observed_value=EXCLUDED.last_observed_value,
        last_source_event_key=EXCLUDED.last_source_event_key,
        updated_at=NOW()
    `, [CLIENT_ID]);

    const verification = await client.query(`
      WITH effective_touch AS (
        SELECT p.id, p.client_id, p.icp_score,
          GREATEST(p.email_touched_at, MAX(t.created_at)) AS touched_at
        FROM prospects p
        LEFT JOIN touchpoints t ON t.client_id=p.client_id AND t.prospect_id=p.id AND t.channel='email'
        WHERE p.client_id=$1
        GROUP BY p.id, p.client_id, p.icp_score, p.email_touched_at
      ), reply_sources AS (
        SELECT ee.prospect_id, ee.event_at AS observed_at
        FROM email_events ee
        WHERE ee.client_id=$1 AND ee.event_type IN ('replied','reply')
        UNION ALL
        SELECT t.prospect_id, t.created_at
        FROM touchpoints t
        WHERE t.client_id=$1 AND t.action_type IN ('inbound','reply','email_reply','inbound_reply')
      ), projected AS (
        SELECT p.id, 'ICP_ELIGIBILITY' reason
        FROM effective_touch p
        JOIN warm_signal_state s ON s.client_id=p.client_id AND s.prospect_id=p.id
          AND s.signal_type='ICP_ELIGIBILITY'
        WHERE p.client_id=$1 AND COALESCE(p.icp_score,0)>=80
          AND COALESCE(p.touched_at, '-infinity'::timestamptz)>=NOW()-INTERVAL '14 days'
          AND s.is_active=FALSE
        UNION ALL
        SELECT p.id, 'ICP_SCORE'
        FROM prospects p
        JOIN icp_score_history h ON h.prospect_id=p.id
        JOIN warm_signal_state s ON s.client_id=p.client_id AND s.prospect_id=p.id
          AND s.signal_type='ICP_SCORE'
        WHERE p.client_id=$1 AND h.id>COALESCE(NULLIF(s.last_source_event_key,'')::bigint,0)
        UNION ALL
        SELECT p.id, 'ENGAGEMENT_CLUSTER'
        FROM prospects p
        JOIN warm_signal_state s ON s.client_id=p.client_id AND s.prospect_id=p.id
          AND s.signal_type='ENGAGEMENT_CLUSTER'
        WHERE p.client_id=$1 AND s.is_active=FALSE
          AND ((s.last_observed_value->>'opens_24h')::int>=3 OR (s.last_observed_value->>'clicks_24h')::int>0)
        UNION ALL
        SELECT source.prospect_id, 'REPLY'
        FROM reply_sources source
        JOIN warm_signal_state s ON s.client_id=$1 AND s.prospect_id=source.prospect_id
          AND s.signal_type='REPLY'
        WHERE source.observed_at>COALESCE(
          NULLIF(s.last_observed_value->>'cursor_at','')::timestamptz,
          'epoch'::timestamptz
        )
      )
      SELECT COUNT(DISTINCT id)::int AS projected_first_run_fires,
        COALESCE(jsonb_agg(jsonb_build_object('prospect_id', id, 'reason', reason)), '[]'::jsonb) AS leaks
      FROM projected
    `, [CLIENT_ID]);
    const projected = Number(verification.rows[0]?.projected_first_run_fires || 0);
    if (projected !== 0) {
      throw new Error(`Seed verification failed: projected first run would fire ${projected}`);
    }

    await client.query(`
      INSERT INTO warm_routing_control (
        client_id, seed_version, seeded_at, projected_first_run_fires, seed_details
      ) VALUES ($1,$2,NOW(),0,$3::jsonb)
      ON CONFLICT (client_id) DO UPDATE SET
        seed_version=EXCLUDED.seed_version,
        seeded_at=EXCLUDED.seeded_at,
        projected_first_run_fires=0,
        seed_details=EXCLUDED.seed_details
    `, [CLIENT_ID, SEED_VERSION, JSON.stringify({ verification: 'passed', projected_first_run_fires: 0 })]);

    await client.query('COMMIT');
    return { client_id: CLIENT_ID, seed_version: SEED_VERSION, projected_first_run_fires: 0 };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

if (require.main === module) {
  seedWarmRoutingEdgeState()
    .then(result => {
      console.log(JSON.stringify(result, null, 2));
      return pool.end();
    })
    .catch(async err => {
      console.error(err.stack || err.message);
      await pool.end().catch(() => {});
      process.exitCode = 1;
    });
}

module.exports = { seedWarmRoutingEdgeState, SEED_VERSION };
