// ─────────────────────────────────────────────────────────────────────────────
// Dynamic ICP Scoring
// =============================================================================
// recalculateICP(prospectId) recomputes a prospect's ICP score by combining a
// deterministic base score (same component breakdown Scout uses at scrape time)
// with an engagement bonus/penalty layer derived from the prospect's live
// touchpoint, agent_log, activity_log, and pipeline history.
//
// Base components (max 100):
//   vertical match   0-25
//   location match   0-20
//   contact quality  0-20
//   web presence     0-12
//   size estimate    0-15
//   client fit       0-8
//
// Engagement bonus (added on top of base):
//   1 email open                     +3
//   3+ email opens                   +8   (tiered — highest tier only)
//   5+ email opens                   +15
//   email reply (any)                +20
//   link click                       +10
//   voicemail left by Cal            +5   (Cal outcome — highest tier only)
//   Cal call answered                +10
//   Cal call answered + interested   +20
//   setter contacted                 +10
//   discovery call booked            +25
//
// Penalties:
//   hard bounce                      -20
//   unsubscribe                      -30
//   5+ touches, no response in 30d   -10
//
// Final score is clamped to [0, 100]. Every change is written to
// icp_score_history with the old score, new score, and a reason string.
// ─────────────────────────────────────────────────────────────────────────────

const sharedPool = require('../db');
const { getClientConfig, getRuntimeClientId } = require('./clientContext');
const { OPEN_SOURCE, ensureOpenSignalSchema } = require('./openSignalGate');
const { resolveVerticalTier } = require('./verticalTiers');

// ── TABLE MIGRATION ──────────────────────────────────────────────────────────
// Idempotent. Wired into server.js startup alongside the other ensure* helpers,
// and called lazily on the first recalc as a safety net.
let _historyTableReady = false;
async function ensureIcpScoreHistoryTable() {
  await sharedPool.query(`
    CREATE TABLE IF NOT EXISTS icp_score_history (
      id SERIAL PRIMARY KEY,
      prospect_id UUID REFERENCES prospects(id),
      old_score INTEGER,
      new_score INTEGER,
      reason TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `);
  await sharedPool.query(`
    CREATE INDEX IF NOT EXISTS icp_score_history_prospect_idx
      ON icp_score_history (prospect_id, created_at DESC)
  `);
  await sharedPool.query(`
    CREATE INDEX IF NOT EXISTS icp_score_history_created_idx
      ON icp_score_history (created_at DESC)
  `);
  _historyTableReady = true;
}

async function ensureHistoryReady() {
  if (_historyTableReady) return;
  try {
    await ensureIcpScoreHistoryTable();
  } catch (err) {
    console.error('[icpScoring] ensureIcpScoreHistoryTable failed:', err.message);
  }
}

function clampScore(value) {
  return Math.max(0, Math.min(100, Math.round(Number(value) || 0)));
}

// ── BASE SCORE ───────────────────────────────────────────────────────────────
function scoreVertical(prospect, clientConfig) {
  return resolveVerticalTier(prospect.vertical, clientConfig).vertical_points;
}

function scoreLocation(prospect, clientConfig) {
  const locHay = `${prospect.service_area_match || ''} ${prospect.company_location || ''}`.toLowerCase();
  if (!locHay.trim()) return 0;

  const city = String(clientConfig?.city || '').toLowerCase().trim();
  const state = String(clientConfig?.state || '').toLowerCase().trim();
  const serviceArea = Array.isArray(clientConfig?.service_area) ? clientConfig.service_area : [];

  if (city && locHay.includes(city)) return 20;
  if (serviceArea.some(area => area && locHay.includes(String(area).toLowerCase()))) return 15;
  if (state && locHay.includes(state)) return 8;
  return 0;
}

function scoreContact(prospect) {
  const hasEmail = !!(prospect.email && String(prospect.email).includes('@'));
  const hasPhone = !!(prospect.phone && String(prospect.phone).trim());
  if (hasEmail && hasPhone) return 20;
  if (hasEmail) return 12;
  if (hasPhone) return 8;
  return 0;
}

function scoreWeb(prospect) {
  const hasSite = !!(prospect.has_website || prospect.website_url || prospect.company_website || prospect.company_domain);
  const hasSocial = !!(prospect.has_facebook || prospect.has_instagram || prospect.facebook_url || prospect.instagram_url);
  if (hasSite && hasSocial) return 12;
  if (hasSite) return 8;
  if (hasSocial) return 4;
  return 0;
}

function scoreSize(prospect) {
  const hay = `${prospect.company_name || ''} ${prospect.employee_count_estimate || ''} ${prospect.company_size || ''}`.toLowerCase();
  const STRONG = ['llc', 'inc', 'corp', 'commercial', 'team', 'staff', 'locations', 'group'];
  const hasStrong = STRONG.some(k => hay.includes(k)) || !!prospect.employee_count_estimate || !!prospect.company_size;
  const hasBasic = !!(prospect.phone || prospect.company_location);
  if (hasStrong) return 15;
  if (hasBasic) return 8;
  return 0;
}

function scoreClientFit(prospect, clientConfig, tierResolution) {
  const hay = `${prospect.company_name || ''} ${prospect.service_area_match || ''} ${prospect.company_location || ''}`.toLowerCase();

  // MSHI (client 2): reward HOA/property/county signals like Scout does.
  if (Number(prospect.client_id) === 2) {
    const targetSignals = [
      'hoa', 'homeowners association', 'landlord', 'property management',
      'property manager', 'bank', 'reo', 'foreclosure', 'probate',
      'estate planning', 'estate sale', 'executor',
    ];
    const countySignals = ['kanawha', 'putnam', 'cabell', 'logan', 'boone', 'lincoln', 'fayette'];
    if (targetSignals.some(k => hay.includes(k))) return 8;
    if (countySignals.some(k => hay.includes(k))) return 6;
    return 2;
  }

  // target_clients is legacy free text. Tier config is the structured,
  // reachable client-fit signal for all migrated clients.
  if (tierResolution?.tier === 'A') return 8;
  if (tierResolution?.tier === 'B') return 4;
  if (prospect.service_area_match) return 4;
  return 0;
}

function computeBaseScore(prospect, clientConfig) {
  const tierResolution = resolveVerticalTier(prospect.vertical, clientConfig);
  const components = {
    vertical: scoreVertical(prospect, clientConfig),
    location: scoreLocation(prospect, clientConfig),
    contact: scoreContact(prospect),
    web: scoreWeb(prospect),
    size: scoreSize(prospect),
    client_fit: scoreClientFit(prospect, clientConfig, tierResolution),
  };
  const rawTotal = Object.values(components).reduce((sum, n) => sum + n, 0);
  return {
    total: Math.min(rawTotal, tierResolution.score_ceiling),
    raw_total: rawTotal,
    components,
    tier: tierResolution.tier,
    normalized_vertical: tierResolution.vertical,
    score_ceiling: tierResolution.score_ceiling,
  };
}

// ── ENGAGEMENT + PENALTIES ─────────────────────────────────────────────────
async function gatherEngagement(prospectId, clientId) {
  await ensureOpenSignalSchema(sharedPool);
  const email = await sharedPool.query(`
    SELECT
      COUNT(*) FILTER (
        WHERE ee.event_type IN ('opened', 'open')
          AND ee.open_source = $3::open_source
          AND (
            EXISTS (
              SELECT 1
              FROM email_events sent
              WHERE sent.client_id = ee.client_id
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
            )
            OR EXISTS (
              SELECT 1
              FROM agent_log al
              WHERE al.client_id = ee.client_id
                AND al.prospect_id = ee.prospect_id
                AND al.agent_name = 'emmett'
                AND al.action = 'email_sent'
                AND (
                  (ee.brevo_message_id IS NOT NULL AND al.payload->>'message_id' = ee.brevo_message_id)
                  OR (
                    ee.brevo_message_id IS NULL
                    AND al.payload->>'subject' IS NOT DISTINCT FROM ee.subject_line
                    AND al.ran_at <= ee.event_at
                  )
                )
            )
          )
      )::int AS opens,
      COUNT(*) FILTER (
        WHERE ee.event_type IN ('clicked', 'click')
          AND (
            EXISTS (
              SELECT 1
              FROM email_events sent
              WHERE sent.client_id = ee.client_id
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
            )
            OR EXISTS (
              SELECT 1
              FROM agent_log al
              WHERE al.client_id = ee.client_id
                AND al.prospect_id = ee.prospect_id
                AND al.agent_name = 'emmett'
                AND al.action = 'email_sent'
                AND (
                  (ee.brevo_message_id IS NOT NULL AND al.payload->>'message_id' = ee.brevo_message_id)
                  OR (
                    ee.brevo_message_id IS NULL
                    AND al.payload->>'subject' IS NOT DISTINCT FROM ee.subject_line
                    AND al.ran_at <= ee.event_at
                  )
                )
            )
          )
      )::int AS clicks
    FROM email_events ee
    WHERE ee.prospect_id = $1
      AND ee.client_id = $2
  `, [prospectId, clientId, OPEN_SOURCE.HUMAN]);

  const touch = await sharedPool.query(`
    SELECT
      COUNT(*) FILTER (WHERE action_type IN ('inbound_reply', 'inbound', 'reply', 'email_reply'))::int                        AS replies,
      COUNT(*) FILTER (WHERE action_type = 'email_bounced')::int                                                              AS hard_bounces,
      COUNT(*) FILTER (WHERE action_type IN ('email_unsubscribed', 'unsubscribed', 'email_spam'))::int                        AS unsubscribes,
      COUNT(*) FILTER (WHERE channel = 'phone' AND agent_id = 'cal' AND LOWER(COALESCE(outcome, '')) LIKE '%voicemail%')::int AS cal_voicemails,
      COUNT(*) FILTER (WHERE channel = 'phone' AND agent_id = 'cal' AND LOWER(COALESCE(outcome, '')) IN ('completed', 'answered'))::int AS cal_answered,
      COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '30 days')::int                                                    AS touches_30d
    FROM touchpoints
    WHERE prospect_id = $1 AND client_id = $2
  `, [prospectId, clientId]);

  // "Answered and interested" — a Cal call that the transcript parser flagged as
  // a booking. Logged by the bland webhook as cal_agent/call_completed booked=true.
  const calInterested = await sharedPool.query(`
    SELECT COUNT(*)::int AS count
    FROM agent_log
    WHERE prospect_id = $1
      AND client_id = $2
      AND agent_name IN ('cal', 'cal_agent')
      AND action = 'call_completed'
      AND (payload->>'booked') = 'true'
  `, [prospectId, clientId]).catch(() => ({ rows: [{ count: 0 }] }));

  const dispositions = await sharedPool.query(`
    SELECT
      COUNT(*) FILTER (WHERE disposition = 'voicemail')::int AS voicemail,
      COUNT(*) FILTER (WHERE disposition = 'answered_interested')::int AS answered_interested,
      COUNT(*) FILTER (WHERE disposition = 'answered_callback')::int AS answered_callback,
      COUNT(*) FILTER (WHERE disposition = 'answered_not_interested')::int AS answered_not_interested,
      COUNT(*) FILTER (WHERE disposition IN ('answered_interested', 'answered_callback', 'answered_not_interested'))::int AS answered
    FROM call_dispositions
    WHERE prospect_id = $1 AND client_id = $2
  `, [prospectId, clientId]).catch(() => ({
    rows: [{
      voicemail: 0,
      answered_interested: 0,
      answered_callback: 0,
      answered_not_interested: 0,
      answered: 0,
    }],
  }));

  // Legacy setter contact. New manual calls use call_dispositions/touchpoints,
  // while this preserves the pre-structured history without crossing clients.
  const setter = await sharedPool.query(`
    SELECT EXISTS (
      SELECT 1
      FROM activity_log al
      WHERE al.lead_id = $1 AND al.client_id = $2
    ) AS has_activity
  `, [prospectId, clientId]).catch(() => ({ rows: [{ has_activity: false }] }));

  const row = touch.rows[0] || {};
  const emailRow = email.rows[0] || {};
  const dispositionRow = dispositions.rows[0] || {};
  return {
    opens: Number(emailRow.opens || 0),
    clicks: Number(emailRow.clicks || 0),
    replies: Number(row.replies || 0),
    hard_bounces: Number(row.hard_bounces || 0),
    unsubscribes: Number(row.unsubscribes || 0),
    cal_voicemails: Number(row.cal_voicemails || 0) + Number(dispositionRow.voicemail || 0),
    cal_answered: Number(row.cal_answered || 0) + Number(dispositionRow.answered || 0),
    cal_interested: Number(calInterested.rows[0]?.count || 0) + Number(dispositionRow.answered_interested || 0),
    cal_callback: Number(dispositionRow.answered_callback || 0),
    cal_not_interested: Number(dispositionRow.answered_not_interested || 0),
    touches_30d: Number(row.touches_30d || 0),
    setter_has_activity: !!setter.rows[0]?.has_activity,
  };
}

function computeEngagementBonus(eng, prospect) {
  let bonus = 0;

  // Email opens — tiered, highest applicable tier only.
  if (eng.opens >= 5) bonus += 15;
  else if (eng.opens >= 3) bonus += 8;
  else if (eng.opens >= 1) bonus += 3;

  if (eng.replies > 0) bonus += 20;
  if (eng.clicks > 0) bonus += 10;

  // Cal outcome — tiered, highest applicable tier only.
  if (eng.cal_interested > 0) bonus += 20;
  else if (eng.cal_callback > 0) bonus += 10;
  else if (eng.cal_answered > 0) bonus += 10;
  else if (eng.cal_voicemails > 0) bonus += 5;

  const setterContacted = eng.setter_has_activity ||
    ['contacted', 'follow_up', 'booked'].includes(String(prospect.setter_status || ''));
  if (setterContacted) bonus += 10;

  const booked = !!prospect.booked_at || String(prospect.setter_status || '') === 'booked';
  if (booked) bonus += 25;

  return bonus;
}

function computePenalties(eng, prospect) {
  let penalty = 0;
  if (eng.hard_bounces > 0) penalty += 20;
  if (eng.unsubscribes > 0) penalty += 30;
  if (eng.cal_not_interested > 0) penalty += 5;
  // 5+ touches in the last 30 days with no reply at all.
  if (eng.touches_30d >= 5 && eng.replies === 0) penalty += 10;
  return penalty;
}

// ── PROSPECT LOADER ──────────────────────────────────────────────────────────
async function loadProspect(prospectId, clientId) {
  const res = await sharedPool.query(`
    SELECT
      p.id, p.client_id, p.email, p.phone, p.vertical, p.icp_score,
      p.service_area_match, p.has_website, p.website_url,
      p.has_facebook, p.has_instagram, p.facebook_url, p.instagram_url,
      p.employee_count_estimate, p.setter_status, p.booked_at, p.do_not_contact,
      c.name AS company_name, c.location AS company_location,
      c.website AS company_website, c.domain AS company_domain,
      c.industry AS industry, c.size AS company_size
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.id = $1 AND p.client_id = $2
    LIMIT 1
  `, [prospectId, clientId]);
  return res.rows[0] || null;
}

// ── PUBLIC: recalculateICP ─────────────────────────────────────────────────
// Recomputes and persists a prospect's ICP score. Returns a result object;
// never throws on a missing prospect (returns { found: false }).
async function recalculateICP(prospectId, options = {}) {
  if (!prospectId) return { found: false };
  await ensureHistoryReady();

  const clientId = options.clientId != null ? Number(options.clientId) : getRuntimeClientId();
  const reason = options.reason || 'recalculation';

  const prospect = await loadProspect(prospectId, clientId);
  if (!prospect) {
    console.warn(`[icpScoring] recalculateICP: prospect ${prospectId} not found for client ${clientId}`);
    return { found: false };
  }

  let clientConfig = null;
  try {
    clientConfig = await getClientConfig(clientId);
  } catch (err) {
    console.error('[icpScoring] getClientConfig failed:', err.message);
  }

  const base = computeBaseScore(prospect, clientConfig);
  if (base.tier === 'unknown') {
    console.warn(`[icpScoring] Unknown vertical tier: client=${clientId} prospect=${prospectId} vertical=${prospect.vertical || '(blank)'}`);
    await sharedPool.query(
      `INSERT INTO agent_log (agent_name, action, client_id, prospect_id, payload, status, ran_at)
       VALUES ('icp_scoring', 'unknown_vertical_tier', $1, $2, $3::jsonb, 'completed', NOW())`,
      [clientId, prospectId, JSON.stringify({ raw_vertical: prospect.vertical || null, normalized_vertical: base.normalized_vertical })]
    ).catch(err => console.error('[icpScoring] unknown vertical audit failed:', err.message));
  }
  const eng = await gatherEngagement(prospectId, clientId);
  const bonus = computeEngagementBonus(eng, prospect);
  const penalty = computePenalties(eng, prospect);

  const oldScore = prospect.icp_score == null ? null : Number(prospect.icp_score);
  const newScore = clampScore(Math.min(base.score_ceiling, base.total + bonus - penalty));

  if (oldScore === newScore) {
    return { found: true, changed: false, old_score: oldScore, new_score: newScore, base: base.total, base_components: base.components, tier: base.tier, engagement: bonus, penalties: penalty };
  }

  await sharedPool.query(
    `UPDATE prospects SET icp_score = $1, updated_at = NOW() WHERE id = $2 AND client_id = $3`,
    [newScore, prospectId, clientId]
  );
  const history = await logScoreChange(prospectId, oldScore, newScore, reason);
  if (history?.id) {
    const { safeIngestIcpScoreChange } = require('./maxSignalIngestion');
    await safeIngestIcpScoreChange({
      prospectId,
      clientId,
      historyId: history.id,
      oldScore,
      newScore,
      createdAt: history.created_at,
    });
  }

  console.log(`[icpScoring] ${prospectId} ICP ${oldScore ?? 'null'} → ${newScore} (base ${base.total} + eng ${bonus} - pen ${penalty}) [${reason}]`);
  return {
    found: true,
    changed: true,
    old_score: oldScore,
    new_score: newScore,
    base: base.total,
    base_components: base.components,
    tier: base.tier,
    score_ceiling: base.score_ceiling,
    engagement: bonus,
    penalties: penalty,
  };
}

// Read-only counterpart for approval-gated migrations. It intentionally does
// not ensure tables, update prospects, or append score history.
async function previewRecalculateICP(prospectId, options = {}) {
  if (!prospectId) return { found: false };
  const clientId = options.clientId != null ? Number(options.clientId) : getRuntimeClientId();
  const prospect = await loadProspect(prospectId, clientId);
  if (!prospect) return { found: false };
  const clientConfig = options.clientConfig || await getClientConfig(clientId);
  const base = computeBaseScore(prospect, clientConfig);
  const engagement = await gatherEngagement(prospectId, clientId);
  const bonus = computeEngagementBonus(engagement, prospect);
  const penalties = computePenalties(engagement, prospect);
  return {
    found: true,
    prospect_id: prospectId,
    old_score: prospect.icp_score == null ? null : Number(prospect.icp_score),
    new_score: clampScore(Math.min(base.score_ceiling, base.total + bonus - penalties)),
    normalized_vertical: base.normalized_vertical,
    tier: base.tier,
    score_ceiling: base.score_ceiling,
    base: base.total,
    base_components: base.components,
    engagement: bonus,
    penalties,
  };
}

// Write a single row to icp_score_history. Best-effort — logs but never throws.
async function logScoreChange(prospectId, oldScore, newScore, reason) {
  try {
    await ensureHistoryReady();
    const result = await sharedPool.query(
      `INSERT INTO icp_score_history (prospect_id, old_score, new_score, reason)
       VALUES ($1, $2, $3, $4)
       RETURNING id, created_at`,
      [prospectId, oldScore, newScore, String(reason || 'recalculation').slice(0, 200)]
    );
    return result.rows[0] || null;
  } catch (err) {
    console.error('[icpScoring] logScoreChange failed:', err.message);
    return null;
  }
}

// Record Scout's initial score for a freshly inserted prospect (old_score = null).
async function recordScoutBaseline(prospectId, score, reason = 'scout_initial') {
  if (!prospectId) return;
  await logScoreChange(prospectId, null, clampScore(score), reason);
}

module.exports = {
  recalculateICP,
  recordScoutBaseline,
  logScoreChange,
  ensureIcpScoreHistoryTable,
  computeBaseScore,
  previewRecalculateICP,
};
