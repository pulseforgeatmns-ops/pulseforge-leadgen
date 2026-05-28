require('dotenv').config();
const pool = require('./db');
const axios = require('axios');
const Anthropic = require('@anthropic-ai/sdk');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');
const { getExpansionReport } = require('./scoutExpansion');

const client = new Anthropic();
const AGENT_NAME = 'max';
const CLIENT_ID = getRuntimeClientId();
let CLIENT_CONFIG = null;

const DIGEST_AGENT_NAMING_RULES = `Agent naming rules — use these exactly, never substitute:

Emmett = the email outreach agent. He sends cold emails, manages sequences, tracks opens and clicks.
Vera = the Google Business Profile review response agent. She does NOT send emails. Only mention Vera in context of GBP reviews.
Riley = the receptionist/triage agent. She monitors inbound signals and classifies replies.
Paige = the content agent. She generates and publishes social posts and blog content.
Sam = the SMS agent. He sends text notifications.
Cal = the calling agent. He makes outbound Bland.ai calls.
Scout = the lead scraper. He finds and enriches new prospects.

Never attribute email sending, open rates, or click rates to Vera. That data always belongs to Emmett.`;

const CLIENT_MARKET_LABELS = {
  1: 'Manchester NH',
  2: 'Charleston WV',
  5: 'Nashville TN',
};

function compactMarketLabel(value) {
  return String(value || '')
    .replace(/\s*,\s*/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function hasStateToken(value) {
  return /\b[A-Z]{2}\b/.test(String(value || '').toUpperCase());
}

function clientMarketLabel() {
  return CLIENT_MARKET_LABELS[CLIENT_ID] || compactMarketLabel([CLIENT_CONFIG?.city, CLIENT_CONFIG?.state].filter(Boolean).join(' '));
}

function prospectMarketLabel(prospect = {}) {
  const serviceArea = compactMarketLabel(prospect.service_area_match);
  if (!serviceArea) return clientMarketLabel();
  if (hasStateToken(serviceArea)) return serviceArea;

  const state = CLIENT_CONFIG?.state || clientMarketLabel().split(' ').pop();
  return compactMarketLabel([serviceArea, state].filter(Boolean).join(' '));
}

function formatProspectBusinessLabel(prospect = {}) {
  const business = prospect.company_name || prospect.company || prospect.business_name;
  if (!business) return business;

  const market = prospectMarketLabel(prospect);
  if (!market || String(business).includes(`(${market})`)) return business;
  return `${business} (${market})`;
}

function enrichProspectLabels(rows = []) {
  return rows.map(row => {
    const companyNameWithMarket = formatProspectBusinessLabel(row);
    return {
      ...row,
      raw_company_name: row.company_name,
      company_name: companyNameWithMarket || row.company_name,
      company_name_with_market: companyNameWithMarket,
      market_label: prospectMarketLabel(row),
    };
  });
}

function digestSnapshotWithMarketLabels(snapshot) {
  return {
    ...snapshot,
    recentTouchpoints: enrichProspectLabels(snapshot.recentTouchpoints),
    untouched: enrichProspectLabels(snapshot.untouched),
    cold: enrichProspectLabels(snapshot.cold),
    clickedToday: enrichProspectLabels(snapshot.clickedToday),
  };
}

function normalizeCompanyName(value) {
  if (!value) return '';
  return String(value)
    .replace(/\s*\([^)]*\)\s*$/, '') // drop trailing market parenthetical
    .toLowerCase()
    .replace(/[^a-z0-9&]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// Business names actually handed to the LLM this run — the only ones it may reference.
function snapshotCompanyNameSet(snapshot) {
  const names = new Set();
  for (const key of ['recentTouchpoints', 'untouched', 'cold', 'clickedToday']) {
    for (const row of snapshot[key] || []) {
      for (const v of [row.raw_company_name, row.company_name, row.company_name_with_market]) {
        const n = normalizeCompanyName(v);
        if (n) names.add(n);
      }
    }
  }
  return names;
}

async function getKnownCompanyNames() {
  const res = await pool.query(
    `SELECT name FROM companies WHERE client_id = $1 AND name IS NOT NULL`,
    [CLIENT_ID]
  ).catch(() => ({ rows: [] }));
  const names = new Set();
  for (const row of res.rows) {
    const n = normalizeCompanyName(row.name);
    if (n) names.add(n);
  }
  return names;
}

// Post-generation guardrail: strip any business the model named that does not
// trace back to a fetched snapshot row or a row in the companies table.
async function verifyDigestProspects(digestText, snapshot) {
  if (!digestText) return digestText;

  const allowed = snapshotCompanyNameSet(snapshot);
  for (const n of await getKnownCompanyNames()) allowed.add(n);

  // Max is instructed to tag every prospect mention as "Business Name (Market ST)".
  // Only treat parentheticals carrying a state token as company mentions.
  const mentionRe = /([A-Z][A-Za-z0-9&'.\-]*(?:\s+[A-Z0-9&][A-Za-z0-9&'.\-]*)*)\s*\(([^)]+)\)/g;
  const flagged = new Set();
  let match;
  while ((match = mentionRe.exec(digestText)) !== null) {
    const [full, rawName, market] = match;
    if (!hasStateToken(market)) continue;
    if (allowed.has(normalizeCompanyName(rawName))) continue;
    flagged.add(full.trim());
  }

  if (!flagged.size) {
    await insertAgentLog('digest_prospect_validation', { flagged: 0 }).catch(() => {});
    return digestText;
  }

  let cleaned = digestText;
  for (const mention of flagged) {
    console.warn(`[Max] Digest referenced unverified business "${mention}" — stripping from output`);
    cleaned = cleaned.split(mention).join('[unverified prospect removed]');
  }

  await insertAgentLog('digest_prospect_validation', {
    flagged: flagged.size,
    stripped: [...flagged],
  }, 'partial').catch(() => {});

  return cleaned;
}

async function getSystemSnapshot() {
  // Prospect breakdown by status
  const prospectStats = await pool.query(`
    SELECT status, COUNT(*) as count
    FROM prospects
    WHERE client_id = $1
    GROUP BY status
    ORDER BY count DESC
  `, [CLIENT_ID]);

  // Recent touchpoints (last 7 days)
  const recentTouchpoints = await pool.query(`
    SELECT 
      p.first_name, p.last_name, p.email,
      ${prospectCompanySql('p')} AS company_name,
      p.notes, p.service_area_match,
      t.channel, t.action_type, t.content_summary,
      t.created_at
    FROM touchpoints t
    JOIN prospects p ON t.prospect_id = p.id
    LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
    WHERE t.created_at > NOW() - INTERVAL '7 days'
      AND t.client_id = $1
      AND p.client_id = $1
    ORDER BY t.created_at DESC
    LIMIT 20
  `, [CLIENT_ID]);

  // Prospects with no touchpoints (never contacted)
  const untouched = await pool.query(`
    SELECT
      p.first_name, p.last_name, p.email, p.icp_score, p.status,
      ${prospectCompanySql('p')} AS company_name,
      p.notes, p.service_area_match
    FROM prospects p
    LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
    WHERE NOT EXISTS (
      SELECT 1 FROM touchpoints t
      WHERE t.prospect_id = p.id
        AND t.client_id = p.client_id
    )
    AND p.do_not_contact = false
    AND p.email IS NOT NULL
    AND p.client_id = $1
    ORDER BY p.icp_score DESC
    LIMIT 10
  `, [CLIENT_ID]);

  // Prospects touched but gone cold (no activity in 14+ days)
  const cold = await pool.query(`
    SELECT 
      p.first_name, p.last_name, p.email, p.status,
      ${prospectCompanySql('p')} AS company_name,
      p.notes, p.service_area_match,
      MAX(t.created_at) as last_touch,
      COUNT(t.id) as touch_count
    FROM prospects p
    JOIN touchpoints t ON t.prospect_id = p.id AND t.client_id = p.client_id
    LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
    WHERE p.do_not_contact = false
      AND p.client_id = $1
    GROUP BY p.id, p.first_name, p.last_name, p.email, p.status, c.name, p.notes, p.service_area_match
    HAVING MAX(t.created_at) < NOW() - INTERVAL '14 days'
    ORDER BY last_touch ASC
    LIMIT 10
  `, [CLIENT_ID]);

  // Pending comments awaiting approval
  const pending = await pool.query(`
    SELECT channel, COUNT(*) as count
    FROM pending_comments
    WHERE status = 'pending' AND client_id = $1
    GROUP BY channel
  `, [CLIENT_ID]);

  // Channel performance
  const channelStats = await pool.query(`
    SELECT channel, COUNT(*) as total
    FROM touchpoints
    WHERE client_id = $1
    GROUP BY channel
    ORDER BY total DESC
  `, [CLIENT_ID]);

  // Recent posts published (last 2 days) with engagement data
  const recentPosts = await pool.query(`
    SELECT channel, content_type, engagement_rate, likes, comments, shares, reach,
           metrics_fetched_at
    FROM post_analytics
    WHERE client_id = $1 AND published_at > NOW() - INTERVAL '2 days'
    ORDER BY published_at DESC
    LIMIT 10
  `, [CLIENT_ID]).catch(() => ({ rows: [] }));

  // Best performing content type per channel (all time)
  const bestContentTypes = await pool.query(`
    SELECT cps.channel, cps.content_type, cps.avg_engagement_rate, cps.post_count
    FROM content_performance_summary cps
    JOIN companies c ON cps.company_id = c.id
    WHERE c.client_id = $1
      AND cps.post_count >= 2
    ORDER BY cps.channel, cps.avg_engagement_rate DESC
  `, [CLIENT_ID]).catch(() => ({ rows: [] }));

  // Channel posting frequency: posts in last 7 days vs expected (4 channels × 1/week)
  const postFreq = await pool.query(`
    SELECT channel, COUNT(*) AS posts_this_week
    FROM post_analytics
    WHERE client_id = $1 AND published_at > NOW() - INTERVAL '7 days'
    GROUP BY channel
  `, [CLIENT_ID]).catch(() => ({ rows: [] }));

  // Prospects clicked a link today — high priority signals
  const clickedToday = await pool.query(`
    SELECT DISTINCT
      p.id, p.first_name, p.last_name,
      ${prospectCompanySql('p')} AS company_name,
      p.notes, p.service_area_match
    FROM touchpoints t
    JOIN prospects p ON t.prospect_id = p.id
    LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
    WHERE t.action_type = 'email_clicked'
      AND t.client_id = $1
      AND p.client_id = $1
      AND t.created_at > NOW() - INTERVAL '1 day'
  `, [CLIENT_ID]).catch(() => ({ rows: [] }));

  // Warm upgrades today
  const warmToday = await pool.query(`
    SELECT COUNT(*)::int AS count FROM prospects
    WHERE status = 'warm'
      AND client_id = $1
      AND updated_at > NOW() - INTERVAL '1 day'
      AND EXISTS (
        SELECT 1 FROM touchpoints t WHERE t.prospect_id = prospects.id
          AND t.client_id = prospects.client_id
          AND t.action_type = 'email_clicked'
      )
  `, [CLIENT_ID]).catch(() => ({ rows: [{ count: 0 }] }));

  // Email open rate this week
  const emailStats = await pool.query(`
    SELECT
      COUNT(CASE WHEN action_type = 'outbound'      THEN 1 END)::int AS sent,
      COUNT(CASE WHEN action_type = 'email_opened'  THEN 1 END)::int AS opened,
      COUNT(CASE WHEN action_type = 'email_clicked' THEN 1 END)::int AS clicked
    FROM touchpoints
    WHERE channel = 'email' AND client_id = $1 AND created_at > NOW() - INTERVAL '7 days'
  `, [CLIENT_ID]).catch(() => ({ rows: [{ sent: 0, opened: 0, clicked: 0 }] }));

  const contentQuality = await pool.query(`
    SELECT
      ROUND(AVG(COALESCE(NULLIF(payload->>'total', '')::int, NULLIF(payload->'scores'->>'total', '')::int))::numeric, 1) AS avg_score,
      COUNT(*) FILTER (WHERE payload->>'regenerated' = 'true')::int AS regenerated_count,
      COUNT(*)::int AS total_scored,
      MODE() WITHIN GROUP (ORDER BY NULLIF(payload->>'weak_dimension', '')) AS most_common_weakness
    FROM agent_log
    WHERE agent_name = 'paige'
      AND action = 'content_scored'
      AND client_id = $1
      AND ran_at >= NOW() - INTERVAL '7 days'
  `, [CLIENT_ID]).catch(() => ({ rows: [{ avg_score: null, regenerated_count: 0, total_scored: 0, most_common_weakness: null }] }));

  const closerMetrics = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE p.booked_at >= date_trunc('week', NOW()))::int AS booked_week,
      COUNT(*) FILTER (WHERE p.closer_status IN ('showed','closed') OR p.setter_status = 'closed')::int AS showed,
      COUNT(*) FILTER (WHERE p.booked_at IS NOT NULL)::int AS booked_total,
      COUNT(*) FILTER (WHERE p.setter_status = 'closed')::int AS closed,
      COALESCE(SUM(p.mrr_value) FILTER (
        WHERE p.setter_status = 'closed'
          AND p.closed_at >= date_trunc('month', NOW())
      ), 0)::numeric AS mrr_this_month,
      COALESCE(SUM(c.commission_amt) FILTER (WHERE c.status = 'pending'), 0)::numeric AS pending_commissions
    FROM prospects p
    LEFT JOIN commissions c ON c.prospect_id = p.id AND c.client_id = p.client_id
    WHERE p.client_id = $1
  `, [CLIENT_ID]).catch(() => ({ rows: [{
    booked_week: 0,
    showed: 0,
    booked_total: 0,
    closed: 0,
    mrr_this_month: 0,
    pending_commissions: 0,
  }] }));

  // Copy performance — Emmett email_sent logs (last 7 days) grouped by sequence/step,
  // joined to touchpoints to compute open rate per sequence/step combo.
  const copyPerformance = await pool.query(`
    SELECT
      s.sequence,
      s.step,
      COUNT(*)::int AS sent,
      COUNT(*) FILTER (WHERE o.opened)::int AS opens
    FROM (
      SELECT
        al.prospect_id,
        al.client_id,
        al.ran_at,
        al.payload->>'sequence' AS sequence,
        al.payload->>'step'     AS step
      FROM agent_log al
      WHERE al.agent_name = 'emmett'
        AND al.action = 'email_sent'
        AND al.client_id = $1
        AND al.ran_at >= NOW() - INTERVAL '7 days'
    ) s
    LEFT JOIN LATERAL (
      SELECT TRUE AS opened
      FROM touchpoints t
      WHERE t.prospect_id = s.prospect_id
        AND t.client_id = s.client_id
        AND t.channel = 'email'
        AND t.action_type IN ('open', 'email_opened')
        AND t.created_at >= s.ran_at
      LIMIT 1
    ) o ON TRUE
    GROUP BY s.sequence, s.step
    ORDER BY s.sequence, s.step
  `, [CLIENT_ID]).catch(() => ({ rows: [] }));

  return {
    prospectStats: prospectStats.rows,
    recentTouchpoints: recentTouchpoints.rows,
    untouched: untouched.rows,
    cold: cold.rows,
    pending: pending.rows,
    channelStats: channelStats.rows,
    recentPosts: recentPosts.rows,
    bestContentTypes: bestContentTypes.rows,
    postFreq: postFreq.rows,
    clickedToday:  clickedToday.rows,
    warmToday:     warmToday.rows[0]?.count || 0,
    emailStats:    emailStats.rows[0],
    contentQuality: contentQuality.rows[0],
    closerMetrics: closerMetrics.rows[0],
    copyPerformance: copyPerformance.rows,
    client: CLIENT_CONFIG,
  };
}

// Concatenate every text-type content block. Anthropic occasionally returns
// multiple blocks (e.g. with thinking enabled or when responses are split);
// the previous `message.content[0].text` only read the first block and broke
// silently — returning undefined — when block 0 was non-text or content was
// empty. Throw a descriptive error so the outer handler can log it instead.
function extractTextFromMessage(message) {
  const blocks = Array.isArray(message?.content) ? message.content : [];
  const texts = blocks
    .filter(b => b && b.type === 'text' && typeof b.text === 'string')
    .map(b => b.text);
  const joined = texts.join('\n').trim();
  if (!joined) {
    const shape = JSON.stringify({
      block_count: blocks.length,
      block_types: blocks.map(b => b?.type),
      stop_reason: message?.stop_reason || null,
      usage: message?.usage || null,
    });
    throw new Error(`Claude response contained no usable text — ${shape}`);
  }
  return joined;
}

// Build the four-section digest directly from snapshot data. Used as the
// LLM fallback and as the MSHI deterministic path. Same shape as the LLM
// output (ACTIONS EXECUTED / EXCEPTIONS / PIPELINE SNAPSHOT / RECOMMENDATION)
// so the dashboard format never regresses to the old verbose template.
function buildDeterministicDigest(snapshot, autoExec) {
  const autoExecLine = formatAutoExecSummary(autoExec);
  const closer = snapshot.closerMetrics || {};
  const replies = snapshot.recentTouchpoints.filter(t =>
    ['reply', 'inbound', 'email_reply'].includes(t.action_type)
  ).length;
  const pending = snapshot.pending.reduce((sum, p) => sum + parseInt(p.count || 0), 0);
  const totalProspects = snapshot.prospectStats.reduce((sum, r) => sum + parseInt(r.count || 0), 0);

  const exceptions = [];
  if (snapshot.clickedToday.length) {
    const labels = snapshot.clickedToday
      .slice(0, 3)
      .map(p => formatProspectBusinessLabel(p) || p.email || `${p.first_name || ''} ${p.last_name || ''}`.trim())
      .filter(Boolean);
    exceptions.push(`${snapshot.clickedToday.length} prospect${snapshot.clickedToday.length === 1 ? '' : 's'} clicked an email today${labels.length ? ` (${labels.join(', ')})` : ''} — worth personal outreach`);
  }
  if (replies) exceptions.push(`${replies} new repl${replies === 1 ? 'y' : 'ies'} this week`);
  if (pending) exceptions.push(`${pending} post${pending === 1 ? '' : 's'} awaiting approval`);
  if (snapshot.warmToday) exceptions.push(`${snapshot.warmToday} prospect${snapshot.warmToday === 1 ? '' : 's'} upgraded to warm today`);

  const exceptionsText = exceptions.length
    ? exceptions.map(e => `- ${e}`).join('\n')
    : 'No exceptions today.';

  let recommendation = '';
  if (snapshot.clickedToday.length) {
    recommendation = `RECOMMENDATION: Have Cal or Sam reach out to today's email clickers while they're hot.`;
  } else if (pending >= 3) {
    recommendation = `RECOMMENDATION: Clear the approval queue — ${pending} posts are blocking the content calendar.`;
  } else if (snapshot.cold.length >= 5 && (autoExec?.stale_reset || 0) === 0) {
    recommendation = `RECOMMENDATION: ${snapshot.cold.length} prospects are 14+ days cold — consider a re-engagement push.`;
  }

  const greeting = CLIENT_ID === 2
    ? `Good morning Brad & Dustin — your MSHI update for ${new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric' })}`
    : `Pulseforge daily digest — ${new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' })}`;

  return `${greeting}

ACTIONS EXECUTED: ${autoExecLine}

EXCEPTIONS:
${exceptionsText}

PIPELINE SNAPSHOT
Booked calls this week: ${closer.booked_week || 0}
MRR closed this month: $${Number(closer.mrr_this_month || 0).toLocaleString()}
Pending approvals: ${pending}
Total prospects: ${totalProspects}${recommendation ? `\n\n${recommendation}` : ''}

— Pulseforge`;
}

async function generateInsightsViaLLM(snapshot, autoExec) {
  const autoExecLine = formatAutoExecSummary(autoExec);
  const dataString = JSON.stringify(digestSnapshotWithMarketLabels(snapshot), null, 2);

  const message = await client.messages.create({
    model: 'claude-sonnet-4-5',
    max_tokens: 500,
    system: DIGEST_AGENT_NAMING_RULES,
    messages: [{
      role: 'user',
      content: `Today is ${new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })}. You are Max, the manager agent for Pulseforge.

Max already auto-executed these actions before this digest was generated. Do NOT repeat, re-flag, or recommend them:
${autoExecLine}

Second brain snapshot (the ONLY source for prospect references):

${dataString}

Generate a tight, streamlined digest with EXACTLY these four sections, in order:

1. ACTIONS EXECUTED — One short paragraph restating the auto-executed actions above in a manager's voice.
2. EXCEPTIONS — Items needing a human decision. Only include: warm signals worth personal outreach (from clickedToday), new replies (from recentTouchpoints with action_type reply / inbound / email_reply), unusual patterns. If none, say "No exceptions today."
3. PIPELINE SNAPSHOT — 3-4 lines max. Pull from closerMetrics, prospectStats, pending. Example lines: booked calls this week, MRR closed this month, total pending approvals, warm prospects today.
4. RECOMMENDATION — ONE recommendation only, if any. If nothing is actionable, omit the section entirely.

Style rules:
- Plain text. No markdown. No headers beyond the four labels above.
- DO NOT include: click rate commentary, generic content quality observations, routine bounce mentions, untouched prospect lists, copy performance.
- When naming a prospect, copy company_name_with_market verbatim from the snapshot, e.g. "TT Hair Salon (Manchester NH)". Never invent a business name.
- Keep total length under 200 words.`
    }]
  });

  const text = extractTextFromMessage(message);
  console.log(`[Max] generateInsights LLM produced ${text.length} chars (stop_reason=${message?.stop_reason || 'unknown'})`);
  return text;
}

async function generateInsights(snapshot, autoExec) {
  console.log(`[Max] generateInsights called for client_id=${CLIENT_ID} (autoExec keys: ${autoExec ? Object.keys(autoExec).join(',') : 'none'})`);

  // MSHI is always deterministic (no LLM cost or latency for a 2-line update).
  if (CLIENT_ID === 2) {
    console.log('[Max] MSHI client — using deterministic digest path.');
    return buildDeterministicDigest(snapshot, autoExec);
  }

  // For Pulseforge clients we prefer the LLM-shaped digest, but fall back to
  // the deterministic four-section template on any failure. This guarantees
  // every dashboard trigger writes a fresh daily_digest row instead of
  // silently leaving the previous day's brief in place.
  try {
    return await generateInsightsViaLLM(snapshot, autoExec);
  } catch (err) {
    console.error(`[Max] LLM digest failed (${err.message}) — falling back to deterministic template.`);
    await insertAgentLog('digest_llm_fallback', {
      reason: err.message,
      stack: (err.stack || '').slice(0, 1500),
    }, 'partial').catch(() => {});
    return buildDeterministicDigest(snapshot, autoExec);
  }
}

function formatScoutExpansionSection(report) {
  if (!report) return '';

  const {
    yieldByCombo = [],
    saturatedThisWeek = [],
    queuedForExpansion = [],
    successfullyExpanded = [],
  } = report;

  const hasActivity =
    saturatedThisWeek.length > 0 ||
    queuedForExpansion.length > 0 ||
    successfullyExpanded.length > 0;
  if (!hasActivity) return '';

  const lines = ['SCOUT EXPANSION'];
  const activeYields = [...yieldByCombo]
    .filter(y => Number(y.prospects_found || 0) > 0)
    .sort((a, b) => b.prospects_found - a.prospects_found);
  if (activeYields.length) {
    const yieldBits = activeYields.slice(0, 4).map(y => `${y.vertical}/${y.location} ${y.prospects_found}`);
    const yieldExtra = activeYields.length > 4 ? ` (+${activeYields.length - 4} more)` : '';
    lines.push(`Yield: ${yieldBits.join(', ')}${yieldExtra}.`);
  }

  if (saturatedThisWeek.length) {
    const satBits = saturatedThisWeek
      .slice(0, 5)
      .map(s => `${s.vertical}/${s.location} (${s.prospects_found})`);
    const satExtra = saturatedThisWeek.length > satBits.length
      ? ` +${saturatedThisWeek.length - satBits.length} more`
      : '';
    lines.push(`Saturated: ${satBits.join(', ')}${satExtra}.`);
  }

  if (queuedForExpansion.length) {
    const queueBits = queuedForExpansion
      .slice(0, 5)
      .map(q => `${q.location} (${q.vertical})`);
    const queueExtra = queuedForExpansion.length > queueBits.length
      ? ` +${queuedForExpansion.length - queueBits.length} more`
      : '';
    lines.push(`Queued: ${queueBits.join(', ')}${queueExtra}.`);
  }

  if (successfullyExpanded.length) {
    const winBits = successfullyExpanded
      .slice(0, 5)
      .map(e => `${e.location}/${e.vertical} (${e.prospects_found})`);
    const winExtra = successfullyExpanded.length > winBits.length
      ? ` +${successfullyExpanded.length - winBits.length} more`
      : '';
    lines.push(`Expanded: ${winBits.join(', ')}${winExtra}.`);
  } else if (queuedForExpansion.length) {
    lines.push('Expansion scouts running — no first prospects from new markets yet.');
  }

  return lines.slice(0, 6).join('\n');
}

async function sendDigest(digestText, snapshot, expansionReport) {
  // Refuse to ship an empty or non-string digest — better to skip the email
  // and surface the failure than to send "null" / "undefined" to the client.
  if (typeof digestText !== 'string' || !digestText.trim()) {
    console.error(`[Max] sendDigest refused — digestText is ${typeof digestText} / ${digestText === null ? 'null' : digestText === undefined ? 'undefined' : 'empty'}`);
    return false;
  }

  const scoutExpansionBlock = formatScoutExpansionSection(expansionReport);
  const scoutExpansionSection = scoutExpansionBlock
    ? `\n${'─'.repeat(50)}\n${scoutExpansionBlock}\n`
    : '';

  const toEmail = CLIENT_CONFIG?.max_email || 'jacob@gopulseforge.com';
  const toName = CLIENT_ID === 2 ? 'Brad & Dustin' : 'Jake Maynard';
  const subject = `${CLIENT_ID === 2 ? 'MSHI' : 'Pulseforge'} Daily Digest — ${new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' })}`;

  const body = CLIENT_ID === 2 ? `${digestText}
${scoutExpansionSection}
Pulseforge · gopulseforge.com` : `PULSEFORGE DAILY DIGEST
${new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })}
${'─'.repeat(50)}

${digestText}
${scoutExpansionSection}
${'─'.repeat(50)}
Pulseforge · gopulseforge.com
To adjust digest frequency reply to this email.`;

  try {
    const response = await axios.post('https://api.brevo.com/v3/smtp/email', {
      sender: { name: 'Max — Pulseforge', email: 'jacob@gopulseforge.com' },
      to: [{ email: toEmail, name: toName }],
      subject,
      textContent: body
    }, {
      headers: {
        'api-key': process.env.BREVO_API_KEY,
        'Content-Type': 'application/json'
      }
    });

    console.log('Digest sent — Message ID:', response.data.messageId);
    return true;
  } catch (err) {
    console.error('Failed to send digest:', err.response?.data || err.message);
    return false;
  }
}

async function logAgentRun(insights) {
  const safeInsights = typeof insights === 'string' ? insights.slice(0, 2000) : '';
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
    VALUES ($1, $2, $3, $4, NOW(), $5)
  `, [AGENT_NAME, 'daily_digest', JSON.stringify({ insights: safeInsights, client_id: CLIENT_ID }), 'success', CLIENT_ID]);
}

async function createActions(snapshot) {
  const actions = [];

  // Clicked-not-warm: prospects who clicked but aren't warm yet
  for (const p of snapshot.clickedToday) {
    actions.push({
      action_type: 'follow_up_clicked',
      title: `Follow up with ${p.first_name} ${p.last_name}`,
      description: `${p.first_name} at ${p.company_name || 'their company'} clicked a link in your email today. Cal or Sam should reach out now while they're hot.`,
      payload: { prospect_id: p.id, first_name: p.first_name, last_name: p.last_name, company: p.company_name },
    });
  }

  // >10 untouched prospects
  if (snapshot.untouched.length >= 10) {
    actions.push({
      action_type: 'untouched_backlog',
      title: `${snapshot.untouched.length} prospects never contacted`,
      description: `There are ${snapshot.untouched.length} prospects with no touchpoints. Run Emmett or trigger outreach to start working the pipeline.`,
      payload: { count: snapshot.untouched.length },
    });
  }

  // Cold 14d+ prospects
  if (snapshot.cold.length >= 5) {
    actions.push({
      action_type: 'cold_prospects',
      title: `${snapshot.cold.length} prospects gone cold (14+ days)`,
      description: `${snapshot.cold.length} prospects haven't been touched in 14+ days. Consider a re-engagement sequence or clean them out of the pipeline.`,
      payload: { count: snapshot.cold.length },
    });
  }

  // >5 pending approvals
  const totalPending = snapshot.pending.reduce((acc, p) => acc + parseInt(p.count), 0);
  if (totalPending >= 5) {
    const breakdown = snapshot.pending.map(p => `${p.count} ${p.channel}`).join(', ');
    actions.push({
      action_type: 'pending_approvals',
      title: `${totalPending} posts waiting for approval`,
      description: `Pending queue has ${totalPending} items (${breakdown}). Review and approve to keep the content calendar moving.`,
      payload: { total: totalPending, breakdown: snapshot.pending },
    });
  }

  if (actions.length === 0) return;

  for (const action of actions) {
    try {
      await pool.query(`
        INSERT INTO agent_actions (created_by, action_type, title, description, payload, status, client_id)
        VALUES ('max', $1, $2, $3, $4, 'pending', $5)
        ON CONFLICT ON CONSTRAINT agent_actions_action_type_payload_key DO NOTHING
      `, [action.action_type, action.title, action.description, JSON.stringify(action.payload), CLIENT_ID]);
    } catch (_) {
      // Fallback if constraint doesn't exist — just insert
      try {
        await pool.query(`
          INSERT INTO agent_actions (created_by, action_type, title, description, payload, status, client_id)
          SELECT 'max', $1, $2, $3, $4, 'pending', $5
          WHERE NOT EXISTS (
            SELECT 1 FROM agent_actions
            WHERE action_type = $1 AND status = 'pending' AND client_id = $5
              AND created_at > NOW() - INTERVAL '24 hours'
          )
        `, [action.action_type, action.title, action.description, JSON.stringify(action.payload), CLIENT_ID]);
      } catch (e2) {
        console.error('[Max] createActions insert error:', e2.message);
      }
    }
  }
  console.log(`[Max] Deposited ${actions.length} action(s) into agent_actions`);
}

async function insertAgentLog(action, payload, status = 'success', prospectId = null) {
  await pool.query(`
    INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
    VALUES ($1, $2, $3, $4, $5, NOW(), $6)
  `, [AGENT_NAME, action, prospectId, JSON.stringify({ ...payload, client_id: CLIENT_ID }), status, CLIENT_ID]);
}

function prospectCompanySql(alias = 'p') {
  return `COALESCE(c.name, NULLIF(TRIM(SPLIT_PART(${alias}.notes, ' — ', 1)), ''), CONCAT(${alias}.first_name, ' ', ${alias}.last_name))`;
}

// Trigger 1 — warm prospects stale 14+ days → queue Sam re-engagement via cold status
async function runReengagementTrigger() {
  const res = await pool.query(`
    SELECT
      p.id,
      p.email,
      ${prospectCompanySql('p')} AS company
    FROM prospects p
    LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
    WHERE p.client_id = $1
      AND p.status = 'warm'
      AND COALESCE(p.do_not_contact, false) = false
      AND (
        SELECT MAX(t.created_at)
        FROM touchpoints t
        WHERE t.prospect_id = p.id AND t.client_id = p.client_id
      ) < NOW() - INTERVAL '14 days'
      AND NOT EXISTS (
        SELECT 1 FROM agent_log al
        WHERE al.prospect_id = p.id
          AND al.client_id = p.client_id
          AND al.action = 'reengagement_trigger'
          AND al.status = 'pending'
      )
  `, [CLIENT_ID]);

  let count = 0;
  for (const row of res.rows) {
    await insertAgentLog('reengagement_trigger', {
      prospect_id: row.id,
      email: row.email,
      company: row.company,
    }, 'pending', row.id);
    await pool.query(
      `UPDATE prospects SET status = 'cold', updated_at = NOW() WHERE id = $1 AND client_id = $2`,
      [row.id, CLIENT_ID]
    );
    count++;
  }

  await insertAgentLog('reengagement_trigger_summary', { count });
  console.log(`[Max] Trigger 1 (re-engagement): ${count} prospect(s) reverted to cold`);
  return count;
}

// Trigger 2 — exhausted cold email sequences with no reply → mark dead
async function runMarkSequenceDeadTrigger() {
  const res = await pool.query(`
    SELECT
      p.id,
      p.email,
      ${prospectCompanySql('p')} AS company
    FROM prospects p
    LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
    WHERE p.client_id = $1
      AND p.status = 'cold'
      AND COALESCE(p.do_not_contact, false) = false
      AND (
        SELECT COUNT(*)::int
        FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.client_id = p.client_id
          AND t.channel = 'email'
          AND t.action_type = 'outbound'
      ) >= 4
      AND NOT EXISTS (
        SELECT 1 FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.client_id = p.client_id
          AND t.channel = 'email'
          AND t.action_type IN ('inbound', 'reply', 'email_reply')
      )
      AND (
        SELECT MAX(t.created_at)
        FROM touchpoints t
        WHERE t.prospect_id = p.id
          AND t.client_id = p.client_id
          AND t.channel = 'email'
          AND t.action_type = 'outbound'
      ) < NOW() - INTERVAL '14 days'
  `, [CLIENT_ID]);

  let count = 0;
  for (const row of res.rows) {
    await pool.query(
      `UPDATE prospects SET status = 'dead', updated_at = NOW() WHERE id = $1 AND client_id = $2`,
      [row.id, CLIENT_ID]
    );
    await insertAgentLog('auto_marked_dead', {
      prospect_id: row.id,
      email: row.email,
      company: row.company,
    }, 'success', row.id);
    count++;
  }

  await insertAgentLog('auto_marked_dead_summary', { count });
  console.log(`[Max] Trigger 2 (sequence complete): ${count} prospect(s) marked dead`);
  return count;
}

// Trigger 3 — low Paige content scores → queue regeneration on next Paige run
async function runPaigeQualityGateTrigger() {
  const res = await pool.query(`
    SELECT id, payload, client_id
    FROM agent_log
    WHERE agent_name = 'paige'
      AND action = 'content_scored'
      AND client_id = $1
      AND ran_at >= NOW() - INTERVAL '24 hours'
      AND COALESCE(
        NULLIF(payload->>'total', '')::int,
        NULLIF(payload->'scores'->>'total', '')::int
      ) < 20
      AND NOT EXISTS (
        SELECT 1 FROM agent_log pending
        WHERE pending.agent_name = 'max'
          AND pending.action = 'paige_regenerate_trigger'
          AND pending.status = 'pending'
          AND pending.client_id = agent_log.client_id
          AND pending.payload->>'channel' = agent_log.payload->>'channel'
          AND pending.ran_at >= NOW() - INTERVAL '24 hours'
      )
  `, [CLIENT_ID]);

  let count = 0;
  for (const row of res.rows) {
    const payload = typeof row.payload === 'string' ? JSON.parse(row.payload) : (row.payload || {});
    const channel = payload.channel;
    if (!channel) continue;

    await insertAgentLog('paige_regenerate_trigger', {
      channel,
      source_log_id: row.id,
      total: payload.total ?? payload.scores?.total ?? null,
    }, 'pending');
    count++;
  }

  await insertAgentLog('paige_regenerate_trigger_summary', { count });
  console.log(`[Max] Trigger 3 (Paige quality gate): ${count} regenerate trigger(s) queued`);
  return count;
}

// Trigger 4 — underperforming email copy → flag for human review
// Any sequence/step combo with an open rate below 8% and 30+ sends in the last
// 7 days gets a copy_review_needed flag (status 'pending') so it surfaces in the digest.
async function runCopyReviewTrigger() {
  const res = await pool.query(`
    SELECT
      s.sequence,
      s.step,
      COUNT(*)::int AS sent,
      COUNT(*) FILTER (WHERE o.opened)::int AS opens,
      (ARRAY_AGG(DISTINCT s.subject) FILTER (WHERE s.subject IS NOT NULL AND s.subject <> ''))[1:3] AS sample_subjects
    FROM (
      SELECT
        al.prospect_id,
        al.client_id,
        al.ran_at,
        al.payload->>'sequence' AS sequence,
        al.payload->>'step'     AS step,
        al.payload->>'subject'  AS subject
      FROM agent_log al
      WHERE al.agent_name = 'emmett'
        AND al.action = 'email_sent'
        AND al.client_id = $1
        AND al.ran_at >= NOW() - INTERVAL '7 days'
    ) s
    LEFT JOIN LATERAL (
      SELECT TRUE AS opened
      FROM touchpoints t
      WHERE t.prospect_id = s.prospect_id
        AND t.client_id = s.client_id
        AND t.channel = 'email'
        AND t.action_type IN ('open', 'email_opened')
        AND t.created_at >= s.ran_at
      LIMIT 1
    ) o ON TRUE
    GROUP BY s.sequence, s.step
    HAVING COUNT(*) >= 30
      AND (COUNT(*) FILTER (WHERE o.opened))::numeric / NULLIF(COUNT(*), 0) < 0.08
  `, [CLIENT_ID]);

  let count = 0;
  for (const row of res.rows) {
    const sent = Number(row.sent || 0);
    const opens = Number(row.opens || 0);
    const openRate = sent ? Number(((opens / sent) * 100).toFixed(1)) : 0;

    // De-dup: skip if a pending copy_review_needed already exists for this combo in the last 7 days
    const existing = await pool.query(`
      SELECT 1 FROM agent_log
      WHERE agent_name = 'max'
        AND action = 'copy_review_needed'
        AND status = 'pending'
        AND client_id = $1
        AND payload->>'sequence' = $2
        AND payload->>'step' = $3
        AND ran_at >= NOW() - INTERVAL '7 days'
      LIMIT 1
    `, [CLIENT_ID, row.sequence, String(row.step)]);
    if (existing.rows.length) continue;

    await insertAgentLog('copy_review_needed', {
      sequence: row.sequence,
      step: row.step,
      sent,
      opens,
      open_rate: openRate,
      sample_subjects: row.sample_subjects || [],
    }, 'pending');
    count++;
  }

  await insertAgentLog('copy_review_needed_summary', { count });
  console.log(`[Max] Trigger 4 (copy review): ${count} flag(s) raised`);
  return count;
}

// ── AUTO-EXECUTE ACTIONS ────────────────────────────────────────────────────
// Low-risk cleanup work Max performs automatically before generating the digest.
// All actions log to agent_log and return counts that feed the digest's
// "Actions executed" section.

async function ensureCalQueueTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS cal_queue (
      id SERIAL PRIMARY KEY,
      prospect_id INTEGER NOT NULL,
      client_id INTEGER NOT NULL,
      priority INTEGER NOT NULL DEFAULT 1,
      reason TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      status TEXT NOT NULL DEFAULT 'pending'
    )
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS cal_queue_pending_idx
      ON cal_queue (client_id, status, priority, created_at)
  `);
}

// 1. Mark hard-bounce prospects dead + DNC.
async function runMarkBouncesDead() {
  const res = await pool.query(`
    SELECT DISTINCT p.id, p.email, ${prospectCompanySql('p')} AS company
    FROM agent_log al
    JOIN prospects p ON p.id = al.prospect_id
    LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
    WHERE al.agent_name = 'riley'
      AND al.action = 'email_bounced'
      AND al.payload->>'event' = 'hard_bounce'
      AND al.ran_at > NOW() - INTERVAL '24 hours'
      AND p.client_id = $1
      AND (p.status IS DISTINCT FROM 'dead' OR COALESCE(p.do_not_contact, false) = false)
  `, [CLIENT_ID]);

  let count = 0;
  for (const row of res.rows) {
    await pool.query(
      `UPDATE prospects
       SET status = 'dead', do_not_contact = true, updated_at = NOW()
       WHERE id = $1 AND client_id = $2`,
      [row.id, CLIENT_ID]
    );
    await insertAgentLog('auto_marked_dead', {
      prospect_id: row.id,
      email: row.email,
      company: row.company,
      reason: 'hard_bounce',
    }, 'success', row.id);
    count++;
  }
  await insertAgentLog('auto_marked_dead_bounce_summary', { count });
  console.log(`[Max] Auto-action: marked ${count} hard-bounce prospect(s) dead`);
  return count;
}

// 2. Null out generic inbox addresses (info@, contact@, etc.).
async function runNullGenericEmails() {
  const res = await pool.query(`
    SELECT id, email FROM prospects
    WHERE client_id = $1
      AND email IS NOT NULL
      AND LOWER(SPLIT_PART(email, '@', 1)) IN ('info','contact','support','office','admin','hello')
  `, [CLIENT_ID]);

  let count = 0;
  for (const row of res.rows) {
    await pool.query(
      `UPDATE prospects SET email = NULL, updated_at = NOW()
       WHERE id = $1 AND client_id = $2`,
      [row.id, CLIENT_ID]
    );
    await insertAgentLog('auto_email_nulled', {
      prospect_id: row.id,
      previous_email: row.email,
    }, 'success', row.id);
    count++;
  }
  await insertAgentLog('auto_email_nulled_summary', { count });
  console.log(`[Max] Auto-action: nulled ${count} generic-inbox email(s)`);
  return count;
}

// 3. Queue warm/hot prospects for Cal if no Cal call in the last 7 days.
async function runQueueWarmForCal() {
  const res = await pool.query(`
    SELECT p.id, p.status, p.is_hot
    FROM prospects p
    WHERE p.client_id = $1
      AND COALESCE(p.do_not_contact, false) = false
      AND (p.is_hot = true OR p.status = 'warm')
      AND NOT EXISTS (
        SELECT 1 FROM touchpoints t
        WHERE t.prospect_id = p.id AND t.client_id = p.client_id
          AND t.channel = 'phone' AND t.agent_id = 'cal'
          AND t.created_at > NOW() - INTERVAL '7 days'
      )
      AND NOT EXISTS (
        SELECT 1 FROM cal_queue q
        WHERE q.prospect_id = p.id AND q.client_id = p.client_id
          AND q.status = 'pending'
      )
  `, [CLIENT_ID]);

  let count = 0;
  for (const row of res.rows) {
    const reason = row.is_hot ? 'is_hot' : 'warm_status';
    await pool.query(
      `INSERT INTO cal_queue (prospect_id, client_id, priority, reason, status)
       VALUES ($1, $2, 1, $3, 'pending')`,
      [row.id, CLIENT_ID, reason]
    );
    await insertAgentLog('auto_queued_cal', {
      prospect_id: row.id,
      priority: 1,
      reason,
    }, 'success', row.id);
    count++;
  }
  await insertAgentLog('auto_queued_cal_summary', { count });
  console.log(`[Max] Auto-action: queued ${count} warm prospect(s) for Cal`);
  return count;
}

// 4. Hand off 5+ touchpoint no-response prospects to Cal for a nurture call.
async function runHandoff5TouchNoReply() {
  const res = await pool.query(`
    SELECT p.id
    FROM prospects p
    WHERE p.client_id = $1
      AND p.status IN ('cold', 'contacted')
      AND COALESCE(p.do_not_contact, false) = false
      AND (
        SELECT COUNT(*) FROM touchpoints t
        WHERE t.prospect_id = p.id AND t.client_id = p.client_id
      ) >= 5
      AND NOT EXISTS (
        SELECT 1 FROM touchpoints t
        WHERE t.prospect_id = p.id AND t.client_id = p.client_id
          AND t.action_type IN ('inbound', 'reply', 'email_reply')
      )
      AND NOT EXISTS (
        SELECT 1 FROM cal_queue q
        WHERE q.prospect_id = p.id AND q.client_id = p.client_id
          AND q.status = 'pending'
      )
  `, [CLIENT_ID]);

  let count = 0;
  for (const row of res.rows) {
    await pool.query(
      `INSERT INTO cal_queue (prospect_id, client_id, priority, reason, status)
       VALUES ($1, $2, 2, '5_touch_no_reply', 'pending')`,
      [row.id, CLIENT_ID]
    );
    await insertAgentLog('auto_queued_cal_nurture', {
      prospect_id: row.id,
      priority: 2,
      reason: '5_touch_no_reply',
    }, 'success', row.id);
    count++;
  }
  await insertAgentLog('auto_queued_cal_nurture_summary', { count });
  console.log(`[Max] Auto-action: queued ${count} 5+ touch no-reply prospect(s) for Cal`);
  return count;
}

// 5. Reset 'contacted' prospects stale for 21+ days back to cold so Emmett re-enters them.
async function runResetStaleSequences() {
  const res = await pool.query(`
    SELECT p.id
    FROM prospects p
    WHERE p.client_id = $1
      AND p.status = 'contacted'
      AND COALESCE(p.do_not_contact, false) = false
      AND (
        SELECT MAX(t.created_at) FROM touchpoints t
        WHERE t.prospect_id = p.id AND t.client_id = p.client_id
      ) < NOW() - INTERVAL '21 days'
  `, [CLIENT_ID]);

  let count = 0;
  for (const row of res.rows) {
    await pool.query(
      `UPDATE prospects SET status = 'cold', updated_at = NOW()
       WHERE id = $1 AND client_id = $2`,
      [row.id, CLIENT_ID]
    );
    await insertAgentLog('auto_reset_stale', {
      prospect_id: row.id,
    }, 'success', row.id);
    count++;
  }
  await insertAgentLog('auto_reset_stale_summary', { count });
  console.log(`[Max] Auto-action: reset ${count} stale 'contacted' prospect(s) to cold`);
  return count;
}

async function runAutoExecuteActions() {
  const summary = {
    bounces_dead: 0,
    emails_nulled: 0,
    queued_cal_warm: 0,
    queued_cal_nurture: 0,
    stale_reset: 0,
    errors: [],
  };

  try { await ensureCalQueueTable(); }
  catch (err) {
    summary.errors.push({ step: 'ensure_cal_queue', message: err.message });
    console.error('[Max] cal_queue bootstrap failed:', err.message);
  }

  const steps = [
    ['bounces_dead',        runMarkBouncesDead],
    ['emails_nulled',       runNullGenericEmails],
    ['queued_cal_warm',     runQueueWarmForCal],
    ['queued_cal_nurture',  runHandoff5TouchNoReply],
    ['stale_reset',         runResetStaleSequences],
  ];

  for (const [key, fn] of steps) {
    try {
      summary[key] = await fn();
    } catch (err) {
      summary.errors.push({ step: key, message: err.message });
      console.error(`[Max] auto-action ${key} failed:`, err.message);
    }
  }

  await insertAgentLog('auto_execute_summary', summary,
    summary.errors.length ? 'partial' : 'success'
  );
  return summary;
}

function formatAutoExecSummary(autoExec) {
  if (!autoExec) return 'No auto-executed actions this run.';
  const parts = [];
  if (autoExec.bounces_dead)       parts.push(`${autoExec.bounces_dead} hard-bounce prospect${autoExec.bounces_dead === 1 ? '' : 's'} marked dead`);
  if (autoExec.emails_nulled)      parts.push(`${autoExec.emails_nulled} generic inbox email${autoExec.emails_nulled === 1 ? '' : 's'} nulled`);
  if (autoExec.queued_cal_warm)    parts.push(`${autoExec.queued_cal_warm} warm prospect${autoExec.queued_cal_warm === 1 ? '' : 's'} queued for Cal`);
  if (autoExec.queued_cal_nurture) parts.push(`${autoExec.queued_cal_nurture} 5+ touch no-reply prospect${autoExec.queued_cal_nurture === 1 ? '' : 's'} queued for Cal nurture`);
  if (autoExec.stale_reset)        parts.push(`${autoExec.stale_reset} stale sequence${autoExec.stale_reset === 1 ? '' : 's'} reset to cold`);
  if (!parts.length) parts.push('No auto-executable items today.');
  return parts.join('; ');
}

async function runAutonomousTriggers() {
  const summary = { reengagement: 0, marked_dead: 0, paige_regenerate: 0, copy_review: 0, errors: [] };

  try {
    summary.reengagement = await runReengagementTrigger();
  } catch (err) {
    summary.errors.push({ trigger: 'reengagement', message: err.message });
    console.error('[Max] Trigger 1 failed:', err.message);
  }

  try {
    summary.marked_dead = await runMarkSequenceDeadTrigger();
  } catch (err) {
    summary.errors.push({ trigger: 'auto_marked_dead', message: err.message });
    console.error('[Max] Trigger 2 failed:', err.message);
  }

  try {
    summary.paige_regenerate = await runPaigeQualityGateTrigger();
  } catch (err) {
    summary.errors.push({ trigger: 'paige_regenerate', message: err.message });
    console.error('[Max] Trigger 3 failed:', err.message);
  }

  try {
    summary.copy_review = await runCopyReviewTrigger();
  } catch (err) {
    summary.errors.push({ trigger: 'copy_review', message: err.message });
    console.error('[Max] Trigger 4 failed:', err.message);
  }

  await insertAgentLog('autonomous_triggers_summary', {
    count: summary.reengagement + summary.marked_dead + summary.paige_regenerate + summary.copy_review,
    reengagement: summary.reengagement,
    marked_dead: summary.marked_dead,
    paige_regenerate: summary.paige_regenerate,
    copy_review: summary.copy_review,
    errors: summary.errors,
  }, summary.errors.length ? 'partial' : 'success');

  console.log('[Max] Autonomous triggers complete:', summary);
}

async function run(args = {}) {
  const triggeredBy = args.triggered_by || args.triggeredBy || 'unspecified';
  const requestedClientId = args.client_id ?? args.clientId ?? null;
  const startedAt = new Date().toISOString();
  console.log(`\n[Max] run() invoked at ${startedAt} triggered_by=${triggeredBy} requested_client_id=${requestedClientId ?? '(none)'} effective_client_id=${CLIENT_ID}\n`);
  if (requestedClientId != null && Number(requestedClientId) !== Number(CLIENT_ID)) {
    console.warn(`[Max] client_id arg (${requestedClientId}) does not match module CLIENT_ID (${CLIENT_ID}) — the route should have reset ACTIVE_CLIENT_ID and reloaded the module before calling run().`);
  }

  const result = {
    triggered_by: triggeredBy,
    client_id: CLIENT_ID,
    started_at: startedAt,
    auto_exec: null,
    digest: { generated: false, sent: false, length: 0, error: null },
    errors: [],
  };

  try {
    CLIENT_CONFIG = await getClientConfig(CLIENT_ID);
    if (!CLIENT_CONFIG) throw new Error(`Active client not found: ${CLIENT_ID}`);

    console.log('Running auto-execute actions...');
    result.auto_exec = await runAutoExecuteActions();
    console.log('[Max] Auto-execute summary:', result.auto_exec);

    console.log('Reading second brain...');
    const snapshot = await getSystemSnapshot();

    let expansionReport = null;
    try {
      console.log('Loading scout expansion report...');
      expansionReport = await getExpansionReport(CLIENT_ID);
    } catch (err) {
      console.error('[Max] scout expansion report error:', err.message);
      result.errors.push({ step: 'scout_expansion_report', message: err.message });
    }

    // Wrap the digest generation block on its own so a failure here writes a
    // visible `digest_generation_failed` row to agent_log (and surfaces on
    // dashboard_trigger.result) instead of being silently swallowed by the
    // outer catch. The auto-execute side effects above are already committed
    // and we still want createActions / runAutonomousTriggers to run.
    try {
      console.log('Generating insights with Claude...');
      const rawInsights = await generateInsights(snapshot, result.auto_exec);
      const insights = await verifyDigestProspects(rawInsights, snapshot);

      if (typeof insights !== 'string' || !insights.trim()) {
        throw new Error(`Digest generation returned ${insights === null ? 'null' : typeof insights} — refusing to send empty digest`);
      }
      result.digest.generated = true;
      result.digest.length = insights.length;

      console.log('\n--- DIGEST PREVIEW ---');
      console.log(insights);
      console.log('--- END PREVIEW ---\n');

      console.log('Sending digest...');
      result.digest.sent = await sendDigest(insights, snapshot, expansionReport);

      await logAgentRun(insights);
    } catch (err) {
      result.digest.error = err.message;
      result.errors.push({ step: 'digest_generation', message: err.message });
      console.error('[Max] digest generation failed:', err.message);
      console.error(err.stack);
      try {
        await pool.query(`
          INSERT INTO agent_log (agent_name, action, payload, status, error_msg, ran_at, client_id)
          VALUES ($1, 'digest_generation_failed', $2, 'failed', $3, NOW(), $4)
        `, [
          AGENT_NAME,
          JSON.stringify({
            error: err.message,
            stack: (err.stack || '').slice(0, 2000),
            auto_exec: result.auto_exec,
            client_id: CLIENT_ID,
          }),
          err.message,
          CLIENT_ID,
        ]);
      } catch (logErr) {
        console.error('[Max] could not write digest_generation_failed log:', logErr.message);
      }
    }

    console.log('Creating action items...');
    await createActions(snapshot);

    console.log('Running autonomous triggers...');
    await runAutonomousTriggers();

    console.log('\nMax complete.');
  } catch (err) {
    console.error('Max error:', err.message);
    result.errors.push({ step: 'run', message: err.message });
  }

  return result;
}

module.exports = { run };

if (require.main === module) {
  run().catch(err => {
    console.error('[Max] Fatal error:', err.message);
    process.exit(1);
  });
}
