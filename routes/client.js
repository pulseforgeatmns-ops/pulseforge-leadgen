const express = require('express');
const path = require('path');
const pool = require('../db');
const {
  publishBlogPost,
} = require('../utils/blogPublisher');
const {
  publishToGoogleBusiness,
  publishToFacebookPage,
  publishFayeComment,
  publishToLinkedInPage,
  publishLinkComment,
} = require('../utils/publishPipeline');

const router = express.Router();

const CLIENT_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS clients (
    id SERIAL PRIMARY KEY,
    name TEXT,
    business_name TEXT,
    vertical TEXT,
    pin TEXT,
    email TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
  )
`;

const CLEANING_AGENTS = ['scout', 'emmett', 'riley', 'paige', 'vera', 'max'];
const AGENT_META = {
  scout:  { name: 'Scout',  label: 'SCOUT',  role: 'Lead Scraper',        icon: '🔍', color: '#00d4b4', ringOffset: 25 },
  emmett: { name: 'Emmett', label: 'EMMETT', role: 'Email Agent',         icon: '✉️', color: '#ff6b35', ringOffset: 188 },
  riley:  { name: 'Riley',  label: 'RILEY',  role: 'Receptionist Agent',  icon: '🙋', color: '#64c864', ringOffset: 200 },
  paige:  { name: 'Paige',  label: 'PAIGE',  role: 'Content Agent',       icon: '✍️', color: '#00c896', ringOffset: 230 },
  vera:   { name: 'Vera',   label: 'VERA',   role: 'Review Agent',        icon: '⭐', color: '#f4b942', ringOffset: 230 },
  max:    { name: 'Max',    label: 'MAX',    role: 'Manager Agent',       icon: '🧠', color: '#8b5cf6', ringOffset: 12 },
};
const AGENT_ALIASES = {
  scout: 'scout',
  scout_agent: 'scout',
  email: 'emmett',
  email_agent: 'emmett',
  emmett: 'emmett',
  emmett_agent: 'emmett',
  riley: 'riley',
  riley_agent: 'riley',
  paige: 'paige',
  paige_agent: 'paige',
  vera: 'vera',
  vera_agent: 'vera',
  max: 'max',
  max_agent: 'max',
};
const PREVIEW_CLIENT = {
  id: 'preview',
  name: 'Jordan Rivera',
  business_name: 'Manchester Clean Co.',
  vertical: 'commercial cleaning',
  email: 'owner@manchestercleanco.example',
};

let clientsTableReady = false;

async function ensureClientsTable() {
  if (clientsTableReady) return;
  await pool.query(CLIENT_TABLE_SQL);
  clientsTableReady = true;
}

function normalizeAgentName(name = '') {
  const cleaned = String(name).toLowerCase().replace(/[^a-z0-9_]/g, '');
  return AGENT_ALIASES[cleaned] || AGENT_ALIASES[cleaned.replace(/_agent$/, '')] || cleaned.replace(/_agent$/, '');
}

function clientAgentsFor(vertical = '') {
  const v = vertical.toLowerCase();
  if (v.includes('clean')) return CLEANING_AGENTS;
  return CLEANING_AGENTS;
}

async function getClient(clientId) {
  await ensureClientsTable();
  if (!/^\d+$/.test(String(clientId))) return null;

  const { rows } = await pool.query(
    `SELECT id, name, business_name, vertical, email, pin
     FROM clients
     WHERE id = $1
     LIMIT 1`,
    [clientId]
  );
  return rows[0] || null;
}

function publicClient(client) {
  return {
    id: client.id,
    name: client.name,
    business_name: client.business_name,
    vertical: client.vertical,
    email: client.email,
  };
}

function isClientAuthed(req, clientId) {
  return Boolean(req.session?.clients?.[clientId]);
}

async function requireClient(req, res, next) {
  try {
    const client = await getClient(req.params.clientId);
    if (!client) return res.status(404).json({ error: 'Client not found' });
    if (!isClientAuthed(req, String(client.id))) {
      return res.status(401).json({ error: 'PIN required' });
    }
    req.client = client;
    next();
  } catch (err) {
    console.error('[client] auth error:', err.message);
    res.status(500).json({ error: 'Unable to load client' });
  }
}

function prospectVerticalClause(alias = 'p') {
  return `LOWER(TRIM(COALESCE(${alias}.vertical, ''))) = LOWER(TRIM($1))`;
}

function companyVerticalClause(alias = 'c') {
  return `LOWER(TRIM(COALESCE(${alias}.industry, ''))) = LOWER(TRIM($1))`;
}

function approvalVerticalClause() {
  return `(
    LOWER(TRIM(COALESCE(pc.author_title, ''))) = LOWER(TRIM($1))
    OR LOWER(TRIM(COALESCE(c.industry, ''))) = LOWER(TRIM($1))
  )`;
}

function prospectName(row) {
  const person = `${row.first_name || ''} ${row.last_name || ''}`.trim();
  const notesBusiness = (row.notes || '').split('—')[0].trim();
  return person || notesBusiness || row.company_name || 'Prospect';
}

function relColor(channel = '') {
  if (channel === 'email') return 'fi-o';
  if (['google_business', 'gbp', 'google_review'].includes(channel)) return 'fi-g';
  if (['facebook', 'facebook_page', 'linkedin', 'linkedin_page'].includes(channel)) return 'fi-p';
  return 'fi-t';
}

function previewNow() {
  const now = Date.now();
  return {
    hoursAgo: hours => new Date(now - hours * 60 * 60 * 1000).toISOString(),
    daysAgo: days => new Date(now - days * 24 * 60 * 60 * 1000).toISOString(),
  };
}

function previewPayload() {
  const { hoursAgo, daysAgo } = previewNow();
  const agents = clientAgentsFor(PREVIEW_CLIENT.vertical).map((key, index) => ({
    key,
    ...AGENT_META[key],
    active: index < 3,
    status: 'success',
    last_run: index < 3 ? hoursAgo(2 + index) : daysAgo(index),
    week_runs: [7, 5, 9, 2, 1, 6][index],
    ring: [0.9, 0.72, 0.8, 0.35, 0.2, 1][index],
  }));
  const recent = [
    { channel: 'email', action_type: 'email_opened', prospect: 'Avery Facilities', content_summary: 'Opened spring cleaning follow-up', created_at: hoursAgo(2) },
    { channel: 'email', action_type: 'outbound', prospect: 'North Elm Dental', content_summary: 'Sent Day 4 office cleaning sequence', created_at: hoursAgo(5) },
    { channel: 'google_business', action_type: 'published_post', prospect: 'Manchester Clean Co.', content_summary: 'Published Google Business Profile update', created_at: hoursAgo(8) },
    { channel: 'email', action_type: 'email_clicked', prospect: 'Bedford Property Group', content_summary: 'Clicked quote request link', created_at: daysAgo(1) },
    { channel: 'linkedin_page', action_type: 'published_post', prospect: 'Manchester Clean Co.', content_summary: 'Published LinkedIn credibility post', created_at: daysAgo(2) },
    { channel: 'facebook_page', action_type: 'published_post', prospect: 'Manchester Clean Co.', content_summary: 'Published service-area post', created_at: daysAgo(3) },
  ];

  return {
    client: publicClient(PREVIEW_CLIENT),
    agents,
    stats: {
      prospects_this_month: 42,
      emails_sent_this_week: 118,
      open_rate: 38.6,
      opened_contacts: 27,
      emailed_contacts: 70,
      pending_approvals: 3,
      content_published_week: 6,
      total_touchpoints: 284,
      total_prospects: 96,
    },
    recent_touchpoints: recent,
    feed: recent.map(item => ({
      agent: item.channel === 'email' ? 'Emmett' : 'Paige',
      action: `${item.action_type.replace(/_/g, ' ')} · ${item.prospect}`,
      time: item.created_at,
      icon: item.channel === 'email' ? '✉️' : '✍️',
      color: relColor(item.channel),
    })),
    content_published_this_week: { google_business: 2, facebook_page: 3, linkedin_page: 1 },
    max_brief: 'Pipeline momentum is healthy this week. Emmett is seeing above-average opens from office and property management prospects, and Paige has three client posts waiting for approval before publishing.',
    generated_at: new Date().toISOString(),
  };
}

function previewProspects() {
  const { hoursAgo, daysAgo } = previewNow();
  return [
    { id: 'p1', business: 'Avery Facilities', first_name: 'Maya', last_name: 'Avery', email: 'maya@example.com', phone: '(603) 555-0144', status: 'warm', icp_score: 86, last_contacted_at: hoursAgo(2), touchpoint_count: 8, created_at: daysAgo(3), notes: 'Avery Facilities — property manager' },
    { id: 'p2', business: 'North Elm Dental', first_name: 'Chris', last_name: 'Cole', email: 'chris@example.com', phone: '(603) 555-0191', status: 'cold', icp_score: 72, last_contacted_at: hoursAgo(5), touchpoint_count: 4, created_at: daysAgo(5), notes: 'North Elm Dental — office cleaning' },
    { id: 'p3', business: 'Bedford Property Group', first_name: 'Dana', last_name: 'Lee', email: 'dana@example.com', phone: '', status: 'warm', icp_score: 91, last_contacted_at: daysAgo(1), touchpoint_count: 6, created_at: daysAgo(8), notes: 'Bedford Property Group — clicked quote link' },
  ];
}

function previewApprovals() {
  const { hoursAgo } = previewNow();
  return [
    { id: 'a1', author_name: PREVIEW_CLIENT.business_name, author_title: PREVIEW_CLIENT.vertical, channel: 'google_business', post_content: 'Google Business · educational', comment: 'A clean office is not just about appearances. It helps teams start the day focused, keeps shared spaces healthier, and gives visitors confidence the moment they walk in.', created_at: hoursAgo(2) },
    { id: 'a2', author_name: PREVIEW_CLIENT.business_name, author_title: PREVIEW_CLIENT.vertical, channel: 'facebook_page', post_content: 'Facebook Page · seasonal', comment: 'Spring dust has a way of finding every corner. Our team is helping Manchester offices reset their spaces with detailed floor, restroom, and common-area cleaning this month.', created_at: hoursAgo(5) },
    { id: 'a3', author_name: PREVIEW_CLIENT.business_name, author_title: PREVIEW_CLIENT.vertical, channel: 'linkedin_page', post_content: 'LinkedIn Page · credibility', comment: 'Facility maintenance gets easier when cleaning is consistent, documented, and handled before small issues become distractions for the team.', created_at: hoursAgo(8) },
  ];
}

function previewAnalytics() {
  return {
    outbound_volume: [
      { date: '2026-05-06', email: 18, sms: 0 },
      { date: '2026-05-07', email: 22, sms: 0 },
      { date: '2026-05-08', email: 16, sms: 0 },
      { date: '2026-05-09', email: 20, sms: 0 },
      { date: '2026-05-10', email: 14, sms: 0 },
      { date: '2026-05-11', email: 21, sms: 0 },
      { date: '2026-05-12', email: 7, sms: 0 },
    ],
    reply_rate: [
      { week: 'Apr 20', outbound: 82, inbound: 4, rate: 4.9 },
      { week: 'Apr 27', outbound: 96, inbound: 7, rate: 7.3 },
      { week: 'May 04', outbound: 118, inbound: 10, rate: 8.5 },
    ],
    icp_distribution: [
      { bucket: '61-80', count: 31 },
      { bucket: '81-100', count: 18 },
      { bucket: '41-60', count: 11 },
    ],
    pipeline_funnel: [
      { stage: 'Cold', count: 61 },
      { stage: 'Warm', count: 23 },
      { stage: 'Hot', count: 4 },
    ],
    top_prospects: previewProspects(),
    channel_performance: [
      { channel: 'Google Business', posts: 2, engagement_rate: 4.2 },
      { channel: 'Facebook Page', posts: 3, engagement_rate: 3.6 },
      { channel: 'LinkedIn Page', posts: 1, engagement_rate: 5.1 },
    ],
    email: {
      sent_week: 118,
      opened_week: 52,
      clicked_week: 9,
      open_rate_week: 44.1,
      click_rate_week: 7.6,
    },
  };
}

async function buildDashboardData(client) {
  const vertical = client.vertical || '';
  const allowed = clientAgentsFor(vertical);

  const [
    prospectsMonth,
    emailsWeek,
    emailEngagement,
    totalProspects,
    totalTouchpoints,
    pendingApprovals,
    recentTouchpoints,
    contentWeek,
    agents,
  ] = await Promise.all([
    pool.query(`SELECT COUNT(*)::int AS count FROM prospects p WHERE ${prospectVerticalClause('p')} AND p.created_at >= DATE_TRUNC('month', NOW())`, [vertical]),
    pool.query(`
      SELECT COUNT(*)::int AS count
      FROM touchpoints t JOIN prospects p ON p.id = t.prospect_id
      WHERE ${prospectVerticalClause('p')} AND t.channel = 'email'
        AND t.action_type IN ('outbound', 'email_warm')
        AND t.created_at >= NOW() - INTERVAL '7 days'
    `, [vertical]),
    pool.query(`
      WITH emailed AS (
        SELECT DISTINCT p.id FROM prospects p JOIN touchpoints t ON t.prospect_id = p.id
        WHERE ${prospectVerticalClause('p')} AND t.channel = 'email' AND t.action_type IN ('outbound', 'email_warm')
      ),
      opened AS (
        SELECT DISTINCT p.id FROM prospects p JOIN touchpoints t ON t.prospect_id = p.id
        WHERE ${prospectVerticalClause('p')} AND t.channel = 'email' AND t.action_type = 'email_opened'
      )
      SELECT (SELECT COUNT(*)::int FROM emailed) AS emailed_count, (SELECT COUNT(*)::int FROM opened) AS opened_count
    `, [vertical]),
    pool.query(`SELECT COUNT(*)::int AS count FROM prospects p WHERE ${prospectVerticalClause('p')}`, [vertical]),
    pool.query(`SELECT COUNT(*)::int AS count FROM touchpoints t JOIN prospects p ON p.id = t.prospect_id WHERE ${prospectVerticalClause('p')}`, [vertical]),
    pool.query(`
      SELECT COUNT(*)::int AS count
      FROM pending_comments pc
      LEFT JOIN companies c ON LOWER(TRIM(c.name)) = LOWER(TRIM(pc.author_name))
      WHERE pc.status = 'pending' AND ${approvalVerticalClause()}
    `, [vertical]),
    pool.query(`
      SELECT t.channel, t.action_type, t.content_summary, t.outcome, t.sentiment, t.created_at,
        p.first_name, p.last_name, p.notes, c.name AS company_name
      FROM touchpoints t
      JOIN prospects p ON p.id = t.prospect_id
      LEFT JOIN companies c ON c.id = p.company_id
      WHERE ${prospectVerticalClause('p')}
      ORDER BY t.created_at DESC
      LIMIT 10
    `, [vertical]),
    pool.query(`
      SELECT pa.channel, COUNT(*)::int AS count
      FROM post_analytics pa
      LEFT JOIN companies c ON c.id = pa.company_id
      WHERE pa.published_at >= DATE_TRUNC('week', NOW()) AND ${companyVerticalClause('c')}
      GROUP BY pa.channel
    `, [vertical]),
    pool.query(`
      WITH latest AS (
        SELECT DISTINCT ON (LOWER(REPLACE(agent_name, '_agent', '')))
          LOWER(REPLACE(agent_name, '_agent', '')) AS raw_agent,
          status,
          ran_at
        FROM agent_log
        ORDER BY LOWER(REPLACE(agent_name, '_agent', '')), ran_at DESC
      ),
      week_runs AS (
        SELECT LOWER(REPLACE(agent_name, '_agent', '')) AS raw_agent, COUNT(*)::int AS runs
        FROM agent_log
        WHERE ran_at >= NOW() - INTERVAL '7 days'
        GROUP BY LOWER(REPLACE(agent_name, '_agent', ''))
      )
      SELECT l.raw_agent, l.status, l.ran_at, COALESCE(w.runs, 0)::int AS week_runs
      FROM latest l
      LEFT JOIN week_runs w ON w.raw_agent = l.raw_agent
    `),
  ]);

  const engagement = emailEngagement.rows[0] || {};
  const emailedCount = Number(engagement.emailed_count || 0);
  const openedCount = Number(engagement.opened_count || 0);
  const openRate = emailedCount > 0 ? +((openedCount / emailedCount) * 100).toFixed(1) : 0;
  const runRows = new Map();
  for (const row of agents.rows) {
    const key = normalizeAgentName(row.raw_agent);
    if (allowed.includes(key)) runRows.set(key, row);
  }

  const agentRows = allowed.map(key => {
    const row = runRows.get(key) || {};
    const meta = AGENT_META[key];
    const active = row.status === 'success' && row.ran_at && new Date(row.ran_at) > new Date(Date.now() - 24 * 60 * 60 * 1000);
    return {
      key,
      ...meta,
      active: Boolean(active),
      status: row.status || 'standby',
      last_run: row.ran_at || null,
      week_runs: Number(row.week_runs || 0),
      ring: Math.min(Number(row.week_runs || 0) / 10, 1),
    };
  });

  const recent = recentTouchpoints.rows.map(row => ({
    channel: row.channel,
    action_type: row.action_type,
    content_summary: row.content_summary,
    outcome: row.outcome,
    sentiment: row.sentiment,
    created_at: row.created_at,
    prospect: prospectName(row),
  }));
  const content = {};
  for (const row of contentWeek.rows) content[row.channel] = Number(row.count || 0);

  return {
    client: publicClient(client),
    agents: agentRows,
    stats: {
      prospects_this_month: Number(prospectsMonth.rows[0]?.count || 0),
      emails_sent_this_week: Number(emailsWeek.rows[0]?.count || 0),
      open_rate: openRate,
      opened_contacts: openedCount,
      emailed_contacts: emailedCount,
      pending_approvals: Number(pendingApprovals.rows[0]?.count || 0),
      content_published_week: Object.values(content).reduce((sum, count) => sum + count, 0),
      total_touchpoints: Number(totalTouchpoints.rows[0]?.count || 0),
      total_prospects: Number(totalProspects.rows[0]?.count || 0),
    },
    recent_touchpoints: recent,
    feed: recent.slice(0, 8).map(item => ({
      agent: item.channel === 'email' ? 'Emmett' : item.channel === 'google_review' ? 'Vera' : 'Paige',
      action: `${String(item.action_type || '').replace(/_/g, ' ')} · ${item.prospect}`,
      time: item.created_at,
      icon: item.channel === 'email' ? '✉️' : item.channel === 'google_review' ? '⭐' : '✍️',
      color: relColor(item.channel),
    })),
    content_published_this_week: content,
    max_brief: `Your ${vertical || 'client'} pipeline is being monitored. This view is filtered to client activity only: prospects, content, email engagement, and approvals.`,
    generated_at: new Date().toISOString(),
  };
}

async function getProspects(vertical) {
  const { rows } = await pool.query(`
    SELECT p.id, p.first_name, p.last_name, p.email, p.phone, p.status, p.icp_score, p.notes,
      p.last_contacted_at, p.created_at, c.name AS company_name, COUNT(t.id)::int AS touchpoint_count
    FROM prospects p
    LEFT JOIN companies c ON p.company_id = c.id
    LEFT JOIN touchpoints t ON t.prospect_id = p.id
    WHERE p.do_not_contact = false AND ${prospectVerticalClause('p')}
    GROUP BY p.id, c.name
    ORDER BY p.icp_score DESC NULLS LAST
    LIMIT 200
  `, [vertical]);
  return rows.map(row => ({
    ...row,
    business: prospectName(row),
  }));
}

async function getActivity(vertical, limit = 50) {
  const { rows } = await pool.query(`
    SELECT t.id, t.channel, t.action_type, t.content_summary, t.outcome, t.created_at,
      p.first_name, p.last_name, p.notes, c.name AS company_name
    FROM touchpoints t
    JOIN prospects p ON p.id = t.prospect_id
    LEFT JOIN companies c ON c.id = p.company_id
    WHERE ${prospectVerticalClause('p')}
    ORDER BY t.created_at DESC
    LIMIT $2
  `, [vertical, limit]);
  return rows.map(row => ({
    id: row.id,
    agent: row.channel === 'email' ? 'Emmett' : row.channel === 'google_review' ? 'Vera' : 'Paige',
    channel: row.channel,
    action: row.action_type,
    summary: row.content_summary || row.outcome,
    prospect: prospectName(row),
    created_at: row.created_at,
    color: relColor(row.channel),
  }));
}

async function getApprovals(vertical) {
  const { rows } = await pool.query(`
    SELECT pc.id, pc.author_name, pc.author_title, pc.post_content, pc.comment, pc.channel, pc.status, pc.created_at
    FROM pending_comments pc
    LEFT JOIN companies c ON LOWER(TRIM(c.name)) = LOWER(TRIM(pc.author_name))
    WHERE pc.status = 'pending' AND ${approvalVerticalClause()}
    ORDER BY pc.created_at DESC
    LIMIT 100
  `, [vertical]);
  return rows;
}

async function getAnalytics(vertical) {
  const [volume, reply, icp, funnel, topProspects, content, email] = await Promise.all([
    pool.query(`
      SELECT DATE(t.created_at)::text AS date, t.channel, COUNT(*)::int AS count
      FROM touchpoints t JOIN prospects p ON p.id = t.prospect_id
      WHERE ${prospectVerticalClause('p')} AND t.channel IN ('email','sms')
        AND t.action_type IN ('outbound', 'email_warm') AND t.created_at >= NOW() - INTERVAL '30 days'
      GROUP BY DATE(t.created_at), t.channel ORDER BY date ASC
    `, [vertical]),
    pool.query(`
      SELECT DATE_TRUNC('week', t.created_at)::date::text AS week, t.action_type, COUNT(*)::int AS count
      FROM touchpoints t JOIN prospects p ON p.id = t.prospect_id
      WHERE ${prospectVerticalClause('p')} AND t.channel = 'email' AND t.created_at >= NOW() - INTERVAL '56 days'
      GROUP BY DATE_TRUNC('week', t.created_at), t.action_type ORDER BY week ASC
    `, [vertical]),
    pool.query(`
      SELECT CASE
        WHEN icp_score IS NULL THEN 'Unknown'
        WHEN icp_score BETWEEN 0 AND 20 THEN '0-20'
        WHEN icp_score BETWEEN 21 AND 40 THEN '21-40'
        WHEN icp_score BETWEEN 41 AND 60 THEN '41-60'
        WHEN icp_score BETWEEN 61 AND 80 THEN '61-80'
        ELSE '81-100'
      END AS bucket, COUNT(*)::int AS count
      FROM prospects p WHERE p.do_not_contact = false AND ${prospectVerticalClause('p')}
      GROUP BY bucket
    `, [vertical]),
    pool.query(`SELECT status AS stage, COUNT(*)::int AS count FROM prospects p WHERE ${prospectVerticalClause('p')} GROUP BY status`, [vertical]),
    pool.query(`
      SELECT p.id, p.status, p.icp_score, p.last_contacted_at, p.first_name, p.last_name, p.notes,
        c.name AS company_name, COUNT(t.id)::int AS touchpoint_count
      FROM prospects p
      LEFT JOIN companies c ON p.company_id = c.id
      LEFT JOIN touchpoints t ON t.prospect_id = p.id
      WHERE p.do_not_contact = false AND ${prospectVerticalClause('p')}
      GROUP BY p.id, c.name
      ORDER BY COUNT(t.id) DESC, p.icp_score DESC NULLS LAST
      LIMIT 10
    `, [vertical]),
    pool.query(`
      SELECT pa.channel, COUNT(*)::int AS posts, AVG(pa.engagement_rate)::float AS engagement_rate
      FROM post_analytics pa
      LEFT JOIN companies c ON c.id = pa.company_id
      WHERE ${companyVerticalClause('c')} AND pa.published_at >= NOW() - INTERVAL '30 days'
      GROUP BY pa.channel
      ORDER BY posts DESC
    `, [vertical]),
    pool.query(`
      SELECT
        COUNT(CASE WHEN t.action_type IN ('outbound', 'email_warm') THEN 1 END)::int AS sent_week,
        COUNT(CASE WHEN t.action_type = 'email_opened' THEN 1 END)::int AS opened_week,
        COUNT(CASE WHEN t.action_type = 'email_clicked' THEN 1 END)::int AS clicked_week
      FROM touchpoints t JOIN prospects p ON p.id = t.prospect_id
      WHERE ${prospectVerticalClause('p')} AND t.channel = 'email' AND t.created_at >= NOW() - INTERVAL '7 days'
    `, [vertical]),
  ]);

  const weekMap = {};
  for (const row of reply.rows) {
    weekMap[row.week] = weekMap[row.week] || { week: row.week, outbound: 0, inbound: 0, rate: 0 };
    if (['outbound', 'email_warm'].includes(row.action_type)) weekMap[row.week].outbound += row.count;
    if (['inbound_reply', 'reply', 'email_clicked'].includes(row.action_type)) weekMap[row.week].inbound += row.count;
  }
  const replyRate = Object.values(weekMap).map(row => ({
    ...row,
    rate: row.outbound > 0 ? +((row.inbound / row.outbound) * 100).toFixed(1) : 0,
  }));
  const e = email.rows[0] || {};
  const sent = Number(e.sent_week || 0);
  const opened = Number(e.opened_week || 0);
  const clicked = Number(e.clicked_week || 0);

  return {
    outbound_volume: volume.rows,
    reply_rate: replyRate,
    icp_distribution: icp.rows,
    pipeline_funnel: funnel.rows,
    top_prospects: topProspects.rows.map(row => ({ ...row, business: prospectName(row) })),
    channel_performance: content.rows,
    email: {
      sent_week: sent,
      opened_week: opened,
      clicked_week: clicked,
      open_rate_week: sent > 0 ? +((opened / sent) * 100).toFixed(1) : 0,
      click_rate_week: sent > 0 ? +((clicked / sent) * 100).toFixed(1) : 0,
    },
  };
}

router.get('/preview', (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'client-dashboard.html'));
});

router.get('/preview/api/session', (req, res) => {
  res.json({ authenticated: true, client: publicClient(PREVIEW_CLIENT) });
});

router.post('/preview/login', (req, res) => {
  res.json({ success: true });
});

router.post('/preview/logout', (req, res) => {
  res.json({ success: true });
});

router.get('/preview/api/dashboard', (req, res) => {
  res.json(previewPayload());
});

router.get('/preview/api/prospects', (req, res) => {
  res.json(previewProspects());
});

router.get('/preview/api/activity', (req, res) => {
  res.json(previewPayload().recent_touchpoints.map((item, index) => ({
    id: `act-${index}`,
    agent: item.channel === 'email' ? 'Emmett' : 'Paige',
    channel: item.channel,
    action: item.action_type,
    summary: item.content_summary,
    prospect: item.prospect,
    created_at: item.created_at,
    color: relColor(item.channel),
  })));
});

router.get('/preview/api/approvals', (req, res) => {
  res.json(previewApprovals());
});

router.post('/preview/api/approvals/:id', (req, res) => {
  res.json({ success: true, id: req.params.id, action: req.body.action });
});

router.get('/preview/api/analytics', (req, res) => {
  res.json(previewAnalytics());
});

router.get('/:clientId', async (req, res) => {
  try {
    const client = await getClient(req.params.clientId);
    if (!client) return res.status(404).send('Client not found');
    res.sendFile(path.join(__dirname, '..', 'public', 'client-dashboard.html'));
  } catch (err) {
    console.error('[client] page error:', err.message);
    res.status(500).send('Unable to load client dashboard');
  }
});

router.get('/:clientId/api/session', async (req, res) => {
  try {
    const client = await getClient(req.params.clientId);
    if (!client) return res.status(404).json({ error: 'Client not found' });
    res.json({ authenticated: isClientAuthed(req, String(client.id)), client: publicClient(client) });
  } catch (err) {
    console.error('[client] session error:', err.message);
    res.status(500).json({ error: 'Unable to load session' });
  }
});

router.post('/:clientId/login', async (req, res) => {
  try {
    const client = await getClient(req.params.clientId);
    if (!client) return res.status(404).json({ error: 'Client not found' });

    const pin = String(req.body.pin || '').trim();
    if (!/^\d{4}$/.test(pin) || !client.pin || pin !== String(client.pin).trim()) {
      return res.status(401).json({ error: 'Invalid PIN' });
    }

    req.session.clients = req.session.clients || {};
    req.session.clients[String(client.id)] = true;
    res.json({ success: true });
  } catch (err) {
    console.error('[client] login error:', err.message);
    res.status(500).json({ error: 'Unable to verify PIN' });
  }
});

router.post('/:clientId/logout', requireClient, (req, res) => {
  delete req.session.clients[String(req.client.id)];
  res.json({ success: true });
});

router.get('/:clientId/api/dashboard', requireClient, async (req, res) => {
  try {
    res.json(await buildDashboardData(req.client));
  } catch (err) {
    console.error('[client] dashboard error:', err.message);
    res.status(500).json({ error: 'Unable to load client dashboard' });
  }
});

router.get('/:clientId/api/prospects', requireClient, async (req, res) => {
  try {
    res.json(await getProspects(req.client.vertical || ''));
  } catch (err) {
    console.error('[client] prospects error:', err.message);
    res.status(500).json({ error: 'Unable to load prospects' });
  }
});

router.get('/:clientId/api/prospects/:prospectId/touchpoints', requireClient, async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT t.channel, t.action_type, t.content_summary, t.outcome, t.created_at
      FROM touchpoints t
      JOIN prospects p ON p.id = t.prospect_id
      WHERE t.prospect_id = $2 AND ${prospectVerticalClause('p')}
      ORDER BY t.created_at ASC
    `, [req.client.vertical || '', req.params.prospectId]);
    res.json(rows);
  } catch (err) {
    console.error('[client] prospect touchpoints error:', err.message);
    res.status(500).json({ error: 'Unable to load touchpoints' });
  }
});

router.get('/:clientId/api/activity', requireClient, async (req, res) => {
  try {
    res.json(await getActivity(req.client.vertical || ''));
  } catch (err) {
    console.error('[client] activity error:', err.message);
    res.status(500).json({ error: 'Unable to load activity' });
  }
});

router.get('/:clientId/api/approvals', requireClient, async (req, res) => {
  try {
    res.json(await getApprovals(req.client.vertical || ''));
  } catch (err) {
    console.error('[client] approvals error:', err.message);
    res.status(500).json({ error: 'Unable to load approvals' });
  }
});

router.post('/:clientId/api/approvals/:id', requireClient, async (req, res) => {
  const { action } = req.body;
  if (!['approved', 'rejected'].includes(action)) {
    return res.status(400).json({ error: 'Invalid action' });
  }

  try {
    const { rows } = await pool.query(`
      SELECT pc.*
      FROM pending_comments pc
      LEFT JOIN companies c ON LOWER(TRIM(c.name)) = LOWER(TRIM(pc.author_name))
      WHERE pc.id = $2 AND pc.status = 'pending' AND ${approvalVerticalClause()}
      LIMIT 1
    `, [req.client.vertical || '', req.params.id]);
    const item = rows[0];
    if (!item) return res.status(404).json({ error: 'Approval not found' });

    await pool.query('UPDATE pending_comments SET status = $1 WHERE id = $2', [action, item.id]);
    res.json({ success: true, id: item.id, action });

    if (action === 'approved') {
      const publishers = {
        blog: () => publishBlogPost(item),
        google_business: () => publishToGoogleBusiness(item),
        facebook_page: () => publishToFacebookPage(item),
        facebook: () => publishFayeComment(item),
        linkedin_page: () => publishToLinkedInPage(item),
        linkedin: () => publishLinkComment(item),
      };
      const publish = publishers[item.channel];
      if (publish) publish().catch(err => console.error(`[ClientPublisher:${item.channel}]`, err.message));
    }
  } catch (err) {
    console.error('[client] approval update error:', err.message);
    res.status(500).json({ error: 'Unable to update approval' });
  }
});

router.get('/:clientId/api/analytics', requireClient, async (req, res) => {
  try {
    res.json(await getAnalytics(req.client.vertical || ''));
  } catch (err) {
    console.error('[client] analytics error:', err.message);
    res.status(500).json({ error: 'Unable to load analytics' });
  }
});

ensureClientsTable().catch(err => {
  console.error('[startup] clients table error:', err.message);
});

module.exports = router;
