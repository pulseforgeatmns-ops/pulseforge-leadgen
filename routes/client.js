const express = require('express');
const path = require('path');
const pool = require('../db');

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

const AGENT_LABELS = {
  scout: 'Scout',
  email: 'Emmett',
  emmett: 'Emmett',
  riley: 'Riley',
  paige: 'Paige',
  facebook: 'Faye',
  faye: 'Faye',
  linkedin: 'Link',
  link: 'Link',
  sam: 'Sam',
  cal: 'Cal',
  ivy: 'Ivy',
  vera: 'Vera',
  max: 'Max',
  rex: 'Rex',
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
  return String(name)
    .toLowerCase()
    .replace(/_agent$/, '')
    .replace(/[^a-z0-9_]/g, '');
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

function verticalClause(alias = 'p') {
  return `LOWER(TRIM(COALESCE(${alias}.vertical, ''))) = LOWER(TRIM($1))`;
}

function mapTouchpoint(row) {
  const person = `${row.first_name || ''} ${row.last_name || ''}`.trim();
  const notesBusiness = (row.notes || '').split('—')[0].trim();
  return {
    channel: row.channel,
    action_type: row.action_type,
    content_summary: row.content_summary,
    outcome: row.outcome,
    sentiment: row.sentiment,
    created_at: row.created_at,
    prospect: person || notesBusiness || row.company_name || 'Prospect',
  };
}

function previewDashboardData() {
  const now = Date.now();
  const daysAgo = days => new Date(now - days * 24 * 60 * 60 * 1000).toISOString();
  const hoursAgo = hours => new Date(now - hours * 60 * 60 * 1000).toISOString();

  return {
    client: PREVIEW_CLIENT,
    stats: {
      prospects_this_month: 42,
      emails_sent_this_week: 118,
      open_rate: 38.6,
      opened_contacts: 27,
      emailed_contacts: 70,
    },
    recent_touchpoints: [
      { channel: 'email', action_type: 'email_opened', prospect: 'Avery Facilities', content_summary: 'Opened spring cleaning follow-up', created_at: hoursAgo(2) },
      { channel: 'email', action_type: 'outbound', prospect: 'North Elm Dental', content_summary: 'Sent Day 4 office cleaning sequence', created_at: hoursAgo(5) },
      { channel: 'linkedin', action_type: 'generate_comment', prospect: 'Queen City Coworking', content_summary: 'Drafted local business owner comment', created_at: hoursAgo(8) },
      { channel: 'facebook', action_type: 'generate_comment', prospect: 'Millyard Fitness', content_summary: 'Prepared group reply for facility cleaning request', created_at: hoursAgo(12) },
      { channel: 'email', action_type: 'email_clicked', prospect: 'Bedford Property Group', content_summary: 'Clicked quote request link', created_at: daysAgo(1) },
      { channel: 'sms', action_type: 'send_sms', prospect: 'Granite State Office Suites', content_summary: 'Warm follow-up text queued', created_at: daysAgo(1) },
      { channel: 'email', action_type: 'outbound', prospect: 'Elm Street Accounting', content_summary: 'Sent Day 1 intro email', created_at: daysAgo(2) },
      { channel: 'gbp', action_type: 'published_post', prospect: 'Manchester Clean Co.', content_summary: 'Published Google Business Profile update', created_at: daysAgo(3) },
      { channel: 'linkedin', action_type: 'published_post', prospect: 'Manchester Clean Co.', content_summary: 'Published LinkedIn credibility post', created_at: daysAgo(4) },
      { channel: 'facebook', action_type: 'published_post', prospect: 'Manchester Clean Co.', content_summary: 'Published Facebook service-area post', created_at: daysAgo(5) },
    ],
    content_published_this_week: {
      gbp: 2,
      facebook: 3,
      linkedin: 1,
    },
    agents: [
      { key: 'scout', name: 'Scout', active: true, status: 'success', last_run: hoursAgo(3), week_runs: 7 },
      { key: 'emmett', name: 'Emmett', active: true, status: 'success', last_run: hoursAgo(5), week_runs: 5 },
      { key: 'riley', name: 'Riley', active: true, status: 'success', last_run: hoursAgo(2), week_runs: 9 },
      { key: 'paige', name: 'Paige', active: false, status: 'success', last_run: daysAgo(2), week_runs: 2 },
      { key: 'faye', name: 'Faye', active: false, status: 'success', last_run: daysAgo(1), week_runs: 3 },
      { key: 'link', name: 'Link', active: false, status: 'success', last_run: daysAgo(1), week_runs: 2 },
      { key: 'sam', name: 'Sam', active: false, status: 'standby', last_run: daysAgo(4), week_runs: 1 },
      { key: 'cal', name: 'Cal', active: false, status: 'standby', last_run: null, week_runs: 0 },
    ],
    generated_at: new Date().toISOString(),
  };
}

router.get('/preview', (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'client-dashboard.html'));
});

router.get('/preview/api/session', (req, res) => {
  res.json({
    authenticated: true,
    client: PREVIEW_CLIENT,
  });
});

router.post('/preview/login', (req, res) => {
  res.json({ success: true });
});

router.post('/preview/logout', (req, res) => {
  res.json({ success: true });
});

router.get('/preview/api/dashboard', (req, res) => {
  res.json(previewDashboardData());
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

    res.json({
      authenticated: isClientAuthed(req, String(client.id)),
      client: {
        id: client.id,
        name: client.name,
        business_name: client.business_name,
        vertical: client.vertical,
        email: client.email,
      },
    });
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
    if (!client.pin || pin !== String(client.pin).trim()) {
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
  const client = req.client;
  const vertical = client.vertical || '';

  try {
    const [
      prospectsMonth,
      emailsWeek,
      emailEngagement,
      recentTouchpoints,
      contentWeek,
      agents,
    ] = await Promise.all([
      pool.query(`
        SELECT COUNT(*)::int AS count
        FROM prospects p
        WHERE ${verticalClause('p')}
          AND p.created_at >= DATE_TRUNC('month', NOW())
      `, [vertical]),
      pool.query(`
        SELECT COUNT(*)::int AS count
        FROM touchpoints t
        JOIN prospects p ON p.id = t.prospect_id
        WHERE ${verticalClause('p')}
          AND t.channel = 'email'
          AND t.action_type IN ('outbound', 'email_warm')
          AND t.created_at >= NOW() - INTERVAL '7 days'
      `, [vertical]),
      pool.query(`
        WITH emailed AS (
          SELECT DISTINCT p.id
          FROM prospects p
          JOIN touchpoints t ON t.prospect_id = p.id
          WHERE ${verticalClause('p')}
            AND t.channel = 'email'
            AND t.action_type IN ('outbound', 'email_warm')
        ),
        opened AS (
          SELECT DISTINCT p.id
          FROM prospects p
          JOIN touchpoints t ON t.prospect_id = p.id
          WHERE ${verticalClause('p')}
            AND t.channel = 'email'
            AND t.action_type = 'email_opened'
        )
        SELECT
          (SELECT COUNT(*)::int FROM emailed) AS emailed_count,
          (SELECT COUNT(*)::int FROM opened) AS opened_count
      `, [vertical]),
      pool.query(`
        SELECT
          t.channel, t.action_type, t.content_summary, t.outcome, t.sentiment, t.created_at,
          p.first_name, p.last_name, p.notes,
          c.name AS company_name
        FROM touchpoints t
        JOIN prospects p ON p.id = t.prospect_id
        LEFT JOIN companies c ON c.id = p.company_id
        WHERE ${verticalClause('p')}
        ORDER BY t.created_at DESC
        LIMIT 10
      `, [vertical]),
      pool.query(`
        SELECT
          CASE
            WHEN pa.channel IN ('google_business', 'google_business_profile', 'gbp') THEN 'gbp'
            WHEN pa.channel IN ('facebook', 'facebook_page') THEN 'facebook'
            WHEN pa.channel IN ('linkedin', 'linkedin_page') THEN 'linkedin'
            ELSE pa.channel
          END AS platform,
          COUNT(*)::int AS count,
          MAX(pa.published_at) AS last_published_at
        FROM post_analytics pa
        LEFT JOIN companies c ON c.id = pa.company_id
        WHERE pa.channel IN (
          'google_business', 'google_business_profile', 'gbp',
          'facebook', 'facebook_page',
          'linkedin', 'linkedin_page'
        )
          AND pa.published_at >= DATE_TRUNC('week', NOW())
          AND LOWER(TRIM(COALESCE(c.industry, ''))) = LOWER(TRIM($1))
        GROUP BY platform
      `, [vertical]),
      pool.query(`
        WITH latest AS (
          SELECT DISTINCT ON (LOWER(REPLACE(agent_name, '_agent', '')))
            LOWER(REPLACE(agent_name, '_agent', '')) AS agent,
            status,
            ran_at
          FROM agent_log
          ORDER BY LOWER(REPLACE(agent_name, '_agent', '')), ran_at DESC
        ),
        week_runs AS (
          SELECT LOWER(REPLACE(agent_name, '_agent', '')) AS agent, COUNT(*)::int AS runs
          FROM agent_log
          WHERE ran_at >= NOW() - INTERVAL '7 days'
          GROUP BY LOWER(REPLACE(agent_name, '_agent', ''))
        )
        SELECT
          l.agent,
          l.status,
          l.ran_at,
          COALESCE(w.runs, 0)::int AS week_runs,
          (l.status = 'success' AND l.ran_at >= NOW() - INTERVAL '24 hours') AS active
        FROM latest l
        LEFT JOIN week_runs w ON w.agent = l.agent
      `),
    ]);

    const engagement = emailEngagement.rows[0] || { emailed_count: 0, opened_count: 0 };
    const emailedCount = Number(engagement.emailed_count || 0);
    const openedCount = Number(engagement.opened_count || 0);
    const openRate = emailedCount > 0 ? Math.round((openedCount / emailedCount) * 1000) / 10 : 0;

    const content = { gbp: 0, facebook: 0, linkedin: 0 };
    for (const row of contentWeek.rows) {
      if (content[row.platform] !== undefined) content[row.platform] = Number(row.count || 0);
    }

    const agentRows = agents.rows.map(row => {
      const key = normalizeAgentName(row.agent);
      return {
        key,
        name: AGENT_LABELS[key] || key,
        active: Boolean(row.active),
        status: row.status,
        last_run: row.ran_at,
        week_runs: Number(row.week_runs || 0),
      };
    });

    res.json({
      client: {
        id: client.id,
        name: client.name,
        business_name: client.business_name,
        vertical: client.vertical,
        email: client.email,
      },
      stats: {
        prospects_this_month: Number(prospectsMonth.rows[0]?.count || 0),
        emails_sent_this_week: Number(emailsWeek.rows[0]?.count || 0),
        open_rate: openRate,
        opened_contacts: openedCount,
        emailed_contacts: emailedCount,
      },
      recent_touchpoints: recentTouchpoints.rows.map(mapTouchpoint),
      content_published_this_week: content,
      agents: agentRows,
      generated_at: new Date().toISOString(),
    });
  } catch (err) {
    console.error('[client] dashboard error:', err.message);
    res.status(500).json({ error: 'Unable to load client dashboard' });
  }
});

ensureClientsTable().catch(err => {
  console.error('[startup] clients table error:', err.message);
});

module.exports = router;
