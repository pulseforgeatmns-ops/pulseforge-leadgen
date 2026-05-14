const express = require('express');
const path = require('path');
const pool = require('../db');

const router = express.Router();

const STAGES = ['new', 'contacted', 'follow_up', 'booked', 'dead'];

function requireAuth(req, res, next) {
  if (req.session && req.session.authenticated) return next();
  res.redirect('/login');
}

function isMax(req) {
  return req.query.role === 'max';
}

function hasMaxSecret(req) {
  return isMax(req) && process.env.CRON_SECRET && req.query.secret === process.env.CRON_SECRET;
}

function requireApiAuth(req, res, next) {
  if (req.session && req.session.authenticated) return next();
  res.status(401).json({ error: 'Unauthorized' });
}

function requireMetricsRead(req, res, next) {
  if (hasMaxSecret(req)) return next();
  return requireApiAuth(req, res, next);
}

function requireSetterRead(req, res, next) {
  if (isMax(req)) return res.status(403).json({ error: 'Setter-only endpoint' });
  return requireApiAuth(req, res, next);
}

function requireSetterWrite(req, res, next) {
  if (isMax(req)) return res.status(403).json({ error: 'Read-only role' });
  return requireApiAuth(req, res, next);
}

async function ensureSetterSchema() {
  await pool.query(`
    ALTER TABLE prospects
    ADD COLUMN IF NOT EXISTS setter_status TEXT DEFAULT 'new',
    ADD COLUMN IF NOT EXISTS setter_updated_at TIMESTAMPTZ DEFAULT NOW()
  `);
  await pool.query(`
    UPDATE prospects
    SET setter_status = 'new'
    WHERE setter_status IS NULL
      AND source = 'scout'
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS activity_log (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      lead_id UUID REFERENCES prospects(id) ON DELETE CASCADE,
      action_type TEXT NOT NULL,
      notes TEXT,
      setter_id TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
}

function scoreBand(score) {
  const n = Number(score || 0);
  if (n >= 70) return 'high';
  if (n >= 40) return 'mid';
  return 'low';
}

function businessName(row) {
  return (row.notes || '').split('—')[0].trim() ||
    `${row.first_name || ''} ${row.last_name || ''}`.trim() ||
    row.email ||
    'Unknown Lead';
}

function website(row) {
  return ((row.notes || '').split('—')[1] || '').trim();
}

function cityFor(row) {
  if (row.city) return row.city;
  return 'Manchester NH';
}

function mapLead(row) {
  return {
    id: row.id,
    business_name: businessName(row),
    vertical: row.vertical || 'unknown',
    city: cityFor(row),
    score: Number(row.icp_score || 0),
    score_band: scoreBand(row.icp_score),
    status: row.setter_status || 'new',
    date_added: row.created_at,
    phone: row.phone,
    email: row.email,
    contact_name: `${row.first_name || ''} ${row.last_name || ''}`.trim(),
    website: website(row),
    notes: row.notes,
  };
}

async function getLeads(where = '', params = [], limit = 250) {
  const { rows } = await pool.query(`
    SELECT p.*
    FROM prospects p
    WHERE p.source = 'scout'
      AND COALESCE(p.do_not_contact, false) = false
      AND COALESCE(p.icp_score, 0) >= 40
      ${where}
    ORDER BY p.icp_score DESC NULLS LAST, p.created_at DESC
    LIMIT $${params.length + 1}
  `, [...params, limit]);
  return rows.map(mapLead);
}

router.get('/', requireAuth, async (req, res) => {
  await ensureSetterSchema().catch(err => console.error('[setter] schema error:', err.message));
  res.sendFile(path.join(__dirname, '..', 'public', 'setter-dashboard.html'));
});

router.get('/api/metrics', requireMetricsRead, async (req, res) => {
  try {
    await ensureSetterSchema();
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE)::int AS leads_today,
        COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days')::int AS leads_week,
        COUNT(*) FILTER (WHERE setter_status IN ('contacted', 'follow_up', 'booked', 'dead'))::int AS contacted,
        COUNT(*) FILTER (WHERE setter_status = 'booked')::int AS booked,
        COUNT(*) FILTER (WHERE setter_status = 'dead')::int AS dead,
        COUNT(*)::int AS total
      FROM prospects
      WHERE source = 'scout'
        AND COALESCE(do_not_contact, false) = false
        AND COALESCE(icp_score, 0) >= 40
    `);
    const m = rows[0] || {};
    const total = Number(m.total || 0);
    const contacted = Number(m.contacted || 0);
    const booked = Number(m.booked || 0);
    res.json({
      leads_today: Number(m.leads_today || 0),
      leads_week: Number(m.leads_week || 0),
      contacted,
      booked,
      dead: Number(m.dead || 0),
      total,
      contacted_rate: total ? +((contacted / total) * 100).toFixed(1) : 0,
      booked_rate: contacted ? +((booked / contacted) * 100).toFixed(1) : 0,
    });
  } catch (err) {
    console.error('[setter] metrics error:', err.message);
    res.status(500).json({ error: 'Unable to load metrics' });
  }
});

router.get('/api/scout-feed', requireMetricsRead, async (req, res) => {
  try {
    await ensureSetterSchema();
    const leads = await getLeads(`AND p.created_at >= NOW() - INTERVAL '7 days'`, [], 80);
    res.json(leads);
  } catch (err) {
    console.error('[setter] scout feed error:', err.message);
    res.status(500).json({ error: 'Unable to load scout feed' });
  }
});

router.get('/api/leads', requireSetterRead, async (req, res) => {
  try {
    await ensureSetterSchema();
    const status = req.query.status;
    const where = status && STAGES.includes(status)
      ? `AND COALESCE(p.setter_status, 'new') = $1`
      : `AND COALESCE(p.setter_status, 'new') = 'new'`;
    const params = status && STAGES.includes(status) ? [status] : [];
    res.json(await getLeads(where, params));
  } catch (err) {
    console.error('[setter] leads error:', err.message);
    res.status(500).json({ error: 'Unable to load leads' });
  }
});

router.get('/api/pipeline', requireSetterRead, async (req, res) => {
  try {
    await ensureSetterSchema();
    const leads = await getLeads('', [], 500);
    const grouped = Object.fromEntries(STAGES.map(stage => [stage, []]));
    for (const lead of leads) {
      const stage = STAGES.includes(lead.status) ? lead.status : 'new';
      grouped[stage].push(lead);
    }
    res.json({ stages: STAGES, grouped, counts: Object.fromEntries(STAGES.map(s => [s, grouped[s].length])) });
  } catch (err) {
    console.error('[setter] pipeline error:', err.message);
    res.status(500).json({ error: 'Unable to load pipeline' });
  }
});

router.patch('/api/leads/:id/status', requireSetterWrite, async (req, res) => {
  const { status } = req.body;
  if (!STAGES.includes(status)) return res.status(400).json({ error: 'Invalid status' });
  try {
    await ensureSetterSchema();
    const { rows } = await pool.query(`
      UPDATE prospects
      SET setter_status = $1, setter_updated_at = NOW()
      WHERE id = $2 AND source = 'scout'
      RETURNING *
    `, [status, req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'Lead not found' });
    res.json({ success: true, lead: mapLead(rows[0]) });
  } catch (err) {
    console.error('[setter] status error:', err.message);
    res.status(500).json({ error: 'Unable to update status' });
  }
});

router.get('/api/activity', requireSetterRead, async (req, res) => {
  try {
    await ensureSetterSchema();
    const { rows } = await pool.query(`
      SELECT
        al.id,
        al.lead_id,
        al.action_type,
        al.notes AS activity_notes,
        al.setter_id,
        al.created_at,
        p.first_name,
        p.last_name,
        p.email,
        p.notes AS prospect_notes,
        p.vertical,
        p.icp_score
      FROM activity_log al
      JOIN prospects p ON p.id = al.lead_id
      ORDER BY al.created_at DESC
      LIMIT 100
    `);
    res.json(rows.map(row => ({
      id: row.id,
      lead_id: row.lead_id,
      business_name: businessName({ ...row, notes: row.prospect_notes }),
      action_type: row.action_type,
      notes: row.activity_notes,
      setter_id: row.setter_id,
      created_at: row.created_at,
    })));
  } catch (err) {
    console.error('[setter] activity error:', err.message);
    res.status(500).json({ error: 'Unable to load activity' });
  }
});

router.post('/api/activity', requireSetterWrite, async (req, res) => {
  const { lead_id, action_type, notes } = req.body;
  if (!lead_id || !['call', 'email', 'text'].includes(action_type)) {
    return res.status(400).json({ error: 'Invalid activity' });
  }
  try {
    await ensureSetterSchema();
    const setterId = req.session?.user || req.sessionID || 'setter';
    const { rows } = await pool.query(`
      INSERT INTO activity_log (lead_id, action_type, notes, setter_id)
      VALUES ($1, $2, $3, $4)
      RETURNING *
    `, [lead_id, action_type, notes || '', setterId]);
    await pool.query(`
      UPDATE prospects
      SET setter_status = CASE WHEN setter_status = 'new' THEN 'contacted' ELSE setter_status END,
          setter_updated_at = NOW()
      WHERE id = $1
    `, [lead_id]);
    res.json({ success: true, activity: rows[0] });
  } catch (err) {
    console.error('[setter] activity create error:', err.message);
    res.status(500).json({ error: 'Unable to save activity' });
  }
});

ensureSetterSchema().catch(err => console.error('[startup] setter schema error:', err.message));

module.exports = router;
