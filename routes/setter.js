const express = require('express');
const path = require('path');
const axios = require('axios');
const pool = require('../db');
const { requireAuth: sessionAuth, requireRole } = require('../middleware/auth');

const router = express.Router();

const STAGES = ['new', 'contacted', 'follow_up', 'booked', 'dead'];
const SETTER_NOTES_MARKER = '\n\n--- setter notes ---\n';

function isMax(req) {
  return req.query.role === 'max';
}

function hasMaxSecret(req) {
  return isMax(req) && process.env.CRON_SECRET && req.query.secret === process.env.CRON_SECRET;
}

function requireMetricsRead(req, res, next) {
  if (hasMaxSecret(req)) return next();
  return sessionAuth(req, res, err => {
    if (err) return next(err);
    return requireRole('admin', 'manager', 'setter')(req, res, next);
  });
}

function requireSetterRead(req, res, next) {
  if (isMax(req)) return res.status(403).json({ error: 'Setter-only endpoint' });
  return sessionAuth(req, res, err => {
    if (err) return next(err);
    return requireRole('admin', 'manager', 'setter')(req, res, next);
  });
}

function requireSetterWrite(req, res, next) {
  if (isMax(req)) return res.status(403).json({ error: 'Read-only role' });
  return sessionAuth(req, res, err => {
    if (err) return next(err);
    return requireRole('admin', 'manager', 'setter')(req, res, next);
  });
}

async function ensureSetterSchema() {
  await pool.query(`
    ALTER TABLE prospects
    ADD COLUMN IF NOT EXISTS notes TEXT,
    ADD COLUMN IF NOT EXISTS callback_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS is_hot BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS setter_status TEXT DEFAULT 'new',
    ADD COLUMN IF NOT EXISTS setter_visible BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS setter_updated_at TIMESTAMPTZ DEFAULT NOW()
  `);
  await pool.query(`
    UPDATE prospects
    SET setter_status = COALESCE(setter_status, 'new'),
        setter_visible = true,
        setter_updated_at = COALESCE(setter_updated_at, NOW())
    WHERE source = 'scout'
      AND COALESCE(icp_score, 0) >= 40
      AND COALESCE(do_not_contact, false) = false
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
  return baseNotes(row.notes).split('—')[0].trim() ||
    `${row.first_name || ''} ${row.last_name || ''}`.trim() ||
    row.email ||
    'Unknown Lead';
}

function website(row) {
  return ((baseNotes(row.notes) || '').split('—')[1] || '').trim();
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
    notes: setterNotes(row.notes),
    raw_notes: row.notes,
    callback_at: row.callback_at,
    is_hot: Boolean(row.is_hot),
    attempt_count: Number(row.attempt_count || 0),
  };
}

async function getLeads(where = '', params = [], limit = 250, orderBy = 'COALESCE(p.is_hot, false) DESC, p.icp_score DESC NULLS LAST, p.created_at DESC') {
  const sort = orderBy || 'COALESCE(p.is_hot, false) DESC, p.icp_score DESC NULLS LAST, p.created_at DESC';
  const { rows } = await pool.query(`
    SELECT p.*,
      (
        SELECT COUNT(*)::int
        FROM activity_log al
        WHERE al.lead_id = p.id
          AND al.action_type = 'call'
      ) AS attempt_count
    FROM prospects p
    WHERE p.source = 'scout'
      AND COALESCE(p.setter_visible, false) = true
      AND COALESCE(p.do_not_contact, false) = false
      AND COALESCE(p.icp_score, 0) >= 40
      ${where}
    ORDER BY ${sort}
    LIMIT $${params.length + 1}
  `, [...params, limit]);
  return rows.map(mapLead);
}

function baseNotes(notes) {
  return String(notes || '').split(SETTER_NOTES_MARKER)[0].trim();
}

function setterNotes(notes) {
  const value = String(notes || '');
  const index = value.indexOf(SETTER_NOTES_MARKER);
  return index === -1 ? '' : value.slice(index + SETTER_NOTES_MARKER.length);
}

function composeNotes(existing, scratchpad) {
  const base = baseNotes(existing);
  const clean = String(scratchpad || '').trimEnd();
  return clean ? `${base}${SETTER_NOTES_MARKER}${clean}` : base;
}

function searchWhere(search, params) {
  const keyword = String(search || '').trim();
  if (!keyword) return '';
  params.push(`%${keyword}%`);
  const idx = params.length;
  return `
    AND (
      p.notes ILIKE $${idx}
      OR p.first_name ILIKE $${idx}
      OR p.last_name ILIKE $${idx}
      OR p.email ILIKE $${idx}
      OR p.phone ILIKE $${idx}
      OR p.vertical ILIKE $${idx}
      OR 'Manchester NH' ILIKE $${idx}
    )
  `;
}

function splitName(row) {
  const full = `${row.first_name || ''} ${row.last_name || ''}`.trim();
  if (!full || full.toLowerCase() === 'there') return {};
  const parts = full.split(/\s+/);
  return { full_name: full, first_name: parts[0], last_name: parts.slice(1).join(' ') };
}

function usableEmail(email) {
  return typeof email === 'string' && email.includes('@') && !email.includes('*');
}

function pickPhone(data) {
  const person = data?.person || {};
  const company = data?.company || {};
  return person.mobile?.mobile_international ||
    person.mobile?.mobile_national ||
    person.mobile?.mobile ||
    company.phone_hq?.phone_hq_international ||
    company.phone_hq?.phone_hq_national ||
    company.phone_hq?.phone_hq ||
    null;
}

async function prospeoEnrichPhone(row) {
  const key = process.env.PROSPEO_API_KEY;
  if (!key) return null;
  const companyName = businessName(row);
  const companyWebsite = website(row);
  const name = splitName(row);
  const data = {
    ...(usableEmail(row.email) ? { email: row.email } : {}),
    ...(name.full_name ? { full_name: name.full_name } : {}),
    ...(companyName ? { company_name: companyName } : {}),
    ...(companyWebsite ? { company_website: companyWebsite } : {}),
  };
  if (!data.email && !data.full_name) return null;

  const headers = { 'Content-Type': 'application/json', 'X-KEY': key };
  try {
    const direct = await axios.post('https://api.prospeo.io/enrich-person', {
      enrich_mobile: true,
      data,
    }, { headers });
    const phone = pickPhone(direct.data);
    if (phone) return phone;
  } catch (err) {
    console.warn('[setter] Prospeo direct enrich:', err.response?.data || err.message);
  }

  if (!companyWebsite) return null;
  try {
    const search = await axios.post('https://api.prospeo.io/search-person', {
      page: 1,
      filters: { company: { websites: { include: [companyWebsite] } } },
    }, { headers });
    const personId = search.data?.results?.[0]?.person?.person_id || search.data?.results?.[0]?.person_id;
    if (!personId) return null;
    const enriched = await axios.post('https://api.prospeo.io/enrich-person', {
      enrich_mobile: true,
      data: { person_id: personId },
    }, { headers });
    return pickPhone(enriched.data);
  } catch (err) {
    console.warn('[setter] Prospeo search enrich:', err.response?.data || err.message);
    return null;
  }
}

router.get('/', sessionAuth, requireRole('admin', 'manager', 'setter'), async (req, res) => {
  await ensureSetterSchema().catch(err => console.error('[setter] schema error:', err.message));
  res.sendFile(path.join(__dirname, '..', 'public', 'setter-dashboard.html'));
});

async function metricsHandler(req, res) {
  try {
    await ensureSetterSchema();
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE)::int AS leads_today,
        COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days')::int AS leads_week,
        COUNT(*) FILTER (WHERE setter_status IN ('contacted', 'follow_up', 'booked', 'dead'))::int AS contacted,
        COUNT(*) FILTER (WHERE setter_status = 'booked')::int AS booked,
        COUNT(*) FILTER (WHERE setter_status = 'dead')::int AS dead,
        COUNT(*) FILTER (WHERE COALESCE(is_hot, false) = true)::int AS hot,
        COUNT(*)::int AS total
      FROM prospects
      WHERE source = 'scout'
        AND COALESCE(setter_visible, false) = true
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
      hot: Number(m.hot || 0),
      total,
      contacted_rate: total ? +((contacted / total) * 100).toFixed(1) : 0,
      booked_rate: contacted ? +((booked / contacted) * 100).toFixed(1) : 0,
    });
  } catch (err) {
    console.error('[setter] metrics error:', err.message);
    res.status(500).json({ error: 'Unable to load metrics' });
  }
}

async function feedHandler(req, res) {
  try {
    await ensureSetterSchema();
    const leads = await getLeads(`AND p.created_at >= NOW() - INTERVAL '7 days'`, [], 80);
    res.json(leads);
  } catch (err) {
    console.error('[setter] scout feed error:', err.message);
    res.status(500).json({ error: 'Unable to load scout feed' });
  }
}

router.get('/api/metrics', requireMetricsRead, metricsHandler);
router.get('/metrics', requireMetricsRead, metricsHandler);
router.get('/api/scout-feed', requireMetricsRead, feedHandler);
router.get('/api/feed', requireMetricsRead, feedHandler);
router.get('/feed', requireMetricsRead, feedHandler);

router.get(['/api/leads', '/leads'], requireSetterRead, async (req, res) => {
  try {
    await ensureSetterSchema();
    const status = req.query.status;
    const params = [];
    const search = searchWhere(req.query.search, params);
    const where = req.query.due === 'today'
      ? `${search} AND p.callback_at IS NOT NULL AND p.callback_at < (CURRENT_DATE + INTERVAL '1 day')`
      : status && STAGES.includes(status)
      ? `${search} AND COALESCE(p.setter_status, 'new') = $${params.push(status)}`
      : `${search} AND COALESCE(p.setter_status, 'new') = 'new'`;
    const order = req.query.due === 'today'
      ? 'p.callback_at ASC NULLS LAST'
      : undefined;
    res.json(await getLeads(where, params, 250, order));
  } catch (err) {
    console.error('[setter] leads error:', err.message);
    res.status(500).json({ error: 'Unable to load leads' });
  }
});

router.get(['/api/stats/today', '/stats/today'], requireSetterRead, async (req, res) => {
  try {
    await ensureSetterSchema();
    const setterId = req.user?.id || null;
    const { rows } = await pool.query(`
      SELECT COUNT(*)::int AS calls_today
      FROM activity_log
      WHERE action_type = 'call'
        AND setter_id = $1
        AND created_at >= CURRENT_DATE
        AND created_at < CURRENT_DATE + INTERVAL '1 day'
    `, [setterId]);
    res.json({ calls_today: Number(rows[0]?.calls_today || 0), goal: 20 });
  } catch (err) {
    console.error('[setter] today stats error:', err.message);
    res.status(500).json({ error: 'Unable to load today stats' });
  }
});

router.get(['/api/pipeline', '/pipeline'], requireSetterRead, async (req, res) => {
  try {
    await ensureSetterSchema();
    const params = [];
    const leads = await getLeads(searchWhere(req.query.search, params), params, 500);
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

router.patch(['/api/leads/:id/status', '/leads/:id/status'], requireSetterWrite, async (req, res) => {
  const { status } = req.body;
  if (!STAGES.includes(status)) return res.status(400).json({ error: 'Invalid status' });
  try {
    await ensureSetterSchema();
    const { rows } = await pool.query(`
      UPDATE prospects
      SET setter_status = $1, setter_visible = true, setter_updated_at = NOW()
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

router.patch(['/api/leads/:id/notes', '/leads/:id/notes'], requireSetterWrite, async (req, res) => {
  try {
    await ensureSetterSchema();
    const incoming = String(req.body.notes || '').slice(0, 5000);
    const current = await pool.query(`
      SELECT *
      FROM prospects
      WHERE id = $1 AND source = 'scout'
    `, [req.params.id]);
    if (!current.rows.length) return res.status(404).json({ error: 'Lead not found' });
    const notes = composeNotes(current.rows[0].notes, incoming);
    const { rows } = await pool.query(`
      UPDATE prospects
      SET notes = $1, updated_at = NOW()
      WHERE id = $2 AND source = 'scout'
      RETURNING *
    `, [notes, req.params.id]);
    res.json({ success: true, lead: mapLead(rows[0]) });
  } catch (err) {
    console.error('[setter] notes error:', err.message);
    res.status(500).json({ error: 'Unable to save notes' });
  }
});

router.patch(['/api/leads/:id/callback', '/leads/:id/callback'], requireSetterWrite, async (req, res) => {
  try {
    await ensureSetterSchema();
    const callbackAt = req.body.callback_at ? new Date(req.body.callback_at) : null;
    if (callbackAt && Number.isNaN(callbackAt.getTime())) {
      return res.status(400).json({ error: 'Invalid callback time' });
    }
    const { rows } = await pool.query(`
      UPDATE prospects
      SET callback_at = $1, updated_at = NOW()
      WHERE id = $2 AND source = 'scout'
      RETURNING *
    `, [callbackAt ? callbackAt.toISOString() : null, req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'Lead not found' });
    res.json({ success: true, lead: mapLead(rows[0]) });
  } catch (err) {
    console.error('[setter] callback error:', err.message);
    res.status(500).json({ error: 'Unable to update callback' });
  }
});

router.patch(['/api/leads/:id/hot', '/leads/:id/hot'], requireSetterWrite, async (req, res) => {
  try {
    await ensureSetterSchema();
    const isHot = Boolean(req.body.is_hot);
    const { rows } = await pool.query(`
      UPDATE prospects
      SET is_hot = $1, updated_at = NOW()
      WHERE id = $2 AND source = 'scout'
      RETURNING *
    `, [isHot, req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'Lead not found' });
    res.json({ success: true, lead: mapLead(rows[0]) });
  } catch (err) {
    console.error('[setter] hot flag error:', err.message);
    res.status(500).json({ error: 'Unable to update hot flag' });
  }
});

router.post(['/api/leads/:id/enrich-phone', '/leads/:id/enrich-phone'], requireSetterWrite, async (req, res) => {
  let status = 'no_result';
  let payload = {};
  try {
    await ensureSetterSchema();
    const { rows } = await pool.query(`
      SELECT *
      FROM prospects
      WHERE id = $1 AND source = 'scout'
    `, [req.params.id]);
    if (!rows.length) return res.status(404).json({ error: 'Lead not found' });

    const phone = await prospeoEnrichPhone(rows[0]);
    if (phone) {
      status = 'success';
      payload = { phone };
      await pool.query('UPDATE prospects SET phone = $1, updated_at = NOW() WHERE id = $2', [phone, req.params.id]);
      await pool.query(`
        INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at)
        VALUES ('setter', 'phone_enrich', $1, $2, $3, NOW())
      `, [req.params.id, JSON.stringify(payload), status]);
      return res.json({ phone });
    }

    await pool.query(`
      INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at)
      VALUES ('setter', 'phone_enrich', $1, $2, $3, NOW())
    `, [req.params.id, JSON.stringify({ reason: 'No phone found' }), status]);
    res.json({ phone: null });
  } catch (err) {
    console.error('[setter] phone enrich error:', err.response?.data || err.message);
    await pool.query(`
      INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, error_msg, ran_at)
      VALUES ('setter', 'phone_enrich', $1, $2, 'no_result', $3, NOW())
    `, [req.params.id, JSON.stringify(payload), err.message]).catch(() => {});
    res.status(500).json({ error: 'Unable to enrich phone', phone: null });
  }
});

router.post(['/api/leads/:id/quick-log-call', '/leads/:id/quick-log-call'], requireSetterWrite, async (req, res) => {
  try {
    await ensureSetterSchema();
    const setterId = req.user?.id || null;
    const exists = await pool.query(`
      SELECT id
      FROM prospects
      WHERE id = $1 AND source = 'scout'
    `, [req.params.id]);
    if (!exists.rows.length) return res.status(404).json({ error: 'Lead not found' });

    await pool.query(`
      INSERT INTO activity_log (lead_id, action_type, notes, setter_id)
      VALUES ($1, 'call', 'No answer', $2)
    `, [req.params.id, setterId]);
    await pool.query(`
      UPDATE prospects
      SET setter_status = CASE WHEN setter_status = 'new' THEN 'contacted' ELSE setter_status END,
          setter_updated_at = NOW()
      WHERE id = $1
    `, [req.params.id]);
    const count = await pool.query(`
      SELECT COUNT(*)::int AS attempt_count
      FROM activity_log
      WHERE lead_id = $1 AND action_type = 'call'
    `, [req.params.id]);
    res.json({ success: true, attempt_count: Number(count.rows[0]?.attempt_count || 0) });
  } catch (err) {
    console.error('[setter] quick call log error:', err.message);
    res.status(500).json({ error: 'Unable to log call' });
  }
});

router.get(['/api/activity', '/activity'], requireSetterRead, async (req, res) => {
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

router.post(['/api/activity', '/activity'], requireSetterWrite, async (req, res) => {
  const { lead_id, action_type, notes } = req.body;
  if (!lead_id || !['call', 'email', 'text'].includes(action_type)) {
    return res.status(400).json({ error: 'Invalid activity' });
  }
  try {
    await ensureSetterSchema();
    const setterId = req.user?.id || null;
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
