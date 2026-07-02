const express = require('express');
const path = require('path');
const axios = require('axios');
const pool = require('../db');
const { requireAuth: sessionAuth, requireRole } = require('../middleware/auth');
const { enrichPhoneWaterfall } = require('../phoneEnrich');
const { ensureCloserSchema } = require('../utils/closerSchema');
const { ensureSetterVisibilitySchema, setSetterVisibility } = require('../utils/setterVisibility');
const { normalizeClientId } = require('../utils/clientContext');
const {
  DISPOSITION_SET,
  applyProspectDisposition,
  ensureCallDispositionSchema,
  resolveCallbackAt,
} = require('../utils/callDispositions');

const router = express.Router();

const STAGES = ['new', 'contacted', 'follow_up', 'booked', 'dead'];
const SETTER_NOTES_MARKER = '\n\n--- setter notes ---\n';

function setterClientId(req) {
  if (hasMaxSecret(req)) return normalizeClientId(req.query.client_id);
  return normalizeClientId(req?.session?.active_client_id || req?.user?.client_id);
}

function clientFilter(req, params, alias = 'p.') {
  params.push(setterClientId(req));
  return ` AND ${alias}client_id = $${params.length}`;
}

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
    return requireRole('admin', 'manager', 'setter', 'sales')(req, res, next);
  });
}

function requireSetterRead(req, res, next) {
  if (isMax(req)) return res.status(403).json({ error: 'Setter-only endpoint' });
  return sessionAuth(req, res, err => {
    if (err) return next(err);
    return requireRole('admin', 'manager', 'setter', 'sales')(req, res, next);
  });
}

function requireSetterWrite(req, res, next) {
  if (isMax(req)) return res.status(403).json({ error: 'Read-only role' });
  return sessionAuth(req, res, err => {
    if (err) return next(err);
    return requireRole('admin', 'manager', 'setter', 'sales')(req, res, next);
  });
}

async function ensureSetterSchema() {
  await ensureCloserSchema();
  await ensureSetterVisibilitySchema(pool);
  await ensureCallDispositionSchema(pool);
  await pool.query(`
    ALTER TABLE prospects
    ADD COLUMN IF NOT EXISTS notes TEXT,
    ADD COLUMN IF NOT EXISTS callback_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS is_hot BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS enrichment_attempted BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS setter_status TEXT DEFAULT 'new',
    ADD COLUMN IF NOT EXISTS setter_visible BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS setter_updated_at TIMESTAMPTZ DEFAULT NOW()
  `);
  await pool.query(`
    UPDATE prospects
    SET setter_status = COALESCE(setter_status, 'new'),
        setter_updated_at = COALESCE(setter_updated_at, NOW())
    WHERE source = 'scout'
      AND (setter_status IS NULL OR setter_updated_at IS NULL)
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS activity_log (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      lead_id UUID REFERENCES prospects(id) ON DELETE CASCADE,
      action_type TEXT NOT NULL,
      notes TEXT,
      setter_id TEXT,
      client_id INTEGER NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await pool.query(`
    ALTER TABLE activity_log
    ADD COLUMN IF NOT EXISTS client_id INTEGER
  `);
  await pool.query(`
    ALTER TABLE activity_log
    ALTER COLUMN client_id DROP DEFAULT
  `);
}

function scoreBand(score) {
  const n = Number(score || 0);
  if (n >= 70) return 'high';
  if (n >= 40) return 'mid';
  return 'low';
}

function businessName(row) {
  return row.company_name ||
    baseNotes(row.notes).split('—')[0].trim() ||
    `${row.first_name || ''} ${row.last_name || ''}`.trim() ||
    row.email ||
    'Unknown Lead';
}

function website(row) {
  return ((baseNotes(row.notes) || '').split('—')[1] || '').trim();
}

function cityFor(row) {
  if (row.service_area_match) return row.service_area_match;
  if (row.city) return row.city;
  return 'Providence RI';
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

async function getLeads(where = '', params = [], limit = 250, orderBy = 'COALESCE(p.is_hot, false) DESC, p.icp_score DESC NULLS LAST, p.created_at DESC', req = null) {
  const sort = orderBy || 'COALESCE(p.is_hot, false) DESC, p.icp_score DESC NULLS LAST, p.created_at DESC';
  const clientScope = clientFilter(req, params);
  const { rows } = await pool.query(`
    SELECT p.*,
      c.name AS company_name,
      ((
        SELECT COUNT(*)::int
        FROM activity_log al
        WHERE al.lead_id = p.id
          AND al.client_id = p.client_id
          AND al.action_type = 'call'
      ) + (
        SELECT COUNT(*)::int
        FROM call_dispositions cd
        WHERE cd.prospect_id = p.id
          AND cd.client_id = p.client_id
      )) AS attempt_count
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.source = 'scout'
      AND COALESCE(p.setter_visible, false) = true
      AND COALESCE(p.do_not_contact, false) = false
      AND COALESCE(p.icp_score, 0) >= 40
      ${clientScope}
      ${where}
    ORDER BY ${sort}
    LIMIT $${params.length + 1}
  `, [...params, limit]);
  return rows.map(mapLead);
}

async function getMissingPhoneProspects(req, limit = 2000) {
  const params = [];
  const clientScope = clientFilter(req, params);
  const { rows } = await pool.query(`
    SELECT p.*,
      c.name AS company_name,
      ((
        SELECT COUNT(*)::int
        FROM activity_log al
        WHERE al.lead_id = p.id
          AND al.client_id = p.client_id
          AND al.action_type = 'call'
      ) + (
        SELECT COUNT(*)::int
        FROM call_dispositions cd
        WHERE cd.prospect_id = p.id
          AND cd.client_id = p.client_id
      )) AS attempt_count
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE NULLIF(BTRIM(COALESCE(p.phone, '')), '') IS NULL
      AND COALESCE(p.enrichment_attempted, false) = false
      ${clientScope}
    ORDER BY COALESCE(p.setter_visible, false) DESC,
      COALESCE(p.icp_score, 0) DESC,
      p.created_at DESC
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

function appendHandoffNote(existing, handoffNote) {
  const clean = String(handoffNote || '').trim();
  if (!clean) return existing;
  const current = setterNotes(existing).trim();
  const next = current ? `${current}\n\nHandoff note for Levi:\n${clean}` : `Handoff note for Levi:\n${clean}`;
  return composeNotes(existing, next);
}

async function getLeviCloser(clientId) {
  const configuredId = Number(process.env.LEVI_CLOSER_ID || 0);
  const params = [clientId];
  let filter = "role = 'closer' AND active = true AND client_id = $1";
  if (configuredId) {
    params.push(configuredId);
    filter += ` AND id = $${params.length}`;
  }
  const { rows } = await pool.query(`
    SELECT id, name, email
    FROM users
    WHERE ${filter}
    ORDER BY id ASC
    LIMIT 1
  `, params);
  return rows[0] || null;
}

function handoffDescription(row, notes) {
  const contact = `${row.first_name || ''} ${row.last_name || ''}`.trim() || 'No contact name';
  return [
    `${businessName(row)} · ${row.vertical || 'unknown'} · ${cityFor(row)}`,
    `Contact: ${contact} · ${row.phone || 'no phone'} · ${row.email || 'no email'}`,
    `Setter notes: ${notes || 'None'}`,
    `Score: ${row.icp_score || 0} · Added: ${row.created_at ? new Date(row.created_at).toLocaleDateString() : 'unknown'}`,
  ].join('\n');
}

async function sendCloserHandoffEmail(closer, row, description) {
  if (!closer?.email || !process.env.BREVO_API_KEY) return false;
  await axios.post('https://api.brevo.com/v3/smtp/email', {
    sender: { name: 'Pulseforge Setter', email: 'jacob@gopulseforge.com' },
    to: [{ email: closer.email, name: closer.name || 'Levi' }],
    subject: `New booked call — ${businessName(row)}`,
    textContent: `Levi,\n\nWilliam booked a new call for you.\n\n${description}\n\nReview it in the closer dashboard:\n${process.env.APP_URL || 'https://gopulseforge.com'}/closer\n\n— Pulseforge`,
  }, {
    headers: {
      'api-key': process.env.BREVO_API_KEY,
      'Content-Type': 'application/json',
    },
  });
  return true;
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
      OR 'Providence RI' ILIKE $${idx}
    )
  `;
}

router.get('/', sessionAuth, requireRole('admin', 'manager', 'setter', 'sales'), async (req, res) => {
  await ensureSetterSchema().catch(err => console.error('[setter] schema error:', err.message));
  res.sendFile(path.join(__dirname, '..', 'public', 'setter-dashboard.html'));
});

async function metricsHandler(req, res) {
  try {
    await ensureSetterSchema();
    const params = [];
    const clientScope = clientFilter(req, params, '');
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
        ${clientScope}
    `, params);
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
    const leads = await getLeads(`AND p.created_at >= NOW() - INTERVAL '7 days'`, [], 80, undefined, req);
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
    if (req.query.missing_phone === 'true') {
      return res.json(await getMissingPhoneProspects(req, 2000));
    }
    const status = req.query.status;
    const params = [];
    const search = searchWhere(req.query.search, params);
    const filters = [search];
    if (req.query.due === 'today') {
      filters.push(`AND p.callback_at IS NOT NULL AND p.callback_at < (CURRENT_DATE + INTERVAL '1 day')`);
    } else if (status && STAGES.includes(status)) {
      filters.push(`AND COALESCE(p.setter_status, 'new') = $${params.push(status)}`);
    } else if (req.query.all_statuses !== 'true') {
      filters.push(`AND COALESCE(p.setter_status, 'new') = 'new'`);
    }
    const where = filters.join('\n');
    const order = req.query.due === 'today'
      ? 'p.callback_at ASC NULLS LAST'
      : undefined;
    res.json(await getLeads(where, params, 250, order, req));
  } catch (err) {
    console.error('[setter] leads error:', err.message);
    res.status(500).json({ error: 'Unable to load leads' });
  }
});

router.get(['/api/stats/today', '/stats/today'], requireSetterRead, async (req, res) => {
  try {
    await ensureSetterSchema();
    const setterId = req.user?.id || null;
    const clientId = setterClientId(req);
    const { rows } = await pool.query(`
      SELECT (
        SELECT COUNT(*)::int
        FROM activity_log
        WHERE action_type = 'call'
          AND setter_id = $1::text
          AND client_id = $2
          AND created_at >= CURRENT_DATE
          AND created_at < CURRENT_DATE + INTERVAL '1 day'
      ) + (
        SELECT COUNT(*)::int
        FROM call_dispositions
        WHERE setter_id = $1
          AND client_id = $2
          AND source = 'manual_setter'
          AND created_at >= CURRENT_DATE
          AND created_at < CURRENT_DATE + INTERVAL '1 day'
      ) AS calls_today
    `, [setterId, clientId]);
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
    const leads = await getLeads(searchWhere(req.query.search, params), params, 500, undefined, req);
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
    const clientId = setterClientId(req);
    const handoffNote = String(req.body.handoff_note || '').slice(0, 5000);
    let rows;
    let handoff = null;

    if (status === 'booked') {
      const current = await pool.query(`
        SELECT *
        FROM prospects
        WHERE id = $1 AND source = 'scout' AND client_id = $2
      `, [req.params.id, clientId]);
      if (!current.rows.length) return res.status(404).json({ error: 'Lead not found' });

      const closer = await getLeviCloser(clientId);
      const notes = appendHandoffNote(current.rows[0].notes, handoffNote);
      const update = await pool.query(`
        UPDATE prospects
        SET setter_status = 'booked',
            setter_updated_at = NOW(),
            booked_at = COALESCE(booked_at, NOW()),
            closer_id = COALESCE($1, closer_id),
            closer_status = 'booked',
            notes = $2
        WHERE id = $3 AND source = 'scout' AND client_id = $4
        RETURNING *
      `, [closer?.id || null, notes, req.params.id, clientId]);
      const visibleRow = update.rows.length
        ? await setSetterVisibility(pool, req.params.id, {
            reason: 'stage_change',
            source: 'scout',
            stageStatus: 'booked',
            clientId,
          })
        : null;
      rows = visibleRow ? [visibleRow] : [];

      if (closer) {
        const description = handoffDescription(rows[0], setterNotes(rows[0].notes));
        await pool.query(`
          INSERT INTO agent_actions (created_by, action_type, title, description, payload, status, client_id)
          VALUES ('setter', 'closer_handoff', $1, $2, $3, 'pending', $4)
        `, [
          `New booked call — ${businessName(rows[0])}`,
          description,
          JSON.stringify({
            prospect_id: rows[0].id,
            setter_id: req.user?.id || null,
            closer_id: closer.id,
          }),
          rows[0].client_id,
        ]);

        let emailed = false;
        try {
          emailed = await sendCloserHandoffEmail(closer, rows[0], description);
        } catch (emailErr) {
          console.error('[setter] closer handoff email error:', emailErr.response?.data || emailErr.message);
        }
        handoff = { assigned: true, closer_id: closer.id, closer_name: closer.name, emailed };
      } else {
        handoff = { assigned: false, reason: 'No active closer user found' };
      }
    } else {
      const update = await pool.query(`
        UPDATE prospects
        SET setter_status = $1,
            status = CASE WHEN $1 = 'dead' THEN 'dead' ELSE status END,
            setter_updated_at = NOW()
        WHERE id = $2 AND source = 'scout' AND client_id = $3
        RETURNING *
      `, [status, req.params.id, clientId]);
      const visibleRow = update.rows.length
        ? await setSetterVisibility(pool, req.params.id, {
            reason: 'stage_change',
            source: 'scout',
            stageStatus: status,
            clientId,
          })
        : null;
      rows = visibleRow ? [visibleRow] : [];
    }
    if (!rows.length) return res.status(404).json({ error: 'Lead not found' });
    res.json({ success: true, lead: mapLead(rows[0]), handoff });
  } catch (err) {
    console.error('[setter] status error:', err.message);
    res.status(500).json({ error: 'Unable to update status' });
  }
});

router.patch(['/api/leads/:id/notes', '/leads/:id/notes'], requireSetterWrite, async (req, res) => {
  try {
    await ensureSetterSchema();
    const clientId = setterClientId(req);
    const incoming = String(req.body.notes || '').slice(0, 5000);
    const current = await pool.query(`
      SELECT *
      FROM prospects
      WHERE id = $1 AND source = 'scout' AND client_id = $2
    `, [req.params.id, clientId]);
    if (!current.rows.length) return res.status(404).json({ error: 'Lead not found' });
    const notes = composeNotes(current.rows[0].notes, incoming);
    const { rows } = await pool.query(`
      UPDATE prospects
      SET notes = $1, updated_at = NOW()
      WHERE id = $2 AND source = 'scout' AND client_id = $3
      RETURNING *
    `, [notes, req.params.id, clientId]);
    res.json({ success: true, lead: mapLead(rows[0]) });
  } catch (err) {
    console.error('[setter] notes error:', err.message);
    res.status(500).json({ error: 'Unable to save notes' });
  }
});

router.patch(['/api/leads/:id/callback', '/leads/:id/callback'], requireSetterWrite, async (req, res) => {
  try {
    await ensureSetterSchema();
    const clientId = setterClientId(req);
    const callbackAt = req.body.callback_at ? new Date(req.body.callback_at) : null;
    if (callbackAt && Number.isNaN(callbackAt.getTime())) {
      return res.status(400).json({ error: 'Invalid callback time' });
    }
    const { rows } = await pool.query(`
      UPDATE prospects
      SET callback_at = $1, updated_at = NOW()
      WHERE id = $2 AND source = 'scout' AND client_id = $3
      RETURNING *
    `, [callbackAt ? callbackAt.toISOString() : null, req.params.id, clientId]);
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
    const clientId = setterClientId(req);
    const isHot = Boolean(req.body.is_hot);
    const { rows } = await pool.query(`
      UPDATE prospects
      SET is_hot = $1, updated_at = NOW()
      WHERE id = $2 AND source = 'scout' AND client_id = $3
      RETURNING *
    `, [isHot, req.params.id, clientId]);
    if (!rows.length) return res.status(404).json({ error: 'Lead not found' });
    res.json({ success: true, lead: mapLead(rows[0]) });
  } catch (err) {
    console.error('[setter] hot flag error:', err.message);
    res.status(500).json({ error: 'Unable to update hot flag' });
  }
});

router.post(['/api/leads/:id/enrich-phone', '/leads/:id/enrich-phone'], requireSetterWrite, async (req, res) => {
  let status = 'skipped';
  let payload = {};
  try {
    await ensureSetterSchema();
    const clientId = setterClientId(req);
    const { rows } = await pool.query(`
      SELECT *
      FROM prospects
      WHERE id = $1 AND source = 'scout' AND client_id = $2
    `, [req.params.id, clientId]);
    if (!rows.length) return res.status(404).json({ error: 'Lead not found' });

    const enrichment = await enrichPhoneWaterfall({
      ...rows[0],
      business_name: businessName(rows[0]),
      website: website(rows[0]),
      city: cityFor(rows[0]),
    }, { verbose: req.query.debug === 'true' });
    const phone = enrichment.phone;
    if (phone) {
      status = 'success';
      payload = { phone, source_hit: enrichment.source, source: enrichment.source, chain: enrichment.chain };
      await pool.query('UPDATE prospects SET phone = $1, enrichment_attempted = true, updated_at = NOW() WHERE id = $2 AND client_id = $3', [phone, req.params.id, clientId]);
      await pool.query(`
        INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
        VALUES ('setter', 'phone_enrich', $1, $2, $3, NOW(), $4)
      `, [req.params.id, JSON.stringify(payload), status, rows[0].client_id]);
      return res.json({ phone });
    }

    await pool.query('UPDATE prospects SET enrichment_attempted = true, updated_at = NOW() WHERE id = $1 AND client_id = $2', [req.params.id, clientId]);
    await pool.query(`
      INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
      VALUES ('setter', 'phone_enrich', $1, $2, $3, NOW(), $4)
    `, [req.params.id, JSON.stringify({ reason: 'No phone found', source_hit: null, chain: enrichment.chain }), status, rows[0].client_id]);
    res.json({ phone: null });
  } catch (err) {
    console.error('[setter] phone enrich error:', err.response?.data || err.message);
    const clientId = setterClientId(req);
    await pool.query('UPDATE prospects SET enrichment_attempted = true, updated_at = NOW() WHERE id = $1 AND client_id = $2', [req.params.id, clientId]).catch(() => {});
    await pool.query(`
      INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, error_msg, ran_at, client_id)
      SELECT 'setter', 'phone_enrich', p.id, $2, 'failed', $3, NOW(), p.client_id
      FROM prospects p
      WHERE p.id = $1 AND p.client_id = $4
    `, [req.params.id, JSON.stringify(payload), err.message, clientId]).catch(() => {});
    res.status(500).json({ error: 'Unable to enrich phone', phone: null });
  }
});

router.post(['/api/leads/:id/call-disposition', '/leads/:id/call-disposition'], requireSetterWrite, async (req, res) => {
  const client = await pool.connect();
  try {
    await ensureSetterSchema();
    const clientId = setterClientId(req);
    const setterId = Number(req.user?.id);
    const disposition = String(req.body.disposition || '').trim();
    const notes = String(req.body.notes || '').trim().slice(0, 5000);
    const duration = req.body.duration_seconds === '' || req.body.duration_seconds == null
      ? null
      : Number(req.body.duration_seconds);
    const requestedCallback = req.body.callback_at ? new Date(req.body.callback_at) : null;

    if (!DISPOSITION_SET.has(disposition)) return res.status(400).json({ error: 'Invalid disposition' });
    if (!Number.isInteger(setterId)) return res.status(400).json({ error: 'Setter identity is required' });
    if (duration != null && (!Number.isInteger(duration) || duration < 0 || duration > 86400)) {
      return res.status(400).json({ error: 'Duration must be whole seconds between 0 and 86400' });
    }
    if (requestedCallback && Number.isNaN(requestedCallback.getTime())) {
      return res.status(400).json({ error: 'Invalid callback time' });
    }
    if (disposition === 'incumbent_all_set' && requestedCallback) {
      const days = (requestedCallback.getTime() - Date.now()) / 86400000;
      if (days < 60 || days > 120) {
        return res.status(400).json({ error: 'All-set nurture callback must be 60 to 120 days out' });
      }
    }

    const callbackAt = resolveCallbackAt(disposition, requestedCallback);
    await client.query('BEGIN');
    const prospectResult = await client.query(`
      SELECT p.*, c.name AS company_name
      FROM prospects p
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.id = $1 AND p.source = 'scout' AND p.client_id = $2
      FOR UPDATE OF p
    `, [req.params.id, clientId]);
    if (!prospectResult.rows.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Lead not found' });
    }
    const prospect = prospectResult.rows[0];

    const dispositionResult = await client.query(`
      INSERT INTO call_dispositions
        (prospect_id, client_id, call_duration_seconds, disposition, notes, setter_id, source, callback_at)
      VALUES ($1, $2, $3, $4, $5, $6, 'manual_setter', $7)
      RETURNING *
    `, [prospect.id, prospect.client_id, duration, disposition, notes || null, setterId, callbackAt]);

    const sentiment = disposition === 'answered_interested'
      ? 'positive'
      : ['answered_not_interested', 'wrong_number', 'disconnected', 'gatekeeper_blocked'].includes(disposition)
        ? 'negative'
        : 'neutral';
    const outcome = JSON.stringify({
      disposition,
      duration_seconds: duration,
      callback_at: callbackAt ? callbackAt.toISOString() : null,
      source: 'manual_setter',
    });
    await client.query(`
      INSERT INTO touchpoints
        (prospect_id, channel, action_type, content_summary, outcome, sentiment, agent_id, external_ref, client_id)
      VALUES ($1, 'call', 'call_disposition', $2, $3, $4, $5, $6, $7)
    `, [
      prospect.id,
      `Manual call: ${disposition.replaceAll('_', ' ')}${notes ? ` — ${notes}` : ''}`,
      outcome,
      sentiment,
      String(setterId),
      `call_disposition:${dispositionResult.rows[0].id}`,
      prospect.client_id,
    ]);

    const updated = await applyProspectDisposition(client, {
      prospectId: prospect.id,
      clientId: prospect.client_id,
      disposition,
      callbackAt,
    });
    const countResult = await client.query(`
      SELECT (
        SELECT COUNT(*)::int FROM activity_log
        WHERE lead_id = $1 AND client_id = $2 AND action_type = 'call'
      ) + (
        SELECT COUNT(*)::int FROM call_dispositions
        WHERE prospect_id = $1 AND client_id = $2
      ) AS attempt_count
    `, [prospect.id, prospect.client_id]);
    await client.query('COMMIT');

    res.json({
      success: true,
      disposition: dispositionResult.rows[0],
      lead: mapLead({
        ...updated,
        company_name: prospect.company_name,
        attempt_count: countResult.rows[0]?.attempt_count,
      }),
    });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_rollbackErr) {}
    console.error('[setter] call disposition error:', err.message);
    res.status(500).json({ error: 'Unable to log call disposition' });
  } finally {
    client.release();
  }
});

router.get(['/api/activity', '/activity'], requireSetterRead, async (req, res) => {
  try {
    await ensureSetterSchema();
    const params = [];
    const clientScope = clientFilter(req, params, 'history.');
    const { rows } = await pool.query(`
      SELECT history.*
      FROM (
        SELECT
          t.id::text AS id,
          t.prospect_id AS lead_id,
          t.channel,
          t.action_type,
          t.content_summary AS activity_notes,
          t.agent_id AS setter_id,
          t.created_at,
          t.client_id,
          p.first_name,
          p.last_name,
          p.email,
          p.notes AS prospect_notes,
          c.name AS company_name,
          p.vertical,
          p.icp_score
        FROM touchpoints t
        JOIN prospects p ON p.id = t.prospect_id AND p.client_id = t.client_id
        LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id

        UNION ALL

        SELECT
          al.id::text AS id,
          al.lead_id,
          CASE al.action_type WHEN 'text' THEN 'sms' ELSE al.action_type END AS channel,
          al.action_type,
          al.notes AS activity_notes,
          al.setter_id,
          al.created_at,
          al.client_id,
          p.first_name,
          p.last_name,
          p.email,
          p.notes AS prospect_notes,
          c.name AS company_name,
          p.vertical,
          p.icp_score
        FROM activity_log al
        JOIN prospects p ON p.id = al.lead_id AND p.client_id = al.client_id
        LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      ) history
      WHERE true ${clientScope}
      ORDER BY history.created_at DESC
      LIMIT 100
    `, params);
    res.json(rows.map(row => ({
      id: row.id,
      lead_id: row.lead_id,
      business_name: businessName({ ...row, notes: row.prospect_notes }),
      channel: row.channel,
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
  if (!lead_id || !['email', 'text'].includes(action_type)) {
    return res.status(400).json({ error: 'Invalid activity' });
  }
  try {
    await ensureSetterSchema();
    const setterId = req.user?.id || null;
    const clientId = setterClientId(req);
    const channel = action_type === 'text' ? 'sms' : action_type;
    const { rows } = await pool.query(`
      INSERT INTO touchpoints
        (prospect_id, channel, action_type, content_summary, outcome, sentiment, agent_id, client_id)
      SELECT p.id, $2, 'manual_touch', $3, 'manual', 'neutral', $4, p.client_id
      FROM prospects p
      WHERE p.id = $1 AND p.source = 'scout' AND p.client_id = $5
      RETURNING touchpoints.*
    `, [lead_id, channel, notes || '', String(setterId || ''), clientId]);
    if (!rows.length) return res.status(404).json({ error: 'Lead not found' });
    await pool.query(`
      UPDATE prospects
      SET setter_status = CASE WHEN setter_status = 'new' THEN 'contacted' ELSE setter_status END,
          setter_updated_at = NOW()
      WHERE id = $1 AND client_id = $2
    `, [lead_id, clientId]);
    res.json({ success: true, activity: rows[0] });
  } catch (err) {
    console.error('[setter] activity create error:', err.message);
    res.status(500).json({ error: 'Unable to save activity' });
  }
});

ensureSetterSchema().catch(err => console.error('[startup] setter schema error:', err.message));

module.exports = router;
