const express = require('express');
const path = require('path');
const axios = require('axios');
const { randomUUID } = require('crypto');
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
const { assertRevenueFlag, loadRevenueFlags } = require('../utils/revenueFlags');
const revenue = require('../services/revenueService');
const { humanSetterPlaybook } = require('../utils/setterPlaybooks');
const {
  callbackSla,
  dispositionContract,
  shouldSampleCall,
  validateStructuredNotes,
} = require('../utils/setterQuality');
const {
  isAnchorPhoneSetter,
  phoneSetterError,
  validateDraftInput,
  validateStructuredDetails,
} = require('../utils/anchorPhoneSetter');

const router = express.Router();

const STAGES = ['new', 'contacted', 'follow_up', 'booked', 'dead'];
const SETTER_NOTES_MARKER = '\n\n--- setter notes ---\n';

function setterClientId(req) {
  if (hasMaxSecret(req)) return normalizeClientId(req.query.client_id);
  if (['setter', 'sales'].includes(req?.user?.role)) {
    const assigned = Number(req?.user?.client_id);
    return Number.isInteger(assigned) && assigned > 0 ? assigned : null;
  }
  const selected = Number(req?.session?.active_client_id || req?.user?.client_id);
  return Number.isInteger(selected) && selected > 0 ? selected : null;
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

function requireManagerWrite(req, res, next) {
  return sessionAuth(req, res, err => {
    if (err) return next(err);
    return requireRole('admin', 'manager')(req, res, next);
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
    ADD COLUMN IF NOT EXISTS assigned_setter_id INTEGER REFERENCES users(id),
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
    callback_sla: callbackSla(row.callback_at),
    is_synthetic: Boolean(row.is_synthetic),
    synthetic_label: row.synthetic_label || null,
    contact_prohibited: Boolean(row.is_synthetic || row.do_not_contact),
    is_hot: Boolean(row.is_hot),
    attempt_count: Number(row.attempt_count || 0),
    client_id: Number(row.client_id),
    category_priority: Number(row.category_priority || 99),
    priority_reason: row.priority_reason || null,
    phone_setter_v1: isAnchorPhoneSetter(row.client_id),
  };
}

const DEFAULT_LEAD_ORDER = `
  CASE WHEN p.client_id = 10 THEN CASE p.vertical
    WHEN 'cleaning_company_overflow' THEN 1 WHEN 'str_manager' THEN 2 WHEN 'property_manager' THEN 3
    WHEN 'realtor' THEN 4 WHEN 'restoration_remodeling_partner' THEN 5 WHEN 'commercial_office' THEN 6
    ELSE 99 END ELSE 99 END ASC,
  COALESCE(p.is_hot, false) DESC, p.icp_score DESC NULLS LAST, p.created_at DESC`;

async function getLeads(where = '', params = [], limit = 250, orderBy = DEFAULT_LEAD_ORDER, req = null) {
  const sort = orderBy || DEFAULT_LEAD_ORDER;
  const clientScope = clientFilter(req, params);
  const includeTest = req?.query?.include_test === 'true';
  params.push(includeTest);
  const testParam = params.length;
  const { rows } = await pool.query(`
    SELECT p.*,
      c.name AS company_name,
      CASE WHEN p.client_id = 10 THEN CASE p.vertical
        WHEN 'cleaning_company_overflow' THEN 1 WHEN 'str_manager' THEN 2 WHEN 'property_manager' THEN 3
        WHEN 'realtor' THEN 4 WHEN 'restoration_remodeling_partner' THEN 5 WHEN 'commercial_office' THEN 6
        ELSE 99 END ELSE 99 END AS category_priority,
      CASE WHEN p.client_id = 10 THEN 'Anchor priority ' || CASE p.vertical
        WHEN 'cleaning_company_overflow' THEN '1: cleaning company overflow' WHEN 'str_manager' THEN '2: STR manager'
        WHEN 'property_manager' THEN '3: property manager' WHEN 'realtor' THEN '4: realtor'
        WHEN 'restoration_remodeling_partner' THEN '5: restoration/remodeling partner' WHEN 'commercial_office' THEN '6: commercial office'
        ELSE '99: unprioritized category' END ELSE NULL END AS priority_reason,
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
      AND (
        (COALESCE(p.is_synthetic, false) = false AND COALESCE(p.do_not_contact, false) = false)
        OR ($${testParam}::boolean = true AND COALESCE(p.is_synthetic, false) = true)
      )
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
      AND COALESCE(p.is_synthetic, false) = false
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
        AND COALESCE(is_synthetic, false) = false
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
    // activity_log.setter_id is TEXT; call_dispositions.setter_id is INTEGER.
    // Cast $1 explicitly on each side so a shared parameter does not infer as
    // text and then fail with "operator does not exist: integer = text".
    const { rows } = await pool.query(`
      SELECT (
        SELECT COUNT(*)::int
        FROM activity_log al
        JOIN prospects p ON p.id = al.lead_id AND p.client_id = al.client_id
        WHERE al.action_type = 'call'
          AND al.setter_id = $1::text
          AND al.client_id = $2
          AND COALESCE(p.is_synthetic, false) = false
          AND al.created_at >= CURRENT_DATE
          AND al.created_at < CURRENT_DATE + INTERVAL '1 day'
      ) + (
        SELECT COUNT(*)::int
        FROM call_dispositions
        WHERE setter_id = $1::integer
          AND client_id = $2
          AND source = 'manual_setter'
          AND COALESCE(is_synthetic, false) = false
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

router.get(['/api/playbook', '/playbook'], requireSetterRead, async (req, res) => {
  try {
    const clientId = setterClientId(req);
    const vertical = String(req.query.vertical || 'general').trim().slice(0, 120);
    const clientResult = await pool.query('SELECT name FROM clients WHERE id = $1 LIMIT 1', [clientId]);
    res.json(humanSetterPlaybook({
      clientId,
      clientName: clientResult.rows[0]?.name || 'the client',
      vertical,
    }));
  } catch (err) {
    console.error('[setter] playbook error:', err.message);
    res.status(500).json({ error: 'Unable to load call playbook' });
  }
});

router.get(['/api/features', '/features'], requireSetterRead, async (req, res) => {
  try {
    await ensureSetterSchema();
    const clientId = setterClientId(req);
    const { rows } = await pool.query(`
      SELECT setter_pipeline_v2_enabled, setter_review_sample_percent
      FROM clients WHERE id = $1 LIMIT 1
    `, [clientId]);
    if (!rows[0]) return res.status(404).json({ error: 'Client not found' });
    res.json({
      client_id: clientId,
      pipeline_experience: rows[0].setter_pipeline_v2_enabled ? 'pilot_v2' : 'legacy',
      setter_pipeline_v2_enabled: rows[0].setter_pipeline_v2_enabled,
      review_sample_percent: Number(rows[0].setter_review_sample_percent || 0),
    });
  } catch (err) {
    res.status(500).json({ error: 'Unable to load setter features' });
  }
});

router.patch(['/api/features/pipeline', '/features/pipeline'], requireManagerWrite, async (req, res) => {
  try {
    await ensureSetterSchema();
    if (typeof req.body.enabled !== 'boolean') return res.status(400).json({ error: 'enabled must be boolean' });
    const clientId = setterClientId(req);
    const { rows } = await pool.query(`
      UPDATE clients
      SET setter_pipeline_v2_enabled = $1, setter_pipeline_v2_configured_at = NOW()
      WHERE id = $2
      RETURNING id, setter_pipeline_v2_enabled
    `, [req.body.enabled, clientId]);
    if (!rows[0]) return res.status(404).json({ error: 'Client not found' });
    res.json({
      client_id: rows[0].id,
      setter_pipeline_v2_enabled: rows[0].setter_pipeline_v2_enabled,
      pipeline_experience: rows[0].setter_pipeline_v2_enabled ? 'pilot_v2' : 'legacy',
      rollback_requires_database_change: false,
    });
  } catch (err) {
    res.status(500).json({ error: 'Unable to update Pipeline release flag' });
  }
});

router.post(['/api/test-prospects', '/test-prospects'], requireManagerWrite, async (req, res) => {
  try {
    await ensureSetterSchema();
    const clientId = setterClientId(req);
    const label = String(req.body.label || 'Setter rehearsal').trim().slice(0, 120);
    const business = String(req.body.business_name || 'Synthetic Pilot Prospect').trim().slice(0, 160);
    const vertical = String(req.body.vertical || 'commercial_office').trim().slice(0, 120);
    const token = randomUUID();
    const { rows } = await pool.query(`
      INSERT INTO prospects
        (first_name, last_name, email, phone, source, icp_score, status, setter_status,
         setter_visible, do_not_contact, is_synthetic, synthetic_label, vertical, notes, client_id)
      VALUES ('Test', 'Prospect', $1, NULL, 'scout', 100, 'cold', 'new', true, true, true, $2, $3, $4, $5)
      RETURNING *
    `, [`setter-test-${token}@example.invalid`, label, vertical, `${business} — synthetic.invalid`, clientId]);
    res.status(201).json({
      lead: mapLead(rows[0]),
      safeguards: {
        do_not_contact: true,
        outbound_prohibited: true,
        reporting_excluded: true,
        revenue_excluded: true,
        max_scoring_excluded: true,
      },
    });
  } catch (err) {
    console.error('[setter] synthetic prospect error:', err.message);
    res.status(500).json({ error: 'Unable to create synthetic prospect' });
  }
});

router.get(['/api/quality/metrics', '/quality/metrics'], requireSetterRead, async (req, res) => {
  try {
    await ensureSetterSchema();
    const clientId = setterClientId(req);
    const days = [7, 30, 90].includes(Number(req.query.days)) ? Number(req.query.days) : 30;
    const requestedSetter = Number(req.query.setter_id);
    const setterId = req.user?.role === 'setter'
      ? Number(req.user.id)
      : (Number.isInteger(requestedSetter) && requestedSetter > 0 ? requestedSetter : null);
    const { rows } = await pool.query(`
      WITH calls AS (
        SELECT * FROM call_dispositions
        WHERE client_id = $1
          AND source = 'manual_setter'
          AND COALESCE(is_synthetic, false) = false
          AND created_at >= NOW() - ($2::int * INTERVAL '1 day')
          AND ($3::int IS NULL OR setter_id = $3)
      ), callbacks AS (
        SELECT * FROM setter_callbacks
        WHERE client_id = $1
          AND COALESCE(is_synthetic, false) = false
          AND created_at >= NOW() - ($2::int * INTERVAL '1 day')
          AND ($3::int IS NULL OR created_by = $3 OR EXISTS (
            SELECT 1 FROM call_dispositions source_call
            WHERE source_call.id = setter_callbacks.source_disposition_id
              AND source_call.client_id = setter_callbacks.client_id
              AND source_call.setter_id = $3
          ))
      ), booked AS (
        SELECT COUNT(*)::int AS count
        FROM prospects
        WHERE client_id = $1
          AND COALESCE(is_synthetic, false) = false
          AND setter_status = 'booked'
          AND booked_at >= NOW() - ($2::int * INTERVAL '1 day')
          AND ($3::int IS NULL OR assigned_setter_id = $3)
      )
      SELECT
        (SELECT COUNT(*)::int FROM calls) AS attempts,
        (SELECT COUNT(*)::int FROM calls WHERE activity_result IN ('connection', 'conversation')) AS connections,
        (SELECT COUNT(*)::int FROM calls WHERE activity_result = 'conversation'
          AND (COALESCE((details->>'decision_maker_reached')::boolean, false)
            OR disposition IN ('answered_interested','answered_not_interested','answered_callback','incumbent_all_set','qualified','disqualified'))) AS decision_maker_conversations,
        (SELECT COUNT(*)::int FROM calls WHERE lifecycle_result = 'qualified') AS qualified_calls,
        (SELECT COUNT(*)::int FROM calls WHERE disposition IN ('answered_interested','answered_not_interested','answered_callback','qualified','disqualified')
          AND structured_notes IS NULL) AS incomplete_dispositions,
        (SELECT COUNT(*)::int FROM callbacks WHERE status = 'completed') AS callbacks_completed,
        (SELECT COUNT(*)::int FROM callbacks WHERE status IN ('pending','completed')) AS callbacks_due_total,
        (SELECT COUNT(*)::int FROM callbacks WHERE status = 'pending' AND due_at < NOW() - INTERVAL '15 minutes') AS overdue_callbacks,
        (SELECT count FROM booked) AS booked_opportunities
    `, [clientId, days, setterId]);
    const m = rows[0] || {};
    const pct = (num, den) => Number(den) ? +((Number(num) / Number(den)) * 100).toFixed(1) : 0;
    res.json({
      client_id: clientId,
      setter_id: setterId,
      window_days: days,
      attempts: Number(m.attempts || 0),
      connections: Number(m.connections || 0),
      connect_rate: pct(m.connections, m.attempts),
      decision_maker_conversations: Number(m.decision_maker_conversations || 0),
      callback_completion: pct(m.callbacks_completed, m.callbacks_due_total),
      callbacks_completed: Number(m.callbacks_completed || 0),
      qualified_meeting_rate: pct(m.booked_opportunities, m.decision_maker_conversations),
      qualified_calls: Number(m.qualified_calls || 0),
      booked_opportunities: Number(m.booked_opportunities || 0),
      incomplete_dispositions: Number(m.incomplete_dispositions || 0),
      overdue_callbacks: Number(m.overdue_callbacks || 0),
    });
  } catch (err) {
    console.error('[setter] quality metrics error:', err.message);
    res.status(500).json({ error: 'Unable to load setter quality metrics' });
  }
});

router.get(['/api/quality/reviews', '/quality/reviews'], requireManagerWrite, async (req, res) => {
  try {
    await ensureSetterSchema();
    const clientId = setterClientId(req);
    const status = req.query.status === 'completed' ? 'completed' : 'pending';
    const { rows } = await pool.query(`
      SELECT cd.*, p.first_name, p.last_name, p.vertical, c.name AS company_name, u.name AS setter_name
      FROM call_dispositions cd
      JOIN prospects p ON p.id = cd.prospect_id AND p.client_id = cd.client_id
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      LEFT JOIN users u ON u.id = cd.setter_id AND u.client_id = cd.client_id
      WHERE cd.client_id = $1 AND cd.review_required = true AND cd.review_status = $2
        AND COALESCE(cd.is_synthetic, false) = false
      ORDER BY cd.created_at ASC
      LIMIT 200
    `, [clientId, status]);
    res.json({ reviews: rows });
  } catch (err) {
    res.status(500).json({ error: 'Unable to load manager review sample' });
  }
});

router.post(['/api/quality/reviews/:id', '/quality/reviews/:id'], requireManagerWrite, async (req, res) => {
  try {
    await ensureSetterSchema();
    const clientId = setterClientId(req);
    const score = Number(req.body.score);
    if (!Number.isInteger(score) || score < 1 || score > 5) return res.status(400).json({ error: 'score must be 1 through 5' });
    if (typeof req.body.outcome_accurate !== 'boolean' || typeof req.body.notes_complete !== 'boolean') {
      return res.status(400).json({ error: 'outcome_accurate and notes_complete are required' });
    }
    const { rows } = await pool.query(`
      UPDATE call_dispositions
      SET review_status = 'completed', reviewed_by = $1, reviewed_at = NOW(), review_score = $2,
          review_outcome_accurate = $3, review_notes_complete = $4, review_notes = $5
      WHERE id = $6 AND client_id = $7 AND review_required = true AND review_status = 'pending'
        AND COALESCE(is_synthetic, false) = false
      RETURNING *
    `, [req.user?.id || null, score, req.body.outcome_accurate, req.body.notes_complete,
      String(req.body.notes || '').trim().slice(0, 2000) || null, req.params.id, clientId]);
    if (!rows[0]) return res.status(404).json({ error: 'Pending review not found' });
    res.json({ review: rows[0] });
  } catch (err) {
    res.status(500).json({ error: 'Unable to save manager review' });
  }
});

router.get(['/api/quality/suppression-verification', '/quality/suppression-verification'], requireManagerWrite, async (req, res) => {
  try {
    await ensureSetterSchema();
    const clientId = setterClientId(req);
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE COALESCE(p.do_not_contact, false) = true)::int AS suppressed_prospects,
        COUNT(*) FILTER (WHERE COALESCE(p.is_synthetic, false) = true AND COALESCE(p.do_not_contact, false) = false)::int AS unsafe_synthetic,
        COUNT(DISTINCT p.id) FILTER (WHERE COALESCE(p.do_not_contact, false) = true AND sc.status = 'pending' AND COALESCE(sc.is_synthetic, false) = false)::int AS suppressed_live_callbacks,
        COUNT(DISTINCT p.id) FILTER (WHERE COALESCE(p.do_not_contact, false) = true AND d.status IN ('draft','reviewed'))::int AS suppressed_follow_up_drafts
      FROM prospects p
      LEFT JOIN setter_callbacks sc ON sc.prospect_id = p.id AND sc.client_id = p.client_id
      LEFT JOIN setter_follow_up_drafts d ON d.prospect_id = p.id AND d.client_id = p.client_id
      WHERE p.client_id = $1
    `, [clientId]);
    const result = rows[0] || {};
    const violations = Number(result.unsafe_synthetic || 0) + Number(result.suppressed_live_callbacks || 0)
      + Number(result.suppressed_follow_up_drafts || 0);
    res.json({
      client_id: clientId,
      verified: violations === 0,
      suppressed_prospects: Number(result.suppressed_prospects || 0),
      violations: {
        unsafe_synthetic: Number(result.unsafe_synthetic || 0),
        live_callbacks: Number(result.suppressed_live_callbacks || 0),
        follow_up_drafts: Number(result.suppressed_follow_up_drafts || 0),
      },
    });
  } catch (err) {
    res.status(500).json({ error: 'Unable to verify tenant suppression' });
  }
});

router.patch(['/api/leads/:id/status', '/leads/:id/status'], requireSetterWrite, async (req, res) => {
  const { status } = req.body;
  if (!STAGES.includes(status)) return res.status(400).json({ error: 'Invalid status' });
  try {
    await ensureSetterSchema();
    const clientId = setterClientId(req);
    const handoffNote = String(req.body.handoff_note || '').slice(0, 5000);
    if (status === 'booked' && !handoffNote.trim()) {
      return res.status(400).json({ error: 'Qualified/booked prospects require a structured handoff note' });
    }
    if (status === 'dead' && !handoffNote.trim()) {
      return res.status(400).json({ error: 'Disqualified prospects require a reason' });
    }
    let rows;
    let handoff = null;

    if (status === 'booked') {
      const current = await pool.query(`
        SELECT *
        FROM prospects
        WHERE id = $1 AND source = 'scout' AND client_id = $2
          AND COALESCE(setter_visible, false) = true AND COALESCE(do_not_contact, false) = false
      `, [req.params.id, clientId]);
      if (!current.rows.length) return res.status(404).json({ error: 'Lead not found' });

      const anchorPhoneSetter = isAnchorPhoneSetter(clientId);
      const closer = anchorPhoneSetter ? null : await getLeviCloser(clientId);
      const notes = appendHandoffNote(current.rows[0].notes, handoffNote);
      const update = await pool.query(`
        UPDATE prospects
        SET setter_status = 'booked',
            setter_updated_at = NOW(),
            booked_at = COALESCE(booked_at, NOW()),
            closer_id = CASE WHEN $1 THEN closer_id ELSE COALESCE($2, closer_id) END,
            closer_status = CASE WHEN $1 THEN closer_status ELSE 'booked' END,
            assigned_setter_id = COALESCE(assigned_setter_id, $3),
            notes = $4
        WHERE id = $5 AND source = 'scout' AND client_id = $6
          AND COALESCE(setter_visible, false) = true AND COALESCE(do_not_contact, false) = false
        RETURNING *
      `, [anchorPhoneSetter, closer?.id || null, req.user?.role === 'setter' ? req.user.id : null, notes, req.params.id, clientId]);
      const visibleRow = update.rows.length
        ? await setSetterVisibility(pool, req.params.id, {
            reason: 'stage_change',
            source: 'scout',
            stageStatus: 'booked',
            clientId,
          })
        : null;
      rows = visibleRow ? [visibleRow] : [];

      if (closer && !anchorPhoneSetter) {
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
      } else if (anchorPhoneSetter) {
        handoff = { assigned: false, reason: 'Anchor walkthrough recorded; no closer email or agent handoff is permitted' };
      } else {
        handoff = { assigned: false, reason: 'No active closer user found' };
      }
    } else {
      const update = await pool.query(`
        UPDATE prospects
        SET setter_status = $1,
            status = CASE WHEN $1 = 'dead' THEN 'dead' ELSE status END,
            notes = CASE WHEN $1 = 'dead' THEN CONCAT(COALESCE(notes, ''), E'\n\nDisqualification reason: ', $4) ELSE notes END,
            setter_updated_at = NOW()
        WHERE id = $2 AND source = 'scout' AND client_id = $3
          AND COALESCE(setter_visible, false) = true AND COALESCE(do_not_contact, false) = false
        RETURNING *
      `, [status, req.params.id, clientId, handoffNote.trim()]);
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
        AND COALESCE(setter_visible, false) = true
        AND (COALESCE(do_not_contact, false) = false OR COALESCE(is_synthetic, false) = true)
    `, [req.params.id, clientId]);
    if (!current.rows.length) return res.status(404).json({ error: 'Lead not found' });
    const notes = composeNotes(current.rows[0].notes, incoming);
    const { rows } = await pool.query(`
      UPDATE prospects
      SET notes = $1, updated_at = NOW()
      WHERE id = $2 AND source = 'scout' AND client_id = $3
        AND COALESCE(setter_visible, false) = true
        AND (COALESCE(do_not_contact, false) = false OR COALESCE(is_synthetic, false) = true)
      RETURNING *
    `, [notes, req.params.id, clientId]);
    res.json({ success: true, lead: mapLead(rows[0]) });
  } catch (err) {
    console.error('[setter] notes error:', err.message);
    res.status(500).json({ error: 'Unable to save notes' });
  }
});

router.patch(['/api/leads/:id/callback', '/leads/:id/callback'], requireSetterWrite, async (req, res) => {
  const client = await pool.connect();
  try {
    await ensureSetterSchema();
    const clientId = setterClientId(req);
    const callbackAt = req.body.callback_at ? new Date(req.body.callback_at) : null;
    if (callbackAt && Number.isNaN(callbackAt.getTime())) {
      return res.status(400).json({ error: 'Invalid callback time' });
    }
    await client.query('BEGIN');
    const { rows } = await client.query(`
      UPDATE prospects
      SET callback_at = $1, updated_at = NOW()
      WHERE id = $2 AND source = 'scout' AND client_id = $3
        AND COALESCE(setter_visible, false) = true
        AND (COALESCE(do_not_contact, false) = false OR COALESCE(is_synthetic, false) = true)
      RETURNING *
    `, [callbackAt ? callbackAt.toISOString() : null, req.params.id, clientId]);
    if (!rows.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Lead not found' });
    }
    await client.query(`
      UPDATE setter_callbacks
      SET status = 'cancelled', cancelled_at = NOW(), updated_at = NOW()
      WHERE client_id = $1 AND prospect_id = $2 AND status = 'pending'
    `, [clientId, req.params.id]);
    if (callbackAt) {
      await client.query(`
        INSERT INTO setter_callbacks (client_id, prospect_id, due_at, created_by, is_synthetic)
        VALUES ($1, $2, $3, $4, $5)
      `, [clientId, req.params.id, callbackAt.toISOString(), req.user?.id || null, Boolean(rows[0].is_synthetic)]);
    }
    await client.query('COMMIT');
    res.json({ success: true, lead: mapLead(rows[0]) });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_rollbackErr) {}
    console.error('[setter] callback error:', err.message);
    res.status(500).json({ error: 'Unable to update callback' });
  } finally {
    client.release();
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
        AND COALESCE(setter_visible, false) = true AND COALESCE(do_not_contact, false) = false
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
        AND COALESCE(setter_visible, false) = true AND COALESCE(do_not_contact, false) = false
    `, [req.params.id, clientId]);
    if (!rows.length) return res.status(404).json({ error: 'Lead not found' });

    if (isAnchorPhoneSetter(clientId)) {
      return res.status(404).json({ error: 'Anchor phone enrichment is disabled for this manual rollout' });
    }
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
    const idempotencyKey = String(req.body.idempotency_key || req.get('Idempotency-Key') || '').trim().slice(0, 200);
    const rawDetails = req.body.details;
    const duration = req.body.duration_seconds === '' || req.body.duration_seconds == null
      ? null
      : Number(req.body.duration_seconds);
    const requestedCallback = req.body.callback_at ? new Date(req.body.callback_at) : null;

    if (!DISPOSITION_SET.has(disposition)) return res.status(400).json({ error: 'Invalid disposition' });
    if (!idempotencyKey) return res.status(400).json({ error: 'Idempotency key is required' });
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

    let structuredNotes;
    try {
      structuredNotes = validateStructuredNotes(disposition, req.body.structured_notes);
    } catch (error) {
      return res.status(error.status || 400).json({ error: error.message, code: error.code });
    }
    const contract = dispositionContract(disposition);
    const callbackAt = resolveCallbackAt(disposition, requestedCallback);
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`${clientId}:${idempotencyKey}`]);
    const prospectResult = await client.query(`
      SELECT p.*, c.name AS company_name
      FROM prospects p
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.id = $1 AND p.source = 'scout' AND p.client_id = $2
        AND COALESCE(p.setter_visible, false) = true
        AND (COALESCE(p.do_not_contact, false) = false OR COALESCE(p.is_synthetic, false) = true)
      FOR UPDATE OF p
    `, [req.params.id, clientId]);
    if (!prospectResult.rows.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Lead not found' });
    }
    const prospect = prospectResult.rows[0];
    const replay = await client.query(`
      SELECT cd.*, p.*, c.name AS company_name, cd.id AS disposition_record_id
      FROM call_dispositions cd
      JOIN prospects p ON p.id = cd.prospect_id AND p.client_id = cd.client_id
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE cd.client_id = $1 AND cd.idempotency_key = $2
      LIMIT 1
    `, [clientId, idempotencyKey]);
    if (replay.rows[0]) {
      await client.query('COMMIT');
      return res.json({
        success: true,
        idempotent: true,
        disposition: { ...replay.rows[0], id: replay.rows[0].disposition_record_id },
        lead: mapLead(replay.rows[0]),
      });
    }
    let details = {};
    if (isAnchorPhoneSetter(clientId)) {
      try {
        details = validateStructuredDetails(rawDetails, prospect.vertical);
      } catch (error) {
        await client.query('ROLLBACK');
        return res.status(error.status || 400).json({ error: error.message, code: error.code });
      }
    }

    const dispositionResult = await client.query(`
      INSERT INTO call_dispositions
        (prospect_id, client_id, call_duration_seconds, disposition, notes, setter_id, source,
         callback_at, details, structured_notes, activity_result, next_action, suppression_state,
         lifecycle_result, is_synthetic, idempotency_key)
      VALUES ($1, $2, $3, $4, $5, $6, 'manual_setter', $7, $8::jsonb, $9::jsonb,
        $10, $11, $12, $13, $14, $15)
      RETURNING *
    `, [
      prospect.id, prospect.client_id, duration, disposition, notes || null, setterId, callbackAt,
      JSON.stringify(details), structuredNotes ? JSON.stringify(structuredNotes) : null,
      contract.activity, contract.next_action, contract.suppression_state, contract.lifecycle_result,
      Boolean(prospect.is_synthetic), idempotencyKey,
    ]);
    const insertedDisposition = dispositionResult.rows[0];
    const sampleConfig = await client.query(
      'SELECT setter_review_sample_percent FROM clients WHERE id = $1',
      [prospect.client_id]
    );
    const reviewRequired = !prospect.is_synthetic && shouldSampleCall({
      clientId: prospect.client_id,
      dispositionId: insertedDisposition.id,
      samplePercent: sampleConfig.rows[0]?.setter_review_sample_percent,
    });
    if (reviewRequired) {
      await client.query(`
        UPDATE call_dispositions
        SET review_required = true, review_status = 'pending'
        WHERE id = $1 AND client_id = $2
      `, [insertedDisposition.id, prospect.client_id]);
      insertedDisposition.review_required = true;
      insertedDisposition.review_status = 'pending';
    }

    await client.query(`
      UPDATE setter_callbacks
      SET status = 'completed', completed_at = NOW(), completed_by_disposition_id = $1, updated_at = NOW()
      WHERE client_id = $2 AND prospect_id = $3 AND status = 'pending'
    `, [insertedDisposition.id, prospect.client_id, prospect.id]);
    if (callbackAt) {
      await client.query(`
        INSERT INTO setter_callbacks
          (client_id, prospect_id, source_disposition_id, due_at, created_by, is_synthetic)
        VALUES ($1, $2, $3, $4, $5, $6)
      `, [prospect.client_id, prospect.id, insertedDisposition.id, callbackAt, setterId, Boolean(prospect.is_synthetic)]);
    }

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
      synthetic: Boolean(prospect.is_synthetic),
      contract,
      structured_notes: structuredNotes,
      details,
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

async function anchorActionableProspect(prospectId, clientId) {
  const { rows } = await pool.query(`
    SELECT p.*, c.name AS company_name
    FROM prospects p
    LEFT JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
    WHERE p.id=$1 AND p.client_id=$2 AND p.source='scout'
      AND COALESCE(p.setter_visible,false)=true AND COALESCE(p.do_not_contact,false)=false
    LIMIT 1
  `, [prospectId, clientId]);
  return rows[0] || null;
}

function requireAnchorPhoneSetter(req, res, next) {
  if (!isAnchorPhoneSetter(setterClientId(req))) {
    return res.status(404).json({ error: 'Not found' });
  }
  return next();
}

function setterIdentity(req) {
  const id = Number(req.user?.id);
  if (!Number.isInteger(id)) throw phoneSetterError('Setter identity is required', 'SETTER_IDENTITY_REQUIRED');
  return id;
}

router.get(['/api/leads/:id/follow-up-drafts', '/leads/:id/follow-up-drafts'], requireSetterRead, requireAnchorPhoneSetter, async (req, res) => {
  try {
    const clientId = setterClientId(req);
    if (!await anchorActionableProspect(req.params.id, clientId)) return res.status(404).json({ error: 'Lead not found' });
    const { rows } = await pool.query(`
      SELECT id,client_id,prospect_id,channel,body,status,reviewer_id,reviewed_at,
        dismissed_by,dismissed_at,manual_sent_by,manual_sent_at,manual_send_reference,
        created_by,created_at,updated_at
      FROM setter_follow_up_drafts WHERE client_id=$1 AND prospect_id=$2 ORDER BY created_at DESC
    `, [clientId, req.params.id]);
    res.json({ drafts: rows });
  } catch (error) {
    console.error('[setter] follow-up draft list error:', error.message);
    res.status(500).json({ error: 'Unable to load follow-up drafts' });
  }
});

router.post(['/api/leads/:id/follow-up-drafts', '/leads/:id/follow-up-drafts'], requireSetterWrite, requireAnchorPhoneSetter, async (req, res) => {
  try {
    const clientId = setterClientId(req);
    const setterId = setterIdentity(req);
    if (!await anchorActionableProspect(req.params.id, clientId)) return res.status(404).json({ error: 'Lead not found' });
    const { channel, body } = validateDraftInput(req.body || {});
    const { rows } = await pool.query(`
      INSERT INTO setter_follow_up_drafts (client_id,prospect_id,channel,body,status,created_by)
      VALUES ($1,$2,$3,$4,'draft',$5) RETURNING *
    `, [clientId, req.params.id, channel, body, setterId]);
    res.status(201).json({ draft: rows[0] });
  } catch (error) {
    res.status(error.status || 500).json({ error: error.status ? error.message : 'Unable to create follow-up draft', code: error.code });
  }
});

router.patch(['/api/follow-up-drafts/:draftId', '/follow-up-drafts/:draftId'], requireSetterWrite, requireAnchorPhoneSetter, async (req, res) => {
  try {
    const clientId = setterClientId(req);
    const { channel, body } = validateDraftInput(req.body || {});
    const { rows } = await pool.query(`
      UPDATE setter_follow_up_drafts d SET channel=$3,body=$4,updated_at=NOW()
      WHERE d.id=$1 AND d.client_id=$2 AND d.status='draft'
        AND EXISTS (SELECT 1 FROM prospects p WHERE p.id=d.prospect_id AND p.client_id=d.client_id
          AND COALESCE(p.setter_visible,false)=true AND COALESCE(p.do_not_contact,false)=false)
      RETURNING d.*
    `, [req.params.draftId, clientId, channel, body]);
    if (!rows[0]) return res.status(404).json({ error: 'Editable draft not found' });
    res.json({ draft: rows[0] });
  } catch (error) {
    res.status(error.status || 500).json({ error: error.status ? error.message : 'Unable to edit follow-up draft', code: error.code });
  }
});

router.post(['/api/follow-up-drafts/:draftId/review', '/follow-up-drafts/:draftId/review'], requireSetterWrite, requireAnchorPhoneSetter, async (req, res) => {
  try {
    if (!['admin', 'manager'].includes(req.user?.role)) return res.status(403).json({ error: 'Reviewer role is required' });
    const clientId = setterClientId(req);
    const reviewerId = setterIdentity(req);
    const { rows } = await pool.query(`
      UPDATE setter_follow_up_drafts d SET status='reviewed',reviewer_id=$3,reviewed_at=NOW(),updated_at=NOW()
      WHERE d.id=$1 AND d.client_id=$2 AND d.status='draft'
        AND EXISTS (SELECT 1 FROM prospects p WHERE p.id=d.prospect_id AND p.client_id=d.client_id
          AND COALESCE(p.setter_visible,false)=true AND COALESCE(p.do_not_contact,false)=false)
      RETURNING d.*
    `, [req.params.draftId, clientId, reviewerId]);
    if (!rows[0]) return res.status(404).json({ error: 'Reviewable draft not found' });
    res.json({ draft: rows[0] });
  } catch (error) {
    res.status(error.status || 500).json({ error: error.status ? error.message : 'Unable to review follow-up draft', code: error.code });
  }
});

router.post(['/api/follow-up-drafts/:draftId/dismiss', '/follow-up-drafts/:draftId/dismiss'], requireSetterWrite, requireAnchorPhoneSetter, async (req, res) => {
  try {
    const clientId = setterClientId(req);
    const setterId = setterIdentity(req);
    const { rows } = await pool.query(`
      UPDATE setter_follow_up_drafts d SET status='dismissed',dismissed_by=$3,dismissed_at=NOW(),updated_at=NOW()
      WHERE d.id=$1 AND d.client_id=$2 AND d.status IN ('draft','reviewed')
        AND EXISTS (SELECT 1 FROM prospects p WHERE p.id=d.prospect_id AND p.client_id=d.client_id
          AND COALESCE(p.setter_visible,false)=true AND COALESCE(p.do_not_contact,false)=false)
      RETURNING d.*
    `, [req.params.draftId, clientId, setterId]);
    if (!rows[0]) return res.status(404).json({ error: 'Dismissible draft not found' });
    res.json({ draft: rows[0] });
  } catch (error) {
    res.status(error.status || 500).json({ error: error.status ? error.message : 'Unable to dismiss follow-up draft', code: error.code });
  }
});

router.post(['/api/follow-up-drafts/:draftId/log-sent', '/follow-up-drafts/:draftId/log-sent'], requireSetterWrite, requireAnchorPhoneSetter, async (req, res) => {
  const client = await pool.connect();
  try {
    const clientId = setterClientId(req);
    const setterId = setterIdentity(req);
    const reference = String(req.body?.manual_send_reference || '').trim().slice(0, 250) || null;
    await client.query('BEGIN');
    const draftResult = await client.query(`
      SELECT d.* FROM setter_follow_up_drafts d
      JOIN prospects p ON p.id=d.prospect_id AND p.client_id=d.client_id
      WHERE d.id=$1 AND d.client_id=$2 AND d.status='reviewed'
        AND COALESCE(p.setter_visible,false)=true AND COALESCE(p.do_not_contact,false)=false
      FOR UPDATE OF d
    `, [req.params.draftId, clientId]);
    const draft = draftResult.rows[0];
    if (!draft) {
      await client.query('ROLLBACK');
      return res.status(409).json({ error: 'Only a reviewed, actionable draft can be logged as sent' });
    }
    const updated = await client.query(`
      UPDATE setter_follow_up_drafts SET status='manual_sent',manual_sent_by=$3,manual_sent_at=NOW(),
        manual_send_reference=$4,updated_at=NOW() WHERE id=$1 AND client_id=$2 RETURNING *
    `, [draft.id, clientId, setterId, reference]);
    await client.query(`
      INSERT INTO touchpoints (prospect_id,channel,action_type,content_summary,outcome,sentiment,agent_id,external_ref,client_id)
      VALUES ($1,$2,'manual_follow_up_logged',$3,$4::jsonb,'neutral',$5,$6,$7)
    `, [draft.prospect_id, draft.channel, 'Operator confirmed manual send; no provider action was performed by Pulseforge.',
      JSON.stringify({ draft_id: draft.id, reviewed_at: draft.reviewed_at, manual_send_reference: reference }), String(setterId),
      `manual_follow_up:${draft.id}`, clientId]);
    await client.query('COMMIT');
    res.json({ draft: updated.rows[0], provider_action: false });
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    res.status(error.status || 500).json({ error: error.status ? error.message : 'Unable to log manual send', code: error.code });
  } finally {
    client.release();
  }
});

router.post(['/api/leads/:id/opportunity', '/leads/:id/opportunity'], requireSetterWrite, requireAnchorPhoneSetter, async (req, res) => {
  try {
    const clientId = setterClientId(req);
    const flags = await loadRevenueFlags(pool, clientId);
    assertRevenueFlag(flags, 'revenue_schema_enabled');
    assertRevenueFlag(flags, 'revenue_operator_writes_enabled');
    if (!await anchorActionableProspect(req.params.id, clientId)) return res.status(404).json({ error: 'Lead not found' });
    const result = await revenue.createOpportunity(clientId, {
      prospectId: req.params.id,
      serviceType: req.body?.serviceType,
      estimatedValueCents: req.body?.estimatedValueCents,
      estimatedCostCents: req.body?.estimatedCostCents,
      expectedCloseDate: req.body?.expectedCloseDate,
      source: 'manual',
      leadSourceDetail: 'anchor_phone_setter_v1',
      attributionStatus: 'deterministic',
      humanOwner: req.user?.name || req.user?.email || String(req.user?.id || ''),
    }, {
      idempotencyKey: req.get('Idempotency-Key'), correlationId: req.get('X-Correlation-ID') || randomUUID(),
      sourceSystem: 'anchor_phone_setter_v1', actorType: 'user', actorId: req.user?.id,
      followupRecommendationsEnabled: false,
    });
    res.status(result.idempotentReplay ? 200 : 201).json(result);
  } catch (error) {
    res.status(error.status || 500).json({ error: error.status ? error.message : 'Opportunity creation failed', code: error.code || 'INTERNAL_ERROR' });
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
          ,COALESCE(p.is_synthetic, false) AS is_synthetic
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
          ,COALESCE(p.is_synthetic, false) AS is_synthetic
        FROM activity_log al
        JOIN prospects p ON p.id = al.lead_id AND p.client_id = al.client_id
        LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      ) history
      WHERE COALESCE(history.is_synthetic, false) = false ${clientScope}
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
