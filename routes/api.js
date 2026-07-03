const express = require('express');
const crypto = require('crypto');
const router = express.Router();
const pool = require('../db');
const { requireAuth: sessionAuth, requireRole } = require('../middleware/auth');
const { ensureClientArchitecture, getActiveClients, getRequestClientId, normalizeClientId } = require('../utils/clientContext');
const { ensureCloserSchema } = require('../utils/closerSchema');
const { getTodoistSnapshot, getCurrentAnchor } = require('../utils/miraContext');
const { LIVE_WORKSTREAMS } = require('../utils/miraWorld');
const { publishBlogPost } = require('../utils/blogPublisher');
const {
  publishToGoogleBusiness,
  publishToFacebookPage,
  publishFayeComment,
  publishToLinkedInPage,
  publishToLinkedInPersonal,
  publishLinkComment,
} = require('../utils/publishPipeline');
const { normalizeVertical } = require('../utils/normalize');
const {
  prospectStateCountColumns,
  getProspectCounts,
  getProspectCountsByClient,
} = require('../utils/prospectCounts');
const { setSetterVisibility } = require('../utils/setterVisibility');
const { ensureTieredEnrichmentSchema } = require('../utils/tieredEnrichmentSchema');
const { deriveBusinessNameShort, ensureBusinessNameShortColumns } = require('../utils/businessNameShort');

const requireOperator = [sessionAuth, requireRole('admin', 'manager')];
const requireDashboardRead = [sessionAuth, requireRole('admin', 'manager', 'viewer', 'client')];
let prospectSetterAssignmentSchemaPromise;
let agentLogStatusSchemaPromise;
let linkedinSlotTestSchemaPromise;
const tableColumnCache = new Map();
const EXCLUDE_COMMAND_FEED_ACTIONS_SQL = `
  NOT (
    LOWER(REPLACE(COALESCE(al.agent_name, ''), '_agent', '')) = 'max'
    AND COALESCE(al.action, '') IN ('daily_brief', 'daily_digest')
  )
`;

function pct(num, den) {
  const n = Number(num || 0);
  const d = Number(den || 0);
  return d > 0 ? +((n / d) * 100).toFixed(1) : 0;
}

function analyticsWindowDays(value) {
  const days = Number.parseInt(value, 10);
  return [7, 30, 90].includes(days) ? days : 7;
}

async function tableColumns(tableName) {
  if (!tableColumnCache.has(tableName)) {
    tableColumnCache.set(tableName, pool.query(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = $1
    `, [tableName]).then(result => new Set(result.rows.map(row => row.column_name))));
  }
  return tableColumnCache.get(tableName);
}

function ensureProspectSetterAssignmentSchema() {
  if (!prospectSetterAssignmentSchemaPromise) {
    prospectSetterAssignmentSchemaPromise = pool.query(`
      ALTER TABLE prospects
      ADD COLUMN IF NOT EXISTS assigned_setter_id INTEGER REFERENCES users(id)
    `).catch(err => {
      prospectSetterAssignmentSchemaPromise = null;
      throw err;
    });
  }
  return prospectSetterAssignmentSchemaPromise;
}

function ensureAgentLogStatusSchema() {
  if (!agentLogStatusSchemaPromise) {
    agentLogStatusSchemaPromise = pool.query(`
      ALTER TABLE agent_log
      DROP CONSTRAINT IF EXISTS agent_log_status_check,
      ADD CONSTRAINT agent_log_status_check
        CHECK (status IN ('success', 'failed', 'skipped', 'pending', 'completed', 'posted', 'in_progress'))
    `).catch(err => {
      agentLogStatusSchemaPromise = null;
      throw err;
    });
  }
  return agentLogStatusSchemaPromise;
}

function ensureLinkedInSlotTestSchema() {
  if (!linkedinSlotTestSchemaPromise) {
    linkedinSlotTestSchemaPromise = (async () => {
      await pool.query(`
        ALTER TABLE pending_comments
          ADD COLUMN IF NOT EXISTS slot INTEGER,
          ADD COLUMN IF NOT EXISTS format TEXT,
          ADD COLUMN IF NOT EXISTS posted_at TIMESTAMPTZ,
          ADD COLUMN IF NOT EXISTS stats JSONB
      `);
      await pool.query(`
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'pending_comments_slot_check'
          ) THEN
            ALTER TABLE pending_comments
              ADD CONSTRAINT pending_comments_slot_check
              CHECK (slot IS NULL OR slot IN (1, 2));
          END IF;
        END $$;
      `);
    })().catch(err => {
      linkedinSlotTestSchemaPromise = null;
      throw err;
    });
  }
  return linkedinSlotTestSchemaPromise;
}

function parseOptionalInt(value, field) {
  if (value === undefined || value === null || value === '') return 0;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    const err = new Error(`${field} must be a non-negative integer`);
    err.statusCode = 400;
    throw err;
  }
  return parsed;
}

router.get('/api/me', sessionAuth, (req, res) => {
  res.json({ user: req.user, active_client_id: getRequestClientId(req) || req.session?.active_client_id || 1 });
});

router.get('/api/clients', requireOperator, async (req, res) => {
  try {
    res.json({
      active_client_id: req.session.active_client_id || 1,
      clients: await getActiveClients(),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/api/clients/active', requireOperator, async (req, res) => {
  const clientId = normalizeClientId(req.body.client_id || req.query.client_id);
  try {
    const clients = await getActiveClients();
    if (!clients.find(c => c.id === clientId)) {
      return res.status(404).json({ error: 'Client not found' });
    }
    req.session.active_client_id = clientId;
    res.json({ ok: true, active_client_id: clientId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/agent-visibility', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const [clientResult, prospectsWithEmail] = await Promise.all([
      pool.query('SELECT id, facebook_url, gbp_url, max_email, enabled_agents FROM clients WHERE id = $1', [clientId]),
      pool.query(`
        SELECT COUNT(*)::int AS count
        FROM prospects
        WHERE client_id = $1 AND NULLIF(TRIM(email), '') IS NOT NULL
      `, [clientId]),
    ]);
    const client = clientResult.rows[0] || {};
    const hasTwilio = Boolean(process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN && process.env.TWILIO_PHONE_NUMBER);
    const hasCalendar = Boolean(
      (process.env.GOOGLE_CALENDAR_REFRESH_TOKEN || process.env.GOOGLE_CALENDAR_TOKEN || process.env.GOOGLE_REFRESH_TOKEN) &&
      (process.env.GOOGLE_CALENDAR_ID || process.env.GOOGLE_CLIENT_ID)
    );

    res.json({
      scout: true,
      emmett: Number(prospectsWithEmail.rows[0]?.count || 0) > 0,
      paige: Boolean(client.facebook_url || client.gbp_url),
      faye: Boolean(client.facebook_url),
      vera: Boolean(client.gbp_url),
      riley: Boolean(client.max_email),
      max: true,
      rex: true,
      sam: hasTwilio,
      cal: hasCalendar,
      link: false,
      ivy: false,
      sketch: false,
      penny: false,
      analytics: true,
      enabled_agents: client.enabled_agents || [],
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/pipeline', requireDashboardRead, async (req, res) => {
  try {
    await ensureClientArchitecture();
    await ensureCloserSchema();
    await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS assigned_city TEXT');
    await pool.query(`
      INSERT INTO clients (name, slug, email, city, state, active)
      VALUES ('McLeod Legal Services', 'mcleod', 'ashley@mcleodlegal.com',
              'Manchester', 'NH', false)
      ON CONFLICT (slug) DO NOTHING
    `);

    const agentNames = [
      'Scout', 'Emmett', 'Riley', 'Paige', 'Max', 'Rex', 'Vera', 'Faye',
      'Link', 'Sam', 'Cal', 'CalBatch', 'WarmSignal', 'Analytics',
      'Sketch', 'SetterHandoff',
    ];
    const agentKeys = {
      scout_agent: 'Scout', scout: 'Scout',
      emmett_agent: 'Emmett', email_agent: 'Emmett', emmett: 'Emmett',
      riley_agent: 'Riley', riley: 'Riley',
      paige_agent: 'Paige', paige: 'Paige',
      max_agent: 'Max', max: 'Max',
      rex_agent: 'Rex', rex: 'Rex',
      vera_agent: 'Vera', vera: 'Vera',
      facebook_agent: 'Faye', faye: 'Faye',
      linkedin_agent: 'Link', link: 'Link',
      sam_agent: 'Sam', sam: 'Sam',
      cal_agent: 'Cal', cal: 'Cal',
      cal_batch_agent: 'CalBatch', calbatch: 'CalBatch', cal_batch: 'CalBatch',
      warm_signal_agent: 'WarmSignal', warm_signal: 'WarmSignal', warmsignal: 'WarmSignal',
      analytics_agent: 'Analytics', analytics: 'Analytics',
      sketch_agent: 'Sketch', sketch: 'Sketch',
      setter_handoff_agent: 'SetterHandoff', setterhandoff: 'SetterHandoff', handoff_utility: 'SetterHandoff',
    };

    const clientId = getRequestClientId(req);
    const clientScoped = req.user?.role === 'client';
    const clientParams = clientScoped ? [clientId] : [];
    const clientsFilter = clientScoped ? 'AND c.id = $1' : '';
    const scopedAnd = clientScoped ? 'AND client_id = $1' : '';
    const scopedWhere = clientScoped ? 'WHERE client_id = $1' : '';
    const prospectClientAnd = clientScoped ? 'AND p.client_id = $1' : '';
    const activityClientJoin = clientScoped ? 'AND al.client_id = p.client_id' : '';

    const [clients, revenueMrr, revenueBooked, revenuePayouts, setters, closers, logs] = await Promise.all([
      pool.query(`
        SELECT
          c.id, c.name, c.slug, c.email, c.city, c.state,
          c.max_email, c.active, c.created_at,
          ${prospectStateCountColumns('p', { includeDead: !clientScoped })},
          COUNT(DISTINCT p.id) FILTER (
            WHERE p.setter_status = 'booked')::int as booked_count,
          COUNT(DISTINCT p.id) FILTER (
            WHERE p.setter_status = 'closed')::int as closed_count
        FROM clients c
        LEFT JOIN prospects p ON p.client_id = c.id
        WHERE c.slug != 'pulseforge'
          ${clientsFilter}
        GROUP BY c.id
        ORDER BY c.created_at ASC
      `, clientParams),
      pool.query(`SELECT COALESCE(SUM(mrr_amount), 0)::numeric as confirmed_mrr FROM commissions WHERE status != 'void' ${scopedAnd}`, clientParams),
      pool.query(`SELECT COUNT(*)::int as booked_pending FROM prospects WHERE setter_status = 'booked' AND closed_at IS NULL ${scopedAnd}`, clientParams),
      pool.query(`
        SELECT
          COALESCE(SUM(commission_amt) FILTER (
            WHERE status = 'pending'), 0)::numeric as pending_payout,
          COALESCE(SUM(commission_amt) FILTER (
            WHERE status = 'paid'), 0)::numeric as paid_out
        FROM commissions
        ${scopedWhere}
      `, clientParams),
      pool.query(`
        SELECT
          u.id, u.name, u.assigned_city,
          COUNT(DISTINCT p.id) FILTER (
            WHERE p.setter_visible = true
            AND p.setter_status = 'new')::int as queue_size,
          COUNT(DISTINCT al.id) FILTER (
            WHERE al.action_type = 'call'
            AND DATE(al.created_at) = CURRENT_DATE)::int as calls_today,
          COUNT(DISTINCT p.id) FILTER (
            WHERE p.setter_status = 'booked'
            AND p.booked_at >= DATE_TRUNC('week', CURRENT_DATE)
            )::int as booked_this_week
        FROM users u
        LEFT JOIN prospects p ON p.setter_visible = true
        LEFT JOIN activity_log al ON al.setter_id::text = u.id::text ${activityClientJoin}
        WHERE u.role = 'setter' AND u.active = true
          ${prospectClientAnd}
        GROUP BY u.id, u.name, u.assigned_city
        ORDER BY u.name ASC
      `, clientParams),
      pool.query(`
        SELECT
          u.id, u.name,
          COUNT(p.id) FILTER (
            WHERE p.closer_id = u.id
            AND p.setter_status = 'booked'
            AND p.closed_at IS NULL)::int as pending_calls,
          COUNT(p.id) FILTER (
            WHERE p.closer_id = u.id
            AND p.closer_status = 'showed'
            AND p.booked_at >= DATE_TRUNC('week', CURRENT_DATE)
            )::int as showed_this_week,
          COUNT(p.id) FILTER (
            WHERE p.closer_id = u.id
            AND p.setter_status = 'closed'
            AND p.closed_at >= DATE_TRUNC('month', CURRENT_DATE)
            )::int as closed_this_month,
          COALESCE(SUM(p.mrr_value) FILTER (
            WHERE p.setter_status = 'closed'
            AND p.closed_at >= DATE_TRUNC('month', CURRENT_DATE)
            ), 0)::numeric as mrr_this_month,
          COUNT(p.id) FILTER (
            WHERE p.closer_id = u.id
            AND p.setter_status = 'booked')::int as total_booked,
          COUNT(p.id) FILTER (
            WHERE p.closer_id = u.id
            AND p.closer_status = 'showed')::int as total_showed
        FROM users u
        LEFT JOIN prospects p ON p.closer_id = u.id
        WHERE u.role = 'closer' AND u.active = true
          ${prospectClientAnd}
        GROUP BY u.id, u.name
        ORDER BY u.name ASC
      `, clientParams),
      pool.query(`
        SELECT DISTINCT ON (agent_name)
          agent_name, status, ran_at, error_msg, duration_ms
        FROM agent_log
        ${scopedWhere}
        ORDER BY agent_name, ran_at DESC
      `, clientParams),
    ]);

    const latestByAgent = {};
    logs.rows.forEach(row => {
      const normalized = agentKeys[String(row.agent_name || '').toLowerCase()];
      if (normalized && !latestByAgent[normalized]) latestByAgent[normalized] = row;
    });

    res.json({
      refreshed_at: new Date().toISOString(),
      prospect_scope: clientScoped ? 'client' : 'global',
      clients: clients.rows,
      revenue: {
        confirmed_mrr: Number(revenueMrr.rows[0]?.confirmed_mrr || 0),
        booked_pending: Number(revenueBooked.rows[0]?.booked_pending || 0),
        pending_payout: Number(revenuePayouts.rows[0]?.pending_payout || 0),
        paid_out: Number(revenuePayouts.rows[0]?.paid_out || 0),
        mrr_target: 5000,
        breakeven: 250,
      },
      setters: setters.rows,
      closers: closers.rows.map(row => ({
        ...row,
        show_rate: Number(row.total_booked || 0) ? +((Number(row.total_showed || 0) / Number(row.total_booked || 0)) * 100).toFixed(1) : 0,
        commission_earned: +(Number(row.mrr_this_month || 0) * 0.15).toFixed(2),
      })),
      agents: agentNames.map(name => ({
        name,
        ...(latestByAgent[name] || { status: 'never_run', ran_at: null, error_msg: null, duration_ms: null }),
      })),
    });
  } catch (err) {
    console.error('[pipeline] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Agent status for dashboard (deduplicated — was registered twice in server.js)
router.get('/api/agent-status', requireDashboardRead, async (req, res) => {
  try {
    const requestClientId = getRequestClientId(req);
    const clientScoped = req.user?.role === 'client';
    const prospectClientId = clientScoped ? requestClientId : null;
    const includeDead = !clientScoped;
    const [prospectBreakdown, touchpoints, pending, agentRuns, channels, weeklyTouchpoints] = await Promise.all([
      getProspectCounts(pool, { clientId: prospectClientId, includeDead }),
      pool.query('SELECT COUNT(*) FROM touchpoints WHERE client_id = $1', [requestClientId]),
      pool.query('SELECT COUNT(*) FROM pending_comments WHERE status = $1 AND client_id = $2', ['pending', requestClientId]),
      pool.query('SELECT agent_name, COUNT(*) as runs, MAX(ran_at) as last_run FROM agent_log WHERE client_id = $1 GROUP BY agent_name', [requestClientId]),
      pool.query('SELECT channel, COUNT(*) as count FROM pending_comments WHERE client_id = $1 GROUP BY channel', [requestClientId]),
      pool.query('SELECT COUNT(*) FROM touchpoints WHERE client_id = $1 AND created_at > NOW() - INTERVAL \'7 days\'', [requestClientId])
    ]);
    const prospectClients = await getProspectCountsByClient(pool, { clientId: prospectClientId, includeDead });

    const runMap = {};
    agentRuns.rows.forEach(r => { runMap[r.agent_name] = parseInt(r.runs); });

    const activeProspects = Number(prospectBreakdown.active || 0);
    const coldProspects = Number(prospectBreakdown.cold || 0);
    const deadProspects = Number(prospectBreakdown.dead || 0);
    const totalProspects = Number(prospectBreakdown.total || 0);
    const totalTouchpoints = parseInt(touchpoints.rows[0].count);
    const fbPending = channels.rows.find(c => c.channel === 'facebook')?.count || 0;
    const liPending = channels.rows.find(c => c.channel === 'linkedin')?.count || 0;

    const rings = {
      scout:  Math.min((runMap['scout_agent'] || 0) / 20, 1),
      link:   totalTouchpoints > 0 ? Math.min(parseInt(liPending) / Math.max(runMap['linkedin_agent'] || 1, 1), 1) : 0,
      faye:   totalTouchpoints > 0 ? Math.min(parseInt(fbPending) / Math.max(runMap['facebook_agent'] || 1, 1), 1) : 0,
      emmett: Math.min((runMap['email_agent'] || 0) / Math.max(totalProspects, 1), 1),
      max:    runMap['max_agent'] ? 1 : 0,
      rex:    runMap['rex_agent'] ? 1 : 0
    };

    res.json({
      prospects: activeProspects,
      prospectBreakdown: {
        active: activeProspects,
        cold: coldProspects,
        ...(includeDead ? { dead: deadProspects } : {}),
        total: totalProspects,
      },
      prospectClients,
      prospectScope: clientScoped ? 'client' : 'global',
      touchpoints: totalTouchpoints,
      pending: parseInt(pending.rows[0].count),
      weeklyTouchpoints: parseInt(weeklyTouchpoints.rows[0].count),
      agentRuns: runMap,
      rings,
      channels: channels.rows
    });
  } catch (err) {
    console.error('API error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Get pending approvals
router.get('/api/approvals', requireOperator, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT id, author_name, author_title, post_content, comment, channel, status, created_at, client_id
      FROM pending_comments
      WHERE status = 'pending' AND client_id = $1
      ORDER BY created_at DESC
      LIMIT 50
    `, [clientId]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Approve or reject a comment
router.post('/api/approvals/:id', requireOperator, async (req, res) => {
  const { id } = req.params;
  const { action } = req.body;
  if (!['approved', 'rejected'].includes(action)) {
    return res.status(400).json({ error: 'Invalid action' });
  }
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(
      'UPDATE pending_comments SET status = $1 WHERE id = $2 AND client_id = $3 RETURNING *',
      [action, id, clientId]
    );
    res.json({ success: true, id, action });

    const item = result.rows[0];
    if (item && action === 'approved') {
      const publishers = {
        blog:             () => publishBlogPost(item),
        google_business:  () => publishToGoogleBusiness(item),
        facebook_page:    () => publishToFacebookPage(item),
        facebook:         () => publishFayeComment(item),
        linkedin_page:    () => publishToLinkedInPage(item),
        linkedin_personal:() => publishToLinkedInPersonal(item),
        linkedin:         () => publishLinkComment(item),
      };
      const publish = publishers[item.channel];
      if (publish) {
        publish().catch(err =>
          console.error(`[Publisher:${item.channel}] Unhandled error:`, err.message)
        );
      }
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/api/linkedin-personal-posts/:id/stats', requireOperator, async (req, res) => {
  const { id } = req.params;
  try {
    await ensureLinkedInSlotTestSchema();
    const clientId = getRequestClientId(req);
    const postRes = await pool.query(`
      SELECT id, channel, posted_at, created_at
      FROM pending_comments
      WHERE id = $1 AND client_id = $2
      LIMIT 1
    `, [id, clientId]);

    const post = postRes.rows[0];
    if (!post) return res.status(404).json({ error: 'Post not found' });
    if (post.channel !== 'linkedin_personal') {
      return res.status(400).json({ error: 'Stats capture is only for linkedin_personal posts' });
    }

    const stats = {
      impressions: parseOptionalInt(req.body.impressions, 'impressions'),
      reactions: parseOptionalInt(req.body.reactions, 'reactions'),
      comments: parseOptionalInt(req.body.comments, 'comments'),
      outside_network_comments: parseOptionalInt(req.body.outside_network_comments, 'outside_network_comments'),
      saves: parseOptionalInt(req.body.saves, 'saves'),
      dms: parseOptionalInt(req.body.dms, 'dms'),
      top_viewer_segment: req.body.top_viewer_segment == null
        ? null
        : String(req.body.top_viewer_segment).trim().slice(0, 200),
      captured_at: new Date().toISOString(),
    };

    const warnings = [];
    const maturityBase = post.posted_at || post.created_at;
    if (!post.posted_at) warnings.push('Post has no posted_at timestamp yet; using created_at for the 48h maturity check.');
    if (maturityBase && Date.now() - new Date(maturityBase).getTime() < 48 * 60 * 60 * 1000) {
      warnings.push('Post is younger than 48h; stats may not be mature yet.');
    }

    const updated = await pool.query(`
      UPDATE pending_comments
      SET stats = $1
      WHERE id = $2 AND client_id = $3
      RETURNING id, slot, format, stats
    `, [JSON.stringify(stats), id, clientId]);

    res.json({ success: true, post: updated.rows[0], warnings });
  } catch (err) {
    res.status(err.statusCode || 500).json({ error: err.message });
  }
});

// Prospects table
router.get('/api/prospects', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT
        p.id, p.company_id, p.first_name, p.last_name, p.email, p.phone,
        p.vertical, p.status, p.icp_score, p.do_not_contact, p.notes, p.last_contacted_at, p.created_at,
        p.assigned_setter_id,
        su.name AS assigned_setter_name,
        c.name as company_name, c.location AS city,
        COUNT(t.id)::int as touchpoint_count
      FROM prospects p
      LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
      LEFT JOIN users su ON su.id = p.assigned_setter_id
      LEFT JOIN touchpoints t ON t.prospect_id = p.id AND t.client_id = p.client_id
      WHERE p.do_not_contact = false
        AND p.client_id = $1
      GROUP BY p.id, c.name, c.location, su.name
      ORDER BY p.icp_score DESC NULLS LAST
      LIMIT 200
    `, [clientId]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/prospect-pipeline', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT
        p.id, p.vertical, p.status, p.icp_score, p.last_contacted_at,
        c.name as company_name
      FROM prospects p
      LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
      WHERE p.do_not_contact = false
        AND p.client_id = $1
      ORDER BY p.icp_score DESC NULLS LAST
      LIMIT 200
    `, [clientId]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/enrichment/manual-queue', requireDashboardRead, async (req, res) => {
  try {
    await ensureTieredEnrichmentSchema();
    const clientId = getRequestClientId(req);
    const status = String(req.query.status || 'open').trim().toLowerCase();
    const result = await pool.query(`
      SELECT
        q.id,
        q.client_id,
        q.prospect_id,
        q.company_name,
        q.website,
        q.missing_fields,
        q.candidate_names,
        q.candidate_emails,
        q.partial_data,
        q.status,
        q.last_attempted_at,
        q.created_at,
        q.updated_at,
        p.first_name,
        p.last_name,
        p.email,
        p.email_status,
        p.email_verified,
        p.email_verification_method,
        p.do_not_contact,
        p.practice_area,
        p.firm_size,
        p.vertical,
        p.icp_score
      FROM enrichment_manual_queue q
      LEFT JOIN prospects p
        ON p.id = q.prospect_id
        AND p.client_id = q.client_id
      WHERE q.client_id = $1
        AND q.status = $2
      ORDER BY q.updated_at DESC, q.id DESC
    `, [clientId, status]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

async function selectUpdatedProspect(id, clientId) {
  const result = await pool.query(`
    SELECT
      p.id, p.company_id, p.first_name, p.last_name, p.email, p.phone,
      p.vertical, p.status, p.icp_score, p.do_not_contact, p.notes,
      p.last_contacted_at, p.created_at,
      p.assigned_setter_id,
      su.name AS assigned_setter_name,
      c.name AS company_name, c.location AS city,
      COALESCE(tp.touchpoint_count, 0)::int AS touchpoint_count
    FROM prospects p
    LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
    LEFT JOIN users su ON su.id = p.assigned_setter_id
    LEFT JOIN (
      SELECT prospect_id, COUNT(*)::int AS touchpoint_count
      FROM touchpoints
      WHERE client_id = $2
      GROUP BY prospect_id
    ) tp ON tp.prospect_id = p.id
    WHERE p.id = $1 AND p.client_id = $2
    LIMIT 1
  `, [id, clientId]);
  return result.rows[0] || null;
}

function cleanNullable(value, maxLength = 500) {
  if (value === null) return null;
  if (value === undefined) return undefined;
  const cleaned = String(value).trim().slice(0, maxLength);
  return cleaned || null;
}

function combineLocation(city, state) {
  const cleanedCity = cleanNullable(city, 120);
  const cleanedState = cleanNullable(state, 40);
  return [cleanedCity, cleanedState].filter(Boolean).join(', ') || null;
}

function splitLocation(location) {
  const parts = String(location || '').split(',').map(part => part.trim()).filter(Boolean);
  if (parts.length >= 2) return { city: parts.slice(0, -1).join(', '), state: parts[parts.length - 1] };
  const words = String(location || '').trim().split(/\s+/).filter(Boolean);
  if (words.length > 1 && words[words.length - 1].length <= 3) {
    return { city: words.slice(0, -1).join(' '), state: words[words.length - 1] };
  }
  return { city: String(location || '').trim(), state: '' };
}

router.put('/api/prospects/:id', requireOperator, async (req, res) => {
  const client = await pool.connect();
  try {
    const clientId = getRequestClientId(req);
    const currentRes = await client.query(`
      SELECT p.*, c.name AS company_name, c.location AS company_location
      FROM prospects p
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.id = $1 AND p.client_id = $2
      LIMIT 1
    `, [req.params.id, clientId]);
    if (!currentRes.rows.length) return res.status(404).json({ error: 'Prospect not found' });

    const current = currentRes.rows[0];
    const has = key => Object.prototype.hasOwnProperty.call(req.body || {}, key);
    const prospectFields = [];
    const values = [];
    const addProspectField = (column, value) => {
      if (current[column] === value) return;
      values.push(value);
      prospectFields.push(`${column} = $${values.length}`);
    };

    if (has('first_name')) addProspectField('first_name', cleanNullable(req.body.first_name, 120));
    if (has('last_name')) addProspectField('last_name', cleanNullable(req.body.last_name, 120));
    if (has('email')) addProspectField('email', cleanNullable(req.body.email, 320));
    if (has('phone')) addProspectField('phone', cleanNullable(req.body.phone, 80));
    if (has('vertical')) addProspectField('vertical', normalizeVertical(cleanNullable(req.body.vertical, 120)));
    if (has('notes')) addProspectField('notes', cleanNullable(req.body.notes, 4000));

    if (has('icp_score')) {
      const score = req.body.icp_score === null || req.body.icp_score === '' ? null : Number(req.body.icp_score);
      if (score !== null && (!Number.isFinite(score) || score < 0 || score > 100)) {
        return res.status(400).json({ error: 'ICP score must be between 0 and 100' });
      }
      addProspectField('icp_score', score);
    }

    if (has('status')) {
      const status = String(req.body.status || '').toLowerCase();
      if (!['cold', 'contacted', 'warm', 'dead', 'disqualified', 'closed', 'bounced', 'auto_responder', 'do_not_email'].includes(status)) return res.status(400).json({ error: 'Invalid status' });
      addProspectField('status', status);
    }

    if (has('do_not_contact')) {
      addProspectField('do_not_contact', Boolean(req.body.do_not_contact));
    }

    const companyChanged = has('company') && cleanNullable(req.body.company, 220) !== (current.company_name || null);
    const existingLocation = splitLocation(current.company_location);
    const location = (has('city') || has('state')) ? combineLocation(
      has('city') ? req.body.city : existingLocation.city,
      has('state') ? req.body.state : existingLocation.state
    ) : undefined;
    const locationChanged = location !== undefined && location !== (current.company_location || null);

    if (companyChanged || locationChanged) {
      await ensureBusinessNameShortColumns(pool);
    }

    await client.query('BEGIN');

    if (companyChanged || locationChanged) {
      const companyName = has('company') ? cleanNullable(req.body.company, 220) : current.company_name;
      const companyNameForWrite = companyName || 'Unknown Company';
      const companyLocation = location !== undefined ? location : current.company_location;
      const shortName = deriveBusinessNameShort(companyNameForWrite);
      if (current.company_id) {
        const setParts = [];
        const companyValues = [];
        if (companyChanged) {
          companyValues.push(companyNameForWrite);
          setParts.push(`name = $${companyValues.length}`);
          companyValues.push(shortName.business_name_short);
          setParts.push(`business_name_short = $${companyValues.length}`);
          companyValues.push(shortName.confidence);
          setParts.push(`business_name_short_confidence = $${companyValues.length}`);
          companyValues.push(shortName.flags);
          setParts.push(`business_name_short_flags = $${companyValues.length}::text[]`);
        } else {
          companyValues.push(shortName.business_name_short);
          setParts.push(`business_name_short = COALESCE(NULLIF(business_name_short, ''), $${companyValues.length})`);
          companyValues.push(shortName.confidence);
          setParts.push(`business_name_short_confidence = COALESCE(NULLIF(business_name_short_confidence, ''), $${companyValues.length})`);
          companyValues.push(shortName.flags);
          setParts.push(`business_name_short_flags = CASE
            WHEN COALESCE(array_length(business_name_short_flags, 1), 0) = 0 THEN $${companyValues.length}::text[]
            ELSE business_name_short_flags
          END`);
        }
        if (locationChanged) {
          companyValues.push(companyLocation);
          setParts.push(`location = $${companyValues.length}`);
        }
        if (setParts.length) {
          companyValues.push(current.company_id, clientId);
          await client.query(`
            UPDATE companies
            SET ${setParts.join(', ')}, updated_at = NOW()
            WHERE id = $${companyValues.length - 1} AND client_id = $${companyValues.length}
          `, companyValues);
        }
      } else if (companyName || companyLocation) {
        const companyRes = await client.query(`
          INSERT INTO companies (
            name, business_name_short, business_name_short_confidence, business_name_short_flags,
            industry, location, client_id
          )
          VALUES ($1, $2, $3, $4::text[], $5, $6, $7)
          RETURNING id
        `, [
          companyNameForWrite,
          shortName.business_name_short,
          shortName.confidence,
          shortName.flags,
          req.body.vertical || current.vertical || null,
          companyLocation || null,
          clientId,
        ]);
        addProspectField('company_id', companyRes.rows[0].id);
      }
    }

    if (prospectFields.length) {
      values.push(req.params.id, clientId);
      await client.query(`
        UPDATE prospects
        SET ${prospectFields.join(', ')}, updated_at = NOW()
        WHERE id = $${values.length - 1} AND client_id = $${values.length}
      `, values);
    }

    await client.query('COMMIT');
    const updated = await selectUpdatedProspect(req.params.id, clientId);
    res.json({ success: true, prospect: updated });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_rollbackErr) {}
    if (err.code === '23514' && /vertical_canonical_chk/.test(err.constraint || '')) {
      return res.status(400).json({ error: 'Vertical must use lowercase snake_case.' });
    }
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Touchpoints for a single prospect
router.get('/api/prospects/:id/touchpoints', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT channel, action_type, content_summary, outcome, created_at
      FROM touchpoints
      WHERE prospect_id = $1 AND client_id = $2
      ORDER BY created_at ASC
    `, [req.params.id, clientId]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/prospects/:id/preview', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT
        p.id, p.first_name, p.last_name, p.email, p.phone, p.vertical,
        p.status, p.icp_score, p.last_contacted_at, p.notes,
        c.name AS company_name, c.location AS city,
        COALESCE(eng.open_count, 0)::int AS open_count,
        eng.last_open_at,
        COALESCE(seq.send_count, 0)::int AS send_count
      FROM prospects p
      LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
      LEFT JOIN (
        SELECT prospect_id,
          COUNT(*) FILTER (WHERE action_type IN ('open', 'email_opened'))::int AS open_count,
          MAX(created_at) FILTER (WHERE action_type IN ('open', 'email_opened')) AS last_open_at
        FROM touchpoints
        WHERE client_id = $2 AND channel = 'email'
        GROUP BY prospect_id
      ) eng ON eng.prospect_id = p.id
      LEFT JOIN (
        SELECT prospect_id,
          COUNT(*) FILTER (WHERE action_type IN ('send', 'outbound', 'email_warm'))::int AS send_count
        FROM touchpoints
        WHERE client_id = $2 AND channel = 'email'
        GROUP BY prospect_id
      ) seq ON seq.prospect_id = p.id
      WHERE p.id = $1 AND p.client_id = $2
      LIMIT 1
    `, [req.params.id, clientId]);
    if (!result.rows.length) return res.status(404).json({ error: 'Prospect not found' });
    const r = result.rows[0];
    const sequenceStep = r.send_count <= 0 ? 'Not started'
      : r.send_count === 1 ? 'Day 0 email'
      : r.send_count === 2 ? 'Day 4 email'
      : r.send_count === 3 ? 'Day 8 email'
      : 'Day 13 email';
    res.set('Cache-Control', 'private, max-age=30');
    res.json({
      id: r.id,
      name: r.company_name || String(r.notes || '').split('—')[0].trim() || `${r.first_name || ''} ${r.last_name || ''}`.trim(),
      contact_name: `${r.first_name || ''} ${r.last_name || ''}`.trim(),
      email: r.email,
      phone: r.phone,
      vertical: r.vertical,
      city: r.city,
      icp_score: r.icp_score,
      status: r.status,
      last_contacted_at: r.last_contacted_at,
      open_count: r.open_count,
      sequence_step: sequenceStep,
      last_open_at: r.last_open_at,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/prospects/:id/detail', requireDashboardRead, async (req, res) => {
  try {
    await ensureProspectSetterAssignmentSchema();
    const clientId = getRequestClientId(req);
    const prospectRes = await pool.query(`
      SELECT
        p.*, c.name AS company_name, c.location AS city, c.website,
        u.name AS closer_name,
        su.name AS assigned_setter_name,
        su.email AS assigned_setter_email,
        COALESCE(eng.open_count, 0)::int AS open_count,
        COALESCE(eng.click_count, 0)::int AS click_count,
        COALESCE(seq.send_count, 0)::int AS send_count,
        eng.last_open_at
      FROM prospects p
      LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
      LEFT JOIN users u ON u.id = p.closer_id
      LEFT JOIN users su ON su.id = p.assigned_setter_id
      LEFT JOIN (
        SELECT prospect_id,
          COUNT(*) FILTER (WHERE action_type IN ('open', 'email_opened'))::int AS open_count,
          COUNT(*) FILTER (WHERE action_type IN ('click', 'email_clicked'))::int AS click_count,
          MAX(created_at) FILTER (WHERE action_type IN ('open', 'email_opened')) AS last_open_at
        FROM touchpoints
        WHERE client_id = $2 AND channel = 'email'
        GROUP BY prospect_id
      ) eng ON eng.prospect_id = p.id
      LEFT JOIN (
        SELECT prospect_id,
          COUNT(*) FILTER (WHERE action_type IN ('send', 'outbound', 'email_warm'))::int AS send_count
        FROM touchpoints
        WHERE client_id = $2 AND channel = 'email'
        GROUP BY prospect_id
      ) seq ON seq.prospect_id = p.id
      WHERE p.id = $1 AND p.client_id = $2
      LIMIT 1
    `, [req.params.id, clientId]);
    if (!prospectRes.rows.length) return res.status(404).json({ error: 'Prospect not found' });

    const historyRes = await pool.query(`
      SELECT t.id, t.channel, t.action_type, t.content_summary, t.outcome,
        t.sentiment, t.agent_id, t.created_at, u.name AS logged_by
      FROM touchpoints t
      LEFT JOIN users u ON t.agent_id = u.id::text
      WHERE t.prospect_id = $1 AND t.client_id = $2
      ORDER BY t.created_at DESC
    `, [req.params.id, clientId]);

    const p = prospectRes.rows[0];
    const sends = historyRes.rows
      .filter(t => t.channel === 'email' && ['send', 'outbound', 'email_warm'].includes(t.action_type))
      .slice()
      .reverse();
    const sequenceDays = [0, 4, 8, 13];
    const sequence = sequenceDays.map((day, index) => {
      const sent = sends[index] || null;
      const sentAt = sent?.created_at ? new Date(sent.created_at) : null;
      const nextSentAt = sends[index + 1]?.created_at ? new Date(sends[index + 1].created_at) : null;
      const related = historyRes.rows.filter(t => {
        if (t.channel !== 'email' || !['open', 'email_opened', 'click', 'email_clicked'].includes(t.action_type)) return false;
        const created = new Date(t.created_at);
        return sentAt && created >= sentAt && (!nextSentAt || created < nextSentAt);
      });
      return {
        day,
        status: sent ? 'sent' : index === sends.length ? 'current' : 'pending',
        sent_at: sent?.created_at || null,
        opened: related.some(t => ['open', 'email_opened'].includes(t.action_type)),
        clicked: related.some(t => ['click', 'email_clicked'].includes(t.action_type)),
      };
    });
    const lastSend = sends[sends.length - 1]?.created_at ? new Date(sends[sends.length - 1].created_at) : null;
    const nextDays = p.send_count === 1 || p.send_count === 2 ? 4 : p.send_count === 3 ? 5 : null;
    const nextScheduled = nextDays && lastSend ? new Date(lastSend.getTime() + nextDays * 86400000).toISOString() : null;

    res.json({
      id: p.id,
      name: p.company_name || String(p.notes || '').split('—')[0].trim() || `${p.first_name || ''} ${p.last_name || ''}`.trim(),
      contact_name: `${p.first_name || ''} ${p.last_name || ''}`.trim(),
      email: p.email,
      phone: p.phone,
      vertical: p.vertical,
      city: p.city,
      website: p.website,
      icp_score: p.icp_score,
      status: p.status,
      setter_status: p.setter_status,
      assigned_setter_id: p.assigned_setter_id,
      assigned_setter_name: p.assigned_setter_name,
      assigned_setter_email: p.assigned_setter_email,
      closer_id: p.closer_id,
      closer_name: p.closer_name,
      notes: p.notes,
      created_at: p.created_at,
      last_contacted_at: p.last_contacted_at,
      open_count: p.open_count,
      click_count: p.click_count,
      send_count: p.send_count,
      last_open_at: p.last_open_at,
      sequence_step: p.send_count <= 0 ? 'Not started' : `Day ${sequenceDays[Math.min(p.send_count - 1, 3)]} email`,
      next_scheduled_send_at: nextScheduled,
      history: historyRes.rows,
      email_opens: historyRes.rows.filter(t => ['open', 'email_opened'].includes(t.action_type)),
      sequence,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/setters/assignable', requireOperator, async (req, res) => {
  try {
    const clientId = normalizeClientId(req.session?.active_client_id || req.user?.client_id);
    const result = await pool.query(`
      SELECT id, name, email, role
      FROM users
      WHERE role IN ('setter', 'sales')
        AND active = true
        AND client_id = $1
      ORDER BY name ASC, email ASC
    `, [clientId]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/api/prospects/:id/assign-setter', requireOperator, async (req, res) => {
  const client = await pool.connect();
  try {
    await ensureProspectSetterAssignmentSchema();
    const clientId = normalizeClientId(req.session?.active_client_id || req.user?.client_id);
    const setterId = Number.parseInt(req.body.setter_id, 10);
    const note = String(req.body.note || '').slice(0, 2000).trim();
    if (!Number.isInteger(setterId)) return res.status(400).json({ error: 'Setter is required' });

    await client.query('BEGIN');
    const setterRes = await client.query(`
      SELECT id, name, email
      FROM users
      WHERE id = $1
        AND role IN ('setter', 'sales')
        AND active = true
        AND client_id = $2
      LIMIT 1
    `, [setterId, clientId]);
    if (!setterRes.rows.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Setter not found' });
    }

    const prospectRes = await client.query(`
      UPDATE prospects
      SET assigned_setter_id = $1,
          setter_status = COALESCE(setter_status, 'new'),
          setter_updated_at = NOW(),
          updated_at = NOW()
      WHERE id = $2
        AND client_id = $3
      RETURNING id, client_id
    `, [setterId, req.params.id, clientId]);
    if (!prospectRes.rows.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Prospect not found' });
    }
    const assignedProspect = await setSetterVisibility(client, prospectRes.rows[0].id, {
      reason: 'manual',
      clientId,
    });

    const setter = setterRes.rows[0];
    const subject = `Assigned to setter ${setter.name}`;
    await client.query(`
      INSERT INTO touchpoints
        (prospect_id, channel, action_type, content_summary, outcome, sentiment, agent_id, client_id)
      VALUES ($1, 'manual', 'setter_assigned', $2, $3, 'neutral', $4, $5)
    `, [
      prospectRes.rows[0].id,
      subject,
      note || null,
      String(req.user?.id || ''),
      prospectRes.rows[0].client_id,
    ]);

    await client.query('COMMIT');
    res.json({ success: true, setter, setter_visible: Boolean(assignedProspect?.setter_visible) });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_rollbackErr) {}
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

router.post('/api/prospects/:id/touch', requireOperator, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const actionType = String(req.body.action_type || 'manual_touch').slice(0, 80);
    const notes = String(req.body.notes || '').slice(0, 2000);
    const outcome = String(req.body.outcome || 'neutral').slice(0, 120);
    const result = await pool.query(`
      INSERT INTO touchpoints
        (prospect_id, channel, action_type, content_summary, outcome, sentiment, agent_id, client_id)
      SELECT p.id, $3, $4, $5, $6, 'neutral', $7, p.client_id
      FROM prospects p
      WHERE p.id = $1 AND p.client_id = $2
      RETURNING id, created_at
    `, [req.params.id, clientId, req.body.channel || 'manual', actionType, notes, outcome, String(req.user?.id || '')]);
    if (!result.rows.length) return res.status(404).json({ error: 'Prospect not found' });
    await pool.query('UPDATE prospects SET last_contacted_at = NOW(), updated_at = NOW() WHERE id = $1 AND client_id = $2', [req.params.id, clientId]);
    res.json({ success: true, touchpoint: result.rows[0] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.patch('/api/prospects/:id/status', requireOperator, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const status = String(req.body.status || '').toLowerCase();
    if (!['cold', 'contacted', 'warm', 'dead', 'disqualified', 'closed'].includes(status)) return res.status(400).json({ error: 'Invalid status' });
    const result = await pool.query(
      'UPDATE prospects SET status = $1, updated_at = NOW() WHERE id = $2 AND client_id = $3 RETURNING id',
      [status, req.params.id, clientId]
    );
    if (!result.rows.length) return res.status(404).json({ error: 'Prospect not found' });
    res.json({ success: true, prospect: await selectUpdatedProspect(req.params.id, clientId) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.patch('/api/prospects/:id/do-not-contact', requireOperator, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(
      'UPDATE prospects SET do_not_contact = true, updated_at = NOW() WHERE id = $1 AND client_id = $2 RETURNING id, do_not_contact',
      [req.params.id, clientId]
    );
    if (!result.rows.length) return res.status(404).json({ error: 'Prospect not found' });
    res.json({ success: true, prospect: result.rows[0] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Agent stats for sparklines
router.get('/api/agent-stats', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT
        CASE WHEN agent_name = 'email_agent' THEN 'emmett_agent' ELSE agent_name END as agent_name,
        COUNT(*) as total_runs,
        MAX(ran_at) as last_run,
        COUNT(CASE WHEN ran_at > NOW() - INTERVAL '7 days' THEN 1 END) as week_runs,
        COUNT(CASE WHEN status = 'success' THEN 1 END) as success_count
      FROM agent_log
      WHERE client_id = $1
      GROUP BY CASE WHEN agent_name = 'email_agent' THEN 'emmett_agent' ELSE agent_name END
    `, [clientId]);

    const daily = await pool.query(`
      SELECT
        CASE WHEN agent_name = 'email_agent' THEN 'emmett_agent' ELSE agent_name END as agent_name,
        DATE(ran_at) as date, COUNT(*) as count
      FROM agent_log
      WHERE client_id = $1 AND ran_at > NOW() - INTERVAL '7 days'
      GROUP BY CASE WHEN agent_name = 'email_agent' THEN 'emmett_agent' ELSE agent_name END, DATE(ran_at)
      ORDER BY date ASC
    `, [clientId]);

    const stats = {};
    result.rows.forEach(r => {
      stats[r.agent_name] = {
        total: parseInt(r.total_runs),
        weekRuns: parseInt(r.week_runs),
        successCount: parseInt(r.success_count),
        lastRun: r.last_run,
        daily: []
      };
    });

    daily.rows.forEach(r => {
      if (stats[r.agent_name]) {
        stats[r.agent_name].daily.push({ date: r.date, count: parseInt(r.count) });
      }
    });

    res.json(stats);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Agent health strip for dashboard cards
router.get('/api/agent-health', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      WITH roster(agent_key) AS (
        VALUES
          ('scout'), ('emmett'), ('riley'), ('paige'), ('max'), ('rex'), ('vera'), ('faye'),
          ('link'), ('sam'), ('cal'), ('cal_batch'), ('warm_signal'), ('analytics'),
          ('sketch'), ('setter_handoff'), ('ivy'), ('penny')
      ),
      scoped AS (
        SELECT
          CASE
            WHEN LOWER(agent_name) IN ('email_agent', 'emmett_agent', 'emmett') THEN 'emmett'
            WHEN LOWER(agent_name) IN ('facebook_agent', 'faye_agent', 'faye', 'facebook') THEN 'faye'
            WHEN LOWER(agent_name) IN ('linkedin_agent', 'link_agent', 'link', 'linkedin') THEN 'link'
            WHEN LOWER(agent_name) IN ('warm_signal_agent', 'warm_signal', 'warmsignal') THEN 'warm_signal'
            WHEN LOWER(agent_name) IN ('setter_handoff_agent', 'handoff_utility', 'setterhandoff') THEN 'setter_handoff'
            WHEN LOWER(agent_name) IN ('cal_batch_agent', 'cal_batch', 'calbatch') THEN 'cal_batch'
            ELSE REGEXP_REPLACE(LOWER(agent_name), '_agent$', '')
          END AS agent_key,
          agent_name,
          status,
          error_msg,
          duration_ms,
          ran_at
        FROM agent_log
        WHERE client_id = $1
      ),
      latest AS (
        SELECT DISTINCT ON (agent_key)
          agent_key, agent_name, status AS last_status, error_msg, duration_ms, ran_at AS last_run
        FROM scoped
        ORDER BY agent_key, ran_at DESC
      ),
      rollup_24h AS (
        SELECT
          agent_key,
          COUNT(*)::int AS runs_24h,
          COUNT(*) FILTER (WHERE status IN ('error', 'failed'))::int AS errors_24h
        FROM scoped
        WHERE ran_at >= NOW() - INTERVAL '24 hours'
        GROUP BY agent_key
      )
      SELECT
        r.agent_key,
        l.agent_name,
        l.last_run,
        COALESCE(l.last_status, 'never_run') AS last_status,
        l.error_msg,
        l.duration_ms,
        COALESCE(roll.errors_24h, 0)::int AS errors_24h,
        COALESCE(roll.runs_24h, 0)::int AS runs_24h
      FROM roster r
      LEFT JOIN latest l ON l.agent_key = r.agent_key
      LEFT JOIN rollup_24h roll ON roll.agent_key = r.agent_key
      ORDER BY r.agent_key
    `, [clientId]);

    const agents = {};
    result.rows.forEach(row => {
      agents[row.agent_key] = {
        agent_name: row.agent_name || row.agent_key,
        last_run: row.last_run,
        last_status: row.last_status,
        error_msg: row.error_msg,
        duration_ms: row.duration_ms,
        errors_24h: Number(row.errors_24h || 0),
        runs_24h: Number(row.runs_24h || 0),
      };
    });

    res.json({
      refreshed_at: new Date().toISOString(),
      client_id: clientId,
      agents,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Agent weekly stats for hover tooltips
router.get('/api/agent-weekly-stats', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const WEEK = `created_at > NOW() - INTERVAL '7 days'`;
    const WEEK_AL = `ran_at > NOW() - INTERVAL '7 days'`;

    const [logRows, emmettRow, scoutRow, linkRow, fayeRow, ivyRow] = await Promise.all([
      pool.query(`
        SELECT LOWER(REPLACE(agent_name, '_agent', '')) AS agent, action, COUNT(*) AS count
        FROM agent_log
        WHERE ${WEEK_AL} AND status = 'success' AND client_id = $1
        GROUP BY agent, action
      `, [clientId]),
      pool.query(`SELECT COUNT(*) AS count FROM touchpoints WHERE client_id = $1 AND channel = 'email' AND action_type = 'outbound' AND ${WEEK}`, [clientId]),
      pool.query(`SELECT COUNT(*) AS count FROM prospects WHERE client_id = $1 AND source = 'scout' AND ${WEEK}`, [clientId]),
      pool.query(`SELECT COUNT(*) AS count FROM pending_comments WHERE client_id = $1 AND channel = 'linkedin' AND ${WEEK}`, [clientId]),
      pool.query(`SELECT COUNT(*) AS count FROM pending_comments WHERE client_id = $1 AND channel = 'facebook' AND ${WEEK}`, [clientId]),
      pool.query(`SELECT COUNT(*) AS count FROM pending_comments WHERE client_id = $1 AND channel = 'instagram' AND ${WEEK}`, [clientId]),
    ]);

    const raw = {};
    for (const r of logRows.rows) {
      if (!raw[r.agent]) raw[r.agent] = {};
      raw[r.agent][r.action] = parseInt(r.count);
    }
    const pick = (r, ...actions) => actions.reduce((s, a) => s + (r[a] || 0), 0);

    const stats = {
      scout:     { count: parseInt(scoutRow.rows[0].count),                                    label: 'prospects found'   },
      emmett:    { count: parseInt(emmettRow.rows[0].count),                                   label: 'emails sent'       },
      link:      { count: parseInt(linkRow.rows[0].count),                                     label: 'drafts generated'  },
      faye:      { count: parseInt(fayeRow.rows[0].count),                                     label: 'drafts generated'  },
      ivy:       { count: parseInt(ivyRow.rows[0].count),                                      label: 'drafts generated'  },
      paige:     { count: pick(raw.paige  || {}, 'generate_content'),                          label: 'posts generated'   },
      max:       { count: pick(raw.max    || {}, 'daily_digest', 'weekly_report'),             label: 'digests sent'      },
      sam:       { count: pick(raw.sam    || {}, 'send_sms', 'batch_sms'),                     label: 'SMS sent'          },
      rex:       { count: pick(raw.rex    || {}, 'weekly_report', 'run'),                      label: 'reports generated' },
      riley:     { count: pick(raw.riley  || {}, 'triage', 'classify_email'),                  label: 'emails triaged'    },
      vera:      { count: pick(raw.vera   || {}, 'analyze_reviews', 'run'),                    label: 'reviews monitored' },
      cal:       { count: pick(raw.cal    || {}, 'initiate_call', 'run'),                      label: 'calls initiated'   },
      penny:     { count: pick(raw.penny     || {}, 'analyze_account', 'run'),                 label: 'accounts analyzed' },
      sketch:    { count: pick(raw.sketch    || {}, 'generate_mockup', 'run'),                 label: 'mockups generated' },
      analytics: { count: pick(raw.analytics || {}, 'fetch_metrics', 'run'),                   label: 'posts analyzed'    },
    };

    res.json(stats);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Live activity feed
router.get('/api/activity', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 20, 1), 100);
    const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);
    const perSourceLimit = Math.max(20, Math.min(limit + offset, 100));
    const [agentResult, emailResult, excludedResult] = await Promise.all([
      pool.query(`
      SELECT al.id, al.agent_name, al.action, al.status, al.ran_at, al.payload,
        COALESCE(
          al.prospect_id,
          CASE
            WHEN (al.payload->>'prospect_id') ~* '^[0-9a-f-]{36}$' THEN (al.payload->>'prospect_id')::uuid
            ELSE NULL
          END
        ) AS prospect_id,
        p.first_name, p.last_name, p.notes as prospect_notes
      FROM agent_log al
      LEFT JOIN prospects p
        ON p.id = COALESCE(
          al.prospect_id,
          CASE
            WHEN (al.payload->>'prospect_id') ~* '^[0-9a-f-]{36}$' THEN (al.payload->>'prospect_id')::uuid
            ELSE NULL
          END
        )
        AND p.client_id = al.client_id
      WHERE al.client_id = $1
        AND ${EXCLUDE_COMMAND_FEED_ACTIONS_SQL}
      ORDER BY al.ran_at DESC
      LIMIT $2
    `, [clientId, perSourceLimit]),
      pool.query(`
        SELECT
          ee.id,
          ee.event_type,
          ee.event_at,
          ee.recipient_email,
          ee.sending_domain,
          ee.subject_line,
          ee.prospect_id AS event_prospect_id,
          p.id AS prospect_id,
          p.first_name,
          p.last_name,
          p.notes AS prospect_notes,
          c.name AS company_name
        FROM email_events ee
        LEFT JOIN LATERAL (
          SELECT p.*
          FROM prospects p
          WHERE p.client_id = ee.client_id
            AND (
              p.id = ee.prospect_id
              OR LOWER(p.email) = LOWER(ee.recipient_email)
            )
          ORDER BY (p.id = ee.prospect_id) DESC, p.created_at DESC
          LIMIT 1
        ) p ON true
        LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
        WHERE ee.client_id = $1
        ORDER BY ee.event_at DESC
        LIMIT $2
      `, [clientId, perSourceLimit]),
      pool.query(`
        SELECT
          epl.id,
          epl.email,
          epl.domain,
          epl.source,
          epl.exclusion_reason,
          epl.exclusion_detail,
          epl.excluded_at AS created_at,
          p.id AS prospect_id,
          p.first_name,
          p.last_name,
          p.notes AS prospect_notes,
          c.name AS company_name
        FROM excluded_prospect_log epl
        LEFT JOIN LATERAL (
          SELECT p.*
          FROM prospects p
          WHERE p.client_id = $1
            AND LOWER(p.email) = LOWER(epl.email)
          ORDER BY p.created_at DESC
          LIMIT 1
        ) p ON true
        LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
        ORDER BY epl.excluded_at DESC
        LIMIT $2
      `, [clientId, perSourceLimit]),
    ]);

    const agentNameMap = {
      facebook: 'Faye', linkedin: 'Link', emmett: 'Emmett',
      max: 'Max', rex: 'Rex', scout: 'Scout', sketch: 'Sketch', email: 'Emmett'
    };
    const timeLabelFor = (value) => {
      const minutesAgo = Math.max(0, Math.floor((Date.now() - new Date(value)) / 60000));
      return minutesAgo < 60 ? `${minutesAgo}m` : minutesAgo < 1440 ? `${Math.floor(minutesAgo/60)}h` : `${Math.floor(minutesAgo/1440)}d`;
    };
    const companyFromNotes = (notes) => String(notes || '').split(/\s(?:\u2013|\u2014|-)\s/)[0].trim() || null;

    const agentFeed = agentResult.rows.map(row => {
      const rawAgent = row.agent_name?.replace('_agent', '') || 'system';
      const agent = agentNameMap[rawAgent] || rawAgent.charAt(0).toUpperCase() + rawAgent.slice(1);
      const payload = row.payload || {};
      const companyName = companyFromNotes(row.prospect_notes) || payload.company || null;
      const prospectName = cleanProspectName(row.first_name, row.last_name, companyName);
      const displayProspectName = payload.prospect_name || prospectName;
      const prospect = displayProspectName && displayProspectName !== 'Unknown contact' ? `· ${displayProspectName}` : '';
      const actionLabels = {
        generate_comment: `generated a comment draft ${prospect}`,
        daily_digest: 'daily digest sent · jacob@gopulseforge.com',
        weekly_report: 'weekly report dispatched',
        generate_mockup: `generated a mockup ${prospect}`,
        outbound: `sent email sequence ${prospect}`,
        email_sent: `Email sent ${prospect}`,
        email_skipped: `Email skipped ${prospect}`,
        dashboard_trigger: 'triggered from dashboard',
        email_opened: `email opened ${prospect}`,
        open: `email opened ${prospect}`,
        email_clicked: `link clicked ${prospect}`,
        click: `link clicked ${prospect}`,
        reply: `reply received ${prospect}`,
        inbound: `reply received ${prospect}`,
        call_answered: `phone call answered ${prospect}`,
        triage: `triaged reply ${prospect}`,
        triage_summary: 'triaged inbox'
      };
      const label = actionLabels[row.action] || row.action;
      const icons = {
        Faye: { icon: '📣', color: 'fi-t' }, Link: { icon: '💬', color: 'fi-p' },
        Emmett: { icon: '✉️', color: 'fi-o' }, Max: { icon: '🧠', color: 'fi-p' },
        Rex: { icon: '📊', color: 'fi-p' }, Scout: { icon: '🔍', color: 'fi-t' },
        Sketch: { icon: '🎨', color: 'fi-t' }, Riley: { icon: '🤝', color: 'fi-t' }
      };
      const { icon, color } = icons[agent] || { icon: '⚡', color: 'fi-g' };
      const rawAction = String(row.action || '').toLowerCase();
      const isWarmSignal = ['open', 'email_opened', 'click', 'email_clicked', 'reply', 'inbound', 'call_answered', 'triage'].includes(rawAction);
      return {
        id: row.id,
        source: 'agent_log',
        detail_id: row.id,
        detailable: true,
        agent, action: label, raw_action: rawAction, icon, color,
        time: timeLabelFor(row.ran_at), ran_at: row.ran_at, status: row.status,
        prospect_id: row.prospect_id,
        prospect: displayProspectName || null,
        is_warm_signal: isWarmSignal
      };
    });

    const eventLabels = {
      sent: 'email sent',
      delivered: 'email delivered',
      opened: 'email opened',
      clicked: 'link clicked',
      replied: 'reply received',
      hard_bounce: 'hard bounce recorded',
      soft_bounce: 'soft bounce recorded',
      blocked: 'email blocked',
      unsubscribed: 'unsubscribe recorded',
      spam: 'spam complaint recorded',
    };
    const emailFeed = emailResult.rows.map(row => {
      const rawAction = String(row.event_type || 'email_event').toLowerCase();
      const companyName = row.company_name || companyFromNotes(row.prospect_notes);
      const prospectName = cleanProspectName(row.first_name, row.last_name, companyName) || row.recipient_email;
      const warm = ['opened', 'clicked', 'replied'].includes(rawAction);
      const risk = ['hard_bounce', 'soft_bounce', 'blocked', 'unsubscribed', 'spam'].includes(rawAction);
      return {
        id: `email_event:${row.id}`,
        source: 'email_events',
        detailable: false,
        agent: rawAction === 'replied' ? 'Riley' : 'Emmett',
        action: `${eventLabels[rawAction] || rawAction.replace(/_/g, ' ')} · ${prospectName || row.recipient_email}`,
        raw_action: rawAction,
        icon: risk ? '!' : warm ? '↗' : '@',
        color: risk ? 'fi-o' : warm ? 'fi-t' : 'fi-p',
        time: timeLabelFor(row.event_at),
        ran_at: row.event_at,
        status: rawAction,
        prospect_id: row.prospect_id || row.event_prospect_id || null,
        prospect: prospectName || row.recipient_email,
        is_warm_signal: warm,
      };
    });

    const excludedFeed = excludedResult.rows.map(row => {
      const detail = row.exclusion_detail || {};
      const companyName = row.company_name || companyFromNotes(row.prospect_notes) || detail.company || row.domain || row.email;
      const reason = row.exclusion_reason || 'excluded';
      return {
        id: `track1:${row.id}`,
        source: 'excluded_prospect_log',
        detailable: false,
        agent: 'Scout',
        action: `Track 1 excluded ${companyName || row.email || 'prospect'} · ${reason}`,
        raw_action: 'track1_excluded',
        icon: '×',
        color: 'fi-o',
        time: timeLabelFor(row.created_at),
        ran_at: row.created_at,
        status: reason,
        prospect_id: row.prospect_id || detail.prospect_id || null,
        prospect: companyName || row.email,
        is_warm_signal: false,
      };
    });

    const feed = agentFeed
      .concat(emailFeed, excludedFeed)
      .sort((a, b) => new Date(b.ran_at || 0) - new Date(a.ran_at || 0))
      .slice(offset, offset + limit);

    res.json(feed);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Expandable detail panel for a single live-feed (agent_log) item
const ACTIVITY_DETAIL_AGENTS = {
  scout: 'Scout', linkedin: 'Link', facebook: 'Faye', emmett: 'Emmett',
  email: 'Emmett', max: 'Max', rex: 'Rex', riley: 'Riley', sketch: 'Sketch',
  paige: 'Paige', sam: 'Sam', vera: 'Vera', cal: 'Cal', ivy: 'Ivy', penny: 'Penny',
};
const SEQUENCE_DAYS = [0, 4, 8, 13];

function cleanProspectName(firstName, lastName, companyName) {
  const first = String(firstName || '').trim();
  const last = String(lastName || '').trim();
  const generic = !first || first.toLowerCase() === 'there';
  if (!generic) return `${first} ${last}`.trim();
  return companyName || 'Unknown contact';
}

function activityDetailTitle(action) {
  const titles = {
    email_opened: 'Email Opened',
    email_bounced: 'Email Bounced',
    email_soft_bounce: 'Email Soft Bounce',
    hot_prospect_alert: 'Hot Prospect Alert',
    triage: 'Triage',
    triage_summary: 'Triage Summary',
    reengagement_trigger: 'Re-engagement Trigger',
    auto_marked_dead: 'Auto-marked Dead',
    content_scored: 'Content Scored',
    cron_run: 'Cron Run',
    send_failure: 'Send Failure',
  };
  return titles[action] || action;
}

function sequenceStepLabel(sendCount) {
  if (!sendCount || sendCount < 1) return 'No email sent yet';
  const idx = Math.min(sendCount, SEQUENCE_DAYS.length);
  return `Step ${idx} — Day ${SEQUENCE_DAYS[idx - 1]}`;
}

router.get('/api/activity/:id/details', requireDashboardRead, async (req, res) => {
  try {
    const logId = String(req.params.id || '').trim();
    if (!/^[0-9a-f-]{36}$/i.test(logId) && !/^\d+$/.test(logId)) {
      return res.status(400).json({ error: 'Invalid id' });
    }
    const clientId = getRequestClientId(req);

    const prospectIdSql = `COALESCE(
      al.prospect_id,
      CASE WHEN (al.payload->>'prospect_id') ~* '^[0-9a-f-]{36}$' THEN (al.payload->>'prospect_id')::uuid ELSE NULL END
    )`;
    const logRes = await pool.query(`
      SELECT al.id, al.agent_name, al.action, al.status, al.ran_at, al.payload,
        al.error_msg, al.duration_ms, al.client_id,
        ${prospectIdSql} AS prospect_id,
        p.first_name, p.last_name, p.email AS prospect_email, p.phone, p.vertical,
        p.icp_score, p.status AS prospect_status, p.last_contacted_at, p.notes,
        p.assigned_setter_id,
        su.name AS assigned_setter_name,
        c.name AS company_name
      FROM agent_log al
      LEFT JOIN prospects p
        ON p.id = ${prospectIdSql}
        AND p.client_id = COALESCE(al.client_id, $2)
      LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
      LEFT JOIN users su ON su.id = p.assigned_setter_id
      WHERE al.id::text = $1 AND (al.client_id = $2 OR al.client_id IS NULL)
      LIMIT 1
    `, [logId, clientId]);

    if (!logRes.rows.length) {
      return res.status(404).json({ error: 'Activity not found' });
    }
    const row = logRes.rows[0];
    const action = String(row.action || '');
    const payload = row.payload || {};

    // Brevo/Riley webhook fallback: Riley logs Brevo email events (opens, clicks,
    // bounces) under one client_id while the prospect actually lives under a
    // different one (e.g. NH log row, Nashville prospect). The strict-client JOIN
    // above silently misses, so re-run an unscoped lookup so the detail panel can
    // still surface name/company/score. The session already has access to this log
    // row (which references prospect_id), so widening this single lookup does not
    // expose any new prospect.
    const isBrevoEvent = action === 'email_bounced' || action === 'email_soft_bounce'
      || action === 'email_opened' || action === 'email_clicked';
    let recoveredClientId = null;
    if (isBrevoEvent && !row.first_name && !row.last_name && !row.prospect_email) {
      let fbRes = null;
      if (row.prospect_id) {
        fbRes = await pool.query(`
          SELECT p.id, p.first_name, p.last_name, p.email AS prospect_email, p.phone,
                 p.vertical, p.icp_score, p.status AS prospect_status,
                 p.last_contacted_at, p.notes, p.client_id AS p_client_id,
                 p.assigned_setter_id,
                 su.name AS assigned_setter_name,
                 c.name AS company_name
          FROM prospects p
          LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
          LEFT JOIN users su ON su.id = p.assigned_setter_id
          WHERE p.id = $1
          LIMIT 1
        `, [row.prospect_id]);
      }
      if ((!fbRes || !fbRes.rows.length) && payload && payload.email) {
        fbRes = await pool.query(`
          SELECT p.id, p.first_name, p.last_name, p.email AS prospect_email, p.phone,
                 p.vertical, p.icp_score, p.status AS prospect_status,
                 p.last_contacted_at, p.notes, p.client_id AS p_client_id,
                 p.assigned_setter_id,
                 su.name AS assigned_setter_name,
                 c.name AS company_name
          FROM prospects p
          LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
          LEFT JOIN users su ON su.id = p.assigned_setter_id
          WHERE LOWER(p.email) = LOWER($1)
          ORDER BY p.last_contacted_at DESC NULLS LAST
          LIMIT 1
        `, [payload.email]);
      }
      if (fbRes && fbRes.rows.length) {
        const fb = fbRes.rows[0];
        row.prospect_id = row.prospect_id || fb.id;
        row.first_name = fb.first_name;
        row.last_name = fb.last_name;
        row.prospect_email = fb.prospect_email;
        row.phone = fb.phone;
        row.vertical = fb.vertical;
        row.icp_score = fb.icp_score;
        row.prospect_status = fb.prospect_status;
        row.last_contacted_at = fb.last_contacted_at;
        row.notes = fb.notes;
        row.company_name = fb.company_name;
        row.assigned_setter_id = fb.assigned_setter_id;
        row.assigned_setter_name = fb.assigned_setter_name;
        recoveredClientId = fb.p_client_id;
      }
    }

    const effectiveClientId = recoveredClientId || row.client_id || clientId;

    const companyName = row.company_name
      || String(row.notes || '').split('—')[0].trim()
      || payload.company
      || null;
    const prospectName = cleanProspectName(row.first_name, row.last_name, companyName);

    let eng = {};
    let lastSubject = null;
    if (row.prospect_id) {
      const engRes = await pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE action_type IN ('open','email_opened'))::int AS open_count,
          COUNT(*) FILTER (WHERE action_type IN ('open','email_opened')
            AND created_at >= NOW() - INTERVAL '24 hours')::int AS open_count_24h,
          COUNT(*)::int AS touch_count,
          COUNT(*) FILTER (WHERE channel = 'email'
            AND action_type IN ('outbound','email_warm','send'))::int AS send_count,
          MAX(created_at) AS last_touch_at
        FROM touchpoints
        WHERE prospect_id = $1 AND client_id = $2
      `, [row.prospect_id, effectiveClientId]);
      eng = engRes.rows[0] || {};
      const subjRes = await pool.query(`
        SELECT content_summary AS subject
        FROM touchpoints
        WHERE prospect_id = $1 AND client_id = $2
          AND channel = 'email' AND action_type IN ('outbound','email_warm','send')
        ORDER BY created_at DESC
        LIMIT 1
      `, [row.prospect_id, effectiveClientId]);
      lastSubject = subjRes.rows[0]?.subject || null;
    }

    const daysSince = (d) => d ? Math.floor((Date.now() - new Date(d).getTime()) / 86400000) : null;
    const fmtDate = (d) => d ? new Date(d).toISOString().slice(0, 10) : '—';

    const fields = [];
    const f = (label, value) => fields.push({ label, value: (value === null || value === undefined || value === '') ? '—' : String(value) });
    let actions = [];

    // Surface the assigned setter wherever the panel mentions the prospect.
    // Fallback to "Unassigned" so the panel never silently omits this column.
    const setterLabel = row.assigned_setter_name || (row.prospect_id ? 'Unassigned' : null);

    switch (action) {
      case 'email_opened':
        f('Prospect', prospectName);
        f('Company', companyName);
        f('Email', row.prospect_email || payload.email);
        f('Setter', setterLabel);
        f('Vertical', row.vertical);
        f('ICP score', row.icp_score);
        f('Total opens', eng.open_count ?? 0);
        f('Last email subject', lastSubject || payload.subject);
        break;
      case 'email_bounced':
      case 'email_soft_bounce':
        f('Prospect', prospectName);
        f('Company', companyName);
        f('Email', row.prospect_email || payload.email);
        f('Setter', setterLabel);
        f('Vertical', row.vertical);
        f('ICP score', row.icp_score);
        f('Total touchpoints', eng.touch_count ?? 0);
        f('Bounce type', action === 'email_bounced' ? 'Hard' : 'Soft');
        f('Sequence step bounced', sequenceStepLabel(eng.send_count));
        break;
      case 'hot_prospect_alert':
        f('Prospect', prospectName);
        f('Company', companyName);
        f('Setter', setterLabel);
        f('ICP score', row.icp_score);
        f('Opens in 24h', payload.open_count ?? eng.open_count_24h ?? 0);
        f('Phone', row.phone);
        actions = [
          { key: 'assign_setter', label: 'Assign to Setter' },
          { key: 'log_touch', label: 'Log Touch' },
        ];
        break;
      case 'triage':
        f('Prospect', payload.prospect_name || prospectName);
        f('Company', payload.company || companyName);
        f('Email', payload.email || row.prospect_email);
        f('Setter', setterLabel);
        f('Vertical', payload.vertical || row.vertical);
        f('ICP Score', payload.icp_score ?? row.icp_score);
        f('Total Opens', payload.total_opens ?? eng.open_count ?? 0);
        f('Triage Bucket', payload.triage_bucket);
        f('Trigger', payload.trigger);
        f('Recommended Next Action', payload.recommended_action);
        f('Signal Received', payload.signal_timestamp);
        break;
      case 'reengagement_trigger':
        f('Prospect', prospectName);
        f('Company', companyName);
        f('Setter', setterLabel);
        f('Days since last touch', daysSince(eng.last_touch_at ?? row.last_contacted_at));
        f('Current status', row.prospect_status);
        break;
      case 'auto_marked_dead':
        f('Prospect', prospectName);
        f('Company', companyName);
        f('Setter', setterLabel);
        f('Touch count', eng.touch_count ?? 0);
        f('Last contact date', fmtDate(eng.last_touch_at ?? row.last_contacted_at));
        break;
      case 'content_scored':
        f('Channel', payload.channel);
        f('Score', `${payload.total ?? payload.scores?.total ?? '—'}/30`);
        f('Weak dimension', payload.weak_dimension || 'none');
        f('Regenerated', payload.regenerated ? 'Yes' : 'No');
        break;
      case 'cron_run': {
        const rawAgent = String(row.agent_name || '').replace('_agent', '').toLowerCase();
        f('Agent', ACTIVITY_DETAIL_AGENTS[rawAgent] || row.agent_name || 'System');
        f('Sent count', payload.sent ?? 0);
        f('Prospects evaluated', payload.prospects_evaluated ?? '—');
        f('Duration', row.duration_ms != null ? `${row.duration_ms} ms` : '—');
        break;
      }
      case 'send_failure':
        f('Prospect email', row.prospect_email || payload.email);
        f('Error message', row.error_msg || 'Unknown error');
        break;
      default:
        if (prospectName) {
          f('Prospect', prospectName);
          if (setterLabel) f('Setter', setterLabel);
        }
        break;
    }

    const responseBody = {
      id: row.id,
      action,
      agent_name: row.agent_name || null,
      status: row.status || null,
      ran_at: row.ran_at || null,
      title: activityDetailTitle(action),
      prospect_id: row.prospect_id || null,
      fields,
      actions: ['viewer', 'client'].includes(req.user?.role) ? [] : actions,
      payload,
      error_msg: row.error_msg || null,
      duration_ms: row.duration_ms ?? null,
      prospect_found: Boolean(row.prospect_id && (row.first_name || row.last_name || row.prospect_email || row.phone || row.notes)),
    };
    res.json(responseBody);
  } catch (err) {
    console.error('[activity-details] error', { id: req.params.id, error: err.message });
    res.status(500).json({ error: err.message });
  }
});

// Activity panel (sequences + timeline)
router.get('/api/activity-panel', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const [seqResult, timelineResult] = await Promise.all([
      pool.query(`
        SELECT
          p.id, p.first_name, p.last_name, p.notes, p.status,
          c.name as company_name,
          COUNT(t.id)::int as emails_sent,
          MAX(t.created_at) as last_touch,
          COALESCE(eng.open_count,  0)::int as open_count,
          COALESCE(eng.click_count, 0)::int as click_count,
          CASE
            WHEN COUNT(t.id) = 1 THEN MAX(t.created_at) + INTERVAL '4 days'
            WHEN COUNT(t.id) = 2 THEN MAX(t.created_at) + INTERVAL '4 days'
            WHEN COUNT(t.id) = 3 THEN MAX(t.created_at) + INTERVAL '5 days'
            ELSE NULL
          END as next_due_at
        FROM prospects p
        LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
        INNER JOIN touchpoints t
          ON t.prospect_id = p.id
          AND t.client_id = p.client_id
          AND t.channel = 'email'
          AND t.action_type = 'outbound'
        LEFT JOIN (
          SELECT
            prospect_id,
            COUNT(CASE WHEN action_type = 'email_opened'  THEN 1 END)::int AS open_count,
            COUNT(CASE WHEN action_type = 'email_clicked' THEN 1 END)::int AS click_count
          FROM touchpoints
          WHERE channel = 'email' AND client_id = $1
          GROUP BY prospect_id
        ) eng ON eng.prospect_id = p.id
        WHERE p.do_not_contact = false
          AND p.client_id = $1
        GROUP BY p.id, c.name, eng.open_count, eng.click_count
        ORDER BY MAX(t.created_at) DESC
        LIMIT 100
      `, [clientId]),
      pool.query(`
        SELECT
          al.id, al.agent_name, al.action, al.status, al.ran_at,
          p.first_name, p.last_name, p.notes as prospect_notes
        FROM agent_log al
        LEFT JOIN prospects p ON al.prospect_id = p.id AND p.client_id = al.client_id
        WHERE al.client_id = $1
          AND ${EXCLUDE_COMMAND_FEED_ACTIONS_SQL}
        ORDER BY al.ran_at DESC
        LIMIT 50
      `, [clientId])
    ]);

    const STAGE_LABELS = ['', 'Day 0 sent · next Day 4', 'Day 4 sent · next Day 8', 'Day 8 sent · next Day 13', 'Complete'];
    const sequences = seqResult.rows.map(r => {
      const count = r.emails_sent;
      return {
        id:           r.id,
        business:     r.company_name || (r.notes || '').split('—')[0].trim() || `${r.first_name} ${r.last_name}`.trim(),
        status:       r.status,
        emails_sent:  count,
        stage_label:  STAGE_LABELS[Math.min(count, 4)] || 'Unknown',
        last_touch:   r.last_touch,
        next_due_at:  r.next_due_at,
        overdue:      r.next_due_at ? new Date(r.next_due_at) < new Date() : false,
        complete:     count >= 4,
        open_count:   r.open_count  || 0,
        click_count:  r.click_count || 0,
        has_opened:   (r.open_count  || 0) > 0,
        has_clicked:  (r.click_count || 0) > 0,
      };
    });

    const AGENT_LABELS = {
      scout: { name: 'Scout', icon: '🔍' }, linkedin: { name: 'Link', icon: '💬' },
      facebook: { name: 'Faye', icon: '📣' }, emmett: { name: 'Emmett', icon: '✉️' },
      email: { name: 'Emmett', icon: '✉️' }, max: { name: 'Max', icon: '🧠' },
      rex: { name: 'Rex', icon: '📊' }, riley: { name: 'Riley', icon: '🙋' },
      sketch: { name: 'Sketch', icon: '🎨' }, paige: { name: 'Paige', icon: '✍️' },
      sam: { name: 'Sam', icon: '📱' }, vera: { name: 'Vera', icon: '⭐' },
      cal: { name: 'Cal', icon: '📞' }, ivy: { name: 'Ivy', icon: '📸' },
      penny: { name: 'Penny', icon: '💰' }
    };
    const ACTION_LABELS = {
      generate_comment: 'drafted comment', daily_digest: 'sent daily digest',
      weekly_report: 'sent weekly report', generate_mockup: 'generated mockup',
      outbound: 'sent email', dashboard_trigger: 'triggered from dashboard',
      send_sms: 'sent SMS', generate_content: 'generated content',
      triage: 'triaged inbox', batch_sms: 'ran SMS batch',
      analyze_account: 'analyzed ad account', initiate_call: 'initiated call',
      analyze_reviews: 'analyzed reviews'
    };
    const timeline = timelineResult.rows.map(r => {
      const rawAgent = (r.agent_name || '').replace('_agent', '');
      const agentInfo = AGENT_LABELS[rawAgent] || { name: rawAgent, icon: '⚡' };
      const companyName = (r.prospect_notes || '').split('—')[0].trim() || null;
      const prospectBiz = cleanProspectName(r.first_name, r.last_name, companyName);
      return {
        id: r.id,
        agent: agentInfo.name,
        icon: agentInfo.icon,
        action: ACTION_LABELS[r.action] || r.action,
        prospect: prospectBiz,
        status: r.status,
        ran_at: r.ran_at
      };
    });

    res.json({ sequences, timeline });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Load more timeline items
router.get('/api/activity-timeline', requireDashboardRead, async (req, res) => {
  const offset = parseInt(req.query.offset) || 0;
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT al.id, al.agent_name, al.action, al.status, al.ran_at,
        p.first_name, p.last_name, p.notes as prospect_notes
      FROM agent_log al
      LEFT JOIN prospects p ON al.prospect_id = p.id AND p.client_id = al.client_id
      WHERE al.client_id = $2
        AND ${EXCLUDE_COMMAND_FEED_ACTIONS_SQL}
      ORDER BY al.ran_at DESC
      LIMIT 50 OFFSET $1
    `, [offset, clientId]);

    const AGENT_LABELS = {
      scout: { name: 'Scout', icon: '🔍' }, linkedin: { name: 'Link', icon: '💬' },
      facebook: { name: 'Faye', icon: '📣' }, emmett: { name: 'Emmett', icon: '✉️' },
      email: { name: 'Emmett', icon: '✉️' }, max: { name: 'Max', icon: '🧠' },
      rex: { name: 'Rex', icon: '📊' }, riley: { name: 'Riley', icon: '🙋' },
      sketch: { name: 'Sketch', icon: '🎨' }, paige: { name: 'Paige', icon: '✍️' },
      sam: { name: 'Sam', icon: '📱' }, vera: { name: 'Vera', icon: '⭐' },
      cal: { name: 'Cal', icon: '📞' }, ivy: { name: 'Ivy', icon: '📸' },
      penny: { name: 'Penny', icon: '💰' }
    };
    const ACTION_LABELS = {
      generate_comment: 'drafted comment', daily_digest: 'sent daily digest',
      weekly_report: 'sent weekly report', generate_mockup: 'generated mockup',
      outbound: 'sent email', dashboard_trigger: 'triggered from dashboard',
      send_sms: 'sent SMS', generate_content: 'generated content',
      triage: 'triaged inbox', batch_sms: 'ran SMS batch',
      analyze_account: 'analyzed ad account', initiate_call: 'initiated call',
      analyze_reviews: 'analyzed reviews'
    };
    const rows = result.rows.map(r => {
      const rawAgent = (r.agent_name || '').replace('_agent', '');
      const agentInfo = AGENT_LABELS[rawAgent] || { name: rawAgent, icon: '⚡' };
      const companyName = (r.prospect_notes || '').split('—')[0].trim() || null;
      const prospectBiz = cleanProspectName(r.first_name, r.last_name, companyName);
      return {
        id: r.id, agent: agentInfo.name, icon: agentInfo.icon,
        action: ACTION_LABELS[r.action] || r.action,
        prospect: prospectBiz, status: r.status, ran_at: r.ran_at
      };
    });
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Analytics
router.get('/api/analytics', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const [vol, reply, icp, agents, funnel, topProspects] = await Promise.all([
      pool.query(`
        SELECT
          DATE(created_at)::text AS date,
          channel,
          COUNT(*) AS count
        FROM touchpoints
        WHERE channel IN ('email','sms')
          AND action_type = 'outbound'
          AND client_id = $1
          AND created_at >= NOW() - INTERVAL '30 days'
        GROUP BY DATE(created_at), channel
        ORDER BY date ASC
      `, [clientId]),
      pool.query(`
        SELECT
          DATE_TRUNC('week', created_at)::text AS week,
          action_type,
          COUNT(*) AS count
        FROM touchpoints
        WHERE channel = 'email'
          AND client_id = $1
          AND created_at >= NOW() - INTERVAL '56 days'
        GROUP BY DATE_TRUNC('week', created_at), action_type
        ORDER BY week ASC
      `, [clientId]),
      pool.query(`
        SELECT
          CASE
            WHEN icp_score IS NULL          THEN 'Unknown'
            WHEN icp_score BETWEEN 0  AND 20 THEN '0–20'
            WHEN icp_score BETWEEN 21 AND 40 THEN '21–40'
            WHEN icp_score BETWEEN 41 AND 60 THEN '41–60'
            WHEN icp_score BETWEEN 61 AND 80 THEN '61–80'
            ELSE '81–100'
          END AS bucket,
          COUNT(*) AS count
        FROM prospects
        WHERE do_not_contact = false AND client_id = $1
        GROUP BY bucket
      `, [clientId]),
      pool.query(`
        SELECT agent_name, COUNT(*) AS count
        FROM agent_log
        WHERE client_id = $1
          AND ran_at >= NOW() - INTERVAL '30 days'
          AND agent_name IS NOT NULL
        GROUP BY agent_name
        ORDER BY count DESC
      `, [clientId]),
      pool.query(`
        SELECT
          COALESCE(status, 'cold') AS stage,
          COUNT(*) AS count
        FROM prospects
        WHERE do_not_contact = false AND client_id = $1
        GROUP BY stage
      `, [clientId]),
      pool.query(`
        SELECT
          p.id,
          p.first_name,
          p.last_name,
          p.notes,
          p.status,
          c.name AS company_name,
          COUNT(t.id)::int AS touchpoint_count,
          MAX(t.created_at) AS last_contacted_at
        FROM prospects p
        LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
        LEFT JOIN touchpoints t ON t.prospect_id = p.id AND t.client_id = p.client_id
        WHERE p.do_not_contact = false
          AND p.client_id = $1
        GROUP BY p.id, c.name
        ORDER BY touchpoint_count DESC
        LIMIT 10
      `, [clientId])
    ]);

    const days = [];
    for (let i = 29; i >= 0; i--) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      days.push(d.toISOString().slice(0, 10));
    }
    const volByDay = {};
    vol.rows.forEach(r => {
      if (!volByDay[r.date]) volByDay[r.date] = { email: 0, sms: 0 };
      volByDay[r.date][r.channel] = parseInt(r.count);
    });
    const outbound_volume = days.map(d => ({
      date: d,
      email: volByDay[d]?.email || 0,
      sms:   volByDay[d]?.sms   || 0
    }));

    const weekMap = {};
    reply.rows.forEach(r => {
      if (!weekMap[r.week]) weekMap[r.week] = { outbound: 0, inbound: 0 };
      weekMap[r.week][r.action_type] = parseInt(r.count);
    });
    const reply_rate = Object.entries(weekMap).map(([week, v]) => ({
      week: new Date(week).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      outbound: v.outbound,
      inbound:  v.inbound,
      rate: v.outbound > 0 ? Math.round((v.inbound / v.outbound) * 100) : 0
    }));

    const BUCKETS = ['0–20', '21–40', '41–60', '61–80', '81–100', 'Unknown'];
    const icpMap = {};
    icp.rows.forEach(r => { icpMap[r.bucket] = parseInt(r.count); });
    const icp_distribution = BUCKETS.map(b => ({ bucket: b, count: icpMap[b] || 0 }));

    const AGENT_NAME_MAP = {
      faye_agent: 'faye', faye_agent1: 'faye', facebook_agent: 'faye',
      link_agent: 'link', link_agent1: 'link', linkedin_agent: 'link',
      cal_agent: 'cal', analytics_agent: 'analytics',
      emmett_agent: 'emmett', emmett_agent1: 'emmett', email_agent: 'emmett',
      scout_agent: 'scout', sketch_agent: 'sketch', max_agent: 'max',
      rex_agent: 'rex', riley_agent: 'riley', sam_agent: 'sam',
      vera_agent: 'vera', paige_agent: 'paige', penny_agent: 'penny', ivy_agent: 'ivy',
      facebook_page_publisher: 'paige', linkedin_page_publisher: 'paige',
      google_business_publisher: 'paige', blog_publisher: 'paige',
    };
    function normalizeAgentName(raw) {
      if (!raw) return 'unknown';
      const lower = raw.toLowerCase();
      if (AGENT_NAME_MAP[lower]) return AGENT_NAME_MAP[lower];
      return lower.replace(/_(agent|publisher)\d*$/, '').replace(/\d+$/, '');
    }
    const agentTotals = {};
    agents.rows.forEach(r => {
      const name = normalizeAgentName(r.agent_name);
      agentTotals[name] = (agentTotals[name] || 0) + parseInt(r.count);
    });
    const agent_breakdown = Object.entries(agentTotals)
      .map(([agent, count]) => ({ agent, count }))
      .sort((a, b) => b.count - a.count);

    const STAGES = ['cold', 'warm', 'replied', 'converted'];
    const stageMap = {};
    funnel.rows.forEach(r => { stageMap[r.stage] = parseInt(r.count); });
    const total = Object.values(stageMap).reduce((s, v) => s + v, 0);
    const pipeline_funnel = STAGES
      .filter(s => stageMap[s] !== undefined)
      .map(s => ({ stage: s, count: stageMap[s], pct: total > 0 ? Math.round((stageMap[s] / total) * 100) : 0 }));
    if (!pipeline_funnel.find(f => f.stage === 'cold')) {
      pipeline_funnel.unshift({ stage: 'cold', count: 0, pct: 0 });
    }

    const top_prospects = topProspects.rows.map(r => ({
      id: r.id,
      name: `${r.first_name} ${r.last_name}`.trim(),
      business: r.company_name || (r.notes || '').split('—')[0].trim() || `${r.first_name} ${r.last_name}`.trim(),
      status: r.status || 'cold',
      touchpoint_count: r.touchpoint_count,
      last_contacted_at: r.last_contacted_at
    }));

    res.json({ outbound_volume, reply_rate, icp_distribution, agent_breakdown, pipeline_funnel, top_prospects });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Content analytics: recent posts with metrics
router.get('/api/analytics/posts', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT
        pa.id, pa.channel, pa.content_type, pa.post_text,
        pa.platform_post_id, pa.published_at,
        pa.post_day_of_week, pa.post_hour,
        pa.likes, pa.comments, pa.shares, pa.reach, pa.impressions, pa.clicks,
        pa.engagement_rate, pa.metrics_fetched_at,
        c.name AS company_name
      FROM post_analytics pa
      LEFT JOIN companies c ON pa.company_id = c.id AND c.client_id = pa.client_id
      WHERE pa.client_id = $1
      ORDER BY pa.published_at DESC
      LIMIT 100
    `, [clientId]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Content performance summary by channel/type
router.get('/api/analytics/summary', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT
        cps.channel, cps.content_type,
        cps.post_count, cps.avg_likes, cps.avg_comments,
        cps.avg_shares, cps.avg_reach, cps.avg_impressions, cps.avg_engagement_rate,
        cps.best_day_of_week, cps.best_hour,
        c.name AS company_name
      FROM content_performance_summary cps
      LEFT JOIN companies c ON cps.company_id = c.id
      WHERE COALESCE(c.client_id, $1) = $1
      ORDER BY cps.avg_engagement_rate DESC
    `, [clientId]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Top posts by engagement rate
router.get('/api/analytics/top-posts', requireDashboardRead, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT
        pa.id, pa.channel, pa.content_type,
        LEFT(pa.post_text, 120) AS post_preview,
        pa.published_at, pa.likes, pa.comments, pa.shares,
        pa.reach, pa.impressions, pa.engagement_rate,
        c.name AS company_name
      FROM post_analytics pa
      LEFT JOIN companies c ON pa.company_id = c.id AND c.client_id = pa.client_id
      WHERE pa.engagement_rate > 0
        AND pa.client_id = $2
      ORDER BY pa.engagement_rate DESC
      LIMIT $1
    `, [limit, clientId]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Email engagement stats
router.get('/api/analytics/email', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const days = analyticsWindowDays(req.query.days);
    const [prospectColumns, excludedColumns] = await Promise.all([
      tableColumns('prospects'),
      tableColumns('excluded_prospect_log'),
    ]);
    const hasEmailStatus = prospectColumns.has('email_status');
    const excludedHasCreatedAt = excludedColumns.has('created_at');

    const pipelineSql = hasEmailStatus ? `
      SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (
          WHERE COALESCE(email_status, 'unverified_legacy') = 'valid'
            AND COALESCE(do_not_contact, false) = false
        )::int AS sendable,
        COUNT(*) FILTER (WHERE COALESCE(do_not_contact, false) = true)::int AS excluded,
        COUNT(*) FILTER (
          WHERE COALESCE(do_not_contact, false) = false
            AND COALESCE(NULLIF(email_status, ''), 'unverified_legacy') IN ('unknown', 'unverified_legacy')
        )::int AS unknown
      FROM prospects
      WHERE client_id = $1
    ` : `
      SELECT
        COUNT(*)::int AS total,
        0::int AS sendable,
        COUNT(*) FILTER (WHERE COALESCE(do_not_contact, false) = true)::int AS excluded,
        COUNT(*) FILTER (WHERE COALESCE(do_not_contact, false) = false)::int AS unknown
      FROM prospects
      WHERE client_id = $1
    `;
    const verifierSql = hasEmailStatus ? `
      SELECT email_bucket AS status, COUNT(*)::int AS count
      FROM (
        SELECT CASE
          WHEN COALESCE(NULLIF(email_status, ''), 'unverified_legacy') IN ('valid', 'invalid', 'catchall', 'risky', 'unknown', 'unverified_legacy')
            THEN COALESCE(NULLIF(email_status, ''), 'unverified_legacy')
          ELSE 'unknown'
        END AS email_bucket
        FROM prospects
        WHERE client_id = $1
      ) scoped_email_statuses
      GROUP BY email_bucket
    ` : `
      SELECT 'unverified_legacy' AS status, COUNT(*)::int AS count
      FROM prospects
      WHERE client_id = $1
    `;
    const track1Where = excludedHasCreatedAt
      ? "WHERE epl.created_at >= NOW() - INTERVAL '7 days'"
      : '';

    const [totals, weekTotals, domainBreakdown, verticalBreakdown, pipelineHealth, track1Reasons, verifierStats, warmUpgraded] = await Promise.all([
      pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE event_type = 'sent')::int AS sent_total,
          COUNT(DISTINCT LOWER(TRIM(recipient_email))) FILTER (WHERE event_type = 'delivered')::int AS delivered_total,
          COUNT(DISTINCT LOWER(TRIM(recipient_email))) FILTER (WHERE event_type = 'opened')::int AS opened_total,
          COUNT(*) FILTER (WHERE event_type = 'opened_proxy')::int AS opened_proxy_total,
          COUNT(*) FILTER (WHERE event_type = 'clicked')::int AS clicked_total,
          COUNT(*) FILTER (WHERE event_type = 'replied')::int AS replied_total,
          COUNT(*) FILTER (WHERE event_type IN ('hard_bounce', 'blocked'))::int AS bounced_total,
          COUNT(*) FILTER (WHERE event_type = 'unsubscribed')::int AS unsub_total
        FROM email_events
        WHERE client_id = $1
          AND event_at >= NOW() - ($2 * INTERVAL '1 day')
      `, [clientId, days]),
      pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE event_type = 'sent')::int AS sent_week,
          COUNT(DISTINCT LOWER(TRIM(recipient_email))) FILTER (WHERE event_type = 'delivered')::int AS delivered_week,
          COUNT(DISTINCT LOWER(TRIM(recipient_email))) FILTER (WHERE event_type = 'opened')::int AS opened_week,
          COUNT(*) FILTER (WHERE event_type = 'opened_proxy')::int AS opened_proxy_week,
          COUNT(*) FILTER (WHERE event_type = 'clicked')::int AS clicked_week,
          COUNT(*) FILTER (WHERE event_type IN ('hard_bounce', 'blocked'))::int AS bounced_week
        FROM email_events
        WHERE client_id = $1
          AND event_at >= NOW() - INTERVAL '7 days'
      `, [clientId]),
      pool.query(`
        SELECT
          COALESCE(NULLIF(sending_domain, ''), 'unknown') AS sending_domain,
          COUNT(*) FILTER (WHERE event_type = 'sent')::int AS sent,
          COUNT(DISTINCT LOWER(TRIM(recipient_email))) FILTER (WHERE event_type = 'delivered')::int AS delivered,
          COUNT(DISTINCT LOWER(TRIM(recipient_email))) FILTER (WHERE event_type = 'opened')::int AS opened,
          COUNT(*) FILTER (WHERE event_type = 'opened_proxy')::int AS opened_proxy,
          COUNT(*) FILTER (WHERE event_type = 'clicked')::int AS clicked,
          COUNT(*) FILTER (WHERE event_type = 'replied')::int AS replied,
          COUNT(*) FILTER (WHERE event_type IN ('hard_bounce', 'blocked'))::int AS bounced
        FROM email_events
        WHERE client_id = $1
          AND event_at >= NOW() - ($2 * INTERVAL '1 day')
        GROUP BY sending_domain
        ORDER BY bounced DESC, sent DESC, delivered DESC
      `, [clientId, days]),
      pool.query(`
        SELECT
          COALESCE(NULLIF(p.vertical, ''), 'unknown') AS vertical,
          COUNT(*) FILTER (WHERE ee.event_type = 'sent')::int AS sent,
          COUNT(DISTINCT LOWER(TRIM(ee.recipient_email))) FILTER (WHERE ee.event_type = 'delivered')::int AS delivered,
          COUNT(DISTINCT LOWER(TRIM(ee.recipient_email))) FILTER (WHERE ee.event_type = 'opened')::int AS opened,
          COUNT(*) FILTER (WHERE ee.event_type = 'opened_proxy')::int AS opened_proxy,
          COUNT(*) FILTER (WHERE ee.event_type = 'clicked')::int AS clicked,
          COUNT(*) FILTER (WHERE ee.event_type = 'replied')::int AS replied,
          COUNT(*) FILTER (WHERE ee.event_type IN ('hard_bounce', 'blocked'))::int AS bounced
        FROM email_events ee
        LEFT JOIN LATERAL (
          SELECT vertical
          FROM prospects p
          WHERE p.client_id = ee.client_id
            AND LOWER(p.email) = LOWER(ee.recipient_email)
          ORDER BY p.created_at DESC
          LIMIT 1
        ) p ON true
        WHERE ee.client_id = $1
          AND ee.event_at >= NOW() - ($2 * INTERVAL '1 day')
        GROUP BY COALESCE(NULLIF(p.vertical, ''), 'unknown')
        ORDER BY delivered DESC, sent DESC
      `, [clientId, days]),
      pool.query(pipelineSql, [clientId]),
      pool.query(`
        SELECT
          COALESCE(NULLIF(epl.exclusion_reason, ''), 'unknown') AS reason,
          COUNT(*)::int AS count
        FROM excluded_prospect_log epl
        ${track1Where}
        GROUP BY reason
        ORDER BY count DESC, reason ASC
      `),
      pool.query(verifierSql, [clientId]),
      pool.query(`
        SELECT COUNT(*)::int AS count
        FROM prospects
        WHERE status = 'warm'
          AND client_id = $1
          AND updated_at > NOW() - INTERVAL '7 days'
          AND EXISTS (
            SELECT 1 FROM email_events ee
            WHERE ee.client_id = prospects.client_id
              AND LOWER(ee.recipient_email) = LOWER(prospects.email)
              AND ee.event_type = 'clicked'
          )
      `, [clientId]),
    ]);

    const t = totals.rows[0];
    const w = weekTotals.rows[0];
    const denominator = Number(t.sent_total || 0);
    const denominatorLabel = 'sent';
    const weekDenominator = Number(w.sent_week || 0);
    const decorateBreakdown = row => {
      const den = Number(row.sent || 0);
      const denLabel = 'sent';
      return {
        ...row,
        denominator: den,
        denominator_label: denLabel,
        open_rate: pct(row.opened, Number(row.delivered || 0)),
        reply_rate: pct(row.replied, den),
        bounce_pct: pct(row.bounced, den),
        kill_switch_bounce_pct: pct(row.bounced, row.sent),
      };
    };

    res.json({
      days,
      denominator_label:  denominatorLabel,
      sent_total:         denominator,
      raw_sent_total:     t.sent_total,
      delivered_total:    t.delivered_total,
      opened_total:       t.opened_total,
      opened_proxy_total: t.opened_proxy_total,
      sent_week:          weekDenominator,
      raw_sent_week:      w.sent_week,
      delivered_week:     w.delivered_week,
      opened_week:        w.opened_week,
      opened_proxy_week:  w.opened_proxy_week,
      open_rate:          pct(t.opened_total, t.delivered_total),
      click_rate:         pct(t.clicked_total, denominator),
      reply_rate:         pct(t.replied_total, denominator),
      bounce_rate:        pct(t.bounced_total, denominator),
      unsub_rate:         pct(t.unsub_total, denominator),
      open_rate_week:     pct(w.opened_week, w.delivered_week),
      warm_upgraded_week: warmUpgraded.rows[0].count,
      pipeline_health:    pipelineHealth.rows[0],
      track1_reasons:     track1Reasons.rows,
      track1_window_days: excludedHasCreatedAt ? 7 : null,
      verifier_stats:     verifierStats.rows,
      domain_health:      domainBreakdown.rows.map(decorateBreakdown),
      vertical_breakdown: verticalBreakdown.rows.map(decorateBreakdown),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Max daily brief
router.get('/api/max-brief', requireDashboardRead, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT payload, ran_at
      FROM agent_log
      WHERE agent_name = 'max' AND action = 'daily_digest' AND client_id = $1
      ORDER BY ran_at DESC
      LIMIT 1
    `, [clientId]);
    if (!result.rows.length) {
      return res.json({
        client_id: clientId,
        insights: null,
        ran_at: null,
        message: 'No brief generated yet for this client',
      });
    }
    const row = result.rows[0];
    res.json({ client_id: clientId, insights: row.payload?.insights || null, ran_at: row.ran_at });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Rex cross-market executive summary
router.get('/api/rex-executive-summary', requireDashboardRead, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT payload, ran_at
      FROM agent_log
      WHERE agent_name = 'rex'
        AND action = 'executive_summary'
        AND status = 'success'
      ORDER BY ran_at DESC
      LIMIT 1
    `);
    if (!result.rows.length) return res.json({ summary: null, ran_at: null });
    const row = result.rows[0];
    const payload = typeof row.payload === 'string' ? JSON.parse(row.payload || '{}') : (row.payload || {});
    res.json({ summary: payload.summary || null, ran_at: row.ran_at });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Agent actions (deposited by Max)
router.get('/api/actions', requireOperator, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT id, created_by, action_type, title, description, payload, status, created_at, executed_at, result
      FROM agent_actions
      WHERE status IN ('pending', 'in_progress')
        AND client_id = $1
      ORDER BY created_at DESC
    `, [clientId]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/api/actions/:id/dismiss', requireOperator, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    await pool.query(
      `UPDATE agent_actions SET status = 'dismissed', executed_at = NOW() WHERE id = $1 AND client_id = $2`,
      [req.params.id, clientId]
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/api/actions/:id/execute', requireOperator, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    await pool.query(
      `UPDATE agent_actions SET status = 'executed', executed_at = NOW(), result = $2 WHERE id = $1 AND client_id = $3`,
      [req.params.id, req.body.result || 'Marked done from dashboard', clientId]
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Trigger agents
router.post('/api/run/:agent', requireOperator, async (req, res) => {
  const { agent } = req.params;
  const clientId = getRequestClientId(req);
  const localOnly = ['ivy'];
  if (localOnly.includes(agent)) {
    return res.json({ success: false, message: `${agent} requires local execution — run from your terminal` });
  }
  const agentModules = {
    scout: '../leadgen', emmett: '../emmettAgent',
    max: '../maxAgent', rex: '../rexAgent', sketch: '../sketchAgent',
    paige: '../paigeAgent', faye: '../facebookAgent', link: '../linkedinAgent',
    sam: '../samAgent', vera: '../veraAgent', cal: '../calAgent', ivy: '../ivyAgent',
    penny: '../pennyAgent', analytics: '../analyticsAgent', riley: '../rileyAgent',
    warm_signal: '../warmSignalAgent',
    tiered_enrichment: '../tieredEnrichmentAgent',
  };
  if (!agentModules[agent]) return res.status(400).json({ error: 'Unknown agent' });
  await ensureAgentLogStatusSchema();
  const triggerLog = await pool.query(
    `INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
     VALUES ($1, $2, $3, $4, NOW(), $5)
     RETURNING id`,
    [agent, 'dashboard_trigger', JSON.stringify({ triggered_by: 'dashboard', client_id: clientId }), 'pending', clientId]
  );
  const triggerLogId = triggerLog.rows[0]?.id;
  res.json({ success: true, message: `${agent} triggered successfully` });

  (async () => {
    try {
      delete require.cache[require.resolve(agentModules[agent])];
      process.env.ACTIVE_CLIENT_ID = String(clientId);
      const mod = require(agentModules[agent]);
      if (typeof mod.run !== 'function') {
        throw new Error(`Agent ${agent} does not export run()`);
      }

      const result = agent === 'scout'
        ? await mod.run({
            client_id: clientId,
            industry: req.body?.industry,
            location: req.body?.location,
            sourceMode: req.body?.sourceMode || req.body?.source_mode,
            titleFilter: req.body?.titleFilter || req.body?.title_filter,
            titleExclude: req.body?.titleExclude || req.body?.title_exclude,
            sizeSignal: req.body?.sizeSignal || req.body?.size_signal,
            dryRun: req.body?.dryRun ?? req.body?.dry_run,
            maxRequests: req.body?.maxRequests || req.body?.max_requests,
            pageDepth: req.body?.pageDepth || req.body?.page_depth,
          })
        : agent === 'emmett'
          ? await mod.run({ client_id: clientId, triggered_by: 'dashboard' })
          : await mod.run({ client_id: clientId });

      if (triggerLogId) {
        await pool.query(`
          UPDATE agent_log
             SET status = 'completed',
                 payload = COALESCE(payload, '{}'::jsonb) || $2::jsonb
           WHERE id = $1
        `, [triggerLogId, JSON.stringify({ completed_at: new Date().toISOString(), result: result || null })]);
      }
    } catch (err) {
      console.error(`Agent ${agent} error:`, err.message);
      if (triggerLogId) {
        await pool.query(`
          UPDATE agent_log
             SET status = 'failed',
                 error_msg = $2,
                 payload = COALESCE(payload, '{}'::jsonb) || $3::jsonb
           WHERE id = $1
        `, [
          triggerLogId,
          err.message,
          JSON.stringify({
            failed_at: new Date().toISOString(),
            error: err.message,
            stack_preview: String(err.stack || '').split('\n').slice(0, 4).join('\n'),
          }),
        ]).catch(logErr => console.error(`Agent ${agent} trigger log update failed:`, logErr.message));
      }
    }
  })();
});

// ---------------------------------------------------------------------------
// Mira context endpoint (READ-ONLY — for Claude.ai consumption)
//
// GET /api/mira/context and GET /api/mira/context/:secret return a JSON snapshot
// of Jacob's current Mira state so an external Claude.ai conversation can read
// live context when Jacob talks to it. This endpoint is strictly read-only: it
// issues SELECT-only DB reads and outbound Todoist GET requests, and never
// mutates any Mira table, agent, or schema. It does NOT use session auth —
// instead it is guarded by the MIRA_CONTEXT_SECRET env var (passed as
// ?secret=..., an `Authorization: Bearer <secret>` header, or the :secret path
// parameter), mirroring the CRON_SECRET guard pattern used by the /cron routes.
// ---------------------------------------------------------------------------
function requireMiraContextSecret(req, res, next) {
  const expected = process.env.MIRA_CONTEXT_SECRET;
  const header = String(req.get('authorization') || '');
  const hasBearer = header.toLowerCase().startsWith('bearer ');
  const bearer = hasBearer ? header.slice(7).trim() : null;
  const hasQuery = Object.prototype.hasOwnProperty.call(req.query, 'secret');
  const query = typeof req.query.secret === 'string' ? req.query.secret : null;
  const hasPath = Object.prototype.hasOwnProperty.call(req.params, 'secret');
  const path = typeof req.params.secret === 'string' ? req.params.secret : null;

  const matchesExpected = provided => {
    if (typeof provided !== 'string' || typeof expected !== 'string') return false;
    const providedBuffer = Buffer.from(provided, 'utf8');
    const expectedBuffer = Buffer.from(expected, 'utf8');
    return providedBuffer.length === expectedBuffer.length
      && crypto.timingSafeEqual(providedBuffer, expectedBuffer);
  };

  // All credential sources are independent; any valid value grants access.
  if (expected && (matchesExpected(query) || matchesExpected(bearer) || matchesExpected(path))) {
    return next();
  }

  const sources = [];
  if (hasQuery) sources.push({ source: 'query', length: query === null ? null : Buffer.byteLength(query, 'utf8') });
  if (hasBearer) sources.push({ source: 'bearer', length: Buffer.byteLength(bearer, 'utf8') });
  if (hasPath) sources.push({ source: 'path', length: path === null ? null : Buffer.byteLength(path, 'utf8') });
  console.warn(`[mira_context] auth rejected ${JSON.stringify({
    secret_provided: sources.some(source => Number(source.length) > 0),
    sources,
  })}`);

  return res.status(401).json({ error: 'Unauthorized' });
}

router.get(['/api/mira/context', '/api/mira/context/:secret'], requireMiraContextSecret, async (req, res) => {
  try {
    // Every table read below is SELECT-only and individually fault-isolated: a
    // missing or empty table yields [] / null instead of failing the snapshot.
    const safeRows = (promise, fallback = []) => promise.then(r => r.rows).catch(err => {
      console.error('[mira_context] query failed:', err.message);
      return fallback;
    });

    const [
      recentCaptures,
      openBlockers,
      activeClients,
      recentClientNotes,
      recentCorrections,
      currentAnchor,
      todoist,
      dailyHealthYesterday,
      dailyHealthToday,
      dailyHealthTrend,
    ] = await Promise.all([
      // Last 10 captures with a 150-char preview of transcript (preferred) or raw_text.
      safeRows(pool.query(`
        SELECT ci.id,
               LEFT(COALESCE(NULLIF(ci.transcript, ''), ci.raw_text, ''), 150) AS content_preview,
               ci.classification, ci.status, ci.received_at,
               ci.capture_type, ci.source, ci.linked_entity_type, ci.linked_entity_id,
               ci.linked_capture_id, ci.captured_at
        FROM capture_inbox ci
        LEFT JOIN clients direct_client ON direct_client.id = ci.client_id
        LEFT JOIN prospects linked_prospect
          ON ci.linked_entity_type = 'prospect'
         AND linked_prospect.id::text = ci.linked_entity_id
        LEFT JOIN clients linked_client ON linked_client.id = linked_prospect.client_id
        WHERE COALESCE(ci.archived, false) = false
          AND (ci.client_id IS NULL OR direct_client.active = true)
          AND (
            linked_prospect.id IS NULL
            OR (linked_client.active = true AND COALESCE(linked_prospect.mira_archived, false) = false)
          )
        ORDER BY ci.received_at DESC
        LIMIT 10
      `)),
      // Open (unresolved) blockers, oldest first, with days_open computed at read time.
      safeRows(pool.query(`
        SELECT id, content, blocking,
               GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (NOW() - created_at)) / 86400))::int AS days_open
        FROM blockers
        WHERE resolved = false
        ORDER BY created_at ASC
      `)),
      // Active clients. The clients table uses an `active` boolean (there is no
      // `status` column), so "status='active'" maps to active = true here.
      safeRows(pool.query(`
        SELECT id, name FROM clients WHERE active = true ORDER BY name ASC
      `)),
      // Last 10 client notes joined to the client name, 150-char content preview.
      safeRows(pool.query(`
        SELECT cn.created_at,
               c.name AS client_name,
               LEFT(COALESCE(cn.content, ''), 150) AS content_preview
        FROM client_notes cn
        JOIN clients c ON c.id = cn.client_id AND c.active = true
        WHERE COALESCE(cn.archived, false) = false
        ORDER BY cn.created_at DESC
        LIMIT 10
      `)),
      // Last 5 Mira misclassification corrections.
      safeRows(pool.query(`
        SELECT mc.original_class, mc.corrected_class, mc.created_at
        FROM mira_corrections mc
        JOIN capture_inbox ci ON ci.id = mc.capture_id
        WHERE COALESCE(mc.archived, false) = false
          AND COALESCE(ci.archived, false) = false
          AND mc.original_class <> mc.corrected_class
        ORDER BY mc.created_at DESC
        LIMIT 5
      `)),
      getCurrentAnchor(pool).catch(err => {
        console.error('[mira_context] anchor failed:', err.message);
        return null;
      }),
      getTodoistSnapshot().catch(err => {
        console.error('[mira_context] todoist failed:', err.message);
        return { configured: false, open_tasks_count: 0, stale_tasks: [], error: err.message };
      }),
      safeRows(pool.query(`
        SELECT *
        FROM daily_health_log
        WHERE log_date = (NOW() AT TIME ZONE 'America/New_York')::date - 1
      `)),
      safeRows(pool.query(`
        SELECT *
        FROM daily_health_log
        WHERE log_date = (NOW() AT TIME ZONE 'America/New_York')::date
      `)),
      safeRows(pool.query(`
        SELECT log_date, send_count_today, bounce_count_today, reply_count_today,
               warm_signals_fired_today, health_flags
        FROM daily_health_log
        WHERE log_date >= (NOW() AT TIME ZONE 'America/New_York')::date - 6
          AND log_date <= (NOW() AT TIME ZONE 'America/New_York')::date
        ORDER BY log_date DESC
      `)),
    ]);

    res.json({
      now: new Date().toISOString(),
      live_workstreams: LIVE_WORKSTREAMS,
      recent_captures: recentCaptures,
      current_anchor: currentAnchor,
      open_tasks_count: todoist.open_tasks_count,
      active_tasks: todoist.active_tasks || [],
      stale_tasks: todoist.stale_tasks,
      open_blockers: openBlockers,
      active_clients: activeClients,
      recent_client_notes: recentClientNotes,
      recent_corrections: recentCorrections,
      daily_health_yesterday: dailyHealthYesterday[0] || null,
      daily_health_today: dailyHealthToday[0] || null,
      daily_health_trend_7d: dailyHealthTrend,
    });
  } catch (err) {
    console.error('[mira_context] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
