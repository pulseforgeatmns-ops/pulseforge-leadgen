const express = require('express');
const router = express.Router();
const pool = require('../db');
const { requireAuth: sessionAuth, requireRole } = require('../middleware/auth');
const { ensureClientArchitecture, getActiveClients, getRequestClientId, normalizeClientId } = require('../utils/clientContext');
const { ensureCloserSchema } = require('../utils/closerSchema');
const { publishBlogPost } = require('../utils/blogPublisher');
const {
  publishToGoogleBusiness,
  publishToFacebookPage,
  publishFayeComment,
  publishToLinkedInPage,
  publishLinkComment,
} = require('../utils/publishPipeline');

const requireAuth = [sessionAuth, requireRole('admin', 'manager')];

router.get('/api/me', sessionAuth, (req, res) => {
  res.json({ user: req.user, active_client_id: req.session?.active_client_id || 1 });
});

router.get('/api/clients', requireAuth, async (req, res) => {
  try {
    res.json({
      active_client_id: req.session.active_client_id || 1,
      clients: await getActiveClients(),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/api/clients/active', requireAuth, async (req, res) => {
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

router.get('/api/agent-visibility', requireAuth, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const [clientResult, prospectsWithEmail] = await Promise.all([
      pool.query('SELECT id, facebook_url, gbp_url, max_email FROM clients WHERE id = $1', [clientId]),
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
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/pipeline', requireAuth, async (_req, res) => {
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

    const [clients, revenueMrr, revenueBooked, revenuePayouts, setters, closers, logs] = await Promise.all([
      pool.query(`
        SELECT
          c.id, c.name, c.slug, c.email, c.city, c.state,
          c.max_email, c.active, c.created_at,
          COUNT(DISTINCT p.id)::int as prospect_count,
          COUNT(DISTINCT p.id) FILTER (
            WHERE p.setter_status = 'booked')::int as booked_count,
          COUNT(DISTINCT p.id) FILTER (
            WHERE p.setter_status = 'closed')::int as closed_count
        FROM clients c
        LEFT JOIN prospects p ON p.client_id = c.id
        GROUP BY c.id
        ORDER BY c.created_at ASC
      `),
      pool.query(`SELECT COALESCE(SUM(mrr_amount), 0)::numeric as confirmed_mrr FROM commissions WHERE status != 'void'`),
      pool.query(`SELECT COUNT(*)::int as booked_pending FROM prospects WHERE setter_status = 'booked' AND closed_at IS NULL`),
      pool.query(`
        SELECT
          COALESCE(SUM(commission_amt) FILTER (
            WHERE status = 'pending'), 0)::numeric as pending_payout,
          COALESCE(SUM(commission_amt) FILTER (
            WHERE status = 'paid'), 0)::numeric as paid_out
        FROM commissions
      `),
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
        LEFT JOIN activity_log al ON al.setter_id::text = u.id::text
        WHERE u.role = 'setter' AND u.active = true
        GROUP BY u.id, u.name, u.assigned_city
        ORDER BY u.name ASC
      `),
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
        GROUP BY u.id, u.name
        ORDER BY u.name ASC
      `),
      pool.query(`
        SELECT DISTINCT ON (agent_name)
          agent_name, status, ran_at, error_msg, duration_ms
        FROM agent_log
        ORDER BY agent_name, ran_at DESC
      `),
    ]);

    const latestByAgent = {};
    logs.rows.forEach(row => {
      const normalized = agentKeys[String(row.agent_name || '').toLowerCase()];
      if (normalized && !latestByAgent[normalized]) latestByAgent[normalized] = row;
    });

    res.json({
      refreshed_at: new Date().toISOString(),
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
router.get('/api/agent-status', requireAuth, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const [prospects, touchpoints, pending, agentRuns, channels, weeklyTouchpoints] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM prospects WHERE client_id = $1', [clientId]),
      pool.query('SELECT COUNT(*) FROM touchpoints WHERE client_id = $1', [clientId]),
      pool.query('SELECT COUNT(*) FROM pending_comments WHERE status = $1 AND client_id = $2', ['pending', clientId]),
      pool.query('SELECT agent_name, COUNT(*) as runs, MAX(ran_at) as last_run FROM agent_log WHERE client_id = $1 GROUP BY agent_name', [clientId]),
      pool.query('SELECT channel, COUNT(*) as count FROM pending_comments WHERE client_id = $1 GROUP BY channel', [clientId]),
      pool.query('SELECT COUNT(*) FROM touchpoints WHERE client_id = $1 AND created_at > NOW() - INTERVAL \'7 days\'', [clientId])
    ]);

    const runMap = {};
    agentRuns.rows.forEach(r => { runMap[r.agent_name] = parseInt(r.runs); });

    const totalProspects = parseInt(prospects.rows[0].count);
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
      prospects: totalProspects,
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
router.get('/api/approvals', requireAuth, async (req, res) => {
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
router.post('/api/approvals/:id', requireAuth, async (req, res) => {
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

// Prospects table
router.get('/api/prospects', requireAuth, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT
        p.id, p.first_name, p.last_name, p.email, p.phone,
        p.status, p.icp_score, p.notes, p.last_contacted_at, p.created_at,
        c.name as company_name,
        COUNT(t.id)::int as touchpoint_count
      FROM prospects p
      LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
      LEFT JOIN touchpoints t ON t.prospect_id = p.id AND t.client_id = p.client_id
      WHERE p.do_not_contact = false
        AND p.client_id = $1
      GROUP BY p.id, c.name
      ORDER BY p.icp_score DESC NULLS LAST
      LIMIT 200
    `, [clientId]);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Touchpoints for a single prospect
router.get('/api/prospects/:id/touchpoints', requireAuth, async (req, res) => {
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

// Agent stats for sparklines
router.get('/api/agent-stats', requireAuth, async (req, res) => {
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

// Agent weekly stats for hover tooltips
router.get('/api/agent-weekly-stats', requireAuth, async (req, res) => {
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
router.get('/api/activity', requireAuth, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT al.agent_name, al.action, al.status, al.ran_at, al.payload,
        p.first_name, p.last_name
      FROM agent_log al
      LEFT JOIN prospects p ON al.prospect_id = p.id AND p.client_id = al.client_id
      WHERE al.client_id = $1
      ORDER BY al.ran_at DESC
      LIMIT 20
    `, [clientId]);

    const agentNameMap = {
      facebook: 'Faye', linkedin: 'Link', emmett: 'Emmett',
      max: 'Max', rex: 'Rex', scout: 'Scout', sketch: 'Sketch', email: 'Emmett'
    };

    const feed = result.rows.map(row => {
      const rawAgent = row.agent_name?.replace('_agent', '') || 'system';
      const agent = agentNameMap[rawAgent] || rawAgent.charAt(0).toUpperCase() + rawAgent.slice(1);
      const minutesAgo = Math.floor((Date.now() - new Date(row.ran_at)) / 60000);
      const timeLabel = minutesAgo < 60 ? `${minutesAgo}m` : minutesAgo < 1440 ? `${Math.floor(minutesAgo/60)}h` : `${Math.floor(minutesAgo/1440)}d`;
      const prospect = row.first_name ? `· ${row.first_name} ${row.last_name}`.trim() : '';
      const actionLabels = {
        generate_comment: `generated a comment draft ${prospect}`,
        daily_digest: 'daily digest sent · jacob@gopulseforge.com',
        weekly_report: 'weekly report dispatched',
        generate_mockup: `generated a mockup ${prospect}`,
        outbound: `sent email sequence ${prospect}`,
        dashboard_trigger: 'triggered from dashboard'
      };
      const label = actionLabels[row.action] || row.action;
      const icons = {
        Faye: { icon: '📣', color: 'fi-t' }, Link: { icon: '💬', color: 'fi-p' },
        Emmett: { icon: '✉️', color: 'fi-o' }, Max: { icon: '🧠', color: 'fi-p' },
        Rex: { icon: '📊', color: 'fi-p' }, Scout: { icon: '🔍', color: 'fi-t' },
        Sketch: { icon: '🎨', color: 'fi-t' }
      };
      const { icon, color } = icons[agent] || { icon: '⚡', color: 'fi-g' };
      return { agent, action: label, icon, color, time: timeLabel, status: row.status };
    });

    res.json(feed);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Activity panel (sequences + timeline)
router.get('/api/activity-panel', requireAuth, async (req, res) => {
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
      const prospectName = r.first_name ? `${r.first_name} ${r.last_name}`.trim() : null;
      const prospectBiz = prospectName || (r.prospect_notes || '').split('—')[0].trim() || null;
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
router.get('/api/activity-timeline', requireAuth, async (req, res) => {
  const offset = parseInt(req.query.offset) || 0;
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT al.id, al.agent_name, al.action, al.status, al.ran_at,
        p.first_name, p.last_name, p.notes as prospect_notes
      FROM agent_log al
      LEFT JOIN prospects p ON al.prospect_id = p.id AND p.client_id = al.client_id
      WHERE al.client_id = $2
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
      const prospectName = r.first_name ? `${r.first_name} ${r.last_name}`.trim() : null;
      const prospectBiz = prospectName || (r.prospect_notes || '').split('—')[0].trim() || null;
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
router.get('/api/analytics', requireAuth, async (req, res) => {
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
router.get('/api/analytics/posts', requireAuth, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT
        pa.id, pa.channel, pa.content_type, pa.post_text,
        pa.platform_post_id, pa.published_at,
        pa.post_day_of_week, pa.post_hour,
        pa.likes, pa.comments, pa.shares, pa.reach, pa.clicks,
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
router.get('/api/analytics/summary', requireAuth, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT
        cps.channel, cps.content_type,
        cps.post_count, cps.avg_likes, cps.avg_comments,
        cps.avg_shares, cps.avg_reach, cps.avg_engagement_rate,
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
router.get('/api/analytics/top-posts', requireAuth, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT
        pa.id, pa.channel, pa.content_type,
        LEFT(pa.post_text, 120) AS post_preview,
        pa.published_at, pa.likes, pa.comments, pa.shares,
        pa.reach, pa.engagement_rate,
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
router.get('/api/analytics/email', requireAuth, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const [totals, weekTotals, warmUpgraded] = await Promise.all([
      pool.query(`
        SELECT
          COUNT(CASE WHEN action_type = 'outbound'            THEN 1 END)::int AS sent_total,
          COUNT(CASE WHEN action_type = 'email_opened'        THEN 1 END)::int AS opened_total,
          COUNT(CASE WHEN action_type = 'email_clicked'       THEN 1 END)::int AS clicked_total,
          COUNT(CASE WHEN action_type = 'email_bounced'       THEN 1 END)::int AS bounced_total,
          COUNT(CASE WHEN action_type = 'email_unsubscribed'  THEN 1 END)::int AS unsub_total
        FROM touchpoints WHERE channel = 'email' AND client_id = $1
      `, [clientId]),
      pool.query(`
        SELECT
          COUNT(CASE WHEN action_type = 'outbound'           THEN 1 END)::int AS sent_week,
          COUNT(CASE WHEN action_type = 'email_opened'       THEN 1 END)::int AS opened_week,
          COUNT(CASE WHEN action_type = 'email_clicked'      THEN 1 END)::int AS clicked_week,
          COUNT(CASE WHEN action_type = 'email_bounced'      THEN 1 END)::int AS bounced_week
        FROM touchpoints
        WHERE channel = 'email' AND client_id = $1 AND created_at > NOW() - INTERVAL '7 days'
      `, [clientId]),
      pool.query(`
        SELECT COUNT(*)::int AS count
        FROM prospects
        WHERE status = 'warm'
          AND client_id = $1
          AND updated_at > NOW() - INTERVAL '7 days'
          AND EXISTS (
            SELECT 1 FROM touchpoints t
            WHERE t.prospect_id = prospects.id AND t.client_id = prospects.client_id AND t.action_type = 'email_clicked'
          )
      `, [clientId]),
    ]);

    const t = totals.rows[0];
    const w = weekTotals.rows[0];
    const pct = (num, den) => den > 0 ? +((num / den) * 100).toFixed(1) : 0;

    res.json({
      sent_total:         t.sent_total,
      sent_week:          w.sent_week,
      open_rate:          pct(t.opened_total, t.sent_total),
      click_rate:         pct(t.clicked_total, t.sent_total),
      bounce_rate:        pct(t.bounced_total, t.sent_total),
      unsub_rate:         pct(t.unsub_total, t.sent_total),
      open_rate_week:     pct(w.opened_week, w.sent_week),
      warm_upgraded_week: warmUpgraded.rows[0].count,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Max daily brief
router.get('/api/max-brief', requireAuth, async (req, res) => {
  try {
    const clientId = getRequestClientId(req);
    const result = await pool.query(`
      SELECT payload, ran_at
      FROM agent_log
      WHERE agent_name = 'max' AND action = 'daily_digest' AND client_id = $1
      ORDER BY ran_at DESC
      LIMIT 1
    `, [clientId]);
    if (!result.rows.length) return res.json({ insights: null, ran_at: null });
    const row = result.rows[0];
    res.json({ insights: row.payload?.insights || null, ran_at: row.ran_at });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Agent actions (deposited by Max)
router.get('/api/actions', requireAuth, async (req, res) => {
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

router.post('/api/actions/:id/dismiss', requireAuth, async (req, res) => {
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

router.post('/api/actions/:id/execute', requireAuth, async (req, res) => {
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
router.post('/api/run/:agent', requireAuth, async (req, res) => {
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
  };
  if (!agentModules[agent]) return res.status(400).json({ error: 'Unknown agent' });
  await pool.query(
    `INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id) VALUES ($1, $2, $3, $4, NOW(), $5)`,
    [agent, 'dashboard_trigger', JSON.stringify({ triggered_by: 'dashboard', client_id: clientId }), 'pending', clientId]
  );
  res.json({ success: true, message: `${agent} triggered successfully` });
  try {
    delete require.cache[require.resolve(agentModules[agent])];
    process.env.ACTIVE_CLIENT_ID = String(clientId);
    const mod = require(agentModules[agent]);
    if (agent === 'scout' && typeof mod.run === 'function') {
      mod.run({ client_id: clientId }).catch(err => console.error(`Agent ${agent} error:`, err.message));
    } else if (typeof mod.run === 'function') {
      mod.run({ client_id: clientId }).catch(err => console.error(`Agent ${agent} error:`, err.message));
    }
  } catch (err) {
    console.error(`Agent ${agent} error:`, err.message);
  }
});

module.exports = router;
