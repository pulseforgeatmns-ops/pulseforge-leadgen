const express = require('express');
const Anthropic = require('@anthropic-ai/sdk');
const pool = require('../db');
const { requireAuth: sessionAuth, requireRole } = require('../middleware/auth');
const { getRequestClientId } = require('../utils/clientContext');

const router = express.Router();
const requireDashboardAuth = [sessionAuth, requireRole('admin', 'manager')];
const anthropic = new Anthropic();

async function loadPipelineContext(clientId) {
  const [prospectCounts, recentTouchpoints, warmSignals] = await Promise.all([
    pool.query(`
      SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE status = 'cold')::int AS cold,
        COUNT(*) FILTER (WHERE status = 'warm')::int AS warm,
        COUNT(*) FILTER (WHERE status = 'hot')::int AS hot,
        COUNT(*) FILTER (WHERE status = 'dead')::int AS dead,
        COUNT(*) FILTER (WHERE COALESCE(setter_visible, false) = true)::int AS setter_visible,
        COUNT(*) FILTER (WHERE setter_status = 'booked')::int AS booked,
        COUNT(*) FILTER (WHERE setter_status = 'closed')::int AS closed
      FROM prospects
      WHERE client_id = $1
    `, [clientId]),
    pool.query(`
      SELECT
        p.first_name, p.last_name, p.email, p.status, p.icp_score,
        COALESCE(c.name, NULLIF(COALESCE(p.notes, ''), '')) AS business,
        t.channel, t.action_type, t.content_summary, t.outcome, t.created_at
      FROM touchpoints t
      JOIN prospects p ON p.id = t.prospect_id AND p.client_id = t.client_id
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE t.client_id = $1
        AND t.created_at >= NOW() - INTERVAL '7 days'
      ORDER BY t.created_at DESC
      LIMIT 25
    `, [clientId]),
    pool.query(`
      SELECT
        p.first_name, p.last_name, p.email, p.status, p.icp_score,
        COALESCE(c.name, NULLIF(COALESCE(p.notes, ''), '')) AS business,
        MAX(t.created_at) AS last_signal_at,
        COUNT(*) FILTER (WHERE t.action_type IN ('email_opened', 'open'))::int AS opens,
        COUNT(*) FILTER (WHERE t.action_type IN ('email_clicked', 'click'))::int AS clicks,
        COUNT(*) FILTER (WHERE t.action_type IN ('reply', 'inbound', 'email_reply'))::int AS replies
      FROM touchpoints t
      JOIN prospects p ON p.id = t.prospect_id AND p.client_id = t.client_id
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE t.client_id = $1
        AND t.created_at >= NOW() - INTERVAL '14 days'
        AND t.action_type IN ('email_opened', 'open', 'email_clicked', 'click', 'reply', 'inbound', 'email_reply')
      GROUP BY p.id, p.first_name, p.last_name, p.email, p.status, p.icp_score, c.name
      ORDER BY clicks DESC, replies DESC, last_signal_at DESC
      LIMIT 15
    `, [clientId]),
  ]);

  return {
    generated_at: new Date().toISOString(),
    prospect_counts: prospectCounts.rows[0] || {},
    recent_touchpoints: recentTouchpoints.rows,
    warm_signals: warmSignals.rows,
  };
}

router.post('/api/max/ask', requireDashboardAuth, async (req, res) => {
  try {
    const question = String(req.body?.question || '').trim().slice(0, 2000);
    if (!question) return res.status(400).json({ error: 'Question is required' });
    if (!process.env.ANTHROPIC_API_KEY) return res.status(500).json({ error: 'ANTHROPIC_API_KEY is not configured' });

    const clientId = getRequestClientId(req);
    const context = await loadPipelineContext(clientId);
    const message = await anthropic.messages.create({
      model: process.env.MAX_CHAT_MODEL || 'claude-sonnet-4-5',
      max_tokens: 700,
      system: `You are Max, the manager agent for Pulseforge. You answer operator questions about the sales and marketing pipeline using only the provided database context. Be concise, direct, and practical. If the context does not contain enough evidence, say what is missing instead of inventing details. Prioritize warm signals, pipeline risk, next actions, and anomalies.`,
      messages: [{
        role: 'user',
        content: `Current pipeline context:
${JSON.stringify(context, null, 2)}

User question:
${question}`,
      }],
    });

    const answer = message.content
      .filter(part => part.type === 'text')
      .map(part => part.text)
      .join('\n')
      .trim();

    res.json({ answer, context_generated_at: context.generated_at });
  } catch (err) {
    console.error('[max_chat] ask error:', err.response?.data || err.message);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
