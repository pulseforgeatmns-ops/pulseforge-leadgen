const express = require('express');
const router = express.Router();
const pool = require('../db');

const BREVO_EVENT_MAP = {
  opened:           'email_opened',
  click:            'email_clicked',
  loaded_by_proxy:  'email_opened',
  hard_bounce:      'email_bounced',
  soft_bounce:      'email_soft_bounce',
  unsubscribed:     'email_unsubscribed',
  spam:             'email_spam',
};

async function checkAndUpdateWarmStatus(prospectId, email) {
  try {
    const res = await pool.query(`
      SELECT
        COUNT(CASE WHEN action_type = 'email_opened'
              AND created_at > NOW() - INTERVAL '14 days' THEN 1 END)::int AS opens_14d,
        COUNT(CASE WHEN action_type = 'email_clicked' THEN 1 END)::int AS clicks_all
      FROM touchpoints
      WHERE prospect_id = $1 AND channel = 'email'
    `, [prospectId]);
    const { opens_14d, clicks_all } = res.rows[0];
    const opens  = parseInt(opens_14d  || 0);
    const clicks = parseInt(clicks_all || 0);
    if (clicks >= 1 || opens >= 3) {
      const upd = await pool.query(
        `UPDATE prospects SET status = 'warm', updated_at = NOW()
         WHERE id = $1 AND status = 'cold' RETURNING id`,
        [prospectId]
      );
      if (upd.rows.length > 0) {
        console.log(`[Riley] ${email} upgraded to warm — ${opens} opens / ${clicks} clicks`);
      }
    }
  } catch (err) {
    console.error('[Riley] checkAndUpdateWarmStatus error:', err.message);
  }
}

router.post('/webhooks/brevo', (req, res) => {
  res.status(200).json({ ok: true });
  setImmediate(async () => {
    try {
      const payload = req.body || {};
      const event      = payload.event;
      const email      = (payload.email || '').toLowerCase().trim();
      const actionType = BREVO_EVENT_MAP[event];

      if (!actionType || !email) return;

      const prospectRes = await pool.query(
        `SELECT id, status FROM prospects WHERE LOWER(email) = $1 LIMIT 1`,
        [email]
      );
      if (!prospectRes.rows.length) {
        console.warn(`[Riley] No prospect for email: ${email} (event: ${event})`);
        return;
      }
      const prospect = prospectRes.rows[0];

      const outcomeJson = JSON.stringify({
        event,
        subject:    payload.subject  || null,
        link:       payload.link     || null,
        brevo_id:   payload.id       || null,
        message_id: payload.messageId || null,
        date:       payload.date     || null,
      });
      await pool.query(`
        INSERT INTO touchpoints
          (prospect_id, channel, action_type, content_summary, outcome, sentiment, external_ref)
        VALUES ($1, 'email', $2, $3, $4, 'neutral', $5)
      `, [
        prospect.id, actionType,
        payload.subject || null,
        outcomeJson,
        payload.messageId || null,
      ]);

      if (['email_bounced', 'email_spam', 'email_unsubscribed'].includes(actionType)) {
        await pool.query(
          `UPDATE prospects SET do_not_contact = true, updated_at = NOW() WHERE id = $1`,
          [prospect.id]
        );
        console.log(`[Riley] ${email} marked do_not_contact (${event})`);
      }

      if (['email_opened', 'email_clicked'].includes(actionType)) {
        await checkAndUpdateWarmStatus(prospect.id, email);
      }

      await pool.query(`
        INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at)
        VALUES ('riley', $1, $2, $3, 'success', NOW())
      `, [
        actionType,
        prospect.id,
        JSON.stringify({ event, email, subject: payload.subject, link: payload.link }),
      ]);

      console.log(`[Riley] Tracked ${event} for ${email}`);
    } catch (err) {
      console.error('[Riley] Webhook error:', err.message);
    }
  });
});

router.post('/webhooks/bland', async (req, res) => {
  res.sendStatus(200);

  const { call_id, status, duration, transcript, summary, metadata } = req.body || {};
  if (!call_id) return;

  const prospectId  = metadata?.prospect_id;
  const companyName = metadata?.company_name || 'Unknown';

  console.log(`[bland webhook] call_id=${call_id} status=${status} prospect=${prospectId}`);

  try {
    if (prospectId) {
      await pool.query(`
        UPDATE touchpoints
        SET outcome = $1, payload = payload || $2::jsonb
        WHERE prospect_id = $3
          AND channel = 'call'
          AND external_ref = $4
      `, [
        status || 'completed',
        JSON.stringify({ duration, summary }),
        prospectId,
        call_id,
      ]);
    }

    const fullText = typeof transcript === 'string'
      ? transcript
      : Array.isArray(transcript) ? transcript.map(t => `${t.user}: ${t.text}`).join('\n') : '';

    if (!fullText || status !== 'completed') return;

    const Anthropic = require('@anthropic-ai/sdk');
    const anthropic = new Anthropic();

    const parseRes = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 200,
      messages: [{
        role: 'user',
        content: `Read this phone call transcript and extract booking information if a discovery call was booked.

Transcript:
${fullText.slice(0, 3000)}

Respond with JSON only — no explanation:
{
  "booked": true/false,
  "agreed_day": "Monday" or null,
  "agreed_time": "2pm" or null,
  "agreed_iso": "ISO 8601 datetime in America/New_York if determinable, else null",
  "confirmed_email": "email if stated, else null",
  "prospect_name": "name if stated, else null"
}`
      }]
    });

    let parsed;
    try {
      const raw = parseRes.content[0].text.trim();
      parsed = JSON.parse(raw.replace(/^```json\n?/, '').replace(/\n?```$/, ''));
    } catch {
      console.log('[bland webhook] Could not parse Claude response');
      return;
    }

    const { createCalendarEvent, notify } = require('../calAgent');

    let calendarCreated = false;
    if (parsed.booked && parsed.agreed_iso) {
      const event = await createCalendarEvent(
        parsed.prospect_name || 'Prospect',
        companyName,
        parsed.agreed_iso
      );
      calendarCreated = !!event;
    }

    const lines = [
      parsed.booked ? `✅ Discovery call BOOKED — Cal` : `📞 Call complete — Cal`,
      ``,
      `Business: ${companyName}`,
      `Outcome: ${status || 'completed'}`,
      duration ? `Duration: ${Math.round(duration / 60)} min` : null,
    ];

    if (parsed.booked) {
      if (parsed.agreed_day || parsed.agreed_time) {
        lines.push(`Agreed time: ${[parsed.agreed_day, parsed.agreed_time].filter(Boolean).join(' ')}`);
      }
      if (parsed.confirmed_email) lines.push(`Email confirmed: ${parsed.confirmed_email}`);
      lines.push(calendarCreated ? `📅 Calendar invite created` : `⚠️ Calendar invite skipped — set GOOGLE_CALENDAR_REFRESH_TOKEN`);
    }

    await notify(lines.filter(l => l !== null).join('\n'));

    await pool.query(
      `INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at)
       VALUES ($1, $2, $3, $4, $5, NOW())`,
      ['cal_agent', 'call_completed', prospectId,
       JSON.stringify({ call_id, booked: parsed.booked, calendar_created: calendarCreated }),
       'success']
    );

  } catch (err) {
    console.error('[bland webhook] Error processing callback:', err.message);
  }
});

module.exports = router;
