const express = require('express');
const axios = require('axios');
const router = express.Router();
const pool = require('../db');
const {
  depositWarmSignalAction,
  logSignalDroppedNoProspect,
  prospectExists,
  qualifyingOpenSignal,
  recalcICPAfterEmailEvent,
} = require('../rileyAgent');
const { recordEvent } = require('../utils/emailPerformance');
const { ensureMiraSchema } = require('../utils/miraSchema');
const {
  VALID_MIRA_CATEGORIES,
  correctMiraCapture,
  sendMiraTelegramMessage,
} = require('../utils/miraCorrections');
const {
  isWithinAnchorWindow,
  getAnchorForToday,
  parseAnchorReply,
  insertAnchor,
} = require('../utils/miraAnchor');

const miraSchemaReady = ensureMiraSchema().catch(err => {
  console.error('[mira] schema error:', err.message);
});

// Map a tracked email action_type to the email_performance event verb.
const PERF_EVENT_MAP = {
  email_opened: 'open',
  email_clicked: 'click',
  email_bounced: 'bounce',
  email_soft_bounce: 'bounce',
};

const MIRA_ACK = {
  voice: '🎙️ Got it',
  text: '📝 Got it',
  photo: '🖼️ Got it',
  link: '🔗 Got it',
  document: '📝 Got it',
};

function normalizeChatId(value) {
  return value === undefined || value === null ? '' : String(value).trim();
}

function telegramMessage(update = {}) {
  return update.message || update.edited_message || null;
}

function telegramCallbackQuery(update = {}) {
  return update.callback_query || null;
}

function extractTelegramUrl(message = {}) {
  const text = message.text || message.caption || '';
  const entities = message.entities || message.caption_entities || [];
  const textLink = entities.find(entity => entity.type === 'text_link' && entity.url);
  if (textLink) return textLink.url;

  const urlEntity = entities.find(entity => entity.type === 'url');
  if (urlEntity && Number.isInteger(urlEntity.offset) && Number.isInteger(urlEntity.length)) {
    return text.slice(urlEntity.offset, urlEntity.offset + urlEntity.length);
  }

  const match = text.match(/\bhttps?:\/\/[^\s<>"']+/i) || text.match(/\bwww\.[^\s<>"']+/i);
  if (!match) return null;
  return match[0].startsWith('www.') ? `https://${match[0]}` : match[0];
}

function parseMiraCapture(message = {}) {
  const rawText = message.text || message.caption || null;

  if (message.voice?.file_id) {
    return {
      content_type: 'voice',
      raw_text: rawText,
      voice_file_id: message.voice.file_id,
      photo_file_id: null,
      link_url: null,
    };
  }

  if (Array.isArray(message.photo) && message.photo.length) {
    const photo = message.photo[message.photo.length - 1];
    return {
      content_type: 'photo',
      raw_text: rawText,
      voice_file_id: null,
      photo_file_id: photo.file_id,
      link_url: extractTelegramUrl(message),
    };
  }

  if (message.document?.file_id) {
    return {
      content_type: 'document',
      raw_text: rawText,
      voice_file_id: null,
      photo_file_id: null,
      document_file_id: message.document.file_id,
      link_url: null,
    };
  }

  const linkUrl = extractTelegramUrl(message);
  return {
    content_type: linkUrl ? 'link' : 'text',
    raw_text: rawText,
    voice_file_id: null,
    photo_file_id: null,
    link_url: linkUrl,
  };
}

async function sendMiraAck(chatId, contentType, captureId) {
  const botToken = process.env.MIRA_TELEGRAM_BOT_TOKEN;
  if (!botToken) {
    console.warn('[mira] MIRA_TELEGRAM_BOT_TOKEN not set; ack skipped');
    return;
  }

  const prefix = MIRA_ACK[contentType] || '📝 Got it';
  await axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    chat_id: chatId,
    text: `${prefix} #${captureId}`,
  }, { timeout: 800 });
}

async function answerMiraCallback(callbackQueryId, text = null) {
  const botToken = process.env.MIRA_TELEGRAM_BOT_TOKEN;
  if (!botToken || !callbackQueryId) return;

  await axios.post(`https://api.telegram.org/bot${botToken}/answerCallbackQuery`, {
    callback_query_id: callbackQueryId,
    ...(text ? { text } : {}),
  }, { timeout: 3000 });
}

function buildMiraCategoryKeyboard(captureId) {
  return {
    inline_keyboard: [
      VALID_MIRA_CATEGORIES.slice(0, 2).map(category => ({
        text: category,
        callback_data: `mira_correct:${captureId}:${category}`,
      })),
      VALID_MIRA_CATEGORIES.slice(2, 4).map(category => ({
        text: category,
        callback_data: `mira_correct:${captureId}:${category}`,
      })),
      VALID_MIRA_CATEGORIES.slice(4, 6).map(category => ({
        text: category,
        callback_data: `mira_correct:${captureId}:${category}`,
      })),
      VALID_MIRA_CATEGORIES.slice(6, 8).map(category => ({
        text: category,
        callback_data: `mira_correct:${captureId}:${category}`,
      })),
    ],
  };
}

async function sendMiraCorrectionReply(result) {
  await sendMiraTelegramMessage(`Fixed. ${result.snippet} now classified as ${result.corrected_class}.`);
}

async function handleMiraCorrectCommand(message) {
  const match = String(message.text || '').trim().match(/^\/correct(?:@\w+)?\s+(\d+)\s+([a-z_]+)\s*$/i);
  if (!match) {
    await sendMiraTelegramMessage('Use /correct <capture_id> <new_category>.');
    return;
  }

  const captureId = Number(match[1]);
  const newCategory = match[2];
  const result = await correctMiraCapture(captureId, newCategory);
  await sendMiraCorrectionReply(result);
}

// Morning Anchor: the first plain-text reply during the 6am–noon ET window
// (when no anchor is set yet) is Jacob committing to his day's anchor. Parse it
// into a primary/secondary anchor, persist it, and confirm back over Telegram.
// A "skip today" / "out today" style reply is stored as a no-anchor day.
async function handleMiraAnchorReply(message) {
  const text = String(message.text || '').trim();
  const parsed = await parseAnchorReply(text);

  const inserted = await insertAnchor({
    primary_anchor: parsed.no_anchor ? null : parsed.primary_anchor,
    secondary_anchors: parsed.secondary_anchors || [],
    completion_notes: parsed.completion_notes || null,
  });

  // ON CONFLICT returned nothing — an anchor was already set (race). Stay quiet.
  if (!inserted) return;

  if (parsed.no_anchor || !parsed.primary_anchor) {
    const notes = parsed.completion_notes ? ` (${parsed.completion_notes})` : '';
    await sendMiraTelegramMessage(`Noted — no anchor set for today.${notes} Take care of what you need to.`);
    return;
  }

  const secondary = parsed.secondary_anchors || [];
  let reply = `Anchor set: ${parsed.primary_anchor}.`;
  if (secondary.length) reply += ` Secondary: ${secondary.join(', ')}.`;
  reply += ' End of day check at 9:30 PM.';
  await sendMiraTelegramMessage(reply);
}

async function handleMiraCallback(callbackQuery) {
  const data = String(callbackQuery.data || '');

  if (data.startsWith('mira_fix:')) {
    const captureId = Number(data.split(':')[1]);
    await answerMiraCallback(callbackQuery.id);
    await sendMiraTelegramMessage(`Fix #${captureId}: choose the right category.`, {
      reply_markup: buildMiraCategoryKeyboard(captureId),
    });
    return;
  }

  if (data.startsWith('mira_correct:')) {
    const [, captureIdText, newCategory] = data.split(':');
    const result = await correctMiraCapture(Number(captureIdText), newCategory);
    await answerMiraCallback(callbackQuery.id, 'Fixed.');
    await sendMiraCorrectionReply(result);
  }
}

async function getTelegramFileUrl(fileId) {
  const botToken = process.env.MIRA_TELEGRAM_BOT_TOKEN;
  if (!botToken || !fileId) return null;

  const fileRes = await axios.get(`https://api.telegram.org/bot${botToken}/getFile`, {
    params: { file_id: fileId },
    timeout: 5000,
  });
  const filePath = fileRes.data?.result?.file_path;
  return filePath ? `https://api.telegram.org/file/bot${botToken}/${filePath}` : null;
}

async function updateMiraFileUrl(captureId, capture) {
  try {
    if (capture.content_type === 'voice' && capture.voice_file_id) {
      const voiceUrl = await getTelegramFileUrl(capture.voice_file_id);
      if (voiceUrl) {
        await pool.query('UPDATE capture_inbox SET voice_url = $1 WHERE id = $2', [voiceUrl, captureId]);
      }
      return;
    }

    if (capture.content_type === 'photo' && capture.photo_file_id) {
      const photoUrl = await getTelegramFileUrl(capture.photo_file_id);
      if (photoUrl) {
        await pool.query('UPDATE capture_inbox SET photo_url = $1 WHERE id = $2', [photoUrl, captureId]);
      }
    }
  } catch (err) {
    console.error('[mira] file URL fetch error:', err.response?.data?.description || err.message);
  }
}

router.post('/telegram/mira', async (req, res) => {
  const update = req.body || {};
  const callbackQuery = telegramCallbackQuery(update);
  const message = telegramMessage(update);
  const chatId = normalizeChatId(callbackQuery?.message?.chat?.id || message?.chat?.id);
  const jacobChatId = normalizeChatId(process.env.JACOB_TELEGRAM_CHAT_ID);

  if (!jacobChatId) {
    console.error('[mira] JACOB_TELEGRAM_CHAT_ID not set');
    return res.sendStatus(500);
  }

  if ((!message && !callbackQuery) || chatId !== jacobChatId) {
    if (chatId && chatId !== jacobChatId) {
      console.log(`[mira] rejected telegram chat_id=${chatId}`);
    }
    return res.sendStatus(200);
  }

  try {
    await miraSchemaReady;

    if (callbackQuery) {
      res.status(200).json({ ok: true });
      handleMiraCallback(callbackQuery).catch(err => {
        console.error('[mira] callback error:', err.response?.data?.description || err.message);
        answerMiraCallback(callbackQuery.id, 'Could not fix this one.').catch(() => {});
      });
      return;
    }

    if (String(message.text || '').trim().startsWith('/correct')) {
      res.status(200).json({ ok: true });
      handleMiraCorrectCommand(message).catch(err => {
        console.error('[mira] correct command error:', err.response?.data?.description || err.message);
        sendMiraTelegramMessage(`Could not correct capture: ${err.message}`).catch(() => {});
      });
      return;
    }

    // Morning Anchor interception: a plain-text reply in the 6am–noon ET window,
    // before any anchor is set for today, is Jacob committing to his day's
    // anchor — not a thought to capture. Handle it and stop here.
    const anchorReplyText = String(message.text || '').trim();
    if (
      anchorReplyText &&
      !anchorReplyText.startsWith('/') &&
      isWithinAnchorWindow() &&
      !(await getAnchorForToday())
    ) {
      res.status(200).json({ ok: true });
      handleMiraAnchorReply(message).catch(err => {
        console.error('[mira] anchor reply error:', err.response?.data?.description || err.message);
      });
      return;
    }

    const capture = parseMiraCapture(message);
    const rawMetadata = {
      update_id: update.update_id || null,
      chat: message.chat || null,
      from: message.from || null,
      message_date: message.date || null,
      entities: message.entities || null,
      caption_entities: message.caption_entities || null,
      voice: message.voice || null,
      photo: message.photo || null,
      document: message.document || null,
    };

    const insert = await pool.query(`
      INSERT INTO capture_inbox
        (telegram_msg_id, content_type, raw_text, voice_file_id, photo_file_id, link_url, raw_metadata, status)
      VALUES
        ($1, $2, $3, $4, $5, $6, $7::jsonb, 'new')
      RETURNING id
    `, [
      message.message_id || null,
      capture.content_type,
      capture.raw_text,
      capture.voice_file_id,
      capture.photo_file_id,
      capture.link_url,
      JSON.stringify(rawMetadata),
    ]);

    const captureId = insert.rows[0].id;

    try {
      await sendMiraAck(chatId, capture.content_type, captureId);
    } catch (err) {
      console.error('[mira] ack failed:', err.response?.data?.description || err.message);
    }

    res.status(200).json({ ok: true });

    if (capture.voice_file_id || capture.photo_file_id) {
      setImmediate(() => updateMiraFileUrl(captureId, capture));
    }
  } catch (err) {
    console.error('[mira] webhook error:', err.stack || err.message);
    res.sendStatus(500);
  }
});

function brevoMessageId(payload = {}) {
  return payload.messageId ||
    payload.message_id ||
    payload['message-id'] ||
    payload['messageId'] ||
    payload['Message-ID'] ||
    null;
}

async function resolveProspectForBrevoEvent({ email, clientId, messageId, subject }) {
  if (messageId) {
    const byMessage = await pool.query(`
      SELECT
        p.id, p.status, p.client_id, p.first_name, p.last_name, p.email,
        p.vertical, p.icp_score, p.notes,
        c.name AS company_name
      FROM agent_log al
      JOIN prospects p ON p.id = al.prospect_id AND p.client_id = al.client_id
      LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
      WHERE al.agent_name = 'emmett'
        AND al.action = 'email_sent'
        AND al.payload->>'message_id' = $1
      ORDER BY al.ran_at DESC
      LIMIT 1
    `, [messageId]);
    if (byMessage.rows.length) return byMessage;
  }

  const params = [email, subject || null];
  if (clientId) params.push(clientId);
  return pool.query(
    `SELECT
       p.id, p.status, p.client_id, p.first_name, p.last_name, p.email,
       p.vertical, p.icp_score, p.notes,
       c.name AS company_name
     FROM prospects p
     LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
     LEFT JOIN LATERAL (
       SELECT al.ran_at
       FROM agent_log al
       WHERE al.agent_name = 'emmett'
         AND al.action = 'email_sent'
         AND al.prospect_id = p.id
         AND al.client_id = p.client_id
         AND ($2::text IS NULL OR al.payload->>'subject' = $2)
       ORDER BY al.ran_at DESC
       LIMIT 1
     ) latest_send ON TRUE
     WHERE LOWER(p.email) = $1
     ${clientId ? 'AND p.client_id = $3' : ''}
     ORDER BY latest_send.ran_at DESC NULLS LAST, p.created_at DESC
     LIMIT 1`,
    params
  );
}

// Roll an email engagement event into email_performance, pulling the
// vertical / sequence / step that Emmett recorded for this send from agent_log.
// Matches on the Brevo message_id when available, falling back to the
// prospect's most recent send. Best-effort — never throws into the webhook.
async function findMatchingSend(prospect, messageId, subject = null) {
  const params = [prospect.id, prospect.client_id];
  let sql = `
    SELECT payload FROM agent_log
    WHERE agent_name = 'emmett' AND action = 'email_sent'
      AND prospect_id = $1 AND client_id = $2`;
  if (messageId) {
    sql += ` AND payload->>'message_id' = $3`;
    params.push(messageId);
  }
  sql += ` ORDER BY ran_at DESC LIMIT 1`;

  let logRes = await pool.query(sql, params);
  if (!logRes.rows.length) {
    logRes = await pool.query(`
      SELECT payload FROM agent_log
      WHERE agent_name = 'emmett' AND action = 'email_sent'
        AND prospect_id = $1 AND client_id = $2
        AND ($3::text IS NULL OR payload->>'subject' = $3)
      ORDER BY ran_at DESC LIMIT 1
    `, [prospect.id, prospect.client_id, subject || null]);
  }

  return logRes.rows[0]?.payload || null;
}

async function recordPerformanceEvent(prospect, actionType, messageId, subject = null) {
  const perfEvent = PERF_EVENT_MAP[actionType];
  if (!perfEvent) return;
  try {
    const raw = await findMatchingSend(prospect, messageId, subject);
    const sendPayload = typeof raw === 'string' ? JSON.parse(raw) : (raw || {});
    const vertical = sendPayload.vertical ?? prospect.vertical ?? '';
    const sequence = sendPayload.sequence ?? '';
    const step = sendPayload.step ?? 0;

    await recordEvent(prospect.client_id, vertical, sequence, step, perfEvent);
  } catch (err) {
    console.error('[Riley] recordPerformanceEvent error:', err.message);
  }
}

const BREVO_EVENT_MAP = {
  opened:           'email_opened',
  email_opened:     'email_opened',
  click:            'email_clicked',
  clicked:          'email_clicked',
  loaded_by_proxy:  'email_opened',
  loadedByProxy:    'email_opened',
  loadedbyproxy:    'email_opened',
  hard_bounce:      'email_bounced',
  hardBounces:      'email_bounced',
  email_bounced:    'email_bounced',
  soft_bounce:      'email_soft_bounce',
  softBounces:      'email_soft_bounce',
  unsubscribed:     'email_unsubscribed',
  spam:             'email_spam',
};

function signalIso(value) {
  const parsed = value ? new Date(value) : null;
  if (parsed && !Number.isNaN(parsed.getTime())) return parsed.toISOString();
  return new Date().toISOString();
}

async function checkAndUpdateWarmStatus(prospectId, email, clientId) {
  try {
    if (!(await prospectExists(prospectId, clientId))) {
      await logSignalDroppedNoProspect({
        prospect_id: prospectId,
        client_id: clientId,
        source: 'riley',
        trigger: 'warm_status_update',
        payload: { email },
      });
      console.warn(`[Riley] Dropped warm status update for missing prospect_id=${prospectId}`);
      return;
    }

    const clickRes = await pool.query(`
      SELECT COUNT(*)::int AS clicks_all
      FROM touchpoints
      WHERE prospect_id = $1 AND client_id = $2 AND channel = 'email'
        AND action_type = 'email_clicked'
    `, [prospectId, clientId]);
    const clicks = Number(clickRes.rows[0]?.clicks_all || 0);
    const gate = await qualifyingOpenSignal(prospectId, clientId);

    if (clicks >= 1 || gate.qualifies) {
      const upd = await pool.query(
        `UPDATE prospects SET status = 'warm', updated_at = NOW()
         WHERE id = $1 AND client_id = $2 AND status IN ('cold', 'contacted') RETURNING id`,
        [prospectId, clientId]
      );
      if (upd.rows.length > 0) {
        const spreadFmt = gate.spread_minutes.toFixed(1);
        console.log(`[Riley] ${email} upgraded to warm — ${gate.total_opens} opens (spread ${spreadFmt}m) / ${clicks} clicks`);
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

      const payloadClientId = Number(payload.client_id || payload.clientId || payload.metadata?.client_id) || null;
      const messageId = brevoMessageId(payload);
      const prospectRes = await resolveProspectForBrevoEvent({
        email,
        clientId: payloadClientId,
        messageId,
        subject: payload.subject || null,
      });
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
        message_id: messageId,
        date:       payload.date     || null,
      });
      await pool.query(`
        INSERT INTO touchpoints
          (prospect_id, channel, action_type, content_summary, outcome, sentiment, external_ref, client_id)
        VALUES ($1, 'email', $2, $3, $4, 'neutral', $5, $6)
      `, [
        prospect.id, actionType,
        payload.subject || null,
        outcomeJson,
        messageId,
        prospect.client_id,
      ]);

      if (actionType === 'email_opened') {
        // Single open-trigger gate: ≥2 opens AND ≥5 min between first & most
        // recent open. Filters preview-pane bursts that fire in seconds while
        // catching genuine re-engagement when someone reopens minutes/hours later.
        const gate = await qualifyingOpenSignal(prospect.id, prospect.client_id);

        if (gate.qualifies) {
          await depositWarmSignalAction({
            prospect_id: prospect.id,
            client_id: prospect.client_id,
            trigger: 'qualifying_opens',
            signal_timestamp: signalIso(payload.date),
            subject: payload.subject || null,
            total_opens: gate.total_opens,
            email,
            company: prospect.company_name,
          });

          const company =
            prospect.company_name ||
            String(prospect.notes || '').split('—')[0].trim() ||
            `${prospect.first_name || ''} ${prospect.last_name || ''}`.trim() ||
            email;
          const spreadMinutes = Number(gate.spread_minutes.toFixed(1));
          const alertPayload = {
            prospect_id: prospect.id,
            email,
            company,
            open_count: gate.total_opens,
            total_opens: gate.total_opens,
            spread_minutes: spreadMinutes,
            subject: payload.subject || null,
            signal_timestamp: signalIso(payload.date),
            client_id: prospect.client_id,
          };

          const hotFlagRes = await pool.query(`
            INSERT INTO touchpoints
              (prospect_id, channel, action_type, content_summary, outcome, sentiment, client_id)
            SELECT $1, 'email', 'hot_flag', $2, $3, 'positive', $4
            WHERE NOT EXISTS (
              SELECT 1
              FROM touchpoints
              WHERE prospect_id = $1
                AND client_id = $4
                AND action_type = 'hot_flag'
                AND created_at >= NOW() - INTERVAL '24 hours'
            )
            RETURNING id
          `, [
            prospect.id,
            `Hot flag: ${gate.total_opens} email opens spread over ${spreadMinutes} min`,
            JSON.stringify(alertPayload),
            prospect.client_id,
          ]);

          if (hotFlagRes.rows.length) {
            await pool.query(
              `UPDATE prospects
               SET is_hot = true,
                   setter_visible = true,
                   setter_updated_at = NOW(),
                   updated_at = NOW()
               WHERE id = $1 AND client_id = $2`,
              [prospect.id, prospect.client_id]
            );

            await pool.query(`
              INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
              VALUES ('riley', 'hot_prospect_alert', $1, $2, 'pending', NOW(), $3)
            `, [
              prospect.id,
              JSON.stringify(alertPayload),
              prospect.client_id,
            ]);
          }
        }
      }

      if (['email_bounced', 'email_spam', 'email_unsubscribed'].includes(actionType)) {
        await pool.query(
          `UPDATE prospects
           SET do_not_contact = true,
               status = CASE WHEN status = 'closed' THEN status ELSE 'dead' END,
               updated_at = NOW()
           WHERE LOWER(email) = $1 AND client_id = $2`,
          [email, prospect.client_id]
        );
        console.log(`[Riley] ${email} marked do_not_contact + dead (${event})`);
      }

      if (['email_opened', 'email_clicked'].includes(actionType)) {
        await checkAndUpdateWarmStatus(prospect.id, email, prospect.client_id);
      }

      if (actionType === 'email_clicked') {
        await depositWarmSignalAction({
          prospect_id: prospect.id,
          client_id: prospect.client_id,
          trigger: 'click',
          signal_timestamp: signalIso(payload.date),
          subject: payload.subject || null,
          email,
          company: prospect.company_name,
        });
      }

      // Recompute the prospect's ICP score so this engagement signal (open /
      // click) or penalty (bounce / unsubscribe / spam) is reflected at once.
      if (['email_opened', 'email_clicked', 'email_bounced', 'email_unsubscribed', 'email_spam'].includes(actionType)) {
        await recalcICPAfterEmailEvent(prospect.id, prospect.client_id, actionType);
      }

      // Roll opens / clicks / bounces into the email_performance table.
      await recordPerformanceEvent(prospect, actionType, messageId, payload.subject || null);

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
          AND channel = 'phone'
          AND action_type = 'outbound'
          AND agent_id = 'cal'
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
