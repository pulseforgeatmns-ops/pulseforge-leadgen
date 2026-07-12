const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const router = express.Router();
const pool = require('../db');
const {
  depositWarmSignalAction,
  logSignalDroppedNoProspect,
  prospectExists,
  qualifyingOpenSignal,
  recalcICPAfterEmailEvent,
} = require('../rileyAgent');
const {
  insertBrevoEvent,
  internalEventType,
  recipientEmail,
} = require('../utils/brevoEvents');
const { OPEN_SOURCE, isOpenOrClickEventType } = require('../utils/openSignalGate');
const { ensureMiraSchema } = require('../utils/miraSchema');
const {
  VALID_MIRA_CATEGORIES,
  correctMiraCapture,
  sendMiraTelegramMessage,
} = require('../utils/miraCorrections');
const {
  isWithinAnchorWindow,
  getAnchorForToday,
  parseAnchorSetIntent,
  setCurrentAnchor,
  clearCurrentAnchor,
  buildAnchorSetConfirmation,
  buildAnchorClearConfirmation,
  DEFAULT_ANCHOR_CLIENT_ID,
  parseAnchorReply,
  insertAnchor,
} = require('../utils/miraAnchor');
const { handleWarmTelegramCallback } = require('../warmRoutingAgent');
const { setSetterVisibility } = require('../utils/setterVisibility');
const { resolveVerticalTier } = require('../utils/verticalTiers');

const miraSchemaReady = ensureMiraSchema().catch(err => {
  console.error('[mira] schema error:', err.message);
});

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

function isTelegramControlCommand(message = {}) {
  return /^\/[a-z][a-z0-9_]*(?:@[a-z0-9_]+)?(?:\s|$)/i.test(String(message.text || '').trim());
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

function resolveMiraAnchorClient() {
  return {
    clientId: DEFAULT_ANCHOR_CLIENT_ID,
    assumed: true,
  };
}

async function handleMiraAnchorSetIntent(message, intent) {
  const { clientId, assumed } = resolveMiraAnchorClient(message);

  if (intent.action === 'clear') {
    await clearCurrentAnchor(clientId);
    await sendMiraTelegramMessage(buildAnchorClearConfirmation({ clientId, assumed }));
    return;
  }

  if (!intent.anchorText) {
    await sendMiraTelegramMessage('Use /anchor <text> or /anchor clear.');
    return;
  }

  const row = await setCurrentAnchor({
    client_id: clientId,
    primary_anchor: intent.anchorText,
    secondary_anchors: [],
    completion_notes: null,
  });
  await sendMiraTelegramMessage(buildAnchorSetConfirmation({
    clientId,
    assumed,
    anchorText: row.primary_anchor,
  }));
}

async function handleMiraCallback(callbackQuery) {
  const data = String(callbackQuery.data || '');

  if (/^(working|today|tomorrow):[0-9a-f-]+$/i.test(data)) {
    await handleWarmTelegramCallback(callbackQuery);
    return;
  }

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
      const callbackData = String(callbackQuery.data || '');

      if (/^(working|today|tomorrow):/.test(callbackData)) {
        handleWarmTelegramCallback(callbackQuery).catch(err => {
          console.error('[warm_routing] callback error:', err.response?.data?.description || err.message);
          answerMiraCallback(callbackQuery.id, 'Could not process warm action.').catch(() => {});
        });
        return;
      }

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

    const anchorSetIntent = parseAnchorSetIntent(message.text || '');
    if (anchorSetIntent?.matched) {
      res.status(200).json({ ok: true });
      handleMiraAnchorSetIntent(message, anchorSetIntent).catch(err => {
        console.error('[mira] anchor set error:', err.response?.data?.description || err.message);
        sendMiraTelegramMessage(`Could not set anchor: ${err.message}`).catch(() => {});
      });
      return;
    }

    // Telegram slash commands control the bot; they are not Mira captures.
    // /correct is handled above, while commands such as /start and /help are
    // acknowledged here without entering the classifier/router pipeline.
    if (isTelegramControlCommand(message)) {
      console.log(`[mira] ignored telegram control command=${String(message.text || '').trim().split(/\s+/, 1)[0]}`);
      return res.status(200).json({ ok: true });
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
    payload.uuid ||
    null;
}

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
      FROM email_events ee
      WHERE ee.prospect_id = $1
        AND ee.client_id = $2
        AND ee.event_type IN ('clicked', 'click')
        AND (
          EXISTS (
            SELECT 1
            FROM email_events sent
            WHERE sent.client_id = ee.client_id
              AND sent.prospect_id = ee.prospect_id
              AND sent.event_type IN ('sent', 'delivered')
              AND (
                (ee.brevo_message_id IS NOT NULL AND sent.brevo_message_id = ee.brevo_message_id)
                OR (
                  ee.brevo_message_id IS NULL
                  AND LOWER(sent.recipient_email) = LOWER(ee.recipient_email)
                  AND sent.subject_line IS NOT DISTINCT FROM ee.subject_line
                  AND sent.event_at <= ee.event_at
                )
              )
          )
          OR EXISTS (
            SELECT 1
            FROM agent_log al
            WHERE al.client_id = ee.client_id
              AND al.prospect_id = ee.prospect_id
              AND al.agent_name = 'emmett'
              AND al.action = 'email_sent'
              AND (
                (ee.brevo_message_id IS NOT NULL AND al.payload->>'message_id' = ee.brevo_message_id)
                OR (
                  ee.brevo_message_id IS NULL
                  AND al.payload->>'subject' IS NOT DISTINCT FROM ee.subject_line
                  AND al.ran_at <= ee.event_at
                )
              )
          )
        )
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

function getBrevoSignature(req) {
  return req.get('x-brevo-signature') ||
    req.get('x-sib-signature') ||
    req.get('x-sendinblue-signature') ||
    req.get('x-mailin-signature') ||
    '';
}

function safeCompare(a, b) {
  const left = Buffer.from(String(a || ''));
  const right = Buffer.from(String(b || ''));
  return left.length === right.length && crypto.timingSafeEqual(left, right);
}

function verifyBrevoSignature(req) {
  const secret = process.env.BREVO_WEBHOOK_SECRET;
  const signature = getBrevoSignature(req);
  if (!secret || !signature) return false;

  const rawBody = Buffer.isBuffer(req.rawBody)
    ? req.rawBody
    : Buffer.from(JSON.stringify(req.body || {}));
  const hex = crypto.createHmac('sha256', secret).update(rawBody).digest('hex');
  const base64 = crypto.createHmac('sha256', secret).update(rawBody).digest('base64');
  const candidates = String(signature)
    .split(',')
    .map(part => part.trim().split('=').pop())
    .filter(Boolean);

  return candidates.some(candidate =>
    safeCompare(candidate, hex) ||
    safeCompare(candidate, `sha256=${hex}`) ||
    safeCompare(candidate, base64)
  );
}

function actionTypeForEmailEvent(eventType) {
  return {
    opened: 'email_opened',
    clicked: 'email_clicked',
    soft_bounce: 'email_soft_bounce',
    hard_bounce: 'email_bounced',
    blocked: 'email_bounced',
    spam: 'email_spam',
    unsubscribed: 'email_unsubscribed',
    replied: 'email_reply',
  }[eventType] || null;
}

async function loadProspectForBrevoSideEffects(prospectId, clientId) {
  const res = await pool.query(`
    SELECT
      p.id, p.status, p.client_id, p.first_name, p.last_name, p.email,
      p.vertical, p.icp_score, p.notes,
      client.vertical_tiers,
      c.name AS company_name
    FROM prospects p
    JOIN clients client ON client.id = p.client_id
    LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
    WHERE p.id = $1
      AND p.client_id = $2
    LIMIT 1
  `, [prospectId, clientId]);
  return res.rows[0] || null;
}

async function processBrevoEventSideEffects(result, payload) {
  if (!result.inserted || !result.prospect_id || !result.client_id) return;
  const actionType = actionTypeForEmailEvent(result.event_type);
  if (!actionType) return;
  if (isOpenOrClickEventType(result.event_type) && result.has_corresponding_send === false) {
    console.warn(`[Brevo] Suppressed ${result.event_type} side effects for zero-send prospect_id=${result.prospect_id} client_id=${result.client_id}`);
    return;
  }

  const prospect = await loadProspectForBrevoSideEffects(result.prospect_id, result.client_id);
  if (!prospect) return;
  const tier = resolveVerticalTier(prospect.vertical, { vertical_tiers: prospect.vertical_tiers });
  const warmTierEligible = tier.warm_eligible;

  const email = result.recipient_email;
  const messageId = brevoMessageId(payload);
  const outcomeJson = JSON.stringify({
    event: payload.event || null,
    subject: payload.subject || null,
    link: payload.link || null,
    brevo_id: payload.id || null,
    message_id: messageId,
    date: payload.date || null,
    open_source: result.open_source || null,
    open_source_reason: result.open_source_reason || null,
  });
  const effectiveActionType = actionType === 'email_opened' && result.open_source !== OPEN_SOURCE.HUMAN
    ? `email_opened_${result.open_source || OPEN_SOURCE.UNKNOWN}`
    : actionType;

  await pool.query(`
    INSERT INTO touchpoints
      (prospect_id, channel, action_type, content_summary, outcome, sentiment, external_ref, client_id)
    VALUES ($1, 'email', $2, $3, $4, 'neutral', $5, $6)
  `, [
    prospect.id,
    effectiveActionType,
    payload.subject || null,
    outcomeJson,
    messageId,
    prospect.client_id,
  ]);

  if (effectiveActionType === 'email_opened' && warmTierEligible) {
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
        String(prospect.notes || '').split('\u2014')[0].trim() ||
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
               setter_updated_at = NOW(),
               updated_at = NOW()
           WHERE id = $1 AND client_id = $2`,
          [prospect.id, prospect.client_id]
        );
        await setSetterVisibility(pool, prospect.id, {
          reason: 'engagement',
          clientId: prospect.client_id,
        });

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

  if (['email_bounced', 'email_spam', 'email_unsubscribed'].includes(effectiveActionType)) {
    await pool.query(
      `UPDATE prospects
       SET do_not_contact = true,
           status = CASE WHEN status = 'closed' THEN status ELSE 'dead' END,
           updated_at = NOW()
       WHERE LOWER(email) = $1 AND client_id = $2`,
      [email, prospect.client_id]
    );
    console.log(`[Brevo] ${email} marked do_not_contact and dead (${result.event_type})`);
  }

  if (['email_opened', 'email_clicked'].includes(effectiveActionType) && warmTierEligible) {
    await checkAndUpdateWarmStatus(prospect.id, email, prospect.client_id);
  }

  if (effectiveActionType === 'email_clicked' && warmTierEligible) {
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

  if (['email_opened', 'email_clicked'].includes(effectiveActionType) && !warmTierEligible) {
    await pool.query(
      `INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
       VALUES ('riley', 'warm_signal_tier_blocked', $1, $2::jsonb, 'skipped', NOW(), $3)`,
      [prospect.id, JSON.stringify({ trigger: effectiveActionType, raw_vertical: prospect.vertical || null, normalized_vertical: tier.vertical, tier: tier.tier }), prospect.client_id]
    );
  }

  if (['email_opened', 'email_clicked', 'email_bounced', 'email_unsubscribed', 'email_spam'].includes(effectiveActionType)) {
    await recalcICPAfterEmailEvent(prospect.id, prospect.client_id, effectiveActionType);
  }
}

function acceptBrevoWebhook(req, res) {
  const payload = req.body || {};
  const eventType = internalEventType(payload);
  const email = recipientEmail(payload);
  res.status(200).json({ ok: true });

  setImmediate(async () => {
    try {
      const result = await insertBrevoEvent(payload);
      if (result.skipped) {
        console.warn(`[Brevo] Skipped webhook event: ${result.reason || 'unknown'}`);
        return;
      }
      await processBrevoEventSideEffects(result, payload);
      console.log(`[Brevo] Received ${eventType || result.event_type} for ${email || result.recipient_email}`);
    } catch (err) {
      console.error('[Brevo] Webhook persistence error:', err.message);
    }
  });
}

function handleSignedBrevoWebhook(req, res) {
  if (!verifyBrevoSignature(req)) {
    return res.status(401).json({ error: 'Invalid Brevo webhook signature' });
  }

  return acceptBrevoWebhook(req, res);
}

router.post('/api/webhooks/brevo', handleSignedBrevoWebhook);
router.post('/webhooks/brevo', acceptBrevoWebhook);

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
