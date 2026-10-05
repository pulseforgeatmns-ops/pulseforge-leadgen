'use strict';

const crypto = require('node:crypto');
const axios = require('axios');
const { CREATED_BY, ACTION_TYPE } = require('./substralAssessmentIntake');

const DEFAULT_NOTIFY_EMAIL = 'hello@studiosubstral.com';

function syntheticSubmission(values = {}) {
  if (values.is_test === true || values.is_demo === true || values.is_synthetic === true) return true;
  if (/^(test|demo|preview|seed|synthetic|smoke)$/i.test(String(values.submission_mode || ''))) return true;
  const domain = String(values.email || '').trim().toLowerCase().split('@').pop();
  if (/(^|\.)(example|test|invalid|localhost)$/.test(domain)
    || /(^|\.)example\.(com|net|org)$/.test(domain)) return true;
  return false;
}

function notificationBlockReason(values, env = process.env) {
  if (env.NODE_TEST_CONTEXT || env.JEST_WORKER_ID || env.VITEST || env.NODE_ENV === 'test') return 'test_runtime';
  if (String(env.SUBSTRAL_ASSESSMENT_NOTIFY_ENABLED || '').toLowerCase() === 'false') return 'disabled';
  if (syntheticSubmission(values)) return 'synthetic_submission';
  if (!env.BREVO_API_KEY) return 'missing_provider_key';
  return null;
}

function productionDeliveryRequired(env = process.env) {
  if (notificationBlockReason({ email: 'delivery-check@customer.com' }, env) === 'test_runtime') return false;
  if (String(env.SUBSTRAL_ASSESSMENT_NOTIFY_ENABLED || '').toLowerCase() === 'false') return false;
  if (env.NODE_ENV !== 'production') return false;
  if (env.RAILWAY_ENVIRONMENT_NAME && env.RAILWAY_ENVIRONMENT_NAME !== 'production') return false;
  return true;
}

function resolveRecipient(env = process.env) {
  const configured = String(env.SUBSTRAL_ASSESSMENT_NOTIFY_EMAIL || '').trim().toLowerCase();
  return configured || DEFAULT_NOTIFY_EMAIL;
}

function resolveSender(env = process.env) {
  return {
    name: String(env.SUBSTRAL_ASSESSMENT_NOTIFY_SENDER_NAME || 'Studio Substral').trim() || 'Studio Substral',
    email: String(
      env.SUBSTRAL_ASSESSMENT_NOTIFY_SENDER
      || env.BREVO_SENDER_EMAIL
      || env.FROM_EMAIL
      || 'hello@gopulseforge.com'
    ).trim().toLowerCase(),
  };
}

function fingerprint(values) {
  const context = values.context
    ? String(values.context).trim().toLowerCase().replace(/\s+/g, ' ')
    : null;
  return crypto.createHash('sha256').update(JSON.stringify([
    String(values.domain || '').trim().toLowerCase(),
    String(values.email || '').trim().toLowerCase(),
    context,
  ])).digest('hex');
}

function buildNotificationContent(values, actionId) {
  const subject = `Website assessment request — ${values.domain}`;
  const lines = [
    'New website assessment request from studiosubstral.com',
    '',
    `Domain: ${values.domain}`,
    `Reply to: ${values.email}`,
    values.context ? `Decision context: ${values.context}` : null,
    `Action id: ${actionId}`,
  ].filter(Boolean);
  return { subject, textContent: lines.join('\n') };
}

/**
 * @returns {Promise<{ status: 'sent'|'suppressed', reason?: string, recipient?: string, sender?: object, subject?: string, providerMessageId?: string|null }>}
 */
async function notifyAssessmentRequest(pool, values, actionId, clientId, env = process.env) {
  const blocked = notificationBlockReason(values, env);
  if (blocked) {
    console.info('[substral-assessment] notification suppressed:', blocked);
    return { status: 'suppressed', reason: blocked };
  }
  if (!actionId) return { status: 'suppressed', reason: 'missing_action' };

  const recipient = resolveRecipient(env);
  const sender = resolveSender(env);
  const { subject, textContent } = buildNotificationContent(values, actionId);
  const claimId = crypto.randomUUID();
  const fp = fingerprint(values);
  const actionIdText = String(actionId);

  let canonicalActionId;
  try {
    const action = await pool.query(
      `SELECT id::text AS id FROM agent_actions
        WHERE id::text = $1 AND client_id = $2
          AND created_by = $3 AND action_type = $4`,
      [actionIdText, clientId, CREATED_BY, ACTION_TYPE]
    );
    if (!action.rows.length) {
      console.info('[substral-assessment] notification suppressed: missing_action', actionIdText);
      return { status: 'suppressed', reason: 'missing_action', recipient, sender, subject };
    }
    canonicalActionId = action.rows[0].id;
  } catch (err) {
    if (/agent_actions/.test(String(err.message))) throw err;
    throw err;
  }

  const readClaimStatus = async () => {
    const result = await pool.query(
      `SELECT status, provider_message_id FROM substral_assessment_notification_claims
        WHERE client_id = $1 AND (fingerprint = $2 OR action_id = $3)
        ORDER BY CASE status WHEN 'sent' THEN 0 WHEN 'claimed' THEN 1 ELSE 2 END
        LIMIT 1`,
      [clientId, fp, canonicalActionId]
    );
    return result.rows[0] || null;
  };

  let claim;
  try {
    const existing = await readClaimStatus();
    if (existing?.status === 'sent') {
      console.info('[substral-assessment] notification suppressed: notification_already_sent', canonicalActionId);
      return {
        status: 'suppressed',
        reason: 'notification_already_sent',
        recipient,
        sender,
        subject,
        providerMessageId: existing.provider_message_id || null,
      };
    }

    claim = await pool.query(
      `INSERT INTO substral_assessment_notification_claims (client_id, fingerprint, claim_id, action_id)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (client_id, fingerprint) DO UPDATE
         SET claim_id = EXCLUDED.claim_id, action_id = EXCLUDED.action_id,
             claimed_at = NOW(), status = 'claimed', provider_message_id = NULL
         WHERE substral_assessment_notification_claims.status = 'failed_or_uncertain'
            OR substral_assessment_notification_claims.claimed_at < NOW() - INTERVAL '24 hours'
            OR (
              substral_assessment_notification_claims.status = 'claimed'
              AND substral_assessment_notification_claims.provider_message_id IS NULL
              AND substral_assessment_notification_claims.claimed_at < NOW() - INTERVAL '2 minutes'
            )
       RETURNING claim_id, status`,
      [clientId, fp, claimId, canonicalActionId]
    );
  } catch (err) {
    if (/substral_assessment_notification_claims/.test(String(err.message))) {
      const required = productionDeliveryRequired(env);
      const error = new Error(required
        ? 'Notification schema unavailable; apply substral assessment notification migration'
        : 'Notification schema unavailable');
      error.code = 'NOTIFICATION_SCHEMA_MISSING';
      throw error;
    }
    throw err;
  }

  if (!claim.rows.length) {
    const existing = await readClaimStatus();
    if (existing?.status === 'sent') {
      console.info('[substral-assessment] notification suppressed: notification_already_sent', canonicalActionId);
      return {
        status: 'suppressed',
        reason: 'notification_already_sent',
        recipient,
        sender,
        subject,
        providerMessageId: existing.provider_message_id || null,
      };
    }
    console.info('[substral-assessment] notification suppressed: notification_in_progress', canonicalActionId);
    return { status: 'suppressed', reason: 'notification_in_progress', recipient, sender, subject };
  }

  let result;
  try {
    result = await axios.post('https://api.brevo.com/v3/smtp/email', {
      sender: { name: sender.name, email: sender.email },
      to: [{ email: recipient }],
      replyTo: { email: values.email, name: values.email },
      subject,
      textContent,
    }, { headers: { 'api-key': env.BREVO_API_KEY, 'Content-Type': 'application/json' }, timeout: 10000 });
  } catch (err) {
    await pool.query(
      "UPDATE substral_assessment_notification_claims SET status = 'failed_or_uncertain' WHERE claim_id = $1",
      [claimId]
    );
    const error = new Error('Notification provider failed or timed out; claim retained to prevent duplicate delivery');
    error.code = 'NOTIFICATION_PROVIDER_FAILED';
    throw error;
  }

  const providerMessageId = result.data?.messageId || null;
  await pool.query(
    "UPDATE substral_assessment_notification_claims SET status = 'sent', provider_message_id = $2 WHERE claim_id = $1",
    [claimId, providerMessageId]
  );
  console.info('[substral-assessment] notification sent', {
    actionId: canonicalActionId,
    recipient,
    sender: sender.email,
    subject,
    providerMessageId,
  });
  return {
    status: 'sent',
    recipient,
    sender,
    subject,
    providerMessageId,
  };
}

module.exports = {
  DEFAULT_NOTIFY_EMAIL,
  syntheticSubmission,
  notificationBlockReason,
  productionDeliveryRequired,
  resolveRecipient,
  resolveSender,
  fingerprint,
  buildNotificationContent,
  notifyAssessmentRequest,
};
