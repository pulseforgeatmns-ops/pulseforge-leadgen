'use strict';

const crypto = require('node:crypto');
const axios = require('axios');

function syntheticSubmission(values = {}) {
  if (values.is_test === true || values.is_demo === true || values.is_synthetic === true) return true;
  if (/^(test|demo|preview|seed|synthetic)$/i.test(String(values.submission_mode || ''))) return true;
  const domain = String(values.email || '').trim().toLowerCase().split('@').pop();
  if (/(^|\.)(example|test|invalid|localhost)$/.test(domain)
    || /(^|\.)example\.(com|net|org)$/.test(domain)) return true;
  const phone = String(values.phone_digits || values.phone || '').replace(/\D/g, '').replace(/^1(?=\d{10}$)/, '');
  // NANP reserves 555-0100 through 555-0199 for fictional use.
  return /^\d{3}55501\d{2}$/.test(phone);
}

function notificationBlockReason(values, env = process.env) {
  // node --test does not set NODE_ENV; railway run can inherit NODE_ENV=production.
  if (env.NODE_TEST_CONTEXT || env.JEST_WORKER_ID || env.VITEST || env.NODE_ENV === 'test') return 'test_runtime';
  if (env.NODE_ENV !== 'production') return 'non_production_runtime';
  if (env.RAILWAY_ENVIRONMENT_NAME && env.RAILWAY_ENVIRONMENT_NAME !== 'production') return 'non_production_environment';
  if (String(env.ANCHOR_WALKTHROUGH_NOTIFY_ENABLED || '').toLowerCase() === 'false') return 'disabled';
  if (syntheticSubmission(values)) return 'synthetic_submission';
  if (!env.BREVO_API_KEY) return 'missing_provider_key';
  return null;
}

function fingerprint(values) {
  const normalized = ['name', 'business_name', 'email', 'city', 'space_type']
    .map(key => String(values[key] || '').trim().toLowerCase().replace(/\s+/g, ' '));
  normalized.push(String(values.phone_digits || values.phone || '').replace(/\D/g, '').replace(/^1(?=\d{10}$)/, ''));
  return crypto.createHash('sha256').update(JSON.stringify(normalized)).digest('hex');
}

async function notifyWalkthrough(pool, values, actionId) {
  const blocked = notificationBlockReason(values);
  if (blocked) {
    console.info('[walkthrough] notification suppressed:', blocked);
    return { status: 'suppressed', reason: blocked };
  }
  if (!actionId) return { status: 'suppressed', reason: 'missing_action' };
  const claimId = crypto.randomUUID();
  // A persisted action and atomic 24-hour claim are required before any network send.
  // Keep failed/uncertain claims: a timeout can occur after the provider accepts mail.
  const claim = await pool.query(
    `INSERT INTO walkthrough_notification_claims (client_id, fingerprint, claim_id, action_id)
     SELECT 10, $1, $2, id::text FROM agent_actions
      WHERE id::text = $3 AND client_id = 10 AND action_type = 'walkthrough_request'
     ON CONFLICT (client_id, fingerprint) DO UPDATE
       SET claim_id = EXCLUDED.claim_id, action_id = EXCLUDED.action_id,
           claimed_at = NOW(), status = 'claimed', provider_message_id = NULL
       WHERE walkthrough_notification_claims.claimed_at < NOW() - INTERVAL '24 hours'
     RETURNING claim_id`,
    [fingerprint(values), claimId, String(actionId)]
  );
  if (!claim.rows.length) {
    console.info('[walkthrough] notification suppressed: duplicate_or_missing_action', String(actionId));
    return { status: 'suppressed', reason: 'duplicate_or_missing_action' };
  }
  const notifyTo = process.env.ANCHOR_WALKTHROUGH_NOTIFY_EMAIL || 'jacob@goanchorcleaning.com';
  const lines = [
    'New Facility Assessment request from goanchorcleaning.com', '',
    `Name: ${values.name}`, `Business: ${values.business_name}`, `Phone: ${values.phone}`,
    `Email: ${values.email}`, `City / town: ${values.city}`,
    `Type of space: ${values.space_type_label}`, `Action id: ${actionId}`,
  ];
  let result;
  try {
    result = await axios.post('https://api.brevo.com/v3/smtp/email', {
      sender: { name: 'Anchor Cleaning Site', email: notifyTo },
      to: [{ email: notifyTo }],
      subject: `Facility Assessment request — ${values.business_name}`,
      textContent: lines.join('\n'),
    }, { headers: { 'api-key': process.env.BREVO_API_KEY, 'Content-Type': 'application/json' }, timeout: 10000 });
  } catch (_) {
    await pool.query("UPDATE walkthrough_notification_claims SET status = 'failed_or_uncertain' WHERE claim_id = $1", [claimId]);
    // Do not log the Axios error/config, which contains the provider credential.
    throw new Error('Notification provider failed or timed out; claim retained to prevent duplicate delivery');
  }
  await pool.query("UPDATE walkthrough_notification_claims SET status = 'sent', provider_message_id = $2 WHERE claim_id = $1", [claimId, result.data?.messageId || null]);
  console.info('[walkthrough] notification sent:', String(actionId));
  return { status: 'sent' };
}

module.exports = { syntheticSubmission, notificationBlockReason, fingerprint, notifyWalkthrough };
