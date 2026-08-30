'use strict';

/**
 * Brevo email transport — provider boundary only.
 * Accepts execution-ready send commands; no mission or campaign logic.
 */

const axios = require('axios');

/**
 * @param {object} input
 * @param {string} input.toEmail
 * @param {string} [input.toName]
 * @param {string} input.subject
 * @param {string} input.body
 * @param {string[]} [input.tags]
 * @param {string} [input.idempotencyKey]
 * @param {object} [input.sender] - { name, email }
 * @param {string} [input.apiKey]
 * @returns {Promise<{ success: boolean, messageId?: string|null, providerMessageId?: string|null, error?: string, providerErrorCode?: string, providerErrorMessage?: string, brevoResponse?: object }>}
 */
async function sendEmail(input = {}) {
  const toEmail = String(input.toEmail || '').trim();
  const toName = input.toName != null ? String(input.toName) : '';
  const subject = String(input.subject || '');
  const body = String(input.body || '');
  const tags = Array.isArray(input.tags) ? input.tags : [];
  const apiKey = input.apiKey || process.env.BREVO_API_KEY;

  // Env fallbacks remain for unrelated internal/ops usage only.
  // Canonical AMO execution must pass requireExplicitSender + explicit sender.
  if (input.requireExplicitSender) {
    const email = input.sender && String(input.sender.email || '').trim();
    const name = input.sender && String(input.sender.name || '').trim();
    if (!email || !name) {
      return {
        success: false,
        providerErrorCode: 'missing_explicit_sender',
        providerErrorMessage: 'Canonical AMO sends require an explicit sender { email, name }.',
        error: 'Canonical AMO sends require an explicit sender { email, name }.',
      };
    }
  }

  const sender = input.sender || {
    name: process.env.BREVO_SENDER_NAME || process.env.FROM_NAME || 'Pulseforge',
    email: process.env.BREVO_SENDER_EMAIL || process.env.FROM_EMAIL || 'hello@gopulseforge.com',
  };

  if (!toEmail) {
    return {
      success: false,
      providerErrorCode: 'missing_recipient',
      providerErrorMessage: 'toEmail is required',
      error: 'toEmail is required',
    };
  }
  if (!apiKey) {
    return {
      success: false,
      providerErrorCode: 'missing_api_key',
      providerErrorMessage: 'BREVO_API_KEY not set',
      error: 'BREVO_API_KEY not set',
    };
  }

  const payload = {
    sender: { name: sender.name, email: sender.email },
    to: [{ email: toEmail, name: toName }],
    subject,
    htmlContent: '<html><body style="font-family:Georgia,serif;font-size:16px;line-height:1.6;color:#1a1a1a;max-width:560px;margin:0 auto;padding:20px;">'
      + body.replace(/\n/g, '<br>') + '</body></html>',
    textContent: body,
  };
  if (tags.length) payload.tags = tags.map(String);
  if (input.idempotencyKey) payload.headers = { 'Idempotency-Key': String(input.idempotencyKey) };

  try {
    const res = await axios.post('https://api.brevo.com/v3/smtp/email', payload, {
      headers: { 'api-key': apiKey, 'Content-Type': 'application/json' },
      timeout: 15000,
    });

    const messageId =
      res.data?.messageId
      || res.data?.messageID
      || res.data?.message_id
      || res.data?.messageIds?.[0]
      || res.headers?.['message-id']
      || res.headers?.['x-message-id']
      || null;

    return {
      success: true,
      messageId,
      providerMessageId: messageId,
      brevoResponse: res.data || null,
    };
  } catch (err) {
    const errorDetail = err.response?.data || err.message;
    const providerErrorMessage = typeof errorDetail === 'string'
      ? errorDetail
      : JSON.stringify(errorDetail);
    const providerErrorCode = err.response?.status
      ? `brevo_http_${err.response.status}`
      : 'brevo_request_failed';
    return {
      success: false,
      error: providerErrorMessage,
      providerErrorCode,
      providerErrorMessage,
    };
  }
}

module.exports = {
  sendEmail,
};
