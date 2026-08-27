'use strict';

/**
 * Brevo email transport — provider boundary only.
 * No mission, Scout, Max, Paige, or Emmett cognition.
 */

const axios = require('axios');

/**
 * @param {object} input
 * @param {string} input.toEmail
 * @param {string} [input.toName]
 * @param {string} input.subject
 * @param {string} input.body
 * @param {string[]} [input.tags]
 * @param {object} [input.sender] - { name, email }
 * @param {string} [input.idempotencyKey] - forwarded as Brevo header when supported
 * @param {string} [input.apiKey]
 * @returns {Promise<object>}
 */
async function sendEmail(input = {}) {
  const toEmail = String(input.toEmail || '').trim();
  const subject = String(input.subject || '').trim();
  const body = String(input.body || '');
  const apiKey = input.apiKey || process.env.BREVO_API_KEY;

  if (!toEmail) {
    return {
      success: false,
      status: 'failed',
      provider: 'brevo',
      providerErrorCode: 'missing_recipient',
      providerErrorMessage: 'toEmail is required',
    };
  }
  if (!apiKey) {
    return {
      success: false,
      status: 'failed',
      provider: 'brevo',
      providerErrorCode: 'missing_api_key',
      providerErrorMessage: 'BREVO_API_KEY not set',
    };
  }

  const sender = input.sender || {};
  const payload = {
    sender: {
      name: sender.name || 'Pulseforge',
      email: sender.email || sender.fromEmail || '',
    },
    to: [{ email: toEmail, name: input.toName || toEmail }],
    subject,
    htmlContent:
      '<html><body style="font-family:Georgia,serif;font-size:16px;line-height:1.6;color:#1a1a1a;max-width:560px;margin:0 auto;padding:20px;">'
      + body.replace(/\n/g, '<br>')
      + '</body></html>',
    textContent: body,
  };
  if (Array.isArray(input.tags) && input.tags.length) {
    payload.tags = input.tags.map(String);
  }

  const headers = {
    'api-key': apiKey,
    'Content-Type': 'application/json',
  };
  if (input.idempotencyKey) {
    headers['Idempotency-Key'] = String(input.idempotencyKey);
  }

  try {
    const res = await axios.post('https://api.brevo.com/v3/smtp/email', payload, {
      headers,
      timeout: 15000,
    });
    const providerMessageId =
      res.data?.messageId
      || res.data?.messageID
      || res.data?.message_id
      || res.data?.messageIds?.[0]
      || res.headers?.['message-id']
      || res.headers?.['x-message-id']
      || null;

    return {
      success: true,
      status: 'sent',
      provider: 'brevo',
      providerMessageId,
      brevoResponse: res.data || null,
      attemptedAt: new Date().toISOString(),
      sentAt: new Date().toISOString(),
    };
  } catch (err) {
    const detail = err.response?.data || err.message;
    const providerErrorMessage = typeof detail === 'string' ? detail : JSON.stringify(detail);
    return {
      success: false,
      status: 'failed',
      provider: 'brevo',
      providerErrorCode: err.response?.status ? String(err.response.status) : 'provider_error',
      providerErrorMessage,
      attemptedAt: new Date().toISOString(),
    };
  }
}

module.exports = {
  sendEmail,
};
