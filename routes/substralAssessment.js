/**
 * Public Studio Substral assessment intake.
 * Unauthenticated POST. Mirrors routes/walkthrough.js.
 *
 * The response never contains a finding about the submitted domain — only
 * confirmation that the request was accepted. See lib/substralAssessmentIntake.js
 */

const express = require('express');
const pool = require('../db');
const {
  validateAssessmentPayload,
  captureAssessmentRequest,
} = require('../lib/substralAssessmentIntake');

const router = express.Router();

const rateBuckets = new Map();
const RATE_WINDOW_MS = 60 * 60 * 1000;
const RATE_MAX = 6;

/**
 * Copy for each rejection reason. Deliberately plain: the instrument tells the
 * visitor what it could not do, and never implies anything about the site.
 */
const REASON_MESSAGES = Object.freeze({
  empty: 'Enter the domain you want assessed.',
  no_tld: 'That needs to be a full domain — example.com, not example.',
  malformed: 'That does not parse as a domain. Check for a typo.',
  not_public:
    'That address is not reachable from the public internet, so there is nothing to measure.',
  not_a_subject:
    'That is a search engine, directory or social profile. Enter the business’s own domain.',
  own_domain: 'That one we already know about. Enter the domain you want assessed.',
  email: 'A valid email address is required — the assessment is sent, not displayed.',
});

function clientIp(req) {
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return forwarded || req.ip || req.socket?.remoteAddress || 'unknown';
}

function allowRequest(ip) {
  const now = Date.now();
  const prior = (rateBuckets.get(ip) || []).filter((ts) => now - ts < RATE_WINDOW_MS);
  if (prior.length >= RATE_MAX) {
    rateBuckets.set(ip, prior);
    return false;
  }
  prior.push(now);
  rateBuckets.set(ip, prior);
  return true;
}

function successMessage(domain, email) {
  return (
    `${domain} is queued. We run the measurement pass, a person reviews the findings, ` +
    `and the assessment goes to ${email}. If the evidence does not support a ` +
    'recommendation, the report will say so.'
  );
}

/**
 * POST /api/public/website-assessment
 * Body: domain, email, context (optional), referer (optional)
 */
router.post('/api/public/website-assessment', async (req, res) => {
  try {
    // Honeypot: silently accept and discard.
    if (String(req.body?.company_website || '').trim()) {
      return res.status(204).end();
    }

    if (!allowRequest(clientIp(req))) {
      return res.status(429).json({
        error: 'That is more requests than we can take from one place right now.',
      });
    }

    const validated = validateAssessmentPayload(req.body);
    if (!validated.ok) {
      const code = validated.errors.domain || validated.errors.email;
      return res.status(400).json({
        error: REASON_MESSAGES[code] || REASON_MESSAGES.malformed,
        error_code: code,
        details: validated.errors,
      });
    }

    const stored = await captureAssessmentRequest(pool, validated.values);

    return res.status(201).json({
      ok: true,
      request_id: stored.id,
      domain: stored.domain,
      message: successMessage(stored.domain, validated.values.email),
    });
  } catch (err) {
    console.error('[substral-assessment] submit failed:', err.message);
    return res.status(500).json({
      error: 'We could not queue that request. Try again, or email hello@studiosubstral.com.',
    });
  }
});

module.exports = router;
module.exports._rateBuckets = rateBuckets;
module.exports.REASON_MESSAGES = REASON_MESSAGES;
module.exports.successMessage = successMessage;
