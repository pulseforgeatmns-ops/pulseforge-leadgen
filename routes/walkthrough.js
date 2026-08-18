/**
 * Public Anchor Cleaning walkthrough intake.
 * Unauthenticated POST. No sessionAuth. Mirrors the scorecard public funnel.
 */

const express = require('express');
const { validateWalkthroughPayload } = require('../lib/walkthroughValidate');
const { captureWalkthroughLead } = require('../lib/walkthroughCapture');

const router = express.Router();

const SUCCESS_MESSAGE =
  "Thanks. I'll reach out to set up a quick facilities assessment and give you a clear monthly quote.";

const rateBuckets = new Map();
const RATE_WINDOW_MS = 60 * 60 * 1000;
const RATE_MAX = 8;

function clientIp(req) {
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return forwarded || req.ip || req.socket?.remoteAddress || 'unknown';
}

function allowRequest(ip) {
  const now = Date.now();
  const prior = (rateBuckets.get(ip) || []).filter(ts => now - ts < RATE_WINDOW_MS);
  if (prior.length >= RATE_MAX) {
    rateBuckets.set(ip, prior);
    return false;
  }
  prior.push(now);
  rateBuckets.set(ip, prior);
  return true;
}

/**
 * POST /api/public/walkthrough
 * Body: name, business_name, phone, email, city, space_type
 */
router.post('/api/public/walkthrough', async (req, res) => {
  try {
    if (String(req.body?.company_website || '').trim()) {
      return res.status(204).end();
    }

    if (!allowRequest(clientIp(req))) {
      return res.status(429).json({ error: 'Please try again in a little while.' });
    }

    const validated = validateWalkthroughPayload(req.body);
    if (!validated.ok) {
      return res.status(400).json({ error: 'Validation failed', details: validated.errors });
    }

    const stored = await captureWalkthroughLead(validated.values);
    return res.status(201).json({
      ok: true,
      submission_id: stored.id,
      message: SUCCESS_MESSAGE,
    });
  } catch (err) {
    console.error('[walkthrough] submit failed:', err.message);
    return res.status(500).json({ error: 'Could not send your request. Please try again or call.' });
  }
});

module.exports = router;
module.exports._rateBuckets = rateBuckets;
module.exports.SUCCESS_MESSAGE = SUCCESS_MESSAGE;
