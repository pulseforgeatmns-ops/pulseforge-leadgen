/**
 * Public Anchor Cleaning walkthrough intake.
 * Unauthenticated POST. No sessionAuth. Mirrors the scorecard public funnel.
 */

const express = require('express');
const { validateWalkthroughPayload } = require('../lib/walkthroughValidate');
const { captureWalkthroughLead } = require('../lib/walkthroughCapture');
const { buildAttributionRecord } = require('../lib/walkthroughAttribution');
const { syntheticSubmission } = require('../lib/walkthroughNotification');
const { normalizeSubmissionId } = require('../lib/walkthroughSubmissionId');
const { isResidentialWalkthroughSpaceType } = require('../lib/walkthroughValidate');

const router = express.Router();

const SUCCESS_MESSAGE =
  "Thank you. We'll be in touch to arrange your Facility Assessment.";

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

function logWalkthroughAttempt(req, { outcome, formKind, spaceType, payloadKeys, detail }) {
  console.info('[walkthrough] intake', {
    at: new Date().toISOString(),
    route: 'POST /api/public/walkthrough',
    outcome,
    formKind: formKind || 'unknown',
    space_type: spaceType || null,
    payloadKeys: payloadKeys || [],
    ip: clientIp(req),
    ...(detail ? { detail } : {}),
  });
}

function payloadKeysFromBody(body = {}) {
  return Object.keys(body).filter(key => key !== 'company_website').sort();
}

function formKindFromSpaceType(spaceType) {
  if (!spaceType) return 'unknown';
  return isResidentialWalkthroughSpaceType(spaceType) ? 'residential' : 'commercial';
}

/**
 * POST /api/public/walkthrough
 * Body: name, business_name, phone, email, city, space_type
 */
router.post('/api/public/walkthrough', async (req, res) => {
  const payloadKeys = payloadKeysFromBody(req.body);
  try {
    if (String(req.body?.company_website || '').trim()) {
      logWalkthroughAttempt(req, { outcome: 'honeypot', payloadKeys });
      return res.status(204).end();
    }

    if (!allowRequest(clientIp(req))) {
      logWalkthroughAttempt(req, { outcome: 'rate_limited', payloadKeys });
      return res.status(429).json({ error: 'Please try again in a little while.' });
    }

    const validated = validateWalkthroughPayload(req.body);
    if (!validated.ok) {
      logWalkthroughAttempt(req, {
        outcome: 'validation_failed',
        payloadKeys,
        detail: Object.keys(validated.errors || {}),
      });
      return res.status(400).json({ error: 'Validation failed', details: validated.errors });
    }
    const formKind = formKindFromSpaceType(validated.values.space_type);
    // Keep demo contacts out of the CRM and downstream outreach as well as email.
    if (syntheticSubmission(validated.values)) {
      logWalkthroughAttempt(req, {
        outcome: 'synthetic_rejected',
        formKind,
        spaceType: validated.values.space_type,
        payloadKeys,
      });
      return res.status(422).json({ error: 'Please use real contact details to request a Facility Assessment.' });
    }

    const serverReferer = String(req.headers.referer || req.headers.referrer || '').trim() || null;
    const attributionRecord = validated.values.attribution
      ? buildAttributionRecord(validated.values.attribution, { serverReferer })
      : null;

    const stored = await captureWalkthroughLead(validated.values, attributionRecord);
    const submissionId = normalizeSubmissionId(stored);
    logWalkthroughAttempt(req, {
      outcome: 'accepted',
      formKind,
      spaceType: validated.values.space_type,
      payloadKeys,
      detail: { submission_id: submissionId },
    });
    return res.status(201).json({
      ok: true,
      submission_id: submissionId,
      message: SUCCESS_MESSAGE,
    });
  } catch (err) {
    logWalkthroughAttempt(req, { outcome: 'error', payloadKeys, detail: err.message });
    console.error('[walkthrough] submit failed:', err.message);
    return res.status(500).json({ error: 'Could not send your request. Please try again or call.' });
  }
});

module.exports = router;
module.exports._rateBuckets = rateBuckets;
module.exports.SUCCESS_MESSAGE = SUCCESS_MESSAGE;
