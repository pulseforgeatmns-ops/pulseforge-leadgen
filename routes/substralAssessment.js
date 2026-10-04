/** Public intake: a persisted request for human review, never an audit. */
const express = require('express');
const { createHash } = require('node:crypto');
const pool = require('../db');
const { validateAssessmentPayload, captureAssessmentRequest } = require('../lib/substralAssessmentIntake');
const {
  notifyAssessmentRequest,
  productionDeliveryRequired,
  notificationBlockReason,
} = require('../lib/substralAssessmentNotification');
const ENDPOINT = '/api/public/website-assessment';
const ALLOWED_ORIGINS = ['https://studiosubstral.com', 'https://www.studiosubstral.com'];
const REASON_MESSAGES = Object.freeze({
  empty: 'Enter the domain you want assessed.',
  no_tld: 'That needs to be a full domain — example.com, not example.',
  malformed: 'That does not parse as a domain. Check for a typo.',
  not_public: 'Enter a public website address that our team can review.',
  not_a_subject: 'That is a search engine, directory or social profile. Enter the business’s own domain.',
  own_domain: 'That one we already know about. Enter the domain you want assessed.',
  email: 'A valid email address is required — the assessment is sent, not displayed.',
  request_key: 'Reload this page and try submitting the request again.',
});
function successMessage(domain, email) {
  return `Your request for ${domain} has been received. A person will review the site and send a written assessment to ${email}. This is a request for human review, not an instant audit.`;
}
const escapeHtml = (value) => String(value).replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
function reply(req, res, code, body) {
  // Progressive enhancement: native POSTs never put personal details in a URL.
  if (req.is('application/x-www-form-urlencoded') && req.accepts('html')) {
    const title = body.ok ? 'Request received' : 'Request not confirmed';
    return res.status(code).type('html').send(`<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>${title} — Studio Substral</title><style>body{margin:0;background:#11110f;color:#f0ede5;font:1.2rem/1.6 system-ui}main{max-width:42rem;margin:12vh auto;padding:2rem}a{color:#7fa890}</style><main><p>Studio Substral / Human reviewed</p><h1>${title}</h1><p>${escapeHtml(body.message || body.error)}</p>${body.ok ? `<p>Request reference: ${escapeHtml(body.request_id)}</p>` : '<p>Your browser’s Back button returns to the form so you can try again.</p>'}<a href="https://studiosubstral.com/#assessment">Return to Studio Substral</a></main></html>`);
  }
  return res.status(code).json(body);
}
function createAssessmentRouter({ db = pool, allowedOrigins = ALLOWED_ORIGINS, now = Date.now } = {}) {
  const router = express.Router();
  const buckets = new Map();
  const windowMs = 60 * 60 * 1000;
  const allow = (key, max) => {
    const time = now();
    for (const [id, entry] of buckets) if (time >= entry.until) buckets.delete(id);
    if (!buckets.has(key) && buckets.size >= 10000) return false;
    const entry = buckets.get(key) || { count: 0, until: time + windowMs };
    buckets.set(key, entry);
    entry.count += 1;
    return entry.count <= max;
  };
  router.use(ENDPOINT, (req, res, next) => {
    res.set({ 'Cache-Control': 'no-store', 'X-Robots-Tag': 'noindex, nofollow', 'X-Content-Type-Options': 'nosniff' });
    const origin = req.get('origin');
    if (origin && !allowedOrigins.includes(origin)) {
      return reply(req, res, 403, { error: 'Submit the form from studiosubstral.com.' });
    }
    if (origin) {
      res.set('Access-Control-Allow-Origin', origin);
      res.vary('Origin');
    }
    if (req.method === 'OPTIONS') {
      res.set({ 'Access-Control-Allow-Methods': 'POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type' });
      return res.status(204).end();
    }
    // Never trust a visitor-supplied X-Forwarded-For. The transport cap also
    // bounds traffic through a shared edge; the per-email cap applies below.
    if (req.method === 'POST' && !allow(`peer:${req.socket?.remoteAddress || 'unknown'}`, 120)) {
      res.set('Retry-After', '3600');
      return reply(req, res, 429, { error: 'Too many requests. Please try again in an hour.' });
    }
    next();
  });
  router.post(ENDPOINT, express.json({ limit: '8kb', strict: false }), express.urlencoded({ extended: false, limit: '8kb', parameterLimit: 8 }), async (req, res) => {
    try {
      if (!req.is(['application/json', 'application/x-www-form-urlencoded'])) {
        return reply(req, res, 415, { error: 'Submit the form using the website.' });
      }
      if (String(req.body?.company_website || '').trim()) {
        return reply(req, res, 400, { error: 'We could not accept that request. Please try again.' });
      }
      const validated = validateAssessmentPayload(req.body);
      if (!validated.ok) {
        const code = Object.values(validated.errors)[0];
        return reply(req, res, 400, { error: REASON_MESSAGES[code] || REASON_MESSAGES.malformed, error_code: code, details: validated.errors });
      }
      const emailKey = createHash('sha256').update(validated.values.email).digest('hex');
      if (!allow(`email:${emailKey}`, 6)) {
        res.set('Retry-After', '3600');
        return reply(req, res, 429, { error: 'Too many requests for this email address. Please try again in an hour.' });
      }
      const stored = await captureAssessmentRequest(db, validated.values);
      console.info('[substral-assessment] intake accepted', {
        actionId: String(stored.id),
        duplicate: stored.duplicate,
        clientId: stored.client_id,
      });

      const deliveryRequired = productionDeliveryRequired();
      const preflightBlock = notificationBlockReason(validated.values);
      if (deliveryRequired && preflightBlock === 'missing_provider_key') {
        console.error('[substral-assessment] notification blocked: missing_provider_key');
        return reply(req, res, 503, {
          error: 'We could not confirm your request. Your details are still in the form. Please try again.',
        });
      }

      try {
        const notification = await notifyAssessmentRequest(
          db,
          validated.values,
          stored.id,
          stored.client_id
        );
        if (deliveryRequired
          && notification.status === 'suppressed'
          && notification.reason !== 'duplicate_or_missing_action'
          && notification.reason !== 'already_sent'
          && notification.reason !== 'synthetic_submission') {
          console.error('[substral-assessment] notification suppressed in production', notification.reason);
          return reply(req, res, 503, {
            error: 'We could not confirm your request. Your details are still in the form. Please try again.',
          });
        }
      } catch (notifyErr) {
        console.error('[substral-assessment] notification failed:', notifyErr.code || notifyErr.message);
        return reply(req, res, 503, {
          error: 'We could not confirm your request. Your details are still in the form. Please try again.',
        });
      }

      return reply(req, res, stored.duplicate ? 200 : 201, {
        ok: true, request_id: stored.id, domain: stored.domain, review_mode: 'human',
        message: successMessage(stored.domain, validated.values.email),
      });
    } catch (error) {
      if (error.code === 'REQUEST_KEY_CONFLICT') return reply(req, res, 409, { error: error.message });
      // Log the failure class, never visitor details or database credentials.
      console.error('[substral-assessment] persistence failed:', error.code || error.name);
      return reply(req, res, 503, { error: 'We could not confirm your request. Your details are still in the form. Please try again.' });
    }
  });
  router.use(ENDPOINT, (error, req, res, next) => {
    if (!error) return next();
    return reply(req, res, error.type === 'entity.too.large' ? 413 : 400, {
      error: error.type === 'entity.too.large' ? 'That request is too large. Keep your decision context under 300 characters.' : 'We could not read that request. Please try again.',
    });
  });
  router._rateBuckets = buckets;
  return router;
}
module.exports = createAssessmentRouter();
module.exports.createAssessmentRouter = createAssessmentRouter;
module.exports.REASON_MESSAGES = REASON_MESSAGES;
module.exports.successMessage = successMessage;
