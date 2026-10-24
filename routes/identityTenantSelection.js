'use strict';

const { createIdentityTenantSelection, TARGET_CLIENT_ID, WORKFLOW } = require('../services/identityTenantSelection');
const FORM_PATH = '/identity-check/tenant';

function canonicalOrigin(value) {
  try {
    const url = new URL(value);
    if (url.username || url.password || !['https:', 'http:'].includes(url.protocol)) return null;
    if (url.protocol === 'http:' && !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)) return null;
    return url.origin;
  } catch (_) { return null; }
}
function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function createIdentityTenantSelectionRouter({ Router, pool, appUrl = () => process.env.APP_URL }) {
  const router = Router();
  const parseForm = require('express').urlencoded({ extended: false, limit: '2kb', parameterLimit: 8 });
  const selection = createIdentityTenantSelection({ pool });
  router.all(FORM_PATH, async (req, res) => {
    // Capture only native session middleware identity, never body/query identity.
    const sessionId = req.sessionID;
    const session = req.session;
    // This route writes the locked existing row directly. With the application's
    // default unset=keep, null prevents Express's stale whole-session upsert/touch,
    // including on failures. It does NOT destroy the stored session or its cookie.
    req.session = null;
    res.set('Cache-Control', 'no-store');
    // Native same-origin form navigation needs Origin; no-referrer makes it null.
    res.set('Referrer-Policy', 'same-origin');
    res.set('Content-Security-Policy', "default-src 'none'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'");
    res.set('X-Frame-Options', 'DENY');
    const fail = (status, error) => res.status(status).json({ error,
      message: error === 'selection_outcome_unknown'
        ? 'The result is uncertain. Check /api/me without resubmitting this form.'
        : 'No tenant selection was completed. Return to /api/me, then open a fresh identity-check form.' });
    if (!['GET', 'POST'].includes(req.method)) {
      res.set('Allow', 'GET, POST'); return fail(405, 'method_not_allowed');
    }
    if (!session?.user || session.authenticated !== true || session.user.role !== 'admin'
      || session.impersonation || session.user.password_change_required || session.user.email_verified === false) {
      return fail(403, 'authenticated_admin_required');
    }
    const origin = canonicalOrigin(appUrl());
    if (!origin) return fail(503, 'canonical_origin_unavailable');
    // Host and forwarding headers can never choose the authorized origin.
    if (req.get('host')?.toLowerCase() !== new URL(origin).host.toLowerCase()
      || (req.get('origin') && req.get('origin') !== origin)
      || req.get('sec-fetch-site') === 'cross-site') return fail(403, 'origin_denied');
    if (Object.keys(req.query || {}).length) return fail(400, 'unexpected_selector');
    if (req.method === 'POST') {
      if (req.get('origin') !== origin) return fail(403, 'origin_denied');
      if (!req.is('application/x-www-form-urlencoded')) return fail(415, 'form_content_type_required');
      try {
        await new Promise((resolve, reject) => parseForm(req, res, error => error ? reject(error) : resolve()));
      } catch (error) {
        return fail(error.status === 413 ? 413 : 400, 'invalid_form_body');
      }
      const body = req.body || {};
      if (Object.keys(body).some(key => !['workflow', 'csrf', 'client_id', 'confirm'].includes(key))
        || body.workflow !== WORKFLOW || body.client_id !== String(TARGET_CLIENT_ID)
        || body.confirm !== 'select-anchor' || typeof body.csrf !== 'string' || !/^[a-f0-9]{64}$/.test(body.csrf)) {
        return fail(400, 'invalid_explicit_selection');
      }
    }
    try {
      const context = { sessionId, actorId: session.user.id, fromClientId: session.active_client_id, sessionClientId: session.user.client_id, origin };
      if (req.method === 'POST') {
        await selection.select({ ...context, csrf: req.body.csrf });
        return res.redirect(303, '/api/me');
      }
      const form = await selection.issue(context);
      return res.type('html').send(`<!doctype html><html><head><meta charset="utf-8"><title>Identity-only tenant selection</title></head><body>
<h1>Identity-only tenant selection</h1>
<p>Current tenant: ${escapeHtml(form.fromClientId)}. Select ${escapeHtml(form.client.name)} (tenant ${TARGET_CLIENT_ID}) for this session only.</p>
<p>This does not change account access or load a dashboard. Close other Pulseforge tabs before continuing. The form expires in five minutes; opening a new form replaces this one.</p>
<form method="POST" action="${FORM_PATH}">
<input type="hidden" name="workflow" value="${WORKFLOW}">
<input type="hidden" name="client_id" value="${TARGET_CLIENT_ID}">
<input type="hidden" name="csrf" value="${form.token}">
<button type="submit" name="confirm" value="select-anchor">Select tenant ${TARGET_CLIENT_ID} and show account information</button>
</form><p><a href="/api/me">Cancel and show account information</a></p>
<p>After selection, verify active_client_id is 10 before making any tenant-scoped request. Do not resubmit if a result is uncertain.</p>
</body></html>`);
    } catch (error) {
      return fail(error.status || 503, error.code || 'identity_selection_unavailable');
    }
  });
  return router;
}

module.exports = { createIdentityTenantSelectionRouter, FORM_PATH, canonicalOrigin };
