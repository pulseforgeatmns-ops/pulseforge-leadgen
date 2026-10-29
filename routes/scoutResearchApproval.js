'use strict';

// Admin-only, display-only shell for the SPEC-SCOUT-P0-003 research mission and
// research-only grant. The existing POST /api/v1/tenant-outreach/anchor-program
// research-mission/* and research-only-grant/* endpoints remain the sole
// authority; this router serves the page, a non-secret identity summary, and the
// password step-up that those install endpoints require.
const express = require('express');
const path = require('node:path');
const { requireAuth, requireRole } = require('../middleware/auth');
const fresh = require('../utils/freshConfirmation');

const PAGE_PATH = '/admin/scout-research-approval';
const CONTEXT_PATH = '/api/v1/admin/scout-research-approval/context';
const CONFIRM_PATH = '/api/v1/auth/fresh-confirmation';
const ASSETS = { [`${PAGE_PATH}/assets/style.css`]: 'scout-research-approval.css',
  [`${PAGE_PATH}/assets/controller.js`]: 'scout-research-approval.js',
  [`${PAGE_PATH}/assets/tokens.css`]: 'shared/tokens.css' };
const CSP = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";

function guard(res) {
  res.set({ 'Cache-Control': 'no-store', 'Vary': 'Cookie', 'X-Content-Type-Options': 'nosniff',
    'Referrer-Policy': 'same-origin', 'Content-Security-Policy': CSP });
}
function sameOrigin(req, env) {
  const origin = require('./identityTenantSelection').canonicalOrigin(env.APP_URL);
  return Boolean(origin) && req.get('origin') === origin && req.get('sec-fetch-site') !== 'cross-site' && req.is('application/json');
}

function createScoutResearchApprovalRouter({ pool, env = process.env } = {}) {
  const router = express.Router();
  const db = () => pool || require('../db');
  const adminOnly = [requireAuth, requireRole('admin')];

  router.get([PAGE_PATH, ...Object.keys(ASSETS)], ...adminOnly, (req, res) => {
    guard(res);
    const file = ASSETS[req.path] || 'scout-research-approval.html';
    return res.sendFile(path.join(__dirname, '..', 'public', file));
  });

  // What the page may show about the signed-in operator. No session ID, cookie,
  // hash, credential or account secret is ever returned.
  router.get(CONTEXT_PATH, ...adminOnly, (req, res) => {
    guard(res);
    const user = req.session?.user || {};
    const tenant = Number(req.session?.active_client_id);
    let approvalReady = true;
    try { require('../services/anchorCrmPreparationAction').actorFromRequest(req); } catch { approvalReady = false; }
    return res.json({ name: user.name || null, email: user.email || null, role: user.role || null, activeClientId: Number.isInteger(tenant) ? tenant : null,
      anchorSelected: tenant === 10, impersonating: Boolean(req.session?.impersonation), approvalReady,
      rememberedSession: Boolean(req.session?.remembered), freshConfirmationSeconds: fresh.VALID_MS / 1000 });
  });

  // Password step-up. Required by the research install endpoints for every
  // session, remembered or not. Never echoes the password or any token.
  router.post(CONFIRM_PATH, express.json({ limit: '4kb', strict: true }), ...adminOnly, async (req, res) => {
    guard(res);
    if (!sameOrigin(req, env)) return res.status(403).json({ error: 'fresh_confirmation_forbidden' });
    const user = req.session?.user;
    const purpose = req.body?.purpose, password = req.body?.password;
    if (!user?.id || !fresh.PURPOSES.includes(purpose) || typeof password !== 'string' || !password || password.length > 1024)
      return res.status(400).json({ error: 'fresh_confirmation_invalid' });
    const locked = fresh.lockedUntil(user.id);
    if (locked) return res.status(429).json({ error: 'fresh_confirmation_locked', retryAfterSeconds: Math.ceil((locked - Date.now()) / 1000) });
    let row;
    try {
      row = (await db().query('SELECT id, role, active, password_hash FROM users WHERE id = $1', [user.id])).rows[0];
    } catch { return res.status(503).json({ error: 'fresh_confirmation_unavailable' }); }
    const { bcrypt } = require('../middleware/auth');
    const ok = row && row.active === true && row.role === 'admin' && await bcrypt.compare(password, row.password_hash || '');
    if (!ok) { fresh.recordFailure(user.id); return res.status(401).json({ error: 'fresh_confirmation_rejected' }); }
    fresh.clearFailures(user.id);
    req.session.freshConfirmation = fresh.marker(user.id, purpose);
    try { await new Promise((resolve, reject) => req.session.save(error => error ? reject(error) : resolve())); }
    catch { return res.status(503).json({ error: 'fresh_confirmation_unavailable' }); }
    return res.json({ confirmed: true, purpose, validForSeconds: fresh.VALID_MS / 1000, singleUse: true });
  });

  return router;
}

module.exports = { createScoutResearchApprovalRouter, PAGE_PATH, CONTEXT_PATH, CONFIRM_PATH };
