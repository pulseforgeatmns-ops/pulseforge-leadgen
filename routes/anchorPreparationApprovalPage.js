'use strict';

// Display-only shell/context. The existing approval POSTs remain the sole authority.
const express = require('express');
const path = require('node:path');
const PAGE_PATH = '/admin/anchor-preparation-approval';
const CONTEXT_PATH = '/api/v1/tenant-outreach/anchor-program/preparation-approval/context';
function createAnchorPreparationApprovalPageRouter({ pool, env = process.env } = {}) {
  const router = express.Router();
  const assets = { [`${PAGE_PATH}/assets/style.css`]: 'anchor-preparation-approval.css',
    [`${PAGE_PATH}/assets/controller.js`]: 'anchor-preparation-approval.js',
    [`${PAGE_PATH}/assets/tokens.css`]: 'shared/tokens.css',
    '/public/anchor-preparation-approval.js': 'anchor-preparation-approval.js',
    '/public/anchor-preparation-approval.css': 'anchor-preparation-approval.css' };
  const paths = [PAGE_PATH, '/public/anchor-preparation-approval.html', '/anchor-preparation-approval.html', CONTEXT_PATH, ...Object.keys(assets)];
  router.all(paths, async (req, res) => {
    res.set({ 'Cache-Control': 'no-store', 'Vary': 'Cookie', 'X-Content-Type-Options': 'nosniff',
      'Referrer-Policy': 'same-origin', 'Content-Security-Policy': "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'" });
    const hadIdentity = Boolean(req.session?.user || req.session?.authenticated);
    let actor;
    try { actor = require('../services/anchorCrmPreparationAction').actorFromRequest(req); }
    catch { /* The response below must never save an obsolete session snapshot. */ }
    req.session = null;
    try {
      if (req.method !== 'GET') return res.set('Allow', 'GET').status(405).json({ error: 'method_not_allowed' });
      const origin = require('./identityTenantSelection').canonicalOrigin(env.APP_URL);
      if (!origin || req.get('host') !== new URL(origin).host
        || (req.get('origin') && req.get('origin') !== origin)
        || (req.path === CONTEXT_PATH && req.get('sec-fetch-site') === 'cross-site')) throw new Error('origin_required');
      if (!actor) throw new Error('identity_required');
      const context = await require('../services/anchorCrmPreparationApproval')
        .readPreparationApprovalContext(pool || require('../db'), actor, { env });
      if (req.path === CONTEXT_PATH) return res.json(context);
      if (assets[req.path]) return res.sendFile(path.join(__dirname, '..', 'public', assets[req.path]));
      return res.sendFile(path.join(__dirname, '..', 'public', 'anchor-preparation-approval.html'));
    } catch {
      const status = hadIdentity ? 403 : 401;
      if (req.path === CONTEXT_PATH) return res.status(status).json({ error: 'anchor_approval_page_identity_required' });
      return res.status(status).type('html').send('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Approval controls unavailable</title><link rel="icon" href="data:,"></head><body><main><h1>Approval controls unavailable</h1><p>A current, verified administrator session with Anchor (tenant 10) selected is required. No approval was recorded.</p><p><a href="/login?workflow=identity-check">Sign in</a> · <a href="/identity-check/tenant">Choose Anchor using the native identity flow</a></p><p>Do not resubmit a previous approval if its result is uncertain. Return with its exact manifest ID and hash to check the receipt.</p></main></body></html>');
    }
  });
  return router;
}
module.exports = { createAnchorPreparationApprovalPageRouter, PAGE_PATH, CONTEXT_PATH };
