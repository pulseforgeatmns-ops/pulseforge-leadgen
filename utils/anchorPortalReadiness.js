'use strict';
const path = require('node:path');

function isAnchorPortalPath(rawPath) {
  const matches = pathname => ['/portal/anchor', '/api/anchor-portal', '/public/anchor-portal.html', '/anchor-portal.html']
    .some(prefix => pathname === prefix || pathname.startsWith(`${prefix}/`));
  // Express matches the raw route prefix before decoding route parameters.
  // Normalization must not erase that prefix when an encoded parameter contains ../.
  if (matches(String(rawPath).toLowerCase())) return true;
  try { return matches(path.posix.normalize(decodeURIComponent(rawPath)).toLowerCase()); } catch (_) { return false; }
}

// Only portal traffic is gated; unrelated application boot behavior is unchanged.
function createAnchorPortalReadiness({ initAuth, ensureSchema, onError = console.error }) {
  let ready = false;
  const initialized = Promise.resolve()
    .then(initAuth)
    .then(ensureSchema)
    .then(() => { ready = true; })
    .catch(error => { onError(error); });
  function middleware(req, res, next) {
    if (!isAnchorPortalPath(req.path || '')) return next();
    if (ready) return next();
    res.set('Cache-Control', 'no-store');
    res.set('Retry-After', '30');
    return res.status(503).json({ error: 'anchor_portal_unavailable' });
  }
  return { initialized, middleware, isReady: () => ready };
}

module.exports = { createAnchorPortalReadiness, isAnchorPortalPath };
