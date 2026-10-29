'use strict';

// Opt-in "Remember me" for the existing express-session/connect-pg-simple
// login. It never creates a second credential: the remembered session is the
// same server-side session row with a longer, absolute-capped lifetime. The
// browser only ever holds the HttpOnly signed session-ID cookie.
//
// Remembered sessions are revalidated against `users` on a short interval, so
// deactivation, a role change, a password change or a forced password reset
// ends them without waiting for the 30-day cookie to lapse. Sessions that are
// not remembered keep the existing 24-hour policy unchanged.
const { createHmac } = require('node:crypto');

const DEFAULT_MAX_AGE_MS = 24 * 60 * 60 * 1000;
const REMEMBER_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000;
const REVALIDATE_INTERVAL_MS = 30 * 1000;
const CACHE_LIMIT = 2000;

const verified = new Map();

function wantsRemember(body) {
  return ['on', 'true', '1', 'yes'].includes(String(body?.remember || '').toLowerCase());
}

// Binds the remembered session to the password it was opened with, without
// storing the password hash (or anything reversible to it) in the session.
function credentialTag(userId, passwordHash) {
  return createHmac('sha256', process.env.SESSION_SECRET || 'pulseforge-secret-key')
    .update(`remember:${userId}:${passwordHash || ''}`).digest('hex').slice(0, 32);
}

function setMaxAge(session, ms) {
  if (!session?.cookie) return;
  // express-session resets maxAge to originalMaxAge on every touch.
  session.cookie.originalMaxAge = ms;
  session.cookie.maxAge = ms;
}

// Mirrors express-session's `proxy: true` check. A spoofed header can only make
// the cookie stricter (Secure), never weaker.
function isSecureRequest(req) {
  if (req.secure === true || req.connection?.encrypted) return true;
  const proto = String(req.headers?.['x-forwarded-proto'] || '').split(',')[0].trim().toLowerCase();
  return proto === 'https';
}

// Called by POST /login after the password and account checks pass.
function applyLoginPersistence(req, user, remember, now = Date.now()) {
  // secure:'auto' is only evaluated when a session is generated; a session that
  // pre-dates this login must not keep a non-Secure cookie over HTTPS.
  if (req.session?.cookie && isSecureRequest(req)) req.session.cookie.secure = true;
  if (remember && user?.id) {
    setMaxAge(req.session, REMEMBER_MAX_AGE_MS);
    req.session.remembered = { since: now, expiresAt: now + REMEMBER_MAX_AGE_MS,
      tag: credentialTag(user.id, user.password_hash) };
  } else {
    setMaxAge(req.session, DEFAULT_MAX_AGE_MS);
    delete req.session.remembered;
  }
}

function forgetUser(userId) {
  for (const key of verified.keys()) if (key.startsWith(`${userId}:`)) verified.delete(key);
}

function accountStillMatches(row, sessionUser, remembered) {
  return Boolean(row && row.active === true && row.role === sessionUser.role
    && row.email_verified !== false && Boolean(row.password_change_required) === Boolean(sessionUser.password_change_required)
    && credentialTag(row.id, row.password_hash) === remembered.tag);
}

function endSession(req, next) {
  // Regenerate (not destroy) so downstream handlers still see an empty session
  // and treat the request as signed out; the old row is deleted from the store.
  req.session.regenerate(error => next(error));
}

function createRememberedSessionGuard({ pool, intervalMs = REVALIDATE_INTERVAL_MS, now = () => Date.now() } = {}) {
  return async function rememberedSessionGuard(req, res, next) {
    const remembered = req.session?.remembered;
    if (!remembered) return next();
    const user = req.session.user;
    if (!user?.id || !remembered.tag || !(now() < Number(remembered.expiresAt))) return endSession(req, next);
    const key = `${user.id}:${remembered.tag}:${user.role}:${Boolean(user.password_change_required)}`;
    const checkedAt = verified.get(key);
    if (checkedAt && now() - checkedAt < intervalMs) return next();
    let row;
    try {
      const db = pool || require('../db');
      row = (await db.query(`SELECT id, role, active, email_verified, password_change_required, password_hash
        FROM users WHERE id = $1`, [user.id])).rows[0];
    } catch {
      // Never extend a remembered session on an unverifiable account.
      res.set('Cache-Control', 'no-store');
      return res.status(503).json({ error: 'session_check_unavailable' });
    }
    if (!accountStillMatches(row, user, remembered)) { verified.delete(key); return endSession(req, next); }
    if (verified.size >= CACHE_LIMIT) verified.clear();
    verified.set(key, now());
    return next();
  };
}

module.exports = {
  DEFAULT_MAX_AGE_MS,
  REMEMBER_MAX_AGE_MS,
  REVALIDATE_INTERVAL_MS,
  wantsRemember,
  isSecureRequest,
  credentialTag,
  applyLoginPersistence,
  createRememberedSessionGuard,
  forgetUser,
};
