'use strict';

// Lifecycle of the express-session/connect-pg-simple login session:
//
// - Rotation: a fresh session ID is issued on every successful sign-in, so a
//   pre-login (possibly attacker-planted) session ID never becomes authenticated.
// - Persistence: unremembered sessions keep the 24-hour policy; opt-in
//   "Remember me" extends the SAME server-side session to an absolute 30-day
//   cap. The browser only ever holds the HttpOnly signed session-ID cookie.
// - Revalidation: every account-backed session (remembered or not) is checked
//   against `users` at most every 30 seconds. Deactivation, deletion, a role
//   change, a password change/reset, a forced reset or an unverified email ends
//   it, even if it was re-saved by a request that was in flight at the time.
// - Revocation: account changes made through the app delete the account's
//   session rows immediately (revokeUserSessions).
const { createHmac } = require('node:crypto');

const DEFAULT_MAX_AGE_MS = 24 * 60 * 60 * 1000;
const REMEMBER_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000;
const REVALIDATE_INTERVAL_MS = 30 * 1000;
const CACHE_LIMIT = 2000;

const verified = new Map();

function wantsRemember(body) {
  return ['on', 'true', '1', 'yes'].includes(String(body?.remember || '').toLowerCase());
}

// Binds a session to the password it was opened with, without storing the
// password hash (or anything reversible to it) in the session.
function credentialTag(userId, passwordHash) {
  return createHmac('sha256', process.env.SESSION_SECRET || 'pulseforge-secret-key')
    .update(`session:${userId}:${passwordHash || ''}`).digest('hex').slice(0, 32);
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

// Replaces the pre-authentication session with a new, empty one under a new ID
// and deletes the old store row. Nothing from before sign-in is carried over.
function rotateSession(req) {
  return new Promise((resolve, reject) => {
    if (typeof req.session?.regenerate !== 'function') return reject(new Error('session_rotation_unavailable'));
    req.session.regenerate(error => {
      if (error) return reject(error);
      skipRepeatSave(req.session);
      resolve(req.session);
    });
  });
}

// express-session does not track saves of a regenerated session, so after the
// login route's explicit save it would write the same session again at the end
// of the request (and could recreate a row a concurrent logout just deleted).
// Skip a repeat save whose content is unchanged apart from the cookie's
// rolling expiry; any real change is still written.
function skipRepeatSave(session) {
  const save = session.save;
  if (typeof save !== 'function') return;
  const content = sess => JSON.stringify({ ...sess, cookie: { originalMaxAge: sess.cookie?.originalMaxAge,
    secure: sess.cookie?.secure } });
  let persisted = null;
  Object.defineProperty(session, 'save', { configurable: true, enumerable: false, writable: true, value(callback) {
    const snapshot = content(this);
    if (persisted !== null && snapshot === persisted) { if (callback) process.nextTick(callback); return this; }
    return save.call(this, error => { if (!error) persisted = snapshot; if (callback) callback(error); });
  } });
}

// Called after the password and account checks pass, on the rotated session.
function applyLoginPersistence(req, user, remember, now = Date.now()) {
  if (req.session?.cookie && isSecureRequest(req)) req.session.cookie.secure = true;
  if (user?.id != null && user.password_hash) req.session.credentialTag = credentialTag(user.id, user.password_hash);
  else delete req.session.credentialTag;
  if (remember && user?.id != null) {
    setMaxAge(req.session, REMEMBER_MAX_AGE_MS);
    req.session.remembered = { since: now, expiresAt: now + REMEMBER_MAX_AGE_MS };
  } else {
    setMaxAge(req.session, DEFAULT_MAX_AGE_MS);
    delete req.session.remembered;
  }
}

function forgetUser(userId) {
  for (const key of verified.keys()) if (key.startsWith(`${userId}:`)) verified.delete(key);
}

// Deletes every stored session of this account (optionally keeping the
// caller's own, already re-bound session). Call after deactivation, deletion,
// a password reset, or a role/tenant change.
async function revokeUserSessions(db, userId, { exceptSid = null } = {}) {
  forgetUser(userId);
  const { rowCount } = await db.query(`DELETE FROM "session" WHERE sess::jsonb #>> '{user,id}' = $1
    AND ($2::text IS NULL OR sid <> $2)`, [String(userId), exceptSid]);
  forgetUser(userId);
  return rowCount;
}

function accountStillMatches(row, session) {
  const user = session.user;
  return Boolean(row && row.active === true && row.role === user.role
    && row.email_verified !== false
    && Boolean(row.password_change_required) === Boolean(user.password_change_required)
    // Sessions created before credential tags existed are checked on everything else.
    && (!session.credentialTag || credentialTag(row.id, row.password_hash) === session.credentialTag));
}

function endSession(req, next) {
  // Regenerate (not destroy) so downstream handlers still see an empty session
  // and treat the request as signed out; the old row is deleted from the store.
  req.session.regenerate(error => next(error));
}

function createSessionAccountGuard({ pool, intervalMs = REVALIDATE_INTERVAL_MS, now = () => Date.now() } = {}) {
  return async function sessionAccountGuard(req, res, next) {
    const session = req.session;
    const user = session?.user;
    // The env-password fallback has no account row; it is never remembered.
    if (!user || user.id == null) return next();
    if (session.remembered && !(now() < Number(session.remembered.expiresAt))) return endSession(req, next);
    const key = `${user.id}:${user.role}:${Boolean(user.password_change_required)}:${session.credentialTag || '-'}`;
    const checkedAt = verified.get(key);
    if (checkedAt && now() - checkedAt < intervalMs) return next();
    let row;
    try {
      const db = pool || require('../db');
      row = (await db.query(`SELECT id, role, active, email_verified, password_change_required, password_hash
        FROM users WHERE id = $1`, [user.id])).rows[0];
    } catch {
      // Never continue a session on an account that cannot be verified.
      res.set('Cache-Control', 'no-store');
      return res.status(503).json({ error: 'session_check_unavailable' });
    }
    if (!accountStillMatches(row, session)) { verified.delete(key); return endSession(req, next); }
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
  rotateSession,
  applyLoginPersistence,
  createSessionAccountGuard,
  revokeUserSessions,
  forgetUser,
};
