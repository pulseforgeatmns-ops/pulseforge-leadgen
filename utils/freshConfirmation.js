'use strict';

// Step-up confirmation for sensitive approvals (Scout research spending).
// A remembered or long-lived session is never enough on its own: the signed-in
// admin re-enters their password, which records a short-lived, single-use,
// purpose-bound marker inside the existing server-side session row. The
// session ID is unchanged, so grants pinned to the approver's session hash
// still bind. The marker is consumed atomically in SQL, so it cannot be
// replayed by a concurrent request and consuming it never re-saves (or
// resurrects) a session that was logged out in another tab.
const PURPOSES = Object.freeze(['scout_research_approval', 'scout_research_cycle_release']);
const VALID_MS = 2 * 60 * 1000;
const MAX_FAILURES = 5;
const LOCKOUT_MS = 15 * 60 * 1000;

const failures = new Map();

function lockedUntil(userId, now = Date.now()) {
  const entry = failures.get(String(userId));
  return entry && entry.count >= MAX_FAILURES && now < entry.until ? entry.until : 0;
}
function recordFailure(userId, now = Date.now()) {
  const key = String(userId), entry = failures.get(key);
  const next = entry && now < entry.until ? { count: entry.count + 1, until: entry.until } : { count: 1, until: now + LOCKOUT_MS };
  if (next.count >= MAX_FAILURES) next.until = now + LOCKOUT_MS;
  failures.set(key, next);
  if (failures.size > 5000) failures.clear();
}
function clearFailures(userId) { failures.delete(String(userId)); }

function marker(userId, purpose, now = Date.now()) {
  return { userId: String(userId), purpose, at: now, expiresAt: now + VALID_MS };
}

// Returns true only if this exact session held an unexpired marker for this
// user and purpose; the marker is removed in the same statement.
async function consume(db, { sessionId, userId, purpose, now = Date.now() }) {
  if (typeof sessionId !== 'string' || !sessionId || !PURPOSES.includes(purpose) || !userId) return false;
  const { rows } = await db.query(`UPDATE "session" SET sess = (sess::jsonb - 'freshConfirmation')::json
    WHERE sid = $1 AND expire > clock_timestamp()
      AND sess::jsonb #>> '{freshConfirmation,userId}' = $2
      AND sess::jsonb #>> '{freshConfirmation,purpose}' = $3
      AND (sess::jsonb #>> '{freshConfirmation,expiresAt}')::bigint > $4
    RETURNING sid`, [sessionId, String(userId), purpose, now]);
  return rows.length === 1;
}

module.exports = { PURPOSES, VALID_MS, MAX_FAILURES, LOCKOUT_MS, lockedUntil, recordFailure, clearFailures, marker, consume };
