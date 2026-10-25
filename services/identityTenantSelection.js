'use strict';

const { randomBytes, createHash, timingSafeEqual } = require('node:crypto');

const TARGET_CLIENT_ID = 10;
const WORKFLOW = 'identity-tenant-check';
const CHALLENGE_TTL_MS = 5 * 60 * 1000;
const CHALLENGE_KEY = 'identityTenantSelection';

function denied(code, status = 403) {
  return Object.assign(new Error(code), { code, status });
}
function positiveId(value) {
  if (!['string', 'number'].includes(typeof value) || !/^[1-9][0-9]*$/.test(String(value))) return null;
  const id = Number(value);
  return Number.isSafeInteger(id) ? id : null;
}
function clientBinding(value) {
  if (value == null) return null;
  const id = positiveId(value);
  if (!id) throw denied('invalid_account_binding');
  return id;
}
function digest(value) { return createHash('sha256').update(value).digest('hex'); }
function sameDigest(left, right) {
  return typeof left === 'string' && /^[a-f0-9]{64}$/.test(left)
    && timingSafeEqual(Buffer.from(left, 'hex'), Buffer.from(right, 'hex'));
}
function checkSession(sess, actorId) {
  if (!sess || sess.authenticated !== true || positiveId(sess.user?.id) !== actorId) {
    throw denied('authenticated_account_required', 401);
  }
  if (sess.impersonation || sess.user.role !== 'admin') throw denied('identity_selection_denied');
  if (sess.user.password_change_required || sess.user.email_verified === false) {
    throw denied('account_action_required');
  }
  const fromClientId = positiveId(sess.active_client_id);
  if (!fromClientId) throw denied('current_tenant_required');
  return fromClientId;
}

// Only the existing authenticated session row is written. No schema helper,
// workspace hydrator, audit publisher, credential, user or client write belongs here.
function createIdentityTenantSelection({ pool }) {
  async function transact({ sessionId, actorId, origin, csrf, fromClientId, sessionClientId, select }) {
    actorId = positiveId(actorId);
    if (!actorId || typeof sessionId !== 'string' || !sessionId) throw denied('authenticated_account_required', 401);
    let db; let begun = false; let commitAttempted = false;
    try {
      db = await pool.connect();
      await db.query('BEGIN'); begun = true;
      await db.query("SET LOCAL lock_timeout = '3s'");
      await db.query("SET LOCAL statement_timeout = '5s'");
      const locked = await db.query('SELECT sess FROM "session" WHERE sid = $1 FOR UPDATE', [sessionId]);
      if (!locked.rows.length) throw denied('session_expired', 401);
      // Evaluate the clock AFTER acquiring the row lock, not before a lock wait.
      const clock = await db.query(`SELECT EXTRACT(EPOCH FROM clock_timestamp()) * 1000 AS now_ms,
        expire > clock_timestamp() AS unexpired,
        EXTRACT(EPOCH FROM expire) * 1000 AS expire_ms FROM "session" WHERE sid = $1`, [sessionId]);
      const now = Number(clock.rows[0]?.now_ms);
      if (clock.rows[0]?.unexpired !== true || !Number.isFinite(now)) throw denied('session_expired', 401);
      const sess = locked.rows[0].sess;
      const currentClientId = checkSession(sess, actorId);
      const accountClientId = clientBinding(sess.user.client_id);
      if (clientBinding(sessionClientId) !== accountClientId) throw denied('session_snapshot_changed', 409);
      if (fromClientId !== undefined && positiveId(fromClientId) !== currentClientId) {
        throw denied('session_snapshot_changed', 409);
      }
      const userResult = await db.query(`SELECT id, role, active, client_id, email_verified, password_change_required
        FROM users WHERE id = $1 FOR SHARE`, [actorId]);
      const user = userResult.rows[0];
      if (!user || positiveId(user.id) !== actorId || user.active !== true || user.role !== 'admin') {
        throw denied('current_admin_required');
      }
      if (user.email_verified === false || user.password_change_required !== false) {
        throw denied('account_action_required');
      }
      const dbClientId = clientBinding(user.client_id);
      // Match the existing operator-switcher NULL semantics, but never fall back
      // when this column/schema is absent. A missing/inactive/hidden target denies.
      const target = await db.query(`SELECT id, name FROM clients WHERE id = $1
        AND active = true AND COALESCE(operator_switchable, true) = true FOR SHARE`, [TARGET_CLIENT_ID]);
      if (!target.rows.length) throw denied('target_not_available');
      let token;
      let result;
      if (select) {
        const challenge = sess[CHALLENGE_KEY];
        if (!challenge || challenge.version !== 1 || challenge.actorId !== actorId
          || challenge.fromClientId !== currentClientId || challenge.sessionClientId !== accountClientId
          || challenge.dbClientId !== dbClientId || challenge.targetClientId !== TARGET_CLIENT_ID
          || challenge.origin !== origin || !Number.isFinite(challenge.expiresAt)
          || !Number.isFinite(challenge.issuedAt) || challenge.issuedAt > now
          || challenge.expiresAt <= now || challenge.expiresAt - challenge.issuedAt > CHALLENGE_TTL_MS
          || typeof csrf !== 'string' || !/^[a-f0-9]{64}$/.test(csrf)
          || !sameDigest(challenge.digest, digest(csrf))) {
          throw denied('form_expired_or_used', 409);
        }
        result = await db.query(`UPDATE "session" SET sess = jsonb_set(
          sess::jsonb - 'identityTenantSelection', '{active_client_id}', '10'::jsonb, true)::json
          WHERE sid = $1 AND expire > clock_timestamp()
            AND (sess->'identityTenantSelection'->>'expiresAt')::numeric
              > EXTRACT(EPOCH FROM clock_timestamp()) * 1000
          RETURNING sess`, [sessionId]);
      } else {
        token = randomBytes(32).toString('hex');
        const challenge = { version: 1, actorId, fromClientId: currentClientId,
          targetClientId: TARGET_CLIENT_ID, sessionClientId: accountClientId, dbClientId, origin, issuedAt: now,
          expiresAt: Math.min(now + CHALLENGE_TTL_MS, Number(clock.rows[0].expire_ms)), digest: digest(token) };
        result = await db.query(`UPDATE "session" SET sess = jsonb_set(
          sess::jsonb, '{identityTenantSelection}', $2::jsonb, true)::json
          WHERE sid = $1 AND expire > clock_timestamp() RETURNING sess`,
        [sessionId, JSON.stringify(challenge)]);
      }
      if (result.rows.length !== 1) throw denied('session_or_form_expired', 409);
      // Verify the persisted projection inside the same transaction before commit.
      const saved = result.rows[0].sess;
      if (checkSession(saved, actorId) !== (select ? TARGET_CLIENT_ID : currentClientId)
        || (select && saved[CHALLENGE_KEY] !== undefined)) throw denied('session_save_failed', 503);
      commitAttempted = true;
      await db.query('COMMIT'); begun = false;
      return { token, fromClientId: currentClientId, client: target.rows[0] };
    } catch (error) {
      if (begun && db) {
        try { await db.query('ROLLBACK'); } catch (_) { /* Outcome remains unknown after COMMIT was attempted. */ }
      }
      if (commitAttempted) throw denied('selection_outcome_unknown', 503);
      if (error.status) throw error;
      throw denied('identity_selection_unavailable', 503);
    } finally {
      if (db) db.release();
    }
  }
  return {
    issue: options => transact({ ...options, select: false }),
    select: options => transact({ ...options, select: true }),
  };
}

module.exports = { checkSession, clientBinding, positiveId, createIdentityTenantSelection, TARGET_CLIENT_ID, WORKFLOW, CHALLENGE_TTL_MS };
