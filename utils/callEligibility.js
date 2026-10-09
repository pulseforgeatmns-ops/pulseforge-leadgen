'use strict';

// Channel-specific restriction. Never use this guard for email eligibility or
// clear it when a prospect changes stage, phone number, owner, or import source.
function callProhibited(prospect) {
  return !prospect || Boolean(prospect.do_not_contact || prospect.is_synthetic || prospect.ao_call_suppressed || prospect.ao_outreach_review_required);
}

function callSuppressionError() {
  return Object.assign(new Error('Calls are suppressed for this prospect'), {
    code: 'CALL_SUPPRESSED', status: 409,
  });
}

// Re-read tenant-scoped state immediately before dispatch; a queued snapshot is
// not authorization. Missing identity, missing records and read errors fail shut.
async function assertCallAllowed(db, prospectId, clientId, { channel = 'call' } = {}) {
  if (!prospectId || !Number.isInteger(Number(clientId)) || Number(clientId) <= 0) {
    throw callSuppressionError();
  }
  const { rows } = await db.query(
    `SELECT do_not_contact, is_synthetic, ao_call_suppressed, ao_outreach_review_required
     FROM prospects WHERE id = $1 AND client_id = $2`,
    [prospectId, Number(clientId)]
  );
  if (channel === 'email') {
    if (!rows[0] || rows[0].do_not_contact || rows[0].is_synthetic || rows[0].ao_outreach_review_required) {
      throw Object.assign(new Error('Separate outreach review or valid contact permission is required'), { code: 'outreach_review_required', status: 409, providerBoundaryCrossed: false });
    }
  } else if (callProhibited(rows[0])) throw callSuppressionError();
  return rows[0];
}

function callLockKey(prospectId, clientId) {
  if (!prospectId || !Number.isInteger(Number(clientId)) || Number(clientId) <= 0) throw callSuppressionError();
  return `max:call:${Number(clientId)}:${prospectId}`;
}

// SUPPRESS_CALL commits take the same transaction advisory lock. Thus either
// suppression wins and prevents dispatch, or provider handoff finishes before
// suppression commits. Calls already handed to a provider are not recalled.
async function withCallAuthorization(pool, identities, dispatch, { channel = 'call' } = {}) {
  const ordered = [...new Map(identities.map(({ prospectId, clientId }) => [
    callLockKey(prospectId, clientId), { prospectId, clientId },
  ])).entries()].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
  if (!ordered.length) throw callSuppressionError();
  const client = await pool.connect();
  // Server lock/statement deadlines bound contention; the driver deadline also
  // bounds lost responses. All settings are LOCAL to this dedicated transaction.
  const query = (text, values) => client.query({ text, values, query_timeout: 6000 });
  let transactionStarted = false;
  let discard = false;
  try {
    transactionStarted = true;
    await query('BEGIN READ ONLY');
    await query(`SELECT set_config('lock_timeout', '5s', true),
      set_config('statement_timeout', '5s', true),
      set_config('idle_in_transaction_session_timeout', '0', true)`);
    for (const [key] of ordered) {
      await query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [key]);
    }
    for (const [, identity] of ordered) {
      await assertCallAllowed({ query }, identity.prospectId, identity.clientId, { channel });
    }
    const result = await dispatch();
    await query('COMMIT');
    transactionStarted = false;
    return result;
  } catch (error) {
    // An interrupted acquisition/commit may have reached PostgreSQL even when
    // its response did not reach us. Never reuse that connection.
    discard = true;
    throw error;
  } finally {
    if (transactionStarted) {
      try { await query('ROLLBACK'); }
      catch (_) { discard = true; }
    }
    client.release(discard);
  }
}

function withEmailAuthorization(pool, identities, dispatch) {
  return withCallAuthorization(pool, identities, dispatch, { channel: 'email' });
}

module.exports = { withSmsAuthorization: withEmailAuthorization, withEmailAuthorization, assertCallAllowed, callLockKey, callProhibited, callSuppressionError, withCallAuthorization };
