'use strict';
const { loadRecoveryState, assertRecoveryLeaf } = require('../services/anchorEmailRecovery');
const { anchorOutreachReviewRequired, assertAnchorOutreachReviewCleared } = require('./anchorOutreachReview');

// Channel-specific restriction. Never use this guard for email eligibility or
// clear it when a prospect changes stage, phone number, owner, or import source.
function callProhibited(prospect) {
  return !prospect || Boolean(prospect.do_not_contact || prospect.is_synthetic || prospect.ao_call_suppressed || prospect.ao_outreach_review_required)
    || anchorOutreachReviewRequired(prospect);
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
    if (!rows[0] || rows[0].do_not_contact || rows[0].is_synthetic || rows[0].ao_outreach_review_required
      || anchorOutreachReviewRequired(rows[0], clientId)) {
      throw Object.assign(new Error('Separate outreach review or valid contact permission is required'), { code: 'outreach_review_required', status: 409, providerBoundaryCrossed: false });
    }
  } else if (callProhibited(rows[0]) || anchorOutreachReviewRequired(rows[0], clientId)) throw callSuppressionError();
  return rows[0];
}

function callLockKey(prospectId, clientId) {
  if (!prospectId || !Number.isInteger(Number(clientId)) || Number(clientId) <= 0) throw callSuppressionError();
  return `max:call:${Number(clientId)}:${prospectId}`;
}

// SUPPRESS_CALL commits take the same transaction advisory lock. Thus either
// suppression wins and prevents dispatch, or provider handoff finishes before
// suppression commits. Calls already handed to a provider are not recalled.
async function withCallAuthorization(pool, identities, dispatch, { channel = 'call', context, command } = {}) {
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
    // SPEC-262 isolates the recovery boundary to trusted Anchor email. Other
    // tenants and all calls/SMS keep their pre-recovery queries and policy.
    const hasAnchor = ordered.some(([, identity]) => Number(identity.clientId) === 10);
    const allAnchor = ordered.every(([, identity]) => Number(identity.clientId) === 10);
    if (channel === 'email' && ((hasAnchor && !allAnchor)
      || (context?.tenantId != null && ordered.some(([, identity]) => String(identity.clientId) !== String(context.tenantId))))) {
      throw Object.assign(new Error('Conflicting email tenant identities'), { code: 'anchor_recovery_scope_invalid', providerBoundaryCrossed: false });
    }
    const isOutreach = channel === 'email' && allAnchor;
    const initialState = isOutreach ? await loadRecoveryState({ query }) : { mode: 'legacy' };
    transactionStarted = true;
    // Anchor also locks current CRM/company rows through the provider handoff.
    // PostgreSQL row locks require a read-write transaction; this path still
    // performs no CRM writes. Other tenants retain the original transaction.
    await query(initialState.mode === 'legacy' && !hasAnchor ? 'BEGIN READ ONLY' : 'BEGIN');
    await query(`SELECT set_config('lock_timeout', '5s', true),
      set_config('statement_timeout', '5s', true),
      set_config('idle_in_transaction_session_timeout', '0', true)`);
    if (isOutreach) {
      // Shared by quarantine/schema admission. Always acquire before CRM locks,
      // including legacy paths, so arming cannot race an in-flight handoff.
      await query("SELECT pg_advisory_xact_lock_shared(hashtextextended('anchor-email-recovery:10',0))");
    }
    if (isOutreach && typeof context?.programId === 'string' && context.programId) {
      // Match claim order: program authority before recipient/CRM locks.
      await query('SELECT id FROM acquisition_outbound_programs WHERE id=$1 AND tenant_id=$2 FOR SHARE', [context.programId, '10']);
    }
    if (!isOutreach) for (const [key] of ordered) {
      await query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [key]);
    }
    if (isOutreach) {
      // Re-read under the boundary: installed maintenance still runs universal
      // actual-recipient denial before its independent send-authority denial.
      const liveState = await loadRecoveryState({ query });
      if (liveState.mode !== 'legacy') await require('../services/anchorScopedResume').assertCrmEmailQuarantine(
        { query }, { identities, command, tenantId: '10', itemId: context?.itemId });
    }
    const recovery = isOutreach
      ? await assertRecoveryLeaf({ query }, { channel, identities, context, command })
      : { legacy: true };
    if (isOutreach && recovery.legacy) for (const [key] of ordered) {
      await query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [key]);
    }
    for (const [, identity] of ordered) {
      if (recovery.legacy) await assertCallAllowed({ query }, identity.prospectId, identity.clientId,
        { channel: channel === 'sms' ? 'email' : channel });
      await assertAnchorOutreachReviewCleared({ query }, identity.prospectId, identity.clientId, { lock: true });
    }
    if (!recovery.legacy) await require('../services/anchorEmailRecoveryIntegration').consumeRecoveryDispatch(pool, recovery);
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

function withEmailAuthorization(pool, identities, dispatch, options = {}) {
  return withCallAuthorization(pool, identities, dispatch, { ...options, channel: 'email' });
}

function withSmsAuthorization(pool, identities, dispatch) {
  // Distinct channel at the recovery boundary; legacy SMS retains email checks.
  return withCallAuthorization(pool, identities, dispatch, { channel: 'sms' });
}

module.exports = { withSmsAuthorization, withEmailAuthorization, assertCallAllowed, callLockKey, callProhibited, callSuppressionError, withCallAuthorization };
