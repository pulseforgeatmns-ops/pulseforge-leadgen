'use strict';
// Preparation-only identity binding. A session digest is audit data, never a
// replacement for the native session credential required at install/action.
const { createHash } = require('node:crypto');
const { checkSession, clientBinding, positiveId } = require('./identityTenantSelection');
function fail() { throw Object.assign(new Error('anchor_crm_preparation_approver_invalid'), {
  code: 'anchor_crm_preparation_approver_invalid', providerBoundaryCrossed: false, retryAllowed: false }); }
function sessionBindingHash(sessionId) {
  if (typeof sessionId !== 'string' || !sessionId || sessionId.length > 512) fail();
  return createHash('sha256').update(sessionId).digest('hex');
}
function assertActorSession(actor, manifest) {
  if (!actor || String(actor.id) !== manifest.operatorId || actor.role !== 'admin'
    || sessionBindingHash(actor.sessionId) !== manifest.preparationAuthority.approverSessionHash) fail();
}
async function assertPreparationApprover(db, manifest, { lock = true, actor, sessionId } = {}) {
  if (actor) { assertActorSession(actor, manifest); sessionId = actor.sessionId; }
  if (sessionBindingHash(sessionId) !== manifest.preparationAuthority.approverSessionHash) fail();
  const a = manifest.preparationAuthority, actorId = positiveId(manifest.operatorId);
  if (!actorId || manifest.operatorRole !== 'admin') fail();
  const suffix = lock ? ' FOR SHARE' : '';
  const sessions = await db.query(`SELECT sid,sess FROM "session" WHERE sid=$1${suffix}`, [sessionId]);
  if (sessions.rows.length !== 1) fail();
  const { sid, sess } = sessions.rows[0];
  // Read database clock after acquiring session lock; injected policy clocks
  // must never extend revoked/expired authentication.
  const expiry = await db.query('SELECT expire > clock_timestamp() AS unexpired FROM "session" WHERE sid=$1', [sid]);
  if (expiry.rows[0]?.unexpired !== true) fail();
  try {
    if (checkSession(sess, actorId) !== 10 || !Object.hasOwn(sess.user, 'client_id')
      || clientBinding(sess.user.client_id) !== a.approverAccountClientId) fail();
  } catch { fail(); }
  const users = await db.query(`SELECT id,role,active,client_id,email_verified,password_change_required
    FROM users WHERE id::text=$1${suffix}`, [String(actorId)]);
  const user = users.rows[0];
  if (users.rows.length !== 1 || !user || positiveId(user.id) !== actorId || user.active !== true
    || user.role !== 'admin' || user.email_verified === false || user.password_change_required !== false
    || !Object.hasOwn(user, 'client_id')) fail();
  try { if (clientBinding(user.client_id) !== a.approverAccountClientId
    || ![null, 10].includes(a.approverAccountClientId)) fail(); } catch { fail(); }
  const targets = await db.query(`SELECT id FROM clients WHERE id=10 AND active=true
    AND COALESCE(operator_switchable,true)=true${suffix}`);
  if (targets.rows.length !== 1) fail();
  // Account/target locks can wait too: authentication must still be live after
  // those waits and at each caller's final pre-write validation.
  const finalExpiry = await db.query('SELECT expire > clock_timestamp() AS unexpired FROM "session" WHERE sid=$1', [sid]);
  if (finalExpiry.rows[0]?.unexpired !== true) fail();
  return user;
}
// Revocation can only close authority. The same pinned approver/account may
// authenticate with a fresh native session after logout; this ephemeral binding
// never replaces the stored grant, authorizes consumption, or refunds a claim.
async function assertPreparationRevoker(db, manifest, actor, { lock = true } = {}) {
  if (!actor || String(actor.id) !== manifest.operatorId || actor.role !== 'admin') fail();
  const closingIdentity = { ...manifest, preparationAuthority: { ...manifest.preparationAuthority,
    approverSessionHash: sessionBindingHash(actor.sessionId) } };
  return assertPreparationApprover(db, closingIdentity, { actor, lock });
}
module.exports = { sessionBindingHash, assertActorSession, assertPreparationApprover, assertPreparationRevoker };
