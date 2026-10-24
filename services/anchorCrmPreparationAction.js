'use strict';
// Explicit HTTP action, not a scheduler, activation or retry authority.
const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const recovery = require('./anchorEmailRecovery');
const { prepareExistingCrm } = require('./anchorScopedResumeCrmPreparation');
function reject() { throw Object.assign(new Error('anchor_crm_action_rejected'), { code: 'anchor_crm_action_rejected' }); }
function actorFromRequest(req) {
  const user = req.session?.user;
  const tenant = req.session?.active_client_id ?? user?.client_id;
  if (!user?.id || user.role !== 'admin' || user.active === false || user.password_change_required
    || req.session?.impersonation?.active || req.impersonation?.active
    || (tenant !== 10 && tenant !== '10')) reject();
  return { id: String(user.id), role: 'admin' };
}
function validateInput(input) {
  if (!input || Object.keys(input).sort().join(',') !== 'manifestHash,revision,sourceMissionHash'
    || !/^[a-f0-9]{40}$/.test(input.revision || '')
    || !/^[a-f0-9]{64}$/.test(input.manifestHash || '')
    || !/^[a-f0-9]{64}$/.test(input.sourceMissionHash || '')) reject();
}
async function connect(pool) {
  let expired = false, timer;
  const pending = pool.connect().then(client => {
    if (expired) { client.release(true); reject(); }
    return client;
  });
  let client;
  try { client = await Promise.race([pending, new Promise((_, rejectTimeout) => {
    timer = setTimeout(() => { expired = true; rejectTimeout(new Error('connection_timeout')); }, 6000);
  })]); } finally { clearTimeout(timer); }
  return { query: (text, values = []) => client.query({ text, values, query_timeout: 6000 }),
    release: () => client.release(true) };
}
async function prepareDrafts(pool, input, actor, { env = process.env, now = () => new Date() } = {}) {
  let started = false;
  try {
    validateInput(input);
    if (!actor?.id || actor.role !== 'admin' || env.RAILWAY_GIT_COMMIT_SHA !== input.revision) reject();
    // Bounded read-only preflight. No schema bootstrap, pool shutdown or secret output.
    const db = await connect(pool);
    let begun = false, manifest;
    try {
      await db.query('BEGIN READ ONLY'); begun = true;
      await db.query("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='5s'");
      const state = await recovery.loadRecoveryState(db, { env, now: now(), lockControl: false });
      manifest = state.manifest;
      if (state.mode !== 'scoped_resume' || hash(manifest) !== input.manifestHash
        || manifest.operatorId !== String(actor.id) || manifest.operatorRole !== 'admin'
        || manifest.scopes.email.enabled || manifest.scopes.research.enabled
        || !manifest.scopes.preparation.enabled
        || !manifest.scopes.preparation.operations.includes('prepare_existing_crm')) reject();
      const users = (await db.query('SELECT id FROM users WHERE id::text=$1 AND client_id=10 AND role=\'admin\' AND active=true', [String(actor.id)])).rows;
      if (users.length !== 1) reject();
      const source = (await db.query('SELECT payload FROM acquisition_missions WHERE id=$1 AND tenant_id=\'10\'', [manifest.sourceMissionId])).rows[0]?.payload;
      if (!source || source.id !== manifest.sourceMissionId || String(source.tenantId) !== '10'
        || source.stage !== 'discover' || source.structuredMission?.immutable !== true
        || source.planCancelled || /cancel/i.test(String(source.status))
        || hash(source) !== input.sourceMissionHash || hash(missionScope(source)) !== manifest.scopeHash) reject();
      await db.query('COMMIT'); begun = false;
    } finally { if (begun) await db.query('ROLLBACK').catch(() => {}); db.release(true); }
    const pinnedEnv = new Proxy(env, { get(target, key) {
      if (key === 'ANCHOR_EMAIL_SCOPED_RESUME_ID' && target[key] !== manifest.id) return undefined;
      return Reflect.get(target, key);
    }, set() { reject(); } });
    started = true;
    const result = await prepareExistingCrm(pool, { programId: manifest.programId,
      sourceMissionHash: input.sourceMissionHash, admissions: manifest.admissions },
    { env: pinnedEnv, now, actionAuthority: { actorId: String(actor.id), manifestHash: input.manifestHash, revision: input.revision } });
    if (result.readyForReview !== true || result.executionApproved !== false || result.envelopeAuthorized !== false
      || result.sent !== 0 || result.providerCalls !== 0) reject();
    return { status: 'ready_for_review', missionId: result.missionId, operationId: result.operationId,
      preparedArtifactRevision: result.preparedArtifactRevision, candidateCount: result.candidateCount,
      readyForReview: true, executionApproved: false, envelopeAuthorized: false, sent: 0, providerCalls: 0,
      providerReadiness: 'not_checked', retryAllowed: false };
  } catch (error) {
    // A consumed claim is not success. No refunds, auto-resume or repeat provider work.
    const consumed = error.code === 'anchor_scoped_resume_operation_consumed';
    throw Object.assign(new Error('anchor_crm_action_rejected'), { code: 'anchor_crm_action_rejected',
      status: consumed ? 'already_consumed' : started ? 'blocked_review_required' : 'rejected',
      writesMayHaveOccurred: started, retryAllowed: false });
  }
}
module.exports = { actorFromRequest, validateInput, prepareDrafts };
