'use strict';

// SPEC-261: shared by all governed uncertainty entrypoints. The raw historical
// count is never rewritten; only an independently approved exact quarantine can
// isolate a row. A malformed unresolved row remains blocking.
const { loadRecoveryState } = require('./anchorEmailRecovery');
const { requireQuarantine, listUnresolved, classifyBlockingRows, assertNotQuarantined } = require('./anchorEmailQuarantine');
const { fail, windowReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');

async function uncertaintyState(db, tenantId, options = {}) {
  if (String(tenantId) !== '10') return null;
  const state = await loadRecoveryState(db, options);
  if (state.mode === 'legacy') return null;
  const rows = await listUnresolved(db, String(tenantId));
  let quarantines = [];
  const scopedActive = ['active','scoped_resume'].includes(state.mode) && String(tenantId) === '10'
    && options.programId === state.manifest.programId;
  if (scopedActive) {
    await requireQuarantine(db, state.manifest.quarantineDecisionHash);
    quarantines = (await db.query('SELECT * FROM anchor_email_quarantines WHERE tenant_id=$1', ['10'])).rows;
  }
  const classified = classifyBlockingRows(rows, quarantines, { ownAttempt: options.ownAttempt });
  return {
    recovery_mode: state.mode,
    uncertain: classified.rawCount,
    quarantined_uncertain: classified.quarantinedCount,
    own_attempt_uncertain: classified.ownAttemptCount,
    blocking_uncertain: scopedActive
      ? classified.blockingCount : Math.max(1, classified.blockingCount),
  };
}

function blockingUncertainty(counts) {
  return counts.blocking_uncertain ?? counts.uncertain;
}

async function assertNotQuarantinedWhenInstalled(db, identity, options = {}) {
  if (String(identity?.tenantId) !== '10') return { mode: 'legacy' };
  const state = await loadRecoveryState(db);
  if (state.mode !== 'legacy') await assertNotQuarantined(db, { ...identity, ...options });
  return state;
}

async function reserveAdmission(db, authorization, claimed) {
  if (authorization.legacy) return;
  const { manifest, admission } = authorization;
  if (authorization.resumeGrant) {
    const budget = await scopedAttemptBudget(db, authorization.program.id, require('../packages/acquisition-mission/DailyOutboundPolicy').clock(new Date(claimed.attempted_at)).day);
    const scope = manifest.scopes.email;
    // The claimed row is already in the UNION. Exactly the cap-th attempt is allowed.
    if (budget.today > scope.dailyCap || budget.total > scope.totalCap) fail('anchor_scoped_resume_cap_exhausted');
    if (!claimed.attempt_token) fail('anchor_recovery_attempt_token_required');
    await db.query(`INSERT INTO anchor_email_recovery_reservations
      (manifest_id,item_id,attempt_token,prospect_id,company_id,recipient_email,used_at)
      VALUES($1,$2,$3,$4,$5,$6,$7)`,
      [manifest.id, claimed.id, claimed.attempt_token, admission.prospectId, admission.companyId, admission.recipientEmail, claimed.attempted_at]);
    return;
  }
  const count = (await db.query(`SELECT count(*)::int AS n FROM anchor_email_recovery_reservations
    WHERE manifest_id=$1`, [manifest.id])).rows[0]?.n;
  if (!Number.isInteger(count) || count >= manifest.maxFreshSends) fail('anchor_recovery_cap_exhausted');
  if (!claimed.attempt_token) fail('anchor_recovery_attempt_token_required');
  await db.query(`INSERT INTO anchor_email_recovery_reservations
    (manifest_id,item_id,attempt_token,prospect_id,company_id,recipient_email,used_at)
    VALUES($1,$2,$3,$4,$5,$6,now())`,
  [manifest.id, claimed.id, claimed.attempt_token, admission.prospectId, admission.companyId, admission.recipientEmail]);
}

// This autocommit CAS must survive a later rollback/lost COMMIT in the
// authorization transaction. Losing its response consumes the attempt anyway;
// it never authorizes a provider retry. No provider request precedes this write.
async function consumeRecoveryDispatch(pool, authorization, { now = () => new Date(), env = process.env } = {}) {
  if (authorization.legacy) return;
  let expired = false, timer;
  const pending = pool.connect().then(client => {
    if (expired) { client.release(true); throw Object.assign(new Error('anchor_recovery_connection_timeout'), { code: 'anchor_recovery_connection_timeout' }); }
    return client;
  });
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => { expired = true; reject(Object.assign(new Error('anchor_recovery_connection_timeout'), { code: 'anchor_recovery_connection_timeout' })); }, 6000);
  });
  let client;
  try { client = await Promise.race([pending, timeout]); }
  finally { clearTimeout(timer); }
  let discard = false;
  try {
    const result = await client.query({ text: `UPDATE anchor_email_recovery_reservations
      SET dispatch_started_at=now() WHERE manifest_id=$1 AND item_id=$2
        AND attempt_token=$3 AND dispatch_started_at IS NULL RETURNING item_id`,
    values: [authorization.manifest.id, authorization.item.id, authorization.attemptToken], query_timeout: 6000 });
    if (result.rows.length !== 1) fail('anchor_recovery_dispatch_already_consumed');
    // The consumed marker is deliberately retained if the deadline passed while
    // acquiring a second connection or waiting for the durable CAS response.
    const current = now();
    const scoped = Boolean(authorization.resumeGrant);
    (scoped ? require('./anchorScopedResume').validateScopedResumeManifest : require('./anchorEmailRecovery').validateManifest)(authorization.manifest, { now: current });
    if (env[scoped ? 'ANCHOR_EMAIL_SCOPED_RESUME_ID' : 'ANCHOR_EMAIL_RECOVERY_MANIFEST_ID'] !== authorization.manifest.id
      || ![undefined, '', 'false'].includes(env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE)) fail('anchor_recovery_manifest_deactivated');
    if (scoped && require('../packages/acquisition-mission/DailyOutboundPolicy').clock(current).day !== require('../packages/acquisition-mission/DailyOutboundPolicy').clock(new Date(authorization.reservation.used_at)).day) fail('anchor_recovery_dispatch_day_changed');
    if (!authorization.program || windowReason(authorization.program.policy, current, true)) fail('anchor_recovery_dispatch_window_expired');
  } catch (error) { discard = true; throw error; }
  finally { client.release(discard); }
}

async function withRecoveryMaintenanceBarrier(pool, dispatch, { tenantId } = {}) {
  if (tenantId == null) fail('anchor_recovery_tenant_required');
  if (String(tenantId) !== '10') return dispatch();
  const client = await pool.connect();
  const db = { query: (text, values) => client.query({ text, values, query_timeout: 6000 }) };
  let begun = false, discard = false;
  try {
    await db.query('BEGIN READ ONLY'); begun = true;
    await db.query("SET LOCAL lock_timeout='5s'");
    await db.query("SET LOCAL statement_timeout='5s'");
    await db.query("SELECT pg_advisory_xact_lock_shared(hashtextextended('anchor-email-recovery:10',0))");
    const state = await loadRecoveryState(db);
    if (state.mode !== 'legacy') fail('anchor_recovery_maintenance_closed');
    const result = await dispatch();
    await db.query('COMMIT'); begun = false;
    return result;
  } catch (error) { discard = true; throw error; }
  finally { if (begun) { try { await db.query('ROLLBACK'); } catch { discard = true; } } client.release(discard); }
}

async function assertLegacyPreparation(db, { tenantId, program, operation } = {}, options = {}) {
  const tenant = tenantId ?? program?.tenant_id ?? program?.tenantId;
  const programTenant = program?.tenant_id ?? program?.tenantId;
  if (tenantId != null && programTenant != null && String(tenantId) !== String(programTenant)) fail('anchor_recovery_tenant_mismatch');
  if (tenant == null) fail('anchor_recovery_tenant_required');
  if (String(tenant) !== '10') return { mode: 'legacy' };
  const state = await loadRecoveryState(db);
  if (state.mode === 'scoped_resume' && program && operation) {
    return require('./anchorScopedResume').assertScopedResumeCapability(db, { program, capability: 'preparation', operation }, options);
  }
  if (state.mode !== 'legacy') fail('anchor_recovery_separate_preparation_approval_required');
  return state;
}


// Permanent reservations are UNIONed with original attempted rows. A proven
// pre-provider release, new grant ID, next day or lost commit never refunds usage.
async function scopedAttemptBudget(db, programId, day) {
  const corrupt = await db.query(`SELECT r.item_id FROM anchor_email_recovery_reservations r
    LEFT JOIN acquisition_outbound_items i ON i.id=r.item_id
    LEFT JOIN acquisition_outbound_envelopes e ON e.id=i.envelope_id
    WHERE i.id IS NULL OR e.id IS NULL OR i.tenant_id IS DISTINCT FROM '10' OR e.tenant_id IS DISTINCT FROM '10'
      OR i.prospect_id IS DISTINCT FROM r.prospect_id OR i.company_id IS DISTINCT FROM r.company_id
      OR i.email IS DISTINCT FROM r.recipient_email OR e.program_id IS NULL OR e.local_day IS NULL LIMIT 1`);
  if (corrupt.rows.length) fail('anchor_scoped_resume_budget_binding_invalid');
  const row = (await db.query(`WITH consumed AS (
    SELECT i.id,e.local_day FROM acquisition_outbound_items i
      JOIN acquisition_outbound_envelopes e ON e.id=i.envelope_id
      WHERE i.tenant_id='10' AND e.program_id=$1 AND i.attempted_at IS NOT NULL
    UNION
    SELECT i.id,e.local_day FROM anchor_email_recovery_reservations r
      JOIN acquisition_outbound_items i ON i.id=r.item_id
      JOIN acquisition_outbound_envelopes e ON e.id=i.envelope_id
      WHERE i.tenant_id='10' AND e.program_id=$1)
    SELECT count(*)::int AS total,count(*) FILTER(WHERE local_day=$2::date)::int AS today FROM consumed`, [programId,day])).rows[0];
  if (!row || !Number.isSafeInteger(row.today) || !Number.isSafeInteger(row.total)) fail('anchor_scoped_resume_budget_unavailable');
  return row;
}
async function assertScopedPreparedIdentity(db, program, entry, options = {}) {
  if (String(program?.tenant_id) !== '10') return { mode: 'legacy' };
  const state = await loadRecoveryState(db, options);
  if (state.mode === 'legacy') return state;
  if (state.mode !== 'scoped_resume' || state.manifest.programId !== program.id) fail('anchor_scoped_resume_closed');
  const scoped = require('./anchorScopedResume');
  const admission = state.manifest.admissions.find(a => a.prospectId === String(entry.prospectId || entry.prospect_id)
    && a.companyId === String(entry.companyId || entry.company_id) && a.recipientEmail === String(entry.email).toLowerCase());
  if (!admission) fail('anchor_scoped_resume_admission_missing');
  const identity = await scoped.readScopedIdentity(db, { tenantId: '10', ...admission });
  scoped.assertIdentityReview(identity, admission);
  await assertNotQuarantined(db, { tenantId: '10', prospectId: admission.prospectId, companyId: admission.companyId,
    email: admission.recipientEmail, lockCompanyRows: true });
  return state;
}

async function reserveScopedPreparation(pool, { tenantId, program, operation, entries, key }, options = {}) {
  if (String(tenantId) !== '10') return { mode: 'legacy' };
  if (String(program?.tenant_id) !== '10') fail('anchor_recovery_tenant_mismatch');
  const state = await loadRecoveryState(pool, options);
  if (state.mode === 'legacy') return state;
  const bound = await require('./anchorScopedResume').assertScopedResumeCapability(pool, { program, capability: 'preparation', operation }, options);
  const identities = [];
  for (const entry of entries) {
    await assertScopedPreparedIdentity(pool, program, entry, options);
    const identity = bound.manifest.admissions.find(a => a.prospectId === String(entry.prospectId || entry.prospect_id)
      && a.companyId === String(entry.companyId || entry.company_id) && a.recipientEmail === String(entry.email).toLowerCase());
    if (!identity) fail('anchor_scoped_resume_admission_missing');
    identities.push(identity);
  }
  if (!identities.length) fail('anchor_scoped_resume_preparation_empty');
  await require('./anchorScopedResumeControl').consumeScopedOperation(pool, { manifest: bound.manifest,
    capability: 'preparation', operation, operationId: require('../packages/acquisition-mission/DailyOutboundPolicy').hash(['scoped-preparation',program.id,operation,key]),
    identities }, options);
  return bound;
}
async function checkScopedPreparation(db, program, operation, entries, options = {}) {
  if (String(program?.tenant_id) !== '10') return { mode: 'legacy' };
  const state = await loadRecoveryState(db, options);
  if (state.mode === 'legacy') return state;
  const bound = await require('./anchorScopedResume').assertScopedResumeCapability(db, { program, capability: 'preparation', operation }, options);
  for (const entry of entries) await assertScopedPreparedIdentity(db, program, entry, options);
  return bound;
}

module.exports = { reserveScopedPreparation, checkScopedPreparation, scopedAttemptBudget, assertScopedPreparedIdentity, assertLegacyPreparation, uncertaintyState, blockingUncertainty, assertNotQuarantinedWhenInstalled, reserveAdmission, consumeRecoveryDispatch, withRecoveryMaintenanceBarrier };
