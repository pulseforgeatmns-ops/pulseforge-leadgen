'use strict';

const { hash, fail, policy, missionScope, clock, windowReason, candidateReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { GovernedOutboundStore } = require('./governedOutboundStore');
const { governedOutboundEnabledForTenant, governedOutboundSendingDisabledForTenant, governedOutboundPreparationEnabledForTenant } = require('./governedOutboundTenant');
const { createGovernedOutboundContext } = require('./governedOutboundContext');
const {
  evaluatePreparationRefill,
  remainingDispatchCapacity,
  remainingScheduleSlots,
  selectRefillEntries,
  selectInventoryRefillEntries,
  observabilityFromRefill,
  finalizePreparationObservability,
  PREPARATION_BATCH_LIMIT,
} = require('./governedOutboundRefill');
const {
  createProviderBoundaryTracker,
  isPreProviderOutboundFailure,
  terminalPreProviderReason,
  providerBoundaryWasCrossed,
  attachProviderBoundaryCrossed,
  markLeafProviderSend,
} = require('./governedOutboundProviderBoundary');
const { reconcileUncertainItemFromEvidence } = require('./governedUncertainSendReconciliation');
const {
  resolveOperatorDelegatedMaximumDailyCapacity,
  resolveOperatorProgramTotalCapForDelegation,
  resolveBoundedGrantHorizon,
  describeOperatorAuthorityEnvelope,
  countGrantWeekdaySlots,
  DEFAULT_BOUNDED_GRANT_HORIZON_DAYS,
} = require('../packages/emmett-outbound/OperatorDelegatedCapacity');

function service({
  pool,
  adapters,
  tenantId,
  governedContext = null,
  now = () => new Date(),
  enabled = null,
} = {}) {
  const governed = governedContext || createGovernedOutboundContext({ tenantId });
  const resolvedTenantId = governed.tenantId;
  const isEnabled = enabled || (() => governedOutboundEnabledForTenant(resolvedTenantId));
  const isPreparationEnabled = () => governedOutboundPreparationEnabledForTenant(resolvedTenantId, isEnabled());
  const store = new GovernedOutboundStore(pool, resolvedTenantId);
  async function authorize(input, actor) {
    if (!actor?.id || !['admin', 'manager'].includes(actor.role)) fail('operator_required');
    const p = policy({ ...input, tenantId: String(input.tenantId || resolvedTenantId) }, now());
    const source = await adapters.loadMission(p.sourceMissionId);
    if (!source?.mission?.structuredMission?.immutable || String(source.mission.tenantId) !== p.tenantId) fail('approved_source_mission_required');
    if (require('./governedOutboundTenant').createGovernedOutboundTenantContext(p.tenantId).usesTenantMailboxTransport
      && source.mission.stage !== 'ready') fail('ready_source_mission_required');
    const scopeHash = hash(missionScope(source.mission));
    const reviewHash = hash({ policy: p, scopeHash });
    if (input.reviewHash !== reviewHash) return { reviewRequired: true, reviewHash, policy: p, scopeHash };
    return store.createProgram(p, scopeHash, String(actor.id));
  }
  async function validateProgram(program, sending = false) {
    if (!program || ['paused', 'revoked'].includes(program.mode)) fail('program_disabled');
    if (hash(program.policy) !== program.policy_hash) fail('policy_changed');
    const reason = windowReason(program.policy, now(), sending);
    if (reason) fail(reason);
    const source = await adapters.loadMission(program.source_mission_id);
    if (!source?.mission || source.mission.planCancelled || /cancel/i.test(source.mission.status)
      || hash(missionScope(source.mission)) !== program.scope_hash) fail('source_scope_changed');
    await adapters.validateTenant(program);
    return source;
  }
  function buildOperatorDelegatedMigrationReview(input, program, authorizationNow) {
    const current = program.policy || {};
    const delegatedDaily = input.operatorDelegatedMaximumDailyCapacity
      ?? current.operatorDelegatedMaximumDailyCapacity;
    const grantHorizonDays = input.grantHorizonDays != null
      ? Number(input.grantHorizonDays)
      : (input.renewBoundedGrantHorizon ? DEFAULT_BOUNDED_GRANT_HORIZON_DAYS : null);
    let startsAt = input.startsAt || current.startsAt;
    let expiresAt = input.expiresAt || current.expiresAt;
    let grantHorizon = null;
    if (grantHorizonDays != null) {
      grantHorizon = resolveBoundedGrantHorizon(authorizationNow, grantHorizonDays);
      startsAt = grantHorizon.startsAt;
      expiresAt = grantHorizon.expiresAt;
    }
    const policyBasisForTotalCap = grantHorizon != null
      ? { ...current, startsAt, expiresAt }
      : current;
    const migratedTotalCap = input.totalCap != null
      ? input.totalCap
      : resolveOperatorProgramTotalCapForDelegation(policyBasisForTotalCap, delegatedDaily);
    const authorityBefore = describeOperatorAuthorityEnvelope(current);
    const p = policy({
      ...current,
      ...input,
      tenantId: String(input.tenantId || current.tenantId || resolvedTenantId),
      sourceMissionId: input.sourceMissionId || current.sourceMissionId || program.source_mission_id,
      senderEmail: input.senderEmail || current.senderEmail,
      inboxIntegrationId: input.inboxIntegrationId || current.inboxIntegrationId,
      sendingIdentityId: input.sendingIdentityId || current.sendingIdentityId,
      startsAt,
      expiresAt,
      dailyCap: input.dailyCap ?? current.dailyCap,
      totalCap: migratedTotalCap,
      operatorDelegatedMaximumDailyCapacity: delegatedDaily,
    }, authorizationNow);
    const authorityAfter = describeOperatorAuthorityEnvelope(p);
    const authorizationInstant = authorizationNow.toISOString();
    const reviewHash = hash({ policy: p, scopeHash: program.scope_hash, authorizationInstant });
    return {
      reviewHash,
      policy: p,
      scopeHash: program.scope_hash,
      programId: program.id,
      tenantId: resolvedTenantId,
      authorizationInstant,
      migration: 'operator_delegated_maximum_daily_capacity',
      authority: { before: authorityBefore, after: authorityAfter },
      grantHorizon: grantHorizon || undefined,
      programTotalCapMigration: {
        previousTotalCap: authorityBefore.totalCap,
        nextTotalCap: authorityAfter.totalCap,
        grantWeekdaySlots: countGrantWeekdaySlots(p),
      },
    };
  }
  async function migrateOperatorDelegatedCapacity(input, actor) {
    if (!actor?.id || !['admin', 'manager'].includes(actor.role)) fail('operator_required');
    const program = await store.program();
    if (!program) fail('program_not_found');
    const applyAttempt = input.reviewHash != null && String(input.reviewHash).length > 0;
    if (applyAttempt && !input.authorizationInstant) fail('policy_review_stale');
    const authorizationNow = applyAttempt
      ? new Date(input.authorizationInstant)
      : now();
    if (applyAttempt && Number.isNaN(authorizationNow.getTime())) fail('policy_review_stale');
    const review = buildOperatorDelegatedMigrationReview(input, program, authorizationNow);
    if (input.reviewHash !== review.reviewHash) {
      if (applyAttempt) fail('policy_review_stale');
      return { reviewRequired: true, ...review };
    }
    if (hash(review.policy) === program.policy_hash) return program;
    const authorization = {
      kind: 'operator_delegated_maximum_daily_capacity',
      operatorDelegatedMaximumDailyCapacity: review.policy.operatorDelegatedMaximumDailyCapacity,
      totalCap: review.policy.totalCap,
      previousTotalCap: review.authority.before.totalCap,
      grantHorizonDays: review.grantHorizon?.grantHorizonDays,
      startsAt: review.policy.startsAt,
      expiresAt: review.policy.expiresAt,
      recordedAt: review.authorizationInstant,
      actor: String(actor.id),
      note: input.authorizationNote || 'Emmett-authoritative dynamic outbound capacity migration',
    };
    return store.migrateProgramPolicy(program, review.policy, String(actor.id), authorization);
  }
  async function setMode(id, mode, reviewHash, actor) {
    if (!actor?.id || !['admin', 'manager'].includes(actor.role)) fail('operator_required');
    const program = await store.program();
    if (program?.id !== id) fail('program_not_found');
    if (mode === 'active') {
      if (reviewHash !== program.policy_hash) fail('policy_review_required');
      if (windowReason(program.policy, now(), false) === 'authorization_expired') fail('authorization_expired');
    }
    await store.mode(program, mode, String(actor.id));
    return store.status();
  }
  async function prepare(program, source, day, recovery = null) {
    const snapshot = await adapters.prepare(program, source, day, store, recovery);
    if (snapshot.mission.stage !== 'ready') fail('daily_mission_not_ready');
    const prepared = await adapters.prepared(snapshot, program);
    const selected = [];
    const emails = new Set(); const companies = new Set();
    const excluded = [];
    for (const row of prepared.candidates) {
      const crm = await adapters.contact(row.candidateId);
      const reason = candidateReason(row.item, crm, row.message, program.policy);
      const entry = { candidateId: String(row.candidateId), prospectId: String(crm?.prospect_id || crm?.id || ''),
        companyId: String(crm?.company_id || ''), email: String(row.item.email || '').toLowerCase(),
        message: row.message, sender: prepared.sender, revision: prepared.revision };
      const suppressed = !reason && await store.suppression(entry);
      if (reason || suppressed || !entry.companyId || emails.has(entry.email) || companies.has(entry.companyId)) {
        excluded.push({ candidateId: entry.candidateId, reason: reason || suppressed || 'duplicate_or_missing_company' });
        continue;
      }
      const operatorDailyCeiling = resolveOperatorDelegatedMaximumDailyCapacity(program.policy) ?? program.policy.dailyCap;
      if (selected.length >= Math.min(operatorDailyCeiling, prepared.capacity, PREPARATION_BATCH_LIMIT)) break;
      selected.push(entry); emails.add(entry.email); companies.add(entry.companyId);
    }
    await store.event('batch_eligibility', [program.id, day, prepared.revision, hash({ selected: selected.map(x => x.candidateId), excluded })], { programId: program.id, selected: selected.length,
      selectedCandidates: selected.map(row => ({ candidateId: row.candidateId, prospectId: row.prospectId, companyId: row.companyId })), excluded });
    if (!selected.length) fail('verified_inventory_shortfall');
    if (recovery) {
      const current = await store.program();
      if (isEnabled() || current?.id !== program.id || current.mode !== 'shadow'
        || current.policy_hash !== program.policy_hash || clock(now()).day !== day) fail('replenishment_grant_changed');
      await validateProgram(current);
    }
    return store.freeze(program, day, snapshot.mission.id, prepared.revision, selected, { requireShadow: Boolean(recovery) });
  }
  async function initializePreparation(input, actor) {
    if (!actor?.id || !['admin', 'manager'].includes(actor.role)) fail('operator_required');
    const disabled = () => {
      if (isEnabled() || !governedOutboundSendingDisabledForTenant(resolvedTenantId)) fail('preparation_requires_disabled_sending');
    };
    disabled();
    return store.lock(async () => {
      const db = await pool.connect();
      try {
        await db.query('BEGIN');
        // Serialize with mode changes as well as ordinary preparation/recovery.
        const program = (await db.query('SELECT * FROM acquisition_outbound_programs WHERE tenant_id=$1 AND mode<>\'revoked\' FOR UPDATE', [resolvedTenantId])).rows[0];
        if (!program || program.id !== input.programId || program.mode !== 'shadow') fail('preparation_shadow_grant_required');
        if (String(actor.id) !== program.authorized_by) fail('preparation_authorizing_operator_required');
        if (program.policy_hash !== input.policyHash) fail('policy_changed');
        if (program.scope_hash !== input.scopeHash || program.source_mission_id !== input.sourceMissionId
          || program.policy.sourceMissionId !== program.source_mission_id) fail('source_scope_changed');
        const source = await validateProgram(program);
        if (!source.mission.structuredMission?.immutable || String(source.mission.tenantId) !== resolvedTenantId) fail('approved_source_mission_required');
        const day = clock(now()).day;
        if (input.localDay !== day) fail('preparation_day_changed');
        const envelope = await store.one('SELECT id FROM acquisition_outbound_envelopes WHERE tenant_id=$1 AND (program_id=$2 OR local_day=$3::date) LIMIT 1', [resolvedTenantId, program.id, day]);
        if (envelope) fail('preparation_envelope_exists');
        const counts = await store.counts(program, day);
        if (counts.today || counts.total || counts.uncertain) fail('preparation_attempt_exists');
        disabled();
        const currentTime = now();
        if (clock(currentTime).day !== day) fail('preparation_day_changed');
        const reason = windowReason(program.policy, currentTime, true);
        if (reason) fail(reason);
        // Only the canonical empty row and its audit event are written. No
        // runtime hydration, attempt reservation, mission creation or tick.
        const { created, progress } = await store.ensurePreparation(program, day, db);
        if (created) await store.event('preparation_initialized', [program.id, day], {
          programId: program.id, localDay: day, missionId: progress.mission_id,
          policyHash: program.policy_hash, scopeHash: program.scope_hash, actor: String(actor.id), attempts: 0,
        }, db);
        await db.query('COMMIT');
        return { mode: 'shadow', initialized: created, programId: program.id, localDay: day,
          missionId: progress.mission_id, attempts: progress.attempts,
          preparationAttemptsPerDay: program.policy.preparationAttemptsPerDay,
          lastAttemptAt: progress.last_attempt_at, lastError: progress.last_error,
          preparationAttemptsReserved: 0, sent: 0 };
      } catch (error) { await db.query('ROLLBACK'); throw error; }
      finally { db.release(); }
    });
  }
  async function resumeReservedPreparation(input, actor) {
    return store.lock(async () => {
      if (isEnabled() || !actor?.id || !['admin','manager'].includes(actor.role)) fail('reserved_resume_requires_disabled_operator');
      const program = await store.program();
      if (program?.mode !== 'shadow' || program.id !== input.programId || program.authorized_by !== String(actor.id)) fail('replenishment_shadow_grant_required');
      const source = await validateProgram(program);
      const day = clock(now()).day;
      const sourceRow = await store.one('SELECT * FROM acquisition_missions WHERE tenant_id=$1 AND id=$2', [resolvedTenantId, program.source_mission_id]);
      const sourceProjection = { ...sourceRow.payload, id:sourceRow.id, tenantId:sourceRow.tenant_id,stage:sourceRow.stage,
        status:sourceRow.status,objective:sourceRow.objective,targetSegment:sourceRow.target_segment };
      const receipt = await store.one(`SELECT payload FROM acquisition_outbound_events WHERE tenant_id=$1
        AND program_id=$2 AND event_type='preparation_recovery_reserved' AND payload->>'reviewHash'=$3`, [resolvedTenantId, program.id, input.reviewHash]);
      const reserved = receipt?.payload;
      const review = reserved?.review;
      const progress = await store.one('SELECT * FROM acquisition_outbound_preparation WHERE program_id=$1 AND local_day=$2',[program.id,day]);
      if (!review || hash(review) !== input.reviewHash || review.localDay !== day || review.policyHash !== program.policy_hash
        || review.scopeHash !== program.scope_hash || review.operator !== String(actor.id)
        || review.sourceMissionHash !== hash(sourceProjection)
        || reserved.missionId !== `mission_daily_${hash([program.id,day,'replenishment',input.reviewHash]).slice(0,24)}`
        || progress?.mission_id !== reserved.missionId || progress.attempts !== review.nextAttempt
        || progress.attempts > program.policy.preparationAttemptsPerDay
        || (progress.last_error && !['Query read timeout','Connection terminated unexpectedly'].includes(progress.last_error))) fail('reserved_preparation_changed');
      if (await store.one('SELECT id FROM acquisition_missions WHERE tenant_id=$1 AND id=$2', [resolvedTenantId, reserved.missionId])) fail('reserved_mission_already_created');
      if (await store.envelope(day)) fail('replenishment_envelope_exists');
      const counts = await store.counts(program,day);
      if (counts.today || counts.total || counts.uncertain) fail('replenishment_attempt_exists');
      for (const candidate of review.research) if (await store.candidateOwnership({company:candidate.name,domain:candidate.domain})) fail('research_candidate_ao_owned');
      const plan = {program,source,progress,review,reviewHash:input.reviewHash,nextMissionId:reserved.missionId};
      await store.event('preparation_recovery_resumed',[program.id,input.reviewHash],{programId:program.id,missionId:reserved.missionId,reviewHash:input.reviewHash,actor:String(actor.id),reason:'Resume reserved preparation after interrupted connection before mission creation; no additional attempt.'});
      try {
        const envelope = await prepare(program,source,day,plan);
        await store.health(program);
        return {mode:'shadow',envelopeId:envelope.id,missionId:envelope.mission_id,planned:envelope.manifest.length,sent:0,preparationAttempt:progress.attempts};
      } catch(error) { await store.health(program,error.code || error.message); throw error; }
    });
  }
  async function replenish(input, actor, commit = false) {
    return store.lock(async () => {
      const recovery = require('./governedOutboundReplenishment');
      const plan = await recovery.reviewReplenishment(store, input, actor, now(), isEnabled());
      if (!commit) return { reviewRequired: true, reviewHash: plan.reviewHash, review: plan.review, nextMissionId: plan.nextMissionId };
      if (input.reviewHash !== plan.reviewHash) fail('replenishment_review_changed');
      await recovery.reserveReplenishment(store, plan, now());
      try {
        await adapters.validateTenant(plan.program);
        const envelope = await prepare(plan.program, plan.source, plan.review.localDay, plan);
        await store.health(plan.program);
        await store.event('preparation_recovery_completed', plan.reviewHash,
          { programId: plan.program.id, missionId: envelope.mission_id, envelopeId: envelope.id, reviewHash: plan.reviewHash });
        return { mode: 'shadow', envelopeId: envelope.id, missionId: envelope.mission_id,
          planned: envelope.manifest.length, sent: 0, preparationAttempt: plan.review.nextAttempt };
      } catch (error) {
        const reason = error.code || error.message;
        await store.health(plan.program, reason);
        await store.event('preparation_recovery_failed', plan.reviewHash,
          { programId: plan.program.id, missionId: plan.nextMissionId, reason, reviewHash: plan.reviewHash });
        return { mode: (await store.program())?.mode || null, halted: reason, sent: 0, preparationAttempt: plan.review.nextAttempt };
      }
    });
  }
  async function bindApproval(program, envelope) {
    if (hash(envelope.manifest) !== envelope.manifest_hash) fail('manifest_changed');
    const snapshot = await adapters.loadMission(envelope.mission_id);
    const prepared = await adapters.prepared(snapshot, program);
    if (prepared.revision !== envelope.revision) fail('artifacts_changed');
    const binding = { id: envelope.id, programId: program.id, policyHash: program.policy_hash,
      manifestHash: envelope.manifest_hash, localDay: clock(now()).day,
      candidateIds: envelope.manifest.map(x => x.candidateId) };
    const approval = await adapters.approve(snapshot, program, binding);
    if (hash(approval?.payload?.dailyEnvelope) !== hash(binding)) fail('approval_binding_mismatch');
    await store.approve(envelope, approval.id);
    return store.envelope(clock(now()).day);
  }
  async function finishPreProviderAttempt(item, envelope, error, tracker) {
    const reason = terminalPreProviderReason(error);
    const row = (await store.items(envelope.id)).find(x => x.id === item.id);
    if (row?.status !== 'attempted') return;
    if (reason === '23502') {
      await store.releaseUnsent(item, reason);
      return;
    }
    await store.finish(item, 'failed', reason);
  }

  async function dispatch(program, envelope, item) {
    let called = false;
    let claimed = false;
    let acceptedMessageId = null;
    const providerBoundary = createProviderBoundaryTracker();
    const beforeAttempt = async command => {
      if (claimed || called) fail('provider_call_budget_exceeded');
      const current = await store.program();
      if (current?.id !== program.id || current.mode !== 'active' || !isEnabled()) fail('kill_switch');
      await validateProgram(current, true);
      const liveEnvelope = await store.envelope(clock(now()).day);
      if (liveEnvelope?.id !== envelope.id || liveEnvelope.status !== 'authorized'
        || liveEnvelope.approval_id !== envelope.approval_id || hash(liveEnvelope.manifest) !== envelope.manifest_hash) fail('envelope_invalid');
      const live = (await store.items(envelope.id)).find(x => x.id === item.id);
      if (live?.status !== 'pending' || hash(live.snapshot) !== hash(item.snapshot)) fail('item_not_pending');
      const frozen = liveEnvelope.manifest.find(x => x.candidateId === item.candidate_id);
      if (!frozen || hash(frozen) !== hash(live.snapshot) || frozen.email !== live.email
        || frozen.prospectId !== live.prospect_id || frozen.companyId !== live.company_id) fail('item_manifest_mismatch');
      if (String(command.toEmail).toLowerCase() !== item.email || command.subject !== item.snapshot.message.subject
        || command.body !== item.snapshot.message.body || command.sender?.email !== item.snapshot.sender.senderEmail) fail('provider_payload_changed');
      const snapshot = await adapters.loadMission(envelope.mission_id);
      const prepared = await adapters.prepared(snapshot, current);
      if (prepared.revision !== envelope.revision) fail('artifacts_changed');
      const selected = prepared.candidates.find(x => x.candidateId === item.candidate_id);
      const refillBound = !selected && item.snapshot?.refill === true && item.snapshot.message;
      if (selected && hash(selected.message) !== hash(item.snapshot.message)) fail('copy_changed');
      if (!selected && !refillBound) fail('copy_changed');
      const queueItem = selected?.item || item.snapshot;
      const message = selected?.message || item.snapshot.message;
      const crm = await adapters.contact(item.candidate_id);
      if (String(crm?.prospect_id || crm?.id) !== item.prospect_id || String(crm?.company_id) !== item.company_id) fail('crm_binding_changed');
      const reason = candidateReason(queueItem, crm, message, current.policy)
        || await store.suppression(item.snapshot, envelope.mission_id);
      if (reason) { await store.finish(item, 'suppressed', reason); fail(reason); }
      await adapters.liveGate(current, item, prepared, now());
      await store.claim(item, current, clock(now()).day, now());
      claimed = true;
    };
    const sendFn = adapters.sendFor ? adapters.sendFor(program, { envelope, item }) : adapters.send;
    const guardedSend = async command => {
      if (!claimed || called) fail('provider_call_budget_exceeded');
      called = true;
      // Check once more after the durable claim. This also catches a pause while
      // the preceding live readiness calls were in flight.
      const finalProgram = await store.program();
      const finalItem = (await store.items(envelope.id)).find(x => x.id === item.id);
      const stop = await store.one('SELECT 1 FROM acquisition_outbound_lifecycle WHERE tenant_id=$1 AND suppressed AND (email=$2 OR company_id=$3) LIMIT 1', [resolvedTenantId, item.email, item.company_id]);
      if (!isEnabled() || finalProgram?.mode !== 'active' || finalItem?.status !== 'attempted'
        || stop || windowReason(program.policy, now(), true)) {
        await store.finish(item, 'suppressed', 'pre_provider_stop');
        fail('pre_provider_stop');
      }
      try {
        markLeafProviderSend(sendFn, providerBoundary);
        const result = await sendFn({ ...command, providerBoundary });
        const messageId = result?.providerMessageId || result?.messageId;
        const rejected = !result?.success && /^brevo_http_4/.test(String(result?.providerErrorCode || ''));
        if (rejected) {
          await store.finish(item, 'failed', result.providerErrorCode || 'provider_rejected');
          fail('provider_rejected');
        }
        // Transport failures include timeouts after acceptance. Never retry them.
        if (!result?.success || !messageId) {
          if (providerBoundaryWasCrossed({}, providerBoundary)) {
            await store.finish(item, 'uncertain', result?.providerErrorCode || 'provider_acceptance_unknown', messageId);
            fail('provider_acceptance_unknown');
          }
          await finishPreProviderAttempt(item, envelope, { code: result?.providerErrorCode || 'provider_acceptance_unknown' }, providerBoundary);
          fail(result?.providerErrorCode || 'provider_acceptance_unknown');
        }
        acceptedMessageId = messageId;
        return result;
      } catch (e) {
        if (isPreProviderOutboundFailure(e, providerBoundary)) {
          await finishPreProviderAttempt(item, envelope, e, providerBoundary);
        } else if (providerBoundaryWasCrossed(e, providerBoundary) || providerBoundary.crossed) {
          const row = (await store.items(envelope.id)).find(x => x.id === item.id);
          if (row?.status === 'attempted') await store.finish(item, 'uncertain', 'provider_or_persistence_error');
        }
        throw attachProviderBoundaryCrossed(e, providerBoundaryWasCrossed(e, providerBoundary) || providerBoundary.crossed);
      }
    };
    guardedSend.beforeAttempt = beforeAttempt;
    try {
      const result = await adapters.execute(envelope, item, program, guardedSend);
      if (!called) fail('canonical_execution_did_not_dispatch');
      if (!acceptedMessageId) fail('provider_acceptance_unknown');
      const live = (await store.items(envelope.id)).find(x => x.id === item.id);
      if (live?.status === 'attempted') await store.finish(item, 'sent', null, acceptedMessageId);
      return result;
    } catch (e) {
      const row = (await store.items(envelope.id)).find(x => x.id === item.id);
      if (row?.status === 'attempted') {
        if (providerBoundaryWasCrossed(e, providerBoundary) || providerBoundary.crossed) {
          await store.finish(item, 'uncertain', 'provider_or_persistence_error');
        } else if (!called) {
          await store.releaseUnsent(item, e.code || 'pre_provider_persist_failed');
        } else if (isPreProviderOutboundFailure(e, providerBoundary)) {
          await finishPreProviderAttempt(item, envelope, e, providerBoundary);
        }
      }
      throw e;
    }
  }
  async function refillEnvelope(program, source, day, envelope, counts) {
    const items = await store.items(envelope.id);
    const pendingPreparedCount = items.filter(row => row.status === 'pending').length;
    const sentToday = Number(counts.today || 0);
    if (pendingPreparedCount >= PREPARATION_BATCH_LIMIT) {
      return observabilityFromRefill({
        pendingPrepared: pendingPreparedCount,
        remainingDispatchCapacity: Math.max(0, Number(resolveOperatorDelegatedMaximumDailyCapacity(program.policy) ?? program.policy.dailyCap ?? 0) - sentToday),
        remainingScheduleSlots: 0,
        cleanInventory: pendingPreparedCount,
        prepareRequested: 0,
        prepareSkippedReason: 'batch_limit_reached',
      }, { sentToday });
    }
    let operating = null;
    let governor = 'proceed';
    try {
      if (typeof adapters.infrastructure === 'function') {
        const infra = await adapters.infrastructure(program, now(), null, { mode: 'planning' });
        operating = infra.operating || null;
        governor = infra.assessed?.governor?.outcome || infra.operating?.governor || 'proceed';
      }
    } catch (error) {
      const code = error.code || error.message;
      if (code === 'emmett_governor_halted') governor = 'halt';
    }
    const remainingCap = remainingDispatchCapacity({
      dispatchCapacityNow: operating?.dispatchCapacityNow ?? resolveOperatorDelegatedMaximumDailyCapacity(program.policy) ?? program.policy.dailyCap,
      sentToday,
    });
    const remainingSlots = remainingScheduleSlots({
      now: now(),
      lastSendAt: counts.last_attempt,
      allowedSendWindow: operating?.allowedSendWindow || {
        startHour: program.policy.startHour ?? 9,
        endHour: program.policy.endHour ?? 17,
        timezone: program.policy.timeZone || 'America/New_York',
      },
      minSpacingMinutes: operating?.minSpacingMinutes ?? program.policy.spacingMinutes ?? 60,
      dispatchDayAllowed: operating?.dispatchDayAllowed !== false,
    });
    let cleanInventory = 0;
    let cleanRows = [];
    const preparationDecisions = [];
    try {
      const { loadCleanInventory } = require('./maxOutboundControlLoop');
      const inventory = await loadCleanInventory(pool, store, source, store.clientId, program.policy);
      cleanRows = inventory.clean || [];
      cleanInventory = cleanRows.length;
      for (const row of inventory.excluded || []) {
        preparationDecisions.push({ candidateId: row.prospectId, prospectId: row.prospectId,
          source: 'inventory', outcome: 'rejected', reason: row.reason });
      }
    } catch (_err) {
      return observabilityFromRefill({ prepareSkippedReason: 'inventory_evaluation_failed' }, { sentToday });
    }
    const plan = evaluatePreparationRefill({
      pendingPreparedCount,
      remainingDispatchCapacity: remainingCap,
      remainingScheduleSlots: remainingSlots,
      cleanInventory,
      governor,
      grantActive: program.mode === 'active' && isPreparationEnabled(),
      dailyAuthorizationRemaining: Math.max(0, Number(operating?.authorizationLimitedCapacity ?? resolveOperatorDelegatedMaximumDailyCapacity(program.policy) ?? program.policy.dailyCap ?? 0) - sentToday),
      totalAuthorizationRemaining: Math.max(0, Number(program.policy.totalCap || 0) - Number(counts.total || 0)),
      planningDailyCapacity: operating?.planningDailyCapacity,
    });
    if (!plan.shouldPrepare) return observabilityFromRefill(plan, { sentToday, preparedAdded: 0, preparationDecisions });
    if (!['authorized', 'complete', 'frozen'].includes(envelope.status)) {
      return observabilityFromRefill({ ...plan, shouldPrepare: false, prepareSkippedReason: 'envelope_not_refillable' }, { sentToday });
    }
    const snapshot = await adapters.loadMission(envelope.mission_id);
    const prepared = await adapters.prepared(snapshot, program);
    let selected = await selectRefillEntries({
      prepared,
      program,
      store,
      adapters,
      existingItems: items,
      limit: plan.prepareRequested,
      decisions: preparationDecisions,
    });
    if (selected.length < plan.prepareRequested) {
      const inventoryEntries = await selectInventoryRefillEntries({
        cleanRows,
        store,
        adapters,
        prepared,
        program,
        existingItems: [...items, ...selected],
        limit: plan.prepareRequested - selected.length,
        decisions: preparationDecisions,
      });
      selected = selected.concat(inventoryEntries);
    }
    if (!selected.length) {
      return observabilityFromRefill({
        ...plan,
        shouldPrepare: false,
        prepareSkippedReason: plan.cleanInventory > pendingPreparedCount
          ? 'no_eligible_prepared_candidates'
          : 'no_clean_inventory',
      }, { sentToday, preparedAdded: 0, preparationDecisions });
    }
    if (!isPreparationEnabled()) fail('preparation_disabled');
    const updated = await store.appendToEnvelope(envelope, selected, prepared.revision);
    return {
      envelope: updated,
      ...observabilityFromRefill({
        ...plan,
        preparedAdded: selected.length,
        pendingPrepared: pendingPreparedCount + selected.length,
      }, { sentToday, preparedAdded: selected.length, preparationDecisions }),
    };
  }
  async function runPreparationRefill() {
    const lockResult = await store.lock(async () => {
      const program = await store.program();
      if (!program) return { halted: 'no_program' };
      if (program.mode === 'shadow') return { halted: 'shadow_mode' };
      const day = clock(now()).day;
      try {
        const source = await validateProgram(program, false);
        await store.expire(day);
        const counts = await store.counts(program, day);
        if (counts.uncertain) fail('uncertain_send_requires_reconciliation');
        const operatorDailyCeiling = resolveOperatorDelegatedMaximumDailyCapacity(program.policy) ?? program.policy.dailyCap;
        if (counts.total >= program.policy.totalCap || counts.today >= operatorDailyCeiling) fail('cap_reached');
        if (!isPreparationEnabled()) fail('environment_kill_switch');
        let envelope = await store.envelope(day);
        const initialCount = envelope ? (await store.items(envelope.id)).filter(row => row.status === 'pending').length : 0;
        if (!envelope) envelope = await prepare(program, source, day);
        if (envelope.program_id !== program.id) fail('daily_envelope_already_used');
        const refill = await refillEnvelope(program, source, day, envelope, counts);
        const pending = (await store.items(envelope.id)).filter(row => row.status === 'pending').length;
        return { programId: program.id, ...refill, pendingPrepared: pending,
          preparedAdded: Math.max(0, pending - initialCount),
          prepareSkippedReason: pending > initialCount ? null : refill.prepareSkippedReason };
      } catch (error) {
        return {
          programId: program.id,
          preparedAdded: 0,
          prepareSkippedReason: error.code || error.message,
        };
      }
    });
    if (lockResult?.halted === 'overlap') {
      return finalizePreparationObservability({
        preparedAdded: 0,
        prepareSkippedReason: 'send_lock_overlap',
      });
    }
    if (lockResult?.halted) {
      return finalizePreparationObservability({
        preparedAdded: 0,
        prepareSkippedReason: lockResult.halted,
      });
    }
    const result = finalizePreparationObservability({
      ...lockResult,
      sendingEnabled: isEnabled(),
      preparationEnabled: isPreparationEnabled(),
    });
    // Capture each actual preparation evaluation, including terminal rejections.
    await store.event('preparation_refill_evaluated', require('node:crypto').randomUUID(), {
      ...result, envelope: undefined, tenantId: resolvedTenantId,
      evaluatedAt: now().toISOString(),
    });
    return result;
  }
  async function recordTickEvaluated(program, day, outcome) {
    const bucket = Math.floor(+now() / 300000);
    await store.event('governed_tick_evaluated', [program.id, day, bucket], {
      programId: program.id,
      localDay: day,
      tenantId: resolvedTenantId,
      ...outcome,
    });
  }
  async function tick() {
    return store.lock(async () => {
      const program = await store.program();
      if (!program) return { halted: 'no_program' };
      const day = clock(now()).day;
      try {
        const source = await validateProgram(program);
        await store.expire(day);
        const counts = await store.counts(program, day);
        if (counts.uncertain) fail('uncertain_send_requires_reconciliation');
        const operatorDailyCeiling = resolveOperatorDelegatedMaximumDailyCapacity(program.policy) ?? program.policy.dailyCap;
        if (counts.total >= program.policy.totalCap || counts.today >= operatorDailyCeiling) fail('cap_reached');
        let envelope = await store.envelope(day);
        if (!envelope) envelope = await prepare(program, source, day);
        if (envelope.program_id !== program.id) fail('daily_envelope_already_used');
        if (program.mode === 'shadow') {
          await store.health(program);
          const shadowResult = { mode: 'shadow', envelopeId: envelope.id, planned: envelope.manifest.length, sent: 0 };
          await recordTickEvaluated(program, day, { halted: 'shadow_mode', ...shadowResult });
          return shadowResult;
        }
        if (!isEnabled()) fail('environment_kill_switch');
        let refill = { preparedAdded: 0, prepareSkippedReason: null };
        try {
          refill = await refillEnvelope(program, source, day, envelope, counts);
          if (refill.envelope) envelope = refill.envelope;
        } catch (error) {
          refill = {
            preparedAdded: 0,
            prepareSkippedReason: error.code || error.message,
            pendingPrepared: 0,
          };
        }
        if (envelope.status === 'complete') {
          await store.health(program);
          const completeResult = { completed: true, envelopeId: envelope.id, sent: 0, ...refill };
          await recordTickEvaluated(program, day, completeResult);
          return completeResult;
        }
        const window = windowReason(program.policy, now(), true);
        if (window) fail(window);
        if (envelope.status === 'frozen') envelope = await bindApproval(program, envelope);
        if (envelope.status !== 'authorized') {
          const pendingAuth = { halted: envelope.status, envelopeId: envelope.id, sent: 0, ...refill };
          await recordTickEvaluated(program, day, pendingAuth);
          return pendingAuth;
        }
        if (counts.last_attempt && +now() - +new Date(counts.last_attempt) < program.policy.spacingMinutes * 60000) fail('spacing');
        const item = (await store.items(envelope.id)).find(x => x.status === 'pending');
        if (!item) {
          if (adapters.complete) await adapters.complete(envelope);
          await pool.query("UPDATE acquisition_outbound_envelopes SET status='complete' WHERE id=$1", [envelope.id]);
          await store.health(program);
          const drainedResult = { completed: true, envelopeId: envelope.id, sent: 0, ...refill };
          await recordTickEvaluated(program, day, drainedResult);
          return drainedResult;
        }
        await dispatch(program, envelope, item);
        if ((await store.items(envelope.id)).every(row => !['pending','attempted','uncertain'].includes(row.status))) {
          await pool.query("UPDATE acquisition_outbound_envelopes SET status='complete' WHERE id=$1", [envelope.id]);
        }
        await store.health(program);
        const sentResult = { envelopeId: envelope.id, itemId: item.id, sent: 1, ...refill };
        await recordTickEvaluated(program, day, sentResult);
        return sentResult;
      } catch (e) {
        const reason = e.code || e.message;
        await store.health(program, reason);
        await store.event('tick_blocked', [program.id, day, reason], { programId: program.id, reason });
        const blocked = { halted: reason, sent: 0 };
        await recordTickEvaluated(program, day, blocked);
        return blocked;
      }
    });
  }
  async function reconcileFromEvidence(itemId, opts = {}) {
    return reconcileUncertainItemFromEvidence(pool, resolvedTenantId, itemId, opts);
  }
  async function reconcile(itemId, outcome, providerMessageId, evidence, actor) {
    if (!actor?.id || !['admin', 'manager'].includes(actor.role)) fail('operator_required');
    if (!['accepted', 'not_accepted'].includes(outcome) || !String(evidence || '').trim()
      || (outcome === 'accepted' && !providerMessageId)) fail('reconciliation_evidence_required');
    return store.lock(async () => {
      const item = await store.one('SELECT * FROM acquisition_outbound_items WHERE id=$1', [itemId]);
      if (!item || !['attempted', 'uncertain'].includes(item.status)) fail('item_not_uncertain');
      if (outcome === 'accepted') {
        const envelope = await store.one('SELECT * FROM acquisition_outbound_envelopes WHERE id=$1', [item.envelope_id]);
        await pool.query(`UPDATE acquisition_mission_outbound_executions SET status='sent',provider_message_id=$3,
          sent_at=COALESCE(sent_at,attempted_at),updated_at=now() WHERE mission_id=$1 AND prospect_id=$2 AND status IN ('attempted','failed')`,
        [envelope.mission_id, item.candidate_id, providerMessageId]);
        await store.finish(item, 'sent', 'operator_reconciled', providerMessageId);
        await store.event('send_reconciled', [itemId, outcome], {
          itemId, outcome, providerMessageId, evidence, actor: String(actor.id),
          providerOutcome: 'PROVIDER_CONFIRMED_SENT',
        });
      } else {
        await store.releaseUnsent(item, 'reconciled_not_sent', {
          reconciled: true, evidence, actor: String(actor.id),
          providerOutcome: 'PROVIDER_CONFIRMED_NOT_SENT',
        });
      }
      return { itemId, outcome, retryAllowed: outcome !== 'accepted' };
    });
  }
  return {
    authorize,
    migrateOperatorDelegatedCapacity,
    setMode,
    tick,
    runPreparationRefill,
    reconcile,
    reconcileFromEvidence,
    initializePreparation,
    replenish,
    resumeReservedPreparation,
    status: () => store.status(),
    store,
  };
}

function productionService(pool, options = {}) {
  const db = pool || require('../db');
  const governedContext = createGovernedOutboundContext({
    tenantId: options.tenantId,
    governedContext: options.governedContext,
    program: options.program,
  });
  const now = options.now instanceof Date
    ? () => options.now
    : (typeof options.now === 'function' ? options.now : undefined);
  return service({
    pool: db,
    governedContext,
    tenantId: governedContext.tenantId,
    ...(now ? { now } : {}),
    adapters: require('./governedOutboundAdapters').adapters(db, { governedContext }),
  });
}
module.exports = { service, productionService };
