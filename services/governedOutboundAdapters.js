'use strict';

const amo = require('../packages/acquisition-mission');
const { hash, fail, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { unwrapSpecialistPayload } = require('../packages/acquisition-mission/ContributionSupersession');
const { loadMissionSnapshot } = require('./acquisitionMissionPersistence');
const { getAcquisitionMissionRuntime } = require('./acquisitionMissionRuntime');
const { resolveCanonicalSenderIdentity, evaluateCanonicalSenderReadiness } = require('../utils/canonicalSenderIdentity');
const { buildInboxSnapshot } = require('./emmettOutboundSnapshot');
const { createOutboundEngine, assessOperatingCapacity } = require('../packages/emmett-outbound');
const { loadBestCrmProspectForMissionBoundKey } = require('../packages/max/workspace/MissionBoundCrmResolver');
const { governedContactReason } = require('../utils/governedContactEligibility');
const { assertGovernedOutboundTenantId, createGovernedOutboundTenantContext } = require('./governedOutboundTenant');
const { buildTenantMailboxInboxSnapshot } = require('./emmettTenantMailboxSnapshot');
const { createGovernedTenantMailboxSend } = require('../utils/governedOutboundTransport');


// Count every known path; duplicate evidence conservatively consumes capacity.
async function readOutboundHistory(pool, tenantId, clientId, ignoreItem = null) {
  const { rows } = await pool.query(`SELECT count(*)::int AS today,max(attempted_at) AS last_attempt FROM (
      SELECT attempted_at FROM acquisition_mission_outbound_executions WHERE tenant_id=$3 AND status IN ('sent','attempted','failed')
        AND NOT (status='attempted' AND prospect_id=$1 AND prepared_artifact_revision=$2)
      UNION ALL SELECT ran_at FROM agent_log WHERE client_id=$4 AND agent_name='emmett' AND action='email_sent'
      UNION ALL SELECT sent_at FROM tenant_outreach_messages WHERE tenant_id=$3 AND direction='OUTBOUND' AND status='sent'
      ) evidence WHERE (attempted_at AT TIME ZONE 'America/New_York')::date=(now() AT TIME ZONE 'America/New_York')::date`,
  [ignoreItem?.candidate_id || '', ignoreItem?.snapshot?.revision || '', tenantId, clientId]);
  return rows[0];
}

function adapters(pool, dependencies = {}) {
  const ctx = createGovernedOutboundTenantContext(
    assertGovernedOutboundTenantId(dependencies.tenantId),
  );
  const tenantId = ctx.tenantId;
  const clientId = ctx.clientId;
  const loadMission = dependencies.loadMission || (id => loadMissionSnapshot(id, tenantId, pool));
  const contact = dependencies.contact || (id => loadBestCrmProspectForMissionBoundKey({ pool, clientId, missionBoundKey: id }));
  async function tenant(program) {
    if (dependencies.tenant) return dependencies.tenant(program);
    const { rows } = await pool.query('SELECT * FROM clients WHERE id=$1', [clientId]);
    const client = rows[0];
    if (!client || client.active !== true || client.autosend_enabled !== false) fail('tenant_inactive_or_legacy_autosend_enabled');
    const sender = await resolveCanonicalSenderIdentity({ tenantId, clientId, client, pool });
    if (!sender.ok || sender.identity.senderEmail.toLowerCase() !== program.policy.senderEmail) fail('sender_changed');
    const { PostgresTenantMailboxStore } = require('./tenantMailbox');
    const integration = await new PostgresTenantMailboxStore(pool).getIntegration(tenantId, program.policy.inboxIntegrationId);
    if (!integration || integration.status !== 'active' || integration.mailboxAddress.toLowerCase() !== program.policy.senderEmail) fail('anchor_reply_mailbox_not_ready');
    if (ctx.requiresAoOwners) {
      const owners = await pool.query('SELECT id FROM users WHERE client_id=$1 AND active=true AND id=ANY($2::int[])', [clientId, program.policy.aoOwnerIds]);
      if (!owners.rows.length) fail('ao_owner_unavailable');
    }
    const triggers = await pool.query(`SELECT count(*)::int AS n FROM pg_trigger
      WHERE tgname='acquisition_outbound_observe' AND tgenabled IN ('O','A') AND NOT tgisinternal`);
    if (triggers.rows[0]?.n !== 6) fail('suppression_triggers_missing');
    return { client, sender: sender.identity };
  }
  async function infrastructure(program, now = new Date(), ignoreItem = null, opts = {}) {
    if (dependencies.infrastructure) return dependencies.infrastructure(program, now, ignoreItem, opts);
    const { client, sender } = await tenant(program);
    if (ctx.requiresLegacyEmailTelemetry) {
      const readiness = await evaluateCanonicalSenderReadiness({ identity: sender, client, pool });
      if (!readiness.ready) fail(readiness.code || 'sender_not_ready');
    }
    let snapshot;
    if (ctx.requiresLegacyEmailTelemetry) {
      await pool.query('SELECT event_type,event_at,sender_identity_status FROM email_events WHERE client_id=$1 LIMIT 1', [clientId]);
      await pool.query('SELECT action,payload,ran_at FROM agent_log WHERE client_id=$1 LIMIT 1', [clientId]);
      snapshot = await buildInboxSnapshot(clientId, { pool, now });
    } else {
      snapshot = await buildTenantMailboxInboxSnapshot({
        tenantId,
        sendingIdentityId: program.policy.sendingIdentityId,
        mailboxIntegrationId: program.policy.inboxIntegrationId,
      }, { pool, now });
      if (!snapshot?.authentication?.smtp || snapshot.authentication.smtp.state === 'fail') {
        fail('tenant_mailbox_not_ready');
      }
    }
    if (ctx.usesTenantMailboxTransport) {
      const produced = await require('./emmettTenantMailboxCapacity').produceTenantMailboxCapacityEnvelope(
        tenantId, program.policy.sendingIdentityId, { pool, now });
      const envelope = produced.envelope;
      if (envelope.mailboxIntegrationId !== program.policy.inboxIntegrationId) fail('capacity_mailbox_changed');
      const assessed = envelope.emmettContribution;
      if (assessed.governor.halt || !['proceed', 'slow'].includes(envelope.governorState)) fail('emmett_governor_halted');
      const history = await readOutboundHistory(pool, tenantId, clientId, ignoreItem);
      const cap = Math.min(program.policy.dailyCap, envelope.maxSendsPerDay);
      if (!(cap > 0)) fail('emmett_capacity_exhausted');
      const inWindow = require('../packages/acquisition-mission/DailyOutboundPolicy').clock(now);
      const available = envelope.remainingCapacity > 0 || Boolean(ignoreItem);
      const dispatchNow = available && inWindow.hour >= envelope.allowedSendWindow.startHour
        && inWindow.hour < envelope.allowedSendWindow.endHour && inWindow.weekday > 0 && inWindow.weekday < 6 ? cap : 0;
      if (opts.mode === 'dispatch' && !dispatchNow) fail('dispatch_unavailable_now');
      Object.assign(snapshot, sender, { sentToday: envelope.currentSentCount, inboxId: sender.senderEmail, domain: sender.sendingDomain });
      const counts = await new (require('./governedOutboundStore').GovernedOutboundStore)(pool, tenantId).counts(program, inWindow.day);
      return { snapshot, assessed, cap, sender, envelope, totalAttempted: counts.total, lastAttempt: history.last_attempt,
        dispatchUnavailableNow: !dispatchNow,
        operating: { planningDailyCapacity: cap, dispatchCapacityNow: dispatchNow,
          effectiveDailyCapacity: cap, recommendedSafeDailyCapacity: envelope.maxSendsPerDay,
          governor: assessed.governor, healthScore: assessed.health?.score ?? null,
          allowedSendWindow: envelope.allowedSendWindow,
          minSpacingMinutes: Math.max(program.policy.spacingMinutes, envelope.minimumSpacingMinutes) } };
    }
    const history = await readOutboundHistory(pool, tenantId, clientId, ignoreItem);
    snapshot.sentToday = Math.max(snapshot.sentToday, history.today);
    snapshot.inboxId = sender.senderEmail;
    snapshot.domain = sender.sendingDomain;
    Object.assign(snapshot, sender);
    const assessed = createOutboundEngine().assess({ tenantId, snapshot, now });
    if (assessed.governor.halt || !['proceed', 'slow'].includes(assessed.governor.outcome)) fail('emmett_governor_halted');
    const programTotals = await pool.query(`SELECT count(*)::int AS total
      FROM acquisition_outbound_items i
      JOIN acquisition_outbound_envelopes e ON e.id=i.envelope_id
      WHERE e.program_id=$1 AND i.attempted_at IS NOT NULL`, [program.id]).catch(() => ({ rows: [{ total: 0 }] }));

    const grantWindow = {
      startHour: program.policy.startHour ?? 9,
      endHour: program.policy.endHour ?? 17,
      timezone: program.policy.timeZone || 'America/New_York',
    };
    const operating = assessOperatingCapacity({
      assessed,
      policy: program.policy,
      sentToday: snapshot.sentToday,
      totalAttempted: programTotals.rows[0]?.total || 0,
      now,
      schedule: {
        allowedSendWindow: grantWindow,
        minSpacingMinutes: program.policy.spacingMinutes ?? program.policy.minSpacingMinutes ?? 60,
      },
    });
    const mode = opts.mode || 'planning';
    const planningCap = operating.planningDailyCapacity;
    const dispatchNow = operating.dispatchCapacityNow;
    if (!Number.isFinite(planningCap) || planningCap <= 0) fail('emmett_capacity_exhausted');
    if (mode === 'dispatch') {
      if (!Number.isFinite(dispatchNow) || dispatchNow <= 0 || dispatchNow <= snapshot.sentToday) {
        fail('dispatch_unavailable_now');
      }
    }
    return {
      snapshot,
      assessed,
      cap: planningCap,
      operating,
      sender,
      lastAttempt: history.last_attempt,
      dispatchUnavailableNow: dispatchNow <= 0 || dispatchNow <= snapshot.sentToday,
    };
  }
  async function runtimeFor() {
    if (dependencies.runtime) return dependencies.runtime;
    const runtime = getAcquisitionMissionRuntime({ pool, persist: true });
    await runtime.hydrate(tenantId, { pool, persist: true });
    return runtime;
  }
  async function route(runtime, missionId, program, intent, extra = {}) {
    const engine = runtime.engine();
    const mission = engine.get(missionId, tenantId);
    const request = amo.createExecutionRequest({ source: amo.EXECUTION_SOURCES.API, intent,
      missionId, mission, stage: mission.stage, operatorId: program.authorized_by,
      permissions: { canExecute: true, role: 'operator' },
      payload: { question: `Bounded delegation ${program.id}: ${intent}`, maxSends: 1,
        ...(extra.prospectId ? { prospectId: extra.prospectId } : {}) } });
    const result = await amo.routeExecutionRequest(request, { engine, tenantId,
      pool: dependencies.persist === false ? undefined : pool, persist: dependencies.persist !== false,
      operatorId: program.authorized_by, allowFixtureFallback: false, ...extra });
    if (result.executionResult?.rolledBack) throw result.executionResult.error || new Error(result.executionResult.rollbackReason);
    return result;
  }
  async function scoutWithEligibility(mission, opts, program, store) {
    const runScoutForAmoMission = dependencies.runScout || require('../packages/max/workspace/ScoutDiscoveryExecutor').runScoutForAmoMission;
    const result = await runScoutForAmoMission(mission, { ...opts, pool, runScout: undefined, allowFixtureFallback: false });
    const payload = result.payload;
    const { buildMissionBoundCandidates } = require('../packages/max/workspace/EmmettMissionCandidates');
    const admission = dependencies.admission || require('../packages/max/workspace/MissionBoundCrmAdmission');
    const enrichProspectRow = dependencies.enrich || require('../scripts/lib/anchorMissionBoundEnrichment').enrichProspectRow;
    const candidates = buildMissionBoundCandidates(mission, [{ id: `discovery_${mission.id}`, missionId: mission.id,
      specialist: 'scout', kind: 'discovery', payload, createdAt: new Date().toISOString() }]);
    await admission.ensureMissionBoundCrmSchema(pool);
    const eligible = {};
    let attempts = 0;
    for (const candidate of candidates) {
      const id = String(candidate.id);
      if (attempts >= program.policy.enrichmentLimit) { eligible[id] = { eligible: false, reason: 'enrichment_budget' }; continue; }
      try {
        let crm = await contact(id);
        const ownership = store.candidateOwnership && await store.candidateOwnership(candidate);
        if (ownership || (crm && await store.suppression({ candidateId: id, prospectId: crm.prospect_id,
          companyId: crm.company_id, email: String(crm.email || '') }))) {
          eligible[id] = { eligible: false, reason: 'prior_contact_or_human_owned' }; continue;
        }
        if (ctx.usesTenantMailboxTransport && crm && !governedContactReason(crm, program.policy)) {
          eligible[id] = { eligible: true, reason: null, prospectId: crm.prospect_id };
          continue;
        }
        attempts++;
        const admitted = await admission.admitMissionBoundCandidate(pool, candidate, { missionId: mission.id, clientId, mission });
        if (admitted?.blocked) {
          eligible[id] = { eligible: false, reason: admitted.reason, detail: admitted.detail || null }; continue;
        }
        crm = await contact(id);
        if (crm) await enrichProspectRow(crm, { db: pool, dryRun: false });
        crm = await contact(id);
        const reason = governedContactReason(crm, program.policy)
          || await store.suppression({ candidateId: id, prospectId: crm.prospect_id,
            companyId: crm.company_id, email: crm.email });
        eligible[id] = { eligible: !reason, reason, prospectId: crm?.prospect_id || null };
      } catch (e) {
        // A conflicting company/contact (including Stewart duplicates) quarantines
        // that candidate without dropping the remainder of the replenishment run.
        eligible[id] = { eligible: false, reason: e.code || 'enrichment_failed' };
      }
    }
    payload.dailyOutboundEligibility = eligible;
    await store.event('inventory_replenished', [mission.id, hash(eligible)], { programId: program.id,
      missionId: mission.id, attempts, eligible: Object.values(eligible).filter(x => x.eligible).length, candidates: eligible });
    if (!Object.values(eligible).some(x => x.eligible)) fail('verified_inventory_shortfall');
    return result;
  }
  async function prepare(program, source, day, store, recovery = null) {
    if (ctx.usesTenantMailboxTransport && source?.mission?.stage === 'ready' && !recovery) {
      await prepared(source, program);
      return source;
    }
    const runtime = await runtimeFor();
    const engine = runtime.engine();
    const defaultMissionId = `mission_daily_${hash([program.id, day]).slice(0, 24)}`;
    const { progress } = await store.ensurePreparation(program, day);
    const missionId = progress.mission_id || defaultMissionId;
    let mission = engine.get(missionId, tenantId);
    if (mission?.stage === 'ready') return loadMission(missionId);
    if (recovery) {
      if (missionId !== recovery.nextMissionId || progress.attempts !== recovery.review.nextAttempt
        || progress.attempts > program.policy.preparationAttemptsPerDay || mission) fail('replenishment_reservation_changed');
    } else {
      // A crashed recovery cannot silently spend another attempt or lose its
      // reviewed research inputs through an ordinary scheduled tick.
      if (missionId !== defaultMissionId) fail('replenishment_requires_operator_review');
      if (progress.attempts >= program.policy.preparationAttemptsPerDay) fail('preparation_retry_budget');
      if (progress.last_attempt_at && Date.now() - +new Date(progress.last_attempt_at) < 60 * 60000) fail('preparation_backoff');
      await pool.query('UPDATE acquisition_outbound_preparation SET attempts=attempts+1,last_attempt_at=now() WHERE program_id=$1 AND local_day=$2', [program.id, day]);
    }
    try {
      if (!mission) {
        const input = { ...missionScope(source.mission), id: missionId, tenantId, clientId,
          title: `Governed daily outbound ${day}`, createdBy: 'max', orchestrationMissionId: source.mission.id };
        await runtime.create(input, { pool, persist: true });
      }
      const infra = await infrastructure(program);
      for (let steps = 0; steps < 8; steps++) {
        mission = engine.get(missionId, tenantId);
        if (mission.stage === 'ready') return loadMission(missionId);
        const snapshot = engine.inspect(missionId, { tenantId });
        const ctx = amo.specialistContext(snapshot.contributions || [], { missionId });
        let intent;
        if (mission.stage === 'discover') intent = amo.intentFromPendingDecision(mission.pendingOperatorDecision);
        else if (!ctx.maxComplete) fail('max_prioritization_missing');
        else if (!ctx.acquisitionApproachComplete) intent = amo.EXECUTION_INTENTS.DECIDE_ACQUISITION_APPROACH;
        else if (!ctx.paigeComplete) intent = amo.EXECUTION_INTENTS.GENERATE_VARIANTS;
        else intent = amo.EXECUTION_INTENTS.GENERATE_CAPACITY;
        const allowed = ['APPROVE_DISCOVERY','CONTINUE_INVESTIGATION','APPROVE_PRIORITIZATION','DECIDE_ACQUISITION_APPROACH','GENERATE_VARIANTS','GENERATE_CAPACITY'];
      if (!allowed.includes(intent)) fail('preparation_requires_operator_judgment');
        await route(runtime, missionId, program, intent, {
          infrastructureSnapshot: infra.snapshot,
          runEmmett: dependencies.runEmmett,
          runScout: (m, o) => scoutWithEligibility(m, { ...o,
            ...(recovery ? { scoutCompanies: require('./governedOutboundReplenishment').scoutCompanies(recovery.review.research) } : {}) }, program, store),
        });
      }
      fail('preparation_step_budget');
    } catch (e) {
      await pool.query('UPDATE acquisition_outbound_preparation SET last_error=$3 WHERE program_id=$1 AND local_day=$2', [program.id, day, e.code || e.message]);
      throw e;
    }
  }
  async function prepared(snapshot, program) {
    if (!snapshot?.mission || String(snapshot.mission.tenantId) !== tenantId
      || snapshot.mission.planCancelled || !['ready', 'execute'].includes(snapshot.mission.stage)) fail('mission_not_executable');
    if (hash(missionScope(snapshot.mission)) !== program.scope_hash) fail('daily_mission_scope_changed');
    const contributions = snapshot.contributions;
    const paige = amo.findPaigeVariants(contributions, snapshot.mission);
    const emmett = amo.findEmmettCapacity(contributions, snapshot.mission);
    if (!paige || !emmett) fail('missing_prepared_artifacts');
    const capacity = unwrapSpecialistPayload(emmett);
    const variants = unwrapSpecialistPayload(paige);
    if (!amo.validateProspectMessageBindings(capacity).valid) fail('message_binding_contamination');
    if (capacity.governor?.halt || !['proceed', 'slow'].includes(capacity.governor?.outcome)) fail('prepared_governor_halted');
    const { sender } = await tenant(program);
    const binding = require('../utils/canonicalSenderIdentity').assertCapacityMatchesCanonical(capacity, sender);
    if (!binding.ok) fail('capacity_sender_changed');
    if (ctx.usesTenantMailboxTransport) {
      const inventory = await require('./acquisitionMissionInventory').loadKnowledgeInventory(pool, snapshot.mission, program.policy);
      for (const item of capacity.queue?.items || []) {
        const row = inventory.find(r => [String(r.company_id), String(r.id)].includes(String(item.prospectId || item.id)));
        if (!row) continue;
        const copy = amo.resolvePaigeVariant(variants, { candidateId: item.paige?.candidateId || item.id, variantLabel: item.paige?.variantLabel || 'Primary' });
        if (item.paige?.subject !== copy?.subject || item.paige?.body !== copy?.body) fail('capacity_copy_binding_mismatch');
        const approved = row.approved_asset?.content;
        if (!approved || copy?.subject !== approved.subject || copy?.body !== (approved.body || approved.statement)) fail('approved_copy_binding_mismatch');
      }
    }
    return { sender, revision: amo.computePreparedArtifactRevision(snapshot.mission.id, contributions),
      capacity: Number(capacity.capacity?.recommended || 0),
      candidates: (capacity.queue?.items || []).map(item => ({ item,
        candidateId: String(item.prospectId || item.id),
        message: amo.resolvePaigeVariant(variants, { candidateId: item.paige?.candidateId || item.id,
          variantLabel: item.paige?.variantLabel || 'Primary' }) })) };
  }
  async function liveGate(program, item, _prepared, now) {
    const { rows } = await pool.query(`SELECT 1 FROM acquisition_outbound_inbox_health
      WHERE tenant_id=$1 AND integration_id=$2 AND last_success_at>now()-interval '5 minutes'`, [tenantId, program.policy.inboxIntegrationId]);
    if (!rows.length) fail('reply_poll_stale');
    const infra = await infrastructure(program, now, item, { mode: 'dispatch' });
    if (infra.lastAttempt && +now - +new Date(infra.lastAttempt) < Math.max(program.policy.spacingMinutes, infra.envelope?.minimumSpacingMinutes || 0) * 60000) {
      fail('cross_path_spacing');
    }
    if (await require('../dbClient').checkDNC(item.prospect_id, { clientId, pool })) fail('dnc');
  }
  function sendFor(program, binding = {}) {
    if (ctx.usesBrevoTransport) {
      const brevoSend = command => require('../packages/providers/brevo/sendEmail').sendEmail(command);
      brevoSend.beforeAttempt = null;
      return brevoSend;
    }
    return createGovernedTenantMailboxSend({ ...program, tenant_id: tenantId, pool }, binding);
  }
  return {
    tenantId,
    clientId,
    loadMission, contact, infrastructure, prepare, prepared, liveGate, validateTenant: tenant, sendFor,
    complete: async envelope => {
      const runtime = await runtimeFor();
      const engine = runtime.engine();
      const mission = engine.get(envelope.mission_id, tenantId);
      if (!mission || !mission.executionSummary || mission.executionSummary.complete) return;
      mission.executionSummary.complete = true;
      engine.store.putMission(mission);
      await runtime.persistMissionState(mission.id, { pool, persist: true });
    },
    send: command => require('../packages/providers/brevo/sendEmail').sendEmail(command),
    approve: async (snapshot, program, binding) => {
      const runtime = await runtimeFor();
      const result = await route(runtime, snapshot.mission.id, program, amo.EXECUTION_INTENTS.APPROVE_EXECUTION, { governedApproval: binding });
      return result.executionResult?.approval;
    },
    execute: async (envelope, item, program, sendEmail) => {
      const runtime = await runtimeFor();
      const transport = sendEmail || sendFor(program);
      return route(runtime, envelope.mission_id, program, amo.EXECUTION_INTENTS.EXECUTE_OUTBOUND,
        { governedEnvelopeId: envelope.id, maxSends: 1, prospectId: item.candidate_id,
          governedManifestCandidateIds: (envelope.manifest || []).map(row => String(row.candidateId || '')).filter(Boolean),
          governedRefillItem: item.snapshot?.refill === true ? item : null,
          sendEmail: transport, requireProviderReadiness: ctx.requiresLegacyEmailTelemetry });
    },
  };
}
module.exports = { adapters, readOutboundHistory };
