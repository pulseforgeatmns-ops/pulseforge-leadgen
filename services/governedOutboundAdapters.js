'use strict';

const amo = require('../packages/acquisition-mission');
const { hash, fail, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { unwrapSpecialistPayload } = require('../packages/acquisition-mission/ContributionSupersession');
const { loadMissionSnapshot } = require('./acquisitionMissionPersistence');
const { getAcquisitionMissionRuntime } = require('./acquisitionMissionRuntime');
const { resolveCanonicalSenderIdentity, evaluateCanonicalSenderReadiness } = require('../utils/canonicalSenderIdentity');
const { buildInboxSnapshot } = require('./emmettOutboundSnapshot');
const { createOutboundEngine } = require('../packages/emmett-outbound');
const { loadBestCrmProspectForMissionBoundKey } = require('../packages/max/workspace/MissionBoundCrmResolver');
const { canonicalOutboundEmailIneligibilityReason } = require('../utils/canonicalEmailEligibility');


// Count every known path; duplicate evidence conservatively consumes capacity.
async function readOutboundHistory(pool, ignoreItem = null) {
  const { rows } = await pool.query(`SELECT count(*)::int AS today,max(attempted_at) AS last_attempt FROM (
      SELECT attempted_at FROM acquisition_mission_outbound_executions WHERE tenant_id='10' AND status IN ('sent','attempted','failed')
        AND NOT (status='attempted' AND prospect_id=$1 AND prepared_artifact_revision=$2)
      UNION ALL SELECT ran_at FROM agent_log WHERE client_id=10 AND agent_name='emmett' AND action='email_sent'
      UNION ALL SELECT sent_at FROM tenant_outreach_messages WHERE tenant_id='10' AND direction='outbound' AND status='sent'
      ) evidence WHERE (attempted_at AT TIME ZONE 'America/New_York')::date=(now() AT TIME ZONE 'America/New_York')::date`,
  [ignoreItem?.candidate_id || '', ignoreItem?.snapshot?.revision || '']);
  return rows[0];
}

function adapters(pool, dependencies = {}) {
  const loadMission = dependencies.loadMission || (id => loadMissionSnapshot(id, '10', pool));
  const contact = dependencies.contact || (id => loadBestCrmProspectForMissionBoundKey({ pool, clientId: 10, missionBoundKey: id }));
  async function tenant(program) {
    if (dependencies.tenant) return dependencies.tenant(program);
    const { rows } = await pool.query('SELECT * FROM clients WHERE id=10');
    const client = rows[0];
    if (!client || client.active !== true || client.autosend_enabled !== false) fail('tenant_inactive_or_legacy_autosend_enabled');
    const sender = await resolveCanonicalSenderIdentity({ tenantId: '10', clientId: 10, client });
    if (!sender.ok || sender.identity.senderEmail.toLowerCase() !== program.policy.senderEmail) fail('sender_changed');
    const { PostgresTenantMailboxStore } = require('./tenantMailbox');
    const integration = await new PostgresTenantMailboxStore(pool).getIntegration('10', program.policy.inboxIntegrationId);
    if (!integration || integration.status !== 'active' || integration.mailboxAddress.toLowerCase() !== program.policy.senderEmail) fail('anchor_reply_mailbox_not_ready');
    const owners = await pool.query('SELECT id FROM users WHERE client_id=10 AND active=true AND id=ANY($1::int[])', [program.policy.aoOwnerIds]);
    if (!owners.rows.length) fail('ao_owner_unavailable');
    const triggers = await pool.query(`SELECT count(*)::int AS n FROM pg_trigger
      WHERE tgname='acquisition_outbound_observe' AND tgenabled IN ('O','A') AND NOT tgisinternal`);
    if (triggers.rows[0]?.n !== 6) fail('suppression_triggers_missing');
    return { client, sender: sender.identity };
  }
  async function infrastructure(program, now = new Date(), ignoreItem = null) {
    if (dependencies.infrastructure) return dependencies.infrastructure(program, now);
    const { client, sender } = await tenant(program);
    const readiness = await evaluateCanonicalSenderReadiness({ identity: sender, client, pool });
    if (!readiness.ready) fail(readiness.code || 'sender_not_ready');
    // The shared snapshot tolerates missing legacy tables. Governed execution
    // must prove telemetry is readable before consuming that snapshot.
    await pool.query('SELECT event_type,event_at,sender_identity_status FROM email_events WHERE client_id=10 LIMIT 1');
    await pool.query('SELECT action,payload,ran_at FROM agent_log WHERE client_id=10 LIMIT 1');
    const snapshot = await buildInboxSnapshot(10, { pool, now });
    const history = await readOutboundHistory(pool, ignoreItem);
    snapshot.sentToday = Math.max(snapshot.sentToday, history.today);
    snapshot.inboxId = sender.senderEmail;
    snapshot.domain = sender.sendingDomain;
    Object.assign(snapshot, sender);
    const assessed = createOutboundEngine().assess({ tenantId: '10', snapshot, now });
    if (assessed.governor.halt || !['proceed', 'slow'].includes(assessed.governor.outcome)) fail('emmett_governor_halted');
    const cap = Math.min(assessed.capacity.recommended, assessed.governor.slowCap || Infinity, program.policy.dailyCap);
    if (!Number.isFinite(cap) || cap <= snapshot.sentToday) fail('emmett_capacity_exhausted');
    return { snapshot, assessed, cap, sender, lastAttempt: history.last_attempt };
  }
  async function runtimeFor() {
    if (dependencies.runtime) return dependencies.runtime;
    const runtime = getAcquisitionMissionRuntime({ pool, persist: true });
    await runtime.hydrate('10', { pool, persist: true });
    return runtime;
  }
  async function route(runtime, missionId, program, intent, extra = {}) {
    const engine = runtime.engine();
    const mission = engine.get(missionId, '10');
    const request = amo.createExecutionRequest({ source: amo.EXECUTION_SOURCES.API, intent,
      missionId, mission, stage: mission.stage, operatorId: program.authorized_by,
      permissions: { canExecute: true, role: 'operator' },
      payload: { question: `Bounded delegation ${program.id}: ${intent}`, maxSends: 1,
        ...(extra.prospectId ? { prospectId: extra.prospectId } : {}) } });
    const result = await amo.routeExecutionRequest(request, { engine, tenantId: '10',
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
        if (crm?.email && await store.suppression({ candidateId: id, prospectId: crm.prospect_id,
          companyId: crm.company_id, email: crm.email })) {
          eligible[id] = { eligible: false, reason: 'prior_contact_or_human_owned' }; continue;
        }
        attempts++;
        await admission.admitMissionBoundCandidate(pool, candidate, { missionId: mission.id, clientId: 10, mission });
        crm = await contact(id);
        if (crm) await enrichProspectRow(crm, { db: pool, dryRun: false });
        crm = await contact(id);
        const reason = canonicalOutboundEmailIneligibilityReason(crm);
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
  async function prepare(program, source, day, store) {
    const runtime = await runtimeFor();
    const engine = runtime.engine();
    const missionId = `mission_daily_${hash([program.id, day]).slice(0, 24)}`;
    await pool.query(`INSERT INTO acquisition_outbound_preparation(program_id,local_day,mission_id)
      VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, [program.id, day, missionId]);
    const progress = await store.one('SELECT * FROM acquisition_outbound_preparation WHERE program_id=$1 AND local_day=$2', [program.id, day]);
    let mission = engine.get(missionId, '10');
    if (mission?.stage === 'ready') return loadMission(missionId);
    if (progress.attempts >= program.policy.preparationAttemptsPerDay) fail('preparation_retry_budget');
    if (progress.last_attempt_at && Date.now() - +new Date(progress.last_attempt_at) < 60 * 60000) fail('preparation_backoff');
    await pool.query('UPDATE acquisition_outbound_preparation SET attempts=attempts+1,last_attempt_at=now() WHERE program_id=$1 AND local_day=$2', [program.id, day]);
    try {
      if (!mission) {
        const input = { ...missionScope(source.mission), id: missionId, tenantId: '10', clientId: 10,
          title: `Anchor daily outbound ${day}`, createdBy: 'max', orchestrationMissionId: source.mission.id };
        await runtime.create(input, { pool, persist: true });
      }
      const infra = await infrastructure(program);
      for (let steps = 0; steps < 8; steps++) {
        mission = engine.get(missionId, '10');
        if (mission.stage === 'ready') return loadMission(missionId);
        const snapshot = engine.inspect(missionId, { tenantId: '10' });
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
          runScout: (m, o) => scoutWithEligibility(m, o, program, store),
        });
      }
      fail('preparation_step_budget');
    } catch (e) {
      await pool.query('UPDATE acquisition_outbound_preparation SET last_error=$3 WHERE program_id=$1 AND local_day=$2', [program.id, day, e.code || e.message]);
      throw e;
    }
  }
  async function prepared(snapshot, program) {
    if (!snapshot?.mission || String(snapshot.mission.tenantId) !== '10'
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
    return { sender, revision: amo.computePreparedArtifactRevision(snapshot.mission.id, contributions),
      capacity: Number(capacity.capacity?.recommended || 0),
      candidates: (capacity.queue?.items || []).map(item => ({ item,
        candidateId: String(item.prospectId || item.id),
        message: amo.resolvePaigeVariant(variants, { candidateId: item.paige?.candidateId || item.id,
          variantLabel: item.paige?.variantLabel || 'Primary' }) })) };
  }
  async function liveGate(program, item, _prepared, now) {
    const { rows } = await pool.query(`SELECT 1 FROM acquisition_outbound_inbox_health
      WHERE tenant_id='10' AND integration_id=$1 AND last_success_at>now()-interval '5 minutes'`, [program.policy.inboxIntegrationId]);
    if (!rows.length) fail('reply_poll_stale');
    const infra = await infrastructure(program, now, item);
    if (infra.lastAttempt && +now - +new Date(infra.lastAttempt) < program.policy.spacingMinutes * 60000) {
      fail('cross_path_spacing');
    }
    if (await require('../dbClient').checkDNC(item.prospect_id, { clientId: 10, pool })) fail('dnc');
  }
  return {
    loadMission, contact, prepare, prepared, liveGate, validateTenant: tenant,
    complete: async envelope => {
      const runtime = await runtimeFor();
      const engine = runtime.engine();
      const mission = engine.get(envelope.mission_id, '10');
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
      return route(runtime, envelope.mission_id, program, amo.EXECUTION_INTENTS.EXECUTE_OUTBOUND,
        { governedEnvelopeId: envelope.id, maxSends: 1, prospectId: item.candidate_id,
          sendEmail, requireProviderReadiness: true });
    },
  };
}
module.exports = { adapters, readOutboundHistory };
