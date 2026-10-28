'use strict';
// SPEC-SCOUT-P0-003 research mission. An immutable, separately approved,
// research-only copy of the already approved facility-buyer targeting
// (managed facilities + restaurant FOH across the eight Anchor service cities).
// It lives only in scout_research_missions: acquisition_missions and the active
// program are never written, so the outbound mission, its prepared envelope and
// the successor-activation guard are untouched. Discoveries made under it are
// retained in scout_research_candidates, outside every enrichment, inventory,
// preparation and send path, until an explicit reconciliation.
const { hash } = require('./scoutBoundedAcquisitionBudget');

const ID = /^mission_anchor_facility_research_[a-f0-9]{20}$/;
const HEX64 = /^[a-f0-9]{64}$/;
const REF = /^[a-zA-Z0-9_.:-]{1,120}$/;
const PROGRAM_COLUMNS = 'id,tenant_id,source_mission_id,policy,policy_hash,scope_hash,authorized_by,authorized_at,mode';

function fail(code, reason) {
  throw Object.assign(new Error(reason ? `${code}:${reason}` : code), { code, reason: reason || code, retryAllowed: false });
}
function isoTime(value) {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)
    && new Date(Date.parse(value)).toISOString() === value;
}
function facilityBuyerTargeting() {
  // The single approved source of the facility-buyer ICP; never a second copy.
  return require('../scripts/activateAnchorFacilityBuyerMission');
}
// What discovery searches and admits. Equal targeting = same approved ICP.
function targeting(payload) {
  const structured = payload?.structuredMission || {};
  return { segment: structured.market?.segment ?? null, industry: structured.market?.industry ?? null,
    eligibleSubsegments: structured.market?.eligibleSubsegments ?? null,
    retiredSubsegments: structured.market?.retiredSubsegments ?? null,
    cities: structured.geography?.cities ?? null, region: structured.geography?.region ?? null,
    scope: structured.geography?.scope ?? null };
}
function targetingHash(payload) { return hash(targeting(payload)); }

// Deterministic for a given predecessor and staging time.
function buildResearchMission(predecessor, stagedAt) {
  if (!isoTime(stagedAt)) fail('research_mission_invalid', 'staged_at');
  const { facilityBuyerSuccessorMission, ELIGIBLE_SUBSEGMENTS, SERVICE_CITIES } = facilityBuyerTargeting();
  const successor = facilityBuyerSuccessorMission(predecessor);
  const id = `mission_anchor_facility_research_${hash(['scout_research_only', successor.id]).slice(0, 20)}`;
  const payload = { ...successor, id, researchOnly: true, outboundEligible: false, targetSuccessorMissionId: successor.id,
    createdBy: 'operator_authorized_research_only_facility_buyer', createdAt: stagedAt, updatedAt: stagedAt,
    authorizationEvidence: { ...successor.authorizationEvidence, kind: 'admin_session_approval', receivedAt: stagedAt,
      scope: `Research-only discovery under the approved facility-buyer ICP. No enrichment, preparation or outbound. ${successor.authorizationEvidence?.scope || ''}`.trim() } };
  const market = payload.structuredMission?.market || {}, geography = payload.structuredMission?.geography || {};
  if (JSON.stringify(market.eligibleSubsegments) !== JSON.stringify([...ELIGIBLE_SUBSEGMENTS])
    || JSON.stringify(geography.cities) !== JSON.stringify([...SERVICE_CITIES]) || payload.structuredMission?.immutable !== true)
    fail('research_mission_invalid', 'targeting');
  return payload;
}
function row(payload) { return { id: payload.id, objective: payload.objective, target_segment: payload.targetSegment ?? null, payload }; }
// Discovery-source shape used by sourceScope/scoutInput, hashed for the grant.
function researchSource(stored) { return row(stored.payload); }
function researchProgram(program, mission) { return { ...program, source_mission_id: mission.id }; }

async function loadPredecessor(db) {
  const program = (await db.query(`SELECT ${PROGRAM_COLUMNS} FROM acquisition_outbound_programs
    WHERE tenant_id='10' AND mode<>'revoked' ORDER BY authorized_at DESC LIMIT 1`)).rows[0];
  if (!program) fail('research_mission_program_missing');
  const source = (await db.query('SELECT * FROM acquisition_missions WHERE tenant_id=$1 AND id=$2', ['10', program.source_mission_id])).rows[0];
  if (!source) fail('research_mission_predecessor_missing');
  return { program, source };
}

async function transaction(pool, work, { readOnly = false } = {}) {
  const db = await pool.connect(); let committing = false, discard = false;
  try {
    await db.query(readOnly ? 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY' : 'BEGIN');
    await db.query("SET LOCAL lock_timeout='2s'");
    await db.query("SET LOCAL statement_timeout='15s'");
    const result = await work(db);
    committing = true; await db.query('COMMIT'); committing = false;
    return result;
  } catch (failure) {
    try { await db.query('ROLLBACK'); } catch { discard = true; }
    if (committing) fail('research_mission_commit_uncertain');
    throw failure;
  } finally { db.release(discard || committing); }
}

async function assertSchema(db) {
  try {
    await db.query(`SELECT id,tenant_id,program_id,predecessor_mission_id,target_successor_mission_id,approval_ref,payload,mission_hash,
      targeting_hash,operator_id,approver_session_hash,created_at FROM scout_research_missions LIMIT 0`);
    await db.query(`SELECT id,tenant_id,research_mission_id,grant_id,cycle_number,company,name_key,domain,website_url,vertical,location,notes,
      discovered_at,reconciled_at,reconciled_mission_id,reconciled_queue_id,reconcile_approval_ref FROM scout_research_candidates LIMIT 0`);
  } catch { fail('research_mission_schema_unavailable'); }
  const guards = (await db.query(`SELECT count(*)::int AS count FROM pg_trigger WHERE NOT tgisinternal AND tgenabled='O'
    AND tgname = ANY($1::text[])`, [['scout_research_missions_no_delete', 'scout_research_missions_no_update',
    'scout_research_candidates_no_delete', 'scout_research_candidates_reconcile']])).rows[0];
  if (guards?.count !== 4) fail('research_mission_schema_guards_missing');
}

function assertAdmin(actor) {
  if (!actor || actor.role !== 'admin' || !/^[1-9][0-9]{0,18}$/.test(String(actor.id || ''))) fail('research_mission_operator_invalid');
}
function approver(actor, accountClientId) {
  return { operatorId: String(actor.id), operatorRole: 'admin', preparationAuthority: {
    approverSessionHash: require('./anchorCrmPreparationIdentity').sessionBindingHash(actor.sessionId), approverAccountClientId: accountClientId } };
}
async function accountClient(db, actor) {
  const account = (await db.query('SELECT client_id FROM users WHERE id::text=$1', [String(actor.id)])).rows[0];
  if (!account) fail('research_mission_operator_invalid');
  return account.client_id == null ? null : Number(account.client_id);
}

// Read-only. Returns the exact mission document an admin would approve.
async function previewResearchMission(pool, input, actor) {
  if (!input || Object.keys(input).join() !== 'approvalRef' || !REF.test(input.approvalRef || '')) fail('research_mission_preview_invalid');
  assertAdmin(actor);
  return transaction(pool, async db => {
    await assertSchema(db);
    const { program, source } = await loadPredecessor(db);
    const now = (await db.query('SELECT clock_timestamp() AS now')).rows[0].now;
    const payload = buildResearchMission(source, new Date(Math.floor(+new Date(now) / 1000) * 1000).toISOString());
    const blockers = [];
    try { await require('./anchorCrmPreparationIdentity').assertPreparationApprover(db, approver(actor, await accountClient(db, actor)),
      { lock: false, sessionId: actor.sessionId }); } catch { blockers.push('research_mission_operator_invalid'); }
    const existing = (await db.query("SELECT id,mission_hash FROM scout_research_missions WHERE tenant_id='10' AND id=$1", [payload.id])).rows[0];
    return { mission: payload, missionHash: hash(payload), targetingHash: targetingHash(payload), programId: program.id,
      predecessorMissionId: source.id, targetSuccessorMissionId: payload.targetSuccessorMissionId,
      eligibleSubsegments: payload.structuredMission.market.eligibleSubsegments, cities: payload.structuredMission.geography.cities,
      status: existing ? 'already_installed' : blockers.length ? 'blocked' : 'previewed', installedMissionHash: existing?.mission_hash || null,
      blockers, outboundEligible: false, previewOnly: true };
  }, { readOnly: true });
}

async function installResearchMission(pool, input, actor) {
  if (!input || Object.keys(input).sort().join() !== 'approvalRef,mission,missionHash' || !REF.test(input.approvalRef || '')
    || !HEX64.test(input.missionHash || '')) fail('research_mission_explicit_approval_required');
  assertAdmin(actor);
  if (hash(input.mission) !== input.missionHash) fail('research_mission_exact_confirmation_required');
  return transaction(pool, async db => {
    await assertSchema(db);
    const accountClientId = await accountClient(db, actor);
    await require('./anchorCrmPreparationIdentity').assertPreparationApprover(db, approver(actor, accountClientId), { sessionId: actor.sessionId });
    const { program, source } = await loadPredecessor(db);
    // Rebuild from the live predecessor at the approved staging time: the
    // document must be exactly the approved facility-buyer research mission.
    const expected = buildResearchMission(source, input.mission?.createdAt);
    if (hash(expected) !== input.missionHash || !ID.test(expected.id)) fail('research_mission_changed');
    const existing = (await db.query("SELECT * FROM scout_research_missions WHERE tenant_id='10' AND (id=$1 OR approval_ref=$2)",
      [expected.id, input.approvalRef])).rows;
    if (existing.length) {
      if (existing.length === 1 && existing[0].id === expected.id && existing[0].mission_hash === input.missionHash
        && existing[0].approval_ref === input.approvalRef) return { missionId: expected.id, missionHash: input.missionHash, installed: false, idempotent: true };
      fail('research_mission_conflict');
    }
    await db.query(`INSERT INTO scout_research_missions(id,tenant_id,program_id,predecessor_mission_id,target_successor_mission_id,
      approval_ref,payload,mission_hash,targeting_hash,operator_id,approver_session_hash)
      VALUES($1,'10',$2,$3,$4,$5,$6,$7,$8,$9,$10)`, [expected.id, program.id, source.id, expected.targetSuccessorMissionId,
      input.approvalRef, JSON.stringify(expected), input.missionHash, targetingHash(expected), String(actor.id),
      require('./anchorCrmPreparationIdentity').sessionBindingHash(actor.sessionId)]);
    return { missionId: expected.id, missionHash: input.missionHash, targetingHash: targetingHash(expected), installed: true,
      idempotent: false, outboundEligible: false };
  });
}

// Verifies a stored research mission is intact and still the research scope
// of this program and its unchanged outbound predecessor mission.
async function loadResearchMission(db, { id, missionHash, program, lock = true }) {
  const stored = (await db.query(`SELECT * FROM scout_research_missions WHERE tenant_id='10' AND id=$1${lock ? ' FOR SHARE' : ''}`, [id])).rows[0];
  if (!stored || stored.mission_hash !== missionHash || hash(stored.payload) !== missionHash || stored.payload.id !== id
    || stored.payload.researchOnly !== true || stored.payload.outboundEligible !== false || !ID.test(id)
    || stored.targeting_hash !== targetingHash(stored.payload)) fail('research_mission_changed');
  if (program && (stored.program_id !== program.id || stored.predecessor_mission_id !== program.source_mission_id))
    fail('research_mission_changed', 'program_or_predecessor');
  return stored;
}

// The single write point for research-only discoveries. Called from inside the
// existing admission transaction instead of the scout_unenriched insert.
// A company already queued for enrichment or already retained is not copied.
async function retainCandidate(db, sink, candidate) {
  const nameKey = String(candidate.company || '').trim().toLowerCase();
  return db.query(`INSERT INTO scout_research_candidates(tenant_id,research_mission_id,grant_id,cycle_number,company,name_key,domain,
      website_url,vertical,location,notes)
    SELECT '10',$1,$2,$3,$4,$5,$6,$7,$8,$9,$10
    WHERE NOT EXISTS (SELECT 1 FROM scout_unenriched u WHERE u.client_id=10
      AND (lower(u.domain)=lower($6) OR lower(trim(u.company))=$5))
    ON CONFLICT DO NOTHING RETURNING id`,
  [sink.researchMissionId, sink.grantId, sink.cycleNumber, String(candidate.company).trim(), nameKey, candidate.domain,
    candidate.websiteUrl, candidate.vertical, candidate.location, candidate.notes]);
}

// Reconciliation: an explicit operator step, never automatic. It only adds
// retained candidates to the enrichment queue of an authorized mission whose
// targeting equals the research mission's, after recovery maintenance ends.
// It creates no prospect, companies row, envelope or send; ordinary Scout
// enrichment and every outbound gate still apply afterwards. One-time per row.
async function planReconciliation(db, { researchMissionId, env = process.env, now = new Date() }) {
  const state = await require('./anchorEmailRecovery').loadRecoveryState(db, { env, now });
  const blockers = [];
  if (state.mode === 'maintenance') blockers.push('research_reconcile_recovery_maintenance_active');
  const mission = (await db.query("SELECT * FROM scout_research_missions WHERE tenant_id='10' AND id=$1", [researchMissionId])).rows[0];
  if (!mission || hash(mission.payload) !== mission.mission_hash) fail('research_mission_changed');
  const program = (await db.query(`SELECT ${PROGRAM_COLUMNS} FROM acquisition_outbound_programs
    WHERE tenant_id='10' AND mode<>'revoked' ORDER BY authorized_at DESC LIMIT 1`)).rows[0];
  const target = program && (await db.query('SELECT id,payload FROM acquisition_missions WHERE tenant_id=$1 AND id=$2',
    ['10', program.source_mission_id])).rows[0];
  if (!target) blockers.push('research_reconcile_target_mission_missing');
  else if (targetingHash(target.payload) !== mission.targeting_hash) blockers.push('research_reconcile_target_scope_mismatch');
  if ((await db.query("SELECT 1 FROM scout_research_only_grants WHERE tenant_id='10' AND revoked_at IS NULL")).rows.length)
    blockers.push('research_reconcile_grant_still_open');
  const pending = (await db.query(`SELECT id,company,domain,website_url,vertical,location,notes FROM scout_research_candidates
    WHERE tenant_id='10' AND research_mission_id=$1 AND reconciled_at IS NULL ORDER BY id`, [researchMissionId])).rows;
  const plan = { researchMissionId, researchMissionHash: mission.mission_hash, targetMissionId: target?.id || null,
    targetProgramId: program?.id || null, candidateIds: pending.map(c => String(c.id)) };
  return { ...plan, planHash: hash(plan), candidates: pending.length, blockers, recoveryMode: state.mode };
}
async function previewReconciliation(pool, input) {
  return transaction(pool, db => planReconciliation(db, input), { readOnly: true });
}
async function applyReconciliation(pool, { researchMissionId, approvalRef, confirm, env = process.env, now = new Date() }) {
  if (!REF.test(approvalRef || '') || !HEX64.test(confirm || '')) fail('research_reconcile_explicit_approval_required');
  return transaction(pool, async db => {
    await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [`scout-research-reconcile:10:${researchMissionId}`]);
    const plan = await planReconciliation(db, { researchMissionId, env, now });
    if (plan.blockers.length) fail(plan.blockers[0]);
    if (plan.planHash !== confirm) fail('research_reconcile_plan_changed');
    const { GovernedOutboundStore } = require('./governedOutboundStore');
    const { classifyInventoryOwnership, OWNERSHIP_KINDS } = require('./outboundInventory');
    const store = new GovernedOutboundStore({ query: (sql, values) => db.query(sql, values) }, '10');
    const outcome = { queued: 0, alreadyQueued: 0, ownershipHeld: 0 };
    for (const candidate of (await db.query(`SELECT * FROM scout_research_candidates WHERE tenant_id='10' AND research_mission_id=$1
      AND reconciled_at IS NULL AND id::text=ANY($2::text[]) ORDER BY id FOR UPDATE`, [researchMissionId, plan.candidateIds])).rows) {
      // Ownership can change between discovery and reconciliation: re-check.
      const ownership = await classifyInventoryOwnership(store, { company: candidate.company, domain: candidate.domain,
        website: candidate.website_url }, { pool: { query: (sql, values) => db.query(sql, values) }, clientId: 10 });
      if (ownership.kind !== OWNERSHIP_KINDS.CLEAR) { outcome.ownershipHeld += 1; continue; }
      for (const key of [`10:domain:${candidate.domain.toLowerCase()}`, `10:name:${candidate.name_key}`].sort())
        await db.query('SELECT pg_advisory_xact_lock(261023,hashtext($1))', [key]);
      const queued = await db.query(`INSERT INTO scout_unenriched(client_id,company,website_url,domain,vertical,location,source,
        enrichment_attempts,last_attempt_at,notes)
        SELECT 10,$1,$2,$3,$4,$5,'max_buffer_replenishment',0,NULL,$6
        WHERE NOT EXISTS (SELECT 1 FROM scout_unenriched u WHERE u.client_id=10
          AND (lower(u.domain)=lower($3) OR lower(trim(u.company))=lower(trim($1)))) RETURNING id`,
      [candidate.company, candidate.website_url || `https://${candidate.domain}`, candidate.domain, candidate.vertical, candidate.location,
        `${candidate.notes || ''} | reconciled from research mission ${researchMissionId}`.trim()]);
      if (queued.rows.length) outcome.queued += 1; else outcome.alreadyQueued += 1;
      await db.query(`UPDATE scout_research_candidates SET reconciled_at=clock_timestamp(),reconciled_mission_id=$2,reconciled_queue_id=$3,
        reconcile_approval_ref=$4 WHERE id=$1 AND reconciled_at IS NULL`, [candidate.id, plan.targetMissionId, queued.rows[0]?.id ?? null, approvalRef]);
    }
    return { ...plan, ...outcome, applied: true, prospectsCreated: 0, preparationInvoked: false, sent: 0 };
  });
}

module.exports = { ID, targeting, targetingHash, buildResearchMission, researchSource, researchProgram, assertSchema,
  previewResearchMission, installResearchMission, loadResearchMission, retainCandidate,
  previewReconciliation, applyReconciliation };
