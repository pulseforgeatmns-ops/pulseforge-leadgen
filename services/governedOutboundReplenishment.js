'use strict';

const { hash, fail, missionScope, clock, windowReason } = require('../packages/acquisition-mission/DailyOutboundPolicy');

function researchCompanies(input, source, now) {
  if (source.structuredMission?.market?.segment !== 'short_term_rental') fail('replenishment_str_scope_required');
  if (!Array.isArray(input) || input.length < 1 || input.length > 5) fail('replenishment_research_required');
  const cities = source.structuredMission?.geography?.cities || [];
  const seen = new Set();
  return input.map(row => {
    const website = new URL(row.website);
    const domain = website.hostname.toLowerCase().replace(/^www\./, '');
    if (website.protocol !== 'https:' || website.username || website.password || website.port
      || !domain.includes('.') || seen.has(domain)) fail('invalid_research_website');
    seen.add(domain);
    const city = cities.find(city => city.toLowerCase() === String(row.operatingCity || '').trim().toLowerCase());
    if (!city || !String(row.name || '').trim() || !String(row.headquarters || '').trim()) fail('research_scope_evidence_required');
    if (!Array.isArray(row.evidence) || row.evidence.length < 2 || row.evidence.length > 6) fail('research_evidence_required');
    const kinds = new Set();
    const evidence = row.evidence.map(item => {
      const url = new URL(item.url);
      if (url.protocol !== 'https:' || url.hostname.toLowerCase().replace(/^www\./, '') !== domain
        || url.username || url.password || url.port || !['property', 'services', 'contact'].includes(item.kind)
        || !String(item.summary || '').trim() || String(item.summary).length > 1000) fail('invalid_research_evidence');
      const age = +now - Date.parse(item.observedAt);
      if (!Number.isFinite(age) || age < -60000 || age > 7 * 86400000) fail('research_evidence_stale');
      kinds.add(item.kind);
      return { kind: item.kind, url: url.href, summary: String(item.summary).trim(), observedAt: new Date(item.observedAt).toISOString() };
    });
    if (!kinds.has('property') || !kinds.has('services')) fail('research_property_and_services_required');
    // These are research inputs, not verified recipients or qualified candidates.
    // Scout retains responsibility for fit, buying signals and evidence thresholds.
    return { name: String(row.name).trim(), domain, website: website.href, operatingCity: city,
      headquarters: String(row.headquarters).trim(), evidence };
  });
}

function scoutCompanies(research) {
  return research.map(row => ({ id: row.domain, tenantId: '10', name: row.name,
    website: row.website, industry: 'short_term_rental',
    description: row.evidence.map(item => item.summary).join(' '), source: 'operator_research',
    location: `${row.operatingCity}, NH (managed property; headquarters: ${row.headquarters})`,
    signals: [], evidence: row.evidence.map(item => ({ ...item, source: 'operator_research' })),
    updatedAt: row.evidence.map(item => item.observedAt).sort().at(-1) }));
}

function project(row) {
  if (!row) fail('replenishment_mission_missing');
  return { ...row.payload, id: row.id, tenantId: row.tenant_id, stage: row.stage,
    status: row.status, objective: row.objective, targetSegment: row.target_segment };
}

// The governed Scout adapter can fail before its stage transaction commits.
// Permit recovery of that exact failed initial child without manufacturing READY.
async function failedDiscoveryReceipt(store, program, progress, previous, now) {
  const reject = () => fail('replenishment_discovery_failure_unproven');
  const day = clock(now).day;
  if (previous.stage !== 'discover' || previous.version !== 0 || previous.lastTransactionId
    || previous.structuredMissionApproved !== true
    || previous.pendingOperatorDecision?.kind !== 'discovery_approval'
    || previous.orchestrationMissionId !== program.source_mission_id
    || progress.attempts < 1 || progress.attempts >= program.policy.preparationAttemptsPerDay
    || progress.last_error !== 'verified_inventory_shortfall') reject();
  if (progress.attempts === 1) {
    if (previous.id !== `mission_daily_${hash([program.id, day]).slice(0, 24)}`) reject();
  } else {
    const reserved = await store.one(`SELECT payload FROM acquisition_outbound_events
      WHERE tenant_id='10' AND program_id=$1 AND event_type='preparation_recovery_reserved'
      AND payload->>'missionId'=$2 ORDER BY created_at DESC LIMIT 1`, [program.id, previous.id]);
    const receipt = reserved?.payload;
    if (!receipt || receipt.reviewHash !== hash(receipt.review)
      || receipt.review.programId !== program.id || receipt.review.policyHash !== program.policy_hash
      || receipt.review.scopeHash !== program.scope_hash || receipt.review.localDay !== day
      || receipt.review.operator !== program.authorized_by
      || receipt.review.nextAttempt !== progress.attempts
      || receipt.review.priorAttempts !== progress.attempts - 1
      || previous.id !== `mission_daily_${hash([program.id, day, 'replenishment', receipt.reviewHash]).slice(0, 24)}`) reject();
  }
  const event = await store.one(`SELECT id,payload,created_at FROM acquisition_outbound_events
    WHERE tenant_id='10' AND program_id=$1 AND event_type='inventory_replenished'
      AND payload->>'missionId'=$2 ORDER BY created_at DESC LIMIT 1`, [program.id, previous.id]);
  const payload = event?.payload;
  const candidates = payload?.candidates;
  if (!event || payload.programId !== program.id || payload.missionId !== previous.id
    || payload.eligible !== 0 || !Number.isInteger(payload.attempts)
    || payload.attempts < 0 || payload.attempts > program.policy.enrichmentLimit
    || !candidates || Array.isArray(candidates) || !Object.keys(candidates).length
    || !Object.values(candidates).every(row => row?.eligible === false && typeof row.reason === 'string' && row.reason)
    || !Number.isFinite(+new Date(event.created_at))
    || +new Date(event.created_at) < +new Date(progress.last_attempt_at)
    || +new Date(event.created_at) > +now || clock(new Date(event.created_at)).day !== day) reject();
  if (await store.one("SELECT id FROM acquisition_mission_contributions WHERE tenant_id='10' AND mission_id=$1 LIMIT 1", [previous.id])) reject();
  return { id: event.id, payloadHash: hash(payload), createdAt: new Date(event.created_at).toISOString() };
}

async function reviewReplenishment(store, input, actor, now, enabled) {
  if (!actor?.id || !['admin', 'manager'].includes(actor.role)) fail('operator_required');
  if (enabled) fail('replenishment_requires_disabled_sending');
  const program = await store.program();
  if (!program || program.id !== input.programId || program.mode !== 'shadow') fail('replenishment_shadow_grant_required');
  if (String(actor.id) !== program.authorized_by) fail('replenishment_authorizing_operator_required');
  if (program.policy_hash !== input.policyHash || hash(program.policy) !== program.policy_hash) fail('policy_changed');
  if (program.scope_hash !== input.scopeHash) fail('source_scope_changed');
  const reason = windowReason(program.policy, now, false);
  if (reason) fail(reason);
  const day = clock(now).day;
  if (input.localDay !== day) fail('replenishment_day_changed');
  const client = await store.one('SELECT active,autosend_enabled FROM clients WHERE id=10');
  if (client?.active !== true || client.autosend_enabled !== false) fail('tenant_inactive_or_legacy_autosend_enabled');
  const source = project(await store.one("SELECT * FROM acquisition_missions WHERE tenant_id='10' AND id=$1", [program.source_mission_id]));
  if (hash(missionScope(source)) !== program.scope_hash || source.planCancelled || !source.structuredMission?.immutable) fail('source_scope_changed');
  const progress = await store.one('SELECT * FROM acquisition_outbound_preparation WHERE program_id=$1 AND local_day=$2', [program.id, day]);
  if (!progress || progress.mission_id !== input.fromMissionId || progress.attempts < 1) fail('replenishment_preparation_changed');
  if (progress.attempts >= program.policy.preparationAttemptsPerDay) fail('preparation_retry_budget');
  const immediate = input.immediatePreparation === true;
  if (immediate && !String(input.operatorReason || '').trim()) fail('immediate_preparation_reason_required');
  if (!progress.last_attempt_at || (!immediate && +now - +new Date(progress.last_attempt_at) < 60 * 60000)) fail('preparation_backoff');
  const previous = project(await store.one("SELECT * FROM acquisition_missions WHERE tenant_id='10' AND id=$1", [progress.mission_id]));
  if (hash(missionScope(previous)) !== program.scope_hash || previous.planCancelled) fail('daily_mission_scope_changed');
  if (program.last_error !== 'verified_inventory_shortfall') fail('replenishment_requires_blocked_ready_batch');
  const discoveryFailure = previous.stage === 'ready' ? null
    : await failedDiscoveryReceipt(store, program, progress, previous, now);
  const envelope = await store.one("SELECT id FROM acquisition_outbound_envelopes WHERE tenant_id='10' AND (program_id=$1 OR local_day=$2::date) LIMIT 1", [program.id, day]);
  if (envelope) fail('replenishment_envelope_exists');
  const counts = await store.counts(program, day);
  if (counts.today || counts.total || counts.uncertain) fail('replenishment_attempt_exists');
  const execution = await store.one("SELECT id FROM acquisition_mission_outbound_executions WHERE tenant_id='10' AND mission_id=$1 LIMIT 1", [previous.id]);
  if (execution) fail('replenishment_execution_exists');
  const research = researchCompanies(input.research, source, now);
  for (const candidate of research) {
    if (await store.candidateOwnership({ company: candidate.name, domain: candidate.domain })) fail('research_candidate_ao_owned');
  }
  const review = { programId: program.id, policyHash: program.policy_hash, scopeHash: program.scope_hash,
    localDay: day, fromMissionId: previous.id, priorAttempts: progress.attempts,
    priorAttemptAt: new Date(progress.last_attempt_at).toISOString(),
    nextAttempt: progress.attempts + 1, operator: String(actor.id), research,
    previousMissionHash: hash(previous), sourceMissionHash: hash(source),
    dailyCap: program.policy.dailyCap, totalCap: program.policy.totalCap,
    immediatePreparation: immediate, operatorReason: immediate ? String(input.operatorReason).trim() : null,
    ...(discoveryFailure ? { discoveryFailure } : {}) };
  const reviewHash = hash(review);
  return { reviewRequired: true, reviewHash, review,
    nextMissionId: `mission_daily_${hash([program.id, day, 'replenishment', reviewHash]).slice(0, 24)}`,
    program, source: { mission: source }, progress };
}

async function reserveReplenishment(store, plan, now = new Date()) {
  const { review, reviewHash, nextMissionId } = plan;
  const db = await store.pool.connect();
  try {
    await db.query('BEGIN');
    const program = (await db.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1 FOR UPDATE', [review.programId])).rows[0];
    if (program?.mode !== 'shadow' || program.policy_hash !== review.policyHash || hash(program.policy) !== review.policyHash
      || program.scope_hash !== review.scopeHash) fail('replenishment_grant_changed');
    if (review.discoveryFailure) {
      const lockedStore = { one: async (sql, args) => (await db.query(sql, args)).rows[0] || null };
      const previous = project(await lockedStore.one("SELECT * FROM acquisition_missions WHERE tenant_id='10' AND id=$1 FOR UPDATE", [review.fromMissionId]));
      const progress = await lockedStore.one('SELECT * FROM acquisition_outbound_preparation WHERE program_id=$1 AND local_day=$2 FOR UPDATE', [review.programId, review.localDay]);
      if (!progress || progress.mission_id !== review.fromMissionId || hash(previous) !== review.previousMissionHash
        || program.last_error !== 'verified_inventory_shortfall') fail('replenishment_preparation_changed');
      const receipt = await failedDiscoveryReceipt(lockedStore, program, progress, previous, now);
      if (hash(receipt) !== hash(review.discoveryFailure)) fail('replenishment_review_changed');
    }
    const updated = await db.query(`UPDATE acquisition_outbound_preparation
      SET attempts=attempts+1,last_attempt_at=now(),last_error=NULL,mission_id=$5
      WHERE program_id=$1 AND local_day=$2 AND mission_id=$3 AND attempts=$4
      AND date_trunc('milliseconds',last_attempt_at)=$6::timestamptz
      AND NOT EXISTS (SELECT 1 FROM acquisition_outbound_envelopes WHERE tenant_id='10' AND (program_id=$1 OR local_day=$2::date))
      RETURNING *`, [review.programId, review.localDay, review.fromMissionId, review.priorAttempts, nextMissionId, review.priorAttemptAt]);
    if (updated.rows.length !== 1) fail('replenishment_preparation_changed');
    await store.event('preparation_recovery_reserved', [review.programId, reviewHash],
      { programId: review.programId, missionId: nextMissionId, reviewHash, review }, db);
    await db.query('COMMIT');
    return updated.rows[0];
  } catch (error) { await db.query('ROLLBACK'); throw error; }
  finally { db.release(); }
}

module.exports = { researchCompanies, scoutCompanies, reviewReplenishment, reserveReplenishment };
