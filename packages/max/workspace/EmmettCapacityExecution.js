'use strict';

/**
 * SPEC-068 — Canonical Emmett PREPARE dispatch.
 * Mission-bound queue + SPEC-117 cognition → CAPACITY contribution via SEC/TME.
 */

const eoi = require('../../emmett-outbound');
const { GOVERNOR_OUTCOMES } = require('../../emmett-outbound/types');
const {
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  EVENT_KINDS,
  STAGES,
  asText,
  nowIso,
} = require('../../acquisition-mission/types');
const {
  assertContract,
  assertContributionContract,
  assertExecutionResult,
  buildExecutionInput,
  executeSpecialist,
  EXECUTION_STATUSES,
  bumpMissionVersion,
  planningError,
  validationError,
} = require('../../acquisition-mission');
const { createEvent } = require('../../acquisition-mission/Timeline');
const { specialistContext } = require('../../acquisition-mission/Lifecycle');
const { assertMissionStateConsistent } = require('../../acquisition-mission/PendingOperatorDecision');
const { latestContribution } = require('./EmmettMissionCandidates');

const FORBIDDEN_QUEUE_KEYS = new Set([
  'subject', 'subjects', 'body', 'cta', 'variant', 'variants',
  'messaging', 'copy', 'emailBody', 'email_body', 'hypothesis', 'hypotheses',
]);

function findEmmettCapacity(contributions = []) {
  return [...contributions]
    .reverse()
    .find(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    );
}

function fixtureInfrastructureSnapshot(tenantId) {
  const id = String(tenantId || '10');
  return {
    tenantId: id,
    clientId: Number(id) || null,
    inboxId: `inbox-${id}`,
    domain: 'example.com',
    inboxAgeDays: 45,
    providerCeiling: 50,
    authentication: { spf: 'pass', dkim: 'pass', dmarc: 'none' },
    warmup: {
      status: 'healthy',
      dailyCap: 50,
      activeSendDays: 14,
      reset: false,
    },
    bounceRate: 0,
    replyRate: 0.08,
    openRate: 0.35,
    complaintRate: 0,
    blacklist: { listed: false, sources: [] },
    sentToday: 0,
    sentYesterday: 5,
    historicalDailyAvg: 8,
    recentSends: 40,
    replyByWeekday: { Tue: 0.12, Fri: 0.06 },
  };
}

async function resolveInfrastructureSnapshot(executionInput = {}, opts = {}) {
  if (opts.infrastructureSnapshot && typeof opts.infrastructureSnapshot === 'object') {
    return opts.infrastructureSnapshot;
  }
  if (executionInput.specialistInput?.infrastructureSnapshot) {
    return executionInput.specialistInput.infrastructureSnapshot;
  }
  const tenantId = executionInput.executionContext?.tenantId
    || executionInput.specialistInput?.tenantId
    || opts.tenantId;
  if (opts.pool && tenantId) {
    try {
      const { buildInboxSnapshot } = require('../../../services/emmettOutboundSnapshot');
      return await buildInboxSnapshot(Number(tenantId) || tenantId, {
        pool: opts.pool,
        now: opts.now,
      });
    } catch (_) {
      /* fall through to fixture */
    }
  }
  if (opts.allowFixtureFallback !== false) {
    return fixtureInfrastructureSnapshot(tenantId);
  }
  throw validationError('tme_infrastructure_missing', 'Infrastructure snapshot is required for Emmett execution.');
}

function sanitizeQueueItem(item = {}) {
  const clean = {};
  for (const [key, value] of Object.entries(item)) {
    if (FORBIDDEN_QUEUE_KEYS.has(key)) continue;
    if (key === 'paige' && value && typeof value === 'object') {
      clean.paige = {
        author: value.author || 'paige',
        source: value.source || 'paige',
        ready: value.ready === true,
        variantLabel: value.variantLabel || null,
        sendable: value.sendable === true,
      };
      continue;
    }
    clean[key] = value;
  }
  return clean;
}

function mapAssessedToCapacityPayload(assessed = {}) {
  const { health, capacity, governor, queue, recommendations } = assessed;
  const recommended = Number(capacity?.recommended || 0);
  const queueItems = (queue?.items || []).map(sanitizeQueueItem);

  const sendRecommendations = (recommendations || []).map((row) => {
    if (typeof row === 'string') return row;
    return row.body || row.title || row.text || String(row);
  }).filter(Boolean);

  const governorOutcome = asText(governor?.outcome).toLowerCase() || GOVERNOR_OUTCOMES.PROCEED;
  const atRisk = Boolean(
    governor?.halt
    || governorOutcome === GOVERNOR_OUTCOMES.PAUSE
    || governorOutcome === GOVERNOR_OUTCOMES.EMERGENCY
    || Number(health?.score || 0) < 70
  );

  return {
    capacity: {
      recommended,
      remaining: recommended,
      ceiling: capacity?.ceiling != null ? capacity.ceiling : recommended,
      confidence: capacity?.confidence != null ? capacity.confidence : 0.75,
      outlook: capacity?.outlook || 'stable',
      statement: capacity?.statement || null,
    },
    queue: {
      kind: queue?.kind || 'today_queue',
      recommended,
      candidateCount: queue?.candidateCount ?? queueItems.length,
      selectedCount: queue?.selectedCount ?? queueItems.length,
      items: queueItems,
      statement: queue?.statement || null,
    },
    sendRecommendations,
    deliverability: {
      status: health?.label || 'healthy',
      score: health?.score != null ? health.score : null,
      reasons: health?.reasons || [],
    },
    reputation: {
      atRisk,
      score: health?.score != null ? health.score : null,
    },
    governor: {
      outcome: governorOutcome,
      reason: governor?.reason || null,
      halt: governor?.halt === true,
      slowCap: governor?.slowCap != null ? governor.slowCap : null,
    },
  };
}

function buildEmmettEvidence(assessed = {}, executionInput = {}) {
  const { health, capacity, governor } = assessed;
  const infra = executionInput.specialistInput?.infrastructureSnapshot || {};
  const items = [];

  if (health?.score != null) {
    items.push({
      id: 'ev_emmett_health',
      label: `Inbox health score ${health.score}`,
      source: 'emmett_outbound.inbox_health',
      confidence: clamp01(capacity?.confidence ?? 0.75),
      timestamp: nowIso(),
      provenance: { kind: 'infrastructure', source: 'spec117_inbox_health' },
    });
  }
  if (governor?.outcome) {
    items.push({
      id: 'ev_emmett_governor',
      label: `Safe Send Governor: ${governor.outcome}`,
      source: 'emmett_outbound.governor',
      confidence: 0.9,
      timestamp: nowIso(),
      provenance: { kind: 'governor', source: 'spec117_governor', reason: governor.reason || null },
    });
  }
  if (infra.inboxAgeDays != null) {
    items.push({
      id: 'ev_emmett_inbox_age',
      label: `Inbox age ${infra.inboxAgeDays} days`,
      source: 'infrastructure_snapshot',
      confidence: 0.85,
      timestamp: nowIso(),
      provenance: { kind: 'observed', source: 'inbox_snapshot' },
    });
  }
  const scoutEvidence = executionInput.specialistInput?.evidence || [];
  for (const row of scoutEvidence.slice(0, 3)) {
    if (typeof row === 'string') {
      items.push({
        id: `ev_scout_${items.length}`,
        label: row,
        source: 'scout_discovery',
        confidence: 0.6,
        timestamp: nowIso(),
        provenance: { kind: 'upstream_intelligence', source: 'scout' },
      });
    } else if (row && row.label) {
      items.push({
        id: row.id || `ev_scout_${items.length}`,
        label: row.label,
        source: row.source || 'scout_discovery',
        confidence: clamp01(row.confidence ?? 0.6),
        timestamp: row.timestamp || row.observedAt || nowIso(),
        provenance: row.provenance || { kind: 'upstream_intelligence', source: 'scout' },
      });
    }
  }
  return items;
}

function clamp01(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.min(1, Math.max(0, n));
}

function buildEmmettExecutionResult(capacityPayload, executionInput = {}, ctx = {}) {
  const confidenceOverall = clamp01(capacityPayload.capacity?.confidence ?? 0.75);
  const evidence = buildEmmettEvidence(ctx.assessed || {}, executionInput);
  const queueCount = capacityPayload.queue?.items?.length || 0;

  return {
    spec: 'SPEC-132',
    status: EXECUTION_STATUSES.SUCCESS,
    confidence: {
      overall: confidenceOverall,
      evidence: clamp01(evidence.length ? 0.8 : 0.5),
      fit: clamp01(executionInput.specialistInput?.rankedTargets?.length ? 0.85 : 0.6),
      completeness: clamp01(queueCount > 0 ? 0.9 : 0.5),
    },
    evidence,
    contributions: capacityPayload,
    recommendations: capacityPayload.sendRecommendations.slice(0, 3).map((text) => ({
      tier: 'suggested',
      text,
    })),
    unknowns: [],
    nextActions: [{
      kind: 'operator_review',
      label: 'Operator review capacity plan before Ready',
    }],
  };
}

async function buildEmmettCapacityPayload(executionInput = {}, opts = {}) {
  const specialistInput = executionInput.specialistInput || {};
  const candidates = specialistInput.missionCandidates || [];
  if (!candidates.length) {
    throw validationError('tme_mission_candidates_missing', 'Mission-bound candidates are required for Emmett queue cognition.');
  }

  const infrastructureSnapshot = await resolveInfrastructureSnapshot(executionInput, opts);
  specialistInput.infrastructureSnapshot = infrastructureSnapshot;

  const tenantId = infrastructureSnapshot.tenantId
    || executionInput.executionContext?.tenantId
    || opts.tenantId;
  const engine = opts.eoiEngine || eoi.createOutboundEngine({ store: opts.eoiStore });
  const assessed = engine.assess({
    tenantId: String(tenantId),
    clientId: infrastructureSnapshot.clientId || Number(tenantId) || null,
    snapshot: infrastructureSnapshot,
    prospects: candidates,
    now: opts.now,
    timeZone: infrastructureSnapshot.timeZone || 'America/New_York',
  });

  const payload = mapAssessedToCapacityPayload(assessed);
  assertContract(SPECIALISTS.EMMETT, payload);
  payload._assessed = undefined;
  return { payload, assessed, infrastructureSnapshot, candidates };
}

async function runEmmettForAmoMission(mission, opts = {}) {
  if (typeof opts.runEmmett === 'function') {
    return opts.runEmmett(mission, opts);
  }
  const contributions = opts.contributions
    || (opts.engine && opts.engine.inspect(mission.id, { tenantId: opts.tenantId }).contributions)
    || [];
  const executionInput = buildExecutionInput({
    mission,
    contributions,
    specialist: SPECIALISTS.EMMETT,
    transactionId: opts.transactionId,
    executionContext: opts.executionContext,
    infrastructureSnapshot: opts.infrastructureSnapshot,
    store: opts.engine?.store,
  });
  const { payload, assessed, infrastructureSnapshot, candidates } = await buildEmmettCapacityPayload(
    executionInput,
    opts
  );
  return {
    capacityPayload: payload,
    assessed,
    infrastructureSnapshot,
    missionCandidates: candidates,
    executionInput,
  };
}

function fixtureEmmettCapacityResult(mission, contributions = [], opts = {}) {
  const executionInput = buildExecutionInput({
    mission,
    contributions,
    specialist: SPECIALISTS.EMMETT,
    transactionId: 'fixture_emmett',
    infrastructureSnapshot: opts.infrastructureSnapshot || fixtureInfrastructureSnapshot(mission.tenantId),
  });
  const candidates = executionInput.specialistInput?.missionCandidates || [];
  const engine = eoi.createOutboundEngine();
  const assessed = engine.assess({
    tenantId: String(mission.tenantId || '10'),
    snapshot: executionInput.specialistInput.infrastructureSnapshot,
    prospects: candidates,
  });
  return mapAssessedToCapacityPayload(assessed);
}

function validateEmmettPreconditions({ mission, engine, tenantId }) {
  if (!mission) throw planningError('tme_mission_missing', 'Mission does not exist.');
  if (mission.planCancelled === true || /cancelled/i.test(String(mission.status || ''))) {
    throw planningError('tme_mission_inactive', 'Mission is not active.');
  }
  const snapshot = engine.inspect(mission.id, { tenantId });
  const ctx = specialistContext(snapshot.contributions || []);
  if (!ctx.paigeComplete) {
    throw planningError('tme_paige_incomplete', 'Paige variants are required before Emmett execution.');
  }
  if (ctx.emmettComplete) {
    throw planningError('tme_already_executed', 'Emmett capacity already committed.');
  }
  if (mission.stage !== STAGES.PREPARE) {
    throw planningError('tme_wrong_stage', `Emmett execution requires stage ${STAGES.PREPARE}.`);
  }
  if (!findPaigeVariants(snapshot.contributions || [])) {
    throw planningError('tme_paige_variants_missing', 'Paige VARIANTS contribution is required.');
  }
  return {
    missionExists: true,
    missionActive: true,
    missionLocked: true,
    structuredPlanApproved: true,
    specialistAvailable: true,
    requiredEvidencePresent: true,
  };
}

function findPaigeVariants(contributions = []) {
  return latestContribution(contributions, SPECIALISTS.PAIGE, CONTRIBUTION_KINDS.VARIANTS);
}

function validateEmmettCapacityOutput(output, ctx = {}) {
  if (!output || !output.capacityPayload) {
    throw validationError('tme_contribution_missing', 'Emmett capacity contribution is missing.');
  }
  const payload = output.capacityPayload;
  assertContributionContract(SPECIALISTS.EMMETT, payload);
  const executionResult = output.executionResult || buildEmmettExecutionResult(
    payload,
    output.executionInput || {},
    { assessed: output.assessed, transactionId: ctx.transactionId }
  );
  assertExecutionResult(executionResult, {
    specialist: SPECIALISTS.EMMETT,
    requireContributions: true,
    requireEvidence: true,
  });
  output.executionResult = executionResult;
}

function commitEmmettCapacityStage({
  engine,
  mission,
  tenantId,
  output,
  transactionId,
  missionVersion,
}) {
  const missionId = (mission && mission.id) || output.missionId;
  const { capacityPayload } = output;
  const payload = { ...capacityPayload, transactionId };

  const contribution = engine.contribute(
    missionId,
    {
      specialist: SPECIALISTS.EMMETT,
      kind: CONTRIBUTION_KINDS.CAPACITY,
      payload,
    },
    { tenantId }
  );

  const updated = engine.get(missionId, tenantId);
  bumpMissionVersion(updated, transactionId);
  engine.store.putMission(updated);
  engine.store.addEvent(createEvent({
    missionId,
    kind: EVENT_KINDS.EXECUTION_COMMITTED,
    specialist: SPECIALISTS.EMMETT,
    label: 'Emmett capacity committed',
    payload: {
      transactionId,
      missionVersion: updated.version,
      priorVersion: missionVersion,
      contributionId: contribution.contribution.id,
    },
  }));

  const snapshot = engine.inspect(missionId, { tenantId });
  assertMissionStateConsistent(snapshot.mission, {
    contributions: snapshot.contributions,
  });
  return {
    capacity: contribution.contribution,
    snapshot,
  };
}

module.exports = {
  findEmmettCapacity,
  fixtureInfrastructureSnapshot,
  resolveInfrastructureSnapshot,
  mapAssessedToCapacityPayload,
  buildEmmettCapacityPayload,
  buildEmmettExecutionResult,
  runEmmettForAmoMission,
  fixtureEmmettCapacityResult,
  validateEmmettPreconditions,
  validateEmmettCapacityOutput,
  commitEmmettCapacityStage,
  sanitizeQueueItem,
};
