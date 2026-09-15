'use strict';

/**
 * Shared helpers for Anchor tenant 10 canonical AMO inspect + recover-to-READY.
 * Never sends mail. Never enables autosend. Never uses the law-firm mission as
 * a stand-in for the STR operator objective.
 */

const amo = require('../../packages/acquisition-mission');
const {
  EXECUTION_INTENTS,
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  OPERATOR_DECISION_KINDS,
  intentFromPendingDecision,
} = amo;
const { evaluatePrioritizationReadiness } = require('../../packages/acquisition-mission/DecisionReadiness');
const { selectCanonicalContribution } = require('../../packages/acquisition-mission/CanonicalContributionSelection');
const { evaluateUpstreamArtifactCoherence } = require('../../packages/acquisition-mission/UpstreamArtifactCoherence');
const { unwrapContributionPayload, scoutCandidateCount } = require('../validateAnchorCanonicalMission');
const { sendableQueueItems } = require('../executeAnchorOneOutbound');
const { selectActiveCapacityContribution } = require('./activeCapacitySelection');

const TENANT_ID = '10';
const CLIENT_ID = 10;

const STR_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester area.';

const FORBIDDEN_SEND_INTENTS = Object.freeze([
  EXECUTION_INTENTS.APPROVE_EXECUTION,
  EXECUTION_INTENTS.EXECUTE_OUTBOUND,
]);

const FORBIDDEN_SEND_INTENT_SET = new Set(FORBIDDEN_SEND_INTENTS);

/** READY mission that currently fails SPEC-136 inspect; never treat as the STR canonical mission. */
const EXCLUDED_MISSION_IDS = Object.freeze([
  'mission_30b36f10-20ce-4e41-8780-c3d8822e2c8e',
]);

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function normalizeObjective(text) {
  return asText(text).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function isLawFirmObjective(text) {
  return /\blaw firms?\b/.test(normalizeObjective(text));
}

function isStrOutboundObjective(text) {
  const n = normalizeObjective(text);
  if (!n) return false;
  if (isLawFirmObjective(n)) return false;
  const hasCleaning = /commercial cleaning/.test(n);
  const hasStr = /short term rental/.test(n) || /\bstr operators?\b/.test(n);
  const hasGeo = /manchester/.test(n);
  return hasCleaning && hasStr && hasGeo;
}

function latestContribution(contributions, specialist, kind, mission = null) {
  return selectCanonicalContribution(contributions || [], {
    missionId: mission?.id,
    specialist,
    kind,
    mission,
  });
}

function paigeVariantCount(payload) {
  const body = unwrapContributionPayload(payload) || {};
  if (Array.isArray(body.variants)) return body.variants.length;
  if (Array.isArray(body.subjects)) return body.subjects.length;
  return 0;
}

function maxRankedCount(payload) {
  const body = unwrapContributionPayload(payload) || {};
  if (Array.isArray(body.rankedTargets) && body.rankedTargets.length) {
    return body.rankedTargets.length;
  }
  if (Array.isArray(body.priorities) && body.priorities.length) {
    return body.priorities.length;
  }
  if (Number.isFinite(Number(body.rankedCount))) return Number(body.rankedCount);
  return 0;
}

function classifyQueueItems(capacityPayload) {
  const body = unwrapContributionPayload(capacityPayload) || {};
  const items = Array.isArray(body.queue?.items) ? body.queue.items : [];
  const sendable = sendableQueueItems(body);
  const sendableIds = new Set(sendable.map((row) => String(row.prospectId || row.id || row.candidateId || '')));
  const blocked = items
    .filter((item) => !sendableIds.has(String(item?.prospectId || item?.id || item?.candidateId || '')))
    .map((item) => {
      const reasons = [];
      if (!item) reasons.push('missing_item');
      else {
        if (item.sendable === false) reasons.push('sendable_false');
        if (item.dnc === true) reasons.push('dnc');
        if (!String(item.email || '').trim()) reasons.push('missing_recipient_email_on_queue_item');
        if (!item.paige?.subject || !item.paige?.body) reasons.push('missing_paige_copy');
      }
      return {
        prospectId: item?.prospectId || null,
        candidateId: item?.id || item?.candidateId || null,
        placeId: item?.placeId || null,
        crmCompanyId: item?.crmCompanyId || null,
        crmProspectId: item?.crmProspectId || null,
        company: item?.company || null,
        reasons,
      };
    });
  return {
    queueCount: items.length,
    sendableCount: sendable.length,
    blockedCount: blocked.length,
    sendable,
    blocked,
  };
}

function summarizeContributions(contributions = [], mission = {}) {
  const scout = latestContribution(contributions, SPECIALISTS.SCOUT, CONTRIBUTION_KINDS.DISCOVERY, mission);
  const max = latestContribution(contributions, SPECIALISTS.MAX, CONTRIBUTION_KINDS.PRIORITIZATION, mission);
  const paige = latestContribution(contributions, SPECIALISTS.PAIGE, CONTRIBUTION_KINDS.VARIANTS, mission);
  const capacityRows = (contributions || []).filter(
    (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
  );
  const selectedCapacity = selectActiveCapacityContribution(mission, capacityRows);
  const approach = latestContribution(contributions, SPECIALISTS.MAX, CONTRIBUTION_KINDS.ACQUISITION_APPROACH);
  const discoveryApproval = [...(contributions || [])]
    .reverse()
    .find(
      (row) =>
        row.specialist === SPECIALISTS.OPERATOR
        && row.kind === CONTRIBUTION_KINDS.APPROVAL
        && (
          row.payload?.action === 'discovery_approved'
          || row.payload?.kind === 'discovery_approval'
        )
    ) || null;
  const scoutCount = scout ? scoutCandidateCount(scout) : 0;
  const scoutPayload = scout ? unwrapContributionPayload(scout) : null;
  const discoveryReadiness = scoutPayload
    ? evaluatePrioritizationReadiness(scoutPayload)
    : null;
  const queue = selectedCapacity ? classifyQueueItems(selectedCapacity.payload) : {
    queueCount: 0,
    sendableCount: 0,
    blockedCount: 0,
    sendable: [],
    blocked: [],
  };

  return {
    scout: scout
      ? {
        id: scout.id || null,
        at: scout.at || null,
        candidateCount: scoutCount,
        payload: scoutPayload,
      }
      : null,
    discoveryReadiness: discoveryReadiness
      ? {
        sufficient: discoveryReadiness.sufficient === true,
        primaryBlocker: discoveryReadiness.primaryBlocker
          ? {
            code: discoveryReadiness.primaryBlocker.code || null,
            label: discoveryReadiness.primaryBlocker.label || null,
            reason: discoveryReadiness.primaryBlocker.reason || null,
            recommendedAction: discoveryReadiness.primaryBlocker.recommendedAction || null,
            waitingOn: discoveryReadiness.primaryBlocker.waitingOn || null,
          }
          : null,
      }
      : null,
    max: max
      ? {
        id: max.id || null,
        at: max.at || null,
        rankedCount: maxRankedCount(max),
      }
      : null,
    approach: approach
      ? {
        id: approach.id || null,
        selected: unwrapContributionPayload(approach)?.selected
          || unwrapContributionPayload(approach)?.approach
          || null,
      }
      : null,
    paige: paige
      ? {
        id: paige.id || null,
        at: paige.at || null,
        variantCount: paigeVariantCount(paige),
      }
      : null,
    emmett: selectedCapacity
      ? {
        id: selectedCapacity.id || selectedCapacity.capacity_id || null,
        at: selectedCapacity.at || null,
        superseded: selectedCapacity.payload?.superseded === true,
        ...queue,
      }
      : null,
    discoveryApproval: discoveryApproval
      ? { id: discoveryApproval.id || null, at: discoveryApproval.at || null }
      : null,
    discoveryApproved: Boolean(discoveryApproval || scout),
    latest: (contributions || []).slice(-8).map((row) => ({
      id: row.id || null,
      specialist: row.specialist,
      kind: row.kind,
      at: row.at || null,
    })),
  };
}

function summarizeMissionSnapshot(snapshot = {}, extras = {}) {
  const mission = snapshot.mission || {};
  const contributions = snapshot.contributions || [];
  const summarized = summarizeContributions(contributions, mission);
  const pending = mission.pendingOperatorDecision || null;
  const pendingIntent = intentFromPendingDecision(pending);
  const health = snapshot.health || {};
  const upstreamCoherence = evaluateUpstreamArtifactCoherence(mission, contributions);
  return {
    id: mission.id || null,
    title: mission.title || null,
    objective: mission.objective || null,
    targetSegment: mission.targetSegment || null,
    status: mission.status || null,
    stage: mission.stage || null,
    progress: mission.progressPercent ?? mission.progress ?? null,
    confidence: mission.confidence ?? health.confidence ?? null,
    health: health.label || health.status || health.kind || null,
    waitingReason: snapshot.blocker?.reason
      || pending?.reason
      || pending?.prompt
      || (pendingIntent ? `pending:${pending.kind}` : null),
    pendingOperatorDecision: pending
      ? { kind: pending.kind, prompt: pending.prompt || null, reason: pending.reason || null }
      : null,
    pendingIntent,
    isStrObjective: isStrOutboundObjective(mission.objective),
    isLawFirmObjective: isLawFirmObjective(mission.objective),
    contributions: summarized,
    discoveryApproved: summarized.discoveryApproved === true,
    scoutCandidateCount: summarized.scout?.candidateCount || 0,
    discoveryReadiness: summarized.discoveryReadiness || null,
    prioritizationReady: summarized.discoveryReadiness?.sufficient === true,
    upstreamCoherent: upstreamCoherence.coherent === true,
    upstreamBlockers: upstreamCoherence.blockers || [],
    prioritizedCandidateCount: summarized.max?.rankedCount || 0,
    paigeVariantCount: summarized.paige?.variantCount || 0,
    capacityItemCount: summarized.emmett?.queueCount || 0,
    sendableCount: summarized.emmett?.sendableCount || 0,
    blockedCount: summarized.emmett?.blockedCount || 0,
    blockedReasons: summarized.emmett?.blocked || [],
    executionRecords: Array.isArray(snapshot.executionRecords)
      ? snapshot.executionRecords.slice(-5).map((row) => ({
        id: row.id || null,
        status: row.status || null,
        at: row.attemptedAt || row.at || null,
      }))
      : [],
    autosendEnabled: extras.autosendEnabled === true,
    ...extras,
  };
}

function pickCanonicalStrMission(summaries = []) {
  const matches = (summaries || []).filter((row) =>
    row
    && row.isStrObjective === true
    && !EXCLUDED_MISSION_IDS.includes(row.id)
  );
  if (!matches.length) return null;
  const rank = (row) => {
    const stage = String(row.stage || '');
    const stageScore = {
      [STAGES.READY]: 80,
      [STAGES.PREPARE]: 70,
      [STAGES.PLAN]: 60,
      [STAGES.UNDERSTAND]: 50,
      [STAGES.DISCOVER]: 40,
      [STAGES.EXECUTE]: 30,
      [STAGES.OBSERVE]: 20,
    }[stage] || 10;
    const candidateBonus = Number(row.scoutCandidateCount || 0) > 0 ? 25 : 0;
    const cancelled = /cancel/i.test(String(row.status || '')) ? -100 : 0;
    return stageScore + candidateBonus + cancelled;
  };
  return matches.slice().sort((a, b) => rank(b) - rank(a))[0];
}

function hasScoutDiscovery(summary = {}) {
  return Boolean(summary.contributions && summary.contributions.scout);
}

function discoveryApprovalAbsent(summary = {}) {
  if (summary.discoveryApproved === true) return false;
  if (hasScoutDiscovery(summary)) return false;
  return true;
}

function prioritizationApprovalPending(summary = {}) {
  const pending = summary.pendingOperatorDecision || null;
  if (summary.pendingIntent === EXECUTION_INTENTS.APPROVE_PRIORITIZATION) return true;
  return pending?.kind === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL;
}

function canIssuePrioritizationApproval(summary = {}) {
  if (!Number(summary.scoutCandidateCount || 0)) return false;
  if (summary.contributions?.max) return false;
  return prioritizationApprovalPending(summary) || summary.prioritizationReady === true;
}

function isRecoverySummaryIncoherent(summary = {}) {
  if (summary.upstreamCoherent === false) return true;
  const scoutCount = Number(summary.scoutCandidateCount || 0);
  const rankedCount = Number(summary.prioritizedCandidateCount || summary.contributions?.max?.rankedCount || 0);
  if (rankedCount > 0 && scoutCount <= 0) return true;
  if (summary.prioritizationReady === false && scoutCount > 0) return true;
  if (summary.discoveryReadiness && summary.discoveryReadiness.sufficient === false && scoutCount > 0) {
    return true;
  }
  return false;
}

function upstreamIncoherentStop(summary = {}) {
  const readiness = summary.discoveryReadiness || {};
  const blocker = readiness.primaryBlocker || null;
  return {
    intent: null,
    stop: true,
    reason: 'upstream_artifact_incoherent',
    operatorAction:
      blocker?.reason
      || (Number(summary.scoutCandidateCount || 0) <= 0
        ? 'Canonical Scout discovery is stale or empty; re-hydrate the latest committed Scout contribution before execution approval.'
        : 'Upstream artifact chain is incoherent; fix canonical Scout hydration before APPROVE_EXECUTION.'),
    blocker,
    upstreamBlockers: summary.upstreamBlockers || [],
  };
}

function discoveryInvestigationBlocker(summary = {}) {
  const readiness = summary.discoveryReadiness || {};
  const blocker = readiness.primaryBlocker || null;
  return {
    intent: null,
    stop: true,
    reason: 'discovery_investigation_required',
    operatorAction:
      blocker?.reason
      || summary.waitingReason
      || 'Discovery evidence is insufficient for prioritization.',
    blocker,
  };
}

function chooseNextRecoveryIntent(summary = {}) {
  const pendingIntent = summary.pendingIntent || null;
  const stage = summary.stage;
  const scoutCount = Number(summary.scoutCandidateCount || 0);
  const healthyScout = scoutCount > 0;

  const sendable = Number(summary.sendableCount || 0);
  if (FORBIDDEN_SEND_INTENT_SET.has(pendingIntent)) {
    if (isRecoverySummaryIncoherent(summary)) {
      return upstreamIncoherentStop(summary);
    }
    if (sendable > 0) {
      return {
        intent: null,
        stop: true,
        reason: 'ready_awaiting_execution_approval',
        operatorAction:
          'APPROVE_EXECUTION for the current prepared artifacts, then EXECUTE_OUTBOUND. Autosend stays off.',
      };
    }
    return {
      intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      stop: false,
      reason: 'execution_approval_without_sendable_queue',
    };
  }

  if (stage === STAGES.READY && sendable > 0) {
    if (isRecoverySummaryIncoherent(summary)) {
      return upstreamIncoherentStop(summary);
    }
    return {
      intent: null,
      stop: true,
      reason: 'ready_awaiting_execution_approval',
      operatorAction:
        'APPROVE_EXECUTION for the current prepared artifacts, then EXECUTE_OUTBOUND. Autosend stays off.',
    };
  }

  if (pendingIntent === EXECUTION_INTENTS.CONTINUE_INVESTIGATION && healthyScout) {
    if (prioritizationApprovalPending(summary)) {
      return {
        intent: EXECUTION_INTENTS.APPROVE_PRIORITIZATION,
        stop: false,
        reason: 'prioritization_ready_after_discovery',
        operatorAction: null,
      };
    }
    return discoveryInvestigationBlocker(summary);
  }

  const skipPendingFollow = !pendingIntent
    || FORBIDDEN_SEND_INTENT_SET.has(pendingIntent)
    || (pendingIntent === EXECUTION_INTENTS.APPROVE_DISCOVERY && !discoveryApprovalAbsent(summary))
    || (pendingIntent === EXECUTION_INTENTS.CONTINUE_INVESTIGATION && healthyScout);

  if (pendingIntent && !skipPendingFollow) {
    return {
      intent: pendingIntent,
      stop: false,
      reason: 'pending_operator_decision',
      operatorAction: null,
    };
  }

  if (discoveryApprovalAbsent(summary)) {
    return { intent: EXECUTION_INTENTS.APPROVE_DISCOVERY, stop: false, reason: 'missing_discovery' };
  }
  if (!healthyScout) {
    return {
      intent: EXECUTION_INTENTS.CONTINUE_INVESTIGATION,
      stop: false,
      reason: 'approved_empty_discovery',
      operatorAction: null,
    };
  }
  if (!summary.contributions?.max) {
    if (canIssuePrioritizationApproval(summary)) {
      return {
        intent: EXECUTION_INTENTS.APPROVE_PRIORITIZATION,
        stop: false,
        reason: 'missing_prioritization',
      };
    }
    if (healthyScout && summary.prioritizationReady !== true) {
      return discoveryInvestigationBlocker(summary);
    }
  }
  if (!summary.contributions?.approach) {
    return {
      intent: EXECUTION_INTENTS.DECIDE_ACQUISITION_APPROACH,
      stop: false,
      reason: 'missing_acquisition_approach',
      payload: { approach: 'outbound' },
    };
  }
  if (!summary.contributions?.paige) {
    return { intent: EXECUTION_INTENTS.GENERATE_VARIANTS, stop: false, reason: 'missing_variants' };
  }
  if (!summary.contributions?.emmett) {
    return { intent: EXECUTION_INTENTS.GENERATE_CAPACITY, stop: false, reason: 'missing_capacity' };
  }
  if (Number(summary.sendableCount || 0) === 0 && Number(summary.capacityItemCount || 0) >= 0) {
    return {
      intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      stop: false,
      reason: 'capacity_not_sendable',
    };
  }
  return {
    intent: null,
    stop: true,
    reason: 'no_safe_next_intent',
    operatorAction: summary.waitingReason || 'Inspect mission workspace for the next required operator decision.',
  };
}

function payloadForIntent(intent, chosen = {}) {
  if (chosen.payload) return chosen.payload;
  if (intent === EXECUTION_INTENTS.DECIDE_ACQUISITION_APPROACH) {
    return { approach: 'outbound' };
  }
  return {};
}

function questionForIntent(intent) {
  switch (intent) {
    case EXECUTION_INTENTS.APPROVE_PLAN:
      return 'Approved.';
    case EXECUTION_INTENTS.APPROVE_DISCOVERY:
      return 'Approved. Begin Discovery.';
    case EXECUTION_INTENTS.CONTINUE_INVESTIGATION:
      return 'Continue investigation. Run Scout discovery for short-term rental operators.';
    case EXECUTION_INTENTS.START_DISCOVERY:
      return 'Begin Scout discovery.';
    case EXECUTION_INTENTS.APPROVE_PRIORITIZATION:
      return 'Approved prioritization.';
    case EXECUTION_INTENTS.DECIDE_ACQUISITION_APPROACH:
      return 'Proceed with outbound email for ranked short-term rental operator prospects.';
    case EXECUTION_INTENTS.GENERATE_VARIANTS:
      return 'Generate outreach variants.';
    case EXECUTION_INTENTS.GENERATE_CAPACITY:
      return 'Plan outbound capacity.';
    case EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH:
      return 'Revise prepared outreach with live CRM recipient emails.';
    case EXECUTION_INTENTS.MISSION_CONTINUATION:
      return 'Continue the canonical mission.';
    default:
      return 'Advance canonical outbound recovery.';
  }
}

function assertNotSendingIntent(intent) {
  if (FORBIDDEN_SEND_INTENT_SET.has(intent)) {
    const err = new Error(`Refusing send intent ${intent}. Recovery stops at READY.`);
    err.code = 'send_intent_forbidden';
    throw err;
  }
}

module.exports = {
  TENANT_ID,
  CLIENT_ID,
  STR_OBJECTIVE,
  FORBIDDEN_SEND_INTENTS,
  EXCLUDED_MISSION_IDS,
  normalizeObjective,
  isLawFirmObjective,
  isStrOutboundObjective,
  latestContribution,
  classifyQueueItems,
  summarizeContributions,
  summarizeMissionSnapshot,
  pickCanonicalStrMission,
  prioritizationApprovalPending,
  canIssuePrioritizationApproval,
  discoveryInvestigationBlocker,
  isRecoverySummaryIncoherent,
  upstreamIncoherentStop,
  chooseNextRecoveryIntent,
  payloadForIntent,
  questionForIntent,
  assertNotSendingIntent,
};
