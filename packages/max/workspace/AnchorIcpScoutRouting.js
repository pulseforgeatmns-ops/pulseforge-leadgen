'use strict';

/**
 * Anchor Cleaning — separate ICP/strategy decisions from Scout discovery execution.
 * Combined operator turns record mission context first, then run discovery when requested.
 * Must not treat category batch targets as an operator ProspectList paste.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const { resolveAcquisitionActiveMission } = require('./ActiveMissionGuard');
const { resolveAcquisitionMissionRuntime } = require('../../../services/acquisitionMissionRuntime');
const { runScoutForAmoMission } = require('./ScoutDiscoveryExecutor');
const {
  looksLikeProspectingCriteriaMessage,
  isScoutDiscoveryExecutionMessage,
} = require('../../mission-engine/OperatorArtifactInjection');

const ANCHOR_TENANT_IDS = new Set(['10']);

const ICP_DECISION_SIGNALS_RE =
  /\b(?:\bicp\b|ideal customer|prospecting criteria|scoring rule|deprioriti|priority lanes?|target segments?|need-threshold|refining anchor|small professional offices|higher-need recurring)\b/i;

function normalizeText(text) {
  return String(text || '')
    .replace(/\s+/g, ' ')
    .trim();
}

function resolveTenantId(input = {}) {
  const session = input.session || null;
  const ctx =
    (input.context && typeof input.context === 'object' ? input.context : null) ||
    (session && session.context) ||
    {};
  return String(
    input.authorizedTenantId ||
      ctx.tenantId ||
      ctx.clientId ||
      session?.context?.tenantId ||
      ''
  ).trim();
}

function mentionsAnchorCleaning(text) {
  return /\banchor cleaning\b/i.test(String(text || ''));
}

function isAnchorTenantContext(input = {}, question = '') {
  const tenantId = resolveTenantId(input);
  if (ANCHOR_TENANT_IDS.has(tenantId)) return true;
  return mentionsAnchorCleaning(question);
}

function isIcpDecisionUpdateMessage(text) {
  const q = normalizeText(text);
  if (!q) return false;
  if (looksLikeProspectingCriteriaMessage(q)) return true;
  if (/^\s*scout\s*:\s*update\b/i.test(q) && ICP_DECISION_SIGNALS_RE.test(q)) {
    return true;
  }
  return ICP_DECISION_SIGNALS_RE.test(q) && isScoutDiscoveryExecutionMessage(q);
}

function shouldHandleAnchorIcpScoutCombinedTurn(input = {}) {
  const question = normalizeText(input.question);
  if (!question || !isAnchorTenantContext(input, question)) return false;
  if (!isIcpDecisionUpdateMessage(question)) return false;
  return (
    isScoutDiscoveryExecutionMessage(question) ||
    /^\s*scout\s*:/i.test(question) ||
    /\b(?:decision|first controlled batch)\s*:/i.test(question)
  );
}

function parseBatchComposition(text) {
  const raw = String(text || '');
  const match = raw.match(
    /\b(?:first\s+)?controlled\s+batch\s*:\s*([^\n.]+)/i
  );
  if (!match) return null;
  const segment = String(match[1] || '').trim();
  const parts = segment
    .split(',')
    .map((p) => p.trim())
    .filter(Boolean);
  if (!parts.length) return null;
  return parts.map((part) => {
    const m = part.match(/^(\d+)\s+(.+)$/i);
    if (m) {
      return { count: Number(m[1]), lane: m[2].trim() };
    }
    return { count: null, lane: part };
  });
}

function parseAnchorIcpDecision(question) {
  const q = String(question || '');
  const deprioritizeMatch = q.match(
    /\b(?:away from|do not prioritize|deprioriti\w+)\s+([^.\n]+)/i
  );
  const priorityMatch = q.match(
    /\b(?:priority lanes?|new priority lanes?)\s*(?:are|=|:)\s*([^.\n]+)/i
  );
  const regionMatch = q.match(/\bregion\s*:\s*([^.\n]+)/i);
  const outreachDisabled =
    /\boutreach\s+remains\s+disabled\b/i.test(q) ||
    /\b(?:no|without)\s+outreach\b/i.test(q);

  return {
    summary:
      deprioritizeMatch && deprioritizeMatch[1]
        ? deprioritizeMatch[1].trim()
        : null,
    priorityLanes: priorityMatch && priorityMatch[1] ? priorityMatch[1].trim() : null,
    region: regionMatch && regionMatch[1] ? regionMatch[1].trim() : null,
    batchComposition: parseBatchComposition(q),
    outreachDisabled,
    scoutExecutionRequested: isScoutDiscoveryExecutionMessage(q) || /^\s*scout\s*:/im.test(q),
    rawOperatorText: q.trim(),
  };
}

function composeAnchorIcpScoutAcknowledgement(decision, { scoutStarted = false } = {}) {
  const lines = [];
  const awayFrom = decision.summary
    ? decision.summary.replace(/^anchor['']s\s+icp\s+/i, '').trim()
    : 'small professional offices';
  lines.push(
    `Decision recorded: Anchor's ICP has been updated away from ${awayFrom} and toward higher-need recurring facilities.`
  );
  if (decision.priorityLanes) {
    lines.push(`Priority lanes: ${decision.priorityLanes}.`);
  }
  if (decision.batchComposition && decision.batchComposition.length) {
    const batchLabel = decision.batchComposition
      .map((row) => (row.count != null ? `${row.count} ${row.lane}` : row.lane))
      .join(', ');
    lines.push(`Controlled batch targets: ${batchLabel}.`);
  }
  if (scoutStarted) {
    lines.push('Scout discovery started using the updated ICP.');
  } else if (decision.scoutExecutionRequested) {
    lines.push('Scout discovery is queued using the updated ICP.');
  }
  if (decision.outreachDisabled) {
    lines.push('Outreach remains disabled.');
  }
  lines.push("I'll return the controlled prospect batch for operator review.");
  return lines.join('\n\n');
}

async function resolveAnchorMission(input = {}, engine, tenantId) {
  if (input.mission && input.mission.id) return input.mission;
  const amoResolution = await resolveAcquisitionActiveMission({
    ...input,
    question: input.question,
    tenantId,
  });
  if (amoResolution.mission) return amoResolution.mission;
  const missions = engine.list(tenantId) || [];
  return missions[0] || null;
}

async function persistIcpDecision(mission, decision, runtime, tenantId) {
  const payload = {
    specialist: 'max',
    kind: 'constraints',
    payload: {
      source: 'operator_icp_update',
      anchorProspectingCriteria: true,
      strategicContext: {
        icpDecisionSummary: decision.summary,
        priorityLanes: decision.priorityLanes,
        region: decision.region,
        batchComposition: decision.batchComposition,
        outreachDisabled: decision.outreachDisabled,
        operatorText: decision.rawOperatorText,
      },
      constraints: decision.outreachDisabled ? ['outreach_disabled'] : [],
    },
  };
  return runtime.contribute(mission.id, payload, { tenantId });
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleAnchorIcpScoutTurn(input = {}) {
  const question = normalizeText(input.question);
  if (!shouldHandleAnchorIcpScoutCombinedTurn(input)) return null;

  const tenantId = resolveTenantId(input);
  if (!tenantId) return null;

  const runtime = resolveAcquisitionMissionRuntime(input);
  const engine = runtime.engine();
  const mission = await resolveAnchorMission(input, engine, tenantId);
  if (!mission) return null;

  const decision = parseAnchorIcpDecision(question);
  await persistIcpDecision(mission, decision, runtime, tenantId);

  let scoutStarted = false;
  if (decision.scoutExecutionRequested) {
    const scoutResult = await runScoutForAmoMission(mission, {
      engine,
      tenantId,
      allowFixtureFallback: input.allowFixtureFallback === true,
      runScout: input.runScout,
      fixtureScoutDiscoveryResult: input.fixtureScoutDiscoveryResult,
    });
    scoutStarted = Boolean(
      scoutResult &&
        scoutResult.status !== 'blocked' &&
        scoutResult.status !== 'failed'
    );
  }

  const prose = composeAnchorIcpScoutAcknowledgement(decision, { scoutStarted });
  const structured = buildStructuredResponse({
    answer: prose,
    reasoning: [
      'Recorded operator ICP/strategy decision on the acquisition mission.',
      decision.scoutExecutionRequested
        ? 'Dispatched Scout discovery using the updated criteria.'
        : 'No Scout execution language detected beyond the decision update.',
    ],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: 0.92,
    nextInvestigations: [],
    recommendedActions: [],
    confidenceContributors: ['anchor_icp_scout_routing'],
    timelineReferences: [],
    relatedEntities: [
      {
        id: mission.id,
        type: 'acquisition_mission',
        name: mission.title || mission.id,
      },
    ],
    metadata: {
      anchorIcpScoutRouting: true,
      icpDecisionRecorded: true,
      scoutDiscoveryStarted: scoutStarted,
      outreachDisabled: decision.outreachDisabled,
      batchComposition: decision.batchComposition,
      suppressedProspectListDetection: true,
    },
  });

  return {
    reason: 'anchor_icp_scout_combined',
    prose,
    structured,
    mission,
    decision,
    scoutStarted,
  };
}

module.exports = {
  shouldHandleAnchorIcpScoutCombinedTurn,
  isIcpDecisionUpdateMessage,
  parseAnchorIcpDecision,
  composeAnchorIcpScoutAcknowledgement,
  maybeHandleAnchorIcpScoutTurn,
};
