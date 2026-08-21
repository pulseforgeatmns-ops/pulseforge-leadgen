'use strict';

/**
 * SPEC-118 — shared mission context. The same mission follows every capability.
 */

const { SPECIALISTS, clone, asText } = require('./types');
const { formatMissionUnderstanding } = require('./StructuredMission');

function latest(rows, specialist, kind) {
  const match = [...rows].reverse().find((row) =>
    row.specialist === specialist && (!kind || row.kind === kind)
  );
  return match ? match.payload || {} : {};
}

function buildSharedContext(mission, contributions = []) {
  const scout = latest(contributions, SPECIALISTS.SCOUT);
  const max = latest(contributions, SPECIALISTS.MAX);
  const buyingSignals = scout.buyingSignals || scout.buying_signals || scout.signals || [];
  const evidence = scout.evidence || [];
  const constraints = [
    ...(mission.constraints || []),
    ...((max.constraints || []).map((row) => (typeof row === 'string' ? row : row.label || row.text)).filter(Boolean)),
  ];
  const structuredMission = mission.structuredMission || mission.missionPlanDraft || null;

  return {
    spec: 'SPEC-118',
    mission: {
      id: mission.id,
      title: mission.title,
      targetSegment: mission.targetSegment,
      objective: mission.objective,
      campaign: mission.campaign,
      priority: mission.priority,
      status: mission.status,
      stage: mission.stage,
      constraints,
      structuredMission: structuredMission ? clone(structuredMission) : null,
      missionUnderstanding: structuredMission
        ? formatMissionUnderstanding(structuredMission)
        : null,
    },
    buyingSignals: clone(buyingSignals),
    priorityReasoning: clone(max.recommendations || max.priorities || max.reasoning || []),
    evidence: clone(evidence),
    scout,
    max,
  };
}

function formatSharedContext(context) {
  const mission = context.mission || {};
  const lines = [
    'Mission',
    mission.title || 'Acquisition Mission',
    '',
    'Objective',
    mission.objective || '',
  ];
  if (mission.constraints && mission.constraints.length) {
    lines.push('', 'Constraints', ...mission.constraints);
  }
  if (mission.campaign) {
    lines.push('', 'Campaign', mission.campaign);
  }
  if (context.mission.missionUnderstanding) {
    const u = context.mission.missionUnderstanding;
    lines.push('', 'Market', u.market || '', '', 'Region', u.region || '', '', 'Buyer', u.buyer || '');
    if (u.constraints && u.constraints.length) {
      lines.push('', 'Constraints', ...u.constraints);
    }
  }
  if (context.buyingSignals && context.buyingSignals.length) {
    lines.push('', 'Buying signals', 'Scout evidence');
  }
  if (context.priorityReasoning && context.priorityReasoning.length) {
    lines.push('', 'Priority reasoning', 'Max evidence');
  }
  return lines.join('\n');
}

module.exports = {
  buildSharedContext,
  formatSharedContext,
};
