'use strict';

/**
 * SPEC-207 — Post-Prioritization Presentation Projection.
 * Render from committed Max PRIORITIZATION payload — never reconstruct from Scout.
 */

const {
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  STAGES,
} = require('./types');
const {
  deriveProgressionStage,
  deriveMissionPause,
  PROGRESSION_STAGES,
  PROGRESSION_STAGE_LABELS,
} = require('./MissionProgression');
const { presentableOperatorDecision } = require('./PendingOperatorDecision');

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function findLatestPrioritizationContribution(contributions = []) {
  return [...contributions]
    .reverse()
    .find(
      (row) => row.specialist === SPECIALISTS.MAX && row.kind === CONTRIBUTION_KINDS.PRIORITIZATION
    ) || null;
}

function resolvePrioritizationPayload(input = {}) {
  const executionResult = input.executionResult || {};
  const snapshot = input.snapshot || {};
  const contributions = snapshot.contributions || [];

  if (executionResult.prioritization && executionResult.prioritization.payload) {
    return executionResult.prioritization.payload;
  }
  if (executionResult.prioritizationPayload) {
    return executionResult.prioritizationPayload;
  }
  const row = findLatestPrioritizationContribution(contributions);
  return (row && row.payload) || {};
}

function formatPriorityLine(priority = {}, index = 0) {
  const rank = priority.rank != null ? priority.rank : index + 1;
  const name = asText(priority.name) || asText(priority.segment) || `Priority ${rank}`;
  const parts = [`${rank}. ${name}`];
  if (priority.rationale) parts.push(`   Why: ${priority.rationale}`);
  if (priority.confidence != null) parts.push(`   Confidence: ${Number(priority.confidence).toFixed(2)}`);
  if (priority.fit != null) parts.push(`   Fit: ${Number(priority.fit).toFixed(2)}`);
  if (priority.timing != null) parts.push(`   Timing: ${Number(priority.timing).toFixed(2)}`);
  return parts;
}

function formatDelegationLines(delegation = {}) {
  const lines = [];
  if (delegation.paige) lines.push(`Paige: ${delegation.paige}`);
  if (delegation.emmett) lines.push(`Emmett: ${delegation.emmett}`);
  return lines;
}

/**
 * @param {object} payload - committed Max PRIORITIZATION payload
 */
function presentationFromPrioritizationPayload(payload = {}) {
  const priorities = Array.isArray(payload.priorities) ? payload.priorities : [];
  const objectives = Array.isArray(payload.objectives) ? payload.objectives : [];
  const recommendations = Array.isArray(payload.recommendations)
    ? payload.recommendations.map((row) => (typeof row === 'string' ? row : row.text || row.label)).filter(Boolean)
    : [];
  const delegation = payload.delegation && typeof payload.delegation === 'object'
    ? payload.delegation
    : { paige: 'variants', emmett: 'capacity' };
  const evidence = Array.isArray(payload.evidence) ? payload.evidence : [];
  const buyingSignals = Array.isArray(payload.buyingSignals) ? payload.buyingSignals : [];

  return {
    priorities,
    objectives,
    objectiveReason: asText(payload.objectiveReason) || null,
    recommendations,
    delegation,
    timing: asText(payload.timing) || null,
    constraints: Array.isArray(payload.constraints) ? payload.constraints.filter(Boolean) : [],
    confidence: payload.confidence != null ? Number(payload.confidence) : null,
    evidence,
    buyingSignals,
  };
}

/**
 * Derive operator next step from canonical understand / outreach-planning state.
 * @param {object} snapshot
 * @param {object} [mission]
 */
function resolvePrioritizationApprovedNextStep(snapshot = {}, mission = {}) {
  const merged = {
    mission: mission.id ? mission : (snapshot.mission || mission),
    contributions: snapshot.contributions || [],
  };

  const pause = deriveMissionPause(merged);
  if (pause && pause.requiredDecision) {
    return pause.requiredDecision;
  }

  const presented = presentableOperatorDecision(merged);
  if (presented && presented.prompt) {
    return presented.prompt;
  }

  const progressionStage = deriveProgressionStage(merged);
  if (progressionStage === PROGRESSION_STAGES.OUTREACH_PLANNING) {
    return 'Continue to outreach planning.';
  }

  const label = PROGRESSION_STAGE_LABELS[progressionStage];
  return label ? `Continue to ${label.toLowerCase()}.` : 'Continue in mission workspace.';
}

/**
 * @param {ReturnType<typeof presentationFromPrioritizationPayload>} presentation
 * @returns {string[]}
 */
function formatPrioritizationResultsLines(presentation) {
  if (!presentation) return [];
  const lines = ['Max Prioritization', ''];

  if (presentation.priorities.length) {
    lines.push('Top Priorities');
    lines.push('');
    for (let i = 0; i < presentation.priorities.length; i += 1) {
      lines.push(...formatPriorityLine(presentation.priorities[i], i));
      lines.push('');
    }
  }

  if (presentation.objectiveReason) {
    lines.push('Why these targets');
    lines.push('');
    lines.push(presentation.objectiveReason);
    lines.push('');
  } else if (presentation.objectives.length) {
    lines.push('Objectives');
    lines.push('');
    for (const row of presentation.objectives) {
      lines.push(`• ${typeof row === 'string' ? row : row.text || row.label}`);
    }
    lines.push('');
  }

  if (presentation.recommendations.length) {
    lines.push('Recommended Next Action');
    lines.push('');
    lines.push(presentation.recommendations[0]);
    if (presentation.recommendations.length > 1) {
      for (const rec of presentation.recommendations.slice(1, 4)) {
        lines.push(`• ${rec}`);
      }
    }
    lines.push('');
  }

  const delegationLines = formatDelegationLines(presentation.delegation);
  if (delegationLines.length) {
    lines.push('Delegation');
    lines.push('');
    lines.push(...delegationLines);
    lines.push('');
  }

  if (presentation.confidence != null) {
    lines.push('Confidence');
    lines.push('');
    lines.push(Number(presentation.confidence).toFixed(2));
    lines.push('');
  }

  if (presentation.evidence.length) {
    lines.push('Evidence');
    lines.push('');
    for (const item of presentation.evidence.slice(0, 5)) {
      const label = typeof item === 'string' ? item : item.label || item.source || String(item);
      lines.push(`• ${label}`);
    }
    lines.push('');
  }

  if (presentation.buyingSignals.length) {
    lines.push('Buying Signals');
    lines.push('');
    for (const signal of presentation.buyingSignals.slice(0, 5)) {
      const label = typeof signal === 'string' ? signal : signal.label || String(signal);
      lines.push(`• ${label}`);
    }
    lines.push('');
  }

  return lines;
}

function formatPrioritizationResultsProse(payload) {
  return formatPrioritizationResultsLines(presentationFromPrioritizationPayload(payload))
    .join('\n')
    .trim();
}

module.exports = {
  findLatestPrioritizationContribution,
  resolvePrioritizationPayload,
  presentationFromPrioritizationPayload,
  resolvePrioritizationApprovedNextStep,
  formatPrioritizationResultsLines,
  formatPrioritizationResultsProse,
};
