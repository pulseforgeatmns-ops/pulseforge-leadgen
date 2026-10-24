'use strict';

/**
 * Operations (Mission Queue) — SPEC-022 Mission-First UX.
 * Lives beneath Highest Leverage Action on the Command Deck.
 */

const { buildOpenMissionAction } = require('../../workspace/MissionActions');

/**
 * @param {object} input
 * @param {object[]} [input.missions] - mission card payloads from MissionEngine.toCard
 * @param {string} [input.briefingId]
 * @param {string} [input.generatedAt]
 */
function composeOperations(input = {}) {
  const missions = Array.isArray(input.missions) ? input.missions : [];
  const generatedAt = input.generatedAt || new Date().toISOString();

  const cards = missions.map((m) => toOperationsCard(m, generatedAt));

  const summary = summarizeQueue(missions);

  return {
    operations: {
      id: 'operations',
      title: 'Operations',
      missions: cards,
      summary,
      updatedAt: generatedAt,
      empty: cards.length === 0,
      emptyMessage:
        'No active missions. Ask Max to build a campaign or discover prospects.',
    },
  };
}

/**
 * @param {object} mission - MissionEngine.toCard shape or raw mission
 */
function toOperationsCard(mission, updatedAt) {
  const progress = mission.progress || {};
  const status = String(mission.status || 'requested');
  const counts = progress.counts;
  const progressLabel = counts
    ? `${counts.completed} / ${counts.total}`
    : progress.totalSteps
      ? `${progress.completedSteps || 0} / ${progress.totalSteps}`
      : null;

  return {
    id: String(mission.id),
    title: String(mission.title || mission.objectiveText || 'Mission'),
    status,
    statusLabel: statusLabel(status),
    progress: {
      percent: progress.percent || 0,
      completedSteps: progress.completedSteps || 0,
      totalSteps: progress.totalSteps || 0,
      currentStage: progress.currentStage || null,
      label: progressLabel,
      counts: counts || null,
      stageOutcome: progress.stageOutcome || null,
      stageOutcomeLabel: progress.stageOutcomeLabel || null,
    },
    blockingIssues: mission.blockingIssues ||
      (mission.stageReview && mission.stageReview.blockingIssues) ||
      null,
    discoveryProfile: mission.discoveryProfile
      ? {
          name: mission.discoveryProfile.name,
          reason: mission.discoveryProfile.reason || null,
          confidence: mission.discoveryProfile.confidence,
          geography: mission.discoveryProfile.geography || null,
        }
      : null,
    startedAt: mission.startedAt || mission.createdAt || null,
    estimatedCompletion: mission.estimatedCompletion || null,
    createdAt: mission.createdAt || null,
    type: mission.type || null,
    runtime: mission.runtime || null,
    updatedAt,
    actions: [
      buildOpenMissionAction({
        missionId: mission.id,
        runtime: mission.runtime,
        label: 'Expand',
        id: `expand_${mission.id}`,
      }),
    ],
  };
}

function statusLabel(status) {
  const map = {
    requested: 'Requested',
    planning: 'Planning',
    executing: 'Running',
    waiting: 'Blocked',
    review_required: 'Needs attention',
    completed: 'Completed',
    reviewed: 'Reviewed',
    archived: 'Archived',
    failed: 'Failed',
  };
  return map[status] || status;
}

function summarizeQueue(missions) {
  const active = missions.filter((m) =>
    ['requested', 'planning', 'executing'].includes(m.status)
  ).length;
  const attention = missions.filter((m) =>
    ['review_required', 'waiting'].includes(m.status)
  ).length;
  const finished = missions.filter((m) =>
    ['completed', 'reviewed', 'archived'].includes(m.status)
  ).length;
  return {
    active,
    needsAttention: attention,
    finished,
    blocked: missions.filter((m) => m.status === 'waiting').length,
  };
}

module.exports = {
  composeOperations,
  toOperationsCard,
  statusLabel,
  summarizeQueue,
};
