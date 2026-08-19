'use strict';

/**
 * SPEC-118 — mission workspace. Nothing is hidden.
 */

const { SPECIALISTS, clamp } = require('./types');
const { specialistState } = require('./Lifecycle');

function bar(percent) {
  const filled = Math.round(clamp(percent, 0, 100) / 10);
  return `${'█'.repeat(filled)}${'░'.repeat(10 - filled)}`;
}

function buildWorkspace(mission, ctx) {
  const specialists = [SPECIALISTS.SCOUT, SPECIALISTS.MAX, SPECIALISTS.PAIGE, SPECIALISTS.EMMETT, SPECIALISTS.OPERATOR]
    .map((id) => {
      const state = specialistState(id, ctx, mission);
      return { id, ...state };
    });

  return {
    spec: 'SPEC-118',
    missionId: mission.id,
    title: mission.title,
    status: mission.status,
    stage: mission.stage,
    progressPercent: mission.progressPercent,
    bar: bar(mission.progressPercent),
    specialists,
    scout: specialists.find((row) => row.id === SPECIALISTS.SCOUT),
    max: specialists.find((row) => row.id === SPECIALISTS.MAX),
    paige: specialists.find((row) => row.id === SPECIALISTS.PAIGE),
    emmett: specialists.find((row) => row.id === SPECIALISTS.EMMETT),
    operator: specialists.find((row) => row.id === SPECIALISTS.OPERATOR),
  };
}

function formatWorkspace(workspace) {
  const lines = [
    'Mission',
    '',
    workspace.title,
    '',
    '──────────────────────',
    '',
    'Status',
    '',
    workspace.status,
    '',
    'Progress',
    '',
    workspace.bar,
    '',
    `${workspace.progressPercent}%`,
    '',
  ];
  for (const row of workspace.specialists) {
    const mark = row.state === 'complete' || row.state === 'approved' ? '✓ ' : '';
    lines.push(capitalize(row.id), '', `${mark}${row.label}`, '');
  }
  return lines.join('\n').trim();
}

function capitalize(value) {
  return String(value || '').replace(/^./, (ch) => ch.toUpperCase());
}

module.exports = {
  bar,
  buildWorkspace,
  formatWorkspace,
};
