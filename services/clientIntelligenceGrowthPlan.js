'use strict';

/**
 * SPEC-088 — Growth Work Continuation Flow.
 *
 * Builds a guided Growth Plan from an approved CIE session so "Resume Growth
 * Plan" lands on the first incomplete task instead of a readiness report dead end.
 */

const ESTIMATES = Object.freeze({
  growth_conversation: 10,
  infrastructure_readiness: 8,
  setup_default: 3,
});

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function growthWorkState(session) {
  const state = (session && session.interview_state) || {};
  const gw = state.growthWork && typeof state.growthWork === 'object' ? state.growthWork : {};
  return {
    completedTaskIds: asArray(gw.completedTaskIds).map(String),
    history: asArray(gw.history),
    activeTaskId: gw.activeTaskId ? String(gw.activeTaskId) : null,
    updatedAt: gw.updatedAt || null,
  };
}

function firstGrowthPlanPreview(session) {
  const state = (session && session.interview_state) || {};
  const growth = state.growthConversation || null;
  return (
    (growth && (growth.first_growth_plan_preview || growth.firstGrowthPlanPreview)) ||
    state.firstGrowthPlanPreview ||
    null
  );
}

function readinessReport(session) {
  const state = (session && session.interview_state) || {};
  return state.growthInfrastructureReadinessReport || null;
}

function growthConversation(session) {
  const state = (session && session.interview_state) || {};
  return state.growthConversation || null;
}

function isSetupItemComplete(item, completedIds, taskId) {
  if (!item) return true;
  if (item.status === 'ready' || item.status === 'not_applicable') return true;
  if (completedIds.has(taskId)) return true;
  return false;
}

function setupTaskId(areaId, itemId) {
  return `setup:${areaId}:${itemId}`;
}

function estimateForSetup(item) {
  if (!item) return ESTIMATES.setup_default;
  if (item.priority === 'high') return 3;
  if (item.priority === 'medium') return 4;
  return 5;
}

/**
 * Ordered Growth Plan tasks for an approved session.
 * Milestone conversations first, then readiness setup sequence gaps.
 */
function buildGrowthPlanTasks(session, opts = {}) {
  const state = (session && session.interview_state) || {};
  const gw = growthWorkState(session);
  const completedIds = new Set(gw.completedTaskIds);
  const preview = firstGrowthPlanPreview(session);
  const growth = growthConversation(session);
  const report = readinessReport(session);
  const tasks = [];

  const growthComplete =
    Boolean(preview) || completedIds.has('milestone:growth_conversation');
  const growthStarted = Boolean(
    growth &&
      (growth.status ||
        (Array.isArray(growth.turns) && growth.turns.length) ||
        growth.startedAt)
  );
  tasks.push({
    id: 'milestone:growth_conversation',
    type: 'milestone',
    title: growthComplete
      ? 'First Growth Plan focus'
      : growthStarted
        ? 'Finish Growth Conversation'
        : 'Start Growth Conversation',
    description: growthComplete
      ? 'First market focus is defined.'
      : 'Choose the first market segment and validation target.',
    status: growthComplete ? 'complete' : growthStarted ? 'in_progress' : 'incomplete',
    estimatedMinutes: ESTIMATES.growth_conversation,
    resumeAction: 'growth_conversation',
    artifact: 'firstGrowthPlanPreview',
  });

  const readinessComplete =
    Boolean(report) || completedIds.has('milestone:infrastructure_readiness');
  const readiness = state.infrastructureReadiness || null;
  const readinessStarted = Boolean(
    readiness &&
      (readiness.status ||
        readiness.startedAt ||
        (Array.isArray(readiness.turns) && readiness.turns.length))
  );
  tasks.push({
    id: 'milestone:infrastructure_readiness',
    type: 'milestone',
    title: readinessComplete
      ? 'Infrastructure readiness assessed'
      : readinessStarted
        ? 'Finish Infrastructure Readiness'
        : 'Check Growth Infrastructure',
    description: readinessComplete
      ? 'Capture/convert setup has been assessed.'
      : 'Confirm the business can capture, convert, and track demand.',
    status: readinessComplete
      ? 'complete'
      : readinessStarted
        ? 'in_progress'
        : 'incomplete',
    estimatedMinutes: ESTIMATES.infrastructure_readiness,
    resumeAction: 'infrastructure_readiness',
    artifact: 'growthInfrastructureReadinessReport',
  });

  const sequence = asArray(report && report.recommendedSetupSequence);
  for (const step of sequence) {
    if (!step || !step.areaId || !step.itemId) continue;
    const id = setupTaskId(step.areaId, step.itemId);
    let item = null;
    if (
      report.areas &&
      report.areas[step.areaId] &&
      report.areas[step.areaId].items
    ) {
      item = report.areas[step.areaId].items[step.itemId] || null;
    }
    const complete = isSetupItemComplete(item || step, completedIds, id);
    tasks.push({
      id,
      type: 'setup',
      title: step.label || step.action || id,
      description: step.action || (item && item.recommended_next_step) || '',
      status: complete ? 'complete' : 'incomplete',
      estimatedMinutes: estimateForSetup(item || step),
      resumeAction: 'setup_task',
      areaId: step.areaId,
      itemId: step.itemId,
      owner: step.owner || (item && item.owner) || null,
      priority: step.priority || (item && item.priority) || null,
      readinessStatus: (item && item.status) || step.status || null,
      statusLabel: (item && item.statusLabel) || step.statusLabel || null,
    });
  }

  // Optional terminal planning cue once setup is clear — never auto-launches campaigns.
  const setupTasks = tasks.filter((t) => t.type === 'setup');
  const setupDone =
    setupTasks.length > 0 && setupTasks.every((t) => t.status === 'complete');
  if (readinessComplete && (setupTasks.length === 0 || setupDone)) {
    const launchId = 'milestone:campaign_ready';
    const launchComplete = completedIds.has(launchId);
    tasks.push({
      id: launchId,
      type: 'milestone',
      title: 'Choose next growth objective',
      description:
        'All current recommendations are complete. Pick the next objective when ready.',
      status: launchComplete ? 'complete' : 'incomplete',
      estimatedMinutes: 2,
      resumeAction: 'growth_complete',
      artifact: null,
    });
  }

  if (opts.includeCompleted === false) {
    return tasks.filter((t) => t.status !== 'complete');
  }
  return tasks;
}

function findFirstIncompleteTask(tasks) {
  return (
    asArray(tasks).find(
      (t) => t && (t.status === 'incomplete' || t.status === 'in_progress')
    ) || null
  );
}

function percentComplete(tasks) {
  const list = asArray(tasks);
  if (!list.length) return 0;
  const done = list.filter((t) => t.status === 'complete').length;
  return Math.round((done / list.length) * 100);
}

function businessNameFrom(session, blueprint) {
  const state = (session && session.interview_state) || {};
  if (state.businessName) return String(state.businessName).trim();
  const facts = state.normalizedFacts || {};
  if (facts.business_name) return String(facts.business_name).trim();
  if (blueprint && blueprint.sections && blueprint.sections.identity) {
    const summary = blueprint.sections.identity.summary || '';
    const m = String(summary).match(/^([^.!?\n]{2,80})/);
    if (m) return m[1].trim();
  }
  return 'Growth';
}

/**
 * Full Growth Plan workspace model.
 */
function buildGrowthPlan(session, blueprint, opts = {}) {
  const tasks = buildGrowthPlanTasks(session, opts);
  const currentTask = findFirstIncompleteTask(tasks);
  const currentIdx = currentTask
    ? tasks.findIndex((t) => t.id === currentTask.id)
    : -1;
  const nextTask =
    currentIdx >= 0
      ? tasks.slice(currentIdx + 1).find((t) => t.status !== 'complete') || null
      : null;
  const afterTask =
    currentIdx >= 0
      ? tasks
          .slice(currentIdx + 1)
          .filter((t) => t.status !== 'complete')
          .slice(1, 2)[0] || null
      : null;
  const pct = percentComplete(tasks);
  const complete = !currentTask;
  const name = businessNameFrom(session, blueprint);
  const report = readinessReport(session);
  const preview = firstGrowthPlanPreview(session);
  const gw = growthWorkState(session);

  return {
    kind: 'growth_plan',
    title: `${name} Growth Plan`,
    businessName: name,
    sessionId: session && session.id,
    clientId: session && session.client_id,
    percentComplete: pct,
    status: complete ? 'complete' : 'in_progress',
    tasks,
    currentTask,
    nextTask,
    afterTask,
    completedTaskIds: gw.completedTaskIds,
    history: gw.history,
    activeTaskId: (currentTask && currentTask.id) || gw.activeTaskId || null,
    emptyRecommendations: complete,
    completionOptions: complete
      ? [
          { id: 'launch_campaign', label: 'Launch New Campaign' },
          { id: 'expand_market', label: 'Expand Market' },
          { id: 'improve_conversion', label: 'Improve Conversion' },
          { id: 'new_growth_plan', label: 'Create New Growth Plan' },
        ]
      : [],
    artifacts: {
      hasBlueprint: Boolean(blueprint),
      hasInitialGrowthDirection: Boolean(
        session &&
          session.interview_state &&
          session.interview_state.initialGrowthDirection
      ),
      hasFirstGrowthPlanPreview: Boolean(preview),
      hasReadinessReport: Boolean(report),
      readinessOverallStatus: (report && report.overallStatus) || null,
    },
    updatedAt: gw.updatedAt || (session && session.updated_at) || null,
  };
}

/**
 * Resume target for Continue / Resume Growth Plan.
 * Always prefers the workspace (or completion) over a report-only dead end.
 */
function resolveGrowthPlanResumeTarget(session, blueprint) {
  if (!session || session.status !== 'APPROVED') return null;
  const plan = buildGrowthPlan(session, blueprint);
  if (plan.status === 'complete') return 'growth_complete';
  return 'growth_workspace';
}

function applyTaskCompletion(session, taskId, opts = {}) {
  const id = String(taskId || '');
  if (!id) {
    const err = new Error('taskId is required');
    err.code = 'invalid_task';
    err.status = 400;
    throw err;
  }
  const plan = buildGrowthPlan(session, opts.blueprint);
  const task = plan.tasks.find((t) => t.id === id);
  if (!task) {
    const err = new Error('Growth Plan task not found');
    err.code = 'task_not_found';
    err.status = 404;
    throw err;
  }

  const state = { ...(session.interview_state || {}) };
  const gw = growthWorkState(session);
  const completedTaskIds = Array.from(
    new Set(gw.completedTaskIds.concat(id).map(String))
  );
  const history = gw.history.concat({
    taskId: id,
    title: task.title,
    completedAt: new Date().toISOString(),
    note: opts.note ? String(opts.note).slice(0, 500) : null,
    source: opts.source || 'operator',
  });

  // Soft-close readiness gaps when operator marks a setup task done.
  const report = state.growthInfrastructureReadinessReport;
  if (task.type === 'setup' && report && report.areas && task.areaId && task.itemId) {
    const areas = JSON.parse(JSON.stringify(report.areas));
    if (areas[task.areaId] && areas[task.areaId].items && areas[task.areaId].items[task.itemId]) {
      areas[task.areaId].items[task.itemId] = {
        ...areas[task.areaId].items[task.itemId],
        status: 'ready',
        statusLabel: 'Ready',
        evidence: [
          ...asArray(areas[task.areaId].items[task.itemId].evidence),
          {
            kind: 'operator_marked_complete',
            at: new Date().toISOString(),
            note: opts.note || 'Marked complete in Growth Workspace',
          },
        ],
      };
      state.growthInfrastructureReadinessReport = {
        ...report,
        areas,
        recommendedSetupSequence: asArray(report.recommendedSetupSequence).map((step) => {
          if (step.areaId === task.areaId && step.itemId === task.itemId) {
            return { ...step, status: 'ready', statusLabel: 'Ready' };
          }
          return step;
        }),
      };
    }
  }

  state.growthWork = {
    completedTaskIds,
    history,
    activeTaskId: null,
    updatedAt: new Date().toISOString(),
  };

  const nextSession = { ...session, interview_state: state };
  const nextPlan = buildGrowthPlan(nextSession, opts.blueprint);
  return {
    interview_state: state,
    growthPlan: nextPlan,
    completedTask: task,
    nextTask: nextPlan.currentTask,
  };
}

module.exports = {
  ESTIMATES,
  growthWorkState,
  buildGrowthPlanTasks,
  buildGrowthPlan,
  findFirstIncompleteTask,
  percentComplete,
  resolveGrowthPlanResumeTarget,
  applyTaskCompletion,
  setupTaskId,
};
