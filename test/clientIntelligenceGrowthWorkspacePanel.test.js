'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  formatOwnerLabel,
  renderGrowthWorkspaceLeftPanel,
  analyzeLeftPanelHtml,
  taskGuidanceCardHtml,
} = require('../public/shared/growth-workspace-panel');

describe('Growth Workspace left panel', () => {
  it('formats readiness owner labels for operators', () => {
    assert.equal(formatOwnerLabel('client_required'), 'Client/operator');
    assert.equal(formatOwnerLabel('operator_guided'), 'Operator guided');
    assert.equal(formatOwnerLabel('max_can_check'), 'Max can check');
  });

  it('given one active task and no previous plans, renders one plan card and no Previous Plans', () => {
    const currentTask = {
      id: 'setup:domain_dns:branded_email',
      title: 'Branded email available',
      description: 'Create a branded mailbox (e.g. hello@domain).',
      owner: 'client_required',
      priority: 'high',
      estimatedMinutes: 3,
      resumeAction: 'setup_task',
    };
    const currentSession = {
      sessionId: 'sess-active',
      businessName: 'Anchor Cleaning',
      resumeTarget: 'growth_workspace',
      growthPlan: {
        percentComplete: 40,
        status: 'in_progress',
        currentTask,
      },
    };

    const closed = renderGrowthWorkspaceLeftPanel({
      currentSession,
      previousSessions: [],
      currentTask,
      guidanceOpen: false,
    });
    const closedStats = analyzeLeftPanelHtml(closed);
    assert.equal(closedStats.currentPlanCards, 1);
    assert.equal(closedStats.taskGuidanceCards, 0);
    assert.equal(closedStats.hasPreviousPlansSection, false);
    assert.equal(closedStats.taskGuidanceInPreviousPlans, 0);

    const opened = renderGrowthWorkspaceLeftPanel({
      currentSession,
      previousSessions: [],
      currentTask,
      guidanceOpen: true,
    });
    const openedStats = analyzeLeftPanelHtml(opened);
    assert.equal(openedStats.currentPlanCards, 1);
    assert.equal(openedStats.taskGuidanceCards, 1);
    assert.equal(openedStats.hasPreviousPlansSection, false);
    assert.equal(openedStats.taskGuidanceInPreviousPlans, 0);
    assert.equal(openedStats.rawOwnerLeaks, false);
    assert.match(opened, /Client\/operator/);
    assert.doesNotMatch(opened, /client_required/);
    assert.equal(
      (opened.match(/data-role="task-guidance"/g) || []).length,
      1,
      'exactly one expanded task guidance card after opening guidance'
    );
  });

  it('never nests active task guidance inside Previous Plans', () => {
    const currentTask = {
      id: 'setup:domain_dns:branded_email',
      title: 'Branded email available',
      description: 'Create a branded mailbox.',
      owner: 'client_required',
    };
    const currentSession = {
      sessionId: 'sess-active',
      businessName: 'Anchor Cleaning',
      growthPlan: { percentComplete: 40, status: 'in_progress', currentTask },
    };
    const previous = [
      {
        sessionId: 'sess-old',
        businessName: 'Old Co',
        label: 'Old Co · Previous plan',
        resumeTarget: 'growth_complete',
        growthPlan: { percentComplete: 100, status: 'complete', currentTask: null },
      },
    ];

    const html = renderGrowthWorkspaceLeftPanel({
      currentSession,
      previousSessions: previous,
      currentSessionId: 'sess-active',
      currentTask,
      guidanceOpen: true,
    });
    const stats = analyzeLeftPanelHtml(html);
    assert.equal(stats.currentPlanCards, 1);
    assert.equal(stats.previousPlanCards, 1);
    assert.equal(stats.taskGuidanceCards, 1);
    assert.equal(stats.taskGuidanceInPreviousPlans, 0);
    assert.equal(stats.hasPreviousPlansSection, true);
    assert.match(html, /Previous Plans \(1\)/);
    assert.doesNotMatch(
      html.slice(html.indexOf('data-section="previous-plans"')),
      /task-guidance|Branded email available/
    );
  });

  it('opening guidance twice still yields a single guidance card markup helper', () => {
    const task = {
      id: 'setup:domain_dns:branded_email',
      title: 'Branded email available',
      owner: 'client_required',
    };
    const once = taskGuidanceCardHtml(task);
    const twice = taskGuidanceCardHtml(task) + taskGuidanceCardHtml(task);
    assert.equal(analyzeLeftPanelHtml(once).taskGuidanceCards, 1);
    assert.equal(analyzeLeftPanelHtml(twice).taskGuidanceCards, 2);
    // Workspace renderer remains idempotent for a single open flag.
    const panel = renderGrowthWorkspaceLeftPanel({
      currentSession: {
        sessionId: 's1',
        businessName: 'Anchor Cleaning',
        growthPlan: { percentComplete: 10, status: 'in_progress', currentTask: task },
      },
      previousSessions: [],
      currentTask: task,
      guidanceOpen: true,
    });
    assert.equal(analyzeLeftPanelHtml(panel).taskGuidanceCards, 1);
  });

  it('client-intel wires the shared panel and Open Task Guidance', () => {
    const ui = fs.readFileSync(
      path.join(__dirname, '..', 'public', 'client-intel.html'),
      'utf8'
    );
    assert.match(ui, /growth-workspace-panel\.js/);
    assert.match(ui, /Open Task Guidance/);
    assert.match(ui, /taskGuidanceOpen/);
    assert.match(ui, /renderWorkspaceLeftNav/);
    assert.match(ui, /formatOwnerLabel/);
    assert.match(ui, /Client\/operator/);
  });
});
