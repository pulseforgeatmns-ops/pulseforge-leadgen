'use strict';

/**
 * Growth Workspace left-panel rendering helpers (SPEC-088 polish).
 * Shared by /client-intel and Node regression tests.
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.PulseforgeGrowthWorkspacePanel = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  const OWNER_LABELS = Object.freeze({
    client_required: 'Client/operator',
    operator_guided: 'Operator guided',
    max_can_check: 'Max can check',
  });

  function escapeHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function formatOwnerLabel(owner) {
    if (owner == null || owner === '') return '';
    const key = String(owner);
    if (OWNER_LABELS[key]) return OWNER_LABELS[key];
    return key
      .split('_')
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(' ');
  }

  function sessionIdOf(session) {
    if (!session) return null;
    return session.sessionId || session.interviewId || session.id || null;
  }

  function isHistoricalPreviousPlan(session, currentSessionId) {
    if (!session) return false;
    const id = sessionIdOf(session);
    if (!id) return false;
    if (currentSessionId && String(id) === String(currentSessionId)) return false;
    // Active growth work for another session still counts as a previous plan
    // card, but never as task-guidance chrome for the current workspace.
    return true;
  }

  function filterPreviousPlans(sessions, currentSessionId) {
    const list = Array.isArray(sessions) ? sessions : [];
    return list.filter((s) => isHistoricalPreviousPlan(s, currentSessionId));
  }

  function planCardHtml(session, { primary } = {}) {
    if (!session) return '';
    const id = sessionIdOf(session);
    const sample = session.isSample
      ? '<span class="sample-badge">Sample / Dev</span>'
      : '';
    const plan = session.growthPlan || null;
    const pct =
      plan && typeof plan.percentComplete === 'number'
        ? plan.percentComplete
        : null;
    const done =
      session.resumeTarget === 'growth_complete' ||
      (plan && plan.status === 'complete');
    const currentTitle =
      plan && plan.currentTask && plan.currentTask.title
        ? plan.currentTask.title
        : done
          ? 'Growth Plan complete'
          : 'Ready to resume';
    const meta = [
      pct != null ? pct + '% complete' : null,
      primary ? currentTitle : done ? 'Completed plan' : 'Previous plan',
      session.blueprintVersion ? 'Blueprint v' + session.blueprintVersion : null,
    ]
      .filter(Boolean)
      .join(' · ');
    const cta = done ? 'View Completion' : 'Resume Growth Plan';
    const title = primary
      ? (session.businessName || 'Business') + ' · Growth Plan'
      : session.label ||
        (session.businessName || 'Business') + ' · Previous plan';

    return (
      '<article class="session-card' +
      (session.isSample ? ' sample' : '') +
      (primary ? ' current-plan-card' : ' previous-plan-card') +
      '" data-session-id="' +
      escapeHtml(id) +
      '" data-plan-role="' +
      (primary ? 'current' : 'previous') +
      '">' +
      '<p class="session-card-title">' +
      escapeHtml(title) +
      sample +
      '</p>' +
      '<p class="session-card-meta">' +
      escapeHtml(meta) +
      '</p>' +
      '<div class="session-card-actions">' +
      '<button type="button" data-action="continue" data-session-id="' +
      escapeHtml(id) +
      '">' +
      escapeHtml(cta) +
      '</button>' +
      '<button type="button" class="secondary" data-action="view" data-session-id="' +
      escapeHtml(id) +
      '">View Blueprint</button>' +
      (primary
        ? '<button type="button" class="secondary" data-action="new">Start New Interview</button>'
        : '') +
      '</div>' +
      '</article>'
    );
  }

  /**
   * Expanded guidance for the single active setup task.
   * Must never be nested under Previous Plans.
   */
  function taskGuidanceCardHtml(task) {
    if (!task) return '';
    const ownerLabel = formatOwnerLabel(task.owner || 'operator_guided');
    const mins = task.estimatedMinutes
      ? '<br>Estimated time · ' + escapeHtml(String(task.estimatedMinutes)) + ' minutes'
      : '';
    return (
      '<div class="current-task-card task-guidance-card" id="taskGuidanceCard" data-task-id="' +
      escapeHtml(task.id || '') +
      '" data-role="task-guidance">' +
      '<p class="kicker">Current Task Guidance</p>' +
      '<h3>' +
      escapeHtml(task.title || 'Next step') +
      '</h3>' +
      '<p class="task-meta">' +
      escapeHtml(task.description || 'Complete this recommendation, then mark it done to advance.') +
      mins +
      '<br>Owner · ' +
      escapeHtml(ownerLabel) +
      (task.priority ? '<br>Priority · ' + escapeHtml(String(task.priority)) : '') +
      '</p>' +
      '</div>'
    );
  }

  /**
   * Left panel for full-screen Growth Workspace.
   *
   * Sections:
   * 1. Current Growth Plan
   * 2. Current Task Guidance (only when guidanceOpen)
   * 3. Previous Plans (historical only; omitted when empty)
   */
  function renderGrowthWorkspaceLeftPanel(opts) {
    const options = opts && typeof opts === 'object' ? opts : {};
    const currentSession = options.currentSession || null;
    const currentSessionId =
      options.currentSessionId || sessionIdOf(currentSession);
    const previousSessions = filterPreviousPlans(
      options.previousSessions != null
        ? options.previousSessions
        : options.sessions,
      currentSessionId
    );
    const task = options.currentTask || null;
    const guidanceOpen = Boolean(options.guidanceOpen) && Boolean(task);
    const collapsePrevious = options.collapsePrevious !== false;

    let html =
      '<section class="gw-left-section" data-section="current-plan">' +
      '<p class="blueprint-empty gw-section-label" style="margin:0 0 0.25rem">Current Growth Plan</p>' +
      (currentSession
        ? planCardHtml(currentSession, { primary: true })
        : '<p class="blueprint-empty">No active Growth Plan.</p>') +
      '</section>';

    if (guidanceOpen) {
      html +=
        '<section class="gw-left-section" data-section="task-guidance">' +
        '<p class="blueprint-empty gw-section-label" style="margin:0.85rem 0 0.25rem">Current Task Guidance</p>' +
        taskGuidanceCardHtml(task) +
        '</section>';
    }

    if (previousSessions.length) {
      const body =
        '<div class="session-list">' +
        previousSessions
          .map((s) => planCardHtml(s, { primary: false }))
          .join('') +
        '</div>';
      if (collapsePrevious) {
        html +=
          '<details class="previous-plans" data-section="previous-plans">' +
          '<summary>Previous Plans (' +
          previousSessions.length +
          ')</summary>' +
          body +
          '</details>';
      } else {
        html +=
          '<section class="gw-left-section previous-plans" data-section="previous-plans">' +
          '<p class="blueprint-empty gw-section-label" style="margin:0.85rem 0 0.25rem">Previous Plans (' +
          previousSessions.length +
          ')</p>' +
          body +
          '</section>';
      }
    }

    return html;
  }

  /**
   * Count helpers for regression assertions (no DOM required).
   */
  function countMarkers(html, marker) {
    if (!html) return 0;
    let count = 0;
    let idx = 0;
    while (true) {
      const next = html.indexOf(marker, idx);
      if (next === -1) break;
      count += 1;
      idx = next + marker.length;
    }
    return count;
  }

  function analyzeLeftPanelHtml(html) {
    const source = String(html || '');
    const previousIdx = source.indexOf('data-section="previous-plans"');
    const previousHtml =
      previousIdx >= 0 ? source.slice(previousIdx) : '';
    return {
      currentPlanCards: countMarkers(source, 'data-plan-role="current"'),
      previousPlanCards: countMarkers(source, 'data-plan-role="previous"'),
      taskGuidanceCards: countMarkers(source, 'data-role="task-guidance"'),
      taskGuidanceInPreviousPlans: countMarkers(
        previousHtml,
        'data-role="task-guidance"'
      ),
      hasPreviousPlansSection: previousIdx >= 0,
      rawOwnerLeaks: Boolean(
        /\bclient_required\b|\boperator_guided\b|\bmax_can_check\b/.test(source)
      ),
    };
  }

  return {
    OWNER_LABELS,
    escapeHtml,
    formatOwnerLabel,
    sessionIdOf,
    filterPreviousPlans,
    planCardHtml,
    taskGuidanceCardHtml,
    renderGrowthWorkspaceLeftPanel,
    analyzeLeftPanelHtml,
  };
});
