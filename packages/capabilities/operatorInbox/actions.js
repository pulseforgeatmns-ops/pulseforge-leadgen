'use strict';

/**
 * Operator Inbox actions (SPEC-037 / ADR-024).
 * Coordination signals only — never runs workflow capabilities.
 */

const {
  INBOX_STATUS,
  ACTIVE_STATUSES,
  buildAuditEntry,
  buildCompletionEvent,
  buildMissionInboxEvent,
  buildMissionTimelineEntry,
} = require('./types');
const { validateInboxAction } = require('./validate');

function newId(prefix) {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * @param {unknown} raw
 * @returns {object[]}
 */
function normalizeActions(raw) {
  const inputs = raw && typeof raw === 'object' ? raw : {};
  if (Array.isArray(inputs.inboxActions)) return inputs.inboxActions;
  if (Array.isArray(inputs.actions)) return inputs.actions;
  if (inputs.action && typeof inputs.action === 'object') return [inputs.action];
  return [];
}

/**
 * Apply operator actions onto an inbox package.
 * @param {object} package_
 * @param {object[]} actions
 * @param {object} [opts]
 * @returns {{ package: object, errors: string[], changeSummary: string }}
 */
function applyInboxActions(package_, actions, opts = {}) {
  const operator = opts.operator || 'operator';
  const errors = [];
  const changes = [];
  let pkg = clonePackage(package_);

  for (const action of actions || []) {
    const type = String(action.type || action.action || '').toLowerCase();
    if (!type || type === 'ingest') continue;

    const item = findItem(pkg, action.itemId || action.id);
    const gate = validateInboxAction(item, type);
    if (!gate.ok) {
      errors.push(...gate.errors);
      continue;
    }

    const prev = item.status;

    if (type === 'open' || type === 'review') {
      item.status = INBOX_STATUS.IN_PROGRESS;
      item.updatedAt = now();
      pushAudit(pkg, item, type, prev, operator, action.notes);
      pkg.missionEvents.push(
        buildMissionInboxEvent({
          eventType: type === 'review' ? 'inbox_review' : 'inbox_opened',
          operator,
          itemId: item.id,
          summary: `${type}: ${item.title}`,
        })
      );
      changes.push(`${type}:${item.id}`);
      continue;
    }

    if (type === 'assign') {
      item.assignee =
        action.assignee != null ? String(action.assignee) : operator;
      item.updatedAt = now();
      pushAudit(pkg, item, type, prev, operator, `assigned:${item.assignee}`);
      changes.push(`assign:${item.id}:${item.assignee}`);
      continue;
    }

    if (type === 'snooze') {
      const until = action.until || action.snoozedUntil;
      if (!until) {
        errors.push('snooze_requires_until');
        continue;
      }
      item.status = INBOX_STATUS.SNOOZED;
      item.snoozedUntil = String(until);
      item.updatedAt = now();
      pushAudit(pkg, item, type, prev, operator, `until:${until}`);
      changes.push(`snooze:${item.id}`);
      continue;
    }

    if (type === 'reject') {
      item.status = INBOX_STATUS.REJECTED;
      item.updatedAt = now();
      pushAudit(pkg, item, type, prev, operator, action.notes);
      pkg.missionEvents.push(
        buildMissionInboxEvent({
          eventType: 'inbox_rejected',
          operator,
          itemId: item.id,
          summary: `Rejected: ${item.title}`,
        })
      );
      changes.push(`reject:${item.id}`);
      continue;
    }

    if (type === 'archive') {
      item.status = INBOX_STATUS.ARCHIVED;
      item.updatedAt = now();
      pushAudit(pkg, item, type, prev, operator, action.notes);
      changes.push(`archive:${item.id}`);
      continue;
    }

    if (type === 'approve' || type === 'complete') {
      item.status = INBOX_STATUS.COMPLETED;
      item.completedAt = now();
      item.completedBy = operator;
      item.updatedAt = item.completedAt;
      const result = type === 'approve' ? 'approved' : 'completed';
      pushAudit(pkg, item, type, prev, operator, action.notes);
      const completion = buildCompletionEvent({
        itemId: item.id,
        inboxKind: item.kind,
        missionId: item.missionId,
        operator,
        summary: `${result}: ${item.title}`,
        result,
      });
      pkg.completionEvents.push(completion);
      pkg.missionEvents.push(
        buildMissionInboxEvent({
          eventType: 'inbox_completed',
          operator,
          itemId: item.id,
          summary: completion.summary,
        })
      );
      pkg.timeline.push(
        buildMissionTimelineEntry({
          stage: 'operator_inbox',
          status: 'item_completed',
          summary: completion.summary,
          operator,
        })
      );
      changes.push(`${type}:${item.id}`);
      continue;
    }

    errors.push(`unknown_action:${type}`);
  }

  refreshSummary(pkg);
  return {
    package: pkg,
    errors,
    changeSummary: changes.join('; '),
  };
}

/**
 * @param {object} pkg
 * @param {object} item
 * @param {string} action
 * @param {string} prev
 * @param {string} operator
 * @param {string} [notes]
 */
function pushAudit(pkg, item, action, prev, operator, notes) {
  pkg.auditLog.push(
    buildAuditEntry({
      id: newId('aud'),
      itemId: item.id,
      action,
      previousStatus: prev,
      newStatus: item.status,
      operator,
      notes: notes || '',
    })
  );
}

/**
 * @param {object} pkg
 * @param {string} id
 * @returns {object|null}
 */
function findItem(pkg, id) {
  if (!id) return null;
  return (pkg.items || []).find((i) => i.id === String(id)) || null;
}

/**
 * @param {object} pkg
 */
function refreshSummary(pkg) {
  const items = pkg.items || [];
  const active = items.filter((i) => ACTIVE_STATUSES.has(i.status));
  pkg.activeItems = active;
  pkg.summary = {
    ...(pkg.summary || {}),
    totalItems: items.length,
    activeCount: active.length,
    completedCount: items.filter((i) => i.status === INBOX_STATUS.COMPLETED)
      .length,
    criticalCount: active.filter((i) => i.priority === 'critical').length,
    highCount: active.filter((i) => i.priority === 'high').length,
    updatedAt: now(),
  };
}

/**
 * @param {object} pkg
 * @returns {object}
 */
function clonePackage(pkg) {
  return {
    ...pkg,
    items: (pkg.items || []).map((i) => ({
      ...i,
      sources: [...(i.sources || [])],
      deepLink: i.deepLink ? { ...i.deepLink } : null,
    })),
    activeItems: [],
    auditLog: [...(pkg.auditLog || [])],
    completionEvents: [...(pkg.completionEvents || [])],
    missionEvents: [...(pkg.missionEvents || [])],
    timeline: [...(pkg.timeline || [])],
    summary: pkg.summary ? { ...pkg.summary } : {},
  };
}

function now() {
  return new Date().toISOString();
}

module.exports = {
  normalizeActions,
  applyInboxActions,
};
