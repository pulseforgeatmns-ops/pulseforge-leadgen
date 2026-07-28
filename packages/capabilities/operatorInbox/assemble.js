'use strict';

/**
 * Assemble Operator Inbox package (SPEC-037 / ADR-024).
 */

const {
  ACTIVE_STATUSES,
  buildMissionInboxEvent,
  buildMissionTimelineEntry,
} = require('./types');
const { ingestWorkItems } = require('./ingest');
const { dedupeInboxItems } = require('./dedupe');
const { prioritizeItems, sortInboxItems } = require('./priority');

/**
 * @param {object} context
 * @param {object} [opts]
 * @returns {object}
 */
function assembleOperatorInbox(context, opts = {}) {
  const inputs = (context && context.inputs) || {};
  const operator = opts.operator || inputs.operator || 'operator';
  const now = new Date().toISOString();

  const existing = Array.isArray(opts.existingItems) ? opts.existingItems : [];
  const incoming =
    Array.isArray(opts.incoming) && opts.incoming.length
      ? opts.incoming
      : ingestWorkItems(context);

  const { items: deduped, merged, created } = dedupeInboxItems(
    existing,
    incoming
  );
  const prioritized = prioritizeItems(deduped, opts.now || new Date());
  const items = sortInboxItems(prioritized);
  const activeItems = items.filter((i) => ACTIVE_STATUSES.has(i.status));

  const missionEvents = [
    buildMissionInboxEvent({
      eventType: 'inbox_assembled',
      operator,
      timestamp: now,
      summary: `Inbox: ${activeItems.length} active item(s) (${created} new, ${merged} merged)`,
    }),
  ];

  const timeline = [
    buildMissionTimelineEntry({
      stage: 'operator_inbox',
      status: 'assembled',
      timestamp: now,
      summary: `${activeItems.length} outstanding work item(s)`,
      operator,
    }),
  ];

  return {
    items,
    activeItems,
    auditLog: Array.isArray(opts.auditLog) ? opts.auditLog : [],
    completionEvents: Array.isArray(opts.completionEvents)
      ? opts.completionEvents
      : [],
    missionEvents,
    timeline,
    summary: {
      totalItems: items.length,
      activeCount: activeItems.length,
      completedCount: items.filter((i) => i.status === 'completed').length,
      criticalCount: activeItems.filter((i) => i.priority === 'critical').length,
      highCount: activeItems.filter((i) => i.priority === 'high').length,
      created,
      merged,
      updatedAt: now,
    },
  };
}

module.exports = {
  assembleOperatorInbox,
};
