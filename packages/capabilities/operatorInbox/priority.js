'use strict';

/**
 * Deterministic inbox prioritization (SPEC-037).
 * Same inputs → same priority.
 */

const {
  INBOX_CATEGORIES,
  INBOX_PRIORITY,
  PRIORITY_ORDER,
  KIND_CATEGORY,
} = require('./types');

/**
 * Compute priority for an inbox item using deterministic business rules.
 * @param {object} item
 * @param {object} [now]
 * @returns {string}
 */
function computePriority(item, now = new Date()) {
  if (!item || typeof item !== 'object') return INBOX_PRIORITY.NORMAL;

  const category =
    item.category || KIND_CATEGORY[item.kind] || INBOX_CATEGORIES.ACTION_REQUIRED;
  const due = item.dueDate ? new Date(item.dueDate) : null;
  const ts = now instanceof Date ? now : new Date(now);
  const overdue = due && !Number.isNaN(due.getTime()) && due.getTime() < ts.getTime();
  const blocking = item.blocking === true || item.attributes?.blocking === true;

  // Critical: blocking approval / overdue critical work / overdue approvals
  if (
    blocking ||
    (overdue &&
      (category === INBOX_CATEGORIES.APPROVAL_REQUIRED ||
        category === INBOX_CATEGORIES.DECISION_REQUIRED ||
        category === INBOX_CATEGORIES.REVIEW_REQUIRED))
  ) {
    return INBOX_PRIORITY.CRITICAL;
  }

  if (category === INBOX_CATEGORIES.APPROVAL_REQUIRED) {
    return INBOX_PRIORITY.HIGH;
  }

  if (category === INBOX_CATEGORIES.DECISION_REQUIRED) {
    return item.evidenceBacked === false
      ? INBOX_PRIORITY.NORMAL
      : INBOX_PRIORITY.HIGH;
  }

  if (category === INBOX_CATEGORIES.REVIEW_REQUIRED) {
    return overdue ? INBOX_PRIORITY.CRITICAL : INBOX_PRIORITY.HIGH;
  }

  if (category === INBOX_CATEGORIES.ACTION_REQUIRED) {
    return overdue ? INBOX_PRIORITY.HIGH : INBOX_PRIORITY.NORMAL;
  }

  if (category === INBOX_CATEGORIES.COMPLETED) {
    return INBOX_PRIORITY.LOW;
  }

  return INBOX_PRIORITY.NORMAL;
}

/**
 * Sort active items: priority asc, then dueDate asc (nulls last), then createdAt asc.
 * @param {object[]} items
 * @returns {object[]}
 */
function sortInboxItems(items) {
  const list = Array.isArray(items) ? [...items] : [];
  return list.sort((a, b) => {
    const pa = PRIORITY_ORDER[a.priority] ?? 99;
    const pb = PRIORITY_ORDER[b.priority] ?? 99;
    if (pa !== pb) return pa - pb;
    const da = a.dueDate ? Date.parse(a.dueDate) : Number.POSITIVE_INFINITY;
    const db = b.dueDate ? Date.parse(b.dueDate) : Number.POSITIVE_INFINITY;
    if (da !== db) return da - db;
    const ca = a.createdAt ? Date.parse(a.createdAt) : 0;
    const cb = b.createdAt ? Date.parse(b.createdAt) : 0;
    return ca - cb;
  });
}

/**
 * Apply computed priority onto items (immutable copy).
 * @param {object[]} items
 * @param {Date|string} [now]
 * @returns {object[]}
 */
function prioritizeItems(items, now) {
  return (Array.isArray(items) ? items : []).map((item) => ({
    ...item,
    priority: computePriority(item, now),
  }));
}

module.exports = {
  computePriority,
  sortInboxItems,
  prioritizeItems,
};
