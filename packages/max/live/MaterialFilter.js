'use strict';

const { MATERIAL_EVENT_TYPES } = require('./LiveTypes');

/**
 * Material notification filter (SPEC-011).
 * Never spam — only intelligence that materially changed.
 */

/**
 * @param {object} event
 * @returns {boolean}
 */
function isMaterialEvent(event) {
  if (!event) return false;
  if (event.material === true) return true;
  if (event.material === false) return false;
  return MATERIAL_EVENT_TYPES.has(String(event.type || ''));
}

/**
 * @param {object[]} events
 * @returns {object[]} notification view models
 */
function toNotifications(events) {
  return (events || []).filter(isMaterialEvent).map((ev) => ({
    id: ev.id,
    type: ev.type,
    severity: ev.severity,
    timestamp: ev.timestamp,
    summary: ev.summary,
    entity: ev.entity,
    seq: ev.seq,
  }));
}

module.exports = {
  isMaterialEvent,
  toNotifications,
};
