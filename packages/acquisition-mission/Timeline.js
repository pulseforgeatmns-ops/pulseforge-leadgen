'use strict';

/**
 * SPEC-118 — chronological mission timeline.
 */

const { EVENT_KINDS, asText, nowIso, newId, formatClock, clone } = require('./types');

const DEFAULT_LABELS = Object.freeze({
  [EVENT_KINDS.MISSION_CREATED]: 'Mission Created',
  [EVENT_KINDS.STAGE_TRANSITION]: 'Stage advanced',
  [EVENT_KINDS.CONTRIBUTION]: 'Contribution attached',
  [EVENT_KINDS.CONTRACT_REJECTED]: 'Contract rejected',
  [EVENT_KINDS.BLOCKER_SET]: 'Blocker set',
  [EVENT_KINDS.BLOCKER_CLEARED]: 'Blocker cleared',
  [EVENT_KINDS.OBSERVATION]: 'Observation recorded',
  [EVENT_KINDS.OUTCOME]: 'Outcome recorded',
  [EVENT_KINDS.OPERATOR_EDIT]: 'Operator edited',
  [EVENT_KINDS.QUEUED]: 'Emails queued',
  [EVENT_KINDS.LAUNCHED]: 'Campaign launched',
  [EVENT_KINDS.LEARNING]: 'Learning recorded',
  [EVENT_KINDS.EXECUTION_COMMITTED]: 'Stage committed',
});

function createEvent(input = {}) {
  const at = nowIso(input.at || input.now);
  return {
    id: asText(input.id) || newId('evt'),
    missionId: asText(input.missionId),
    kind: asText(input.kind) || EVENT_KINDS.CONTRIBUTION,
    at,
    specialist: asText(input.specialist) || null,
    label: asText(input.label) || DEFAULT_LABELS[input.kind] || 'Event',
    payload: input.payload && typeof input.payload === 'object' ? clone(input.payload) : {},
  };
}

function formatTimeline(events = []) {
  return [...events]
    .sort((a, b) => String(a.at).localeCompare(String(b.at)))
    .map((event) => ({
      ...clone(event),
      clock: formatClock(event.at),
      line: `${formatClock(event.at)}   ${event.label}`,
    }));
}

module.exports = {
  DEFAULT_LABELS,
  createEvent,
  formatTimeline,
};
