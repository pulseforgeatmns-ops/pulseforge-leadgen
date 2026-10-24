'use strict';

/**
 * SPEC-117 — learning store helpers. Evidence never auto-applies.
 */

const { clone } = require('./types');
const { learningRecords } = require('./Outcomes');

function routeOutcome(store, outcome) {
  if (!outcome) return [];
  store.addOutcome(outcome);
  const rows = learningRecords(outcome);
  for (const row of rows) store.addLearning(row);
  return rows;
}

function learningForSink(store, tenantId, sink) {
  return store.listLearning(tenantId)
    .filter((row) => !sink || row.sink === sink)
    .map(clone);
}

module.exports = {
  routeOutcome,
  learningForSink,
};
