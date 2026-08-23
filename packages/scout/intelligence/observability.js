'use strict';

/**
 * SPEC-141 — Scout Intelligence Pipeline observability.
 */

const { SCOUT_INTELLIGENCE_EVENTS } = require('./types');

/** @type {object[]} */
const stageLog = [];

function emitIntelligenceStarted(meta = {}) {
  const entry = {
    event: SCOUT_INTELLIGENCE_EVENTS.STARTED,
    at: new Date().toISOString(),
    ...meta,
  };
  stageLog.push(entry);
  return entry;
}

function emitIntelligenceStage(stage, meta = {}) {
  const entry = {
    event: SCOUT_INTELLIGENCE_EVENTS.STAGE,
    stage,
    at: new Date().toISOString(),
    ...meta,
  };
  stageLog.push(entry);
  return entry;
}

function emitIntelligenceCompleted(meta = {}) {
  const entry = {
    event: SCOUT_INTELLIGENCE_EVENTS.COMPLETED,
    at: new Date().toISOString(),
    ...meta,
  };
  stageLog.push(entry);
  return entry;
}

function listIntelligenceLog() {
  return stageLog.slice();
}

function clearIntelligenceLog() {
  stageLog.length = 0;
}

module.exports = {
  emitIntelligenceStarted,
  emitIntelligenceStage,
  emitIntelligenceCompleted,
  listIntelligenceLog,
  clearIntelligenceLog,
};
