'use strict';

/**
 * Business Signals Capability (SPEC-031 / ADR-018).
 */

const types = require('./types');
const categories = require('./categories');
const confidence = require('./confidence');
const lifecycle = require('./lifecycle');
const decay = require('./decay');
const collect = require('./collect');
const verify = require('./verify');
const knowledgeHandoff = require('./knowledgeHandoff');
const messaging = require('./messaging');
const {
  buildBusinessSignalsForProspect,
  buildBusinessSignalsStage,
  resolveActiveSignals,
} = require('./BusinessSignals');

module.exports = {
  ...types,
  ...categories,
  ...confidence,
  ...lifecycle,
  ...decay,
  ...collect,
  ...verify,
  ...knowledgeHandoff,
  ...messaging,
  buildBusinessSignalsForProspect,
  buildBusinessSignalsStage,
  resolveActiveSignals,
};
