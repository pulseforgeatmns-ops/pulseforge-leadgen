'use strict';

/**
 * Business Intelligence Engine (SPEC-053 / ADR-037).
 */

const types = require('./types');
const reason = require('./reason');
const gates = require('./gates');
const {
  createBusinessIntelligenceCapability,
  resolveProspects,
} = require('./BusinessIntelligenceEngine');

module.exports = {
  ...types,
  ...reason,
  ...gates,
  createBusinessIntelligenceCapability,
  resolveProspects,
};
