'use strict';

/**
 * Sales Intelligence Engine (SPEC-048 / ADR-032).
 */

const types = require('./types');
const derive = require('./derive');
const gates = require('./gates');
const humanTest = require('./humanTest');
const approvalRate = require('./approvalRate');
const {
  createSalesIntelligenceCapability,
  resolveProspects,
} = require('./SalesIntelligenceEngine');

module.exports = {
  ...types,
  ...derive,
  ...gates,
  ...humanTest,
  ...approvalRate,
  createSalesIntelligenceCapability,
  resolveProspects,
};
