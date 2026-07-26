'use strict';

const {
  seedTenant,
  registerScoreWatches,
  TENANT,
  AS_OF,
} = require('../../briefing/tests/helpers');
const { createMaxReasoningRuntime } = require('../..');
const { createKnowledgeRuntime } = require('../../../knowledge');

module.exports = {
  seedTenant,
  registerScoreWatches,
  TENANT,
  AS_OF,
  createMaxReasoningRuntime,
  createKnowledgeRuntime,
};
