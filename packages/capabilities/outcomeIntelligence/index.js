'use strict';

/**
 * Outcome Intelligence Capability (SPEC-036 / ADR-023).
 */

const types = require('./types');
const validate = require('./validate');
const learn = require('./learn');
const recommend = require('./recommend');
const rankingFeedback = require('./rankingFeedback');
const personalization = require('./personalization');
const analytics = require('./analytics');
const assemble = require('./assemble');
const actions = require('./actions');
const {
  InMemoryOutcomeIntelligenceStore,
  createInMemoryOutcomeIntelligenceStore,
} = require('./OutcomeIntelligenceStore');
const {
  createOutcomeIntelligenceCapability,
} = require('./OutcomeIntelligence');

module.exports = {
  ...types,
  ...validate,
  ...learn,
  ...recommend,
  ...rankingFeedback,
  ...personalization,
  ...analytics,
  ...assemble,
  ...actions,
  InMemoryOutcomeIntelligenceStore,
  createInMemoryOutcomeIntelligenceStore,
  createOutcomeIntelligenceCapability,
};
