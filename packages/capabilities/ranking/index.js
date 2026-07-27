'use strict';

/**
 * Opportunity Ranking (SPEC-026).
 */

const types = require('./types');
const factors = require('./factors');
const brief = require('./brief');
const {
  createOpportunityRankingCapability,
  resolveProfile,
  resolveProspects,
  resolveHistoricalOutcomes,
} = require('./OpportunityRanking');

module.exports = {
  ...types,
  ...factors,
  ...brief,
  createOpportunityRankingCapability,
  resolveProfile,
  resolveProspects,
  resolveHistoricalOutcomes,
};
