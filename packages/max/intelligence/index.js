'use strict';

const {
  NAV_TYPES,
  TRAIL_KINDS,
  buildNavRef,
  parseRecommendationId,
  buildRecommendationId,
  pushTrail,
  popTrailTo,
  focusFromTrail,
} = require('./IntelligenceTypes');
const {
  RelatedIntelligenceBuilder,
} = require('./RelatedIntelligence');
const {
  RecommendationDetailComposer,
  createRecommendationDetailComposer,
} = require('./RecommendationDetailComposer');
const {
  CompanyIntelligenceComposer,
  createCompanyIntelligenceComposer,
} = require('./CompanyIntelligenceComposer');

/**
 * Facade holding both intelligence composers + related builder.
 */
class IntelligenceComposer {
  /**
   * @param {object} deps
   * @param {import('../../knowledge/services/KnowledgeService').KnowledgeService} deps.knowledge
   * @param {import('../memory/MemoryEngine').MemoryEngine} deps.memory
   * @param {import('../policy/engine/PolicyEngine').PolicyEngine} deps.policy
   */
  constructor(deps) {
    if (!deps || !deps.knowledge || !deps.memory || !deps.policy) {
      throw new Error(
        'IntelligenceComposer requires knowledge, memory, and policy'
      );
    }
    this._related = new RelatedIntelligenceBuilder({
      knowledge: deps.knowledge,
      memory: deps.memory,
    });
    this._recommendation = new RecommendationDetailComposer({
      knowledge: deps.knowledge,
      memory: deps.memory,
      policy: deps.policy,
      related: this._related,
    });
    this._company = new CompanyIntelligenceComposer({
      knowledge: deps.knowledge,
      memory: deps.memory,
      policy: deps.policy,
      related: this._related,
    });
  }

  /** @returns {RelatedIntelligenceBuilder} */
  get related() {
    return this._related;
  }

  composeRecommendation(input) {
    return this._recommendation.compose(input);
  }

  composeCompany(input) {
    return this._company.compose(input);
  }
}

function createIntelligenceComposer(deps) {
  return new IntelligenceComposer(deps);
}

module.exports = {
  NAV_TYPES,
  TRAIL_KINDS,
  buildNavRef,
  parseRecommendationId,
  buildRecommendationId,
  pushTrail,
  popTrailTo,
  focusFromTrail,
  RelatedIntelligenceBuilder,
  RecommendationDetailComposer,
  createRecommendationDetailComposer,
  CompanyIntelligenceComposer,
  createCompanyIntelligenceComposer,
  IntelligenceComposer,
  createIntelligenceComposer,
};
