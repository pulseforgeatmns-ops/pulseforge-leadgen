'use strict';

const { BriefingBuilder } = require('./builders/BriefingBuilder');
const { DigestBuilder } = require('./digest/DigestBuilder');
const { Prioritizer } = require('./priorities/Prioritizer');

/**
 * Briefing Engine — orchestrates section builders into one Briefing.
 * Thin façade over BriefingBuilder (single entry point remains brief()).
 */
class BriefingEngine {
  /**
   * @param {object} deps
   * @param {import('../../knowledge/services/KnowledgeService').KnowledgeService} deps.knowledge
   * @param {import('../memory/MemoryEngine').MemoryEngine} deps.memory
   * @param {BriefingBuilder} [deps.builder]
   */
  constructor(deps) {
    if (!deps || !deps.knowledge || !deps.memory) {
      throw new Error('BriefingEngine requires knowledge and memory');
    }
    this._builder =
      deps.builder ||
      new BriefingBuilder({
        knowledge: deps.knowledge,
        memory: deps.memory,
      });
    this._knowledge = deps.knowledge;
    this._memory = deps.memory;
  }

  /** @returns {BriefingBuilder} */
  get builder() {
    return this._builder;
  }

  /**
   * @param {Parameters<BriefingBuilder['build']>[0]} input
   */
  async brief(input) {
    return this._builder.build(input);
  }
}

/**
 * @param {object} options
 * @param {import('../../knowledge/services/KnowledgeService').KnowledgeService} options.knowledge
 * @param {import('../memory/MemoryEngine').MemoryEngine} options.memory
 */
function createBriefingEngine(options) {
  return new BriefingEngine(options);
}

module.exports = {
  BriefingEngine,
  createBriefingEngine,
  BriefingBuilder,
  DigestBuilder,
  Prioritizer,
};
