'use strict';

/**
 * @pulseforge/laboratory — Evidence Laboratory (SPEC-019)
 *
 * Ask questions of the Evidence Platform without changing production state.
 * Not paper trading. Experiments are isolated.
 *
 * Guiding principle: The Laboratory asks questions. The Evidence Platform answers.
 *
 * Declarative queries: `await lab.query(\`FIND Claims WHERE subject = "BTC"\`)` (SPEC-020).
 */

const { EvidenceLab, createEvidenceLab } = require('./EvidenceLab');
const { Experiment, createExperiment, versionKey } = require('./Experiment');
const {
  ScenarioRunner,
  createScenarioRunner,
} = require('./ScenarioRunner');
const {
  EvidenceQuery,
  createEvidenceQuery,
  extractClaimIds,
  flattenClaims,
} = require('./EvidenceQuery');
const {
  ComparisonWorkspace,
  createComparisonWorkspace,
  summarizeResult,
  buildSideBySide,
} = require('./ComparisonWorkspace');
const {
  labResolveBundle,
  ontologyVersionLabel,
  packVersionLabel,
} = require('./resolveBundle');

const LAB_RULES = Object.freeze({
  ASK_ONLY: 'laboratory_asks_questions',
  PLATFORM_ANSWERS: 'evidence_platform_provides_answers',
  ISOLATED_EXPERIMENTS: 'experiments_are_isolated',
  NO_PRODUCTION_MUTATION: 'nothing_produced_here_affects_production',
  NOT_PAPER_TRADING: 'laboratory_is_not_paper_trading',
});

module.exports = {
  EvidenceLab,
  createEvidenceLab,
  Experiment,
  createExperiment,
  versionKey,
  ScenarioRunner,
  createScenarioRunner,
  EvidenceQuery,
  createEvidenceQuery,
  extractClaimIds,
  flattenClaims,
  ComparisonWorkspace,
  createComparisonWorkspace,
  summarizeResult,
  buildSideBySide,
  labResolveBundle,
  ontologyVersionLabel,
  packVersionLabel,
  LAB_RULES,
};
