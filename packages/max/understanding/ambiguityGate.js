'use strict';

const { AMBIGUITY_KIND } = require('./types');

const MATERIAL_KINDS = new Set([
  AMBIGUITY_KIND.PRONOUN,
  AMBIGUITY_KIND.ACCOUNT,
  AMBIGUITY_KIND.CONTACT,
]);

function collectAmbiguities(situationModel) {
  const all = [...(situationModel.ambiguities || [])];
  for (const thread of situationModel.threads || []) {
    for (const a of thread.ambiguities || []) all.push(a);
  }
  return all;
}

function validateSituationModel(situationModel) {
  const ambiguities = collectAmbiguities(situationModel);
  const material = ambiguities.filter(a => MATERIAL_KINDS.has(a.kind));
  const clarifications = material
    .map(a => a.clarification)
    .filter(Boolean);

  const blockCommit = material.length > 0;
  const blockDecisionExecution = material.some(a =>
    a.kind === AMBIGUITY_KIND.PRONOUN || a.kind === AMBIGUITY_KIND.ACCOUNT
  );

  return {
    validated: !blockCommit,
    blockCommit,
    blockDecisionExecution,
    ambiguities,
    materialAmbiguities: material,
    clarifications,
    narrowestClarification: clarifications[0] || null,
  };
}

module.exports = {
  validateSituationModel,
  collectAmbiguities,
};
