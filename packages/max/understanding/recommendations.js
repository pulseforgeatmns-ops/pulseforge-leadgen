'use strict';

const { CONTACT_ROLE, EPISTEMIC_CATEGORY } = require('./types');

function activeDecisionMakerTarget(situationModel) {
  const supersededContacts = new Set();
  for (const corr of situationModel?.corrections || []) {
    if (corr.kind === 'decision_maker_role' && corr.contactName) {
      supersededContacts.add(String(corr.contactName).toLowerCase());
    }
  }
  const signals = (situationModel?.threads || []).flatMap(t => t.decisionMakerSignals || []);
  for (const sig of signals) {
    if (sig.epistemic === EPISTEMIC_CATEGORY.UNCERTAIN || sig.epistemic === EPISTEMIC_CATEGORY.INFERRED) {
      continue;
    }
    if (supersededContacts.has(String(sig.contactName).toLowerCase())) continue;
    if (sig.role === CONTACT_ROLE.SUSPECTED_DECISION_MAKER || sig.role === CONTACT_ROLE.DECISION_MAKER) {
      return sig;
    }
  }
  for (const entity of (situationModel?.threads || []).flatMap(t => t.entities || [])) {
    if (entity.kind !== 'contact') continue;
    if (entity.superseded) continue;
    if (entity.decisionMaker && !supersededContacts.has(String(entity.name).toLowerCase())) {
      return { contactName: entity.name, role: entity.role, epistemic: entity.epistemic };
    }
  }
  return null;
}

function deriveRecommendedNextActions(situationModel) {
  if (!situationModel || situationModel.validation?.blockCommit) {
    return [];
  }
  const actions = [];
  const target = activeDecisionMakerTarget(situationModel);
  if (target?.contactName) {
    actions.push({
      id: `rec_follow_${target.contactName.toLowerCase()}`,
      kind: 'follow_up',
      targetContact: target.contactName,
      summary: `Follow up with ${target.contactName}.`,
      status: 'active',
      epistemic: target.epistemic || EPISTEMIC_CATEGORY.REPORTED,
    });
  }

  for (const corr of situationModel.corrections || []) {
    if (corr.kind === 'decision_maker_role' && corr.contactName) {
      for (const action of actions) {
        if (action.targetContact?.toLowerCase() === corr.contactName.toLowerCase()) {
          action.status = 'superseded';
          action.supersededBy = corr.newValue || 'role_correction';
        }
      }
    }
  }

  const reassigned = (situationModel.threads || []).flatMap(t => t.decisionMakerSignals || [])
    .find(s => s.epistemic !== EPISTEMIC_CATEGORY.UNCERTAIN
      && (s.role === CONTACT_ROLE.SUSPECTED_DECISION_MAKER || s.role === CONTACT_ROLE.DECISION_MAKER));
  if (reassigned && !actions.some(a => a.status === 'active' && a.targetContact === reassigned.contactName)) {
    actions.push({
      id: `rec_follow_${reassigned.contactName.toLowerCase()}`,
      kind: 'follow_up',
      targetContact: reassigned.contactName,
      summary: `Follow up with ${reassigned.contactName}.`,
      status: 'active',
      epistemic: reassigned.epistemic,
      replaces: actions.filter(a => a.status === 'superseded').map(a => a.id),
    });
  }

  return actions;
}

module.exports = {
  deriveRecommendedNextActions,
  activeDecisionMakerTarget,
};
