'use strict';

/**
 * MAX-UNDERSTANDING-001 — canonical situation model types.
 * Aligns epistemic categories with ClaimGrounding / decisionExecution conventions.
 */

const EPISTEMIC_CATEGORY = Object.freeze({
  CONFIRMED: 'confirmed',
  REPORTED: 'reported',
  INFERRED: 'inferred',
  UNCERTAIN: 'uncertain',
});

const ENTITY_KIND = Object.freeze({
  ACCOUNT: 'account',
  CONTACT: 'contact',
  AO: 'ao',
  UNKNOWN: 'unknown',
});

const CONTACT_ROLE = Object.freeze({
  DECISION_MAKER: 'decision_maker',
  SUSPECTED_DECISION_MAKER: 'suspected_decision_maker',
  INFLUENCER: 'influencer',
  REFERRAL: 'referral',
  GATEKEEPER: 'gatekeeper',
  UNKNOWN: 'unknown',
});

const AMBIGUITY_KIND = Object.freeze({
  PRONOUN: 'pronoun',
  ACCOUNT: 'account',
  CONTACT: 'contact',
  TEMPORAL: 'temporal',
  CORRECTION_TARGET: 'correction_target',
});

function newSituationId(prefix = 'sit') {
  const crypto = require('crypto');
  return `${prefix}_${crypto.randomUUID()}`;
}

function emptyConfidence() {
  return {
    overall: 0,
    entityResolution: 0,
    semanticInterpretation: 0,
  };
}

function createSituationModel(base = {}) {
  const now = base.interpretedAt || new Date().toISOString();
  return {
    inputId: base.inputId || newSituationId('in'),
    conversationId: base.conversationId || null,
    actor: base.actor || {},
    occurredAt: base.occurredAt || null,
    interpretedAt: now,
    rawText: base.rawText || null,
    entities: base.entities || [],
    events: base.events || [],
    claims: base.claims || [],
    corrections: base.corrections || [],
    commitments: base.commitments || [],
    painPoints: base.painPoints || [],
    objections: base.objections || [],
    relationships: base.relationships || [],
    decisionMakerSignals: base.decisionMakerSignals || [],
    temporalReferences: base.temporalReferences || [],
    questions: base.questions || [],
    requestedActions: base.requestedActions || [],
    recommendedNextActions: base.recommendedNextActions || [],
    ambiguities: base.ambiguities || [],
    confidence: base.confidence || emptyConfidence(),
    evidence: base.evidence || [],
    threads: base.threads || [],
    commentary: base.commentary || [],
  };
}

module.exports = {
  EPISTEMIC_CATEGORY,
  ENTITY_KIND,
  CONTACT_ROLE,
  AMBIGUITY_KIND,
  newSituationId,
  emptyConfidence,
  createSituationModel,
};
