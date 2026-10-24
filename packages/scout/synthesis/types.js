'use strict';

/**
 * SPEC-160 — Evidence Synthesis types.
 * ADR-080 — Understanding emerges from evidence; confidence attaches to understanding.
 *
 * Evidence is atomic. Intelligence is synthesized understanding.
 */

const { evidenceWeight } = require('../credibility/EvidenceWeights');

function nowIso() {
  return new Date().toISOString();
}

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

const UNDERSTANDING_KINDS = Object.freeze({
  BUSINESS_MODEL: 'business_model',
  BUYING_SIGNAL: 'buying_signal',
  GROWTH: 'growth',
  DECISION_MAKER: 'decision_maker',
  SERVICE_NEED: 'service_need',
  ENTITY_IDENTITY: 'entity_identity',
});

const SYNTHESIS_REVISION_KINDS = Object.freeze({
  CREATED: 'understanding_created',
  REVISED: 'understanding_revised',
  CONTRADICTION: 'contradiction_recorded',
  ENTITY_MERGED: 'entity_merged',
});

/**
 * Atomic evidence observation.
 * @param {object} partial
 * @returns {object}
 */
function buildEvidence(partial = {}) {
  const source = asText(partial.source) || 'unknown';
  return {
    id: partial.id || `ev-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    source,
    observation: asText(partial.observation || partial.label || partial.text),
    timestamp: partial.timestamp || partial.observedAt || nowIso(),
    confidence: partial.confidence != null ? Number(partial.confidence) : evidenceWeight(source),
    provenance: partial.provenance || {
      entityId: partial.entityId || null,
      entityName: partial.entityName || null,
      provider: source,
      raw: partial.raw || null,
    },
  };
}

/**
 * Synthesized business understanding for an entity.
 * @param {object} partial
 * @returns {object}
 */
function buildUnderstanding(partial = {}) {
  const supporting = (partial.supportingEvidence || []).map((e) =>
    e.id && e.observation ? e : buildEvidence(e)
  );
  const contradictory = (partial.contradictoryEvidence || []).map((e) =>
    e.id && e.observation ? e : buildEvidence(e)
  );

  return {
    id: partial.id || `und-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    entity: asText(partial.entity),
    entityId: partial.entityId || null,
    aliases: Array.isArray(partial.aliases) ? partial.aliases.slice() : [],
    kind: partial.kind || UNDERSTANDING_KINDS.BUSINESS_MODEL,
    assertions: Array.isArray(partial.assertions)
      ? partial.assertions.map(asText).filter(Boolean)
      : partial.assertion
        ? [asText(partial.assertion)]
        : [],
    supportingEvidence: supporting,
    contradictoryEvidence: contradictory,
    confidence:
      partial.confidence != null
        ? Number(partial.confidence)
        : computeUnderstandingConfidence(supporting, contradictory),
    reasoning: asText(partial.reasoning),
    revisionHistory: Array.isArray(partial.revisionHistory) ? partial.revisionHistory.slice() : [],
    synthesizedAt: partial.synthesizedAt || nowIso(),
    updatedAt: partial.updatedAt || nowIso(),
  };
}

/**
 * Confidence belongs to synthesized understanding, not individual facts.
 * @param {object[]} supportingEvidence
 * @param {object[]} contradictoryEvidence
 * @returns {number}
 */
function computeUnderstandingConfidence(supportingEvidence = [], contradictoryEvidence = []) {
  if (supportingEvidence.length === 0) return 0;

  const weights = supportingEvidence.map((e) => e.confidence ?? evidenceWeight(e.source));
  const sourceCount = new Set(supportingEvidence.map((e) => e.source)).size;
  const avgWeight = weights.reduce((sum, w) => sum + w, 0) / weights.length;

  // Confidence attaches to synthesized understanding — weak with single source, grows with diversity
  const sourceBonus = sourceCount <= 1 ? 0 : Math.min(0.4, (sourceCount - 1) * 0.15);
  let confidence = 0.12 + avgWeight * 0.36 + sourceBonus;

  for (const _ of contradictoryEvidence) {
    confidence -= 0.15;
  }

  return Number(Math.max(0.05, Math.min(0.98, confidence)).toFixed(2));
}

module.exports = {
  UNDERSTANDING_KINDS,
  SYNTHESIS_REVISION_KINDS,
  buildEvidence,
  buildUnderstanding,
  computeUnderstandingConfidence,
  nowIso,
  asText,
};
