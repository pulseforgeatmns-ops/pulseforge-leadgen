'use strict';

/**
 * Client Playbook types (SPEC-028 / ADR-015).
 * Strategy asset: how a specific business wins customers.
 */

const BRAND_VOICES = Object.freeze([
  'professional',
  'friendly',
  'relationship_first',
  'technical',
  'premium',
  'direct',
]);

const PLAYBOOK_STATUS = Object.freeze({
  DRAFT: 'draft',
  ACTIVE: 'active',
  PENDING_REVIEW: 'pending_review',
  SUPERSEDED: 'superseded',
});

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildIdealCustomer(partial = {}) {
  return {
    primaryMarkets: asStringList(partial.primaryMarkets),
    secondaryMarkets: asStringList(partial.secondaryMarkets),
    geographicCoverage: partial.geographicCoverage != null
      ? String(partial.geographicCoverage)
      : '',
    minimumCompanySize:
      partial.minimumCompanySize != null
        ? String(partial.minimumCompanySize)
        : null,
    industriesToAvoid: asStringList(partial.industriesToAvoid),
    buyingTriggers: asStringList(partial.buyingTriggers),
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildOutreachStep(partial = {}) {
  return {
    day: Number.isFinite(Number(partial.day)) ? Number(partial.day) : 0,
    channel: String(partial.channel || ''),
    action: String(partial.action || partial.channel || ''),
    notes: partial.notes != null ? String(partial.notes) : '',
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildPlaybookConstraint(partial = {}) {
  return {
    type: String(partial.type || 'rule'),
    rule: String(partial.rule || ''),
    detail: partial.detail != null ? String(partial.detail) : '',
  };
}

/**
 * @param {object} [partial]
 * @returns {object}
 */
function buildClientPlaybook(partial = {}) {
  const voice = normalizeBrandVoice(partial.brandVoice);
  const ideal =
    partial.idealCustomer && typeof partial.idealCustomer === 'object'
      ? buildIdealCustomer(partial.idealCustomer)
      : buildIdealCustomer({
          primaryMarkets: partial.targetMarkets,
          geographicCoverage: partial.geographicCoverage,
        });

  const targetMarkets = asStringList(
    partial.targetMarkets && partial.targetMarkets.length
      ? partial.targetMarkets
      : [...ideal.primaryMarkets, ...ideal.secondaryMarkets]
  );

  return {
    id: String(partial.id || ''),
    clientId:
      partial.clientId != null ? Number(partial.clientId) || partial.clientId : null,
    name: String(partial.name || ''),
    version: partial.version != null ? String(partial.version) : '1.0',
    status: partial.status || PLAYBOOK_STATUS.ACTIVE,
    targetMarkets,
    valuePropositions: asStringList(partial.valuePropositions),
    idealCustomer: ideal,
    brandVoice: voice,
    preferredChannels: asStringList(partial.preferredChannels),
    outreachSequence: Array.isArray(partial.outreachSequence)
      ? partial.outreachSequence.map(buildOutreachStep)
      : [],
    offers: asStringList(partial.offers),
    constraints: Array.isArray(partial.constraints)
      ? partial.constraints.map(buildPlaybookConstraint)
      : [],
    successMetrics: asStringList(partial.successMetrics),
    notes: partial.notes != null ? String(partial.notes) : '',
    parentId: partial.parentId || null,
    createdAt: partial.createdAt || new Date().toISOString(),
    updatedAt: partial.updatedAt || new Date().toISOString(),
  };
}

function normalizeBrandVoice(voice) {
  const raw = String(voice || 'professional')
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, '_');
  if (BRAND_VOICES.includes(raw)) return raw;
  const aliases = {
    relationship: 'relationship_first',
    'relationship-first': 'relationship_first',
  };
  return aliases[raw] || 'professional';
}

function asStringList(value) {
  if (value == null) return [];
  if (Array.isArray(value)) return value.map(String).filter(Boolean);
  if (typeof value === 'string' && value.trim()) return [value.trim()];
  return [];
}

/**
 * Human label for brand voice.
 * @param {string} voice
 */
function brandVoiceLabel(voice) {
  const v = normalizeBrandVoice(voice);
  return {
    professional: 'Professional',
    friendly: 'Friendly',
    relationship_first: 'Relationship-first',
    technical: 'Technical',
    premium: 'Premium',
    direct: 'Direct',
  }[v] || 'Professional';
}

module.exports = {
  BRAND_VOICES,
  PLAYBOOK_STATUS,
  buildIdealCustomer,
  buildOutreachStep,
  buildPlaybookConstraint,
  buildClientPlaybook,
  normalizeBrandVoice,
  brandVoiceLabel,
  asStringList,
};
