'use strict';

/**
 * Operator-selectable pricing packages (SPEC-027B §10).
 */

const PRICING_PACKAGES = Object.freeze({
  setup_monthly: Object.freeze({
    id: 'setup_monthly',
    label: 'Setup + Monthly',
    setupFee: 'Operator-set setup fee',
    monthly: 'Operator-set monthly retainer',
    description:
      'One-time setup to stand up discovery profile, campaign foundation, and operator workflow, then a monthly retainer for ongoing acquisition work.',
    included: [
      'Discovery Profile configuration',
      'Prospect research and verification',
      'Personalized outreach and follow-up',
      'Pipeline visibility for your team',
      'Monthly campaign refinement from evidence',
    ],
    paymentSchedule: [
      'Setup fee due at kickoff',
      'Monthly retainer billed at the start of each month',
    ],
    optionalTerms: ['90-day initial commitment recommended', 'Either party may exit with 30 days written notice after the initial term'],
  }),
  pilot: Object.freeze({
    id: 'pilot',
    label: 'Pilot',
    setupFee: 'Included in pilot fee',
    monthly: 'Fixed pilot fee for the agreed window',
    description:
      'Time-boxed pilot to prove commercial acquisition in a defined geography and ICP before a longer engagement.',
    included: [
      'Scoped Discovery Profile for the pilot market',
      'First-wave prospect research and outreach',
      'Operator review of replies and walkthrough handoffs',
      'Pilot retrospective with evidence and recommended scale plan',
    ],
    paymentSchedule: [
      'Pilot fee due at kickoff (or 50% kickoff / 50% midpoint — operator choice)',
    ],
    optionalTerms: ['Pilot scope and market locked at kickoff', 'Conversion to monthly package priced at pilot close'],
  }),
  founding_partner: Object.freeze({
    id: 'founding_partner',
    label: 'Founding Partner',
    setupFee: 'Reduced or waived setup (operator-set)',
    monthly: 'Founding Partner monthly rate',
    description:
      'Early-partner economics for operators building a category beachhead with Pulseforge. Preferential pricing in exchange for tight feedback and case-study rights (optional).',
    included: [
      'Full commercial acquisition system',
      'Priority iteration on Discovery Profile and messaging',
      'Shared learning from market evidence',
      'Optional case-study collaboration',
    ],
    paymentSchedule: [
      'Founding terms confirmed in writing at kickoff',
      'Monthly retainer on the agreed Founding Partner rate',
    ],
    optionalTerms: [
      'Founding Partner seats are limited',
      'Rate locks for the agreed founding window',
    ],
  }),
  enterprise: Object.freeze({
    id: 'enterprise',
    label: 'Enterprise',
    setupFee: 'Custom enterprise setup',
    monthly: 'Custom enterprise retainer',
    description:
      'Multi-market or multi-brand commercial acquisition with dedicated operating cadence and reporting.',
    included: [
      'Multiple Discovery Profiles / markets',
      'Expanded research and outreach capacity',
      'Custom reporting and stakeholder cadence',
      'Priority support for walkthrough and close coordination',
    ],
    paymentSchedule: ['Custom schedule — operator-defined'],
    optionalTerms: ['MSA / SOW required', 'Service levels defined per engagement'],
  }),
  custom: Object.freeze({
    id: 'custom',
    label: 'Custom',
    setupFee: 'Custom',
    monthly: 'Custom',
    description:
      'Bespoke commercial structure. Operator fills amounts and terms before send.',
    included: ['Scope defined with the operator before delivery'],
    paymentSchedule: ['Defined by operator'],
    optionalTerms: [],
  }),
});

/**
 * @param {string} [packageId]
 * @param {object} [overrides] operator edits (amounts, terms)
 * @returns {object}
 */
function resolvePricingPackage(packageId, overrides = {}) {
  const id = String(packageId || 'setup_monthly');
  const base = PRICING_PACKAGES[id] || PRICING_PACKAGES.setup_monthly;
  return {
    ...base,
    setupFee: overrides.setupFee != null ? String(overrides.setupFee) : base.setupFee,
    monthly: overrides.monthly != null ? String(overrides.monthly) : base.monthly,
    description:
      overrides.description != null ? String(overrides.description) : base.description,
    included: Array.isArray(overrides.included)
      ? overrides.included.map(String)
      : [...base.included],
    paymentSchedule: Array.isArray(overrides.paymentSchedule)
      ? overrides.paymentSchedule.map(String)
      : [...base.paymentSchedule],
    optionalTerms: Array.isArray(overrides.optionalTerms)
      ? overrides.optionalTerms.map(String)
      : [...base.optionalTerms],
    operatorNotes:
      overrides.operatorNotes != null ? String(overrides.operatorNotes) : null,
  };
}

/**
 * @returns {object[]}
 */
function listPricingPackages() {
  return Object.values(PRICING_PACKAGES).map((p) => ({
    id: p.id,
    label: p.label,
    description: p.description,
  }));
}

module.exports = {
  PRICING_PACKAGES,
  resolvePricingPackage,
  listPricingPackages,
};
