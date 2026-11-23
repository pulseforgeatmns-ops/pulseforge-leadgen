'use strict';

const { TENANT_ID } = require('./leviScoutProductionContract');
const { tenantCriteria } = require('../packages/max/workspace/ProspectInvestigationPriority');

const SLOT_IDS = Object.freeze([
  'financingAmountRange',
  'eligiblePropertyTypes',
  'geographicCoverage',
  'borrowerEligibility',
  'financingProductsAndExclusions',
]);

const QUESTIONS = Object.freeze({
  financingAmountRange:
    'What single approved financing amount range should Scout use for qualification, written as “Financing range: $X–$Y”?',
  eligiblePropertyTypes:
    'Which property and project types are eligible (rehab, bridge, new construction, multifamily, and others), and which are excluded?',
  geographicCoverage:
    'Which geographies are in coverage, and are New York City and Chicago permit markets in scope?',
  borrowerEligibility:
    'Which borrowers are eligible (entity-owned operators, sponsors, owner-occupants, first-time borrowers), and which are excluded?',
  financingProductsAndExclusions:
    'Which financing products are offered, and which request types must Scout exclude?',
});

const PROPERTY_RE = /\b(property types?|project types?|rehab|renovation|multifamily|construction|dwelling|commercial real estate|residential|flip|new construction|mixed[- ]use)\b/i;
const GEO_RE = /\b(geograph|market|cit(?:y|ies)|county|state|nationwide|new york|chicago|borough|msa|service area)\b/i;
const BORROWER_RE = /\b(borrower|sponsor|operator|investor|owner-occupant|guarantor|entity|llc|eligibility|eligible)\b/i;
const PRODUCT_RE = /\b(bridge|rehab|loan|lend|financ|equity[- ]gap|product|exclude|disqualif|not eligible)\b/i;

function knownFacts(summary, key) {
  return (Array.isArray(summary && summary.businessFacts && summary.businessFacts[key])
    ? summary.businessFacts[key]
    : []).filter(fact => fact
      && fact.epistemic_state === 'KNOWN'
      && fact.id
      && fact.provenance
      && typeof fact.value === 'string'
      && fact.value.trim());
}

function factsMatching(facts, pattern) {
  return facts.filter(fact => pattern.test(fact.value)).map(fact => ({
    id: fact.id,
    provenance: fact.provenance,
    slot: fact.slot || null,
    value: fact.value.trim(),
  }));
}

function contractText(contract, key) {
  const value = contract && contract[key];
  return typeof value === 'string' && value.trim() ? value.trim() : '';
}

function slot(id, evidence, { trusted }) {
  const present = trusted && evidence.length > 0;
  return Object.freeze({
    id,
    present,
    trusted,
    evidence: Object.freeze(present ? evidence : []),
    question: present ? null : QUESTIONS[id],
  });
}

function inspectApprovedBlueprint(understanding, tenantId = TENANT_ID) {
  const bundleIds = { tenantId: String(tenantId), clientId: Number(tenantId) };
  const criteria = tenantCriteria(understanding, bundleIds);
  const trusted = Boolean(criteria && criteria.ref);
  const summary = understanding && understanding._bundle && understanding._bundle.summary;
  const contract = understanding && understanding._bundle && understanding._bundle.contract;
  const services = knownFacts(summary, 'services');
  const ideal = knownFacts(summary, 'ideal_customers');
  const disqualified = knownFacts(summary, 'disqualified_customers');
  const geography = knownFacts(summary, 'geography');
  const markets = knownFacts(summary, 'target_markets');
  const growth = knownFacts(summary, 'growth_focus');

  const rangeEvidence = criteria && criteria.range
    ? [{
        id: 'financing_range',
        provenance: 'approved_blueprint',
        value: `Financing range: $${criteria.range.min}–$${criteria.range.max}`,
      }]
    : [];

  const propertyEvidence = [
    ...factsMatching(ideal, PROPERTY_RE),
    ...factsMatching(services, PROPERTY_RE),
    ...factsMatching(growth, PROPERTY_RE),
  ];
  const geoEvidence = [
    ...factsMatching(geography, GEO_RE),
    ...factsMatching(markets, GEO_RE),
    ...factsMatching(growth, GEO_RE),
  ];
  if (!geoEvidence.length && trusted && GEO_RE.test(contractText(contract, 'geography'))) {
    geoEvidence.push({
      id: 'contract.geography',
      provenance: 'approved_blueprint_contract',
      value: contractText(contract, 'geography'),
    });
  }
  const borrowerEvidence = factsMatching(ideal, BORROWER_RE);
  const productEvidence = [
    ...factsMatching(services, PRODUCT_RE),
    ...factsMatching(disqualified, PRODUCT_RE),
  ];

  const slots = Object.freeze({
    financingAmountRange: slot('financingAmountRange', rangeEvidence, { trusted }),
    eligiblePropertyTypes: slot('eligiblePropertyTypes', propertyEvidence, { trusted }),
    geographicCoverage: slot('geographicCoverage', geoEvidence, { trusted }),
    borrowerEligibility: slot('borrowerEligibility', borrowerEvidence, { trusted }),
    financingProductsAndExclusions: slot('financingProductsAndExclusions', productEvidence, { trusted }),
  });
  const missing = SLOT_IDS.filter(id => !slots[id].present).map(id => Object.freeze({
    id,
    question: QUESTIONS[id],
  }));

  return Object.freeze({
    tenantId: String(tenantId),
    trusted,
    criteriaRange: criteria && criteria.range ? Object.freeze({ ...criteria.range }) : null,
    criteriaLimitation: criteria && criteria.limitation ? criteria.limitation : null,
    knowledgeState: (understanding && understanding._bundle && understanding._bundle.knowledgeState) || 'unavailable',
    blueprintSource: (understanding && understanding._bundle && understanding._bundle.blueprintSource) || 'none',
    invented: false,
    authorizedChanges: false,
    slots,
    missing,
    questionsForLevi: missing.map(item => item.question),
  });
}

async function loadTenantUnderstanding({ db, tenantId = TENANT_ID, deps = {} } = {}) {
  if (!db || typeof db.query !== 'function') {
    return {
      _bundle: {
        tenantId: String(tenantId),
        summary: null,
        blueprintSource: 'none',
        knowledgeState: 'unavailable',
        contract: null,
      },
      loadError: 'blueprint_db_unavailable',
    };
  }
  const ctx = deps.clientIntelligence || require('../packages/max/workspace/ClientIntelligenceContext');
  const { buildBusinessUnderstandingContract } = deps.businessUnderstanding
    || require('../packages/max/workspace/BusinessUnderstandingRetrieval');
  const cie = deps.cie || require('../packages/max/clientIntelligence');
  const cieService = {
    getApprovedClientBlueprint: (id, opts = {}) => cie.getApprovedClientBlueprint(id, { ...opts, pool: db }),
  };
  try {
    const loaded = await ctx.loadApprovedClientIntelligence({
      tenantId: String(tenantId),
      clientId: Number(tenantId),
      cieService,
      cieOpts: { pool: db },
      propagateLoadErrors: true,
    });
    const summary = loaded.summary || null;
    return {
      _bundle: {
        tenantId: String(tenantId),
        summary,
        blueprintSource: summary ? 'blueprint' : 'none',
        knowledgeState: summary && summary.approved ? 'available' : 'never_learned',
        contract: buildBusinessUnderstandingContract(summary, null, []),
      },
    };
  } catch (error) {
    const notFound = error && (error.code === 'not_found' || /No approved blueprint/i.test(String(error.message || '')));
    return {
      _bundle: {
        tenantId: String(tenantId),
        summary: null,
        blueprintSource: 'none',
        knowledgeState: notFound ? 'never_learned' : 'retrieval_failure',
        contract: null,
      },
      loadError: notFound ? 'no_approved_blueprint' : (error && (error.code || error.name)) || 'blueprint_load_failed',
    };
  }
}

module.exports = {
  SLOT_IDS,
  QUESTIONS,
  inspectApprovedBlueprint,
  loadTenantUnderstanding,
};
