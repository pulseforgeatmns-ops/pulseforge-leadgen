'use strict';

/**
 * Dev/test fixtures for Client Intelligence Engine.
 * Sample sessions are marked isSample=true and never mixed into real client lineage.
 */

const ANCHOR_SAMPLE_CLIENT_ID = 10;
const ANCHOR_FIXTURE_KEY = 'anchor_cleaning';
const ANCHOR_BUSINESS_NAME = 'Anchor Cleaning';

const ANCHOR_NORMALIZED_FACTS = Object.freeze({
  business_name: ANCHOR_BUSINESS_NAME,
  business_description: 'commercial-focused cleaning company',
  growth_focus:
    'recurring commercial cleaning for customers who need weekly or multiple-times-per-week service',
  ideal_customers: [
    'property managers',
    'short-term rental companies',
    'facility managers',
    'professional offices',
    'rec centers',
    'high-traffic buildings',
  ],
  disqualified_customers: ['lowest-price buyers'],
  geography: [
    'Greater Manchester',
    'Bedford',
    'Londonderry',
    'Auburn',
    'Goffstown',
    'Hooksett',
  ],
  differentiation:
    'customers trust the work will be done right without needing to chase the team',
  brand_voice: 'calm, professional, reliable, and easy to work with',
  ninety_day_outcomes: 'commercial cleaning growth in Greater Manchester',
  success_metrics: [
    'a clearer path to commercial opportunities over the next 90 days',
  ],
});

const ANCHOR_BLUEPRINT_SECTIONS = Object.freeze({
  identity: {
    summary:
      'Anchor Cleaning is a commercial-focused cleaning company. This identity framing is how the operator describes the business today.',
    confidence: 0.9,
    evidenceIds: [],
    unknowns: [],
  },
  services: {
    summary:
      'Today the business delivers commercial cleaning for professional offices. The strongest growth focus is recurring commercial cleaning for customers who need weekly or multiple-times-per-week service.',
    confidence: 0.9,
    evidenceIds: [],
    unknowns: [],
  },
  idealCustomers: {
    summary:
      'Ideal customers are property managers, short-term rental companies, facility managers, professional offices, rec centers, and high-traffic buildings.',
    confidence: 0.88,
    evidenceIds: [],
    unknowns: [],
  },
  avoidCustomers: {
    summary: 'customers who only care about the lowest price',
    confidence: 0.8,
    evidenceIds: [],
    unknowns: [],
  },
  targetMarkets: {
    summary:
      'Priority markets center on Greater Manchester, including Bedford, Londonderry, Auburn, Goffstown, and Hooksett.',
    confidence: 0.9,
    evidenceIds: [],
    unknowns: [],
  },
  competitiveAdvantages: {
    summary:
      'Customers choose Anchor because they trust the work will be done right without needing to chase the team.',
    confidence: 0.85,
    evidenceIds: [],
    unknowns: [],
  },
  brandVoice: {
    summary: 'Calm, professional, reliable, and easy to work with.',
    confidence: 0.8,
    evidenceIds: [],
    unknowns: [],
  },
  campaignGoals: {
    summary:
      'Near-term growth goals focus on commercial cleaning growth in Greater Manchester.',
    confidence: 0.85,
    evidenceIds: [],
    unknowns: [],
  },
  successMetrics: {
    summary: 'A clearer path to commercial opportunities over the next 90 days.',
    confidence: 0.8,
    evidenceIds: [],
    unknowns: [],
  },
});

function fixturesAllowed(env = process.env) {
  if (env.ALLOW_CIE_FIXTURES === '1') return true;
  if (env.ALLOW_CIE_FIXTURES === '0') return false;
  const nodeEnv = String(env.NODE_ENV || '').toLowerCase();
  return nodeEnv !== 'production';
}

function cloneAnchorSections() {
  const out = {};
  for (const [key, section] of Object.entries(ANCHOR_BLUEPRINT_SECTIONS)) {
    out[key] = {
      ...section,
      evidenceIds: [...(section.evidenceIds || [])],
      unknowns: [...(section.unknowns || [])],
    };
  }
  return out;
}

function cloneAnchorNormalizedFacts() {
  return {
    ...ANCHOR_NORMALIZED_FACTS,
    ideal_customers: [...ANCHOR_NORMALIZED_FACTS.ideal_customers],
    disqualified_customers: [...ANCHOR_NORMALIZED_FACTS.disqualified_customers],
    geography: [...ANCHOR_NORMALIZED_FACTS.geography],
  };
}

module.exports = {
  ANCHOR_SAMPLE_CLIENT_ID,
  ANCHOR_FIXTURE_KEY,
  ANCHOR_BUSINESS_NAME,
  ANCHOR_NORMALIZED_FACTS,
  ANCHOR_BLUEPRINT_SECTIONS,
  fixturesAllowed,
  cloneAnchorSections,
  cloneAnchorNormalizedFacts,
};
