'use strict';

/**
 * SPEC-162 — Initial heuristic library.
 * Reusable business patterns learned from prior investigations.
 */

const { buildBusinessHeuristic, HEURISTIC_CATEGORIES } = require('./types');

const INITIAL_HEURISTICS = Object.freeze([
  buildBusinessHeuristic({
    id: 'growth_market',
    name: 'Growth Market',
    category: HEURISTIC_CATEGORIES.MARKET_GROWTH,
    description: 'Commercial activity appears to be increasing in the surrounding market corridor.',
    triggerConditions: {
      patterns: [
        /national\s+retailer/i,
        /starbucks/i,
        /apartment\s+construction/i,
        /new\s+apartment/i,
        /population\s+growth/i,
        /hotel\s+development/i,
        /industrial\s+park/i,
        /business\s+park/i,
        /commercial\s+corridor/i,
        /new\s+development/i,
      ],
      minMatches: 3,
      understandingKinds: ['growth'],
    },
    implications: [
      'Commercial opportunity increasing.',
      'Businesses entering early often establish preferred vendor relationships.',
      'Prioritize outreach before competitors.',
    ],
    confidenceModifier: 0.05,
    evidenceRequirements: { minSignals: 3 },
  }),
  buildBusinessHeuristic({
    id: 'vendor_instability',
    name: 'Vendor Instability',
    category: HEURISTIC_CATEGORIES.VENDOR_REPLACEMENT,
    description: 'Signals suggest incumbent vendor relationships may be weakening.',
    triggerConditions: {
      patterns: [
        /negative.*clean/i,
        /cleanliness.*review/i,
        /poor\s+clean/i,
        /new\s+operations?\s+manager/i,
        /new\s+ownership/i,
        /ownership\s+change/i,
        /poor\s+response/i,
        /facilities?\s+staff/i,
        /hiring\s+facilities/i,
        /vendor\s+dissatisfaction/i,
      ],
      minMatches: 2,
      understandingKinds: ['service_need', 'buying_signal'],
    },
    implications: [
      'Higher probability of vendor change.',
      'Leadership changes often precede vendor evaluation.',
      'Cleaning satisfaction appears weak.',
    ],
    confidenceModifier: 0.04,
    evidenceRequirements: { minSignals: 2 },
    contradicts: ['vendor_stability'],
  }),
  buildBusinessHeuristic({
    id: 'vendor_stability',
    name: 'Vendor Stability',
    category: HEURISTIC_CATEGORIES.VENDOR_STABILITY,
    description: 'Long-standing vendor relationships appear entrenched.',
    triggerConditions: {
      patterns: [
        /long.?standing\s+vendor/i,
        /preferred\s+vendor/i,
        /incumbent\s+vendor/i,
        /satisfied\s+with\s+clean/i,
        /vendor\s+relationship.*years/i,
        /same\s+cleaning\s+company/i,
      ],
      minMatches: 1,
    },
    implications: [
      'Growing company BUT long-standing vendor relationships may slow switching.',
      'Vendor change less likely in the near term.',
    ],
    confidenceModifier: 0.03,
    evidenceRequirements: { minSignals: 1 },
    contradicts: ['vendor_instability'],
  }),
  buildBusinessHeuristic({
    id: 'buying_readiness',
    name: 'Buying Readiness',
    category: HEURISTIC_CATEGORIES.BUYING_READINESS,
    description: 'Organizational signals suggest a buying window may be opening.',
    triggerConditions: {
      patterns: [
        /funding\s+announcement/i,
        /series\s+[a-d]/i,
        /expansion/i,
        /hiring/i,
        /relocation/i,
        /acquisition/i,
        /portfolio\s+growth/i,
        /new\s+location/i,
      ],
      minMatches: 2,
      understandingKinds: ['buying_signal', 'growth'],
    },
    implications: [
      'Buying window likely opening.',
      'Business appears likely to evaluate vendors within the next 90 days.',
    ],
    confidenceModifier: 0.06,
    evidenceRequirements: { minSignals: 2 },
  }),
  buildBusinessHeuristic({
    id: 'expansion_hiring',
    name: 'Expansion Hiring',
    category: HEURISTIC_CATEGORIES.BUYING_READINESS,
    description: 'Leadership or operational hiring suggests organizational change.',
    triggerConditions: {
      patterns: [
        /operations?\s+manager\s+hired/i,
        /hiring\s+operations?\s+manager/i,
        /new\s+operations?\s+manager/i,
        /hiring\s+clean/i,
        /hiring\s+staff/i,
      ],
      minMatches: 1,
      understandingKinds: ['growth', 'buying_signal'],
    },
    implications: [
      'Leadership changes often precede vendor evaluation.',
      'Operational scale-up increases service procurement needs.',
    ],
    confidenceModifier: 0.04,
    evidenceRequirements: { minSignals: 1 },
  }),
  buildBusinessHeuristic({
    id: 'operational_maturity',
    name: 'Operational Maturity',
    category: HEURISTIC_CATEGORIES.OPERATIONAL_MATURITY,
    description: 'Business shows signs of formalized operations and decision structure.',
    triggerConditions: {
      patterns: [
        /standardized\s+brand/i,
        /sop/i,
        /multi.?location/i,
        /leadership\s+structure/i,
        /franchise/i,
        /corporate\s+office/i,
      ],
      minMatches: 2,
      understandingKinds: ['business_model'],
    },
    implications: [
      'Decision process likely formalized.',
      'Expect longer evaluation cycles with multiple stakeholders.',
    ],
    confidenceModifier: 0.02,
    evidenceRequirements: { minSignals: 2 },
  }),
  buildBusinessHeuristic({
    id: 'relationship_leverage',
    name: 'Relationship Leverage',
    category: HEURISTIC_CATEGORIES.RELATIONSHIP_LEVERAGE,
    description: 'Warm introduction or referral paths may exist.',
    triggerConditions: {
      patterns: [
        /shared\s+vendor/i,
        /shared\s+investor/i,
        /chamber\s+of\s+commerce/i,
        /chamber\s+member/i,
        /existing\s+client/i,
        /referral\s+partner/i,
        /common\s+association/i,
      ],
      minMatches: 1,
    },
    implications: [
      'Warm introduction opportunities exist.',
      'Relationship-led outreach may outperform cold contact.',
    ],
    confidenceModifier: 0.03,
    evidenceRequirements: { minSignals: 1 },
  }),
  buildBusinessHeuristic({
    id: 'hospitality_concentration',
    name: 'Hospitality Concentration',
    category: HEURISTIC_CATEGORIES.MARKET_GROWTH,
    description: 'Hospitality or short-term rental density suggests recurring turnover cleaning demand.',
    triggerConditions: {
      patterns: [
        /vacation\s+rental/i,
        /short.?term\s+rental/i,
        /airbnb|vrbo/i,
        /hospitality/i,
        /hotel/i,
        /guest\s+turnover/i,
      ],
      minMatches: 2,
      understandingKinds: ['business_model', 'service_need'],
      assertionPatterns: [/short-term rental|STR|vacation rental/i],
    },
    implications: [
      'Recurring turnover cleaning demand likely.',
      'High unit velocity increases vendor evaluation frequency.',
    ],
    confidenceModifier: 0.04,
    evidenceRequirements: { minSignals: 2 },
  }),
]);

function cloneHeuristicLibrary(library = INITIAL_HEURISTICS) {
  return library.map((heuristic) =>
    buildBusinessHeuristic({
      ...heuristic,
      triggerConditions: { ...heuristic.triggerConditions },
      applicability: { ...heuristic.applicability },
      evidenceRequirements: { ...heuristic.evidenceRequirements },
      implications: heuristic.implications.slice(),
      contradicts: (heuristic.contradicts || []).slice(),
    })
  );
}

function getHeuristicById(library, id) {
  return (library || INITIAL_HEURISTICS).find((h) => h.id === id) || null;
}

module.exports = {
  INITIAL_HEURISTICS,
  cloneHeuristicLibrary,
  getHeuristicById,
};
