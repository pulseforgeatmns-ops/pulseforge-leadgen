const assert = require('assert');
const { buildPerProspectVariants } = require('../packages/max/workspace/PaigeVariantsExecutor');
const { PERSONALIZATION_STATUS, FACT_TYPES } = require('../utils/scoutPersonalizationEvidence');

const variants = buildPerProspectVariants({
  clientId: 10,
  max: {
    rankedTargets: [{
      name: 'Mill City Property Management',
      candidateId: 'co-mill',
      rationale: 'High fit property manager',
    }],
  },
  plan: {
    market: { label: 'property management', segment: 'property_management' },
    geography: { label: 'Manchester' },
  },
  crmByProspectId: {
    'co-mill': {
      first_name: 'Alex',
      company_name: 'Mill City Property Management',
      acquisition_metadata: {
        scout_personalization: {
          personalization_status: PERSONALIZATION_STATUS.SUPPORTED,
          observed_fact: 'Mill City Property Management serves multiple rental portfolios',
          source_url: 'https://millcitypm.com/about',
          supporting_excerpt: 'We manage multiple rental portfolios across Greater Manchester.',
          fact_type: FACT_TYPES.PROPERTY_MANAGEMENT,
          confidence: 0.82,
          checked_at: '2026-09-17T12:00:00.000Z',
        },
      },
      company_fields: {
        name: 'Mill City Property Management',
      },
    },
  },
});

assert.equal(variants.length, 1);
assert.match(variants[0].subject, /Cleaning for Mill City Property Management/i);
assert.match(variants[0].body, /Managed properties often need recurring cleaning/i);
assert.doesNotMatch(variants[0].body, /I saw /i);
assert.match(variants[0].body, /walk the space, agree on the areas and frequency/i);
assert.equal(variants[0].attributableIntelligence.usedPersonalization, true);

const fallbackVariants = buildPerProspectVariants({
  clientId: 10,
  max: {
    rankedTargets: [{
      name: 'Plain Office LLC',
      candidateId: 'co-plain',
    }],
  },
  crmByProspectId: {
    'co-plain': {
      first_name: 'Jamie',
      company_name: 'Plain Office LLC',
      acquisition_metadata: {
        scout_personalization: {
          personalization_status: PERSONALIZATION_STATUS.NO_USEFUL_FACT,
          confidence: 0,
          checked_at: '2026-09-17T12:00:00.000Z',
        },
      },
      company_fields: { name: 'Plain Office LLC' },
    },
  },
});

assert.equal(fallbackVariants[0].subject, 'Cleaning for Plain Office LLC');
assert.doesNotMatch(fallbackVariants[0].body, /I saw /);
assert.match(fallbackVariants[0].body, /Want me to send over what we'd need to price the office properly/i);
assert.equal(fallbackVariants[0].attributableIntelligence.usedPersonalization, false);

const nonAnchor = buildPerProspectVariants({
  clientId: 1,
  max: {
    rankedTargets: [{ name: 'Pulseforge Cafe', candidateId: 'co-cafe' }],
  },
});
assert.match(nonAnchor[0].subject, /Commercial cleaning for Pulseforge Cafe/i);

console.log('anchor paige variant tests passed');
