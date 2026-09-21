const assert = require('assert');
const {
  PERSONALIZATION_STATUS,
  FACT_TYPES,
  normalizePersonalizationEvidence,
  extractSnippetPersonalizationFact,
  extractPlacesPersonalizationFact,
  readScoutPersonalizationFromMetadata,
} = require('../utils/scoutPersonalizationEvidence');

const normalized = normalizePersonalizationEvidence({
  personalization_status: 'SUPPORTED',
  observed_fact: '  multiple locations across New Hampshire  ',
  source_url: 'https://example.com/locations',
  supporting_excerpt: 'We operate multiple locations across New Hampshire.',
  fact_type: 'MULTI_LOCATION',
  confidence: 1.5,
});
assert.equal(normalized.personalization_status, PERSONALIZATION_STATUS.SUPPORTED);
assert.equal(normalized.observed_fact, 'multiple locations across New Hampshire');
assert.equal(normalized.fact_type, FACT_TYPES.MULTI_LOCATION);
assert.equal(normalized.confidence, 1);
assert.ok(normalized.checked_at);

const snippetFact = extractSnippetPersonalizationFact({
  snippet: 'Full-service property management company serving Charleston and Dunbar.',
  url: 'millcitypm.com',
});
assert.equal(snippetFact.personalization_status, PERSONALIZATION_STATUS.SUPPORTED);
assert.equal(snippetFact.fact_type, FACT_TYPES.PROPERTY_MANAGEMENT);
assert.match(snippetFact.observed_fact, /property management/i);

const placesFact = extractPlacesPersonalizationFact({
  address: '100 Commercial Street, Manchester, NH',
  url: 'harborlaw.com',
  place_types: ['lawyer', 'point_of_interest', 'establishment'],
});
assert.equal(placesFact, null);

const noFact = normalizePersonalizationEvidence({
  personalization_status: PERSONALIZATION_STATUS.NO_USEFUL_FACT,
});
assert.equal(noFact.observed_fact, null);
assert.equal(noFact.confidence, 0);

const metadata = readScoutPersonalizationFromMetadata({
  scout_personalization: {
    personalization_status: 'SUPPORTED',
    observed_fact: 'our 3 locations in Manchester, Bedford, and Hooksett',
    source_url: 'https://example.com/locations',
    supporting_excerpt: 'Visit our 3 locations in Manchester, Bedford, and Hooksett.',
    fact_type: 'MULTI_LOCATION',
    confidence: 0.8,
    checked_at: '2026-09-17T12:00:00.000Z',
  },
});
assert.equal(metadata.fact_type, FACT_TYPES.MULTI_LOCATION);

console.log('scoutPersonalizationEvidence tests passed');
