'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  establishBusinessIdentity,
  leadHasEstablishedIdentity,
  collectIdentitySignals,
  EXISTENCE_SIGNAL_TYPES,
  ENRICHMENT_SIGNAL_TYPES,
} = require('../packages/scout/identity/BusinessIdentity');
const {
  resolveMarketHypothesis,
  expandSearchStrategies,
  expandPlacesQueriesForVertical,
  listMarketHypotheses,
  SEARCH_SOURCES,
} = require('../packages/scout/hypothesis/MarketHypothesisRegistry');

describe('ADR-092 — BusinessIdentity', () => {
  it('establishes identity from Google Places signals without a website', () => {
    const identity = establishBusinessIdentity({
      company: 'River City Property Management',
      place_id: 'ChIJabc123',
      address: '100 Elm St, Manchester, NH 03101',
      phone: '(603) 555-0100',
      google_review_count: 12,
      google_rating: 4.6,
      source: ['google_places'],
    });

    assert.equal(identity.established, true);
    assert.equal(identity.websiteOnly, false);
    assert.equal(identity.identityKey, 'place:ChIJabc123');
    assert.ok(identity.existenceSignals.some((row) => row.type === 'google_business_profile'));
    assert.equal(identity.enrichmentSignals.length, 0);
  });

  it('rejects website-only rows', () => {
    const identity = establishBusinessIdentity({
      url: 'example-cleaning.com',
    });

    assert.equal(identity.established, false);
    assert.equal(identity.websiteOnly, true);
    assert.equal(leadHasEstablishedIdentity({ url: 'example-cleaning.com' }), false);
  });

  it('treats website as enrichment when paired with name and address', () => {
    const identity = establishBusinessIdentity({
      company: 'Acme Janitorial',
      address: '22 Main St, Bedford NH',
      url: 'acmejanitorial.com',
    });

    assert.equal(identity.established, true);
    assert.ok(identity.enrichmentSignals.some((row) => row.type === 'website'));
    assert.ok(identity.existenceSignals.some((row) => row.type === 'name'));
  });

  it('exports existence vs enrichment signal partitions', () => {
    assert.ok(!EXISTENCE_SIGNAL_TYPES.includes('website'));
    assert.ok(ENRICHMENT_SIGNAL_TYPES.includes('website'));
    const signals = collectIdentitySignals({ company: 'Test Co', phone: '603-555-1212' });
    assert.ok(signals.some((row) => row.type === 'name'));
    assert.ok(signals.some((row) => row.type === 'phone'));
  });
});

describe('ADR-092 — MarketHypothesisRegistry', () => {
  it('registers Anchor immediate-cash verticals as hypotheses', () => {
    const ids = listMarketHypotheses();
    for (const key of [
      'cleaning_company_overflow',
      'str_manager',
      'property_manager',
      'realtor',
      'restoration_remodeling_partner',
      'commercial_office',
    ]) {
      assert.ok(ids.includes(key), `missing hypothesis ${key}`);
    }
  });

  it('property_manager hypothesis expands into multi-source strategies', () => {
    const hypothesis = resolveMarketHypothesis('property_manager');
    assert.ok(hypothesis);
    assert.match(hypothesis.statement, /likely buyers/i);

    const workloads = expandSearchStrategies(hypothesis, { city: 'Manchester', state: 'NH' });
    const sources = new Set(workloads.map((row) => row.source));

    assert.ok(sources.has(SEARCH_SOURCES.GOOGLE_PLACES));
    assert.ok(sources.has(SEARCH_SOURCES.BUSINESS_REGISTRY));
    assert.ok(sources.has(SEARCH_SOURCES.LINKEDIN));
    assert.ok(workloads.some((row) => row.query.includes('Manchester')));
  });

  it('expandPlacesQueriesForVertical returns deduped Places-ready queries', () => {
    const queries = expandPlacesQueriesForVertical('str_manager', {
      city: 'Manchester',
      state: 'NH',
    });
    assert.ok(queries.length >= 3);
    assert.equal(new Set(queries).size, queries.length);
    assert.ok(queries.every((q) => q.includes('Manchester')));
  });
});
