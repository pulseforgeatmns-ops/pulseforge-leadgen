'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { inferCityFromProvenance, applyCityToMetadata } = require('../services/leviScoutProvenance');

test('trusted Chicago and NYC source hosts infer city without using the entity name', () => {
  assert.deepEqual(inferCityFromProvenance({
    sourceUrls: ['https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=1'],
  }).city, 'Chicago');
  assert.equal(inferCityFromProvenance({
    permalink: 'https://data.cityofnewyork.us/resource/w9ak-ipjd.json?job_filing_number=B00-1',
  }).city, 'New York');
  assert.equal(inferCityFromProvenance({
    city: null,
    location: 'Chicago Holdings LLC',
  }).city, null);
});

test('ambiguous source hosts and existing cities are not overwritten', () => {
  const ambiguous = inferCityFromProvenance({
    sourceUrls: [
      'https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=1',
      'https://data.cityofnewyork.us/resource/w9ak-ipjd.json?job_filing_number=B00-1',
    ],
  });
  assert.equal(ambiguous.city, null);
  assert.equal(ambiguous.source, 'ambiguous_source_hosts');
  assert.equal(ambiguous.conflict, true);

  const preserved = applyCityToMetadata({
    city: 'Chicago',
    sourceUrls: ['https://data.cityofnewyork.us/resource/w9ak-ipjd.json?job_filing_number=B00-1'],
  });
  assert.equal(preserved.metadata.city, 'Chicago');
  assert.equal(preserved.metadata.cityInference.source, 'preserved_existing_city');
  assert.equal(preserved.metadata.cityInference.conflict, true);
});

test('conflicting trusted city evidence preserves the existing city and records the conflict', () => {
  const applied = applyCityToMetadata({
    city: 'Chicago',
    provider: 'chicago_open_data_permits',
    sourceUrls: [
      'https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=1',
      'https://data.cityofnewyork.us/resource/w9ak-ipjd.json?job_filing_number=B00-1',
    ],
  });
  assert.equal(applied.metadata.city, 'Chicago');
  assert.equal(applied.metadata.provider, 'chicago_open_data_permits');
  assert.equal(applied.metadata.cityInference.conflict, true);
  assert.equal(applied.metadata.cityInference.source, 'preserved_existing_city');
  assert.ok(applied.metadata.cityInference.conflictingCities.includes('Chicago'));
  assert.ok(applied.metadata.cityInference.conflictingCities.includes('New York'));

  const unresolved = applyCityToMetadata({
    sourceUrls: [
      'https://data.cityofchicago.org/resource/ydr8-5enu.json?permit_=1',
      'https://data.cityofnewyork.us/resource/w9ak-ipjd.json?job_filing_number=B00-1',
    ],
  });
  assert.equal(unresolved.metadata.city, undefined);
  assert.equal(unresolved.metadata.cityInference.conflict, true);
  assert.equal(unresolved.metadata.cityInference.source, 'conflicting_trusted_evidence');
});

test('service-area match backfills city only when no city is stored', () => {
  const applied = applyCityToMetadata({}, {
    location: '100 N MICHIGAN AVE, Chicago, IL',
  });
  assert.equal(applied.metadata.city, 'Chicago');
  assert.equal(applied.metadata.cityInference.source, 'service_area_match');
  assert.equal(applied.metadata.provider, 'chicago_open_data_permits');
});
