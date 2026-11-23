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

  const preserved = applyCityToMetadata({
    city: 'Chicago',
    sourceUrls: ['https://data.cityofnewyork.us/resource/w9ak-ipjd.json?job_filing_number=B00-1'],
  });
  assert.equal(preserved.metadata.city, 'Chicago');
  assert.equal(preserved.metadata.cityInference.source, 'preserved_existing_city');
});

test('service-area match backfills city only when no city is stored', () => {
  const applied = applyCityToMetadata({}, {
    location: '100 N MICHIGAN AVE, Chicago, IL',
  });
  assert.equal(applied.metadata.city, 'Chicago');
  assert.equal(applied.metadata.cityInference.source, 'service_area_match');
  assert.equal(applied.metadata.provider, 'chicago_open_data_permits');
});
