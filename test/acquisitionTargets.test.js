'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {
  GEOS,
  QUERIES,
  exclusionReason,
  franchiseMatch,
  isSpecialtyOnly,
  parseAddress,
  reviewDates,
  resolveServiceType,
  scoreAgingTarget,
} = require('../scripts/sourceAcquisitionTargets');

assert.deepEqual(GEOS, [
  'Manchester NH',
  'Bedford NH',
  'Nashua NH',
  'Hooksett NH',
  'Goffstown NH',
  'Merrimack NH',
  'Derry NH',
  'Concord NH',
]);
assert.deepEqual(QUERIES, [
  'commercial cleaning',
  'janitorial services',
  'office cleaning',
  'cleaning service',
]);

const scriptText = fs.readFileSync(path.join(__dirname, '../scripts/sourceAcquisitionTargets.js'), 'utf8');
assert.equal(/INSERT\s+INTO\s+prospects/i.test(scriptText), false);
assert.equal(/UPDATE\s+prospects/i.test(scriptText), false);

assert.equal(franchiseMatch('JAN-PRO of Southern New Hampshire'), 'Jan-Pro');
assert.equal(franchiseMatch('Local Office Cleaning LLC'), null);

assert.equal(isSpecialtyOnly('Manchester Window Cleaning'), true);
assert.equal(isSpecialtyOnly('Manchester Commercial Window Cleaning'), false);
assert.equal(isSpecialtyOnly('ABC Carpet Janitorial'), false);

assert.equal(exclusionReason({
  name: 'Molly Maid of Manchester',
  place_id: 'abc',
  user_ratings_total: 10,
}), 'franchise:Molly Maid');

assert.equal(exclusionReason({
  name: 'Local Commercial Cleaning',
  place_id: 'abc',
  user_ratings_total: 151,
}), 'review_count_gt_150');

assert.deepEqual(parseAddress('123 Elm St, Manchester, NH 03101, USA'), {
  address: '123 Elm St',
  city: 'Manchester',
  state: 'NH',
  zip: '03101',
});

const now = new Date('2026-07-09T00:00:00.000Z');
const dates = reviewDates({
  reviews: [
    { time: Math.floor(new Date('2026-01-01T00:00:00.000Z').getTime() / 1000) },
    { time: Math.floor(new Date('2020-06-01T00:00:00.000Z').getTime() / 1000) },
  ],
}, now);
assert.equal(dates.mostRecentReviewDate, '2026-01-01');
assert.equal(dates.reviewsLast12mo, 1);
assert.equal(dates.yearsOnGoogle, 6);

assert.equal(resolveServiceType({
  name: 'ABC Janitorial',
  types: ['cleaning_service'],
  reviews: [{ text: 'Office cleaning and home cleaning' }],
}), 'mixed');

const score = scoreAgingTarget({
  website_url: null,
  website_status: 'none',
  most_recent_review_date: '2024-01-01',
  reviews_last_12mo: 0,
  review_count: 12,
  phone_type: 'landline',
  years_on_google: 8,
}, now);
assert.equal(score.aging_score, 85);
assert.deepEqual(score.aging_signals, [
  'no_or_dead_website',
  'most_recent_review_over_18mo',
  'no_reviews_last_12mo',
  'landline_phone',
  'established_8yr_google_floor',
  'small_review_footprint',
]);

console.log('acquisition target tests passed');
