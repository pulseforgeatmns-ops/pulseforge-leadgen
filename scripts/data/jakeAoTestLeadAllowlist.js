'use strict';

const { normalizeAoBusinessKey } = require('../../utils/aoAssignment');

/** Exact allowlist — normalized match only; never ILIKE '%test%'. */
const JAKE_AO_TEST_LEAD_NAMES = Object.freeze([
  'Test Dental Office',
  'Test med spa',
  'Test Daycare',
  'test school',
  'test test',
]);

const JAKE_AO_TEST_LEAD_KEYS = Object.freeze(
  JAKE_AO_TEST_LEAD_NAMES.map(normalizeAoBusinessKey),
);

function isAllowlistedJakeAoTestLead(businessName) {
  return JAKE_AO_TEST_LEAD_KEYS.includes(normalizeAoBusinessKey(businessName));
}

module.exports = {
  JAKE_AO_TEST_LEAD_NAMES,
  JAKE_AO_TEST_LEAD_KEYS,
  isAllowlistedJakeAoTestLead,
  normalizeAoBusinessKey,
};
