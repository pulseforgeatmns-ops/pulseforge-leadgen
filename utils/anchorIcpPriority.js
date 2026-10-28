'use strict';

// Anchor (tenant 10) acquisition priority. This orders eligible inventory; it
// never admits, rejects or contacts anyone. Mission scope, admission, ownership,
// DNC and suppression stay authoritative. Lower tier is worked first.
const { normalizeVertical } = require('./normalize');

const ANCHOR_ICP_TIERS = Object.freeze({
  school: 1,
  campus: 1,
  property_manager: 2,
  commercial_office: 2,
  manufacturer: 3,
  warehouse: 3,
  dealership: 4,
  restaurant: 6,
  str_manager: 8,
  realtor: 9,
});
const DEFAULT_TIER = 7;
const RESIDENTIAL_PROPERTY_MANAGER_TIER = 5;

const COMMERCIAL_SIGNAL = /\b(?:commercial|industrial|office|retail|facilit(?:y|ies)|business park|office park|medical office)\b/i;
const RESIDENTIAL_SIGNAL = /\b(?:residential|apartments?|rental homes?|single[- ]family|tenant placement|vacation|hoa|condo(?:minium)?s?)\b/i;

function rowText(row = {}) {
  return [row.company_name, row.company, row.name, row.industry, row.vertical, row.description, row.snippet]
    .filter(value => value != null).map(String).join(' ');
}

function anchorIcpTier(row = {}) {
  const vertical = normalizeVertical(row.vertical || row.industry || '');
  const tier = ANCHOR_ICP_TIERS[vertical] ?? DEFAULT_TIER;
  if (vertical !== 'property_manager') return tier;
  // Facility-purchasing responsibility is the point. A residential-only
  // manager stays eligible but follows commercial operators.
  const text = rowText(row);
  return RESIDENTIAL_SIGNAL.test(text) && !COMMERCIAL_SIGNAL.test(text) ? RESIDENTIAL_PROPERTY_MANAGER_TIER : tier;
}

// Mission segments, not row verticals. Managed facilities spans tiers 1-4 and
// leads with its institutional categories.
const SEGMENT_TIERS = Object.freeze({ managed_facilities: 1, restaurant_foh: 6, property_management: 2,
  short_term_rental: 8, short_term_rental_operators: 8, real_estate: 9 });

function anchorSegmentTier(segment) {
  const key = normalizeVertical(segment || '');
  return SEGMENT_TIERS[key] ?? ANCHOR_ICP_TIERS[key] ?? DEFAULT_TIER;
}

// Discovery spends at most one new page per cycle, so segment order decides
// which approved categories are searched first. Membership is unchanged.
function sortSegmentsByAnchorIcpPriority(segments = []) {
  return segments.map((segment, index) => ({ segment, index, tier: anchorSegmentTier(segment) }))
    .sort((a, b) => a.tier - b.tier || a.index - b.index)
    .map(entry => entry.segment);
}

// Stable: rows in the same tier keep the caller's existing order.
function sortByAnchorIcpPriority(rows = []) {
  return rows.map((row, index) => ({ row, index, tier: anchorIcpTier(row) }))
    .sort((a, b) => a.tier - b.tier || a.index - b.index)
    .map(entry => entry.row);
}

module.exports = { ANCHOR_ICP_TIERS, anchorIcpTier, sortByAnchorIcpPriority, anchorSegmentTier, sortSegmentsByAnchorIcpPriority };
