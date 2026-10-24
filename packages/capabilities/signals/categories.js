'use strict';

/**
 * Signal categories, TTLs, and Campaign messaging postures (SPEC-031).
 */

const { SIGNAL_CATEGORY } = require('./types');

/** Soft decay start (days) · hard expire (days) — ADR-018 defaults. */
const CATEGORY_TTL_DAYS = Object.freeze({
  [SIGNAL_CATEGORY.BUYING]: { softDecayDays: 30, hardExpireDays: 90 },
  [SIGNAL_CATEGORY.GROWTH]: { softDecayDays: 45, hardExpireDays: 120 },
  [SIGNAL_CATEGORY.ORGANIZATIONAL]: { softDecayDays: 60, hardExpireDays: 180 },
  [SIGNAL_CATEGORY.OPERATIONAL]: { softDecayDays: 90, hardExpireDays: 365 },
  [SIGNAL_CATEGORY.MARKETING]: { softDecayDays: 30, hardExpireDays: 90 },
});

/**
 * Campaign Builder messaging posture keyed by signal type family.
 * Contract only — Campaign must not invent a type that is not Active.
 */
const MESSAGING_POSTURE = Object.freeze({
  expansion: 'growth',
  new_location: 'growth',
  office_expansion: 'growth',
  service_expansion: 'growth',
  team_growth: 'growth',
  hiring: 'operational_efficiency',
  hiring_office_staff: 'operational_efficiency',
  hiring_facilities: 'operational_efficiency',
  hiring_activity: 'operational_efficiency',
  property_acquisition: 'recurring_maintenance',
  new_lease: 'recurring_maintenance',
  office_renovation: 'recurring_maintenance',
  expansion_announcement: 'recurring_maintenance',
  multi_location: 'operational_efficiency',
  commercial_footprint: 'operational_efficiency',
  commercial_office: 'operational_efficiency',
  new_website: 'growth',
  recent_rebrand: 'growth',
});

const POSTURE_COPY = Object.freeze({
  growth:
    'Growth messaging — tie capacity to expansion and facility readiness.',
  operational_efficiency:
    'Operational efficiency messaging — reduce friction as headcount or footprint grows.',
  recurring_maintenance:
    'Recurring maintenance messaging — new/changed space needs consistent facility care.',
});

/**
 * @param {string} category
 * @returns {{ softDecayDays: number, hardExpireDays: number }}
 */
function ttlForCategory(category) {
  return (
    CATEGORY_TTL_DAYS[category] || CATEGORY_TTL_DAYS[SIGNAL_CATEGORY.OPERATIONAL]
  );
}

/**
 * @param {string} type
 * @returns {'growth'|'operational_efficiency'|'recurring_maintenance'|null}
 */
function postureForSignalType(type) {
  const key = String(type || '').toLowerCase();
  if (MESSAGING_POSTURE[key]) return MESSAGING_POSTURE[key];
  if (key.includes('hir')) return 'operational_efficiency';
  if (key.includes('expans') || key.includes('location') || key.includes('growth')) {
    return 'growth';
  }
  if (
    key.includes('lease') ||
    key.includes('renovat') ||
    key.includes('acquisit') ||
    key.includes('propert')
  ) {
    return 'recurring_maintenance';
  }
  return null;
}

/**
 * @param {string} posture
 * @returns {string}
 */
function postureDescription(posture) {
  return POSTURE_COPY[posture] || '';
}

module.exports = {
  CATEGORY_TTL_DAYS,
  MESSAGING_POSTURE,
  POSTURE_COPY,
  ttlForCategory,
  postureForSignalType,
  postureDescription,
};
