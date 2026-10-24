'use strict';

const PROSPECT_MOTIONS = Object.freeze([
  'EMAIL_LED',
  'AO_LED',
  'HYBRID',
  'NURTURE',
  'SUPPRESS',
]);

const ASSIGNMENT_CATEGORIES = Object.freeze([
  'UNFAIR_ADVANTAGE',
  'HIGH_VALUE_ICP',
  'ROUTE_CLUSTER',
  'WALK_IN_OPPORTUNITY',
  'WARM_SIGNAL',
  'FOLLOW_UP_REQUIRED',
]);

const DEBRIEF_NEXT_ACTIONS = Object.freeze([
  'BOOK_ASSESSMENT',
  'SEND_INFO',
  'AO_FOLLOW_UP',
  'JAKE_REVIEW',
  'NURTURE',
  'SUPPRESS',
  'NEEDS_RESEARCH',
]);

const OPPORTUNITY_TYPES = Object.freeze([
  'recurring_cleaning',
  'backup_overflow',
  'turnover_support',
  'one_time_deep_clean',
  'common_area_cleaning',
  'not_a_fit',
]);

const OPPORTUNITY_STRENGTHS = Object.freeze(['strong', 'moderate', 'weak', 'unclear']);

const OPPORTUNITY_TIMING = Object.freeze(['now', 'later', 'not_at_all']);

const ADVISORY_STAGES = Object.freeze([
  'unassigned',
  'routed',
  'tasked',
  'in_progress',
  'debrief_pending',
  'debrief_complete',
  'closed',
]);

const DEBRIEF_QUALITIES = Object.freeze(['complete', 'incomplete', 'weak']);

const WEEKLY_LIST_MIX = Object.freeze({
  UNFAIR_ADVANTAGE: 5,
  HIGH_VALUE_ICP: 5,
  ROUTE_CLUSTER: 5,
  WALK_IN_OPPORTUNITY: 3,
});

const ANCHOR_SERVICE_AREA = Object.freeze([
  'Manchester', 'Bedford', 'Goffstown', 'Hooksett', 'Londonderry', 'Auburn',
]);

const HYBRID_VERTICALS = Object.freeze([
  'property_manager',
  'str_manager',
  'commercial_office',
  'law_firm',
  'accounting',
  'medical_dental',
  'med_spa',
  'dental',
]);

const RELATIONSHIP_VERTICALS = Object.freeze([
  'property_manager',
  'str_manager',
  'realtor',
  'restoration_remodeling_partner',
  'cleaning_company_overflow',
]);

const SEGMENT_PRIORITY = Object.freeze({
  cleaning_company_overflow: 15,
  str_manager: 12,
  property_manager: 15,
  realtor: 10,
  restoration_remodeling_partner: 8,
  commercial_office: 6,
  law_firm: 8,
  accounting: 8,
  medical_dental: 10,
});

const GENERIC_EMAIL_PREFIX_RE = /^(?:info|hello|contact|admin|support|sales|office|team|service|customerservice|customer\.?service|no-?reply|noreply|mail|inquir(?:y|ies))[\w.+-]*$/i;

function isGenericEmail(email) {
  if (!email) return false;
  const local = String(email).toLowerCase().split('@')[0] || '';
  return GENERIC_EMAIL_PREFIX_RE.test(local);
}

function verticalLabel(vertical) {
  const labels = {
    property_manager: 'Property management',
    str_manager: 'Short-term rental management',
    commercial_office: 'Professional office',
    law_firm: 'Law firm',
    accounting: 'CPA / accounting',
    medical_dental: 'Medical / dental',
    cleaning_company_overflow: 'Cleaning company (overflow partner)',
    realtor: 'Real estate',
    restoration_remodeling_partner: 'Restoration / remodeling partner',
  };
  return labels[vertical] || String(vertical || 'Commercial').replaceAll('_', ' ');
}

module.exports = {
  PROSPECT_MOTIONS,
  ASSIGNMENT_CATEGORIES,
  DEBRIEF_NEXT_ACTIONS,
  OPPORTUNITY_TYPES,
  OPPORTUNITY_STRENGTHS,
  OPPORTUNITY_TIMING,
  ADVISORY_STAGES,
  DEBRIEF_QUALITIES,
  WEEKLY_LIST_MIX,
  ANCHOR_SERVICE_AREA,
  HYBRID_VERTICALS,
  RELATIONSHIP_VERTICALS,
  SEGMENT_PRIORITY,
  GENERIC_EMAIL_PREFIX_RE,
  isGenericEmail,
  verticalLabel,
};
