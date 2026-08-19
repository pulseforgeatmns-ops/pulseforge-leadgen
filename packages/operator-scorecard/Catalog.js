'use strict';

/**
 * SPEC-116 — explainable metric catalog and profile templates.
 * Deterministic. Max never invents a metric without a reason.
 */

const { CATEGORIES, INDICATORS, PROFILES, BUSINESS_STAGES, asText } = require('./types');

function metric(partial) {
  return {
    key: partial.key,
    name: partial.name,
    category: partial.category,
    indicator: partial.indicator || INDICATORS.LEADING,
    defaultConfidence: partial.defaultConfidence == null ? 0.86 : partial.defaultConfidence,
    reason: partial.reason,
    businessOutcome: partial.businessOutcome,
    stages: partial.stages || Object.values(BUSINESS_STAGES),
    profiles: partial.profiles || ['*'],
  };
}

const CATALOG = Object.freeze([
  metric({
    key: 'qualified_prospects',
    name: 'Qualified Prospects',
    category: CATEGORIES.ACQUISITION,
    defaultConfidence: 0.94,
    reason:
      'Measures whether acquisition efforts consistently identify businesses matching the approved ICP.',
    businessOutcome: 'Repeatable Acquisition',
  }),
  metric({
    key: 'qualified_commercial_prospects',
    name: 'Qualified Commercial Prospects',
    category: CATEGORIES.ACQUISITION,
    defaultConfidence: 0.93,
    reason:
      'Measures whether outreach is finding commercial accounts that match the approved beachhead rather than filling a list.',
    businessOutcome: 'Repeatable Acquisition',
    profiles: [PROFILES.COMMERCIAL_CLEANING],
  }),
  metric({
    key: 'positive_reply_rate',
    name: 'Positive Reply Rate',
    category: CATEGORIES.ACQUISITION,
    defaultConfidence: 0.88,
    reason:
      'A positive reply is evidence that the message reached a real decision-maker and created a conversation, not just an open.',
    businessOutcome: 'Repeatable Acquisition',
  }),
  metric({
    key: 'outreach_response_rate',
    name: 'Outreach Response Rate',
    category: CATEGORIES.ACQUISITION,
    defaultConfidence: 0.86,
    reason:
      'Shows whether acquisition language is earning replies from the intended buyer, which is the first proof that the market can be reached repeatably.',
    businessOutcome: 'Repeatable Acquisition',
  }),
  metric({
    key: 'discovery_calls',
    name: 'Discovery Calls',
    category: CATEGORIES.ACQUISITION,
    defaultConfidence: 0.9,
    reason:
      'Discovery calls convert interest into a live conversation where pain, fit, and next steps can be confirmed.',
    businessOutcome: 'Repeatable Acquisition',
  }),
  metric({
    key: 'walkthrough_requests',
    name: 'Walkthrough Requests',
    category: CATEGORIES.ACQUISITION,
    defaultConfidence: 0.92,
    reason:
      'A walkthrough request is the commercial buying motion for facilities work — it proves the prospect is willing to let the operator inspect the site.',
    businessOutcome: 'Repeatable Acquisition',
    profiles: [PROFILES.COMMERCIAL_CLEANING],
  }),
  metric({
    key: 'walkthrough_completion_rate',
    name: 'Walkthrough Completion Rate',
    category: CATEGORIES.ACQUISITION,
    defaultConfidence: 0.9,
    reason:
      'Requested walkthroughs that never happen leak the pipeline before a proposal can be written.',
    businessOutcome: 'Repeatable Acquisition',
    profiles: [PROFILES.COMMERCIAL_CLEANING],
  }),
  metric({
    key: 'qualified_meetings',
    name: 'Qualified Meetings',
    category: CATEGORIES.ACQUISITION,
    defaultConfidence: 0.87,
    reason:
      'Qualified meetings are the handoff from acquisition into a sales conversation with a fit buyer.',
    businessOutcome: 'Repeatable Acquisition',
  }),
  metric({
    key: 'pain_confirmation_rate',
    name: 'Pain Confirmation Rate',
    category: CATEGORIES.MARKET_VALIDATION,
    defaultConfidence: 0.91,
    reason:
      'Confirms that the pains in the approved AIM are showing up in live conversations, not only in research.',
    businessOutcome: 'Market Validation',
    stages: [BUSINESS_STAGES.MARKET_VALIDATION, BUSINESS_STAGES.REPEATABLE_ACQUISITION],
  }),
  metric({
    key: 'icp_confidence',
    name: 'ICP Confidence',
    category: CATEGORIES.MARKET_VALIDATION,
    defaultConfidence: 0.89,
    reason:
      'Tracks whether live conversations increase or decrease confidence in the approved ideal customer.',
    businessOutcome: 'Market Validation',
    stages: [BUSINESS_STAGES.MARKET_VALIDATION, BUSINESS_STAGES.REPEATABLE_ACQUISITION],
  }),
  metric({
    key: 'message_resonance',
    name: 'Message Resonance',
    category: CATEGORIES.MARKET_VALIDATION,
    defaultConfidence: 0.84,
    reason:
      'Measures whether buyers recognize themselves in the language Max and Paige are using.',
    businessOutcome: 'Market Validation',
  }),
  metric({
    key: 'most_common_pain_category',
    name: 'Most Common Pain Category',
    category: CATEGORIES.MARKET_VALIDATION,
    defaultConfidence: 0.88,
    reason:
      'Shows which AIM pain category is appearing most often so acquisition can concentrate on the real market.',
    businessOutcome: 'Market Validation',
    profiles: [PROFILES.FOUNDER_TRANSFORMATION],
  }),
  metric({
    key: 'new_pain_categories',
    name: 'New Pain Categories',
    category: CATEGORIES.MARKET_VALIDATION,
    defaultConfidence: 0.8,
    reason:
      'Surfaces pains the published AIM does not yet cover so the model can be revised deliberately.',
    businessOutcome: 'Market Validation',
    stages: [BUSINESS_STAGES.MARKET_VALIDATION],
  }),
  metric({
    key: 'objection_themes',
    name: 'Objection Themes',
    category: CATEGORIES.MARKET_VALIDATION,
    defaultConfidence: 0.85,
    reason:
      'Recurring objections are market evidence. They tell Max which claims need better proof.',
    businessOutcome: 'Market Validation',
  }),
  metric({
    key: 'objection_frequency',
    name: 'Objection Frequency',
    category: CATEGORIES.MARKET_VALIDATION,
    defaultConfidence: 0.83,
    reason:
      'How often a given objection appears shows whether it is noise or a structural barrier.',
    businessOutcome: 'Market Validation',
  }),
  metric({
    key: 'opportunities_created',
    name: 'Opportunities Created',
    category: CATEGORIES.SALES,
    indicator: INDICATORS.LEADING,
    defaultConfidence: 0.86,
    reason:
      'Counts commercial opportunities that entered a real sales process after qualification.',
    businessOutcome: 'Revenue',
  }),
  metric({
    key: 'win_rate',
    name: 'Win Rate',
    category: CATEGORIES.SALES,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.88,
    reason:
      'Win rate tells whether qualified conversations become paying work.',
    businessOutcome: 'Revenue',
  }),
  metric({
    key: 'sales_cycle',
    name: 'Sales Cycle',
    category: CATEGORIES.SALES,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.82,
    reason:
      'Cycle length reveals friction between first conversation and close.',
    businessOutcome: 'Revenue',
  }),
  metric({
    key: 'enrollment_rate',
    name: 'Enrollment Rate',
    category: CATEGORIES.SALES,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.9,
    reason:
      'Enrollment is the commercial conversion for a transformation offer — the moment a buyer commits to the method.',
    businessOutcome: 'Pilot Enrollments',
    profiles: [PROFILES.FOUNDER_TRANSFORMATION],
  }),
  metric({
    key: 'pilot_enrollments',
    name: 'Pilot Enrollments',
    category: CATEGORIES.COMMERCIAL,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.92,
    reason:
      'Pilot enrollments prove the methodology can be sold, not only described.',
    businessOutcome: 'Repeatable Acquisition',
    profiles: [PROFILES.FOUNDER_TRANSFORMATION],
  }),
  metric({
    key: 'discovery_enrollment_conversion',
    name: 'Discovery → Enrollment Conversion',
    category: CATEGORIES.COMMERCIAL,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.9,
    reason:
      'Measures whether discovery conversations produce enrollments — the commercial proof of a repeatable acquisition process.',
    businessOutcome: 'Repeatable Acquisition',
    profiles: [PROFILES.FOUNDER_TRANSFORMATION],
  }),
  metric({
    key: 'proposal_acceptance_rate',
    name: 'Proposal Acceptance Rate',
    category: CATEGORIES.COMMERCIAL,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.91,
    reason:
      'Proposal acceptance is the commercial close for recurring service work.',
    businessOutcome: 'Recurring Revenue',
    profiles: [PROFILES.COMMERCIAL_CLEANING, PROFILES.HOME_RENOVATION],
  }),
  metric({
    key: 'monthly_recurring_clients',
    name: 'Monthly Recurring Clients',
    category: CATEGORIES.COMMERCIAL,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.93,
    reason:
      'Recurring clients are the durable commercial outcome of a cleaning acquisition engine.',
    businessOutcome: 'Recurring Revenue',
    profiles: [PROFILES.COMMERCIAL_CLEANING],
  }),
  metric({
    key: 'recurring_revenue',
    name: 'Recurring Revenue',
    category: CATEGORIES.BUSINESS_OUTCOMES,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.94,
    reason:
      'Recurring revenue is the financial expression of retained commercial work.',
    businessOutcome: 'Revenue',
    profiles: [PROFILES.COMMERCIAL_CLEANING],
  }),
  metric({
    key: 'monthly_recurring_revenue',
    name: 'Monthly Recurring Revenue',
    category: CATEGORIES.BUSINESS_OUTCOMES,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.9,
    reason:
      'MRR is the lagging financial proof that acquisition and retention are compounding.',
    businessOutcome: 'Revenue',
  }),
  metric({
    key: 'completion_rate',
    name: 'Completion Rate',
    category: CATEGORIES.DELIVERY,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.86,
    reason:
      'Delivery completion is the operational proof that sold work is actually finished.',
    businessOutcome: 'Client Retention',
  }),
  metric({
    key: 'customer_satisfaction',
    name: 'Customer Satisfaction',
    category: CATEGORIES.DELIVERY,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.84,
    reason:
      'Satisfaction predicts retention and referrals more honestly than activity volume.',
    businessOutcome: 'Client Retention',
  }),
  metric({
    key: 'client_retention',
    name: 'Client Retention',
    category: CATEGORIES.OPERATIONS,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.92,
    reason:
      'Retention is the operational counterpart of recurring revenue — lost clients erase acquisition gains.',
    businessOutcome: 'Client Retention',
    stages: [BUSINESS_STAGES.OPERATIONAL_SCALE, BUSINESS_STAGES.MATURE_GROWTH, BUSINESS_STAGES.REPEATABLE_ACQUISITION],
  }),
  metric({
    key: 'repeat_business',
    name: 'Repeat Business',
    category: CATEGORIES.DELIVERY,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.85,
    reason:
      'Repeat work shows the offer created value worth buying again.',
    businessOutcome: 'Client Retention',
  }),
  metric({
    key: 'referral_rate',
    name: 'Referral Rate',
    category: CATEGORIES.OPERATIONS,
    indicator: INDICATORS.LEADING,
    defaultConfidence: 0.87,
    reason:
      'Referrals are the cheapest proof that delivery quality is strong enough to sell itself.',
    businessOutcome: 'Repeatable Acquisition',
    profiles: [PROFILES.COMMERCIAL_CLEANING, PROFILES.HOME_RENOVATION],
  }),
  metric({
    key: 'referral_partners_created',
    name: 'Referral Partners Created',
    category: CATEGORIES.ACQUISITION,
    indicator: INDICATORS.LEADING,
    defaultConfidence: 0.86,
    reason:
      'A named referral partner is a durable acquisition asset, not a one-off introduction.',
    businessOutcome: 'Repeatable Acquisition',
  }),
  metric({
    key: 'cleaner_utilization',
    name: 'Cleaner Utilization',
    category: CATEGORIES.OPERATIONS,
    indicator: INDICATORS.LEADING,
    defaultConfidence: 0.88,
    reason:
      'Utilization shows whether the commercial engine is filling capacity or creating idle labor.',
    businessOutcome: 'Gross Margin',
    profiles: [PROFILES.COMMERCIAL_CLEANING],
    stages: [BUSINESS_STAGES.OPERATIONAL_SCALE, BUSINESS_STAGES.MATURE_GROWTH, BUSINESS_STAGES.REPEATABLE_ACQUISITION],
  }),
  metric({
    key: 'students_started',
    name: 'Students Started',
    category: CATEGORIES.TRANSFORMATION,
    indicator: INDICATORS.LEADING,
    defaultConfidence: 0.9,
    reason:
      'Starts prove the transformation offer is being delivered, not only sold.',
    businessOutcome: 'Business Transformation',
    profiles: [PROFILES.FOUNDER_TRANSFORMATION],
  }),
  metric({
    key: 'students_completed',
    name: 'Students Completed',
    category: CATEGORIES.TRANSFORMATION,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.93,
    reason:
      'Completion is the operational proof that the founder transformation methodology works in practice.',
    businessOutcome: 'Business Transformation',
    profiles: [PROFILES.FOUNDER_TRANSFORMATION],
    stages: [BUSINESS_STAGES.OPERATIONAL_SCALE, BUSINESS_STAGES.MATURE_GROWTH, BUSINESS_STAGES.REPEATABLE_ACQUISITION],
  }),
  metric({
    key: 'student_completion_rate',
    name: 'Student Completion Rate',
    category: CATEGORIES.TRANSFORMATION,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.92,
    reason:
      'Completion rate is the right success metric once the methodology is validated and the work is delivery at scale.',
    businessOutcome: 'Business Transformation',
    profiles: [PROFILES.FOUNDER_TRANSFORMATION],
    stages: [BUSINESS_STAGES.OPERATIONAL_SCALE, BUSINESS_STAGES.MATURE_GROWTH],
  }),
  metric({
    key: 'reported_business_improvements',
    name: 'Reported Business Improvements',
    category: CATEGORIES.TRANSFORMATION,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.88,
    reason:
      'Reported improvements are the customer-facing proof that transformation happened.',
    businessOutcome: 'Business Transformation',
    profiles: [PROFILES.FOUNDER_TRANSFORMATION],
  }),
  metric({
    key: 'testimonials_generated',
    name: 'Testimonials Generated',
    category: CATEGORIES.BUSINESS_OUTCOMES,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.84,
    reason:
      'Testimonials are evidence the market will vouch for the outcome, which supports later acquisition.',
    businessOutcome: 'Business Transformation',
  }),
  metric({
    key: 'revenue',
    name: 'Revenue',
    category: CATEGORIES.BUSINESS_OUTCOMES,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.9,
    reason:
      'Revenue is a lagging business outcome. It confirms the engine works but does not explain why.',
    businessOutcome: 'Revenue',
  }),
  metric({
    key: 'gross_margin',
    name: 'Gross Margin',
    category: CATEGORIES.BUSINESS_OUTCOMES,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.86,
    reason:
      'Margin tells whether growth is healthy or is buying revenue with unprofitable delivery.',
    businessOutcome: 'Gross Margin',
    stages: [BUSINESS_STAGES.OPERATIONAL_SCALE, BUSINESS_STAGES.MATURE_GROWTH],
  }),
  metric({
    key: 'active_clients',
    name: 'Active Clients',
    category: CATEGORIES.BUSINESS_OUTCOMES,
    indicator: INDICATORS.LAGGING,
    defaultConfidence: 0.88,
    reason:
      'Active clients are the stock of the commercial relationship, not just flow from this week\'s outreach.',
    businessOutcome: 'Revenue',
  }),
]);

const PROFILE_KEYS = Object.freeze({
  [PROFILES.FOUNDER_TRANSFORMATION]: [
    'qualified_prospects',
    'outreach_response_rate',
    'discovery_calls',
    'pain_confirmation_rate',
    'icp_confidence',
    'most_common_pain_category',
    'objection_themes',
    'pilot_enrollments',
    'discovery_enrollment_conversion',
    'students_started',
    'students_completed',
    'reported_business_improvements',
    'testimonials_generated',
  ],
  [PROFILES.COMMERCIAL_CLEANING]: [
    'qualified_commercial_prospects',
    'walkthrough_requests',
    'walkthrough_completion_rate',
    'proposal_acceptance_rate',
    'monthly_recurring_clients',
    'recurring_revenue',
    'client_retention',
    'referral_rate',
    'cleaner_utilization',
  ],
  [PROFILES.HOME_RENOVATION]: [
    'qualified_prospects',
    'discovery_calls',
    'proposal_acceptance_rate',
    'win_rate',
    'revenue',
    'client_retention',
    'referral_rate',
    'testimonials_generated',
  ],
  [PROFILES.DEFAULT]: [
    'qualified_prospects',
    'positive_reply_rate',
    'discovery_calls',
    'qualified_meetings',
    'opportunities_created',
    'win_rate',
    'revenue',
    'active_clients',
  ],
});

const PROFILE_GOALS = Object.freeze({
  [PROFILES.FOUNDER_TRANSFORMATION]:
    'Validate founder transformation methodology and establish a repeatable acquisition process.',
  [PROFILES.COMMERCIAL_CLEANING]:
    'Establish a repeatable commercial acquisition engine.',
  [PROFILES.HOME_RENOVATION]:
    'Build a repeatable pipeline of qualified home-renovation jobs.',
  [PROFILES.DEFAULT]:
    'Establish a repeatable acquisition process and measure whether it creates commercial value.',
});

function getCatalogEntry(key) {
  return CATALOG.find((row) => row.key === key) || null;
}

function profileFits(entry, profile) {
  if (!entry.profiles || entry.profiles.includes('*')) return true;
  if (!profile) return entry.profiles.includes(PROFILES.DEFAULT);
  return entry.profiles.includes(profile) || entry.profiles.includes(PROFILES.DEFAULT);
}

function stageFits(entry, stage) {
  if (!entry.stages || !entry.stages.length) return true;
  if (!stage) return true;
  return entry.stages.includes(stage);
}

function metricsForProfile(profile, _stage) {
  const keys = PROFILE_KEYS[profile] || PROFILE_KEYS[PROFILES.DEFAULT];
  return keys.map(getCatalogEntry).filter(Boolean);
}

function detectProfile(input = {}) {
  const blob = [
    input.profile,
    input.businessName,
    input.businessGoal,
    input.vertical,
    input.businessModel,
    ...(input.objectives || []),
  ]
    .map(asText)
    .join(' ')
    .toLowerCase();
  if (input.profile && Object.values(PROFILES).includes(input.profile)) return input.profile;
  if (/babrun|founder transformation|student completion|pilot enrollment/.test(blob)) {
    return PROFILES.FOUNDER_TRANSFORMATION;
  }
  if (/anchor|commercial cleaning|walkthrough|cleaner utilization/.test(blob)) {
    return PROFILES.COMMERCIAL_CLEANING;
  }
  if (/home renovation|mshi|siding|deck/.test(blob)) return PROFILES.HOME_RENOVATION;
  return PROFILES.DEFAULT;
}

module.exports = {
  CATALOG,
  PROFILE_KEYS,
  PROFILE_GOALS,
  getCatalogEntry,
  profileFits,
  stageFits,
  metricsForProfile,
  detectProfile,
};
