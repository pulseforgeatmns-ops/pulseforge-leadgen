const OPPORTUNITY_TRANSITIONS = Object.freeze({
  identified: ['contacted', 'lost', 'cancelled'],
  contacted: ['qualified', 'lost', 'cancelled'],
  qualified: ['quoted', 'lost', 'cancelled'],
  quoted: ['booked', 'lost', 'cancelled'],
  booked: ['won', 'lost', 'cancelled'],
  won: [],
  lost: [],
  cancelled: [],
});

const JOB_TRANSITIONS = Object.freeze({
  scheduled: ['en_route', 'in_progress', 'cancelled'],
  en_route: ['in_progress'],
  in_progress: ['completed', 'partially_completed', 'customer_disputed'],
  completed: ['customer_disputed'],
  partially_completed: ['customer_disputed'],
  customer_disputed: [],
  cancelled: [],
  failed: [],
});

const LEAD_SOURCES = new Set([
  'yelp', 'google_ads', 'google_lsa', 'organic_search', 'direct', 'referral',
  'repeat_customer', 'outbound_email', 'outbound_phone', 'linkedin', 'str_partner',
  'cleaning_company_overflow', 'property_manager', 'realtor', 'manual', 'unknown',
]);

const ATTRIBUTION_STATUSES = new Set([
  'confirmed', 'deterministic', 'inferred', 'unattributed', 'disputed',
]);

function domainError(code, message, status = 400) {
  const error = new Error(message);
  error.code = code;
  error.status = status;
  return error;
}

function assertTransition(kind, from, to) {
  const transitions = kind === 'opportunity' ? OPPORTUNITY_TRANSITIONS : JOB_TRANSITIONS;
  if (!transitions[from] || !transitions[from].includes(to)) {
    throw domainError('INVALID_TRANSITION', `Invalid ${kind} transition: ${from} -> ${to}`, 409);
  }
  return true;
}

function requireCents(value, field, { allowZero = true } = {}) {
  if (!Number.isSafeInteger(value) || value < 0 || (!allowZero && value === 0)) {
    throw domainError('INVALID_AMOUNT', `${field} must be a non-negative integer number of cents`);
  }
  return value;
}

function calculateRevenue({
  quotedAmountCents = 0,
  finalAmountCents = 0,
  jobStatus = 'scheduled',
  successfulPaymentsCents = 0,
  refundedCents = 0,
  directCostCents = null,
}) {
  [quotedAmountCents, finalAmountCents, successfulPaymentsCents, refundedCents]
    .forEach((value, index) => requireCents(value, ['quotedAmountCents', 'finalAmountCents', 'successfulPaymentsCents', 'refundedCents'][index]));
  if (directCostCents !== null) requireCents(directCostCents, 'directCostCents');
  if (refundedCents > successfulPaymentsCents) {
    throw domainError('REFUND_EXCEEDS_PAYMENT', 'Refunds cannot exceed successful payments', 409);
  }

  const delivered = ['completed', 'partially_completed', 'customer_disputed'].includes(jobStatus)
    ? finalAmountCents
    : 0;
  const collected = successfulPaymentsCents - refundedCents;
  const revenueForProfit = Math.max(0, delivered - refundedCents);
  const grossProfit = directCostCents === null ? null : revenueForProfit - directCostCents;
  const grossMargin = grossProfit === null || revenueForProfit <= 0
    ? null
    : Number((grossProfit / revenueForProfit).toFixed(4));

  return {
    bookedRevenueCents: quotedAmountCents,
    deliveredRevenueCents: delivered,
    collectedRevenueCents: collected,
    refundedRevenueCents: refundedCents,
    grossProfitCents: grossProfit,
    grossMargin,
  };
}

function outcomeStatus({ jobStatus, deliveredRevenueCents, collectedRevenueCents, refundedRevenueCents, successfulPaymentsCents }) {
  if (jobStatus === 'cancelled') return 'cancelled';
  if (jobStatus === 'customer_disputed') return 'disputed';
  if (refundedRevenueCents > 0 && refundedRevenueCents >= successfulPaymentsCents) return 'refunded';
  if (refundedRevenueCents > 0) return 'partially_refunded';
  if (deliveredRevenueCents > 0 && collectedRevenueCents >= deliveredRevenueCents) return 'paid';
  if (collectedRevenueCents > 0) return 'partially_paid';
  if (deliveredRevenueCents > 0) return 'delivered';
  return 'booked';
}

function normalizeLeadSource(value) {
  const source = String(value || 'unknown').trim().toLowerCase();
  if (!LEAD_SOURCES.has(source)) throw domainError('INVALID_LEAD_SOURCE', `Unsupported lead source: ${source}`);
  return source;
}

function normalizeAttributionStatus(value, hasProspect = false) {
  const status = value || (hasProspect ? 'deterministic' : 'unattributed');
  if (!ATTRIBUTION_STATUSES.has(status)) {
    throw domainError('INVALID_ATTRIBUTION_STATUS', `Unsupported attribution status: ${status}`);
  }
  return status;
}

module.exports = {
  ATTRIBUTION_STATUSES,
  JOB_TRANSITIONS,
  LEAD_SOURCES,
  OPPORTUNITY_TRANSITIONS,
  assertTransition,
  calculateRevenue,
  domainError,
  normalizeAttributionStatus,
  normalizeLeadSource,
  outcomeStatus,
  requireCents,
};
