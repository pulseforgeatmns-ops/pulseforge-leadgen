const test = require('node:test');
const assert = require('node:assert/strict');
const {
  JOB_TRANSITIONS,
  OPPORTUNITY_TRANSITIONS,
  assertTransition,
  calculateRevenue,
  normalizeAttributionStatus,
  normalizeLeadSource,
  outcomeStatus,
} = require('../utils/revenueDomain');

test('every declared opportunity transition is accepted', () => {
  for (const [from, destinations] of Object.entries(OPPORTUNITY_TRANSITIONS)) {
    for (const to of destinations) assert.equal(assertTransition('opportunity', from, to), true);
  }
});

test('opportunity transitions fail closed, including lost to booked', () => {
  assert.throws(() => assertTransition('opportunity', 'lost', 'booked'), { code: 'INVALID_TRANSITION' });
  assert.throws(() => assertTransition('opportunity', 'identified', 'won'), { code: 'INVALID_TRANSITION' });
  assert.throws(() => assertTransition('opportunity', 'invented', 'quoted'), { code: 'INVALID_TRANSITION' });
});

test('every declared job transition is accepted', () => {
  for (const [from, destinations] of Object.entries(JOB_TRANSITIONS)) {
    for (const to of destinations) assert.equal(assertTransition('job', from, to), true);
  }
});

test('job completion cannot happen from an invalid state', () => {
  assert.throws(() => assertTransition('job', 'scheduled', 'completed'), { code: 'INVALID_TRANSITION' });
  assert.throws(() => assertTransition('job', 'completed', 'scheduled'), { code: 'INVALID_TRANSITION' });
});

test('booking is expected revenue but not delivered or collected revenue', () => {
  assert.deepEqual(calculateRevenue({ quotedAmountCents: 25000 }), {
    bookedRevenueCents: 25000,
    deliveredRevenueCents: 0,
    collectedRevenueCents: 0,
    refundedRevenueCents: 0,
    grossProfitCents: null,
    grossMargin: null,
  });
});

test('completed unpaid job recognizes delivery but not cash', () => {
  const result = calculateRevenue({
    quotedAmountCents: 25000,
    finalAmountCents: 27500,
    jobStatus: 'completed',
    directCostCents: 12000,
  });
  assert.equal(result.deliveredRevenueCents, 27500);
  assert.equal(result.collectedRevenueCents, 0);
  assert.equal(result.grossProfitCents, 15500);
  assert.equal(outcomeStatus({ jobStatus: 'completed', ...result, successfulPaymentsCents: 0 }), 'delivered');
});

test('successful payment recognizes collected revenue once', () => {
  const result = calculateRevenue({
    quotedAmountCents: 27500,
    finalAmountCents: 27500,
    jobStatus: 'completed',
    successfulPaymentsCents: 27500,
    directCostCents: 12000,
  });
  assert.equal(result.collectedRevenueCents, 27500);
  assert.equal(result.grossProfitCents, 15500);
  assert.equal(outcomeStatus({ jobStatus: 'completed', ...result, successfulPaymentsCents: 27500 }), 'paid');
});

test('partial payment changes cash status without rewriting delivered gross profit', () => {
  const result = calculateRevenue({
    quotedAmountCents: 27500,
    finalAmountCents: 27500,
    jobStatus: 'completed',
    successfulPaymentsCents: 10000,
    directCostCents: 12000,
  });
  assert.equal(result.collectedRevenueCents, 10000);
  assert.equal(result.grossProfitCents, 15500);
  assert.equal(outcomeStatus({ jobStatus: 'completed', ...result, successfulPaymentsCents: 10000 }), 'partially_paid');
});

test('partial refund is compensating net cash and gross profit', () => {
  const result = calculateRevenue({
    quotedAmountCents: 27500,
    finalAmountCents: 27500,
    jobStatus: 'completed',
    successfulPaymentsCents: 27500,
    refundedCents: 5000,
    directCostCents: 12000,
  });
  assert.equal(result.collectedRevenueCents, 22500);
  assert.equal(result.refundedRevenueCents, 5000);
  assert.equal(result.grossProfitCents, 10500);
  assert.equal(outcomeStatus({ jobStatus: 'completed', ...result, successfulPaymentsCents: 27500 }), 'partially_refunded');
});

test('refund cannot exceed successful payment', () => {
  assert.throws(() => calculateRevenue({ successfulPaymentsCents: 1000, refundedCents: 1001 }), { code: 'REFUND_EXCEEDS_PAYMENT' });
});

test('amounts must be safe integer cents', () => {
  assert.throws(() => calculateRevenue({ quotedAmountCents: 10.5 }), { code: 'INVALID_AMOUNT' });
  assert.throws(() => calculateRevenue({ quotedAmountCents: Number.NaN }), { code: 'INVALID_AMOUNT' });
  assert.throws(() => calculateRevenue({ quotedAmountCents: -1 }), { code: 'INVALID_AMOUNT' });
});

test('attribution preserves explicit uncertainty and controlled taxonomy', () => {
  assert.equal(normalizeLeadSource('YELP'), 'yelp');
  assert.equal(normalizeAttributionStatus(undefined, true), 'deterministic');
  assert.equal(normalizeAttributionStatus(undefined, false), 'unattributed');
  assert.equal(normalizeAttributionStatus('disputed'), 'disputed');
  assert.throws(() => normalizeLeadSource('made_up_source'), { code: 'INVALID_LEAD_SOURCE' });
  assert.throws(() => normalizeAttributionStatus('certain-ish'), { code: 'INVALID_ATTRIBUTION_STATUS' });
});
