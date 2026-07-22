/**
 * Revenue Leak Scorecard — pure scoring / routing.
 * Used by the public POST handler and unit tests. Keep browser logic in sync
 * via the API response (client does not re-score independently).
 */

const MONTHLY_HIGH = new Set(['31-75', '76+']);
const COMPATIBLE_SYSTEMS = new Set(['jobber_servicem8', 'crm']);

const CALL_PROCESS_GAP = new Set(['inconsistent', 'none', 'voicemail_only']);
const MISSED_CALL_GAP = new Set(['no', 'sometimes']);
const QUOTE_SPEED_GAP = new Set(['3_plus_days', 'rarely']);
const QUOTE_COUNT_GAP = new Set(['0', '1']);
const REVIEW_GAP = new Set(['no', 'sometimes']);

const RESULT_COPY = {
  call_recovery_gap: {
    category: 'call_recovery_gap',
    title: 'Call Recovery Gap',
    summary:
      'Missed and after-hours calls are not getting a consistent, timely response. That is usually the first place booked jobs quietly disappear.',
  },
  quote_follow_up_gap: {
    category: 'quote_follow_up_gap',
    title: 'Quote Follow-Up Gap',
    summary:
      'Quotes are going out without a reliable follow-up cadence. Warm inquiries cool off when the next touch takes too long or stops after one try.',
  },
  review_growth_gap: {
    category: 'review_growth_gap',
    title: 'Review Growth Gap',
    summary:
      'Review requests are not running consistently. That limits social proof that helps the next prospect say yes — especially when call and quote follow-up are already in better shape.',
  },
};

function hasCallRecoveryGap(answers) {
  return (
    CALL_PROCESS_GAP.has(answers.after_hours_process) ||
    MISSED_CALL_GAP.has(answers.missed_call_text)
  );
}

function hasQuoteFollowUpGap(answers) {
  return (
    QUOTE_SPEED_GAP.has(answers.quote_follow_up_speed) ||
    QUOTE_COUNT_GAP.has(answers.quote_follow_up_count)
  );
}

function hasReviewGrowthGap(answers) {
  return REVIEW_GAP.has(answers.automatic_review_request);
}

function isHighIntent(answers) {
  return (
    MONTHLY_HIGH.has(answers.monthly_inquiries) ||
    COMPATIBLE_SYSTEMS.has(answers.current_system)
  );
}

/**
 * Resolve primary result category.
 * Priority: Call Recovery → Quote Follow-Up → Review Growth.
 * Review only wins when the other gaps are not dominant.
 */
function resolveResult(answers) {
  const callGap = hasCallRecoveryGap(answers);
  const quoteGap = hasQuoteFollowUpGap(answers);
  const reviewGap = hasReviewGrowthGap(answers);

  let category;
  if (callGap) {
    category = 'call_recovery_gap';
  } else if (quoteGap) {
    category = 'quote_follow_up_gap';
  } else if (reviewGap) {
    category = 'review_growth_gap';
  } else {
    // Default: still surface the most actionable ops gap for owners who
    // answered "good" across the board — call recovery remains the usual leak.
    category = 'call_recovery_gap';
  }

  const highIntent = isHighIntent(answers);
  const copy = RESULT_COPY[category];

  return {
    category: copy.category,
    title: copy.title,
    summary: copy.summary,
    high_intent: highIntent,
    primary_cta: highIntent ? 'assessment' : 'kit',
    gaps: {
      call_recovery: callGap,
      quote_follow_up: quoteGap,
      review_growth: reviewGap,
    },
  };
}

const REQUIRED_ANSWER_KEYS = [
  'business_type',
  'monthly_inquiries',
  'after_hours_process',
  'missed_call_text',
  'quote_follow_up_speed',
  'quote_follow_up_count',
  'automatic_review_request',
  'current_system',
  'typical_job_value',
  'name',
  'business_name',
  'email',
  'mobile',
];

const ALLOWED = {
  business_type: new Set(['residential', 'commercial', 'both']),
  monthly_inquiries: new Set(['0-10', '11-30', '31-75', '76+']),
  after_hours_process: new Set(['owner_answers', 'dedicated_coverage', 'voicemail_only', 'inconsistent', 'none']),
  missed_call_text: new Set(['yes', 'no', 'sometimes']),
  quote_follow_up_speed: new Set(['same_day', 'within_2_days', '3_plus_days', 'rarely']),
  quote_follow_up_count: new Set(['0', '1', '2', 'until_no']),
  automatic_review_request: new Set(['yes', 'no', 'sometimes']),
  current_system: new Set(['jobber_servicem8', 'crm', 'spreadsheet', 'nothing']),
  typical_job_value: new Set(['under_250', '250-500', '500-1000', '1000-plus']),
};

function validateScorecardPayload(body) {
  const errors = [];
  if (!body || typeof body !== 'object') {
    return { ok: false, errors: ['Invalid payload'] };
  }

  for (const key of REQUIRED_ANSWER_KEYS) {
    const value = typeof body[key] === 'string' ? body[key].trim() : '';
    if (!value) {
      errors.push(`${key} is required`);
      continue;
    }
    if (ALLOWED[key] && !ALLOWED[key].has(value)) {
      errors.push(`${key} is invalid`);
    }
  }

  const email = String(body.email || '').trim();
  if (email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    errors.push('email is invalid');
  }

  const mobile = String(body.mobile || '').replace(/\D/g, '');
  if (mobile && mobile.length < 10) {
    errors.push('mobile is invalid');
  }

  // Marketing consent is optional and must default unchecked — only accept boolean.
  if (body.marketing_consent != null && typeof body.marketing_consent !== 'boolean') {
    errors.push('marketing_consent must be a boolean');
  }

  if (errors.length) return { ok: false, errors };

  const answers = {};
  for (const key of REQUIRED_ANSWER_KEYS) {
    answers[key] = String(body[key]).trim();
  }
  answers.marketing_consent = Boolean(body.marketing_consent);

  return { ok: true, answers };
}

module.exports = {
  resolveResult,
  validateScorecardPayload,
  hasCallRecoveryGap,
  hasQuoteFollowUpGap,
  hasReviewGrowthGap,
  isHighIntent,
  RESULT_COPY,
  ALLOWED,
};
