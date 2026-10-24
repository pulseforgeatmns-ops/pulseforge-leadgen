const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  resolveResult,
  validateScorecardPayload,
  isHighIntent,
} = require('../lib/scorecardScoring');

function baseAnswers(overrides = {}) {
  return {
    business_type: 'residential',
    monthly_inquiries: '11-30',
    after_hours_process: 'owner_answers',
    missed_call_text: 'yes',
    quote_follow_up_speed: 'same_day',
    quote_follow_up_count: '2',
    automatic_review_request: 'yes',
    current_system: 'spreadsheet',
    typical_job_value: '250-500',
    name: 'Alex Owner',
    business_name: 'Sparkle Clean Co',
    email: 'alex@example.com',
    mobile: '6035550100',
    marketing_consent: false,
    ...overrides,
  };
}

describe('scorecard routing', () => {
  it('routes to Call Recovery Gap when missed calls lack consistent response', () => {
    const result = resolveResult(baseAnswers({
      missed_call_text: 'no',
      quote_follow_up_speed: 'rarely',
      automatic_review_request: 'no',
    }));
    assert.equal(result.category, 'call_recovery_gap');
    assert.equal(result.title, 'Call Recovery Gap');
  });

  it('routes to Call Recovery Gap for inconsistent after-hours process', () => {
    const result = resolveResult(baseAnswers({
      after_hours_process: 'inconsistent',
      missed_call_text: 'yes',
    }));
    assert.equal(result.category, 'call_recovery_gap');
  });

  it('routes to Quote Follow-Up Gap when call recovery is fine but quotes lag', () => {
    const result = resolveResult(baseAnswers({
      quote_follow_up_speed: '3_plus_days',
      quote_follow_up_count: 'until_no',
      automatic_review_request: 'no',
    }));
    assert.equal(result.category, 'quote_follow_up_gap');
  });

  it('routes to Quote Follow-Up Gap for thin follow-up count', () => {
    const result = resolveResult(baseAnswers({
      quote_follow_up_count: '0',
      automatic_review_request: 'no',
    }));
    assert.equal(result.category, 'quote_follow_up_gap');
  });

  it('routes to Review Growth Gap only when other gaps are not dominant', () => {
    const result = resolveResult(baseAnswers({
      automatic_review_request: 'sometimes',
    }));
    assert.equal(result.category, 'review_growth_gap');
  });

  it('does not prefer review when a call gap exists', () => {
    const result = resolveResult(baseAnswers({
      missed_call_text: 'sometimes',
      automatic_review_request: 'no',
    }));
    assert.equal(result.category, 'call_recovery_gap');
  });

  it('marks high intent for 31+ monthly inquiries', () => {
    assert.equal(isHighIntent(baseAnswers({ monthly_inquiries: '31-75' })), true);
    assert.equal(isHighIntent(baseAnswers({ monthly_inquiries: '76+' })), true);
    assert.equal(isHighIntent(baseAnswers({ monthly_inquiries: '11-30' })), false);
  });

  it('marks high intent for Jobber/ServiceM8 or CRM', () => {
    assert.equal(isHighIntent(baseAnswers({ current_system: 'jobber_servicem8' })), true);
    assert.equal(isHighIntent(baseAnswers({ current_system: 'crm' })), true);
    assert.equal(isHighIntent(baseAnswers({ current_system: 'nothing' })), false);
  });

  it('sets assessment as primary CTA for high-intent leads', () => {
    const result = resolveResult(baseAnswers({
      monthly_inquiries: '31-75',
      missed_call_text: 'no',
    }));
    assert.equal(result.high_intent, true);
    assert.equal(result.primary_cta, 'assessment');
  });

  it('sets kit as primary CTA for standard leads', () => {
    const result = resolveResult(baseAnswers({
      monthly_inquiries: '0-10',
      current_system: 'spreadsheet',
      missed_call_text: 'no',
    }));
    assert.equal(result.high_intent, false);
    assert.equal(result.primary_cta, 'kit');
  });
});

describe('scorecard result payoff', () => {
  const {
    formatJobValueIllustration,
    RECOVERY_PLAN_STEPS,
  } = require('../lib/scorecardScoring');

  it('includes category meaning and first-move copy for each gap', () => {
    const call = resolveResult(baseAnswers({ missed_call_text: 'no' }));
    assert.equal(call.category, 'call_recovery_gap');
    assert.match(call.payoff.meaning, /live buying opportunity/i);
    assert.match(call.payoff.first_move, /missed-call text-back/i);

    const quote = resolveResult(baseAnswers({ quote_follow_up_count: '0' }));
    assert.equal(quote.category, 'quote_follow_up_gap');
    assert.match(quote.payoff.meaning, /sending a quote is not the same/i);
    assert.match(quote.payoff.first_move, /last 14 days/i);

    const review = resolveResult(baseAnswers({ automatic_review_request: 'no' }));
    assert.equal(review.category, 'review_growth_gap');
    assert.match(review.payoff.meaning, /visible proof/i);
    assert.match(review.payoff.first_move, /last five satisfied/i);
  });

  it('illustrates job value with range language, never a precise invented amount', () => {
    assert.match(
      formatJobValueIllustration('under_250'),
      /approximately under \$250 in booked work/
    );
    assert.match(
      formatJobValueIllustration('250-500'),
      /approximately \$250–\$500 in booked work/
    );
    assert.match(
      formatJobValueIllustration('500-1000'),
      /approximately \$500–\$1,000 in booked work/
    );
    assert.match(
      formatJobValueIllustration('1000-plus'),
      /approximately \$1,000 or more in booked work/
    );

    for (const key of ['under_250', '250-500', '500-1000', '1000-plus']) {
      const line = formatJobValueIllustration(key);
      assert.match(line, /illustration, not a promise/i);
      assert.doesNotMatch(line, /guarantee|will generate|conversion rate/i);
    }

    const result = resolveResult(baseAnswers({
      missed_call_text: 'no',
      typical_job_value: '500-1000',
    }));
    assert.equal(result.payoff.job_value_key, '500-1000');
    assert.equal(
      result.payoff.job_value_illustration,
      formatJobValueIllustration('500-1000')
    );
    assert.deepEqual(result.payoff.recovery_plan, [...RECOVERY_PLAN_STEPS]);
  });
});

describe('scorecard validation', () => {
  it('requires contact fields and valid email/mobile', () => {
    const bad = validateScorecardPayload(baseAnswers({ email: 'not-an-email', mobile: '123' }));
    assert.equal(bad.ok, false);
    assert.ok(bad.errors.some((e) => e.includes('email')));
    assert.ok(bad.errors.some((e) => e.includes('mobile')));
  });

  it('defaults marketing consent to false and rejects non-boolean', () => {
    const ok = validateScorecardPayload(baseAnswers({ marketing_consent: undefined }));
    assert.equal(ok.ok, true);
    assert.equal(ok.answers.marketing_consent, false);

    const bad = validateScorecardPayload(baseAnswers({ marketing_consent: 'yes' }));
    assert.equal(bad.ok, false);
  });

  it('rejects invalid enum values', () => {
    const bad = validateScorecardPayload(baseAnswers({ business_type: 'industrial' }));
    assert.equal(bad.ok, false);
  });
});
