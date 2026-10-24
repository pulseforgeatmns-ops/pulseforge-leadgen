'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  reviewCorrectionOperations,
  projectWorkingSemanticOperations,
  sectionsFromNormalizedFacts,
  buildExecutiveSummary,
} = require('../services/clientIntelligenceInterview');
const { generateDraftScorecard } = require('../packages/operator-scorecard');

const ACTIVE_OBJECTIVE =
  'Prove Babrun can reliably acquire the right founder customers for the 12-week program, beginning with enrolling the first qualified founder and learning which segments and problems drive action.';

const CORRECTION = [
  'Babrun has one primary offer: the 12-week transformation program. Managing employees, delegation, reducing founder dependence, and reducing the owner\'s day-to-day operational burden are transformation areas or outcomes, not separate services.',
  'Geography isn\'t currently a meaningful targeting constraint. We\'re primarily targeting by business stage and characteristics: operating small businesses, generally fewer than 10 employees, where the founder is still too central to operations. Cleaning and other home services, e-commerce, and fitness are initial segments to test.',
  'Our differentiation is still a hypothesis: the practical, transformation-focused 12-week approach may be more compelling than generic business education or open-ended advice because it changes how the owner actually operates. We have not validated that as a consistent buying reason yet.',
  'The core measures I actually want to watch are qualified founder conversations, ICP-qualified conversations, serious program conversations, paid enrollments, and discovery-to-enrollment conversion.',
  'Pain patterns and segment response patterns are learning signals. Lack of owner time, founder dependence, employee problems, and revenue pressure are pain categories, not standalone metrics. Raw lead volume is explicitly not a success metric.',
  'We have not established premium positioning for Babrun.',
].join('\n');

function contaminatedFacts() {
  return {
    business_name: 'Babrun',
    business_description: 'Babrun is a coaching programs for founders',
    services: ['delegation', 'premium positioning', '12-week coaching'],
    ideal_customers: ['any small business', 'ICP-qualified conversations'],
    ideal_customer_traits: [],
    disqualified_customers: ['businesses seeking generic advice'],
    geography: ['United States'],
    differentiation: 'premium positioning',
    brand_voice: 'I did not establish premium positioning for Babrun, so remove that assumption.',
    ninety_day_outcomes: 'Lack of owner time, founder dependence, and revenue pressure are pain categories, not standalone metrics.',
    growth_focus: ACTIVE_OBJECTIVE,
    success_metrics: [
      'ICP-Qualified Conversations',
      'Ideal Customer-Qualified Conversations',
      'Discovery-To-Enrollment Conversion',
      'Discovery-To-Enrollment Conversion.',
      'raw lead volume',
    ],
    epistemic_states: {
      business_description: 'KNOWN',
      services: 'KNOWN',
      ideal_customers: 'KNOWN',
      disqualified_customers: 'KNOWN',
      geography: 'KNOWN',
      differentiation: 'KNOWN',
      brand_voice: 'KNOWN',
      ninety_day_outcomes: 'KNOWN',
      success_metrics: 'KNOWN',
    },
    hypotheses: {},
    evidence_statements: {},
    business_facts: {
      ninety_day_outcomes: [{
        subject: 'ninety_day_outcomes',
        value: ACTIVE_OBJECTIVE,
        epistemic_state: 'KNOWN',
      }],
    },
    transformation_areas: [],
    pains: ['lack of owner time', 'founder dependence', 'revenue pressure'],
    learning_signals: [],
    excluded_metrics: ['raw lead volume'],
  };
}

function staleSections() {
  return {
    brandVoice: { summary: 'Current hypothesis: brand voice tone may align with I did not establish premium positioning for Babrun, so remove that assumption.' },
    campaignGoals: { summary: 'Near-term growth goals focus on Lack of owner time, founder dependence, and revenue pressure are pain categories, not standalone metrics.' },
    competitiveAdvantages: { summary: 'Current hypothesis: competitive advantage around Our hypothesis is that the practical, transformation-focused 12-week approach.' },
    avoidCustomers: { summary: 'The business prefers to avoid businesses seeking generic advice. The business prefers to avoid businesses seeking generic advice.' },
  };
}

describe('SPEC-229 correction authority through Blueprint projection', () => {
  it('passes all three gates with production-equivalent contamination', () => {
    let facts = contaminatedFacts();
    let sections = staleSections();
    for (let round = 0; round < 3; round += 1) {
      const operations = reviewCorrectionOperations(CORRECTION, { normalizedFacts: facts }, `turn-229-${round}`);
      facts = projectWorkingSemanticOperations(facts, operations);
      sections = sectionsFromNormalizedFacts(facts, sections);

      assert.equal(facts.ninety_day_outcomes, ACTIVE_OBJECTIVE);
      assert.doesNotMatch(facts.ninety_day_outcomes, /pain categor|not .*metric|correction/i);
      assert.equal(/premium positioning|did not establish/i.test(sections.brandVoice.summary || ''), false);
      assert.equal(/pain categor|not standalone metrics/i.test(sections.campaignGoals.summary || ''), false);
      assert.equal(/competitive advantage around Our hypothesis/i.test(sections.competitiveAdvantages.summary || ''), false);
      assert.equal((sections.avoidCustomers.summary.match(/businesses seeking generic advice/gi) || []).length, 1);

      const brief = buildExecutiveSummary(sections, { normalizedFacts: facts, clientId: 1 });
      const scorecard = generateDraftScorecard({
        tenantId: 'babrun',
        clientId: 1,
        businessName: 'Babrun',
        businessGoal: facts.ninety_day_outcomes,
        blueprint: { sections },
        normalizedFacts: facts,
        operatorMetrics: facts.success_metrics,
      });
      const rendered = JSON.stringify({ brief, scorecard });
      assert.doesNotMatch(rendered, /premium positioning|not standalone metrics|pain categories/);

      const metricKeys = scorecard.metrics.map((metric) => metric.key);
      assert.equal(metricKeys.filter((key) => key === 'icp_qualified_conversations').length, 1);
      assert.equal(metricKeys.filter((key) => key === 'discovery_enrollment_conversion').length, 1);
      assert.equal(metricKeys.filter((key) => key === 'raw_lead_volume').length, 0);
      assert.equal(scorecard.metrics.filter((metric) => /^(?:lack of owner time|founder dependence|employee problems|revenue pressure)$/i.test(metric.name)).length, 0);
      assert.equal(scorecard.metrics.some((metric) => /pain categories|not standalone metrics/i.test(metric.reason)), false);
      assert.equal(scorecard.objectives.includes(ACTIVE_OBJECTIVE), true);
    }
  });
});
