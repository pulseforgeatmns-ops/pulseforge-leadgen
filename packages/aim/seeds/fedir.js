'use strict';

/**
 * SPEC-112 — Fedir AIM seed.
 * Mission: transform founder-led businesses into business-machine businesses.
 * Geography, size bands, and case studies stay unknown until Fedir names them.
 */

const { AIM_STATUS, PAIN_CATEGORIES, PAIN_IDS } = require('../types');
const { buildMarketUnderstanding } = require('../MarketUnderstanding');
const { buildPainOntology } = require('../PainOntology');
const { buildPainKnowledge, knowledgeMap } = require('../KnowledgeCapture');

const FEDIR_CLIENT_KEY = 'fedir';

function fedirPainOntology() {
  return buildPainOntology([
    {
      id: PAIN_CATEGORIES.PEOPLE_MANAGEMENT,
      label: 'People Management',
      problems: [
        {
          id: PAIN_IDS.FOUNDER_DEPENDENCY,
          label: 'Founder Dependency',
          definition: 'The business cannot operate without the founder in the daily loop.',
          signals: [
            'owner replying to reviews',
            'hiring repeatedly',
            'job postings',
            'growth announcements',
            'i do everything myself',
            'wearing too many hats',
          ],
        },
        {
          id: PAIN_IDS.DELEGATION,
          label: 'Delegation',
          definition: 'Work still returns to the founder because nobody else is trusted with it.',
          signals: ['i have to check everything', 'cannot let go', 'no one else can do it'],
        },
        {
          id: PAIN_IDS.HIRING,
          label: 'Hiring',
          definition: 'The founder is stuck recruiting because there is no management layer.',
          signals: ['we are hiring', 'now hiring', 'join our team', 'job postings'],
        },
        {
          id: PAIN_IDS.ACCOUNTABILITY,
          label: 'Accountability',
          definition: 'Outcomes depend on the founder noticing, not on a management system.',
          signals: ['owner replies to reviews', 'founder still approving', 'no manager'],
        },
      ],
    },
    {
      id: PAIN_CATEGORIES.CUSTOMER_GROWTH,
      label: 'Customer Growth',
      problems: [
        {
          id: PAIN_IDS.INCONSISTENT_PIPELINE,
          label: 'Inconsistent Pipeline',
          definition: 'Demand arrives in bursts; the founder cannot predict next month.',
          signals: ['referral requests', 'looking for referrals', 'slow season'],
        },
        {
          id: PAIN_IDS.POOR_LEAD_GENERATION,
          label: 'Poor Lead Generation',
          definition: 'New work depends on who the founder already knows.',
          signals: ['irregular marketing', 'we need more leads', 'word of mouth only'],
        },
        {
          id: PAIN_IDS.WEAK_SALES_PROCESS,
          label: 'Weak Sales Process',
          definition: 'The founder is the sales process.',
          signals: ['discounting', 'price match', 'call me personally to book'],
        },
      ],
    },
    {
      id: PAIN_CATEGORIES.FINANCE,
      label: 'Finance',
      problems: [
        {
          id: PAIN_IDS.CASH_FLOW,
          label: 'Cash Flow',
          definition: 'Cash follows the founder\'s hustle, not a machine.',
          signals: ['financing', 'cash flow', 'waiting on receivables'],
        },
        {
          id: PAIN_IDS.PRICING,
          label: 'Pricing',
          definition: 'Price is set by gut and discounting, not by a system.',
          signals: ['price increases', 'we raised prices', 'discounting'],
        },
        {
          id: PAIN_IDS.PROFITABILITY,
          label: 'Profitability',
          definition: 'The founder cannot see whether the machine is actually profitable.',
          signals: ['cost-cutting', 'cost cutting', 'margins are tight'],
        },
      ],
    },
  ]);
}

function fedirKnowledge() {
  return [
    buildPainKnowledge({
      painId: PAIN_IDS.FOUNDER_DEPENDENCY,
      label: 'Founder Dependency',
      definition: 'The business cannot operate without the founder in the daily loop.',
      observableEvidence: [
        'Hiring repeatedly without a management layer appearing',
        'Owner personally replying to reviews',
        'Job postings written in the founder\'s voice',
        'Growth announcements that still center the founder',
      ],
      commonObjections: [
        'Nobody can do it like I can.',
        'I am not ready to let go.',
        'I do not have time to build systems.',
      ],
      typicalLanguage: [
        'I do everything myself.',
        'I am wearing too many hats.',
        'I cannot take a vacation.',
      ],
      recommendedMessaging: [
        'Talk about getting the founder out of the daily loop — systems and managers, not more hustle.',
        'CTA: a conversation about what still requires them, not a generic growth pitch.',
      ],
      discoveryQuestions: [
        'What happens if you are out for two weeks?',
        'Which decisions still require you personally?',
        'What breaks first when you step away?',
      ],
      caseStudies: [],
      successStories: [],
    }),
    buildPainKnowledge({
      painId: PAIN_IDS.DELEGATION,
      label: 'Delegation',
      definition: 'Work still returns to the founder because nobody else is trusted with it.',
      observableEvidence: ['Founder still approving routine work', 'No named managers'],
      commonObjections: ['They will not do it right.'],
      typicalLanguage: ['I have to check everything.'],
      recommendedMessaging: ['Managers and standards, not more personal effort.'],
      discoveryQuestions: ['What have you tried to hand off that came back to you?'],
      caseStudies: [],
      successStories: [],
    }),
    buildPainKnowledge({
      painId: PAIN_IDS.INCONSISTENT_PIPELINE,
      label: 'Inconsistent Pipeline',
      definition: 'Demand arrives in bursts; the founder cannot predict next month.',
      observableEvidence: ['Public referral asks', 'Irregular marketing cadence'],
      commonObjections: ['Our work is all word of mouth.'],
      typicalLanguage: ['It is feast or famine.'],
      recommendedMessaging: ['A machine that produces conversations — not another hustle sprint.'],
      discoveryQuestions: ['How predictable is next month\'s work today?'],
      caseStudies: [],
      successStories: [],
    }),
    buildPainKnowledge({
      painId: PAIN_IDS.CASH_FLOW,
      label: 'Cash Flow',
      definition: 'Cash follows the founder\'s hustle, not a machine.',
      observableEvidence: ['Financing mentions', 'Public cost-cutting'],
      commonObjections: ['We just need a couple more jobs.'],
      typicalLanguage: ['Cash is tight this month.'],
      recommendedMessaging: ['Profitability through the machine, not through the founder working harder.'],
      discoveryQuestions: ['If you stopped selling personally, what happens to cash in 60 days?'],
      caseStudies: [],
      successStories: [],
    }),
  ];
}

function buildFedirAim() {
  const market = buildMarketUnderstanding({
    mission: {
      question: 'What transformation does this client create?',
      transformation: 'Transform founder-led businesses into business-machine businesses.',
    },
    icp: {
      company: {
        question: 'What kinds of businesses?',
        reasoning:
          'Founder-led businesses with traction that still run through the founder — agencies, trades, professional services, operator-owned firms. Not a NAICS list.',
        signals: [
          'founder-led',
          'founder led',
          'owner-operated',
          'owner operated',
          'founder',
          'agency',
          'trades',
          'professional services',
        ],
      },
      founder: {
        question: 'What stage are they in?',
        reasoning:
          'Still in the operating loop. Stage: "I do everything myself" and the cost of that is becoming visible.',
        signals: [
          'i do everything myself',
          'wearing too many hats',
          'founder',
          'owner',
          'cannot take a vacation',
          'still in delivery',
        ],
      },
      size: {
        question: 'Employees? Revenue?',
        reasoning:
          'Small enough that the founder is still in delivery/management; large enough that chaos is expensive.',
        known: false,
        unknowns: ['Exact headcount and revenue bands are not yet named by Fedir.'],
      },
      geography: {
        question: 'Where?',
        reasoning: 'Not a primary constraint until Fedir names a beachhead market.',
        known: false,
        unknowns: ['Beachhead geography is not yet named.'],
      },
      exclusions: {
        question: 'Who should we avoid?',
        reasoning:
          'Already systemized with managers; PE-backed / professionally governed; pre-revenue hobbies; non-founder-led enterprise; founders who have opted out of systemization.',
        signals: [
          'pe-backed',
          'private equity',
          'professionally governed',
          'already systemized',
          'enterprise hq',
          'lifestyle business',
          'pre-revenue hobby',
        ],
      },
    },
    transformation: {
      currentState: 'I do everything myself.',
      futureState: 'My business operates through systems and managers.',
    },
  });
  const painOntology = fedirPainOntology();
  const knowledge = fedirKnowledge();
  return {
    id: 'aim-fedir-v1',
    clientKey: FEDIR_CLIENT_KEY,
    clientName: 'Fedir',
    spec: 'SPEC-112',
    status: AIM_STATUS.COMPLETE,
    version: 1,
    isOperatingFact: false,
    ...market,
    painOntology,
    knowledge,
    knowledgeById: knowledgeMap(knowledge),
    createdAt: '2026-08-18T00:00:00.000Z',
  };
}

module.exports = {
  FEDIR_CLIENT_KEY,
  buildFedirAim,
  fedirPainOntology,
  fedirKnowledge,
};
