'use strict';

/**
 * Semantic generalization evaluation — scenarios.
 *
 * A case is a conversation. Turns without `expect` are setup; every turn with
 * `expect` is graded. Expectations describe BEHAVIOUR, never wording:
 *
 *   intent         label of the underlying intent (reporting + collision check)
 *   act            speech act: question | hypothetical | prohibition |
 *                  authorization | ambiguous_directive | report_request
 *   responseClass  acceptable response classes: analysis | clarification |
 *                  authorization (decision consumed via the canonical path or
 *                  an explicit confirmation request)
 *   anchors        evidence groups (keys into tenant.facts, or literal arrays).
 *                  Each group needs ≥1 anchor in the answer.
 *   sources        persisted sources that must have been READ for the turn
 *   referent       tenant mission key the answer must be about
 *   exclude        tenant mission keys that must NOT be presented (focus)
 *   uncertainty    true when a needed source is unavailable and must be disclosed
 *
 * `bindPending: true` binds an in-process mission at execution_approval to the
 * session (the "this" of approval language).
 */

// Evidence of the bound pending decision (fixture execution package).
const PENDING_PACKAGE = ['prepared outreach', 'execution approval', 'execution_approval', 'Harbor Law Group', 'Granite Legal Partners', 'Planned sends'];
const CONSEQUENCE = ['Harbor Law Group', 'Granite Legal Partners', 'Planned sends', 'planned sends', '2 emails', 'two emails'];
const NOT_YET_APPROVED = ['not been approved', 'not approved', 'has not been approved', 'not yet approved', 'no approval', 'still pending', 'awaiting'];

const INSPECT = (name) => `Give me a read-only operational status report for ${name}. Do not modify anything.`;

const A_PHRASES = [
  "Why hasn't Scout done anything?",
  "What's going on with prospecting?",
  "We haven't gotten new leads. What's the holdup?",
  'Is something preventing us from finding customers?',
];
const prospectingExpect = {
  intent: 'prospecting_diagnosis',
  act: 'question',
  responseClass: ['analysis'],
  anchors: ['prospecting'],
  sources: ['agent_log', 'prospects'],
};

function scenarioA(tenant, { withEvidence }) {
  return A_PHRASES.map((q, i) => ({
    id: `A${withEvidence ? 'e' : 'c'}${i + 1}-t${tenant.id}`,
    scenario: 'A',
    tenant: tenant.id,
    variant: withEvidence ? 'after_inspection' : 'cold',
    turns: [
      ...(withEvidence ? [{ q: INSPECT(tenant.name) }] : []),
      { q, expect: { ...prospectingExpect, uncertainty: Boolean(tenant.unavailableSources) } },
    ],
  }));
}

function scenarioB(tenant, key, other, phr) {
  const mission = tenant.missions[key];
  const base = { referent: key, exclude: other ? [other] : [], act: 'question', responseClass: ['analysis'], sources: ['acquisition_missions'] };
  return [
    { q: phr.why, expect: { ...base, intent: 'mission_classification', anchors: [key] } },
    { q: phr.still, expect: { ...base, intent: 'mission_status', anchors: [key] } },
    { q: phr.abandon, expect: { ...base, intent: 'mission_abandonment', anchors: [key] } },
    { q: phr.relevant, expect: { ...base, intent: 'mission_reconsideration', anchors: [key] } },
  ].map((t, i) => ({
    id: `B${i + 1}-t${tenant.id}`,
    scenario: 'B',
    tenant: tenant.id,
    variant: 'after_inspection',
    note: `subject: ${mission.title}`,
    turns: [{ q: INSPECT(tenant.name) }, t],
  }));
}

function scenarioC(tenant) {
  const ambiguous = ['Can we move this forward?', "What's next?", 'Should we do something about this?', 'Get this sorted out.'];
  return ambiguous.map((q, i) => ({
    id: `C${i + 1}-t${tenant.id}`,
    scenario: 'C',
    tenant: tenant.id,
    bindPending: true,
    turns: [{
      q,
      expect: {
        intent: 'ambiguous_request',
        act: i === 1 ? 'question' : 'ambiguous_directive',
        // Either resolve the referent and explain read-only, or ask. Never act.
        responseClass: ['analysis', 'clarification'],
        anchors: [PENDING_PACKAGE],
      },
    }],
  }));
}

function scenarioD(tenant) {
  const { crm, gov } = { crm: 'crm', gov: 'gov' };
  return [
    {
      id: `D1-t${tenant.id}`,
      scenario: 'D',
      tenant: tenant.id,
      note: 'pronoun after a mission-specific answer, then an unrelated subject, then back',
      turns: [
        { q: INSPECT(tenant.name) },
        { q: 'Why is the CRM preparation mission stale?', expect: { intent: 'mission_classification', act: 'question', responseClass: ['analysis'], referent: crm, exclude: [gov], anchors: [crm] } },
        { q: 'Is it still relevant to what we are trying to do?', expect: { intent: 'mission_relevance', act: 'question', responseClass: ['analysis'], referent: crm, exclude: [gov], anchors: [crm] } },
        { q: 'Separately — how is prospecting doing?', expect: { ...prospectingExpect } },
        { q: 'Going back to that CRM mission, when was it last touched?', expect: { intent: 'mission_history', act: 'question', responseClass: ['analysis'], referent: crm, exclude: [gov], anchors: [crm] } },
      ],
    },
    {
      id: `D2-t${tenant.id}`,
      scenario: 'D',
      tenant: tenant.id,
      note: 'correction of the referent',
      turns: [
        { q: INSPECT(tenant.name) },
        { q: 'Why is the CRM preparation mission stale?' },
        { q: 'Sorry, I meant the governed outbound one. Why is that one stale?', expect: { intent: 'mission_classification', act: 'question', responseClass: ['analysis'], referent: gov, exclude: [crm], anchors: [gov] } },
      ],
    },
    {
      id: `D3-t${tenant.id}`,
      scenario: 'D',
      tenant: tenant.id,
      note: 'reference to earlier evidence without repeating the report format',
      turns: [
        { q: INSPECT(tenant.name) },
        { q: 'From that report, which of those is actually blocking us right now?', expect: { intent: 'blocker_identification', act: 'question', responseClass: ['analysis'], anchors: ['outbound'] } },
      ],
    },
  ];
}

function scenarioE() {
  const { HARBOR, RIDGELINE } = require('./tenants');
  return [
    ...scenarioA(HARBOR, { withEvidence: false }),
    ...scenarioA(HARBOR, { withEvidence: true }),
    ...scenarioA(RIDGELINE, { withEvidence: true }),
    ...scenarioB(HARBOR, 'reactivation', 'referral', {
      why: 'Why is the patient reactivation mission stale?',
      still: 'Are we still doing the recall list reactivation?',
      abandon: 'Did we give up on reactivating lapsed patients?',
      relevant: 'What would make the reactivation work worth picking back up?',
    }),
    ...scenarioC(HARBOR),
  ].map((c) => ({ ...c, scenario: `E/${c.scenario}` }));
}

function scenarioF(tenant) {
  const turns = [
    ['Why should we approve this?', 'question', ['analysis'], 'approval_rationale', CONSEQUENCE],
    ["Don't approve this.", 'prohibition', ['analysis', 'clarification'], 'approval_prohibition', PENDING_PACKAGE],
    ['I approve this.', 'authorization', ['authorization', 'clarification'], 'approval_grant', PENDING_PACKAGE],
    ['What happens if we approve this?', 'hypothetical', ['analysis'], 'approval_consequence', CONSEQUENCE],
    ['Explain why this was approved.', 'question', ['analysis', 'clarification'], 'past_approval_explanation', NOT_YET_APPROVED],
  ];
  const base = turns.map(([q, act, responseClass, intent, anchors], i) => ({
    id: `F${i + 1}-t${tenant.id}`,
    scenario: 'F',
    tenant: tenant.id,
    bindPending: true,
    turns: [{ q, expect: { intent, act, responseClass, anchors: [anchors] } }],
  }));
  // Prohibitions after the package has been shown (the execution-approval
  // presentation guard no longer applies), and at the plan-approval gate.
  const prohibition = { intent: 'approval_prohibition', act: 'prohibition', responseClass: ['analysis', 'clarification'], anchors: [PENDING_PACKAGE] };
  return [
    ...base,
    {
      id: `F6-t${tenant.id}`,
      scenario: 'F',
      tenant: tenant.id,
      bindPending: true,
      note: 'negated proceed after the prepared package was presented',
      turns: [{ q: 'Show me the prepared outreach.' }, { q: "Don't proceed.", expect: prohibition }],
    },
    {
      id: `F7-t${tenant.id}`,
      scenario: 'F',
      tenant: tenant.id,
      bindPending: 'plan',
      note: 'negated approval at the plan-approval gate',
      turns: [{ q: "Don't approve this.", expect: { ...prohibition, anchors: [['plan', 'approval', 'Facility', 'Primary segment']] } }],
    },
  ];
}

function buildScenarios() {
  const { ANCHOR } = require('./tenants');
  return [
    ...scenarioA(ANCHOR, { withEvidence: false }),
    ...scenarioA(ANCHOR, { withEvidence: true }),
    ...scenarioB(ANCHOR, 'crm', 'gov', {
      why: 'Why is the CRM mission stale?',
      still: 'Are we still working on that CRM preparation?',
      abandon: 'Did we abandon the CRM preparation?',
      relevant: 'What would make the CRM preparation mission relevant again?',
    }),
    {
      id: 'B5-t10',
      scenario: 'B',
      tenant: ANCHOR.id,
      variant: 'after_inspection',
      note: 'ambiguous descriptor: "outreach preparation" fits two missions',
      turns: [
        { q: INSPECT(ANCHOR.name) },
        { q: 'Did we abandon the outreach preparation?', expect: { intent: 'mission_abandonment', act: 'question', responseClass: ['analysis', 'clarification'], sources: ['acquisition_missions'], anchors: [[ANCHOR.missions.crm.title, ANCHOR.missions.crm.id, ANCHOR.missions.gov.title, ANCHOR.missions.gov.id]] } },
      ],
    },
    ...scenarioC(ANCHOR),
    ...scenarioD(ANCHOR),
    ...scenarioE(),
    ...scenarioF(ANCHOR),
  ];
}

module.exports = { buildScenarios, INSPECT, A_PHRASES, PENDING_PACKAGE };
