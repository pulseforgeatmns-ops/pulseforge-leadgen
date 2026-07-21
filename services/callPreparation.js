'use strict';

// Phase A2 deterministic call preparation.
//
// Builds a prospect-specific CallPreparation object from VERIFIED data only:
// the canonical workspace read model plus vertical templates. Nothing here is
// AI-generated and no unsupported claim is ever fabricated — pain points are
// emitted as clearly labeled hypotheses, separate from verified facts. A later
// phase may add generationMode: 'ai'; the contract already carries the field.

const { ANCHOR_QUESTIONS, humanSetterPlaybook } = require('../utils/setterPlaybooks');
const { getProspectWorkspace } = require('./prospectWorkspace');

const PAIN_POINT_HYPOTHESES = Object.freeze({
  cleaning_company_overflow: [
    'May turn away jobs when their own crews are at capacity.',
    'May struggle to cover short-notice or weekend requests.',
  ],
  str_manager: [
    'Turnover windows between guests are likely tight and unforgiving.',
    'A single no-show cleaner can cost a booking or a review.',
  ],
  property_manager: [
    'Move-out turns and common-area cleaning may bottleneck on vendor availability.',
    'Vendor delays likely create tenant-facing pressure.',
  ],
  realtor: [
    'Pre-listing and move-in cleanings are often needed on short notice.',
    'May lack a reliable go-to vendor for last-minute requests.',
  ],
  restoration_remodeling_partner: [
    'Post-construction cleanup timing may slip against handover dates.',
    'Final-detail cleaning quality may vary between subcontractors.',
  ],
  commercial_office: [
    'Coverage gaps may appear when regular cleaners are absent.',
    'Special projects (floors, windows, deep cleans) may fall outside the current contract.',
  ],
  default: [
    'Current process may create friction the operator can surface with discovery questions.',
  ],
});

const PROOF_POINTS = Object.freeze({
  10: [
    'Local Manchester NH area operator — same-week walkthroughs.',
    'Built for backup and overflow coverage, not to replace an existing vendor.',
  ],
  default: [
    'Local team with a named human owner for every engagement.',
    'No obligation to switch vendors — start with backup or overflow coverage.',
  ],
});

function objectiveFor(stage, clientId) {
  if (stage === 'follow_up') return 'Complete the promised follow-up and agree on the next concrete step.';
  if (stage === 'contacted') return 'Reach the decision maker and qualify the need.';
  if (stage === 'booked') return 'Confirm the booked meeting or walkthrough details.';
  if (Number(clientId) === 10) return 'Confirm a real cleaning need and agree on a specific human follow-up.';
  return 'Understand the need, identify the decision maker, and agree on a specific next action.';
}

function whyNowFor(workspace) {
  const cb = workspace.callback || {};
  const reason = workspace.lifecycle?.lifecycleReason;
  const calling = workspace.calling || {};
  const overdue = cb.dueAt && new Date(cb.dueAt).getTime() < Date.now();
  if (cb.dueAt && overdue) {
    return `Callback overdue — they expected to hear from you (${new Date(cb.dueAt).toLocaleString()}).`;
  }
  if (cb.dueAt) {
    return `Callback promised for ${new Date(cb.dueAt).toLocaleString()}.`;
  }
  if (reason === 'data_remediation') {
    return 'Data remediation — this number was cleared; find a new contact method before calling again.';
  }
  if (reason === 'nurture') {
    return 'Nurture window — prior “not interested”; check whether timing has changed.';
  }
  if (workspace.prospect?.priority?.reason) return workspace.prospect.priority.reason;
  if (calling.lastDispositionLabel) {
    return `Prior outcome: ${calling.lastDispositionLabel}${calling.lastAttemptAt ? ` on ${new Date(calling.lastAttemptAt).toLocaleDateString()}` : ''}.`;
  }
  if (!calling.attempts) return 'New lead — first touch while the opportunity is fresh.';
  return 'Ready for the next attempt.';
}

function openerFor(workspace, playbook) {
  const { prospect } = workspace;
  const template = String(playbook.opener || '');
  const contact = prospect.contactName || 'there';
  // Vertical template + verified facts only. The [operator] placeholder stays
  // literal for the caller to substitute; no invented names.
  let opener = template.replace('[name]', contact);
  const previous = workspace.lastInteraction;
  if (previous && previous.type !== 'lifecycle_transition') {
    opener += ` (Reference: last touch was ${previous.summary} on ${new Date(previous.occurredAt).toLocaleDateString()}.)`;
  }
  return opener;
}

function desiredOutcomeFor(stage) {
  if (stage === 'booked') return 'Meeting confirmed with date, time, and attendee.';
  return 'Meeting or walkthrough booked with a specific date and time.';
}

function fallbackOutcomeFor(stage) {
  if (stage === 'dead') return 'Confirm the disqualification reason is still accurate.';
  return 'A dated callback with an agreed topic, or a clean disqualification with a recorded reason.';
}

async function getCallPreparation({ pool, clientId, prospectId, user = {}, clientName = 'the client' } = {}) {
  const workspace = await getProspectWorkspace({ pool, clientId, prospectId, user });
  if (!workspace) return null;

  const vertical = workspace.prospect.vertical || 'general';
  const playbook = humanSetterPlaybook({ clientId, clientName, vertical });
  const stage = workspace.lifecycle.canonicalStage;

  const verifiedFacts = workspace.knownFacts.map(fact => ({
    text: `${fact.label}: ${fact.value}`,
    sourceType: fact.sourceType,
    sourceId: fact.sourceId,
  }));

  const hypotheses = (PAIN_POINT_HYPOTHESES[vertical] || PAIN_POINT_HYPOTHESES.default)
    .map(text => ({ text, clearlyLabeledHypothesis: true }));

  const discoveryQuestions = Number(clientId) === 10 && ANCHOR_QUESTIONS[vertical]
    ? [...ANCHOR_QUESTIONS[vertical]]
    : [...playbook.qualification_questions];

  return {
    prospectId: workspace.prospect.id,
    objective: objectiveFor(stage, clientId),
    whyNow: whyNowFor(workspace),
    reasonSelected: workspace.prospect.priority.reason || whyNowFor(workspace),
    opener: openerFor(workspace, playbook),
    verifiedFacts,
    discoveryQuestions,
    painPointHypotheses: hypotheses,
    proofPoints: [...(PROOF_POINTS[Number(clientId)] || PROOF_POINTS.default)],
    objections: playbook.objection_prompts.map(item => ({
      objection: item.objection,
      response: item.response,
    })),
    previousInteraction: workspace.lastInteraction,
    nextAction: workspace.nextAction,
    phone: workspace.prospect.phone,
    desiredOutcome: desiredOutcomeFor(stage),
    fallbackOutcome: fallbackOutcomeFor(stage),
    safety: playbook.safety,
    generatedAt: new Date().toISOString(),
    generationMode: 'deterministic',
  };
}

module.exports = { PAIN_POINT_HYPOTHESES, getCallPreparation };
