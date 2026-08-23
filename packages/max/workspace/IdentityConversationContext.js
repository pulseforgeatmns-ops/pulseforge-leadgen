'use strict';

/**
 * SPEC-149 — Identity Conversation routing.
 * Answers questions about Max itself — role, capabilities, boundaries, roster.
 * Does not receive Blueprint strategy or acquisition recommendations.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const { THINKING_MODES } = require('../operatorCognition/ThinkingModes');
const { createDefaultCapabilityRegistry } = require('../specialistDelegation/CapabilityRegistry');
const askPathTrace = require('./audit/AskPathTrace');

const MAX_ROLE =
  'I am Max, the acquisition mission manager for this workspace. I orchestrate planning, ' +
  'operator gates, and stage progression. I interpret specialist evidence — I do not invent it.';

const RESPONSIBILITY_BOUNDARIES = Object.freeze([
  'I explain mission state, recommend next steps, and route work to specialists.',
  'I do not send email, publish content, or mutate CRM state without explicit operator authorization.',
  'I do not substitute Blueprint facts for operating evidence or specialist attachments.',
  'Human approval still gates execution — nothing goes live from my recommendation alone.',
]);

const DELEGATION_RULES = Object.freeze([
  'Observe and recommend by default; draft only when a specialist supports it.',
  'Scout handles discovery and market intelligence; Paige drafts content; Emmett governs outbound send capacity.',
  'Execution commands bind to Mission Runtime — advisory turns stay read-only.',
  'When uncertain, I say so rather than fabricating business or pipeline facts.',
]);

const SPECIALIST_ROSTER = Object.freeze([
  { name: 'Scout', role: 'Discovery — sourcing, scoring, and attaching market evidence.' },
  { name: 'Paige', role: 'Content — drafts outreach and channel copy for operator approval.' },
  { name: 'Emmett', role: 'Outbound infrastructure — send capacity, inbox health, queue governance.' },
  { name: 'Riley', role: 'Inbound triage — classifies replies and deposits action cards.' },
  { name: 'Cal', role: 'Call coaching — discovery prep and role-play.' },
]);

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function operatingModeLabel(session) {
  const ctx = (session && session.context) || {};
  if (ctx.missionId || ctx.acquisitionMissionId) {
    return 'Active acquisition mission — mission runtime owns execution context.';
  }
  if (ctx.executionDomain) {
    return `Workspace mode (${ctx.executionDomain}).`;
  }
  return 'Workspace intelligence mode — read-only unless you issue an execution command.';
}

function capabilitySummary(registry) {
  return registry
    .listCallable()
    .map((entry) => `${entry.specialist}: ${entry.capability}`)
    .slice(0, 6);
}

function classifyIdentityQuestion(question) {
  const q = normalizeText(question).toLowerCase();
  if (/\b(?:who are you|tell me about yourself)\b/.test(q)) return 'introduction';
  if (/\b(?:capabilities|what can you do)\b/.test(q)) return 'capabilities';
  if (/\b(?:responsibilit|boundar)\b/.test(q)) return 'boundaries';
  if (/\b(?:specialist|scout|paige|emmett|riley|cal|roster|team)\b/.test(q)) {
    return 'roster';
  }
  if (/\b(?:delegat|route|routing)\b/.test(q)) return 'delegation';
  if (/\b(?:operating mode|current mode|what mode)\b/.test(q)) return 'operating_mode';
  return 'role';
}

function composeIdentityProse(question, session, registry) {
  const kind = classifyIdentityQuestion(question);
  const mode = operatingModeLabel(session);
  const callable = capabilitySummary(registry);

  switch (kind) {
    case 'introduction':
      return (
        `${MAX_ROLE} ${mode} ` +
        'Ask about my capabilities, the specialist roster, or how I delegate if you want more detail.'
      );
    case 'capabilities':
      return (
        `${MAX_ROLE} Callable capabilities in this workspace: ` +
        `${callable.length ? callable.join('; ') : 'none registered yet'}. ` +
        `${mode}`
      );
    case 'boundaries':
      return (
        `${MAX_ROLE} Responsibility boundaries: ${RESPONSIBILITY_BOUNDARIES.join(' ')} ${mode}`
      );
    case 'roster':
      return (
        'Specialist roster I coordinate with: ' +
        `${SPECIALIST_ROSTER.map((row) => `${row.name} — ${row.role}`).join(' ')} ` +
        `${mode}`
      );
    case 'delegation':
      return `${MAX_ROLE} Delegation rules: ${DELEGATION_RULES.join(' ')} ${mode}`;
    case 'operating_mode':
      return `${MAX_ROLE} ${mode}`;
    case 'role':
    default:
      return (
        `${MAX_ROLE} ${RESPONSIBILITY_BOUNDARIES[0]} ${RESPONSIBILITY_BOUNDARIES[1]} ${mode}`
      );
  }
}

function buildIdentityStructured(prose, conversationIntent, conversationSubject) {
  return buildStructuredResponse({
    answer: prose,
    reasoning: [
      'SPEC-149 — Identity subject routed before business intelligence.',
      'Response grounded in Max role, capability registry, and delegation boundaries only.',
    ],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: 0.96,
    nextInvestigations: [],
    recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
    confidenceContributors: ['spec_149', 'identity_conversation'],
    timelineReferences: [],
    relatedEntities: [],
    metadata: {
      sourcesUsed: {
        briefing: false,
        reasoning: true,
        memory: false,
        policy: true,
        knowledge: false,
      },
      evidenceCount: 0,
      asOf: new Date().toISOString(),
      unavailable: ['blueprint_strategy', 'acquisition_recommendations'],
      identityConversation: true,
      businessIntelligenceUsed: false,
      conversationSubject: conversationSubject && conversationSubject.subject,
      conversationIntent: conversationIntent && conversationIntent.intent,
      readOnlyCognition: true,
    },
  });
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleIdentityTurn(input = {}) {
  const conversationSubject = input.conversationSubject || null;
  if (!conversationSubject || conversationSubject.subject !== 'identity') {
    return null;
  }

  askPathTrace.traceEnter('maybeHandleIdentityTurn', {
    subject: conversationSubject.subject,
    reason: conversationSubject.reason,
  });

  const question = normalizeText(input.question);
  const session = input.session || null;
  const conversationIntent = input.conversationIntent || {
    intent: THINKING_MODES.EXPLAIN,
    thinkingMode: 'reasoning',
    confidence: 0.9,
  };
  const registry =
    input.capabilityRegistry || createDefaultCapabilityRegistry();

  const prose = composeIdentityProse(question, session, registry);
  const structured = buildIdentityStructured(
    prose,
    conversationIntent,
    conversationSubject
  );

  askPathTrace.traceEarlyReturn('maybeHandleIdentityTurn', 'identity_composed');

  return {
    handled: true,
    prose,
    structured,
    reason: 'identity_conversation',
    answered: { kind: 'identity', identityKind: classifyIdentityQuestion(question) },
  };
}

module.exports = {
  MAX_ROLE,
  RESPONSIBILITY_BOUNDARIES,
  DELEGATION_RULES,
  SPECIALIST_ROSTER,
  classifyIdentityQuestion,
  composeIdentityProse,
  maybeHandleIdentityTurn,
};
