'use strict';

/**
 * SPEC-147 — Conversational Intelligence Layer.
 * Mission engine owns truth; this layer owns how Max communicates it.
 *
 * Internal flow:
 *   Operator intent → Mission context → Specialist knowledge → Reasoning → Conversation → Response
 */

const { THINKING_MODES } = require('../operatorCognition/ThinkingModes');
const { looksLikeReasoningRequest } = require('./MissionCommunication');
const {
  ensureConversationMemory,
  deriveTopicKey,
  recordExplanation,
  inferRelationshipMode,
  repetitionPreface,
} = require('./ConversationMemory');
const { buildMissionContext } = require('../../acquisition-mission/Inspection');

const SPECIALIST_KNOWLEDGE = Object.freeze({
  scout:
    'Scout runs discovery — sourcing companies from Google Places and enrichment APIs, scoring them against the mission ICP, and attaching evidence before anything reaches prioritization or outreach.',
  max:
    'I orchestrate the mission lifecycle: planning, prioritization, operator gates, and stage progression. I do not invent evidence — I interpret what specialists attach.',
  paige:
    'Paige drafts campaign variants and channel content. Nothing publishes without operator approval.',
  emmett:
    'Emmett owns outbound infrastructure — send capacity, inbox health, and queue governance. He does not write copy.',
});

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function specialistFromQuestion(question) {
  const q = normalizeText(question).toLowerCase();
  if (/\bpaige\b/.test(q)) return 'paige';
  if (/\bemmett\b/.test(q)) return 'emmett';
  if (/\bscout\b/.test(q)) return 'scout';
  return 'scout';
}

function countDiscoveryCompanies(snapshot) {
  const contributions = snapshot.contributions || [];
  const scout = [...contributions].reverse().find((row) => row.specialist === 'scout');
  const payload = scout && scout.payload ? scout.payload : {};
  const companies = Array.isArray(payload.companies) ? payload.companies : [];
  const prospects = Array.isArray(payload.prospects) ? payload.prospects : [];
  return Math.max(companies.length, prospects.length);
}

function scoutConfidence(snapshot) {
  const contributions = snapshot.contributions || [];
  const scout = [...contributions].reverse().find((row) => row.specialist === 'scout');
  const payload = scout && scout.payload ? scout.payload : {};
  if (payload.confidence != null) return Number(payload.confidence);
  const breakdown = payload.confidenceBreakdown;
  if (breakdown && breakdown.overall != null) return Number(breakdown.overall);
  return null;
}

function extractMissionFacts(snapshot = {}, answered = {}) {
  const mission = snapshot.mission || answered.mission || {};
  const workspace = snapshot.workspace || {};
  const scout = workspace.scout || {};
  const missionContext = answered.missionContext || buildMissionContext(snapshot);
  const pending = mission.pendingOperatorDecision || null;
  const blocker = snapshot.blocker || null;
  const discoveryCount = countDiscoveryCompanies(snapshot);
  const confidence = scoutConfidence(snapshot);

  return {
    mission,
    workspace,
    scout,
    missionContext,
    pending,
    blocker,
    discoveryCount,
    scoutConfidence: confidence,
    stageLabel:
      missionContext.stageLabel ||
      mission.stage ||
      mission.status ||
      'active',
    objective: mission.objective || null,
    targetSegment: mission.targetSegment || null,
    inspectionSummary:
      (answered.structured && answered.structured.summary) ||
      (answered.inspection && answered.prose) ||
      answered.prose ||
      null,
    inspectionProperty:
      (answered.inspection && answered.inspection.property) ||
      null,
    kind: answered.kind || null,
  };
}

function buildInternalReasoning(facts, intent) {
  const lines = [];
  if (facts.pending && facts.pending.prompt) {
    lines.push(`Mission is gated on operator input: ${facts.pending.prompt}`);
  }
  if (facts.blocker && facts.blocker.reason) {
    lines.push(facts.blocker.reason);
  }
  if (facts.discoveryCount === 0 && facts.scout && facts.scout.state === 'complete') {
    lines.push('Discovery completed with zero companies above the evidence threshold.');
  } else if (facts.discoveryCount > 0) {
    lines.push(`Discovery surfaced ${facts.discoveryCount} candidate(s).`);
  }
  if (facts.scoutConfidence != null && facts.scoutConfidence < 0.55) {
    lines.push(`Scout confidence is low (${facts.scoutConfidence.toFixed(2)}).`);
  }
  if (intent === THINKING_MODES.CHALLENGE && facts.discoveryCount === 0) {
    lines.push('Operator may be challenging prioritization when coverage is the constraint.');
  }
  return lines.filter(Boolean);
}

function composeExplainLead(facts, reasoning, opts = {}) {
  const specialist = opts.specialist || 'Scout';
  if (facts.pending && facts.pending.prompt) {
    return `${specialist} finished its pass, but the mission is waiting on you — ${facts.pending.prompt}`;
  }
  if (facts.discoveryCount === 0 && facts.scout.state === 'complete') {
    return `${specialist} finished discovery successfully, but nothing met our evidence threshold on this pass.`;
  }
  if (facts.inspectionSummary) {
    return normalizeText(facts.inspectionSummary);
  }
  if (reasoning.length) {
    return reasoning[0];
  }
  return `We're in ${facts.stageLabel}. I can walk through any part of the mission state if helpful.`;
}

function composeMarketCoverageJudgment(facts) {
  if (facts.discoveryCount > 0) return null;
  if (facts.scout.state !== 'complete') return null;
  return (
    "Right now I don't think the problem is prioritization — it's market coverage. " +
    "We need a wider discovery pass or a lower evidence bar before ranking anything."
  );
}

function composeSelfReflection(facts, intent) {
  if (intent !== THINKING_MODES.CHALLENGE && intent !== THINKING_MODES.STRATEGY) {
    return null;
  }
  if (facts.scoutConfidence != null && facts.scoutConfidence < 0.55 && facts.discoveryCount > 0) {
    return (
      "Scout's confidence reads low on paper, but we do have candidates on file. " +
      "I'd validate quality before dismissing the pass — and I'd still pressure-test coverage."
    );
  }
  if (facts.scoutConfidence != null && facts.scoutConfidence < 0.55) {
    return "Scout's confidence is low. I'd investigate coverage before changing strategy.";
  }
  return null;
}

function composeStrategyLead(facts, reasoning) {
  const coverage = composeMarketCoverageJudgment(facts);
  if (coverage) return coverage;
  if (facts.pending) {
    return `Before we change strategy — we're still waiting on ${facts.pending.kind || 'operator input'}. I'd resolve that gate first.`;
  }
  if (reasoning.length) {
    return `The biggest constraint I see: ${reasoning[0]}`;
  }
  return `We're in ${facts.stageLabel}. Strategy depends on whether discovery gave us enough signal — ask me to inspect evidence if you want detail.`;
}

function composeTeachLead(question) {
  const specialist = specialistFromQuestion(question);
  const knowledge = SPECIALIST_KNOWLEDGE[specialist] || SPECIALIST_KNOWLEDGE.scout;
  const name = specialist.charAt(0).toUpperCase() + specialist.slice(1);
  return `${name}: ${knowledge}`;
}

function composeBrainstormLead(facts) {
  const ideas = [];
  if (facts.discoveryCount === 0) {
    ideas.push('widen geography one ring');
    ideas.push('run a second vertical pass');
    ideas.push('lower the evidence threshold for a pilot batch');
  } else {
    ideas.push('approve prioritization and inspect the ranked list');
    ideas.push('challenge individual scores before advancing');
    ideas.push('hold discovery and teach me how Scout scored the pass');
  }
  return `A few directions worth considering: ${ideas.join('; ')}.`;
}

function composeCompareLead(question, facts) {
  const q = normalizeText(question);
  if (/\bvs\.?\b|\bversus\b|\bcompare\b/i.test(q)) {
    return 'Comparison needs named entities from mission evidence — tell me which companies or segments to contrast and I will stay grounded in what Scout attached.';
  }
  return `I can compare options once you name them. Current mission stage: ${facts.stageLabel}.`;
}

function composeInspectLead(facts) {
  const pending = facts.pending;
  if (pending && pending.prompt) {
    return `We're in ${facts.stageLabel}, waiting on you — ${pending.prompt}`;
  }
  if (facts.discoveryCount === 0 && facts.scout.state === 'complete') {
    return `We're in ${facts.stageLabel}. Scout completed discovery but found no companies above threshold.`;
  }
  return `We're in ${facts.stageLabel}${facts.objective ? `, working toward: ${facts.objective}` : ''}.`;
}

function composeContinuationAmbiguityLead(facts, ambiguity = {}) {
  const options = Array.isArray(ambiguity.options) && ambiguity.options.length
    ? ambiguity.options
    : (Array.isArray(ambiguity.eligible) ? ambiguity.eligible.map((row) => row.label) : []);
  if (options.length) {
    return `I can continue this mission, but more than one branch is available. Which should I take?\n\n${options.map((row) => `• ${row}`).join('\n')}`;
  }
  if (ambiguity.pause && ambiguity.pause.requiredDecision) {
    return `Continue needs your judgment on this mission — ${ambiguity.pause.requiredDecision}`;
  }
  return composeInspectLead(facts);
}

function composeChallengeLead(facts, reasoning) {
  const reflection = composeSelfReflection(facts, THINKING_MODES.CHALLENGE);
  if (reflection) return reflection;
  if (reasoning.length) {
    return `Fair challenge. From mission state: ${reasoning[0]}`;
  }
  return 'Tell me what you disagree with and I will stay grounded in mission evidence.';
}

function composeLead(intent, facts, reasoning, question) {
  switch (intent) {
    case THINKING_MODES.EXPLAIN:
      return composeExplainLead(facts, reasoning, { specialist: 'Scout' });
    case THINKING_MODES.CHALLENGE:
      return composeChallengeLead(facts, reasoning);
    case THINKING_MODES.STRATEGY:
      return composeStrategyLead(facts, reasoning);
    case THINKING_MODES.BRAINSTORM:
      return composeBrainstormLead(facts);
    case THINKING_MODES.TEACH:
      return composeTeachLead(question);
    case THINKING_MODES.COMPARE:
      return composeCompareLead(question, facts);
    case THINKING_MODES.INSPECT:
    default:
      return composeInspectLead(facts);
  }
}

function composeExpansion(intent, facts, reasoning, explicitReasoning) {
  if (!explicitReasoning && intent !== THINKING_MODES.EXPLAIN && intent !== THINKING_MODES.STRATEGY) {
    return '';
  }
  const parts = [];
  if (intent === THINKING_MODES.EXPLAIN || intent === THINKING_MODES.STRATEGY) {
    const coverage = composeMarketCoverageJudgment(facts);
    if (coverage && !parts.includes(coverage)) {
      parts.push(`Here's why: ${coverage}`);
    }
  }
  if (reasoning.length > 1) {
    parts.push(`Here's why: ${reasoning.slice(1).join(' ')}`);
  } else if (reasoning.length === 1 && explicitReasoning) {
    parts.push(`Here's why: ${reasoning[0]}`);
  }
  const reflection = composeSelfReflection(facts, intent);
  if (reflection && explicitReasoning) {
    parts.push(reflection);
  }
  return parts.join('\n\n');
}

function composeExecuteHint(intent) {
  if (intent === THINKING_MODES.EXECUTE) return null;
  return null;
}

/**
 * Compose a natural-language mission response without status-card formatting.
 * @param {object} input
 * @returns {{ prose: string, topicKey: string, relationshipMode: string, reasoning: string[] }}
 */
function composeConversationalResponse(input = {}) {
  const question = normalizeText(input.question);
  const conversationIntent = input.conversationIntent || {};
  const intent = conversationIntent.intent || THINKING_MODES.INSPECT;
  const snapshot = input.snapshot || {};
  const answered = input.answered || {};
  const session = input.session || null;
  const explicitReasoning =
    input.explicitReasoning === true || looksLikeReasoningRequest(question);

  const memory = ensureConversationMemory(session);
  const facts = extractMissionFacts(snapshot, answered);
  const reasoning = buildInternalReasoning(facts, intent);
  const topicKey = deriveTopicKey({
    intent,
    inspectionProperty: facts.inspectionProperty,
    stage: facts.mission.stage,
    specialist: specialistFromQuestion(question),
  });

  const preface = repetitionPreface(memory, topicKey);
  const lead =
    conversationIntent.via === 'mission_continuation_ambiguous'
      ? composeContinuationAmbiguityLead(
        facts,
        conversationIntent.missionContinuationAmbiguity || {}
      )
      : composeLead(intent, facts, reasoning, question);
  const expansion = composeExpansion(intent, facts, reasoning, explicitReasoning);

  const paragraphs = [];
  if (preface) paragraphs.push(preface.trim());
  paragraphs.push(lead);
  if (expansion) paragraphs.push(expansion);

  const prose = paragraphs.join('\n\n').trim();
  const relationshipMode = inferRelationshipMode(conversationIntent, memory);

  if (session) {
    recordExplanation(session, {
      topicKey,
      summary: lead.slice(0, 200),
      intent,
      relationshipMode,
    });
  }

  return {
    prose,
    topicKey,
    relationshipMode,
    reasoning,
    explicitReasoning,
  };
}

function applyConversationalPresentation(structured, conversational) {
  return {
    ...structured,
    answer: conversational.prose,
    reasoning: [],
    metadata: {
      ...(structured.metadata || {}),
      conversationalIntelligence: true,
      spec: 'SPEC-147',
      relationshipMode: conversational.relationshipMode,
      topicKey: conversational.topicKey,
      missionCommunication: false,
      strictOutputShape: true,
      showReasoningDisclosure: conversational.explicitReasoning,
    },
  };
}

module.exports = {
  SPECIALIST_KNOWLEDGE,
  extractMissionFacts,
  buildInternalReasoning,
  composeConversationalResponse,
  applyConversationalPresentation,
};
