'use strict';

/**
 * SPEC-148 — Reflective cognition response composition.
 * Answers questions about Max itself — intent, pipeline, assumptions, and prior turns.
 * Must not retrieve Blueprint, Client Intelligence, or business recommendations.
 */

const { THINKING_MODES } = require('../operatorCognition/ThinkingModes');

const PIPELINE_LABELS = Object.freeze({
  active_mission: 'Active Mission pipeline',
  mission_creation: 'Mission Creation pipeline',
  objective_persistence: 'Objective Persistence pipeline',
  blueprint: 'Blueprint / Client Intelligence pipeline',
  mission_inspection: 'Mission Inspection pipeline',
  specialist_interrogation: 'Specialist Interrogation pipeline',
  specialist_scout: 'Scout specialist pipeline',
  specialist_paige: 'Paige specialist pipeline',
  specialist_cal: 'Cal specialist pipeline',
  specialist_direction: 'Specialist Direction pipeline',
  knowledge_retrieval: 'Knowledge Retrieval pipeline',
  reasoning: 'General Reasoning pipeline',
  reflection: 'Reflection pipeline',
});

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function pipelineLabel(owner) {
  if (!owner) return 'an undetermined pipeline';
  return PIPELINE_LABELS[owner] || `${owner} pipeline`;
}

function describePriorIntent(priorIntent) {
  if (!priorIntent || !priorIntent.intent) return 'an unclassified intent';
  const via = priorIntent.via ? ` (${priorIntent.via})` : '';
  return `${priorIntent.intent}${via}`;
}

function classifyReflectQuestion(question) {
  const q = normalizeText(question).toLowerCase();
  if (/\bwhat did you think i (?:was )?(?:ask(?:ing|ed)|mean(?:t|ing)?)\b/.test(q)) {
    return 'intent_misinterpretation';
  }
  if (/\bwhy did you answer (?:like )?that\b/.test(q)) {
    return 'answer_style';
  }
  if (/\bwalk me through\b|\bwhat (?:was|is) your reasoning\b/.test(q)) {
    return 'reasoning_walkthrough';
  }
  if (/\bwhat assumptions\b/.test(q)) {
    return 'assumptions';
  }
  if (/\bdid you misunderstand me\b/.test(q)) {
    return 'misunderstanding';
  }
  if (/\bwhy didn'?t scout (?:run|execute)\b/.test(q)) {
    return 'scout_not_executed';
  }
  if (/\bwhy did scout (?:run|execute)\b/.test(q)) {
    return 'scout_executed';
  }
  if (/\bwhat pipeline\b/.test(q)) {
    return 'pipeline_selection';
  }
  if (/\bwhy did you recommend\b/.test(q)) {
    return 'recommendation_rationale';
  }
  if (/\bwould you answer differently\b/.test(q)) {
    return 'alternatives';
  }
  if (/\bwhere are you uncertain\b/.test(q)) {
    return 'uncertainty';
  }
  if (/\bwhat else could you have\b/.test(q)) {
    return 'alternatives';
  }
  if (/\bwhat would you improve\b/.test(q)) {
    return 'self_critique';
  }
  if (/\bwhy are you waiting\b/.test(q)) {
    return 'waiting';
  }
  if (/\bwhat were you trying to (?:accomplish|do)\b/.test(q)) {
    return 'accomplishment';
  }
  return 'general_reflection';
}

function composeIntentMisinterpretation(ctx) {
  const priorQ = ctx.previousOperatorMessage;
  const priorIntent = ctx.intentClassification.previous;
  const priorPipeline = ctx.selectedPipeline.previous;

  if (!priorQ) {
    return (
      "I don't have a preceding operator question in this session yet, " +
      'so I cannot reconstruct what I thought you were asking.'
    );
  }

  const intentDesc = describePriorIntent(priorIntent);
  const pipelineDesc = pipelineLabel(priorPipeline);
  let prose =
    `On your previous message — "${priorQ}" — I classified the turn as ${intentDesc} ` +
    `and routed it through the ${pipelineDesc}.`;

  if (priorIntent && priorIntent.intent === 'explain') {
    prose +=
      ' Looking back, I may have treated that as a business or mission explanation request ' +
      'when you may have wanted something else from me.';
  } else if (priorIntent && priorIntent.intent === 'strategy') {
    prose +=
      ' I interpreted that as a strategic business question and answered with a recommendation ' +
      'instead of clarifying what you actually wanted.';
  } else {
    prose += ' That classification drove how I framed my answer.';
  }

  return prose;
}

function composeAnswerStyle(ctx) {
  const priorIntent = ctx.intentClassification.previous;
  const priorPipeline = ctx.selectedPipeline.previous;
  const priorResponse = ctx.previousAssistantResponse;

  if (!priorResponse) {
    return (
      'I do not have a prior assistant response in this session to critique. ' +
      'Once we have an exchange, I can explain why I answered the way I did.'
    );
  }

  return (
    `I answered that way because I classified your prior turn as ${describePriorIntent(priorIntent)} ` +
    `and let the ${pipelineLabel(priorPipeline)} produce the response. ` +
    `My reply leaned on that pipeline's framing rather than stepping back to ask what you meant. ` +
    `In hindsight, I should have checked whether you wanted business advice or a meta explanation first.`
  );
}

function composeReasoningWalkthrough(ctx) {
  const priorIntent = ctx.intentClassification.previous;
  const priorPipeline = ctx.selectedPipeline.previous;
  const priorQ = ctx.previousOperatorMessage;

  const steps = [];
  if (priorQ) steps.push(`1. You asked: "${priorQ}".`);
  steps.push(`2. I classified intent as ${describePriorIntent(priorIntent)}.`);
  steps.push(`3. Ownership resolved to ${pipelineLabel(priorPipeline)}.`);
  steps.push('4. That pipeline selected the evidence and response shape I used.');
  steps.push('5. I did not re-check whether the subject was about the business or about my own reasoning.');

  return steps.join(' ');
}

function composeAssumptions(ctx) {
  const priorIntent = ctx.intentClassification.previous;
  const assumptions = [
    'I assumed your question was about the business or mission unless you explicitly asked about my reasoning.',
  ];

  if (priorIntent && priorIntent.intent) {
    assumptions.push(`I assumed ${priorIntent.intent} was the right thinking mode for your prior turn.`);
  }
  if (ctx.selectedPipeline.previous) {
    assumptions.push(
      `I assumed the ${pipelineLabel(ctx.selectedPipeline.previous)} was the correct owner for that turn.`
    );
  }
  assumptions.push('I assumed you wanted a direct answer rather than a clarification question from me.');

  return `The assumptions I made: ${assumptions.join(' ')}`;
}

function composeMisunderstanding(ctx) {
  const priorQ = ctx.previousOperatorMessage;
  if (!priorQ) {
    return 'I may have misunderstood, but I need a prior exchange in this session to say how.';
  }
  return (
    `Yes — I may have misunderstood you. On "${priorQ}", I treated the turn as ${describePriorIntent(ctx.intentClassification.previous)} ` +
    `and answered through the ${pipelineLabel(ctx.selectedPipeline.previous)}. ` +
    'If that was not what you wanted, I should have asked a follow-up before answering.'
  );
}

function composeScoutPipeline(ctx, executed) {
  const priorPipeline = ctx.selectedPipeline.previous;
  const priorIntent = ctx.intentClassification.previous;

  if (executed) {
    return (
      `Scout ran because the ${pipelineLabel(priorPipeline)} claimed that turn — ` +
      `intent was ${describePriorIntent(priorIntent)}. ` +
      'Scout executes only when the active pipeline delegates discovery or when an approved mission stage calls for it.'
    );
  }

  return (
    `Scout did not execute on the prior turn. Intent was ${describePriorIntent(priorIntent)} ` +
    `and ownership was ${pipelineLabel(priorPipeline)}. ` +
    'Scout only runs on explicit discovery commands, approved mission stages, or specialist-scout ownership — ' +
    'not on read-only cognition or reflection turns.'
  );
}

function composePipelineSelection(ctx) {
  const priorPipeline = ctx.selectedPipeline.previous;
  const priorReason = ctx.selectedPipeline.previousReason;
  const reasonSuffix = priorReason ? ` (${priorReason})` : '';
  return (
    `The ${pipelineLabel(priorPipeline)}${reasonSuffix} answered your previous turn. ` +
    `Intent classification was ${describePriorIntent(ctx.intentClassification.previous)}.`
  );
}

function composeRecommendationRationale(ctx) {
  const priorResponse = ctx.previousAssistantResponse;
  if (!priorResponse) {
    return 'I do not have a prior recommendation in this session to explain.';
  }
  return (
    'I recommended that because the prior turn routed through business or mission reasoning, ' +
    'and I surfaced the highest-confidence option from available evidence. ' +
    'I assumed you wanted a actionable recommendation rather than a reflection on my choice.'
  );
}

function composeAlternatives(ctx) {
  return (
    'Yes — I would answer differently now. I would first confirm whether you are asking about the business ' +
    'or about my own reasoning, and I would reference our immediately preceding exchange before consulting ' +
    'Blueprint, Client Intelligence, or mission evidence.'
  );
}

function composeSelfCritique() {
  return (
    'What I would improve: classify meta-cognitive questions as reflection before any business pipeline runs; ' +
    'cite the previous turn explicitly; and admit uncertainty instead of defaulting to a generic recommendation.'
  );
}

function composeWaiting(ctx) {
  return (
    'I am waiting because the prior pipeline left the turn in a read-only or gated state — ' +
    'either operator approval is required, or I classified the turn as inspection rather than execution. ' +
    'I should have stated that gate explicitly instead of implying progress.'
  );
}

function composeAccomplishment(ctx) {
  const priorIntent = ctx.intentClassification.previous;
  return (
    `On the prior turn I was trying to ${priorIntent && priorIntent.intent ? priorIntent.intent : 'respond'} ` +
    `through the ${pipelineLabel(ctx.selectedPipeline.previous)} — ` +
    'to give you a useful answer within that pipeline\'s contract, not to reflect on my own process.'
  );
}

function composeUncertainty() {
  return (
    'Where I am uncertain: whether your prior question was about business facts or about my interpretation; ' +
    'whether the pipeline I selected was appropriate; and whether my recommendation matched what you actually wanted.'
  );
}

function composeGeneralReflection(ctx) {
  if (!ctx.hasPrecedingTurn) {
    return (
      'This is a reflection turn. I do not have a preceding exchange yet, ' +
      'so I am answering about my process rather than consulting business intelligence.'
    );
  }
  return composeReasoningWalkthrough(ctx);
}

/**
 * @param {object} input
 * @returns {{ prose: string, reflectKind: string, reasoning: string[] }}
 */
function composeReflectiveResponse(input = {}) {
  const question = normalizeText(input.question);
  const ctx = input.reflectionContext || {};
  const reflectKind = classifyReflectQuestion(question);

  let prose;
  switch (reflectKind) {
    case 'intent_misinterpretation':
      prose = composeIntentMisinterpretation(ctx);
      break;
    case 'answer_style':
      prose = composeAnswerStyle(ctx);
      break;
    case 'reasoning_walkthrough':
      prose = composeReasoningWalkthrough(ctx);
      break;
    case 'assumptions':
      prose = composeAssumptions(ctx);
      break;
    case 'misunderstanding':
      prose = composeMisunderstanding(ctx);
      break;
    case 'scout_not_executed':
      prose = composeScoutPipeline(ctx, false);
      break;
    case 'scout_executed':
      prose = composeScoutPipeline(ctx, true);
      break;
    case 'pipeline_selection':
      prose = composePipelineSelection(ctx);
      break;
    case 'recommendation_rationale':
      prose = composeRecommendationRationale(ctx);
      break;
    case 'alternatives':
      prose = composeAlternatives(ctx);
      break;
    case 'self_critique':
      prose = composeSelfCritique(ctx);
      break;
    case 'waiting':
      prose = composeWaiting(ctx);
      break;
    case 'accomplishment':
      prose = composeAccomplishment(ctx);
      break;
    case 'uncertainty':
      prose = composeUncertainty(ctx);
      break;
    default:
      prose = composeGeneralReflection(ctx);
  }

  return {
    prose: normalizeText(prose),
    reflectKind,
    reasoning: [
      `reflect_kind:${reflectKind}`,
      `prior_pipeline:${ctx.selectedPipeline.previous || 'none'}`,
      `prior_intent:${ctx.intentClassification.previous && ctx.intentClassification.previous.intent || 'none'}`,
    ],
  };
}

function buildReflectionStructured(conversational, conversationIntent, conversationSubject) {
  return {
    answer: conversational.prose,
    summary: conversational.prose.slice(0, 240),
    reasoning: [],
    recommendedActions: [],
    metadata: {
      reflectiveCognition: true,
      spec: 'SPEC-148',
      reflectKind: conversational.reflectKind,
      conversationIntent: conversationIntent && conversationIntent.intent,
      conversationSubject: conversationSubject && conversationSubject.subject,
      subjectLocked: Boolean(conversationSubject && conversationSubject.locked),
      showReasoningDisclosure: true,
      businessIntelligenceUsed: false,
    },
  };
}

module.exports = {
  PIPELINE_LABELS,
  classifyReflectQuestion,
  composeReflectiveResponse,
  buildReflectionStructured,
};
