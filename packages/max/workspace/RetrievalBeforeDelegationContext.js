'use strict';

/**
 * SPEC-102 — retrieve durable knowledge before any specialist routing.
 *
 * Memory → reasoning → specialist → execution.
 * Retrieval / explanation / reflection never invoke a specialist.
 * Unknown is a successful epistemic response.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  classifyCognitiveMode,
  COGNITIVE_MODES,
  forbidsSpecialistDelegation,
} = require('../specialistDelegation/CognitiveMode');
const { UNKNOWN_ANSWER } = require('../specialistDelegation/RetrievalGate');

const SERVICE_AREA_RE =
  /\b(service area|geography|market area|territory|where (?:do|are) we (?:serve|operate|work))\b/i;

const ENTITY_KNOW_RE =
  /\bwhat do you (?:currently )?(?:know|understand|remember) about\b/i;

const INDUSTRY_RE =
  /\b(what industries|which industries|who do we (?:target|serve)|ideal customers)\b/i;

const HISTORY_RE =
  /\bwhat happened (?:yesterday|today|last (?:night|week)|in (?:that|the) (?:conversation|investigation|search))\b/i;

const ELEVATION_RE =
  /\bwhy (?:didn'?t|did) you elevate\b/i;

function isHardRetrievalQuestion(question, mode) {
  if (SERVICE_AREA_RE.test(question)) return true;
  if (ENTITY_KNOW_RE.test(question)) return true;
  if (INDUSTRY_RE.test(question)) return true;
  if (HISTORY_RE.test(question)) return true;
  if (ELEVATION_RE.test(question)) return true;
  if (mode && mode.kind === COGNITIVE_MODES.REFLECTION) return true;
  if (mode && mode.kind === COGNITIVE_MODES.EXPLANATION) return true;
  if (mode && mode.via === 'retrieval') return true;
  return false;
}

function present(text) {
  return String(text || '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function entityNameFromQuestion(question) {
  const q = String(question || '');
  const m = q.match(
    /\b(?:know|understand|remember) about\s+(.+?)\??\s*$/i
  );
  if (!m) return '';
  return present(m[1]).replace(/[.]+$/, '');
}

function namesMatch(asked, known) {
  const a = present(asked).toLowerCase();
  const b = present(known).toLowerCase();
  if (!a || !b) return false;
  if (a === b) return true;
  const aTokens = a.split(/\s+/).filter((t) => t.length > 2);
  const bTokens = b.split(/\s+/).filter((t) => t.length > 2);
  if (!aTokens.length) return false;
  return aTokens.every((t) => bTokens.includes(t) || b.includes(t));
}

async function inspectRetrievalSources(input = {}) {
  const session = input.session || {};
  const sessionCtx =
    session.context && typeof session.context === 'object' ? session.context : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};

  const cie = sessionCtx.clientIntelligence || envelope.clientIntelligence || null;
  const summary = cie && (cie.approved === true || cie.identity || cie.businessName || cie.geography)
    ? cie
    : null;

  const serviceArea =
    sessionCtx.serviceArea ||
    sessionCtx.geography ||
    (summary && (summary.geography || summary.targetMarkets)) ||
    (cie && (cie.geography || cie.targetMarkets)) ||
    (envelope.businessContext && envelope.businessContext.serviceGeography) ||
    (envelope.targetContext && envelope.targetContext.geography) ||
    null;

  return {
    blueprint: summary,
    playbook: sessionCtx.playbook || envelope.playbook || (summary && summary.playbook) || null,
    knowledgeGraph: sessionCtx.knowledgeGraph || envelope.knowledgeGraph || null,
    missionState: sessionCtx.activeMission || sessionCtx.mission || envelope.mission || null,
    previousInvestigations:
      sessionCtx.lastScoutInvestigation ||
      sessionCtx.lastCognitiveTraceId ||
      envelope.lastScoutInvestigation ||
      null,
    briefing: sessionCtx.briefing || envelope.briefing || null,
    conversation: Array.isArray(session.messages) ? session.messages.slice(-8) : [],
    serviceArea: serviceArea ? present(serviceArea) : null,
    businessName: present(
      (summary && (summary.businessName || summary.identity)) ||
        (cie && (cie.businessName || cie.identity)) ||
        ''
    ),
    identity: present((summary && summary.identity) || (cie && cie.identity) || ''),
    industries: present(
      (summary && summary.idealCustomers) || (cie && cie.idealCustomers) || ''
    ),
    unknowns: (summary && summary.unknowns) || (cie && cie.unknowns) || [],
    evaluation: sessionCtx.lastScoutEvaluation || envelope.lastScoutEvaluation || null,
    investigation:
      sessionCtx.lastScoutInvestigation || envelope.lastScoutInvestigation || null,
    lastSpecialist:
      (sessionCtx.lastSpecialistEvaluation &&
        sessionCtx.lastSpecialistEvaluation.specialist) ||
      (sessionCtx.lastScoutEvaluation ? 'scout' : null),
  };
}

function sourceOrderUsed(sources, used) {
  const order = [
    'blueprint',
    'playbook',
    'knowledgeGraph',
    'missionState',
    'previousInvestigations',
    'briefing',
    'conversation',
  ];
  return order.filter((key) => used.includes(key) && sources[key]);
}

function composeRetrievalAnswer(question, mode, sources) {
  const used = [];

  if (SERVICE_AREA_RE.test(question) && sources.serviceArea) {
    used.push('blueprint');
    return {
      prose: `I currently understand our service area as ${sources.serviceArea}. That's durable business knowledge — I don't need a specialist to recall it.`,
      used,
    };
  }

  if (ENTITY_KNOW_RE.test(question)) {
    const asked = entityNameFromQuestion(question);
    if (asked && sources.businessName && namesMatch(asked, sources.businessName)) {
      used.push('blueprint');
      const bits = [
        sources.identity || sources.businessName,
        sources.serviceArea ? `Service area: ${sources.serviceArea}` : null,
        sources.industries ? `We target ${sources.industries}` : null,
      ].filter(Boolean);
      return {
        prose: bits.length
          ? `Here's what I already know about ${sources.businessName}: ${bits.join('. ')}.`
          : `I know ${sources.businessName} from our approved business understanding.`,
        used,
      };
    }
  }

  if (INDUSTRY_RE.test(question) && sources.industries) {
    used.push('blueprint');
    return {
      prose: `From our approved understanding, we target ${sources.industries}.`,
      used,
    };
  }

  if (HISTORY_RE.test(question) && sources.investigation) {
    used.push('previousInvestigations');
    const coverage =
      sources.investigation.coverageBand ||
      (sources.investigation.coverage && sources.investigation.coverageBand) ||
      null;
    return {
      prose: coverage
        ? `The most recent investigation coverage was ${coverage}. I can inspect that work — I won't rerun a specialist to narrate it.`
        : 'I have a prior investigation on file. I can inspect that work rather than starting a new one.',
      used,
    };
  }

  if (ELEVATION_RE.test(question) && sources.evaluation) {
    used.push('previousInvestigations');
    if (sources.evaluation.materialChange === true) {
      return {
        prose: 'I elevated Acquisition because the last evaluation found a material change in opportunity.',
        used,
      };
    }
    return {
      prose: "I didn't elevate Acquisition because the last evaluation was not material enough to change Command Deck priority.",
      used,
    };
  }

  if (mode.kind === COGNITIVE_MODES.REFLECTION) {
    const unknowns = Array.isArray(sources.unknowns)
      ? sources.unknowns.filter(Boolean)
      : [];
    if (unknowns.length) {
      used.push('blueprint');
      return {
        prose: `I'm uncertain about: ${unknowns.slice(0, 3).join('; ')}.`,
        used,
      };
    }
    if (sources.evaluation && Array.isArray(sources.evaluation.uncertainties) && sources.evaluation.uncertainties.length) {
      used.push('previousInvestigations');
      return {
        prose: `From the last evaluation, I'm uncertain about: ${sources.evaluation.uncertainties.slice(0, 3).join('; ')}.`,
        used,
      };
    }
    return { prose: UNKNOWN_ANSWER, used };
  }

  if (mode.kind === COGNITIVE_MODES.EXPLANATION && !sources.evaluation) {
    return { prose: UNKNOWN_ANSWER, used };
  }

  return { prose: UNKNOWN_ANSWER, used };
}

function workspaceStructured(answer, extras = {}) {
  return buildStructuredResponse({
    answer,
    reasoning: extras.reasoning || [
      'Answered from durable knowledge before considering a specialist.',
    ],
    supportingEvidence: extras.supportingEvidence || [],
    contradictingEvidence: [],
    confidence: extras.confidence != null ? extras.confidence : 0.84,
    nextInvestigations: [],
    recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
    confidenceContributors: ['retrieval_before_delegation', 'spec_102'],
    timelineReferences: [],
    relatedEntities: extras.relatedEntities || [],
    metadata: {
      sourcesUsed: {
        briefing: extras.used && extras.used.includes('briefing'),
        reasoning: true,
        memory: true,
        policy: true,
        knowledge: extras.used && extras.used.includes('blueprint'),
      },
      evidenceCount: extras.evidenceCount || 0,
      asOf: new Date().toISOString(),
      unavailable: extras.unavailable || [],
      cognitiveMode: extras.cognitiveMode || null,
      retrievalBeforeDelegation: true,
      specialistDelegated: false,
      scoutDelegated: false,
    },
  });
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleRetrievalBeforeDelegationTurn(input = {}) {
  const question = String(input.question || '').trim();
  if (!question) return null;

  const mode =
    input.cognitive ||
    classifyCognitiveMode(question, {
      session: input.session,
      context: input.context,
    });

  if (!forbidsSpecialistDelegation(mode)) return null;

  const sources = await inspectRetrievalSources(input);
  const composed = composeRetrievalAnswer(question, mode, sources);
  if (
    composed.prose === UNKNOWN_ANSWER &&
    !isHardRetrievalQuestion(question, mode)
  ) {
    return null;
  }
  const used = sourceOrderUsed(sources, composed.used || []);

  if (input.session && input.session.context && typeof input.session.context === 'object') {
    input.session.context.lastCognitiveMode = mode.kind;
    input.session.context.lastRetrievalSources = used;
  }

  const structured = workspaceStructured(composed.prose, {
    used,
    cognitiveMode: mode.kind,
    reasoning: [
      `Classified operator intent as ${mode.kind}.`,
      composed.prose === UNKNOWN_ANSWER
        ? 'Durable knowledge did not answer this. Unknown is acceptable — I will not invent work for a specialist.'
        : 'Retrieved from durable knowledge instead of delegating.',
    ],
    evidenceCount: used.length,
    unavailable: composed.prose === UNKNOWN_ANSWER ? ['durable_knowledge'] : [],
  });

  return {
    reason: 'retrieval_before_delegation',
    structured,
    prose: composed.prose,
    mode,
    sourcesUsed: used,
    delegated: false,
  };
}

module.exports = {
  maybeHandleRetrievalBeforeDelegationTurn,
  inspectRetrievalSources,
  composeRetrievalAnswer,
  SERVICE_AREA_RE,
  ENTITY_KNOW_RE,
};
