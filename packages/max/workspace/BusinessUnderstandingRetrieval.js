'use strict';

/**
 * SPEC-103 — durable business understanding retrieval.
 *
 * Canonical read path for approved business knowledge before Max answers
 * retrieval questions or delegates to specialists.
 *
 * Hierarchy (first hit wins for composition; all are inspected):
 *   Blueprint → Playbook → Knowledge Graph → Mission/Objectives →
 *   Campaign Context → Recent Workspace Context → Unknown
 */

const {
  loadApprovedClientIntelligence,
  formatUnderstandingAnswer,
  formatTargetingAnswer,
} = require('./ClientIntelligenceContext');
const { attachActiveObjectiveContext } = require('./OperatorObjectiveContext');

const KNOWLEDGE_STATES = Object.freeze({
  AVAILABLE: 'available',
  NEVER_LEARNED: 'never_learned',
  RETRIEVAL_FAILURE: 'retrieval_failure',
});

const SERVICE_AREA_RE =
  /\b(service area|geography|market area|territory|where (?:do|are) we (?:serve|operate|work))\b/i;

const ENTITY_KNOW_RE =
  /\bwhat do you (?:currently )?(?:know|understand|remember) about\b/i;

const BUSINESS_KNOW_RE =
  /\bwhat do you (?:currently )?(?:know|understand|remember) about (?:my|our) business\b/i;

const INDUSTRY_RE =
  /\b(what industries|which industries|who (?:are|do) we (?:target|serve)|ideal customers?|target customers?|target markets?)\b/i;

const PRIORITIES_RE =
  /\b(current (?:business )?priorit(?:y|ies)|business priorities|what (?:are|should) we focus|near[- ]term focus)\b/i;

const GOALS_RE =
  /\b(business goals?|our goals?|campaign goals?|what (?:are|is) our goal)\b/i;

const VALUE_PROP_RE =
  /\b(value proposition|how do we compete|competitive advantage|why choose us)\b/i;

const CONSTRAINTS_RE =
  /\b(constraints?|who (?:do|should) we avoid|avoid customers?|customers? to avoid)\b/i;

function present(text) {
  return String(text || '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function resolveTenantId(input = {}) {
  const session = input.session || {};
  const sessionCtx =
    session.context && typeof session.context === 'object' ? session.context : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};
  return String(
    input.tenantId ||
      envelope.tenantId ||
      sessionCtx.tenantId ||
      envelope.clientId ||
      sessionCtx.clientId ||
      ''
  ).trim();
}

function sessionHasApprovedUnderstanding(sessionCtx = {}) {
  const cie = sessionCtx.clientIntelligence;
  return Boolean(
    cie &&
      (cie.approved === true ||
        String(cie.status || '').toLowerCase() === 'approved')
  );
}

/**
 * Stable retrieval interface — minimum operating context for Max.
 *
 * @param {object|null} summary normalized Blueprint summary
 * @param {object|null} playbook linked Client Playbook
 * @param {object[]} objectives active operator objectives
 * @returns {object|null}
 */
function buildBusinessUnderstandingContract(summary, playbook = null, objectives = []) {
  if (!summary || !summary.approved) return null;

  const activeObjectives = Array.isArray(objectives)
    ? objectives.filter((o) => o && o.status !== 'completed' && o.status !== 'cancelled')
    : [];

  const currentPriorities = activeObjectives.length
    ? activeObjectives
        .slice(0, 5)
        .map((o) => present(o.title || o.description || o.objective))
        .filter(Boolean)
    : summary.campaignGoals
      ? [present(summary.campaignGoals)]
      : [];

  return {
    companyName: present(summary.businessName || summary.identity),
    serviceArea: present(summary.geography || summary.targetMarkets),
    services: present(summary.services),
    targetCustomers: present(summary.idealCustomers),
    targetGeography: present(summary.geography || summary.targetMarkets),
    valueProposition: present(summary.competitiveAdvantages),
    businessGoals: present(summary.campaignGoals),
    currentPriorities,
    constraints: present(summary.avoidCustomers),
    unknowns: Array.isArray(summary.unknowns) ? summary.unknowns.filter(Boolean) : [],
    playbookStatus: playbook ? playbook.status || null : null,
    playbookName: playbook ? present(playbook.name) : null,
  };
}

function summarizeKnowledgeGraph(envelope = {}, sessionCtx = {}) {
  const kg = sessionCtx.knowledgeGraph || envelope.knowledgeGraph || null;
  if (!kg || typeof kg !== 'object') return null;
  const companies = Array.isArray(kg.companies) ? kg.companies.length : kg.companyCount;
  const people = Array.isArray(kg.people) ? kg.people.length : kg.peopleCount;
  if (companies || people) {
    return { companies: companies || 0, people: people || 0 };
  }
  return null;
}

/**
 * Load durable business understanding from persistent stores.
 * Attaches loaded context to the session when present.
 *
 * @param {object} input
 * @returns {Promise<object>}
 */
async function loadDurableBusinessUnderstanding(input = {}) {
  const session = input.session || {};
  const sessionCtx =
    session.context && typeof session.context === 'object' ? session.context : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};
  const tenantId = resolveTenantId(input);

  let knowledgeState = KNOWLEDGE_STATES.NEVER_LEARNED;
  let loadError = null;
  let summary = null;
  let playbook = null;
  let blueprintSource = null;

  if (sessionHasApprovedUnderstanding(sessionCtx)) {
    summary = sessionCtx.clientIntelligence;
    playbook = sessionCtx.playbook || null;
    blueprintSource = 'session';
    knowledgeState = KNOWLEDGE_STATES.AVAILABLE;
  } else if (tenantId && Number.isFinite(Number(tenantId))) {
    try {
      const loaded = await loadApprovedClientIntelligence({
        tenantId,
        clientId:
          envelope.clientId ??
          sessionCtx.clientId ??
          Number(tenantId),
        cieService: input.cieService,
        cieOpts: input.cieOpts,
        propagateLoadErrors: true,
      });
      summary = loaded.summary;
      playbook = loaded.playbook;
      blueprintSource = summary ? 'blueprint' : 'none';

      if (session && session.context && typeof session.context === 'object') {
        Object.assign(session.context, loaded.attachment);
        if (loaded.summary) {
          session.context.clientId =
            session.context.clientId != null
              ? session.context.clientId
              : loaded.clientId;
        }
      }

      if (summary && summary.approved) {
        knowledgeState = KNOWLEDGE_STATES.AVAILABLE;
      } else {
        knowledgeState = KNOWLEDGE_STATES.NEVER_LEARNED;
      }
    } catch (err) {
      loadError = err;
      const code = err && (err.code || err.name);
      const msg = String((err && err.message) || '');
      if (
        code === 'not_found' ||
        msg.includes('No approved blueprint') ||
        msg.includes('not found')
      ) {
        knowledgeState = KNOWLEDGE_STATES.NEVER_LEARNED;
      } else {
        knowledgeState = KNOWLEDGE_STATES.RETRIEVAL_FAILURE;
      }
    }
  }

  let objectivesAttachment = {
    activeObjectives: [],
    resolvedObjective: null,
    objectiveResolution: 'unresolved',
    attachment: {},
  };
  try {
    objectivesAttachment = await attachActiveObjectiveContext({
      session,
      context: envelope,
      question: input.question,
      objectiveService: input.objectiveService,
      objectiveOpts: input.objectiveOpts,
    });
    if (session && session.context && objectivesAttachment.attachment) {
      Object.assign(session.context, objectivesAttachment.attachment);
    }
  } catch (_) {
    /* fail soft */
  }

  const activeObjectives = objectivesAttachment.activeObjectives || [];
  const contract = buildBusinessUnderstandingContract(summary, playbook, activeObjectives);
  const knowledgeGraph = summarizeKnowledgeGraph(envelope, sessionCtx);

  return {
    tenantId,
    summary,
    playbook,
    contract,
    activeObjectives,
    knowledgeGraph,
    knowledgeState,
    loadError,
    blueprintSource,
    missionState:
      sessionCtx.activeMission || sessionCtx.mission || envelope.mission || null,
    previousInvestigations:
      sessionCtx.lastScoutInvestigation ||
      sessionCtx.lastCognitiveTraceId ||
      envelope.lastScoutInvestigation ||
      null,
    briefing: sessionCtx.briefing || envelope.briefing || null,
    conversation: Array.isArray(session.messages) ? session.messages.slice(-8) : [],
    evaluation: sessionCtx.lastScoutEvaluation || envelope.lastScoutEvaluation || null,
    investigation:
      sessionCtx.lastScoutInvestigation || envelope.lastScoutInvestigation || null,
  };
}

function entityNameFromQuestion(question) {
  const q = String(question || '');
  const m = q.match(/\b(?:know|understand|remember) about\s+(.+?)\??\s*$/i);
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

function groundingPrefix(summary) {
  const name = present(summary && (summary.businessName || summary.identity));
  if (!name) return 'Based on my current understanding';
  return `Based on my current understanding of ${name}`;
}

function formatNeverLearnedAnswer() {
  return (
    "I don't currently know enough about your business to answer that confidently. " +
    'Complete Client Intelligence onboarding and approve your Business Blueprint ' +
    'so I can remember what you teach me.'
  );
}

function formatRetrievalFailureAnswer() {
  return (
    "I learned about your business previously, but I can't retrieve that understanding right now. " +
    "That's an architectural problem on my side — not a gap in what you've taught me."
  );
}

function formatUnknownAnswer(knowledgeState) {
  if (knowledgeState === KNOWLEDGE_STATES.RETRIEVAL_FAILURE) {
    return formatRetrievalFailureAnswer();
  }
  if (knowledgeState === KNOWLEDGE_STATES.NEVER_LEARNED) {
    return formatNeverLearnedAnswer();
  }
  return "I don't currently know enough about your business to answer that confidently.";
}

function sourceOrderUsed(used) {
  const order = [
    'blueprint',
    'playbook',
    'knowledgeGraph',
    'missionState',
    'objectives',
    'previousInvestigations',
    'briefing',
    'conversation',
  ];
  return order.filter((key) => used.includes(key));
}

/**
 * Compose a retrieval answer from durable business understanding.
 *
 * @param {string} question
 * @param {object} mode cognitive mode
 * @param {object} bundle loadDurableBusinessUnderstanding result
 * @returns {{ prose: string, used: string[], knowledgeState: string }}
 */
function composeDurableRetrievalAnswer(question, mode, bundle = {}) {
  const used = [];
  const summary = bundle.summary;
  const contract = bundle.contract;
  const knowledgeState = bundle.knowledgeState || KNOWLEDGE_STATES.NEVER_LEARNED;

  if (!contract || !summary || !summary.approved) {
    return {
      prose: formatUnknownAnswer(knowledgeState),
      used,
      knowledgeState,
    };
  }

  if (SERVICE_AREA_RE.test(question) && contract.serviceArea) {
    used.push('blueprint');
    return {
      prose:
        `${groundingPrefix(summary)}, our service area is ${contract.serviceArea}. ` +
        "That's durable business knowledge — I don't need a specialist to recall it.",
      used,
      knowledgeState,
    };
  }

  if (BUSINESS_KNOW_RE.test(question) || ENTITY_KNOW_RE.test(question)) {
    const asked = entityNameFromQuestion(question);
    const isOwnBusiness =
      BUSINESS_KNOW_RE.test(question) ||
      (asked && summary.businessName && namesMatch(asked, summary.businessName)) ||
      (asked && summary.identity && namesMatch(asked, summary.identity)) ||
      (!asked && BUSINESS_KNOW_RE.test(question));

    if (isOwnBusiness || (asked && namesMatch(asked, summary.businessName || summary.identity))) {
      used.push('blueprint');
      const body = formatUnderstandingAnswer(summary);
      const prefix = groundingPrefix(summary);
      return {
        prose: body.startsWith("Here's what I understand")
          ? body.replace(/^Here's what I understand about/, `${prefix}, here's what I understand about`)
          : `${prefix}:\n\n${body}`,
        used,
        knowledgeState,
      };
    }
  }

  if (INDUSTRY_RE.test(question) && contract.targetCustomers) {
    used.push('blueprint');
    const targeting = formatTargetingAnswer(summary);
    return {
      prose: targeting.startsWith('Based on')
        ? targeting
        : `${groundingPrefix(summary)}, ${targeting.charAt(0).toLowerCase()}${targeting.slice(1)}`,
      used,
      knowledgeState,
    };
  }

  if (PRIORITIES_RE.test(question)) {
    used.push('blueprint');
    if (bundle.activeObjectives && bundle.activeObjectives.length) {
      used.push('objectives');
      const titles = bundle.activeObjectives
        .slice(0, 5)
        .map((o) => present(o.title || o.description))
        .filter(Boolean);
      return {
        prose:
          `${groundingPrefix(summary)}, our current priorities are: ${titles.join('; ')}.`,
        used,
        knowledgeState,
      };
    }
    if (contract.businessGoals) {
      return {
        prose: `${groundingPrefix(summary)}, our near-term focus is ${contract.businessGoals}.`,
        used,
        knowledgeState,
      };
    }
  }

  if (GOALS_RE.test(question) && contract.businessGoals) {
    used.push('blueprint');
    return {
      prose: `${groundingPrefix(summary)}, our business goal is ${contract.businessGoals}.`,
      used,
      knowledgeState,
    };
  }

  if (VALUE_PROP_RE.test(question) && contract.valueProposition) {
    used.push('blueprint');
    return {
      prose: `${groundingPrefix(summary)}, we compete on ${contract.valueProposition}.`,
      used,
      knowledgeState,
    };
  }

  if (CONSTRAINTS_RE.test(question) && contract.constraints) {
    used.push('blueprint');
    return {
      prose: `${groundingPrefix(summary)}, we should avoid ${contract.constraints}.`,
      used,
      knowledgeState,
    };
  }

  if (mode && mode.kind === 'reflection') {
    const unknowns = (contract.unknowns || []).filter(Boolean);
    if (unknowns.length) {
      used.push('blueprint');
      return {
        prose: `${groundingPrefix(summary)}, I'm still uncertain about: ${unknowns.slice(0, 3).join('; ')}.`,
        used,
        knowledgeState,
      };
    }
    if (
      bundle.evaluation &&
      Array.isArray(bundle.evaluation.uncertainties) &&
      bundle.evaluation.uncertainties.length
    ) {
      used.push('previousInvestigations');
      return {
        prose: `From the last evaluation, I'm uncertain about: ${bundle.evaluation.uncertainties.slice(0, 3).join('; ')}.`,
        used,
        knowledgeState,
      };
    }
  }

  return {
    prose: formatUnknownAnswer(knowledgeState),
    used,
    knowledgeState,
  };
}

/**
 * Flatten bundle into legacy inspectRetrievalSources shape for compatibility.
 */
function bundleToLegacySources(bundle = {}) {
  const summary = bundle.summary;
  const contract = bundle.contract;
  return {
    blueprint: summary,
    playbook: bundle.playbook,
    knowledgeGraph: bundle.knowledgeGraph,
    missionState: bundle.missionState,
    objectives: bundle.activeObjectives,
    previousInvestigations: bundle.previousInvestigations,
    briefing: bundle.briefing,
    conversation: bundle.conversation,
    serviceArea: contract ? contract.serviceArea : null,
    businessName: contract ? contract.companyName : null,
    identity: summary ? present(summary.identity) : null,
    industries: contract ? contract.targetCustomers : null,
    unknowns: contract ? contract.unknowns : [],
    evaluation: bundle.evaluation,
    investigation: bundle.investigation,
    contract,
    knowledgeState: bundle.knowledgeState,
    blueprintSource: bundle.blueprintSource,
  };
}

module.exports = {
  KNOWLEDGE_STATES,
  SERVICE_AREA_RE,
  ENTITY_KNOW_RE,
  BUSINESS_KNOW_RE,
  INDUSTRY_RE,
  PRIORITIES_RE,
  buildBusinessUnderstandingContract,
  loadDurableBusinessUnderstanding,
  composeDurableRetrievalAnswer,
  bundleToLegacySources,
  formatNeverLearnedAnswer,
  formatRetrievalFailureAnswer,
  formatUnknownAnswer,
  sourceOrderUsed,
  groundingPrefix,
};
