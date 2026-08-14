'use strict';

/**
 * SPEC-098 — Max Workspace thin adapter for approved Client Intelligence.
 *
 * Runs early in ask() so durable CIE context influences interpretation.
 * Context only — never executes Missions or mutates CRM/outreach state.
 * Does not persist chat history; approved Blueprint/Playbook are authoritative.
 */

const cie = require('../clientIntelligence');
const { buildStructuredResponse } = require('./WorkspaceTypes');

const ACTIVE_ONBOARDING_STATUSES = new Set([
  'NEW',
  'DISCOVERY',
  'CLARIFICATION',
  'VALIDATION',
  'BLUEPRINT_GENERATION',
  'CLIENT_REVIEW',
]);

function defaultCieService() {
  return cie;
}

function sectionSummary(sections, key) {
  const s = sections && sections[key];
  if (!s) return '';
  if (typeof s === 'string') return String(s).trim();
  return s.summary != null ? String(s.summary).trim() : '';
}

/**
 * SPEC-099 — strip literal uncertainty phrases so Max never treats them as facts.
 */
function sanitizeFactSummary(text) {
  const s = String(text || '').trim();
  if (!s) return '';
  if (/\bi don'?t know\b|\bnot sure yet\b|\bhaven'?t figured\b/i.test(s)) {
    return '';
  }
  if (/^ideal customers are\s+(i don'?t know|not sure|unknown)\b/i.test(s)) {
    return '';
  }
  return s;
}

function normalizeBlueprintSummary(blueprint) {
  if (!blueprint || typeof blueprint !== 'object') return null;
  const sections = blueprint.sections || {};
  const identity = sanitizeFactSummary(sectionSummary(sections, 'identity'));
  const services = sanitizeFactSummary(sectionSummary(sections, 'services'));
  const idealCustomers = sanitizeFactSummary(
    sectionSummary(sections, 'idealCustomers')
  );
  const avoidCustomers = sanitizeFactSummary(
    sectionSummary(sections, 'avoidCustomers')
  );
  const targetMarkets = sanitizeFactSummary(
    sectionSummary(sections, 'targetMarkets')
  );
  const competitiveAdvantages = sanitizeFactSummary(
    sectionSummary(sections, 'competitiveAdvantages')
  );
  const brandVoice = sanitizeFactSummary(sectionSummary(sections, 'brandVoice'));
  const campaignGoals = sanitizeFactSummary(
    sectionSummary(sections, 'campaignGoals')
  );
  const successMetrics = sanitizeFactSummary(
    sectionSummary(sections, 'successMetrics')
  );

  const unknowns = [];
  for (const [key, label] of [
    ['identity', 'who you are'],
    ['services', 'what you offer'],
    ['idealCustomers', 'who you want to serve'],
    ['targetMarkets', 'where you operate'],
    ['campaignGoals', 'what you want next'],
  ]) {
    if (!sanitizeFactSummary(sectionSummary(sections, key))) unknowns.push(label);
  }
  // Surface explicit section unknowns (e.g. unresolved commercial ICP) ahead of soft gaps.
  for (const key of Object.keys(sections || {})) {
    const section = sections[key];
    for (const u of (section && section.unknowns) || []) {
      const label = String(u || '').trim();
      if (!label) continue;
      if (!unknowns.some((x) => x.toLowerCase() === label.toLowerCase())) {
        if (/commercial customer segment/i.test(label)) unknowns.unshift(label);
        else unknowns.push(label);
      }
    }
  }

  const confidence = blueprint.confidenceSummary || null;

  return {
    blueprintId: blueprint.id || null,
    sessionId: blueprint.sessionId || blueprint.session_id || null,
    clientId: blueprint.clientId != null ? blueprint.clientId : blueprint.client_id,
    version: blueprint.version || null,
    status: blueprint.status || null,
    approved: String(blueprint.status || '').toLowerCase() === 'approved',
    identity,
    services,
    idealCustomers,
    avoidCustomers,
    targetMarkets,
    competitiveAdvantages,
    brandVoice,
    campaignGoals,
    successMetrics,
    unknowns,
    confidence,
    playbookId: blueprint.playbookId || blueprint.playbook_id || null,
    playbookVersion:
      blueprint.playbookVersion || blueprint.playbook_version || null,
  };
}

function buildClientIntelligenceAttachment(summary, playbook = null) {
  return {
    clientIntelligence: summary
      ? {
          ...summary,
          playbookStatus: playbook
            ? playbook.status || playbook.playbookStatus || null
            : summary.playbookId
              ? 'linked'
              : null,
          playbookPending:
            playbook &&
            String(playbook.status || '').toLowerCase() === 'pending_review',
          source: 'cie_approved_blueprint',
        }
      : {
          approved: false,
          missing: true,
          source: 'cie_none',
        },
    businessBlueprint: summary || null,
  };
}

/**
 * Load the most recently approved Blueprint for a client (fail soft).
 */
async function loadApprovedClientIntelligence(input = {}) {
  const service = input.cieService || defaultCieService();
  const tenantId = String(
    input.tenantId ||
      input.clientId ||
      input.client_id ||
      ''
  ).trim();
  if (!tenantId || !Number.isFinite(Number(tenantId))) {
    return { summary: null, attachment: buildClientIntelligenceAttachment(null) };
  }
  const clientId = Number(tenantId);

  let blueprint = null;
  try {
    if (typeof service.getApprovedClientBlueprint === 'function') {
      blueprint = await service.getApprovedClientBlueprint(clientId, input.cieOpts || {});
    } else {
      const listed = await service.listApprovedBlueprintSessions({
        clientId,
        includeSamples: false,
        samplesOnly: false,
        limit: 5,
        ...(input.cieOpts || {}),
      });
      const first = (listed && listed.sessions && listed.sessions[0]) || null;
      if (first && first.sessionId) {
        const detail = await service.getInterview(first.sessionId, input.cieOpts || {});
        blueprint = detail && detail.blueprint ? detail.blueprint : null;
        if (blueprint && String(blueprint.status || '').toLowerCase() !== 'approved') {
          blueprint = null;
        }
      }
    }
  } catch (_) {
    blueprint = null;
  }

  // Reject non-approved (pending review must stay advisory / not facts)
  if (
    blueprint &&
    String(blueprint.status || '').toLowerCase() !== 'approved'
  ) {
    blueprint = null;
  }

  const summary = normalizeBlueprintSummary(blueprint);
  let playbook = null;
  if (summary && summary.playbookId && typeof service.getPlaybookById === 'function') {
    try {
      playbook = await service.getPlaybookById(summary.playbookId, input.cieOpts || {});
    } catch (_) {
      playbook = null;
    }
  }

  return {
    summary,
    blueprint,
    playbook,
    attachment: buildClientIntelligenceAttachment(summary, playbook),
    clientId,
  };
}

async function attachClientIntelligenceContext(input = {}) {
  const session = input.session || null;
  const sessionCtx =
    session && session.context && typeof session.context === 'object'
      ? session.context
      : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};

  const tenantId = String(
    envelope.tenantId ||
      sessionCtx.tenantId ||
      (session && session.context && session.context.tenantId) ||
      ''
  ).trim();

  const loaded = await loadApprovedClientIntelligence({
    tenantId,
    clientId:
      envelope.clientId ??
      envelope.client_id ??
      sessionCtx.clientId ??
      sessionCtx.client_id ??
      tenantId,
    cieService: input.cieService,
    cieOpts: input.cieOpts,
  });

  if (session && session.context && typeof session.context === 'object') {
    Object.assign(session.context, loaded.attachment);
    if (loaded.summary) {
      session.context.clientId =
        session.context.clientId != null
          ? session.context.clientId
          : loaded.clientId;
    }
  }

  return loaded;
}

function workspaceStructured(answer, reasoning, extras = {}) {
  return buildStructuredResponse({
    answer,
    reasoning,
    supportingEvidence: extras.supportingEvidence || [],
    contradictingEvidence: [],
    confidence: extras.confidence != null ? extras.confidence : 0.88,
    nextInvestigations: extras.nextInvestigations || [],
    recommendedActions: extras.recommendedActions || [
      {
        id: 'acknowledge',
        type: 'review',
        label: 'Continue',
      },
    ],
    confidenceContributors: ['client_intelligence', 'spec_098'],
    timelineReferences: [],
    relatedEntities: extras.relatedEntities || [],
    metadata: {
      sourcesUsed: {
        briefing: false,
        reasoning: true,
        memory: true,
        policy: false,
        knowledge: true,
      },
      evidenceCount: extras.evidenceCount != null ? extras.evidenceCount : 0,
      asOf: new Date().toISOString(),
      unavailable: extras.unavailable || [],
      blueprintId: extras.blueprintId || null,
    },
  });
}

function looksLikeBusinessUnderstandingAsk(question) {
  const q = String(question || '').trim().toLowerCase();
  if (!q) return false;
  return (
    /what do you (know|understand) about (my |our )?business/.test(q) ||
    /what have you learned about (my |our )?business/.test(q) ||
    /tell me what you (know|understand)/.test(q) ||
    /summarize (my |our )?business/.test(q) ||
    /who (am i|are we)( to you)?\b/.test(q)
  );
}

function looksLikeTargetingAsk(question) {
  const q = String(question || '').trim().toLowerCase();
  return (
    /who should we target/.test(q) ||
    /who (do|should) we (pursue|target|go after)/.test(q) ||
    /ideal customer/.test(q) ||
    /first (segment|audience|target)/.test(q)
  );
}

function looksLikeUnknownsAsk(question) {
  const q = String(question || '').trim().toLowerCase();
  return (
    /biggest unknowns?/.test(q) ||
    /what (do|don't|dont) we (still )?know/.test(q) ||
    /known unknowns?/.test(q) ||
    /what('s| is) (still )?unclear/.test(q)
  );
}

function looksLikeFocusAsk(question) {
  const q = String(question || '').trim().toLowerCase();
  return (
    /what should we (focus on|do) (this week|next)/.test(q) ||
    /what should we do next/.test(q) ||
    /focus (for |this )?week/.test(q) ||
    /priority (this week|next)/.test(q)
  );
}

function looksLikeClientIntelligenceAsk(question) {
  return (
    looksLikeBusinessUnderstandingAsk(question) ||
    looksLikeTargetingAsk(question) ||
    looksLikeUnknownsAsk(question) ||
    looksLikeFocusAsk(question)
  );
}

function formatUnderstandingAnswer(summary) {
  const parts = [];
  if (summary.identity) parts.push(summary.identity);
  if (summary.services) parts.push(`You offer: ${summary.services}`);
  if (summary.idealCustomers) {
    parts.push(`Ideal customers: ${summary.idealCustomers}`);
  }
  if (summary.targetMarkets) parts.push(`Markets: ${summary.targetMarkets}`);
  if (summary.competitiveAdvantages) {
    parts.push(`Differentiation: ${summary.competitiveAdvantages}`);
  }
  if (summary.campaignGoals) parts.push(`Goals: ${summary.campaignGoals}`);
  if (!parts.length) {
    return (
      'I have an approved Business Blueprint on file, but the section summaries are thin. ' +
      'We can refine understanding in Client Intelligence before treating more detail as established fact.'
    );
  }
  return (
    `Here is what I understand from your approved Business Blueprint:\n\n` +
    parts.map((p) => `• ${p}`).join('\n') +
    `\n\nThis comes from durable client intelligence — not this chat session. ` +
    `Strategy and execution remain review-controlled.`
  );
}

function formatMissingAnswer() {
  return (
    'I do not yet have an approved Business Blueprint for your client. ' +
    'Complete Client Intelligence onboarding (/client-intel) and approve the Blueprint ' +
    'so I can reason from your established business understanding. ' +
    'I will not invent facts about your business in the meantime.'
  );
}

function formatTargetingAnswer(summary) {
  if (summary.idealCustomers) {
    let out = `Based on your approved Blueprint, prioritize: ${summary.idealCustomers}.`;
    if (summary.avoidCustomers) {
      out += ` Avoid: ${summary.avoidCustomers}.`;
    }
    if (summary.targetMarkets) {
      out += ` Geography/context: ${summary.targetMarkets}.`;
    }
    out +=
      ' This is established understanding from onboarding — campaign execution still needs review.';
    return out;
  }
  return (
    'Your approved Blueprint does not yet spell out ideal customers clearly. ' +
    'That remains an unknown until you refine Client Intelligence.'
  );
}

function formatUnknownsAnswer(summary) {
  if (summary.unknowns && summary.unknowns.length) {
    return (
      `Biggest unknowns from the approved Blueprint:\n` +
      summary.unknowns.map((u) => `• ${u}`).join('\n') +
      `\n\nI will keep these as unknowns rather than inventing answers.`
    );
  }
  const soft = [];
  if (!summary.avoidCustomers) soft.push('who to avoid');
  if (!summary.successMetrics) soft.push('success metrics');
  if (!summary.competitiveAdvantages) soft.push('differentiation');
  if (soft.length) {
    return (
      `Core sections look populated, but these are still thin or missing:\n` +
      soft.map((u) => `• ${u}`).join('\n')
    );
  }
  return (
    'The approved Blueprint covers the core sections. Remaining uncertainty should be treated as operational, not invented business facts.'
  );
}

function formatFocusAnswer(summary, attachment) {
  const bits = [];
  if (summary.campaignGoals) bits.push(summary.campaignGoals);
  if (summary.idealCustomers) {
    bits.push(`Stay focused on ${summary.idealCustomers}`);
  }
  if (attachment && attachment.clientIntelligence && attachment.clientIntelligence.playbookPending) {
    bits.push(
      'Playbook strategy is still pending review — do not treat recommendations as approved execution'
    );
  } else if (summary.successMetrics) {
    bits.push(`Measure against: ${summary.successMetrics}`);
  }
  if (!bits.length) {
    return (
      'From your approved understanding, the next focus is completing any thin Blueprint sections and keeping strategy review-controlled. Max is the primary interface after onboarding.'
    );
  }
  return (
    `This week, grounded in your approved Blueprint:\n` +
    bits.map((b) => `• ${b}`).join('\n') +
    `\n\nI am using durable client intelligence, not a prior chat transcript.`
  );
}

/**
 * Pre-routing CIE handler. Returns a workspace result when it fully handles
 * the turn; otherwise returns null with context already attached.
 */
async function maybeHandleClientIntelligenceTurn(input = {}) {
  const question = String(input.question || '').trim();
  if (!question) return null;

  const pre = await attachClientIntelligenceContext(input);
  const summary = pre.summary;
  const attachment = pre.attachment;

  if (!looksLikeClientIntelligenceAsk(question)) {
    return {
      handled: false,
      summary,
      attachment,
      clientId: pre.clientId,
    };
  }

  if (!summary || !summary.approved) {
    const prose = formatMissingAnswer();
    return {
      reason: 'client_intelligence_missing',
      handled: true,
      prose,
      structured: workspaceStructured(prose, [
        'No approved Business Blueprint for this authenticated client.',
        'Fail closed — do not invent client facts (SPEC-098).',
      ], { unavailable: ['approved_blueprint'] }),
      summary: null,
      attachment,
    };
  }

  let prose;
  let reason = 'client_intelligence_context';
  if (looksLikeBusinessUnderstandingAsk(question)) {
    prose = formatUnderstandingAnswer(summary);
    reason = 'client_intelligence_understanding';
  } else if (looksLikeTargetingAsk(question)) {
    prose = formatTargetingAnswer(summary);
    reason = 'client_intelligence_targeting';
  } else if (looksLikeUnknownsAsk(question)) {
    prose = formatUnknownsAnswer(summary);
    reason = 'client_intelligence_unknowns';
  } else {
    prose = formatFocusAnswer(summary, attachment);
    reason = 'client_intelligence_focus';
  }

  return {
    reason,
    handled: true,
    prose,
    structured: workspaceStructured(
      prose,
      [
        'Answer grounded in the most recently approved Business Blueprint.',
        'Playbook recommendations are not treated as established facts.',
        'No autonomous execution from context reconstruction (SPEC-098).',
      ],
      {
        blueprintId: summary.blueprintId,
        evidenceCount: 1,
        supportingEvidence: [
          {
            id: summary.blueprintId || 'blueprint',
            label: 'Approved Business Blueprint',
            detail: summary.identity || 'Approved client understanding',
          },
        ],
      }
    ),
    summary,
    attachment,
  };
}

module.exports = {
  ACTIVE_ONBOARDING_STATUSES,
  normalizeBlueprintSummary,
  buildClientIntelligenceAttachment,
  loadApprovedClientIntelligence,
  attachClientIntelligenceContext,
  maybeHandleClientIntelligenceTurn,
  looksLikeClientIntelligenceAsk,
  looksLikeBusinessUnderstandingAsk,
  looksLikeTargetingAsk,
  looksLikeUnknownsAsk,
  looksLikeFocusAsk,
  formatUnderstandingAnswer,
  formatMissingAnswer,
  formatUnknownsAnswer,
  sanitizeFactSummary,
};
