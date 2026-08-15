'use strict';

/**
 * SPEC-098 — Max Workspace thin adapter for approved Client Intelligence.
 * SPEC-103 — Client-context business reasoning from approved understanding.
 *
 * Runs early in ask() so durable CIE context influences interpretation.
 * Context only — never executes Missions or mutates CRM/outreach state.
 * Does not persist chat history; approved Blueprint/Playbook are authoritative.
 * Advisory reasoning is not execution authorization.
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
  const metadata = {
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
    // Owner-facing CIE answers are already composed; avoid appending
    // internal architectural narration via PresentationEngine.
    strictOutputShape:
      extras.strictOutputShape != null ? extras.strictOutputShape : true,
    clientFacing: true,
    evidenceBasis: extras.evidenceBasis || 'approved_client_understanding',
    recommendationConfidence: extras.recommendationConfidence || null,
  };
  if (extras.internalReasoning) {
    metadata.internalReasoning = extras.internalReasoning;
  }
  return buildStructuredResponse({
    answer,
    reasoning: extras.clientFacingReasoning != null
      ? extras.clientFacingReasoning
      : reasoning,
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
    confidenceContributors: extras.confidenceContributors || [
      'client_intelligence',
      'approved_blueprint',
    ],
    timelineReferences: [],
    relatedEntities: extras.relatedEntities || [],
    metadata,
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
    /who (am i|are we)( to you)?\b/.test(q) ||
    /what services do we offer/.test(q) ||
    /who are (my |our )?(ideal )?customers/.test(q)
  );
}

function looksLikeTargetingAsk(question) {
  const q = String(question || '').trim().toLowerCase();
  return (
    /who should we target/.test(q) ||
    /who (do|should) we (pursue|target|go after)/.test(q) ||
    /who should (i|we) (reach|contact|pursue|target)/.test(q) ||
    /ideal customer/.test(q) ||
    /first (segment|audience|target)/.test(q)
  );
}

function looksLikeUnknownsAsk(question) {
  const q = String(question || '').trim().toLowerCase();
  return (
    /biggest unknowns?/.test(q) ||
    /what (do|don't|dont) (we|you) (still )?know/.test(q) ||
    /what don'?t (we|you) know/.test(q) ||
    /known unknowns?/.test(q) ||
    /what('s| is) (still )?unclear/.test(q) ||
    /what are we missing/.test(q) ||
    /what (do|don't|dont) we know yet/.test(q)
  );
}

/**
 * Legacy focus detector (SPEC-098). Kept for continuity; SPEC-103 expands
 * via isClientContextReasoningRequest rather than phrase-specific traps.
 */
function looksLikeFocusAsk(question) {
  const q = String(question || '').trim().toLowerCase();
  return (
    /what should we (focus on|do) (this week|next|first)/.test(q) ||
    /what should we focus on\b/.test(q) ||
    /what should we do next/.test(q) ||
    /what should (i|we) do (this week|next|first)/.test(q) ||
    /focus (for |this )?week/.test(q) ||
    /priority (this week|next|first)/.test(q) ||
    /where should we start/.test(q) ||
    /what would you priorit/.test(q)
  );
}

/**
 * SPEC-103 — questions that need live market/prospect evidence, not Blueprint alone.
 * Conceptual: named entities, buying signals, current market ranking.
 */
function isEvidenceDependentClientRequest(question) {
  const q = String(question || '').trim().toLowerCase();
  if (!q) return false;
  if (
    /buying signals?/.test(q) ||
    /showing (buying |purchase )?signals?/.test(q) ||
    /which (companies|prospects|property managers|facility managers|accounts)/.test(
      q
    ) ||
    /who is (expanding|hiring|buying|looking)/.test(q) ||
    /most likely to need (us|our)/.test(q) ||
    /what changed in (our |the )?market/.test(q) ||
    /right now\b/.test(q) &&
      /(which|who|companies|prospects|signals|ranking|hottest)/.test(q)
  ) {
    return true;
  }
  return false;
}

/**
 * SPEC-103 — advisory strategy questions grounded in approved client understanding.
 * Conceptual families (priority, opportunity, risk, approach, weekly action) —
 * not a long phrase-specific trap list. Must not steal desk/canary/mission turns.
 */
function isOperationalDeskOrMissionRequest(question) {
  const q = String(question || '').trim().toLowerCase();
  if (!q) return false;
  return (
    /\b(canary|fillable\s+(?:verification\s+)?table|verification\s+work\s+order|preparation[-\s]*only)\b/.test(
      q
    ) ||
    /\b(prospect_id|mail\s+readiness|draft\s+readiness|execution\s+readiness)\b/.test(
      q
    ) ||
    /\bbuild\s+campaign\b/.test(q) ||
    /\bmonitor\b/.test(q) ||
    /\bcommand\s+deck\b/.test(q) ||
    /\bpacket\s+review\b/.test(q) ||
    /\bcall\s+script\b/.test(q) ||
    isClientContextExecutionRequest(q)
  );
}

function isClientContextReasoningRequest(question) {
  const q = String(question || '').trim().toLowerCase();
  if (!q) return false;
  if (isEvidenceDependentClientRequest(q)) return false;
  if (isOperationalDeskOrMissionRequest(q)) return false;
  if (looksLikeBusinessUnderstandingAsk(q)) return false;

  // Priority / next-action family — require advisory framing, not "…batch first…"
  const priorityFamily =
    /\bwhat should (we|i) (focus on|do|priorit)/.test(q) ||
    /\bwhat should we focus on\b/.test(q) ||
    /\bfocus on first\b/.test(q) ||
    /\bwhere should we start\b/.test(q) ||
    /\bwhat would you priorit/.test(q) ||
    /\bpriorit(y|ies|ize)\b.{0,24}\b(this week|next|first)\b/.test(q) ||
    /\b(this week|next)\b.{0,24}\bpriorit/.test(q);

  const opportunityFamily =
    /\b(biggest opportunity|main opportunity)\b/.test(q) ||
    /\bwhat.*(opportunity|opportunities)\b/.test(q) ||
    /\bwhere (should|would) we grow\b/.test(q);

  const riskFamily =
    /\b(biggest risks?|what should we avoid|what would you avoid)\b/.test(q) ||
    (/\brisks?\b/.test(q) &&
      /\b(what|biggest|our|main)\b/.test(q) &&
      !/\b(mission|canary|mail|launch)\b/.test(q));

  const approachFamily =
    /\b(how would you approach|what would you test first|does (our |the )?current strategy|make sense)\b/.test(
      q
    ) || /\bapproach growth from here\b/.test(q);

  const advisoryCampaign =
    /\bwhat campaign would you (recommend|suggest)\b/.test(q) ||
    /\bwhat would you recommend\b/.test(q);

  // Do NOT claim bare "Why …?" here — that steals Workspace explain intents
  // (e.g. "Why is Marlowe #1?"). Follow-ups use isClientContextReasoningFollowUp.

  return (
    looksLikeFocusAsk(q) ||
    looksLikeTargetingAsk(q) ||
    looksLikeUnknownsAsk(q) ||
    priorityFamily ||
    opportunityFamily ||
    riskFamily ||
    approachFamily ||
    advisoryCampaign
  );
}

/** Explicit execution intent — must not be answered as advisory advice. */
function isClientContextExecutionRequest(question) {
  const q = String(question || '').trim().toLowerCase();
  if (!q) return false;
  if (
    /^(ok(ay)?|yes|sure|sounds good|go ahead|do it|proceed)\.?$/.test(q)
  ) {
    // Bare agreement is not execution authorization (SPEC-103 §18).
    return false;
  }
  return (
    /\b(launch|execute|send|publish|mail|start)\b.{0,40}\b(campaign|emails?|post|outreach|sequence)\b/.test(
      q
    ) ||
    /\b(launch|execute|send|publish|mail)\s+(that|this|it|them|those|now)\b/.test(
      q
    ) ||
    /\bcreate\s+(the|that|this|a)\s+campaign\b/.test(q) ||
    /\brun\s+(the|that|this)\s+campaign\b/.test(q)
  );
}

function isClientContextReasoningFollowUp(question, session) {
  const q = String(question || '').trim().toLowerCase();
  if (!q) return false;
  const prior =
    (session &&
      session.context &&
      session.context.lastClientIntelligenceTurn) ||
    null;
  if (!prior || !prior.kind) return false;
  const reasoningKinds = new Set([
    'reasoning',
    'focus',
    'targeting',
    'opportunity',
    'unknowns',
    'follow_up',
  ]);
  if (!reasoningKinds.has(String(prior.kind))) return false;
  // Short why-follow-ups, or why-that / instead-of after a CIE recommendation.
  if (/^(why\??|why that\??|why not\??|why instead\??|and why\??|tell me why\??)$/.test(q)) {
    return true;
  }
  return (
    /^(why that|why this|why those|why instead)\b/.test(q) ||
    /\bwhy (that|this|those|commercial|residential)\b/.test(q) ||
    (/\binstead of\b/.test(q) && /^(why|and)\b/.test(q))
  );
}

function looksLikeClientIntelligenceAsk(question, session) {
  if (isClientContextExecutionRequest(question)) return false;
  if (isOperationalDeskOrMissionRequest(question)) return false;
  return (
    looksLikeBusinessUnderstandingAsk(question) ||
    looksLikeTargetingAsk(question) ||
    looksLikeUnknownsAsk(question) ||
    looksLikeFocusAsk(question) ||
    isClientContextReasoningRequest(question) ||
    isEvidenceDependentClientRequest(question) ||
    isClientContextReasoningFollowUp(question, session)
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
      'We should refine a few details before treating more of your business as established fact.'
    );
  }
  return (
    `Here is what I understand from your approved Business Blueprint:\n\n` +
    parts.map((p) => `• ${p}`).join('\n') +
    `\n\nThis is durable understanding from your approved Blueprint — not this chat session.`
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
  const unresolvedIcp =
    !summary.idealCustomers ||
    (summary.unknowns || []).some((u) =>
      /ideal customer|commercial customer|who you want|icp|segment/i.test(
        String(u)
      )
    );

  if (unresolvedIcp && !summary.idealCustomers) {
    const geo = summary.targetMarkets
      ? ` in ${summary.targetMarkets}`
      : '';
    const pref = summary.services || summary.campaignGoals || '';
    let out =
      `You've established direction${geo ? geo : ''}` +
      (pref ? ` (${pref})` : '') +
      `, but we haven't chosen the strongest customer segment yet. ` +
      `I'd make resolving who to target first the next decision rather than prematurely building outreach around one audience.`;
    if (summary.avoidCustomers) {
      out += ` We do know to avoid: ${summary.avoidCustomers}.`;
    }
    return out;
  }

  if (summary.idealCustomers) {
    let out = `Based on your approved business direction, I'd start with ${summary.idealCustomers}.`;
    if (summary.targetMarkets) {
      out += ` Geography: ${summary.targetMarkets}.`;
    }
    if (summary.avoidCustomers) {
      out += ` Avoid: ${summary.avoidCustomers}.`;
    }
    out +=
      ` That follows your approved Blueprint — I do not yet have campaign or market evidence proving this segment will outperform alternatives.`;
    return out;
  }

  return (
    'Your approved Blueprint does not yet spell out ideal customers clearly. ' +
    'That remains an unknown — I will not invent an ICP.'
  );
}

function formatUnknownsAnswer(summary) {
  const lines = [];
  if (summary.unknowns && summary.unknowns.length) {
    lines.push('From your approved Blueprint, these remain unresolved:');
    for (const u of summary.unknowns) lines.push(`• ${u}`);
  }
  const soft = [];
  if (!summary.avoidCustomers) soft.push('who to avoid');
  if (!summary.successMetrics) soft.push('success metrics');
  if (!summary.competitiveAdvantages) soft.push('differentiation');
  if (!summary.idealCustomers) soft.push('ideal customer / ICP');
  if (soft.length && !(summary.unknowns && summary.unknowns.length)) {
    lines.push('These sections are still thin or missing:');
    for (const u of soft) lines.push(`• ${u}`);
  }
  lines.push('');
  lines.push(
    'Separately, I do not yet have enough campaign or market evidence to rank live opportunities. ' +
      'I will keep Blueprint unknowns as unknowns rather than inventing answers.'
  );
  if (!lines.length) {
    return (
      'The approved Blueprint covers the core sections. Remaining uncertainty is operational or evidence-based — not invented business facts.'
    );
  }
  return lines.join('\n');
}

function hasUsefulClientContext(summary) {
  if (!summary || !summary.approved) return false;
  return Boolean(
    summary.identity ||
      summary.services ||
      summary.idealCustomers ||
      summary.targetMarkets ||
      summary.campaignGoals ||
      summary.competitiveAdvantages
  );
}

function pickRelevantFacts(summary, mode) {
  const facts = [];
  const push = (label, value) => {
    if (value) facts.push({ label, value: String(value).trim() });
  };
  if (mode === 'targeting' || mode === 'opportunity' || mode === 'focus') {
    push('ICP', summary.idealCustomers);
    push('Market', summary.targetMarkets);
    push('Services', summary.services);
    push('Preference / goals', summary.campaignGoals);
    push('Avoid', summary.avoidCustomers);
    push('Success metrics', summary.successMetrics);
  } else if (mode === 'week') {
    push('Goals', summary.campaignGoals);
    push('ICP', summary.idealCustomers);
    push('Metrics', summary.successMetrics);
    push('Market', summary.targetMarkets);
  } else if (mode === 'risk') {
    push('Avoid', summary.avoidCustomers);
    push('ICP', summary.idealCustomers);
    push('Goals', summary.campaignGoals);
    push('Differentiation', summary.competitiveAdvantages);
  } else {
    push('Identity', summary.identity);
    push('Services', summary.services);
    push('ICP', summary.idealCustomers);
    push('Market', summary.targetMarkets);
    push('Goals', summary.campaignGoals);
    push('Differentiation', summary.competitiveAdvantages);
    push('Metrics', summary.successMetrics);
  }
  return facts;
}

function inferReasoningMode(question, session) {
  const q = String(question || '').trim().toLowerCase();
  if (isClientContextReasoningFollowUp(q, session)) return 'follow_up';
  if (looksLikeUnknownsAsk(q)) return 'unknowns';
  if (looksLikeTargetingAsk(q)) return 'targeting';
  if (
    /\b(this week|do this week|what should i do)\b/.test(q) &&
    !/\bfocus on first\b/.test(q)
  ) {
    return 'week';
  }
  if (/\b(opportunity|opportunities|grow)\b/.test(q)) return 'opportunity';
  if (/\b(risk|avoid)\b/.test(q)) return 'risk';
  if (
    /\bcampaign would you (recommend|suggest)\b/.test(q) ||
    /\bwhat would you recommend\b/.test(q)
  ) {
    return 'campaign_advisory';
  }
  if (/\b(test first|approach|strategy make sense|from here)\b/.test(q)) {
    return 'approach';
  }
  return 'focus';
}

/**
 * SPEC-103 — synthesize a bounded recommendation from approved client facts.
 * Level 1 (Blueprint) → Level 3 (Max inference). Does not invent Level 2 evidence.
 */
function composeClientContextReasoning(summary, question, opts = {}) {
  const mode = opts.mode || inferReasoningMode(question, opts.session);
  const prior =
    (opts.session &&
      opts.session.context &&
      opts.session.context.lastClientIntelligenceTurn) ||
    null;
  const facts = pickRelevantFacts(summary, mode === 'follow_up' ? 'focus' : mode);
  const icp = summary.idealCustomers || null;
  const market = summary.targetMarkets || null;
  const goals = summary.campaignGoals || null;
  const metrics = summary.successMetrics || null;
  const services = summary.services || null;
  const avoid = summary.avoidCustomers || null;
  const unknowns = summary.unknowns || [];
  const unresolvedIcp =
    !icp ||
    unknowns.some((u) =>
      /ideal customer|commercial customer|who you want|icp|segment/i.test(
        String(u)
      )
    );

  if (mode === 'unknowns') {
    return {
      prose: formatUnknownsAnswer(summary),
      kind: 'unknowns',
      confidenceLabel: 'high',
      confidence: 0.9,
    };
  }

  if (mode === 'targeting') {
    return {
      prose: formatTargetingAnswer(summary),
      kind: 'targeting',
      confidenceLabel: unresolvedIcp ? 'low' : 'moderate',
      confidence: unresolvedIcp ? 0.55 : 0.78,
    };
  }

  if (mode === 'follow_up') {
    const focusBit =
      (prior && prior.recommendationFocus) ||
      icp ||
      goals ||
      'that direction';
    const whyParts = [];
    whyParts.push(
      `I'd start with ${focusBit} because it follows what you've already approved about the business.`
    );
    if (icp) whyParts.push(`Your approved ICP points to ${icp}.`);
    if (services && /commercial/i.test(services + ' ' + (goals || ''))) {
      whyParts.push(
        'That aligns with a commercial preference rather than spreading effort across residential by default.'
      );
    }
    if (market) whyParts.push(`Geography stays anchored to ${market}.`);
    if (goals) whyParts.push(`The outcome that matters in your Blueprint is: ${goals}.`);
    whyParts.push(
      'This is Max reasoning from approved understanding — not observed market or campaign performance. ' +
        'If residential starts producing stronger walkthroughs or recurring revenue later, we should revise with evidence.'
    );
    return {
      prose: whyParts.join(' '),
      kind: 'follow_up',
      confidenceLabel: 'moderate',
      confidence: 0.72,
      recommendationFocus: focusBit,
    };
  }

  if (mode === 'risk') {
    const parts = [];
    if (avoid) {
      parts.push(
        `Based on your approved Blueprint, I'd actively avoid ${avoid}.`
      );
    } else {
      parts.push(
        'Your approved Blueprint does not yet list clear avoid-segments, so I will not invent disqualifiers.'
      );
    }
    parts.push(
      'I would also avoid narrowing to a micro-segment based on assumption alone until we have response evidence. ' +
        'Spreading across every possible audience at once is the other risk — it weakens learning.'
    );
    return {
      prose: parts.join(' '),
      kind: 'reasoning',
      confidenceLabel: 'moderate',
      confidence: 0.7,
    };
  }

  // Default synthesis: focus / opportunity / week / approach / campaign advisory
  if (unresolvedIcp && !icp) {
    const geo = market ? ` in ${market}` : '';
    return {
      prose:
        `You've clarified enough${geo} to know you want a tighter commercial motion, ` +
        `but the strongest customer segment is still unresolved. ` +
        `I'd make choosing and testing that segment the first focus — not launching a broad campaign yet. ` +
        `Once the ICP is clear, we can measure a simple loop: qualified prospects → conversations → walkthroughs → recurring revenue.`,
      kind: mode === 'opportunity' ? 'opportunity' : 'focus',
      confidenceLabel: 'moderate',
      confidence: 0.65,
      recommendationFocus: 'resolve commercial ICP before scaling outreach',
    };
  }

  const audience = icp || 'your approved ideal customers';
  const where = market ? ` in ${market}` : '';
  const outcome =
    metrics ||
    goals ||
    'walkthroughs and recurring revenue';
  const commercialCue = /commercial|property|facility|multifamily|apartment/i.test(
    [icp, services, goals, summary.competitiveAdvantages]
      .filter(Boolean)
      .join(' ')
  );

  const paragraphs = [];
  if (mode === 'opportunity') {
    paragraphs.push(
      `Your biggest near-term opportunity looks like proving a repeatable acquisition motion around ${audience}${where}.`
    );
  } else if (mode === 'week') {
    paragraphs.push(
      `This week, I'd concentrate on a small, qualified set of ${audience}${where} and set up a measurement loop — not a full-scale launch.`
    );
  } else if (mode === 'campaign_advisory') {
    paragraphs.push(
      `I'd recommend a focused first campaign toward ${audience}${where}, designed as a learning loop rather than a broad blast.`
    );
  } else if (mode === 'approach') {
    paragraphs.push(
      `From here, I'd approach growth by proving one acquisition motion around ${audience}${where} before expanding.`
    );
  } else {
    paragraphs.push(
      `I'd start by proving a repeatable acquisition motion around ${audience}${where}.`
    );
  }

  const clarityBits = [];
  if (commercialCue) clarityBits.push('you prefer commercial work');
  if (icp) clarityBits.push('you already identified the decision-makers you want to reach');
  if (goals || metrics) clarityBits.push('recurring revenue / walkthrough outcomes are what matter');
  if (clarityBits.length) {
    paragraphs.push(
      `You already have enough clarity to narrow the first experiment: ${clarityBits.join(', ')}.`
    );
  }

  paragraphs.push(
    `What we don't know yet is which part of that market will respond best or produce the strongest contracts. ` +
      `I wouldn't narrow further based on assumption alone.`
  );

  paragraphs.push(
    `I'd start with a qualified group of ${audience}, measure whether that creates progress toward ${outcome}, ` +
      `then track how many become recurring clients. ` +
      `That gives a useful first loop: qualified prospects → conversations → walkthroughs → recurring revenue.`
  );

  paragraphs.push(
    `My confidence is moderate because this direction follows your approved business priorities, ` +
      `but we don't have enough campaign or market evidence yet to know whether this segment will outperform alternatives. ` +
      `This is advisory guidance — not authorization to launch.`
  );

  return {
    prose: paragraphs.join('\n\n'),
    kind: mode === 'opportunity' ? 'opportunity' : 'reasoning',
    confidenceLabel: 'moderate',
    confidence: 0.74,
    recommendationFocus: audience,
    factsUsed: facts,
  };
}

function formatEvidenceDependentGapAnswer(summary) {
  const audience = summary && summary.idealCustomers
    ? summary.idealCustomers
    : 'your target audience';
  const market = summary && summary.targetMarkets ? summary.targetMarkets : 'your market';
  return (
    `I can use your approved Blueprint to say who you want to reach (${audience} in ${market}), ` +
    `but I do not yet have enough live market or prospect evidence to rank specific companies or buying signals right now. ` +
    `I will not invent names, signals, or performance claims. ` +
    `Once Market Intelligence or campaign results are available for this client, I can refine this with observed evidence.`
  );
}

function formatFocusAnswer(summary, attachment) {
  const composed = composeClientContextReasoning(summary, 'What should we focus on first?', {
    mode: 'week',
  });
  if (attachment && attachment.clientIntelligence && attachment.clientIntelligence.playbookPending) {
    return (
      composed.prose +
      '\n\nStrategy recommendations from a pending Playbook are not approved execution.'
    );
  }
  return composed.prose;
}

function recordLastCieTurn(session, turn) {
  if (!session || !session.context || typeof session.context !== 'object') return;
  session.context.lastClientIntelligenceTurn = {
    kind: turn.kind || 'reasoning',
    reason: turn.reason || null,
    recommendationFocus: turn.recommendationFocus || null,
    question: turn.question || null,
    at: new Date().toISOString(),
  };
}

/**
 * Pre-routing CIE handler. Returns a workspace result when it fully handles
 * the turn; otherwise returns handled:false with context already attached.
 *
 * SPEC-098: recall / continuity
 * SPEC-103: client-context business reasoning (advisory only)
 */
async function maybeHandleClientIntelligenceTurn(input = {}) {
  const question = String(input.question || '').trim();
  if (!question) return null;

  const session = input.session || null;
  const pre = await attachClientIntelligenceContext(input);
  const summary = pre.summary;
  const attachment = pre.attachment;

  // Never intercept explicit execution — Missions / review path owns it.
  if (isClientContextExecutionRequest(question)) {
    return {
      handled: false,
      summary,
      attachment,
      clientId: pre.clientId,
      skipReason: 'execution_intent',
    };
  }

  // Never intercept canary / desk / mission operational turns.
  if (isOperationalDeskOrMissionRequest(question)) {
    return {
      handled: false,
      summary,
      attachment,
      clientId: pre.clientId,
      skipReason: 'operational_desk_or_mission',
    };
  }

  if (!looksLikeClientIntelligenceAsk(question, session)) {
    return {
      handled: false,
      summary,
      attachment,
      clientId: pre.clientId,
    };
  }

  if (!summary || !summary.approved) {
    const prose = formatMissingAnswer();
    const structured = workspaceStructured(
      prose,
      [],
      {
        unavailable: ['approved_blueprint'],
        clientFacingReasoning: [],
        internalReasoning: [
          'No approved Business Blueprint for this authenticated client.',
          'Fail closed — do not invent client facts (SPEC-098/103).',
        ],
        confidence: 0.95,
      }
    );
    return {
      reason: 'client_intelligence_missing',
      handled: true,
      prose,
      structured,
      summary: null,
      attachment,
      turnKind: 'missing',
    };
  }

  // Evidence-dependent: Blueprint alone cannot invent live signals.
  if (isEvidenceDependentClientRequest(question)) {
    const prose = formatEvidenceDependentGapAnswer(summary);
    const structured = workspaceStructured(prose, [], {
      blueprintId: summary.blueprintId,
      evidenceCount: 1,
      unavailable: ['market_intelligence', 'live_buying_signals'],
      evidenceBasis: 'approved_client_understanding_insufficient_for_request',
      recommendationConfidence: 'n/a',
      confidence: 0.9,
      clientFacingReasoning: [],
      internalReasoning: [
        'Evidence-dependent question; approved Blueprint cannot supply live company/signal ranking.',
      ],
      supportingEvidence: [
        {
          id: summary.blueprintId || 'blueprint',
          label: 'Approved Business Blueprint',
          detail: summary.identity || 'Approved client understanding',
        },
      ],
      nextInvestigations: [
        'When Market Intelligence is available, ask which accounts show buying signals.',
      ],
    });
    recordLastCieTurn(session, {
      kind: 'evidence_gap',
      reason: 'client_intelligence_evidence_dependent',
      question,
    });
    return {
      reason: 'client_intelligence_evidence_dependent',
      handled: true,
      prose,
      structured,
      summary,
      attachment,
      turnKind: 'evidence_gap',
    };
  }

  let prose;
  let reason = 'client_intelligence_context';
  let turnKind = 'context';
  let recommendationFocus = null;
  let confidence = 0.88;
  let confidenceLabel = null;

  if (looksLikeBusinessUnderstandingAsk(question)) {
    prose = formatUnderstandingAnswer(summary);
    reason = 'client_intelligence_understanding';
    turnKind = 'understanding';
  } else if (
    isClientContextReasoningRequest(question) ||
    isClientContextReasoningFollowUp(question, session) ||
    looksLikeFocusAsk(question) ||
    looksLikeTargetingAsk(question) ||
    looksLikeUnknownsAsk(question)
  ) {
    if (!hasUsefulClientContext(summary)) {
      prose =
        'Your approved Blueprint is on file but too thin to support a useful recommendation yet. ' +
        'I would refine the core sections before prioritizing next moves.';
      reason = 'client_intelligence_reasoning_thin';
      turnKind = 'reasoning';
      confidence = 0.5;
      confidenceLabel = 'low';
    } else {
      const composed = composeClientContextReasoning(summary, question, {
        session,
      });
      prose = composed.prose;
      turnKind = composed.kind || 'reasoning';
      recommendationFocus = composed.recommendationFocus || null;
      confidence = composed.confidence != null ? composed.confidence : 0.74;
      confidenceLabel = composed.confidenceLabel || 'moderate';
      if (turnKind === 'understanding') {
        reason = 'client_intelligence_understanding';
      } else if (turnKind === 'targeting') {
        reason = 'client_intelligence_targeting';
      } else if (turnKind === 'unknowns') {
        reason = 'client_intelligence_unknowns';
      } else if (turnKind === 'follow_up') {
        reason = 'client_intelligence_reasoning_follow_up';
      } else if (turnKind === 'opportunity') {
        reason = 'client_intelligence_opportunity';
      } else {
        reason = 'client_intelligence_reasoning';
      }
    }
  } else {
    // Should not reach — looksLikeClientIntelligenceAsk already gated.
    prose = formatFocusAnswer(summary, attachment);
    reason = 'client_intelligence_focus';
    turnKind = 'focus';
  }

  const structured = workspaceStructured(prose, [], {
    blueprintId: summary.blueprintId,
    evidenceCount: 1,
    confidence,
    recommendationConfidence: confidenceLabel,
    evidenceBasis: 'approved_client_understanding',
    clientFacingReasoning: [],
    internalReasoning: [
      'Answer grounded in the most recently approved Business Blueprint.',
      'Level 3 Max inference — not newly established client fact.',
      'No autonomous execution from advisory reasoning (SPEC-103).',
    ],
    supportingEvidence: [
      {
        id: summary.blueprintId || 'blueprint',
        label: 'Approved Business Blueprint',
        detail: summary.identity || 'Approved client understanding',
      },
    ],
    nextInvestigations:
      turnKind === 'understanding'
        ? ['What should we focus on first?']
        : turnKind === 'evidence_gap'
          ? []
          : ['What do we still not know?', 'Why that direction?'],
  });

  recordLastCieTurn(session, {
    kind: turnKind,
    reason,
    recommendationFocus,
    question,
  });

  return {
    reason,
    handled: true,
    prose,
    structured,
    summary,
    attachment,
    turnKind,
    recommendationFocus,
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
  isClientContextReasoningRequest,
  isEvidenceDependentClientRequest,
  isClientContextExecutionRequest,
  isClientContextReasoningFollowUp,
  isOperationalDeskOrMissionRequest,
  composeClientContextReasoning,
  formatUnderstandingAnswer,
  formatMissingAnswer,
  formatUnknownsAnswer,
  formatTargetingAnswer,
  formatEvidenceDependentGapAnswer,
  sanitizeFactSummary,
  hasUsefulClientContext,
};
