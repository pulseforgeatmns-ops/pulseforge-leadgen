'use strict';

/**
 * SPEC-098 — Max Workspace thin adapter for approved Client Intelligence.
 * SPEC-103 — Client-context business reasoning from approved understanding.
 * SPEC-103A — Semantic context for reasoning (not nested Blueprint prose).
 * SPEC-103B — Semantic client-business routing (general conversation is fallback).
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

/** Known Blueprint section wrapper prefixes — strip when peeling summaries. */
const BLUEPRINT_WRAPPER_PREFIXES = [
  /^Today the business delivers\s+/i,
  /^Ideal customers are\s+/i,
  /^Ideal customers include\s+/i,
  /^The business prefers to avoid\s+/i,
  /^Priority markets center on\s+/i,
  /^Geographic focus centers on\s+/i,
  /^Competitive edge is described as\s+/i,
  /^Brand voice should read as\s+/i,
  /^Near-term growth goals focus on\s+/i,
  /^Success will be judged by\s+/i,
  /^Progress will be judged by\s+/i,
  /^The business is understood as\s+/i,
];

/** Second-sentence Blueprint architecture narration — never feed to Max prose. */
const BLUEPRINT_NARRATION_RE =
  /\b(This identity framing|Service understanding reflects|This ICP picture|Geography and vertical focus|These constraints protect|operator-stated differentiation|Tone guidance constrains|These are desired business outcomes|These signals define whether)/i;

function defaultCieService() {
  return cie;
}

function presentText(text) {
  const raw = String(text || '').trim();
  if (!raw) return '';
  if (typeof cie.normalizeMechanicalTypos === 'function') {
    return cie.normalizeMechanicalTypos(raw).replace(/\s{2,}/g, ' ').trim();
  }
  return raw
    .replace(/\bcreateed\b/gi, 'created')
    .replace(/\bcommeercial\b/gi, 'commercial')
    .replace(/\bcommerical\b/gi, 'commercial')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function midSentencePhrase(text) {
  const s = presentText(text);
  if (!s) return '';
  if (/^[A-Z]{2,}(?:\b|[0-9])/.test(s)) return s;
  if (/^Greater\s+/i.test(s)) return s;
  return s.charAt(0).toLowerCase() + s.slice(1);
}

function joinNatural(items) {
  const list = (items || [])
    .map((item) => presentText(String(item || '').replace(/[.]+$/, '')))
    .filter(Boolean);
  if (!list.length) return '';
  if (typeof cie.formatDecisionMakerProse === 'function' && list.length <= 3) {
    return cie.formatDecisionMakerProse(list);
  }
  if (list.length === 1) return list[0];
  if (list.length === 2) return `${list[0]} and ${list[1]}`;
  return `${list.slice(0, -1).join(', ')}, and ${list[list.length - 1]}`;
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

/**
 * Peel precomposed Blueprint prose down to semantic substance.
 * Compatibility fallback only when normalizedFacts are unavailable.
 */
function peelBlueprintSubstance(sectionKey, summary) {
  let s = sanitizeFactSummary(summary);
  if (!s) return '';

  // Keep the first non-narration sentence.
  const sentences = s
    .split(/(?<=[.!?])\s+/)
    .map((part) => String(part || '').trim())
    .filter(Boolean)
    .filter((part) => !BLUEPRINT_NARRATION_RE.test(part));
  s = sentences[0] || '';
  if (!s) return '';

  for (const re of BLUEPRINT_WRAPPER_PREFIXES) {
    s = s.replace(re, '');
  }

  // Geography: drop growth-focus clause — that is preference, not place.
  if (sectionKey === 'targetMarkets') {
    s = s.replace(/\s+with a near-term growth focus on\s+.+$/i, '');
  }

  // Identity: keep name + short description without framing wrappers.
  if (sectionKey === 'identity') {
    s = s.replace(/\s+This identity framing[\s\S]*$/i, '');
  }

  s = presentText(s.replace(/[.!?]+$/, '').trim());
  return sanitizeFactSummary(s);
}

function splitListSubstance(text) {
  const s = presentText(text);
  if (!s) return [];
  return s
    .split(/\s*,\s*|\s+and\s+/i)
    .map((part) => presentText(part.replace(/[.]+$/, '')))
    .filter(Boolean);
}

function goalPhraseFromFacts(facts) {
  const raw =
    (facts && (facts.ninety_day_outcomes || facts.growth_focus)) || '';
  if (!raw) return '';
  if (typeof cie.normalizeGoalOutcomePhrase === 'function') {
    return presentText(cie.normalizeGoalOutcomePhrase(raw));
  }
  return presentText(raw);
}

/**
 * Build Max reasoning/recall context from structured normalizedFacts.
 * These are semantic values — not rendered Blueprint explanations.
 */
function semanticFieldsFromNormalizedFacts(facts) {
  if (!facts || typeof facts !== 'object') return null;

  const businessName = presentText(facts.business_name || '') || null;
  const description = presentText(
    String(facts.business_description || '').replace(/[.]+$/, '')
  );
  const identity = businessName
    ? description
      ? `${businessName} — ${description}`
      : businessName
    : description || '';

  const serviceList = (facts.services || [])
    .map((item) => presentText(item))
    .filter(Boolean);
  const idealList = (facts.ideal_customers || [])
    .map((item) => presentText(item))
    .filter(Boolean);
  const avoidList = (facts.disqualified_customers || [])
    .map((item) => presentText(String(item || '').replace(/[.]+$/, '')))
    .filter(Boolean);
  const geoList = (facts.geography || [])
    .map((item) => presentText(item))
    .filter(Boolean);
  const metricList = (facts.success_metrics || [])
    .map((item) => presentText(String(item || '').replace(/[.]+$/, '')))
    .filter(Boolean);

  const growthFocus = presentText(facts.growth_focus || '');
  const verticalFocus = presentText(facts.vertical_focus || '');
  const commercialPreference = Boolean(
    /commercial/i.test(growthFocus) ||
      /commercial/i.test(verticalFocus) ||
      /prefer commercial|commercial/i.test(description)
  );

  const differentiation = presentText(
    String(facts.differentiation || '').replace(/[.]+$/, '')
  );
  const brandVoice = presentText(
    String(facts.brand_voice || '').replace(/[.]+$/, '')
  );

  return {
    businessName,
    identity: sanitizeFactSummary(identity),
    services: sanitizeFactSummary(joinNatural(serviceList)),
    serviceList,
    idealCustomers: sanitizeFactSummary(joinNatural(idealList)),
    idealCustomerList: idealList,
    avoidCustomers: sanitizeFactSummary(joinNatural(avoidList)),
    targetMarkets: sanitizeFactSummary(joinNatural(geoList)),
    geography: sanitizeFactSummary(joinNatural(geoList)),
    growthFocus: sanitizeFactSummary(growthFocus),
    commercialPreference,
    competitiveAdvantages: sanitizeFactSummary(differentiation),
    brandVoice: sanitizeFactSummary(brandVoice),
    campaignGoals: sanitizeFactSummary(goalPhraseFromFacts(facts)),
    successMetrics: sanitizeFactSummary(joinNatural(metricList)),
    successMetricList: metricList,
    semanticSource: 'normalized_facts',
  };
}

/**
 * Compatibility fallback: peel section.summary wrappers into substance.
 * Used only when historical Blueprints lack normalizedFacts.
 */
function semanticFieldsFromSections(sections) {
  const identity = peelBlueprintSubstance(
    'identity',
    sectionSummary(sections, 'identity')
  );
  const services = peelBlueprintSubstance(
    'services',
    sectionSummary(sections, 'services')
  );
  const idealCustomers = peelBlueprintSubstance(
    'idealCustomers',
    sectionSummary(sections, 'idealCustomers')
  );
  const avoidCustomers = peelBlueprintSubstance(
    'avoidCustomers',
    sectionSummary(sections, 'avoidCustomers')
  );
  const targetMarkets = peelBlueprintSubstance(
    'targetMarkets',
    sectionSummary(sections, 'targetMarkets')
  );
  const competitiveAdvantages = peelBlueprintSubstance(
    'competitiveAdvantages',
    sectionSummary(sections, 'competitiveAdvantages')
  );
  const brandVoice = peelBlueprintSubstance(
    'brandVoice',
    sectionSummary(sections, 'brandVoice')
  );
  let campaignGoals = peelBlueprintSubstance(
    'campaignGoals',
    sectionSummary(sections, 'campaignGoals')
  );
  if (campaignGoals && typeof cie.normalizeGoalOutcomePhrase === 'function') {
    campaignGoals = presentText(cie.normalizeGoalOutcomePhrase(campaignGoals));
  }
  const successMetrics = peelBlueprintSubstance(
    'successMetrics',
    sectionSummary(sections, 'successMetrics')
  );

  const marketsRaw = sectionSummary(sections, 'targetMarkets');
  const growthFocusMatch = String(marketsRaw || '').match(
    /near-term growth focus on\s+(.+?)(?:\.|$)/i
  );
  const growthFocus = growthFocusMatch
    ? presentText(growthFocusMatch[1])
    : '';
  const commercialPreference = Boolean(
    /commercial/i.test(growthFocus) ||
      /commercial/i.test(identity) ||
      /commercial/i.test(services) ||
      /commercial/i.test(campaignGoals)
  );

  let businessName = null;
  if (identity) {
    const dashParts = identity.split(/\s+[—–]\s+/);
    if (dashParts.length > 1) {
      businessName = presentText(dashParts[0]);
    } else {
      const named = identity.match(
        /^([A-Z][\w&'.]*(?:\s+[A-Z][\w&'.]*){0,5})\b/
      );
      if (named) businessName = presentText(named[1]);
    }
  }

  return {
    businessName,
    identity,
    services,
    serviceList: splitListSubstance(services),
    idealCustomers,
    idealCustomerList: splitListSubstance(idealCustomers),
    avoidCustomers,
    targetMarkets,
    geography: targetMarkets,
    growthFocus,
    commercialPreference,
    competitiveAdvantages,
    brandVoice,
    campaignGoals,
    successMetrics,
    successMetricList: splitListSubstance(successMetrics),
    semanticSource: 'peeled_sections',
  };
}

function collectUnknowns(sections, semantic) {
  const unknowns = [];
  for (const [key, label] of [
    ['identity', 'who you are'],
    ['services', 'what you offer'],
    ['idealCustomers', 'who you want to serve'],
    ['targetMarkets', 'where you operate'],
    ['campaignGoals', 'what you want next'],
  ]) {
    const hasSemantic =
      (key === 'identity' && semantic.identity) ||
      (key === 'services' && semantic.services) ||
      (key === 'idealCustomers' && semantic.idealCustomers) ||
      (key === 'targetMarkets' && semantic.targetMarkets) ||
      (key === 'campaignGoals' && semantic.campaignGoals);
    if (!hasSemantic) unknowns.push(label);
  }
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
  return unknowns;
}

function normalizeBlueprintSummary(blueprint) {
  if (!blueprint || typeof blueprint !== 'object') return null;
  const sections = blueprint.sections || {};
  const facts =
    blueprint.normalizedFacts ||
    blueprint.normalized_facts ||
    null;
  const semantic =
    semanticFieldsFromNormalizedFacts(facts) ||
    semanticFieldsFromSections(sections);

  const unknowns = collectUnknowns(sections, semantic);
  const confidence = blueprint.confidenceSummary || null;

  return {
    blueprintId: blueprint.id || null,
    sessionId: blueprint.sessionId || blueprint.session_id || null,
    clientId: blueprint.clientId != null ? blueprint.clientId : blueprint.client_id,
    version: blueprint.version || null,
    status: blueprint.status || null,
    approved: String(blueprint.status || '').toLowerCase() === 'approved',
    businessName: semantic.businessName || null,
    identity: semantic.identity,
    services: semantic.services,
    serviceList: semantic.serviceList || [],
    idealCustomers: semantic.idealCustomers,
    idealCustomerList: semantic.idealCustomerList || [],
    avoidCustomers: semantic.avoidCustomers,
    targetMarkets: semantic.targetMarkets,
    geography: semantic.geography || semantic.targetMarkets,
    growthFocus: semantic.growthFocus || '',
    commercialPreference: Boolean(semantic.commercialPreference),
    competitiveAdvantages: semantic.competitiveAdvantages,
    brandVoice: semantic.brandVoice,
    campaignGoals: semantic.campaignGoals,
    successMetrics: semantic.successMetrics,
    successMetricList: semantic.successMetricList || [],
    semanticSource: semantic.semanticSource || null,
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

  // SPEC-103A — ensure structured facts are available for semantic reasoning.
  if (
    blueprint &&
    !blueprint.normalizedFacts &&
    !blueprint.normalized_facts
  ) {
    try {
      const sessionId = blueprint.sessionId || blueprint.session_id;
      const store = input.cieOpts && input.cieOpts.store;
      if (sessionId && store && typeof store.getSession === 'function') {
        const session = await store.getSession(sessionId);
        const facts =
          session &&
          session.interview_state &&
          session.interview_state.normalizedFacts;
        if (facts) blueprint.normalizedFacts = facts;
      }
    } catch (_) {
      /* fail soft — peel fallback */
    }
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

/**
 * SPEC-103B — normalize client chat text before semantic scoring.
 * Handles curly apostrophes, light typo collapse, and casing — not phrase traps.
 */
const COMMON_UTTERANCE_LEXICON = Object.freeze([
  'are',
  'here',
  'there',
  'where',
  'what',
  'when',
  'why',
  'who',
  'how',
  'we',
  'you',
  'our',
  'my',
  'us',
  'me',
  'that',
  'this',
  'those',
  'them',
  'then',
  'than',
  'with',
  'from',
  'into',
  'about',
  'after',
  'before',
  'missing',
  'know',
  'known',
  'focus',
  'risk',
  'holes',
  'hole',
  'gaps',
  'gap',
  'weak',
  'worry',
  'concern',
  'business',
  'company',
  'strategy',
  'next',
  'first',
  'start',
  'learn',
  'sure',
  'anything',
  'should',
  'would',
  'could',
  'dont',
  "don't",
  'yet',
  'still',
  'need',
  'needs',
  'money',
  'else',
  'now',
  'okay',
  'alright',
]);

function editDistanceOne(a, b) {
  if (a === b) return true;
  const la = a.length;
  const lb = b.length;
  if (Math.abs(la - lb) > 1) return false;
  if (la === lb) {
    let diffs = 0;
    for (let i = 0; i < la; i += 1) {
      if (a[i] !== b[i]) {
        diffs += 1;
        if (diffs > 1) return false;
      }
    }
    return diffs === 1;
  }
  const shorter = la < lb ? a : b;
  const longer = la < lb ? b : a;
  let i = 0;
  let j = 0;
  let skipped = false;
  while (i < shorter.length && j < longer.length) {
    if (shorter[i] === longer[j]) {
      i += 1;
      j += 1;
    } else if (!skipped) {
      skipped = true;
      j += 1;
    } else {
      return false;
    }
  }
  return true;
}

function looksTypoLike(token) {
  return /(.)\1/.test(token) || token.length >= 6;
}

function repairUtteranceToken(token) {
  if (!token || token.length < 3) return token;
  if (COMMON_UTTERANCE_LEXICON.includes(token)) return token;
  // Only fuzzy-repair tokens that look typo-ish (doubled letters / long junk).
  // Prevents "that"→"what" style damage on clean closed-class words.
  if (!looksTypoLike(token)) return token;
  for (const word of COMMON_UTTERANCE_LEXICON) {
    if (editDistanceOne(token, word)) return word;
  }
  let current = token;
  for (let guard = 0; guard < 4; guard += 1) {
    let next = null;
    for (let i = 0; i < current.length - 1; i += 1) {
      if (current[i] === current[i + 1]) {
        next = `${current.slice(0, i)}${current.slice(i + 1)}`;
        break;
      }
    }
    if (!next || next === current) break;
    current = next;
    if (COMMON_UTTERANCE_LEXICON.includes(current)) return current;
    for (const word of COMMON_UTTERANCE_LEXICON) {
      if (editDistanceOne(current, word)) return word;
    }
  }
  return token;
}

function normalizeClientUtterance(question) {
  const base = String(question || '')
    .normalize('NFKC')
    .replace(/[\u2018\u2019\u2032\u00B4]/g, "'")
    .replace(/[\u201C\u201D]/g, '"')
    .toLowerCase()
    // Triple+ repeats → double (misssing → missing) without erasing real doubles.
    .replace(/([a-z])\1{2,}/g, '$1$1')
    .replace(/\s+/g, ' ')
    .trim();
  if (!base) return base;
  return base
    .split(' ')
    .map((tok) => {
      const m = tok.match(/^([^a-z0-9']*)([a-z0-9']+)([^a-z0-9']*)$/);
      if (!m) return repairUtteranceToken(tok);
      return `${m[1]}${repairUtteranceToken(m[2])}${m[3]}`;
    })
    .join(' ');
}

function tokenizeClientUtterance(normalized) {
  return String(normalized || '')
    .replace(/[^a-z0-9'\s]/g, ' ')
    .split(/\s+/)
    .filter(Boolean);
}

function lightStem(token) {
  let t = String(token || '');
  if (t.length <= 3) return t;
  t = t.replace(/'s$/, '');
  if (t.endsWith('ies') && t.length > 4) return `${t.slice(0, -3)}y`;
  if (t.endsWith('ing') && t.length > 5) return t.slice(0, -3);
  if (t.endsWith('ed') && t.length > 4) return t.slice(0, -2);
  if (t.endsWith('es') && t.length > 4) return t.slice(0, -2);
  if (t.endsWith('s') && t.length > 3 && !t.endsWith('ss')) return t.slice(0, -1);
  return t;
}

/**
 * Concept families as scoring features (stems/prefixes), not allowlisted phrases.
 * Unseen paraphrases generalize by hitting shared stems + discourse frames.
 */
const CLIENT_BUSINESS_CONCEPTS = Object.freeze({
  gap: [
    'unknown',
    'unclear',
    'unsure',
    'uncertain',
    'missing',
    'miss',
    'gap',
    'hole',
    'overlook',
    'assum',
    'learn',
    'figur',
    'investig',
    'evidenc',
    'blind',
  ],
  priority: [
    'focus',
    'priorit',
    'next',
    'start',
    'first',
    'now',
    'begin',
    'direction',
    'move',
  ],
  risk: [
    'risk',
    'weak',
    'worr',
    'concern',
    'fail',
    'avoid',
    'hole',
    'threat',
    'expos',
  ],
  strategy: [
    'strateg',
    'approach',
    'position',
    'growth',
    'acqui',
    'target',
    'opportunit',
    'tradeoff',
    'decision',
    'experiment',
    'campaign',
    'segment',
    'audience',
    'customer',
    'service',
    'metric',
    'goal',
    'constraint',
    'commercial',
    'residential',
    'pipeline',
    'walkthrough',
  ],
  owner: [
    'business',
    'company',
    'thinking',
    'recommend',
    'suggest',
    'advice',
  ],
});

function conceptFamilyHits(tokens, family) {
  const stems = tokens.map(lightStem);
  const joined = stems.join(' ');
  let hits = 0;
  for (const prefix of CLIENT_BUSINESS_CONCEPTS[family] || []) {
    if (stems.some((s) => s.startsWith(prefix) || prefix.startsWith(s))) {
      hits += 1;
      continue;
    }
    if (joined.includes(prefix)) hits += 1;
  }
  return hits;
}

function hasPriorClientReasoning(session) {
  const prior =
    (session &&
      session.context &&
      session.context.lastClientIntelligenceTurn) ||
    null;
  if (!prior || !prior.kind) return false;
  return [
    'reasoning',
    'focus',
    'targeting',
    'opportunity',
    'unknowns',
    'follow_up',
    'clarification',
    'context',
    'understanding',
    'evidence_gap',
  ].includes(String(prior.kind));
}

/**
 * SPEC-103B primary classifier — semantic multi-feature score.
 * Returns structured intent for routing; not a phrase matcher.
 */
function scoreClientBusinessSemantics(question, session) {
  const q = normalizeClientUtterance(question);
  const tokens = tokenizeClientUtterance(q);
  const tokenSet = new Set(tokens);
  const features = {
    unrelated: false,
    execution: false,
    executionAdjacent: false,
    evidence: false,
    understanding: false,
    followUp: false,
    referentAmbiguous: false,
    gap: 0,
    priority: 0,
    risk: 0,
    strategy: 0,
    owner: 0,
    discourse: 0,
    advisory: 0,
  };

  if (!q) {
    return { q, tokens, score: 0, mode: null, features, isClientBusiness: false };
  }

  // Non-business vetoes — do not force Blueprint reasoning.
  if (
    /\bwhat time is it\b/.test(q) ||
    /\b(weather|temperature)\b/.test(q) ||
    /^\s*what('s| is)\s+\d+\s*(times|x|\*|plus|\+|minus|-)\s*\d+/.test(q) ||
    /\b\d+\s*(times|x|\*)\s*\d+\b/.test(q)
  ) {
    features.unrelated = true;
    return { q, tokens, score: 0, mode: null, features, isClientBusiness: false };
  }

  if (isClientContextExecutionRequest(q)) {
    features.execution = true;
    return { q, tokens, score: 0, mode: null, features, isClientBusiness: false };
  }

  // Ambiguous agreement / consequential action — fail closed, do not advise-as-execute.
  if (
    /\b(let'?s|lets)\s+(go after|hit|launch|start|run|send|do)\b/.test(q) ||
    /\b(alright|all right|ok(ay)?),?\s+(let'?s|lets)\b/.test(q) ||
    /\bgo after (them|it|that|those)\b/.test(q)
  ) {
    features.executionAdjacent = true;
  }

  // Evidence-dependent frame: live/current specific actors or need — not ideal ICP advice.
  const asksLiveActors =
    /\b(which|who)\b/.test(q) &&
    /\b(companies|prospects|accounts|businesses|managers)\b/.test(q) &&
    !/\bshould we (target|pursue|reach)\b/.test(q);
  const asksCurrentNeed =
    (/\b(actually|currently|right now|this (week|month)|monday|around here)\b/.test(
      q
    ) &&
      /\b(need|buy|call|companies|prospects|who|which)\b/.test(q)) ||
    /\bmost likely to need (us|our)\b/.test(q) ||
    /\bwho (around here )?(needs|need) (us|our|cleaning)\b/.test(q) ||
    /\bwho actually needs\b/.test(q);
  const asksSignals =
    /\bbuying signals?\b/.test(q) ||
    /\bshowing (buying |purchase )?signals?\b/.test(q) ||
    /\bwho is (expanding|hiring|buying|looking)\b/.test(q) ||
    /\bwhat changed in (our |the )?market\b/.test(q);
  if (asksLiveActors || asksCurrentNeed || asksSignals) {
    features.evidence = true;
  }

  if (
    /what do you (know|understand) about (my |our )?business/.test(q) ||
    /what have you learned about (my |our )?business/.test(q) ||
    /tell me what you (know|understand)/.test(q) ||
    /summarize (my |our )?business/.test(q) ||
    /who (am i|are we)( to you)?\b/.test(q) ||
    /what services do we offer/.test(q) ||
    /who are (my |our )?(ideal )?customers/.test(q)
  ) {
    features.understanding = true;
  }

  features.gap = conceptFamilyHits(tokens, 'gap');
  features.priority = conceptFamilyHits(tokens, 'priority');
  features.risk = conceptFamilyHits(tokens, 'risk');
  features.strategy = conceptFamilyHits(tokens, 'strategy');
  features.owner = conceptFamilyHits(tokens, 'owner');

  // Discourse: inclusive client voice / owner perspective / Max judgment.
  if (/\b(we|us|our|my)\b/.test(q)) features.discourse += 2;
  if (/\b(business|company|customers?|clients?|market|strategy)\b/.test(q)) {
    features.discourse += 2;
  }
  if (
    /\bif (this|that|it) (was|were) your (company|business)\b/.test(q) ||
    /\bif you were (me|us|in my (shoes|position))\b/.test(q) ||
    /\bwhat would you (do|think|be thinking|want|figure|learn|start)\b/.test(q) ||
    /\bhow('s| is) (this|it|that) looking\b/.test(q) ||
    /\bhow do you feel\b/.test(q) ||
    /\bwhat worries you\b/.test(q) ||
    /\bchange your mind\b/.test(q)
  ) {
    features.discourse += 3;
    features.owner += 1;
  }

  // Advisory / interrogative structure (not a required keyword).
  if (
    /^(what|where|who|how|why|anything|do you|would you|okay|ok|alright)\b/.test(
      q
    ) ||
    /\?$/.test(String(question || '').trim())
  ) {
    features.advisory += 2;
  }
  if (
    tokenSet.has('should') ||
    tokenSet.has('would') ||
    tokenSet.has('could') ||
    tokenSet.has('least')
  ) {
    features.advisory += 1;
  }

  // Gap/epistemic frames beyond stem hits (negation + know/learn/sure).
  if (
    (/\b(don'?t|dont|do not|least|still)\b/.test(q) &&
      /\b(know|sure|clear|learn|understand)\b/.test(q)) ||
    /\b(missing|overlooking|assumptions?|gaps?|holes?|uncertaint)/.test(q) ||
    /\bbefore we (spend|put|invest|pay)\b/.test(q)
  ) {
    features.gap += 2;
  }

  // Priority / next-action frames.
  if (
    /\b(now what|what now|from here|where would you (go|start)|what should we)\b/.test(
      q
    ) ||
    /\b(focus|start|next|first)\b/.test(q)
  ) {
    features.priority += 2;
  }

  // Comparative / negation strategy frames.
  if (
    /\binstead of\b/.test(q) ||
    /\bwhy (commercial|residential)\b/.test(q) ||
    /\bwhat shouldn'?t we\b/.test(q) ||
    /\bwhat would make (this|it) fail\b/.test(q)
  ) {
    features.strategy += 2;
    features.risk += 1;
  }

  const prior = hasPriorClientReasoning(session);
  if (prior) {
    features.discourse += 1;
    if (
      /^(why\??|why that\??|why not\??|why instead\??|and why\??|tell me why\??)$/.test(
        q
      ) ||
      /^(why that|why this|why those|why instead)\b/.test(q) ||
      /\bwhy (that|this|those|commercial|residential)\b/.test(q) ||
      (/^(what about|how about)\b/.test(q) &&
        /\b(commercial|residential|that|this|them|it)\b/.test(q)) ||
      /^(anything else|and\??|what else|ok(ay)?\.? what now|so what now)\??$/.test(
        q
      ) ||
      (/^(anything|what) (else|next)\??$/.test(q))
    ) {
      features.followUp = true;
    }
    if (
      /\bhow('s| is) (this|it|that) looking\b/.test(q) ||
      /\bhow do you feel\b/.test(q) ||
      /^(thoughts|and)\??$/.test(q)
    ) {
      features.followUp = true;
    }
  } else if (
    /\bhow('s| is) (this|it|that) looking\b/.test(q) ||
    /\bhow do you feel( about (it|this|that))?\b/.test(q)
  ) {
    // No conversational referent yet — clarify rather than invent a topic.
    features.referentAmbiguous = true;
  }

  const conceptScore =
    features.gap * 1.4 +
    features.priority * 1.2 +
    features.risk * 1.2 +
    features.strategy * 1.0 +
    features.owner * 0.8;
  const score =
    conceptScore + features.discourse + features.advisory + (features.followUp ? 4 : 0);

  let mode = 'focus';
  const ranked = [
    ['unknowns', features.gap],
    ['risk', features.risk],
    ['opportunity', features.strategy > 0 && /\bopportunit/.test(q) ? features.strategy + 2 : 0],
    ['targeting', /\b(target|who should|audience|segment|go after)\b/.test(q) ? 3 : 0],
    ['approach', /\b(approach|from here|if you were|your (company|business)|thinking)\b/.test(q) ? 3 : 0],
    ['campaign_advisory', /\bcampaign\b/.test(q) && /\b(recommend|suggest)\b/.test(q) ? 4 : 0],
    ['week', /\bthis week\b/.test(q) ? 3 : 0],
    ['priority', features.priority],
  ].sort((a, b) => b[1] - a[1]);
  if (features.understanding) mode = 'understanding';
  else if (features.followUp) mode = 'follow_up';
  else if (features.evidence) mode = 'evidence';
  else if (ranked[0] && ranked[0][1] > 0) {
    mode = ranked[0][0] === 'priority' ? 'focus' : ranked[0][0];
  }

  const strongFamily =
    features.gap >= 2 ||
    features.priority >= 2 ||
    features.risk >= 1 ||
    features.strategy >= 2;
  const shortAdvisoryFragment =
    tokens.length <= 4 && features.advisory >= 1 && conceptScore >= 1.0;

  const isClientBusiness =
    !features.unrelated &&
    !features.execution &&
    !features.referentAmbiguous &&
    (features.understanding ||
      features.followUp ||
      features.evidence ||
      features.executionAdjacent ||
      score >= 3.5 ||
      (features.discourse >= 2 && conceptScore >= 1.2) ||
      (features.advisory >= 2 && conceptScore >= 1.2) ||
      (strongFamily && features.advisory >= 1) ||
      shortAdvisoryFragment);

  return {
    q,
    tokens,
    score,
    mode,
    features,
    isClientBusiness,
  };
}

function looksLikeBusinessUnderstandingAsk(question) {
  return scoreClientBusinessSemantics(question).features.understanding;
}

function looksLikeTargetingAsk(question) {
  const scored = scoreClientBusinessSemantics(question);
  return scored.mode === 'targeting' && scored.isClientBusiness;
}

function looksLikeUnknownsAsk(question) {
  const scored = scoreClientBusinessSemantics(question);
  return (
    scored.isClientBusiness &&
    (scored.mode === 'unknowns' || scored.features.gap >= 2)
  );
}

/** Legacy focus detector — now semantic priority/next-action scope. */
function looksLikeFocusAsk(question) {
  const scored = scoreClientBusinessSemantics(question);
  return (
    scored.isClientBusiness &&
    (scored.mode === 'focus' ||
      scored.mode === 'week' ||
      scored.mode === 'approach' ||
      scored.features.priority >= 2)
  );
}

/**
 * SPEC-103/103B — live market/prospect evidence, not Blueprint alone.
 * Semantic current-need / named-actor frames (not phrase allowlists).
 */
function isEvidenceDependentClientRequest(question) {
  return scoreClientBusinessSemantics(question).features.evidence === true;
}

function isOperationalDeskOrMissionRequest(question) {
  const q = normalizeClientUtterance(question);
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

/**
 * SPEC-103B — advisory client-business reasoning via semantic scope.
 * General conversation is the fallback, not the default escape hatch.
 */
function isClientContextReasoningRequest(question, session) {
  const scored = scoreClientBusinessSemantics(question, session);
  if (!scored.isClientBusiness) return false;
  if (scored.features.evidence) return false;
  if (scored.features.understanding) return false;
  if (scored.features.execution || scored.features.executionAdjacent) return false;
  if (isOperationalDeskOrMissionRequest(scored.q)) return false;
  return true;
}

/** Explicit execution intent — must not be answered as advisory advice. */
function isClientContextExecutionRequest(question) {
  const q = normalizeClientUtterance(question);
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

/** Ambiguous strategy-agreement vs execution — clarify, never execute. */
function isAmbiguousExecutionAdjacentRequest(question) {
  return scoreClientBusinessSemantics(question).features.executionAdjacent === true;
}

function isClientContextReasoningFollowUp(question, session) {
  const scored = scoreClientBusinessSemantics(question, session);
  return scored.features.followUp === true;
}

function looksLikeAmbiguousBusinessReferent(question, session) {
  const scored = scoreClientBusinessSemantics(question, session);
  return scored.features.referentAmbiguous === true;
}

function looksLikeClientIntelligenceAsk(question, session) {
  if (isClientContextExecutionRequest(question)) return false;
  if (isOperationalDeskOrMissionRequest(question)) return false;
  if (isAmbiguousExecutionAdjacentRequest(question)) return true;
  const scored = scoreClientBusinessSemantics(question, session);
  if (scored.features.unrelated) return false;
  return (
    scored.features.understanding ||
    scored.features.evidence ||
    scored.features.followUp ||
    scored.features.referentAmbiguous ||
    scored.isClientBusiness ||
    isClientContextReasoningRequest(question, session)
  );
}

function formatUnderstandingAnswer(summary) {
  const name = summary.businessName || 'your business';
  const bullets = [];

  if (summary.commercialPreference) {
    bullets.push(
      'You currently serve both residential and commercial clients, but want to focus more heavily on commercial work.'
    );
  } else if (summary.identity) {
    bullets.push(presentText(summary.identity));
  }

  if (summary.services) {
    bullets.push(`Your services include ${presentText(summary.services)}.`);
  }

  if (summary.idealCustomers) {
    bullets.push(
      `Your current commercial focus is ${presentText(summary.idealCustomers)}.`
    );
  }

  if (summary.targetMarkets || summary.geography) {
    const geo = presentText(summary.geography || summary.targetMarkets);
    bullets.push(
      /^the\s+/i.test(geo) ? `You serve ${geo}.` : `You serve the ${geo}.`
    );
  }

  if (summary.competitiveAdvantages) {
    bullets.push(
      `You compete on ${presentText(summary.competitiveAdvantages)}.`
    );
  }

  if (summary.campaignGoals) {
    const goal = presentText(summary.campaignGoals);
    if (/pipeline|prospect|recurring/i.test(goal)) {
      bullets.push(
        'Your near-term goal is to build a reliable pipeline that turns prospects into recurring clients.'
      );
    } else if (/^(build|establish|create|grow|run)\b/i.test(goal)) {
      bullets.push(
        `Your near-term goal is to ${goal.charAt(0).toLowerCase()}${goal.slice(1)}.`
      );
    } else {
      bullets.push(`Your near-term goal is ${goal}.`);
    }
  }

  if (summary.successMetrics) {
    bullets.push(
      `You're watching ${presentText(summary.successMetrics)} as the clearest signs that it's working.`
    );
  }

  if (!bullets.length) {
    return (
      'I have an approved Business Blueprint on file, but the section details are thin. ' +
      'We should refine a few details before treating more of your business as established fact.'
    );
  }

  let out =
    `Here's what I understand about ${name}:\n\n` +
    bullets.map((p) => `• ${p}`).join('\n');

  const unresolved = (summary.unknowns || []).filter(Boolean);
  if (unresolved.length) {
    out +=
      `\n\nStill unresolved:\n` +
      unresolved.map((u) => `• ${presentText(u)}`).join('\n');
  }

  return out;
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
    const geo = summary.targetMarkets || summary.geography
      ? ` in ${summary.targetMarkets || summary.geography}`
      : '';
    const pref = summary.commercialPreference
      ? 'commercial preference'
      : summary.campaignGoals || '';
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
    if (summary.targetMarkets || summary.geography) {
      out += ` Geography: ${summary.geography || summary.targetMarkets}.`;
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
  const icp = summary.idealCustomers || null;
  const market = summary.geography || summary.targetMarkets || null;
  const goals = summary.campaignGoals || null;
  const metrics = summary.successMetrics || null;
  const knownBits = [];
  if (summary.identity || summary.businessName) {
    knownBits.push(presentText(summary.identity || summary.businessName));
  }
  if (icp) knownBits.push(`ideal customers center on ${icp}`);
  if (market) knownBits.push(`geography is anchored to ${market}`);
  if (goals) knownBits.push(`near-term goal is ${goals}`);
  if (metrics) knownBits.push(`success is judged by ${metrics}`);

  const paragraphs = [];
  paragraphs.push(
    'We have enough clarity to begin, but there are still several things I would not claim to know yet.'
  );

  if (knownBits.length) {
    paragraphs.push(
      `KNOWN from your approved Blueprint: ${knownBits.slice(0, 3).join('; ')}.`
    );
  }

  const inferenceBits = [];
  if (icp) {
    inferenceBits.push(
      `starting with ${icp}${market ? ` in ${market}` : ''} is a reasonable first motion`
    );
  } else {
    inferenceBits.push(
      'a tighter commercial acquisition motion is the right shape of first experiment'
    );
  }
  paragraphs.push(
    `INFERENCE (Max reasoning, not new fact): ${inferenceBits.join('; ')}. ` +
      'That is directional guidance from approved understanding — not observed performance.'
  );

  const unknownBits = [];
  if (market) {
    unknownBits.push(
      `which part of the ${market} commercial market will respond best`
    );
  } else {
    unknownBits.push('which part of the commercial market will respond best');
  }
  if (icp && /property|facility/i.test(icp)) {
    unknownBits.push(
      'whether property managers will outperform facility managers'
    );
  } else {
    unknownBits.push('which decision-maker segment will outperform the others');
  }
  unknownBits.push(
    'which acquisition motion will produce the strongest recurring contracts'
  );
  for (const u of summary.unknowns || []) {
    if (u) unknownBits.push(presentText(u));
  }
  paragraphs.push(`UNKNOWN: ${unknownBits.slice(0, 4).join('; ')}.`);

  paragraphs.push(
    'EVIDENCE NEEDED: we do not yet have enough campaign evidence to know expected walkthrough rate, close rate, or acquisition cost. ' +
      "Those are not reasons to wait — they are the things the first acquisition experiment should help us learn. " +
      'I will not invent live companies, signals, or performance claims.'
  );

  return paragraphs.join('\n\n');
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
  const scored = scoreClientBusinessSemantics(question, session);
  if (scored.features.followUp) return 'follow_up';
  if (scored.mode === 'unknowns' || scored.features.gap >= 2) return 'unknowns';
  if (scored.mode === 'targeting') return 'targeting';
  if (scored.mode === 'week') return 'week';
  if (scored.mode === 'opportunity' || /\bopportunit/.test(scored.q)) {
    return 'opportunity';
  }
  if (scored.mode === 'risk' || scored.features.risk >= 2) return 'risk';
  if (scored.mode === 'campaign_advisory') return 'campaign_advisory';
  if (scored.mode === 'approach' || scored.features.owner >= 2) return 'approach';
  return 'focus';
}

/**
 * SPEC-103 — synthesize a bounded recommendation from approved client facts.
 * Level 1 (Blueprint) → Level 3 (Max inference). Does not invent Level 2 evidence.
 * SPEC-103A — inserts semantic values only (never nested Blueprint explanations).
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
  const market = summary.geography || summary.targetMarkets || null;
  const goals = summary.campaignGoals || null;
  const metrics = summary.successMetrics || null;
  const services = summary.services || null;
  const avoid = summary.avoidCustomers || null;
  const unknowns = summary.unknowns || [];
  const commercialPreference = Boolean(
    summary.commercialPreference ||
      /commercial/i.test(
        [summary.growthFocus, services, goals, summary.competitiveAdvantages]
          .filter(Boolean)
          .join(' ')
      )
  );
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
    const qNorm = normalizeClientUtterance(question);
    if (/^(anything else|what else|and\??)\??$/.test(qNorm)) {
      return {
        prose:
          `Beyond ${focusBit}, I would also watch for early response evidence — ` +
          `which conversations turn into walkthroughs — before widening the audience. ` +
          `That is still advisory reasoning from your approved Blueprint, not live campaign proof.`,
        kind: 'follow_up',
        confidenceLabel: 'moderate',
        confidence: 0.7,
        recommendationFocus: focusBit,
      };
    }
    if (/\bresidential\b/.test(qNorm) && /\b(commercial|why|instead|about)\b/.test(qNorm)) {
      return {
        prose:
          `I am prioritizing commercial over residential because your approved Blueprint prefers commercial work` +
          (icp ? ` and names ${icp} as the decision-makers to reach` : '') +
          `. Residential is not forbidden — it is simply not the first beachhead I would prove. ` +
          `If residential starts producing stronger walkthroughs or recurring revenue, we should revise with evidence.`,
        kind: 'follow_up',
        confidenceLabel: 'moderate',
        confidence: 0.72,
        recommendationFocus: focusBit,
      };
    }
    const whyParts = [];
    whyParts.push(
      `I'd start with ${focusBit} because it follows what you've already approved about the business.`
    );
    if (icp) whyParts.push(`Your approved ICP points to ${icp}.`);
    if (commercialPreference) {
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
  const motion =
    commercialPreference
      ? 'repeatable commercial acquisition motion'
      : 'repeatable acquisition motion';

  const paragraphs = [];
  if (mode === 'opportunity') {
    paragraphs.push(
      `Your biggest near-term opportunity looks like proving a ${motion} around ${audience}${where}.`
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
      `I'd start by proving a ${motion} around ${audience}${where}.`
    );
  }

  const clarityBits = [];
  if (commercialPreference) clarityBits.push('you prefer commercial work');
  if (icp) clarityBits.push('you already identified the decision-makers you want to reach');
  if (goals || metrics) {
    clarityBits.push(
      metrics
        ? `${midSentencePhrase(metrics)} are the outcomes that matter`
        : 'recurring revenue / walkthrough outcomes are what matter'
    );
  }
  if (clarityBits.length) {
    paragraphs.push(
      `You already have enough clarity to run that first experiment: ${clarityBits.join(', ')}.`
    );
  }

  paragraphs.push(
    `What we don't know yet is which part of that market will respond best or produce the strongest contracts. ` +
      `I wouldn't narrow further based on assumption alone.`
  );

  paragraphs.push(
    `I'd start with a qualified group of ${audience}, measure how conversations turn into walkthroughs, ` +
      `then track how those convert toward ${midSentencePhrase(outcome)}. ` +
      `That gives us our first learning loop: qualified prospects → conversations → walkthroughs → recurring revenue.`
  );

  const segmentUncertainty = market
    ? `which specific ${market} segment will perform best`
    : 'which specific segment will perform best';
  paragraphs.push(
    `I'm moderately confident in the direction because it follows your approved business priorities. ` +
      `I'm less confident about ${segmentUncertainty} because we don't have enough campaign or market evidence yet. ` +
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
  const market =
    summary && (summary.geography || summary.targetMarkets)
      ? summary.geography || summary.targetMarkets
      : 'your market';
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

  // Ambiguous agreement vs execution — clarify; never treat as authorization.
  if (isAmbiguousExecutionAdjacentRequest(question)) {
    const focus =
      (session &&
        session.context &&
        session.context.lastClientIntelligenceTurn &&
        session.context.lastClientIntelligenceTurn.recommendationFocus) ||
      (summary && summary.idealCustomers) ||
      'that audience';
    const prose =
      `I hear you leaning toward going after ${focus}. ` +
      `Before anything runs, I need to know whether you want strategy agreement only, ` +
      `or to open a reviewable execution path (campaign / outreach). ` +
      `I will not launch or send anything from this message alone.`;
    const structured = workspaceStructured(prose, [], {
      unavailable: ['execution_authorization'],
      clientFacingReasoning: [],
      internalReasoning: [
        'Execution-adjacent language — fail closed and clarify (SPEC-103B).',
      ],
      confidence: 0.9,
    });
    recordLastCieTurn(session, {
      kind: 'clarification',
      reason: 'client_intelligence_execution_clarify',
      recommendationFocus: focus,
      question,
    });
    return {
      reason: 'client_intelligence_execution_clarify',
      handled: true,
      prose,
      structured,
      summary,
      attachment,
      turnKind: 'clarification',
      recommendationFocus: focus,
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

  // Ambiguous referent with no durable conversational anchor — clarify naturally.
  if (looksLikeAmbiguousBusinessReferent(question, session) && summary && summary.approved) {
    const prose =
      'I can help with that. Are you asking about the commercial targeting direction ' +
      'from your approved Blueprint, or something else in the current conversation?';
    const structured = workspaceStructured(prose, [], {
      clientFacingReasoning: [],
      internalReasoning: [
        'Ambiguous business referent without prior CIE turn — clarify (SPEC-103B).',
      ],
      confidence: 0.85,
    });
    recordLastCieTurn(session, {
      kind: 'clarification',
      reason: 'client_intelligence_referent_clarify',
      question,
    });
    return {
      reason: 'client_intelligence_referent_clarify',
      handled: true,
      prose,
      structured,
      summary,
      attachment,
      turnKind: 'clarification',
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
  isAmbiguousExecutionAdjacentRequest,
  isClientContextReasoningFollowUp,
  looksLikeAmbiguousBusinessReferent,
  isOperationalDeskOrMissionRequest,
  normalizeClientUtterance,
  scoreClientBusinessSemantics,
  composeClientContextReasoning,
  formatUnderstandingAnswer,
  formatMissingAnswer,
  formatUnknownsAnswer,
  formatTargetingAnswer,
  formatEvidenceDependentGapAnswer,
  sanitizeFactSummary,
  hasUsefulClientContext,
  peelBlueprintSubstance,
  semanticFieldsFromNormalizedFacts,
  presentText,
};
