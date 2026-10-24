'use strict';

/**
 * Max Synthesis Layer — BusinessFactNormalizer.
 *
 * Converts raw user answers and prior artifact fields into phrase-safe
 * canonical business meaning. Renderers embed these phrases — never raw
 * instruction paragraphs — into wrapper sentences.
 */

const DEFAULT_TOWNS = Object.freeze([
  'Bedford',
  'Hooksett',
  'Londonderry',
  'Auburn',
  'Goffstown',
]);

const DEFAULT_TARGET_SUBTYPE_PROPERTY_MANAGERS =
  'property managers overseeing offices, mixed-use buildings, small commercial properties, or multi-tenant spaces';

/** Instruction / meta leads that must never be embedded mid-sentence. */
const INSTRUCTION_LEAD_PATTERNS = Object.freeze([
  /^start with\s+/i,
  /^keep\s+/i,
  /^prove that\s+/i,
  /^the first target segment is\s+/i,
  /^the first campaign should prove(?:\s+that)?\s+/i,
  /^this (?:first )?campaign should prove(?:\s+that)?\s+/i,
  /^confirm (?:the )?/i,
  /^focus on\s+/i,
  /^i (?:also )?forgot(?:\s+to\s+mention)?\s+/i,
  /^i also forgot\s+/i,
  /^this revision introduced\s+/i,
  /^for (?:the )?icp[,:\s]*/i,
  /^also[,:\s]+/i,
  /^actually[,:\s]+/i,
  /^correction[,:\s]+/i,
]);

const META_FRAGMENT_RE =
  /\b(i (?:also )?forgot(?:\s+to\s+mention)?|this revision introduced|for (?:the )?icp|please refine|regenerate(?:\s+the\s+brief)?|instructions?\s+to\s+max)\b/gi;

const RAW_PROMPT_FRAGMENT_RES = Object.freeze([
  /\bStart with\b/,
  /\bProve that\b/,
  /\bThe first target segment is\b/i,
  /\bI forgot to mention\b/i,
  /\bThis revision introduced\b/i,
  /\binside Start with\b/,
  /\bthat match Small to\b/,
  /\bMarket focus:\s*Start with\b/i,
  // Criteria / campaign artifact paragraphs must never leak into wrappers.
  /\bSmall to mid-sized local property managers in Greater Manchester who oversee\b/i,
  /\bKeep Greater Manchester in scope\b/i,
  /\bkeep the first test tight enough to learn quickly\b/i,
  /\bin Start with\b/i,
  // Capital-S bleed from raw criteria into "Lead with … for Small to mid-sized…"
  /for Small to mid-sized/,
  /differentiators for /i,
  /(?<!\.)\.\.(?!\.)/,
  /\bCarry forward proof already noted\b/i,
  /\bCompetitive edge is described as\b/i,
  /\bThis is operator-stated differentiation\b/i,
]);

function naturalList(items) {
  const list = (items || []).map((x) => String(x).trim()).filter(Boolean);
  if (!list.length) return '';
  if (list.length === 1) return list[0];
  if (list.length === 2) return `${list[0]} and ${list[1]}`;
  return `${list.slice(0, -1).join(', ')}, and ${list[list.length - 1]}`;
}

function collapseWs(text) {
  return String(text || '')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Strip leading instruction verbs and meta wrappers.
 */
function stripInstructionFraming(text) {
  let s = collapseWs(text);
  if (!s) return '';
  let prev;
  do {
    prev = s;
    for (const re of INSTRUCTION_LEAD_PATTERNS) {
      s = s.replace(re, '').trim();
    }
    s = s
      .replace(META_FRAGMENT_RE, ' ')
      .replace(/^(?:that|to|and|with)\s+/i, '')
      .replace(/^[,;:\-–—]\s*/, '')
      .replace(/\s+/g, ' ')
      .trim();
  } while (s && s !== prev);
  return s;
}

/**
 * Convert text into an embeddable noun/verb phrase (no trailing period,
 * optional leading lowercase for mid-sentence use).
 */
function asEmbeddablePhrase(text, opts = {}) {
  let s = stripInstructionFraming(text);
  if (!s) return '';
  s = s.replace(/[.!?]+$/g, '').trim();
  if (!s) return '';
  if (opts.lowercase !== false && /^[A-Z][a-z]/.test(s) && !/^(Greater|Anchor|Bedford|Hooksett|Londonderry|Auburn|Goffstown|Manchester)\b/.test(s)) {
    s = s.charAt(0).toLowerCase() + s.slice(1);
  }
  return s;
}

function looksLikeSentence(text) {
  const s = collapseWs(text);
  return /[.!?]$/.test(s) || /\b(will|should|can|are|is|have|has)\b/i.test(s);
}

/**
 * If prior field is already a full sentence, either keep as standalone
 * (opts.standalone) or reduce to an embeddable phrase.
 */
function normalizePriorField(text, opts = {}) {
  const raw = collapseWs(text);
  if (!raw) return '';
  if (opts.standalone && looksLikeSentence(raw) && !INSTRUCTION_LEAD_PATTERNS.some((re) => re.test(raw))) {
    let s = stripInstructionFraming(raw);
    if (s && !/[.!?]$/.test(s)) s = `${s}.`;
    if (s) s = s.charAt(0).toUpperCase() + s.slice(1);
    return s;
  }
  return asEmbeddablePhrase(raw, opts);
}

function extractTowns(text, preferredTowns) {
  const towns = (
    Array.isArray(preferredTowns) && preferredTowns.length
      ? preferredTowns
      : DEFAULT_TOWNS
  ).map((t) => String(t));
  const s = String(text || '');
  return towns.filter((t) => new RegExp(`\\b${t}\\b`, 'i').test(s));
}

function normalizeBusinessName(raw, fallback) {
  const s = stripInstructionFraming(raw || fallback || '');
  if (!s) return 'the business';
  return s.replace(/\s+/g, ' ').trim();
}

function normalizeServiceMix(raw) {
  if (Array.isArray(raw)) {
    return raw.map((x) => asEmbeddablePhrase(x)).filter(Boolean);
  }
  const s = asEmbeddablePhrase(raw);
  if (!s) return [];
  return s
    .split(/\s*(?:,|;|\/|\band\b)\s*/i)
    .map((x) => asEmbeddablePhrase(x))
    .filter((x) => x.length > 2)
    .slice(0, 8);
}

function normalizeCustomerList(raw) {
  if (Array.isArray(raw)) {
    return raw.map((x) => asEmbeddablePhrase(x)).filter(Boolean).slice(0, 8);
  }
  const s = asEmbeddablePhrase(raw);
  if (!s) return [];
  if (/property managers?/i.test(s) && s.length < 40) {
    return ['property managers'];
  }
  return [s];
}

/**
 * "Small to mid-sized local property managers in Greater Manchester who oversee…"
 * → "small to mid-sized local property managers"
 */
function normalizeTargetSegmentPhrase(raw, ctx = {}) {
  let s = asEmbeddablePhrase(raw);
  const primary = asEmbeddablePhrase(
    ctx.primarySegment || ctx.targetSegment || 'property managers'
  );

  if (!s) {
    if (/property manager/i.test(primary)) {
      return 'small to mid-sized local property managers';
    }
    return primary || 'the focus segment';
  }

  // Drop geography / who-oversee clauses for the short segment phrase.
  s = s
    .replace(/\bin\s+Greater Manchester(?:\s+NH)?\b.*$/i, '')
    .replace(/\bwho\s+(?:oversee|manage|oversee)\b.*$/i, '')
    .replace(/\boverseeing\b.*$/i, '')
    .replace(/\bthat likely need\b.*$/i, '')
    .replace(/[.!?]+$/g, '')
    .trim();

  if (/^property managers?\b/i.test(s) && s.length < 28) {
    return 'small to mid-sized local property managers';
  }
  if (/small to mid-sized/i.test(s) && /property managers?/i.test(s)) {
    const m = s.match(
      /\bsmall to mid-sized(?:\s+local)?\s+property managers?\b/i
    );
    if (m) return m[0].toLowerCase().replace(/\bmanagers\b/i, 'managers');
  }
  if (!s || s.length < 4) {
    return /property manager/i.test(primary)
      ? 'small to mid-sized local property managers'
      : primary;
  }
  return asEmbeddablePhrase(s);
}

/**
 * Prefer polished subtype noun phrase; never paste a full criteria paragraph twice.
 */
function normalizeTargetSubtypePhrase(raw, ctx = {}) {
  let s = asEmbeddablePhrase(raw);
  if (
    s &&
    /property managers overseeing/i.test(s) &&
    /mixed-use|multi-tenant|offices/i.test(s)
  ) {
    // Drop trailing recurring-cleaning clause for embeddable subtype.
    s = s
      .replace(/\s+that likely need recurring cleaning.*$/i, '')
      .replace(/[.!?]+$/g, '')
      .trim();
    return asEmbeddablePhrase(s);
  }
  if (/property manager/i.test(String(ctx.primarySegment || raw || ''))) {
    return DEFAULT_TARGET_SUBTYPE_PROPERTY_MANAGERS;
  }
  if (s && s.length >= 12) return s;
  return '';
}

/**
 * "Start with Bedford…. Keep Greater Manchester in scope…"
 * → "Bedford, Hooksett, Londonderry, Auburn, and Goffstown, with Greater Manchester kept in scope"
 */
function normalizeMarketBoundPhrase(raw, ctx = {}) {
  const market =
    collapseWs(ctx.targetMarket || ctx.primaryArea || 'Greater Manchester') ||
    'Greater Manchester';
  const preferredTowns =
    Array.isArray(ctx.towns) && ctx.towns.length ? ctx.towns : DEFAULT_TOWNS;
  const fromRaw = extractTowns(raw, preferredTowns);
  const towns = fromRaw.length >= 2 ? fromRaw : preferredTowns.slice(0, 5);
  const townPhrase = naturalList(towns);

  const rawText = collapseWs(raw);
  const keepsMarket =
    !rawText ||
    /greater manchester|keep .+ in scope|in scope/i.test(rawText) ||
    /manchester/i.test(market);

  if (townPhrase && keepsMarket) {
    return `${townPhrase}, with ${market} kept in scope`;
  }
  if (townPhrase) return townPhrase;
  return asEmbeddablePhrase(raw) || market;
}

/**
 * "Prove that …" / core validation → embeddable objective phrase.
 * Example: "test whether qualified property-manager conversations can turn into walkthroughs or estimate requests"
 */
function normalizeObjectivePhrase(raw, ctx = {}) {
  let s = asEmbeddablePhrase(raw);
  const core = asEmbeddablePhrase(ctx.coreValidationQuestion || '');

  if (core) {
    const m = core.match(
      /can\s+.+\s+create\s+(qualified\s+[\w-]+(?:\s+[\w-]+)*\s+conversations?)\s+that\s+turn\s+into\s+(.+)/i
    );
    if (m) {
      const outcome = String(m[2] || '')
        .replace(/\?+$/, '')
        .replace(/\bestimates?\b/i, 'walkthroughs or estimate requests')
        .replace(/walkthroughs or estimate requests or estimate requests/i, 'walkthroughs or estimate requests')
        .trim();
      // Avoid duplicating "walkthroughs or estimate requests" when core already
      // said "walkthroughs or estimates".
      const cleanOutcome = /walkthroughs or estimates?/i.test(String(m[2] || ''))
        ? 'walkthroughs or estimate requests'
        : outcome;
      return `test whether ${m[1]} can turn into ${cleanOutcome}`;
    }
  }

  if (!s) {
    if (/property manager/i.test(String(ctx.primarySegment || ''))) {
      return 'test whether qualified property-manager conversations can turn into walkthroughs or estimate requests';
    }
    return 'test whether qualified conversations can turn into walkthroughs or estimate requests';
  }

  s = s
    .replace(/^that\s+/i, '')
    .replace(/\bwill\s+(?:engage|book|take|request)\b/i, 'can turn into')
    .replace(/[.!?]+$/g, '')
    .trim();

  if (/^test whether\b/i.test(s)) return asEmbeddablePhrase(s);
  if (/qualified .+ conversations/i.test(s)) {
    return asEmbeddablePhrase(`test whether ${s}`);
  }
  if (/walkthrough|estimate|conversation/i.test(s)) {
    return asEmbeddablePhrase(`test whether ${s}`);
  }
  return asEmbeddablePhrase(s);
}

function normalizeProofAssets(raw) {
  if (Array.isArray(raw)) {
    return raw.map((x) => stripInstructionFraming(x)).filter(Boolean).slice(0, 10);
  }
  const s = stripInstructionFraming(raw);
  return s ? [s] : [];
}

function normalizeSignalList(raw) {
  return normalizeProofAssets(raw);
}

/**
 * Build the canonical fact bag used by ArtifactSynthesisContext.
 *
 * @param {object} input
 * @returns {object}
 */
function normalizeBusinessFacts(input = {}) {
  const ctx = input.context || {};
  const factsIn = input.normalizedFacts || {};
  const prior = input.priorArtifact || input.priorCriteriaPreview || {};
  const campaign = input.priorCampaignPreview || {};
  const slots = input.slots || {};

  const businessName = normalizeBusinessName(
    factsIn.business_name ||
      prior.businessName ||
      campaign.businessName ||
      ctx.businessName,
    'the business'
  );

  const serviceMix = normalizeServiceMix(
    factsIn.services || input.serviceMix || []
  );

  const idealCustomers = normalizeCustomerList(
    factsIn.ideal_customers || input.idealCustomers || ctx.primarySegment
  );

  const avoidCustomers = normalizeCustomerList(
    factsIn.disqualified_customers ||
      factsIn.avoid_customers ||
      input.avoidCustomers ||
      []
  );

  const targetSegment = normalizeTargetSegmentPhrase(
    prior.targetSegment ||
      campaign.targetSegment ||
      slots.targetSegment ||
      input.targetSegment,
    {
      primarySegment: ctx.primarySegment || idealCustomers[0],
      targetSegment: slots.targetSegment,
    }
  );

  const targetSubtype = normalizeTargetSubtypePhrase(
    prior.targetSubtype ||
      campaign.targetSubtype ||
      slots.targetSubtype ||
      input.targetSubtype,
    { primarySegment: ctx.primarySegment || targetSegment }
  );

  const marketBound = normalizeMarketBoundPhrase(
    prior.marketBound ||
      campaign.marketBound ||
      slots.marketBound ||
      input.marketBound,
    {
      targetMarket: ctx.targetMarket || ctx.primaryArea,
      towns: ctx.towns,
      primaryArea: ctx.primaryArea,
    }
  );

  const campaignObjective = normalizeObjectivePhrase(
    prior.campaignObjective ||
      campaign.campaignObjective ||
      slots.campaignObjective ||
      input.campaignObjective,
    {
      primarySegment: ctx.primarySegment || targetSegment,
      coreValidationQuestion:
        prior.coreValidationQuestion || campaign.coreValidationQuestion,
    }
  );

  const proofAssets = normalizeProofAssets(
    prior.proofAssetsAvailable ||
      campaign.proofAssetsAvailable ||
      slots.proofAssets ||
      input.proofAssets
  );

  const validationSignals = normalizeSignalList(
    prior.validationMetricsPrimary ||
      campaign.validationMetricsPrimary ||
      slots.validationMetrics ||
      input.validationSignals
  );

  const disqualifyingSignals = normalizeSignalList(
    prior.exclusionCriteria ||
      campaign.exclusionCriteria ||
      slots.exclusionCriteria ||
      input.disqualifyingSignals ||
      avoidCustomers
  );

  const phrases = Object.freeze({
    targetSegmentPhrase: targetSegment,
    targetSubtypePhrase: targetSubtype,
    marketBoundPhrase: marketBound,
    objectivePhrase: campaignObjective,
    businessNamePhrase: asEmbeddablePhrase(businessName, { lowercase: false }),
  });

  // Evidence: raw sources kept for citation — not for display embedding.
  const evidence = Object.freeze({
    rawTargetSegment: prior.targetSegment || slots.targetSegment || null,
    rawMarketBound: prior.marketBound || slots.marketBound || null,
    rawObjective:
      prior.campaignObjective || slots.campaignObjective || null,
    rawTargetSubtype: prior.targetSubtype || slots.targetSubtype || null,
    answers: input.answers || null,
  });

  return Object.freeze({
    businessName,
    serviceMix,
    idealCustomers,
    avoidCustomers,
    marketBound,
    targetSegment,
    targetSubtype,
    campaignObjective,
    proofAssets,
    validationSignals,
    disqualifyingSignals,
    phrases,
    evidence,
    epistemic_states: factsIn.epistemic_states || {},
    hypotheses: factsIn.hypotheses || {},
    evidence_statements: factsIn.evidence_statements || {},
  });
}

/**
 * True when text still contains banned raw prompt / instruction fragments.
 */
function containsRawPromptFragment(text) {
  const s = String(text || '');
  if (!s) return false;
  return RAW_PROMPT_FRAGMENT_RES.some((re) => re.test(s));
}

/**
 * Assert helper for tests — returns matching fragment strings.
 */
function findRawPromptFragments(text) {
  const s = String(text || '');
  const hits = [];
  for (const re of RAW_PROMPT_FRAGMENT_RES) {
    if (re.test(s)) hits.push(re.source);
  }
  // Also catch duplicated full subtype sentences.
  const subtype = DEFAULT_TARGET_SUBTYPE_PROPERTY_MANAGERS;
  if (subtype && s.split(subtype).length > 2) {
    hits.push('duplicated_target_subtype_sentence');
  }
  return hits;
}

module.exports = {
  DEFAULT_TOWNS,
  DEFAULT_TARGET_SUBTYPE_PROPERTY_MANAGERS,
  INSTRUCTION_LEAD_PATTERNS,
  RAW_PROMPT_FRAGMENT_RES,
  stripInstructionFraming,
  asEmbeddablePhrase,
  normalizePriorField,
  normalizeBusinessName,
  normalizeServiceMix,
  normalizeCustomerList,
  normalizeTargetSegmentPhrase,
  normalizeTargetSubtypePhrase,
  normalizeMarketBoundPhrase,
  normalizeObjectivePhrase,
  normalizeBusinessFacts,
  containsRawPromptFragment,
  findRawPromptFragments,
  naturalList,
};
