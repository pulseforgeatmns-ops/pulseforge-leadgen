'use strict';

/**
 * Initial Growth Direction — post-Blueprint-approval consultant preview.
 *
 * Understanding-only. Built strictly from the approved Business Blueprint
 * (plus optional CIE normalizedFacts). Never invents campaigns, prospect lists,
 * channels, or validated-market claims.
 */

const ARTIFACT_KIND = 'initial_growth_direction';
const SEGMENT_RANKING_KIND = 'segment_ranking';
const VALIDATION_TARGET_KIND = 'validation_target';
const DIRECTIONAL_LABEL =
  'This is a directional read, not market validation.';
const SEGMENT_RANKING_CONFIDENCE =
  'Directional, not market-validated.';
const VALIDATION_TARGET_CONFIDENCE =
  'Directional, not market-validated.';

/** Growth Conversation intents that request a ranked segment call. */
const RANKING_INTENTS = Object.freeze([
  'rank_segments',
  'compare_segments',
  'choose_first_segment',
  'make_directional_call',
  'prioritize_segments',
]);

/** Intent for first-win / validation-target definitions after ranking. */
const VALIDATION_TARGET_INTENT = 'define_validation_target';

/** Intent when the user agrees with a ranking or names a preferred first segment. */
const SELECT_PRIMARY_INTENT = 'select_primary_segment';

/** Artifact kinds beyond Segment Ranking / Validation Target. */
const FIRST_SEGMENT_DECISION_KIND = 'first_segment_decision';

/** Focus areas the Growth Conversation can dig into first. */
const FOCUS_AREAS = Object.freeze([
  'segment_mix',
  'market_bound',
  'success_definition',
  'infrastructure_readiness',
  'unknown',
]);

/** Ordered Growth Conversation steps (state machine). */
const GROWTH_STEPS = Object.freeze([
  'choose_focus_area',
  'rank_segments',
  'select_primary_segment',
  'define_validation_target',
  'define_success_signals',
  'summarize_first_growth_plan_preview',
]);

/**
 * Phrases that mean “define the validation target / first win” — checked
 * before ranking so “best first test” does not loop on Segment Ranking.
 */
const VALIDATION_TARGET_PHRASES = Object.freeze([
  /good first win/,
  /\bfirst win\b/,
  /validation target/,
  /first 30 days/,
  /early signals/,
  /what type of property manager/,
  /what size or type/,
  /pain point/,
  /\bproof\b/,
  /worth continuing/,
  /before building any campaign/,
  /not asking for outreach copy/,
  /not asking for a (?:prospect )?list/,
]);

/** Explicit restart / re-compare requests that may re-open the segment-mix fallback. */
const RESTART_SEGMENT_PHRASES = Object.freeze([
  /\brestart\b/,
  /\bstart over\b/,
  /\bre-?rank\b/,
  /\bcompare again\b/,
  /\brank again\b/,
  /\breset (?:the )?(?:segments?|ranking|growth)\b/,
]);
/**
 * Deterministic segment tier hints grounded in recurring commercial-cleaning
 * Blueprint themes. Only applied when the segment appears in the approved
 * Blueprint list — never invents segments.
 */
const SEGMENT_TIER_HINTS = Object.freeze({
  'property managers': 'first',
  'facility managers': 'second',
  'professional offices': 'second',
  'law firms': 'second',
  'accounting practices': 'second',
  'short-term rental companies': 'warm',
  daycares: 'warm',
  'rec centers': 'defer',
  'high-traffic buildings': 'defer',
  homeowners: 'defer',
});

const PRIMARY_AREA_PATTERNS = [
  [/\bGreater Manchester(?:\s+area)?\b/i, 'Greater Manchester'],
  [/\bManchester(?:\s+NH)?\b/i, 'Manchester'],
  [/\bCharleston(?:\s+WV)?\b/i, 'Charleston'],
  [/\bNashville(?:\s+TN)?\b/i, 'Nashville'],
  [/\bMyrtle Beach\b/i, 'Myrtle Beach'],
];

const TOWN_NAMES = [
  'Bedford',
  'Hooksett',
  'Londonderry',
  'Auburn',
  'Goffstown',
  'South Charleston',
  'St. Albans',
  'Dunbar',
  'Nitro',
  'Cross Lanes',
  'Hurricane',
];

function firstSentence(text) {
  const s = String(text || '').trim();
  if (!s) return '';
  const parts = s.split(/(?<=[.!?])\s+/);
  return parts[0] || s;
}

function sectionSummary(sections, key) {
  const s = sections && sections[key];
  return s && s.summary ? String(s.summary).trim() : '';
}

function sanitizeBusinessName(name) {
  let s = String(name || '').trim();
  if (!s) return '';
  s = s
    .replace(/\s+(?:we|we're|we are|i|i'm|i am|my company|our company|the company)\b.*$/i, '')
    .replace(/^(?:we are|we're|i am|i'm|my company is|our company is)\s+/i, '')
    .replace(/[—–,:;.\s]+$/g, '')
    .replace(/\s{2,}/g, ' ')
    .trim();
  if (/\banchor\s+cleaning\b/i.test(s) || /\banchor\s+cleaning\b/i.test(String(name || ''))) {
    return 'Anchor Cleaning';
  }
  if (/^anchor\b/i.test(s) && s.split(/\s+/).length <= 2) return 'Anchor';
  s = s.replace(/\s+(?:we|are|is|a|an|the|and|for|to)$/i, '').trim();
  return s;
}

function extractBusinessName(identitySummary) {
  const s = String(identitySummary || '').trim();
  if (!s) return '';
  if (/\banchor\s+cleaning\b/i.test(s)) return 'Anchor Cleaning';
  const named = s.match(
    /^([A-Z][a-zA-Z0-9&'.-]+(?:\s+[A-Z][a-zA-Z0-9&'.-]+){0,5})\s+(?:is|are)\b/
  );
  if (named) return sanitizeBusinessName(named[1]);
  const cleaning = s.match(/\b(Anchor(?:\s+Cleaning)?)\b/i);
  if (cleaning) return sanitizeBusinessName(cleaning[1]);
  return '';
}

function splitPhrases(text) {
  if (!text) return [];
  return String(text)
    .split(/[,;\n]|•|\u2022|\band\b/i)
    .map((p) => p.trim().replace(/^[–—*•]\s*/, '').replace(/[.]+$/, ''))
    .filter(Boolean)
    .filter((p) => p.split(/\s+/).length <= 12);
}

function uniqueStrings(items) {
  const out = [];
  for (const item of items || []) {
    const s = String(item || '').trim();
    if (!s) continue;
    if (!out.some((x) => x.toLowerCase() === s.toLowerCase())) out.push(s);
  }
  return out;
}

function naturalList(items) {
  const list = uniqueStrings(items);
  if (!list.length) return '';
  if (list.length === 1) return list[0];
  if (list.length === 2) return `${list[0]} and ${list[1]}`;
  return `${list.slice(0, -1).join(', ')}, and ${list[list.length - 1]}`;
}

function extractGrowthFocus(text) {
  const raw = String(text || '');
  const focusMatch = raw.match(
    /(?:strongest\s+)?growth focus is\s+([^.;]+)(?:[.;]|$)/i
  );
  if (focusMatch) {
    let focus = focusMatch[1].trim();
    const forCustomers = focus.match(/^(.*?(?:cleaning|service))\s+for\s+/i);
    if (forCustomers) return forCustomers[1].trim();
    return focus;
  }
  if (/recurring commercial cleaning/i.test(raw)) return 'recurring commercial cleaning';
  if (/commercial cleaning/i.test(raw)) return 'commercial cleaning';
  return '';
}

/**
 * Optional cadence / consistency qualifier for the first-focus sentence.
 * Kept separate from geography so we never produce
 * "weekly or multiple times per week in Greater Manchester".
 */
function extractFocusQualifier(sections, facts) {
  const parts = [];
  if (facts && facts.growth_focus) {
    parts.push(...String(facts.growth_focus).split(';').slice(1));
  }
  parts.push(sectionSummary(sections, 'services'));
  parts.push(sectionSummary(sections, 'campaignGoals'));
  const blob = parts.filter(Boolean).join(' ');

  if (
    /weekly or multiple[\s-]?times[\s-]?per[\s-]?week/i.test(blob) ||
    /multiple[\s-]?times[\s-]?per[\s-]?week/i.test(blob)
  ) {
    return 'customers who need consistent service weekly or multiple times per week';
  }
  if (/weekly/i.test(blob) && /recurring/i.test(blob)) {
    return 'customers who need consistent weekly service';
  }
  if (/customers?\s+who\s+need\s+([^.;]+)/i.test(blob)) {
    const m = blob.match(/customers?\s+who\s+need\s+([^.;]+)/i);
    if (m) {
      const need = m[1]
        .trim()
        .replace(/\s+service\.?$/i, ' service')
        .replace(/multiple-times-per-week/gi, 'multiple times per week');
      if (need && need.length < 90) {
        return `customers who need ${need}`;
      }
    }
  }
  return '';
}

const SEGMENT_PATTERNS = [
  [/\bproperty managers?\b/i, 'property managers'],
  [/\bshort-?term rental(?:\s+companies)?\b/i, 'short-term rental companies'],
  [/\bfacility managers?\b/i, 'facility managers'],
  [/\bprofessional offices?\b/i, 'professional offices'],
  [/\bdaycares?\b/i, 'daycares'],
  [/\brec(?:reation)? centers?\b/i, 'rec centers'],
  [/\bhigh-traffic buildings?\b/i, 'high-traffic buildings'],
  [/\blaw firms?\b/i, 'law firms'],
  [/\baccounting(?:\s+practices?|\s+firms?)?\b/i, 'accounting practices'],
  [/\bhomeowners?\b/i, 'homeowners'],
];

function extractSegments(text) {
  const raw = String(text || '').trim();
  if (!raw) return [];
  const found = [];
  for (const [re, canon] of SEGMENT_PATTERNS) {
    if (re.test(raw) && !found.includes(canon)) found.push(canon);
  }
  if (found.length) return found;
  return splitPhrases(raw)
    .filter((p) => p.split(/\s+/).length <= 6)
    .slice(0, 7);
}

function isPrimaryAreaName(value) {
  const s = String(value || '').trim().toLowerCase();
  return PRIMARY_AREA_PATTERNS.some(([, canon]) => canon.toLowerCase() === s);
}

function splitGeography(marketsSummary, facts) {
  const rawBits = [];
  if (facts && Array.isArray(facts.geography)) rawBits.push(...facts.geography);
  rawBits.push(String(marketsSummary || ''));
  const blob = rawBits.filter(Boolean).join(' ; ');

  let primaryArea = '';
  for (const [re, canon] of PRIMARY_AREA_PATTERNS) {
    if (re.test(blob)) {
      primaryArea = canon;
      break;
    }
  }

  const towns = [];
  for (const town of TOWN_NAMES) {
    if (new RegExp(`\\b${town.replace(/\./g, '\\.')}\\b`, 'i').test(blob)) {
      towns.push(town);
    }
  }

  // If facts listed bare "Manchester" alongside Greater Manchester towns, prefer area.
  if (/greater manchester/i.test(primaryArea)) {
    const filtered = towns.filter((t) => !/^manchester$/i.test(t));
    towns.length = 0;
    towns.push(...filtered);
  }

  if (!primaryArea && facts && Array.isArray(facts.geography) && facts.geography.length) {
    const first = String(facts.geography[0] || '').trim();
    if (first && isPrimaryAreaName(first)) primaryArea = first;
    else if (first && !towns.includes(first)) primaryArea = first;
  }

  if (!primaryArea && !towns.length) {
    const phrases = splitPhrases(marketsSummary).slice(0, 4);
    if (phrases.length) primaryArea = phrases[0];
  }

  return {
    primaryArea,
    towns: uniqueStrings(towns).slice(0, 6),
  };
}

function resolveBusinessName(sections, facts) {
  const fromFacts =
    facts && facts.business_name
      ? sanitizeBusinessName(facts.business_name)
      : '';
  if (fromFacts) return fromFacts;
  const fromIdentity = sanitizeBusinessName(
    extractBusinessName(sectionSummary(sections, 'identity'))
  );
  if (fromIdentity) return fromIdentity;
  const identityLead = firstSentence(sectionSummary(sections, 'identity'));
  const named = String(identityLead || '').match(
    /^([A-Z][a-zA-Z0-9&'.-]+(?:\s+[A-Z][a-zA-Z0-9&'.-]+){0,4})\b/
  );
  if (named) {
    const cleaned = sanitizeBusinessName(named[1]);
    if (cleaned) return cleaned;
  }
  return 'This business';
}

function resolveFirstFocus(sections, facts) {
  if (facts && facts.growth_focus) {
    const fromFacts = String(facts.growth_focus).split(';')[0].trim();
    if (fromFacts && !/^customers?\s+who\b/i.test(fromFacts)) return fromFacts;
  }
  const services = sectionSummary(sections, 'services');
  const goals = sectionSummary(sections, 'campaignGoals');
  const fromServices = extractGrowthFocus(services);
  if (fromServices) return fromServices;
  const fromGoals = extractGrowthFocus(goals);
  if (fromGoals) return fromGoals;
  if (/commercial/i.test(services) || /commercial/i.test(goals)) {
    return 'commercial growth opportunities';
  }
  const goalLead = firstSentence(goals);
  if (goalLead && goalLead.length < 120) {
    return goalLead
      .replace(/^near-term growth goals focus on\s+/i, '')
      .replace(/^over the next 90 days[, ]*(?:this growth work should\s+)?/i, '')
      .replace(/\.$/, '');
  }
  const serviceLead = firstSentence(services);
  if (serviceLead) {
    return serviceLead
      .replace(/^today the business delivers\s+/i, '')
      .replace(/\.$/, '')
      .slice(0, 100);
  }
  return 'the opportunities named in the approved Blueprint';
}

function resolveSegments(sections, facts) {
  if (facts && Array.isArray(facts.ideal_customers) && facts.ideal_customers.length) {
    return uniqueStrings(facts.ideal_customers).slice(0, 7);
  }
  return extractSegments(sectionSummary(sections, 'idealCustomers')).slice(0, 7);
}

function possessive(name) {
  const n = String(name || 'the business').trim();
  if (!n || /^this business$/i.test(n)) return "the business's";
  return /s$/i.test(n) ? `${n}'` : `${n}'s`;
}

function shortName(name) {
  const n = String(name || '').trim();
  if (!n || /^this business$/i.test(n)) return 'the business';
  return n.replace(/\s+Cleaning$/i, '') || n;
}

function cleanAvoidPhrase(summary) {
  let s = firstSentence(summary);
  if (!s) return '';
  s = s
    .replace(/^customers?\s+to\s+avoid\s*(?:are|:)?\s*/i, '')
    .replace(/^avoid(?:s|ing)?\s+/i, '')
    .replace(/^the business (?:deliberately )?avoids?\s+/i, '')
    .replace(/^anchor(?:\s+cleaning)?\s+(?:deliberately )?avoids?\s+/i, '')
    .replace(/\.$/, '')
    .trim();
  if (!s) return '';
  if (/^customers?\s+who\b/i.test(s)) return s;
  if (/^(?:the\s+)?lowest price/i.test(s)) {
    return `customers who prioritize ${s}`;
  }
  if (/prioritize|prefer|want|value/i.test(s)) {
    return /^customers?\b/i.test(s) ? s : `customers who ${s}`;
  }
  return s;
}

function composeFocusSentence(displayName, firstFocus, primaryArea, qualifier) {
  const poss = possessive(displayName);
  let sentence = `Based on this Blueprint, ${poss} first growth focus should be ${firstFocus}`;
  if (primaryArea) sentence += ` in ${primaryArea}`;
  if (qualifier) sentence += `, especially for ${qualifier}`;
  return `${sentence}.`;
}

function composeWhySentence(displayName) {
  const name = shortName(displayName);
  const subject = /^the business$/i.test(name) ? 'the business' : name;
  return (
    `That focus follows directly from the approved Blueprint: what ${subject} delivers today, ` +
    `who counts as an ideal customer, where the business wants to concentrate, ` +
    `and what near-term success looks like. ${DIRECTIONAL_LABEL}`
  );
}

function composeSegmentsSentence(segments, primaryArea, towns) {
  if (!segments.length && !primaryArea && !towns.length) {
    return (
      'The first segments worth comparing come from the Blueprint’s ideal-customer ' +
      'and market picture — sharpened before any outreach list is built.'
    );
  }
  if (!segments.length) {
    if (primaryArea && towns.length) {
      return (
        `The market bound worth comparing first is ${primaryArea}, especially ${naturalList(towns)}.`
      );
    }
    if (primaryArea) return `The market bound worth comparing first is ${primaryArea}.`;
    return `The markets worth comparing first are ${naturalList(towns)}.`;
  }

  let sentence = `The first segments worth comparing are ${naturalList(segments)}`;
  if (primaryArea && towns.length) {
    sentence += ` across ${primaryArea}, especially ${naturalList(towns)}`;
  } else if (primaryArea) {
    sentence += ` across ${primaryArea}`;
  } else if (towns.length) {
    sentence += ` across ${naturalList(towns)}`;
  }
  return `${sentence}.`;
}

function lowercaseLead(text) {
  const s = String(text || '');
  if (!s) return s;
  // Keep proper nouns / acronyms; only lower ordinary sentence leads like "Customers".
  if (/^[A-Z][a-z]+(?:\s|$)/.test(s) && !/^(Anchor|Greater|Manchester)\b/.test(s)) {
    return s.charAt(0).toLowerCase() + s.slice(1);
  }
  return s;
}

function composeAvoidSentence(displayName, avoidSummary) {
  const phrase = lowercaseLead(cleanAvoidPhrase(avoidSummary));
  if (!phrase) return '';
  return (
    `The Blueprint also clarifies who ${shortName(displayName)} should avoid: ${phrase}. ` +
    `That constraint keeps the first growth focus honest instead of chasing volume.`
  );
}

/**
 * Build Initial Growth Direction from an approved Business Blueprint.
 * @param {object} blueprint — public or row-shaped blueprint with sections
 * @param {{ normalizedFacts?: object|null }} [opts]
 */
function buildInitialGrowthDirection(blueprint, opts = {}) {
  if (!blueprint || !blueprint.sections) {
    throw new Error('approved blueprint with sections is required');
  }
  const sections = blueprint.sections || {};
  const facts = opts.normalizedFacts || null;
  const businessName = resolveBusinessName(sections, facts);
  const displayName = shortName(businessName);
  const firstFocus = resolveFirstFocus(sections, facts);
  const focusQualifier = extractFocusQualifier(sections, facts);
  const segments = resolveSegments(sections, facts);
  const { primaryArea, towns } = splitGeography(
    sectionSummary(sections, 'targetMarkets'),
    facts
  );
  const markets = uniqueStrings(
    [primaryArea, ...towns].filter(Boolean)
  );
  const avoid = sectionSummary(sections, 'avoidCustomers');

  const heading = `${possessive(displayName)} first growth focus`.replace(
    /^the business's/i,
    "The business's"
  );

  const paragraphs = [
    composeFocusSentence(displayName, firstFocus, primaryArea, focusQualifier),
    composeWhySentence(displayName),
    composeSegmentsSentence(segments, primaryArea, towns),
  ];

  const avoidPara = composeAvoidSentence(displayName, avoid);
  if (avoidPara) paragraphs.push(avoidPara);

  paragraphs.push(
    'The next conversation should turn this directional read into a focused growth plan: which segment to prioritize first, how tightly to bound the market, and what early signals will show whether the approach is working.'
  );

  // Keep 3–5 body paragraphs; heading is separate.
  const body = paragraphs.slice(0, 5);

  return {
    kind: ARTIFACT_KIND,
    title: 'Initial Growth Direction',
    heading,
    firstFocus,
    focusQualifier,
    businessName,
    segmentsToInspect: segments,
    marketsToInspect: markets,
    primaryArea: primaryArea || null,
    towns,
    paragraphs: body,
    nextConversationPreview:
      'Turn this first focus into a concrete growth conversation — still understanding-led, still before campaigns or prospect lists.',
    directional: true,
    disclaimer: DIRECTIONAL_LABEL,
    blueprintId: blueprint.id || null,
    blueprintVersion: blueprint.version || null,
  };
}

/**
 * Normalize a display segment name to a stable snake_case key.
 * "property managers" → "property_managers"
 * "rec centers or broad high-traffic buildings" → handled via ranking helpers.
 *
 * @param {string} segment
 * @returns {string|null}
 */
function toSegmentKey(segment) {
  const s = String(segment || '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_');
  if (!s) return null;
  if (s === 'high_traffic_buildings') return 'broad_high_traffic_buildings';
  return s;
}

function escapeRegex(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Empty Growth Conversation decision state.
 * @returns {object}
 */
function emptyGrowthState() {
  return {
    selected_focus_area: 'unknown',
    segment_ranking: null,
    primary_segment: null,
    secondary_segment: null,
    held_segments: [],
    deprioritized_segments: [],
    current_growth_step: 'choose_focus_area',
    completed_steps: [],
    confidence_level: 'directional',
    first_segment_decision: null,
    validation_target: null,
  };
}

/**
 * Normalize persisted growthConversation fields into the required state shape.
 * Accepts snake_case and legacy camelCase mirrors.
 *
 * @param {object|null|undefined} raw
 * @returns {object}
 */
function normalizeGrowthState(raw) {
  const base = emptyGrowthState();
  if (!raw || typeof raw !== 'object') return base;

  const ranking =
    raw.segment_ranking ||
    raw.segmentRanking ||
    (raw.context && raw.context.segmentRanking) ||
    null;
  const decision =
    raw.first_segment_decision || raw.firstSegmentDecision || null;
  const validation =
    raw.validation_target || raw.validationTarget || null;

  const completed = Array.isArray(raw.completed_steps)
    ? raw.completed_steps.slice()
    : Array.isArray(raw.completedSteps)
      ? raw.completedSteps.slice()
      : [];

  const held = Array.isArray(raw.held_segments)
    ? raw.held_segments.slice()
    : Array.isArray(raw.heldSegments)
      ? raw.heldSegments.slice()
      : [];
  const deprioritized = Array.isArray(raw.deprioritized_segments)
    ? raw.deprioritized_segments.slice()
    : Array.isArray(raw.deprioritizedSegments)
      ? raw.deprioritizedSegments.slice()
      : [];

  let focus = raw.selected_focus_area || raw.selectedFocusArea || base.selected_focus_area;
  if (!FOCUS_AREAS.includes(focus)) focus = 'unknown';

  let step = raw.current_growth_step || raw.currentGrowthStep || base.current_growth_step;
  if (!GROWTH_STEPS.includes(step)) step = 'choose_focus_area';

  const primary =
    raw.primary_segment ||
    raw.primarySegment ||
    (decision && decision.primary_segment) ||
    null;
  const secondary =
    raw.secondary_segment ||
    raw.secondarySegment ||
    (decision && decision.secondary_segment) ||
    null;

  return {
    ...base,
    selected_focus_area: focus,
    segment_ranking: ranking,
    primary_segment: primary ? toSegmentKey(primary) || primary : null,
    secondary_segment: secondary ? toSegmentKey(secondary) || secondary : null,
    held_segments: held.map((s) => toSegmentKey(s) || s).filter(Boolean),
    deprioritized_segments: deprioritized
      .map((s) => toSegmentKey(s) || s)
      .filter(Boolean),
    current_growth_step: step,
    completed_steps: completed,
    confidence_level: raw.confidence_level || raw.confidenceLevel || 'directional',
    first_segment_decision: decision,
    validation_target: validation,
  };
}

function markStepCompleted(state, step) {
  const completed = Array.isArray(state.completed_steps)
    ? state.completed_steps.slice()
    : [];
  if (step && !completed.includes(step)) completed.push(step);
  return completed;
}

function mentionsKnownSegment(msg) {
  return SEGMENT_PATTERNS.some(([re]) => re.test(msg));
}

function mentionedSegmentsFromMessage(msg) {
  const found = [];
  for (const [re, canon] of SEGMENT_PATTERNS) {
    if (re.test(msg) && !found.includes(canon)) found.push(canon);
  }
  return found;
}

function rankingRoleSegment(ranking, role) {
  if (!ranking || !Array.isArray(ranking.rankings)) return null;
  const hit = ranking.rankings.find((r) => r && r.role === role && r.segment);
  return hit ? hit.segment : null;
}

function deprioritizedKeysFromRanking(ranking) {
  if (!ranking || !Array.isArray(ranking.rankings)) return [];
  const avoid = ranking.rankings.find((r) => r && r.role === 'avoid_for_now');
  if (!avoid) return [];
  const label = String(avoid.displaySegment || avoid.segment || '');
  if (/rec centers?/i.test(label) && /high-traffic/i.test(label)) {
    return ['rec_centers', 'broad_high_traffic_buildings'];
  }
  const key = toSegmentKey(avoid.segment || label);
  return key ? [key] : [];
}

function heldKeysFromRanking(ranking) {
  const warm = rankingRoleSegment(ranking, 'keep_warm');
  const key = toSegmentKey(warm);
  return key ? [key] : [];
}

function isRestartSegmentRequest(msg) {
  return RESTART_SEGMENT_PHRASES.some((re) => re.test(msg));
}

/**
 * True when the message asks to define a validation target / first-win
 * criteria rather than re-rank segments.
 *
 * @param {string} msg lowercased message
 * @returns {boolean}
 */
function isDefineValidationTargetRequest(msg) {
  return VALIDATION_TARGET_PHRASES.some((re) => re.test(msg));
}

/**
 * True when the user is selecting / agreeing on a primary segment rather than
 * asking for a fresh ranking or the generic segment-mix list.
 *
 * @param {string} msg lowercased message
 * @param {object} [state]
 * @returns {boolean}
 */
function isSelectPrimarySegmentRequest(msg, state) {
  const normalized = normalizeUserMessage(msg);
  const hasRanking = Boolean(state && state.segment_ranking);
  const mentioned = mentionedSegmentsFromMessage(normalized);
  if (!mentioned.length && !hasRanking) return false;

  if (/feel(?:s)? like the most attractive/.test(normalized) && mentioned.length) {
    return true;
  }
  if (/let'?s start with/.test(normalized) && mentioned.length) return true;
  if (
    /\bi agree\b/.test(normalized) &&
    (hasRanking ||
      /property managers?|professional offices?|first/.test(normalized))
  ) {
    return true;
  }
  if (/most attractive first segment/.test(normalized) && mentioned.length) {
    return true;
  }
  if (
    mentioned.length &&
    /\b(first|primary)\b/.test(normalized) &&
    /\b(secondary|attractive|start|prefer|validate|path)\b/.test(normalized)
  ) {
    return true;
  }
  if (
    hasRanking &&
    mentioned.length &&
    /\b(first|secondary|attractive|prefer|primary)\b/.test(normalized)
  ) {
    return true;
  }
  if (
    hasRanking &&
    /\block (?:that|it|this) in|go with (?:your|the) (?:ranking|recommendation)|use (?:your|the) ranking/i.test(
      normalized
    )
  ) {
    return true;
  }
  return false;
}

/**
 * Resolve primary / secondary / held / deprioritized from the user message
 * plus the prior Segment Ranking.
 *
 * @param {string} userMessage
 * @param {object|null} priorRanking
 * @returns {{ primary: string|null, secondary: string|null, held: string[], deprioritized: string[], primaryDisplay: string|null, secondaryDisplay: string|null }}
 */
function resolveSegmentSelection(userMessage, priorRanking) {
  const msg = normalizeUserMessage(userMessage);
  const mentioned = mentionedSegmentsFromMessage(msg);

  let primaryDisplay = null;
  let secondaryDisplay = null;

  for (const seg of mentioned) {
    const re = new RegExp(
      `${escapeRegex(seg)}.{0,40}(?:as\\s+)?(?:the\\s+)?secondary|` +
        `(?:as\\s+)?(?:the\\s+)?secondary.{0,40}${escapeRegex(seg)}`,
      'i'
    );
    if (re.test(msg)) {
      secondaryDisplay = seg;
      break;
    }
  }

  for (const seg of mentioned) {
    if (seg === secondaryDisplay) continue;
    const after = new RegExp(
      `${escapeRegex(seg)}.{0,60}(most attractive|attractive first|first segment|to validate|primary|start with|prefer)`,
      'i'
    );
    const before = new RegExp(
      `(start with|let'?s start with|prefer|preferred|primary|most attractive|attractive).{0,60}${escapeRegex(seg)}`,
      'i'
    );
    const firstNear = new RegExp(
      `${escapeRegex(seg)}.{0,40}\\bfirst\\b|\\bfirst\\b.{0,40}${escapeRegex(seg)}`,
      'i'
    );
    if (after.test(msg) || before.test(msg) || firstNear.test(msg)) {
      primaryDisplay = seg;
      break;
    }
  }

  if (
    !primaryDisplay &&
    priorRanking &&
    /\bi agree\b|sounds (?:good|right)|that (?:works|makes sense)|go with (?:that|your)|lock (?:that|it|this) in|use (?:your|the) ranking/i.test(
      msg
    )
  ) {
    primaryDisplay = rankingRoleSegment(priorRanking, 'best_first');
  }

  if (
    !primaryDisplay &&
    mentioned.length &&
    /(attractive|first|start|prefer|primary|validate)/i.test(msg)
  ) {
    primaryDisplay = mentioned.find((m) => m !== secondaryDisplay) || mentioned[0];
  }

  if (!secondaryDisplay && priorRanking) {
    secondaryDisplay = rankingRoleSegment(priorRanking, 'second_best');
  }
  if (!primaryDisplay && priorRanking) {
    primaryDisplay = rankingRoleSegment(priorRanking, 'best_first');
  }

  const held = heldKeysFromRanking(priorRanking);
  const deprioritized = deprioritizedKeysFromRanking(priorRanking);

  return {
    primary: toSegmentKey(primaryDisplay),
    secondary: toSegmentKey(secondaryDisplay),
    held,
    deprioritized,
    primaryDisplay,
    secondaryDisplay,
  };
}

/**
 * Detect which focus area the user wants to dig into first.
 * @param {string} msg lowercased
 * @returns {string|null}
 */
function detectSelectedFocusArea(msg) {
  if (/segment mix|dig (?:into )?(?:the )?segments|compare (?:the )?segments|segment focus/.test(msg)) {
    return 'segment_mix';
  }
  if (/market bound|bound the market|geo(?:graphy)?|service area/.test(msg)) {
    return 'market_bound';
  }
  if (/success definition|what success|ninety days|90 days|success (?:should|criteria)/.test(msg)) {
    return 'success_definition';
  }
  if (/infrastructure readiness|ops readiness|operational readiness/.test(msg)) {
    return 'infrastructure_readiness';
  }
  return null;
}

/**
 * Build the First Segment Decision artifact after the user picks a primary.
 */
function buildFirstSegmentDecision(selection, opts = {}) {
  const primaryDisplay = selection.primaryDisplay || selection.primary || 'the focus segment';
  const secondaryDisplay = selection.secondaryDisplay || null;
  const rationale =
    opts.rationale ||
    (secondaryDisplay
      ? `${displaySegmentName(primaryDisplay)} is the first segment to validate; ${displaySegmentName(secondaryDisplay)} stays the secondary path.`
      : `${displaySegmentName(primaryDisplay)} is the first segment to validate.`);
  const cautions = Array.isArray(opts.cautions)
    ? opts.cautions
    : [
        'Still directional — not market-validated.',
        'Do not build campaigns or prospect lists until a validation target is defined.',
      ];

  return {
    kind: FIRST_SEGMENT_DECISION_KIND,
    title: 'First Segment Decision',
    primary_segment: selection.primary,
    secondary_segment: selection.secondary,
    held_segments: selection.held || [],
    deprioritized_segments: selection.deprioritized || [],
    primarySegmentDisplay: primaryDisplay,
    secondarySegmentDisplay: secondaryDisplay,
    rationale,
    cautions,
    next_step: 'define_validation_target',
    confidence_level: opts.confidence_level || 'directional',
  };
}

function formatFirstSegmentDecisionMessage(decision) {
  const d = decision || {};
  const lines = [d.title || 'First Segment Decision', ''];
  lines.push(`Primary segment: ${d.primarySegmentDisplay || d.primary_segment || '—'}`);
  lines.push(
    `Secondary segment: ${d.secondarySegmentDisplay || d.secondary_segment || '—'}`
  );
  lines.push(
    `Held segments: ${(d.held_segments || []).length ? (d.held_segments || []).join(', ') : '—'}`
  );
  lines.push(
    `Deprioritized segments: ${(d.deprioritized_segments || []).length ? (d.deprioritized_segments || []).join(', ') : '—'}`
  );
  lines.push('');
  lines.push('Rationale:');
  lines.push(d.rationale || '');
  lines.push('');
  lines.push('Cautions:');
  for (const c of d.cautions || []) lines.push(`- ${c}`);
  lines.push('');
  lines.push(`Next step: ${d.next_step || 'define_validation_target'}`);
  return lines.join('\n').trim();
}

/**
 * Detect Growth Conversation intents from a user message.
 * Validation-target intents win over ranking so follow-ups like
 * “good first win” / “best first test” do not repeat Segment Ranking.
 * Primary-segment selection wins over generic dig_segments when the user
 * agrees with a ranking or names a preferred first segment.
 * Ranking / directional-call intents are returned before generic dig intents.
 *
 * @param {string} userMessage
 * @param {{ growthState?: object|null }} [opts]
 * @returns {string|null}
 */
function normalizeUserMessage(userMessage) {
  return String(userMessage || '')
    .trim()
    .toLowerCase()
    .replace(/[\u2018\u2019\u201A\u2032]/g, "'");
}

function detectGrowthConversationIntent(userMessage, opts = {}) {
  const msg = normalizeUserMessage(userMessage);
  if (!msg) return null;
  const state = normalizeGrowthState(opts.growthState || null);

  if (isDefineValidationTargetRequest(msg)) {
    return VALIDATION_TARGET_INTENT;
  }

  // Selecting a primary segment must win over dig_segments (“segment” in msg).
  if (isSelectPrimarySegmentRequest(msg, state)) {
    return SELECT_PRIMARY_INTENT;
  }

  const asksRank =
    /\brank(?:ing|ed|s)?\b/.test(msg) ||
    /\bprioritiz(?:e|es|ing|ation)\b/.test(msg);
  const asksCompare =
    /\bcompare\b/.test(msg) || /\bcomparison\b/.test(msg);
  const asksFirst =
    /\bbest first\b/.test(msg) ||
    /\bfirst segment to test\b/.test(msg) ||
    /\bwhere should we start\b/.test(msg) ||
    /\bwhich segment\b/.test(msg) ||
    /\bsecond[- ]?best\b/.test(msg);
  const asksCall =
    /\bdirectional call\b/.test(msg) ||
    /\bmake a (?:directional )?call\b/.test(msg) ||
    /\bmake the call\b/.test(msg);
  const clearlyMarketsOnly =
    /\bmarkets?\b/.test(msg) &&
    !/\bsegments?\b/.test(msg) &&
    !asksRank &&
    !asksFirst &&
    !asksCall;

  if (
    (asksRank || asksCompare || asksFirst || asksCall) &&
    !clearlyMarketsOnly
  ) {
    if (asksCall) return 'make_directional_call';
    if (/\brank(?:ing|ed|s)?\b/.test(msg)) return 'rank_segments';
    if (/\bprioritiz/.test(msg)) return 'prioritize_segments';
    if (asksFirst && !asksCompare) return 'choose_first_segment';
    if (asksCompare) return 'compare_segments';
    return 'rank_segments';
  }

  if (/segment|customer|icp|who\b/.test(msg)) return 'dig_segments';
  if (/market|geo|area|region|city|where\b/.test(msg)) return 'dig_markets';
  if (/success|metric|90|ninety|goal|win\b/.test(msg)) return 'dig_success';
  return null;
}
function displaySegmentName(segment) {
  const s = String(segment || '').trim();
  if (!s) return s;
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function findSegmentByHint(segments, hintKeys) {
  const list = segments || [];
  for (const key of hintKeys) {
    const hit = list.find((s) => String(s).toLowerCase() === key);
    if (hit) return hit;
  }
  return null;
}

function remainingSegments(segments, used) {
  const usedLc = new Set((used || []).map((s) => String(s).toLowerCase()));
  return (segments || []).filter((s) => !usedLc.has(String(s).toLowerCase()));
}

function segmentWhyBullets(segment, ctx) {
  const s = String(segment || '').toLowerCase();
  const recurring = ctx.recurringFocus;
  const reliability = ctx.reliabilityTheme;

  if (/property manager/.test(s)) {
    return [
      recurring
        ? 'Strong recurring cleaning potential'
        : 'Fits the Blueprint’s named commercial growth focus',
      reliability
        ? 'Clear need for reliability and responsiveness'
        : 'Decision-makers care about consistent service quality',
      'Multiple properties can create account expansion',
      recurring
        ? 'Good fit with recurring commercial cleaning'
        : 'Good fit with the Blueprint first focus',
    ];
  }
  if (/professional office|law firm|accounting/.test(s)) {
    return [
      'Clear recurring need',
      'Easier to understand and service',
      'Faster path to walkthroughs',
    ];
  }
  if (/facility manager/.test(s)) {
    return [
      recurring
        ? 'Aligned with recurring commercial cleaning'
        : 'Aligned with the Blueprint first focus',
      'Single-site operators who value reliability',
      'Reachable decision-maker path when access exists',
    ];
  }
  if (/short-?term rental/.test(s)) {
    return [
      'Strong cleaning frequency',
      'Fits turnover service',
    ];
  }
  if (/daycare/.test(s)) {
    return [
      'Recurring service rhythm when schedules are stable',
      'Fits reliability-sensitive commercial work',
    ];
  }
  if (/rec(?:reation)? center|high-traffic/.test(s)) {
    return [
      'Can look attractive on volume',
      'May fit later once proof and capacity are stronger',
    ];
  }
  return [
    `Named as an ideal-customer segment in the approved Blueprint`,
    ctx.firstFocus
      ? `Compatible with the directional first focus (${ctx.firstFocus})`
      : 'Compatible with the directional first focus',
  ];
}

function segmentCautionBullets(segment, ctx) {
  const s = String(segment || '').toLowerCase();
  if (/property manager/.test(s)) {
    return [
      'Decision makers may be busy or vendor-saturated',
      'May require proof of reliability before switching',
    ];
  }
  if (/professional office|law firm|accounting/.test(s)) {
    return ['Smaller account values than multi-property relationships'];
  }
  if (/facility manager/.test(s)) {
    return ['May need clearer site access before it outranks multi-property paths'];
  }
  if (/short-?term rental/.test(s)) {
    return ['Schedule volatility and price sensitivity may be higher'];
  }
  if (/daycare/.test(s)) {
    return ['Access windows and compliance sensitivity can slow first wins'];
  }
  if (/rec(?:reation)? center|high-traffic/.test(s)) {
    return [
      'Operational complexity may be higher',
      'Requirements may be less predictable',
      ctx.businessName && !/^the business$/i.test(ctx.businessName)
        ? `Better revisited after ${shortName(ctx.businessName)} has stronger proof and capacity`
        : 'Better revisited after the business has stronger proof and capacity',
    ];
  }
  if (ctx.avoidPhrase) {
    return [`Watch the Blueprint avoid constraint: ${ctx.avoidPhrase}`];
  }
  return ['Evidence from the Blueprint alone is thin — treat as directional'];
}

/**
 * Build a structured Segment Ranking from approved Blueprint segments only.
 * Makes a directional call even when evidence is thin; never invents segments
 * or claims market validation.
 *
 * @param {object} growthDirection
 * @param {object} [blueprint]
 * @param {{ intent?: string }} [opts]
 */
function buildSegmentRanking(growthDirection, blueprint, opts = {}) {
  const gd = growthDirection || {};
  const sections = (blueprint && blueprint.sections) || {};
  const segments = uniqueStrings(gd.segmentsToInspect || []).slice(0, 8);
  const firstFocus = gd.firstFocus || resolveFirstFocus(sections, null);
  const businessName = shortName(gd.businessName || resolveBusinessName(sections, null));
  const avoidSummary = sectionSummary(sections, 'avoidCustomers');
  const avoidPhrase = cleanAvoidPhrase(avoidSummary);
  const advantages = sectionSummary(sections, 'competitiveAdvantages');
  const services = sectionSummary(sections, 'services');
  const recurringFocus =
    /recurring/i.test(firstFocus) ||
    /recurring/i.test(services) ||
    /commercial cleaning/i.test(firstFocus);
  const reliabilityTheme =
    /reliab|trust|without needing to chase|responsiv/i.test(advantages) ||
    /lowest price|reliability/i.test(avoidSummary);

  const ctx = {
    firstFocus,
    businessName,
    avoidPhrase,
    recurringFocus,
    reliabilityTheme,
  };

  const used = [];
  let best = findSegmentByHint(segments, ['property managers']);
  if (!best && segments.length) {
    const hinted = segments.find(
      (s) => SEGMENT_TIER_HINTS[String(s).toLowerCase()] === 'first'
    );
    best = hinted || segments[0];
  }
  if (best) used.push(best);

  let second =
    findSegmentByHint(segments, [
      'professional offices',
      'facility managers',
      'law firms',
      'accounting practices',
    ]) || null;
  if (!second) {
    const pool = remainingSegments(segments, used);
    second =
      pool.find((s) => SEGMENT_TIER_HINTS[String(s).toLowerCase()] === 'second') ||
      pool[0] ||
      null;
  }
  if (second) used.push(second);

  let warm =
    findSegmentByHint(segments, ['short-term rental companies', 'daycares']) ||
    null;
  if (!warm) {
    const pool = remainingSegments(segments, used);
    warm =
      pool.find((s) => SEGMENT_TIER_HINTS[String(s).toLowerCase()] === 'warm') ||
      pool[0] ||
      null;
  }
  if (warm) used.push(warm);

  let avoid =
    findSegmentByHint(segments, ['rec centers', 'high-traffic buildings']) ||
    null;
  let avoidLabel = avoid;
  if (avoid) {
    const alsoHighTraffic = findSegmentByHint(segments, ['high-traffic buildings']);
    const alsoRec = findSegmentByHint(segments, ['rec centers']);
    if (
      alsoHighTraffic &&
      alsoRec &&
      String(alsoHighTraffic).toLowerCase() !== String(alsoRec).toLowerCase()
    ) {
      avoidLabel = 'rec centers or broad high-traffic buildings';
      used.push(alsoRec, alsoHighTraffic);
    } else {
      used.push(avoid);
    }
  } else {
    const pool = remainingSegments(segments, used);
    avoid =
      pool.find((s) => SEGMENT_TIER_HINTS[String(s).toLowerCase()] === 'defer') ||
      pool[pool.length - 1] ||
      null;
    avoidLabel = avoid;
    if (avoid) used.push(avoid);
  }

  const thinEvidence = segments.length < 2;
  const rankings = [];

  if (best) {
    rankings.push({
      rank: 1,
      role: 'best_first',
      label: 'Best first segment to test',
      segment: best,
      displaySegment: displaySegmentName(best),
      why: segmentWhyBullets(best, ctx),
      cautions: segmentCautionBullets(best, ctx),
    });
  }
  if (second && String(second).toLowerCase() !== String(best || '').toLowerCase()) {
    rankings.push({
      rank: 2,
      role: 'second_best',
      label: 'Second-best segment',
      segment: second,
      displaySegment: displaySegmentName(second),
      why: segmentWhyBullets(second, ctx),
      cautions: segmentCautionBullets(second, ctx),
    });
  }
  if (warm && !used.slice(0, 2).some((u) => String(u).toLowerCase() === String(warm).toLowerCase())) {
    rankings.push({
      rank: 3,
      role: 'keep_warm',
      label: 'Keep warm',
      segment: warm,
      displaySegment: displaySegmentName(warm),
      why: segmentWhyBullets(warm, ctx),
      cautions: segmentCautionBullets(warm, ctx),
    });
  }
  if (
    avoidLabel &&
    !rankings.some(
      (r) => String(r.segment).toLowerCase() === String(avoid).toLowerCase()
    )
  ) {
    rankings.push({
      rank: 4,
      role: 'avoid_for_now',
      label: 'Avoid for now',
      segment: avoid,
      displaySegment: displaySegmentName(avoidLabel),
      why: segmentWhyBullets(avoid || avoidLabel, ctx),
      cautions: segmentCautionBullets(avoid || avoidLabel, ctx),
    });
  }

  // If Blueprint segments are missing, still make a limited directional call.
  if (!rankings.length) {
    rankings.push({
      rank: 1,
      role: 'best_first',
      label: 'Best first segment to test',
      segment: null,
      displaySegment: 'the ideal-customer segments named in the Blueprint',
      why: [
        'The approved Blueprint does not yet list discrete segments to rank',
        'Sharpen ideal customers before treating any segment as first to test',
      ],
      cautions: [
        'Confidence is limited until segments are explicit in the Blueprint',
      ],
    });
  }

  const bestName = rankings.find((r) => r.role === 'best_first');
  const secondName = rankings.find((r) => r.role === 'second_best');
  let directionalRecommendation = 'Hold for clearer Blueprint segments before prioritizing outreach.';
  if (bestName && bestName.segment && secondName && secondName.segment) {
    directionalRecommendation =
      `Start with ${bestName.segment}, while testing ${secondName.segment} as a secondary path.`;
  } else if (bestName && bestName.segment) {
    directionalRecommendation = `Start with ${bestName.segment} as the first segment to test.`;
  }

  return {
    kind: SEGMENT_RANKING_KIND,
    title: 'Segment Ranking',
    intent: opts.intent || 'rank_segments',
    rankings,
    directionalRecommendation,
    confidence: thinEvidence
      ? 'Limited — directional only; Blueprint segment evidence is thin. Not market-validated.'
      : SEGMENT_RANKING_CONFIDENCE,
    confidenceLevel: thinEvidence ? 'limited_directional' : 'directional',
    marketValidated: false,
    firstFocus: firstFocus || null,
    segmentsConsidered: segments,
    blueprintId: (blueprint && blueprint.id) || gd.blueprintId || null,
    blueprintVersion:
      (blueprint && blueprint.version) || gd.blueprintVersion || null,
  };
}

/**
 * Format a Segment Ranking artifact as the Growth Conversation message.
 */
function formatSegmentRankingMessage(ranking) {
  const r = ranking || {};
  const lines = [r.title || 'Segment Ranking', ''];

  for (const item of r.rankings || []) {
    lines.push(`${item.rank}. ${item.label}: ${item.displaySegment || item.segment}`);
    lines.push('Why:');
    for (const w of item.why || []) lines.push(`- ${w}`);
    lines.push('Cautions:');
    for (const c of item.cautions || []) lines.push(`- ${c}`);
    lines.push('');
  }

  lines.push('Directional recommendation:');
  lines.push(r.directionalRecommendation || '');
  lines.push('');
  lines.push('Confidence:');
  lines.push(r.confidence || SEGMENT_RANKING_CONFIDENCE);

  return lines.join('\n').trim();
}

/**
 * Resolve which Blueprint segment a validation-target request is about.
 * Prefers an explicit mention in the user message, then prior Segment Ranking
 * best-first, then property managers when present in the Blueprint list.
 *
 * @param {string} userMessage
 * @param {string[]} segments
 * @param {object|null} priorRanking
 * @returns {string|null}
 */
function resolveValidationFocusSegment(userMessage, segments, priorRanking) {
  const msg = String(userMessage || '').toLowerCase();
  const list = uniqueStrings(segments || []);

  if (/property manager/.test(msg)) {
    return findSegmentByHint(list, ['property managers']) || 'property managers';
  }
  for (const [re, canon] of SEGMENT_PATTERNS) {
    if (re.test(msg) && findSegmentByHint(list, [canon])) return canon;
  }

  const best =
    priorRanking &&
    Array.isArray(priorRanking.rankings) &&
    priorRanking.rankings.find((r) => r && r.role === 'best_first' && r.segment);
  if (best && best.segment) return best.segment;

  return (
    findSegmentByHint(list, ['property managers']) ||
    (list.length ? list[0] : null)
  );
}

function propertyManagerValidationSections(ctx) {
  const name = ctx.businessName || 'the business';
  const area = ctx.primaryArea || 'the Blueprint market bound';
  return {
    bestFirstType: {
      label: 'Best first property manager type',
      body:
        `Small to mid-sized local property managers overseeing offices, mixed-use buildings, small commercial properties, or multi-tenant spaces in ${area}.`,
    },
    propertySizeType: {
      label: 'Property size/type to pursue first',
      body:
        `Properties large enough to need recurring cleaning weekly or multiple times per week, but not so large that they require complex staffing, compliance, or overnight operations beyond ${name}'s current capacity.`,
    },
    painPoint: {
      label: 'First pain point to test',
      body: ctx.reliabilityTheme
        ? 'Reliability and responsiveness: missed details, inconsistent cleaners, slow issue resolution, and the manager having to chase the vendor.'
        : 'Consistency and vendor follow-through: missed details, uneven quality, and managers having to chase the cleaning vendor.',
    },
    proof: {
      label: `Credibility proof ${name} should show`,
      body:
        'Simple commercial cleaning checklist, before/after photos, proof of responsiveness, references if available, clear service area, and a professional walkthrough/estimate process.',
    },
    earlySignals: {
      label: 'Early signals worth continuing',
      bullets: [
        'Replies mention current cleaning frustration',
        'Property manager agrees to a walkthrough',
        'Prospect asks about recurring schedule',
        'Prospect asks about reliability/process',
        'Estimate request from a qualified property',
        'Interest without pushing immediately to lowest price',
      ],
    },
    first30Days: {
      label: 'Successful first 30 days of validation',
      body:
        `A successful first 30 days would mean ${name} gets a small number of qualified conversations with property managers, at least one walkthrough or estimate request, and clear evidence about which property types respond best.`,
    },
    cautions: {
      label: 'Cautions',
      body:
        'Do not chase every property manager. Avoid prospects whose first question is only price, properties that are operationally too complex, or opportunities that would strain service quality.',
    },
  };
}

function genericValidationSections(segment, ctx) {
  const name = ctx.businessName || 'the business';
  const display = displaySegmentName(segment || 'the focus segment');
  const area = ctx.primaryArea || 'the Blueprint market bound';
  return {
    bestFirstType: {
      label: `Best first ${display} type`,
      body:
        `Start with the clearest ${display.toLowerCase()} fit named in the approved Blueprint inside ${area} — selective, local, and aligned with ${ctx.firstFocus || 'the directional first focus'}.`,
    },
    propertySizeType: {
      label: 'Size/type to pursue first',
      body: ctx.recurringFocus
        ? `Accounts large enough to need recurring service weekly or multiple times per week, but within ${name}'s current capacity.`
        : `Accounts that fit ${name}'s current capacity and the Blueprint first focus — not the largest or most complex opportunities first.`,
    },
    painPoint: {
      label: 'First pain point to test',
      body: ctx.reliabilityTheme
        ? 'Reliability and responsiveness — work done right without the customer chasing the vendor.'
        : 'Whether the segment feels the Blueprint pain clearly enough to take a next step.',
    },
    proof: {
      label: `Credibility proof ${name} should show`,
      body:
        'Clear service scope, proof of responsiveness, references if available, defined service area, and a professional walkthrough or estimate process.',
    },
    earlySignals: {
      label: 'Early signals worth continuing',
      bullets: [
        'Replies mention current vendor frustration',
        'Prospect agrees to a walkthrough or discovery conversation',
        'Prospect asks about recurring schedule or process',
        'Estimate or site-visit request from a qualified account',
        'Interest without pushing immediately to lowest price',
      ],
    },
    first30Days: {
      label: 'Successful first 30 days of validation',
      body:
        `A successful first 30 days would mean ${name} gets a small number of qualified conversations in this segment, at least one walkthrough or estimate request, and clearer evidence of which sub-types respond best.`,
    },
    cautions: {
      label: 'Cautions',
      body: ctx.avoidPhrase
        ? `Stay selective. Watch the Blueprint avoid constraint (${ctx.avoidPhrase}) and do not chase accounts that are operationally too complex or price-only.`
        : 'Stay selective. Avoid price-only prospects and opportunities that would strain service quality.',
    },
  };
}

/**
 * Build a Validation Target / first-win definition from the approved Blueprint
 * and prior Segment Ranking. Does not create campaign copy or prospect lists.
 *
 * @param {object} growthDirection
 * @param {object} [blueprint]
 * @param {{ intent?: string, userMessage?: string, priorSegmentRanking?: object|null }} [opts]
 */
function buildValidationTarget(growthDirection, blueprint, opts = {}) {
  const gd = growthDirection || {};
  const sections = (blueprint && blueprint.sections) || {};
  const segments = uniqueStrings(gd.segmentsToInspect || []).slice(0, 8);
  const firstFocus = gd.firstFocus || resolveFirstFocus(sections, null);
  const businessName = shortName(gd.businessName || resolveBusinessName(sections, null));
  const primaryArea = gd.primaryArea || splitGeography(
    sectionSummary(sections, 'targetMarkets'),
    null
  ).primaryArea || null;
  const avoidSummary = sectionSummary(sections, 'avoidCustomers');
  const avoidPhrase = cleanAvoidPhrase(avoidSummary);
  const advantages = sectionSummary(sections, 'competitiveAdvantages');
  const services = sectionSummary(sections, 'services');
  const recurringFocus =
    /recurring/i.test(firstFocus || '') ||
    /recurring/i.test(services) ||
    /commercial cleaning/i.test(firstFocus || '');
  const reliabilityTheme =
    /reliab|trust|without needing to chase|responsiv/i.test(advantages) ||
    /lowest price|reliability/i.test(avoidSummary);

  let focusSegment = opts.focusSegment || null;
  if (focusSegment && /_/.test(String(focusSegment)) && !/\s/.test(String(focusSegment))) {
    // Accept snake_case keys from growth state (property_managers → property managers).
    const fromKey = String(focusSegment).replace(/_/g, ' ');
    focusSegment =
      findSegmentByHint(segments, [fromKey, String(focusSegment).replace(/_/g, ' ')]) ||
      fromKey;
  }
  if (!focusSegment) {
    focusSegment = resolveValidationFocusSegment(
      opts.userMessage || '',
      segments,
      opts.priorSegmentRanking || null
    );
  }
  const ctx = {
    businessName,
    primaryArea,
    firstFocus,
    avoidPhrase,
    recurringFocus,
    reliabilityTheme,
  };

  const isPropertyManager = /property manager/i.test(String(focusSegment || ''));
  const content = isPropertyManager
    ? propertyManagerValidationSections(ctx)
    : genericValidationSections(focusSegment, ctx);

  const title = isPropertyManager
    ? 'Property Manager Validation Target'
    : `${displaySegmentName(focusSegment || 'Segment')} Validation Target`;

  return {
    kind: VALIDATION_TARGET_KIND,
    title,
    intent: opts.intent || VALIDATION_TARGET_INTENT,
    focusSegment: focusSegment || null,
    displaySegment: displaySegmentName(focusSegment || 'segment'),
    // Flat Validation Target fields (required growth artifact shape).
    target_segment: toSegmentKey(focusSegment),
    best_fit_subtype: content.bestFirstType ? content.bestFirstType.body : null,
    property_size_or_context: content.propertySizeType
      ? content.propertySizeType.body
      : null,
    likely_pain_point: content.painPoint ? content.painPoint.body : null,
    credibility_proof_needed: content.proof ? content.proof.body : null,
    early_signals: content.earlySignals ? content.earlySignals.bullets || [] : [],
    first_30_day_success_definition: content.first30Days
      ? content.first30Days.body
      : null,
    cautions: content.cautions ? content.cautions.body : null,
    sections: content,
    confidence: VALIDATION_TARGET_CONFIDENCE,
    confidenceLevel: 'directional',
    marketValidated: false,
    firstFocus: firstFocus || null,
    primaryArea: primaryArea || null,
    businessName: businessName || null,
    priorRankingIntent:
      (opts.priorSegmentRanking && opts.priorSegmentRanking.intent) || null,
    blueprintId: (blueprint && blueprint.id) || gd.blueprintId || null,
    blueprintVersion:
      (blueprint && blueprint.version) || gd.blueprintVersion || null,
  };
}

/**
 * Format a Validation Target artifact as the Growth Conversation message.
 */
function formatValidationTargetMessage(target) {
  const t = target || {};
  const s = t.sections || {};
  const lines = [t.title || 'Validation Target', ''];

  const pushSection = (key, index) => {
    const section = s[key];
    if (!section) return;
    lines.push(`${index}. ${section.label}`);
    if (section.body) lines.push(section.body);
    if (Array.isArray(section.bullets)) {
      for (const b of section.bullets) lines.push(`- ${b}`);
    }
    lines.push('');
  };

  pushSection('bestFirstType', 1);
  pushSection('propertySizeType', 2);
  pushSection('painPoint', 3);
  pushSection('proof', 4);
  pushSection('earlySignals', 5);
  pushSection('first30Days', 6);
  pushSection('cautions', 7);

  lines.push('Confidence:');
  lines.push(t.confidence || VALIDATION_TARGET_CONFIDENCE);

  return lines.join('\n').trim();
}

/**
 * Max opening message when the Growth Conversation begins.
 */
function buildGrowthConversationOpening(growthDirection) {
  const gd = growthDirection || {};
  const name = shortName(gd.businessName || 'the business');
  const focus = gd.firstFocus || 'the first focus from the Blueprint';
  const primary = gd.primaryArea || null;
  const towns = gd.towns || [];
  const segments = (gd.segmentsToInspect || []).slice(0, 4);
  const segmentHint = segments.length
    ? naturalList(segments)
    : 'the ideal-customer segments in the Blueprint';
  const marketHint = primary
    ? towns.length
      ? `${primary}, especially ${naturalList(towns.slice(0, 4))}`
      : primary
    : towns.length
      ? naturalList(towns.slice(0, 4))
      : 'the markets named in the Blueprint';

  return [
    `Let's grow from the approved Blueprint.`,
    ``,
    `Directional first focus for ${name}: ${focus}${primary ? ` in ${primary}` : ''}.`,
    ``,
    `I'd start by comparing ${segmentHint} across ${marketHint}.`,
    ``,
    `This is still a conversation — not a campaign, not a prospect list, and not a claim that the market is validated.`,
    ``,
    `Where should we dig first: the segment mix, the market bound, or what success should look like in the next ninety days?`,
  ].join('\n');
}

/**
 * Generic segment-mix fallback — only when no ranking / primary decision exists
 * (or the user explicitly asks to restart / compare again).
 */
function buildGenericSegmentMixFallback(segments, avoid) {
  return [
    segments.length
      ? `From the Blueprint, the segments worth comparing first are ${naturalList(segments)}.`
      : `The Blueprint's ideal-customer section is the place to sharpen segments before we rank anything.`,
    avoid
      ? `We should also keep the avoid list in view so the first focus stays selective.`
      : `We should keep selectivity explicit so growth talk does not drift into anyone-with-a-budget.`,
    ``,
    `Still directional — next we can bound the market or define what a good first win looks like.`,
  ].join('\n');
}

function canShowGenericSegmentMixFallback(state, userMessage) {
  const msg = String(userMessage || '').toLowerCase();
  if (isRestartSegmentRequest(msg)) return true;
  if (state.primary_segment) return false;
  if (state.segment_ranking) return false;
  if (
    Array.isArray(state.completed_steps) &&
    (state.completed_steps.includes('rank_segments') ||
      state.completed_steps.includes('select_primary_segment') ||
      state.completed_steps.includes('define_validation_target'))
  ) {
    return false;
  }
  return true;
}

/**
 * Deterministic follow-up for the Growth Conversation (no campaign generation).
 * Stateful: remembers focus area, segment ranking, primary/secondary selection,
 * and advances toward Validation Target instead of looping the segment-mix menu.
 *
 * @param {string} userMessage
 * @param {object} growthDirection
 * @param {object} [blueprint]
 * @param {{ priorSegmentRanking?: object|null, growthState?: object|null }} [opts]
 * @returns {{ message: string, intent: string|null, segmentRanking: object|null, validationTarget: object|null, firstSegmentDecision: object|null, growthState: object }}
 */
function buildGrowthConversationReply(
  userMessage,
  growthDirection,
  blueprint,
  opts = {}
) {
  const gd = growthDirection || {};
  const priorState = normalizeGrowthState({
    ...(opts.growthState || {}),
    segment_ranking:
      (opts.growthState &&
        (opts.growthState.segment_ranking || opts.growthState.segmentRanking)) ||
      opts.priorSegmentRanking ||
      null,
  });
  const intent = detectGrowthConversationIntent(userMessage, {
    growthState: priorState,
  });
  const focus = gd.firstFocus || 'the Blueprint first focus';
  const segments = gd.segmentsToInspect || [];
  const primary = gd.primaryArea || null;
  const towns = gd.towns || [];
  const markets = gd.marketsToInspect || [];
  const sections = (blueprint && blueprint.sections) || {};
  const goals = sectionSummary(sections, 'campaignGoals');
  const avoid = sectionSummary(sections, 'avoidCustomers');

  const baseReply = {
    segmentRanking: null,
    validationTarget: null,
    firstSegmentDecision: null,
  };

  if (intent === VALIDATION_TARGET_INTENT) {
    const ranking = priorState.segment_ranking;
    const validationTarget = buildValidationTarget(gd, blueprint, {
      intent,
      userMessage,
      priorSegmentRanking: ranking,
      focusSegment: priorState.primary_segment || null,
    });
    const nextState = {
      ...priorState,
      current_growth_step: 'define_validation_target',
      completed_steps: markStepCompleted(
        {
          completed_steps: markStepCompleted(priorState, 'select_primary_segment'),
        },
        'define_validation_target'
      ),
      validation_target: validationTarget,
      confidence_level: 'directional',
    };
    if (priorState.primary_segment) {
      nextState.primary_segment = priorState.primary_segment;
    } else if (validationTarget.target_segment) {
      nextState.primary_segment = validationTarget.target_segment;
      nextState.completed_steps = markStepCompleted(
        nextState,
        'select_primary_segment'
      );
    }
    return {
      ...baseReply,
      message: formatValidationTargetMessage(validationTarget),
      intent,
      validationTarget,
      growthState: nextState,
    };
  }

  if (intent === SELECT_PRIMARY_INTENT) {
    const selection = resolveSegmentSelection(
      userMessage,
      priorState.segment_ranking
    );
    const decision = buildFirstSegmentDecision(selection);
    const validationTarget = buildValidationTarget(gd, blueprint, {
      intent: VALIDATION_TARGET_INTENT,
      userMessage,
      priorSegmentRanking: priorState.segment_ranking,
      focusSegment: selection.primaryDisplay || selection.primary,
    });
    const primaryLabel = selection.primaryDisplay || 'the chosen segment';
    const secondaryLabel = selection.secondaryDisplay;
    const ack = secondaryLabel
      ? `Good. I'll treat ${primaryLabel} as the first segment to validate and ${secondaryLabel} as the secondary path. Next, let's define what a good first win looks like before any campaign or prospect list.`
      : `Good. I'll treat ${primaryLabel} as the first segment to validate. Next, let's define what a good first win looks like before any campaign or prospect list.`;
    const nextState = {
      ...priorState,
      selected_focus_area:
        priorState.selected_focus_area === 'unknown'
          ? 'segment_mix'
          : priorState.selected_focus_area,
      primary_segment: selection.primary,
      secondary_segment: selection.secondary,
      held_segments: selection.held || [],
      deprioritized_segments: selection.deprioritized || [],
      current_growth_step: 'define_validation_target',
      completed_steps: markStepCompleted(
        {
          completed_steps: markStepCompleted(
            {
              completed_steps: markStepCompleted(priorState, 'rank_segments'),
            },
            'select_primary_segment'
          ),
        },
        'define_validation_target'
      ),
      first_segment_decision: decision,
      validation_target: validationTarget,
      confidence_level: 'directional',
    };
    return {
      ...baseReply,
      message: [
        ack,
        '',
        formatFirstSegmentDecisionMessage(decision),
        '',
        formatValidationTargetMessage(validationTarget),
      ].join('\n'),
      intent,
      firstSegmentDecision: decision,
      validationTarget,
      growthState: nextState,
    };
  }

  if (intent && RANKING_INTENTS.includes(intent)) {
    const segmentRanking = buildSegmentRanking(gd, blueprint, { intent });
    const nextState = {
      ...priorState,
      selected_focus_area:
        priorState.selected_focus_area === 'unknown'
          ? 'segment_mix'
          : priorState.selected_focus_area,
      segment_ranking: segmentRanking,
      current_growth_step: 'select_primary_segment',
      completed_steps: markStepCompleted(
        {
          completed_steps: markStepCompleted(priorState, 'choose_focus_area'),
        },
        'rank_segments'
      ),
      confidence_level: 'directional',
    };
    return {
      ...baseReply,
      message: formatSegmentRankingMessage(segmentRanking),
      intent,
      segmentRanking,
      growthState: nextState,
    };
  }

  if (intent === 'dig_segments') {
    const focusArea = detectSelectedFocusArea(String(userMessage || '').toLowerCase()) ||
      'segment_mix';
    const restart = isRestartSegmentRequest(String(userMessage || '').toLowerCase());

    // Primary already chosen → advance, do not re-list segments.
    if (priorState.primary_segment && !restart) {
      const validationTarget = buildValidationTarget(gd, blueprint, {
        intent: VALIDATION_TARGET_INTENT,
        userMessage,
        priorSegmentRanking: priorState.segment_ranking,
        focusSegment: priorState.primary_segment,
      });
      const nextState = {
        ...priorState,
        selected_focus_area: 'segment_mix',
        current_growth_step: 'define_validation_target',
        completed_steps: markStepCompleted(priorState, 'define_validation_target'),
        validation_target: validationTarget,
      };
      const primaryLabel =
        (priorState.first_segment_decision &&
          priorState.first_segment_decision.primarySegmentDisplay) ||
        String(priorState.primary_segment).replace(/_/g, ' ');
      return {
        ...baseReply,
        message: [
          `We've already chosen ${primaryLabel} as the first segment to validate.`,
          `Next is the validation target — what a good first win looks like before any campaign or prospect list.`,
          '',
          formatValidationTargetMessage(validationTarget),
        ].join('\n'),
        intent: VALIDATION_TARGET_INTENT,
        validationTarget,
        growthState: nextState,
      };
    }

    // Ranking exists but primary not locked → ask for selection, don't loop mix.
    if (priorState.segment_ranking && !restart) {
      const best =
        rankingRoleSegment(priorState.segment_ranking, 'best_first') ||
        'the top-ranked segment';
      const second = rankingRoleSegment(priorState.segment_ranking, 'second_best');
      const nextState = {
        ...priorState,
        selected_focus_area: 'segment_mix',
        current_growth_step: 'select_primary_segment',
        completed_steps: markStepCompleted(priorState, 'choose_focus_area'),
      };
      return {
        ...baseReply,
        message: [
          `We already have a Segment Ranking — ${best} leads${second ? `, with ${second} as the secondary path` : ''}.`,
          `Which first segment do you want to validate, or shall I lock in that ranking recommendation?`,
        ].join('\n'),
        intent,
        growthState: nextState,
      };
    }

    // True first dig into segments — generic mix only when state is empty.
    if (canShowGenericSegmentMixFallback(priorState, userMessage) || restart) {
      const nextState = {
        ...priorState,
        selected_focus_area: focusArea,
        current_growth_step: 'rank_segments',
        completed_steps: markStepCompleted(priorState, 'choose_focus_area'),
        ...(restart
          ? {
              primary_segment: null,
              secondary_segment: null,
              segment_ranking: null,
              first_segment_decision: null,
              validation_target: null,
              held_segments: [],
              deprioritized_segments: [],
            }
          : {}),
      };
      return {
        ...baseReply,
        message: buildGenericSegmentMixFallback(segments, avoid),
        intent,
        growthState: nextState,
      };
    }

    // State-aware safety net (should be rare).
    return {
      ...baseReply,
      message: [
        `We're past the open segment-mix list.`,
        priorState.segment_ranking
          ? `Use the Segment Ranking we already have, or name the first segment to validate.`
          : `Tell me which Blueprint segment should be first to validate.`,
      ].join('\n'),
      intent,
      growthState: {
        ...priorState,
        selected_focus_area: 'segment_mix',
        current_growth_step: priorState.segment_ranking
          ? 'select_primary_segment'
          : 'rank_segments',
      },
    };
  }

  if (intent === 'dig_markets') {
    const marketLine = primary
      ? towns.length
        ? `From the Blueprint, I'd bound the market to ${primary}, especially ${naturalList(towns)}.`
        : `From the Blueprint, I'd bound the market to ${primary}.`
      : markets.length
        ? `From the Blueprint, the markets I'd compare first are ${naturalList(markets)}.`
        : `The Blueprint's target-markets section is the bound I'd use before widening.`;
    const nextState = {
      ...priorState,
      selected_focus_area: 'market_bound',
      completed_steps: markStepCompleted(priorState, 'choose_focus_area'),
    };
    return {
      ...baseReply,
      message: [
        marketLine,
        `I would not treat that as validated demand yet — only as the approved geographic focus.`,
        ``,
        priorState.primary_segment
          ? `We've already locked ${String(priorState.primary_segment).replace(/_/g, ' ')} as the first segment — want to define the validation target next?`
          : `Want to pressure-test the segment mix next, or define the ninety-day success picture?`,
      ].join('\n'),
      intent,
      growthState: nextState,
    };
  }

  if (intent === 'dig_success') {
    const nextState = {
      ...priorState,
      selected_focus_area: 'success_definition',
      completed_steps: markStepCompleted(priorState, 'choose_focus_area'),
      current_growth_step: priorState.primary_segment
        ? 'define_validation_target'
        : priorState.current_growth_step,
    };
    return {
      ...baseReply,
      message: [
        goals
          ? `The Blueprint already names near-term outcomes: ${firstSentence(goals)}`
          : `The Blueprint's campaign-goals and success-metrics sections are the yardstick for this conversation.`,
        `We can translate those into a sharper “first win” definition — still without launching campaigns or building prospect lists.`,
        ``,
        priorState.primary_segment
          ? `Shall we define the validation target for ${String(priorState.primary_segment).replace(/_/g, ' ')} next?`
          : `Shall we lock the first focus as “${focus}”, or refine the segment/market bound first?`,
      ].join('\n'),
      intent,
      growthState: nextState,
    };
  }

  // Holding reply — still state-aware when decisions already exist.
  if (priorState.primary_segment) {
    return {
      ...baseReply,
      message: [
        `Holding the Growth Conversation state: primary segment is ${String(priorState.primary_segment).replace(/_/g, ' ')}${priorState.secondary_segment ? `, secondary ${String(priorState.secondary_segment).replace(/_/g, ' ')}` : ''}.`,
        `Next step is ${priorState.current_growth_step === 'define_validation_target' || priorState.validation_target ? 'refining the validation target / first-win criteria' : 'defining the validation target'} — still before campaigns or prospect lists.`,
      ].join('\n'),
      intent: null,
      growthState: priorState,
    };
  }
  if (priorState.segment_ranking) {
    const best = rankingRoleSegment(priorState.segment_ranking, 'best_first');
    return {
      ...baseReply,
      message: [
        `Holding the Segment Ranking — ${best || 'the top segment'} is the current directional lead.`,
        `Tell me which first segment to validate, or ask me to define what a good first win looks like.`,
      ].join('\n'),
      intent: null,
      growthState: {
        ...priorState,
        current_growth_step: 'select_primary_segment',
      },
    };
  }

  return {
    ...baseReply,
    message: [
      `Holding to the approved Blueprint, the directional first focus remains ${focus}.`,
      segments.length
        ? `Segments worth comparing: ${naturalList(segments.slice(0, 4))}.`
        : `Next I'd sharpen which segments the Blueprint implies we should compare first.`,
      primary
        ? `Market bound: ${primary}${towns.length ? `, especially ${naturalList(towns.slice(0, 4))}` : ''}.`
        : markets.length
          ? `Markets worth comparing: ${naturalList(markets.slice(0, 4))}.`
          : `Next I'd tighten the market bound from the Blueprint.`,
      ``,
      `Tell me whether to dig into segments, markets, or success criteria — and we'll keep this pre-strategy.`,
    ].join('\n'),
    intent: null,
    growthState: priorState,
  };
}

module.exports = {
  ARTIFACT_KIND,
  SEGMENT_RANKING_KIND,
  VALIDATION_TARGET_KIND,
  FIRST_SEGMENT_DECISION_KIND,
  DIRECTIONAL_LABEL,
  SEGMENT_RANKING_CONFIDENCE,
  VALIDATION_TARGET_CONFIDENCE,
  RANKING_INTENTS,
  VALIDATION_TARGET_INTENT,
  SELECT_PRIMARY_INTENT,
  FOCUS_AREAS,
  GROWTH_STEPS,
  buildInitialGrowthDirection,
  buildGrowthConversationOpening,
  buildGrowthConversationReply,
  detectGrowthConversationIntent,
  isDefineValidationTargetRequest,
  isSelectPrimarySegmentRequest,
  resolveSegmentSelection,
  normalizeGrowthState,
  emptyGrowthState,
  toSegmentKey,
  buildFirstSegmentDecision,
  formatFirstSegmentDecisionMessage,
  buildSegmentRanking,
  formatSegmentRankingMessage,
  buildValidationTarget,
  formatValidationTargetMessage,
  naturalList,
  splitGeography,
  extractFocusQualifier,
};