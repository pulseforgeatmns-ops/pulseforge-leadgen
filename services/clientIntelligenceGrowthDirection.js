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
 * Detect Growth Conversation intents from a user message.
 * Validation-target intents win over ranking so follow-ups like
 * “good first win” / “best first test” do not repeat Segment Ranking.
 * Ranking / directional-call intents are returned before generic dig intents.
 *
 * @param {string} userMessage
 * @returns {string|null}
 */
function detectGrowthConversationIntent(userMessage) {
  const msg = String(userMessage || '').trim().toLowerCase();
  if (!msg) return null;

  if (isDefineValidationTargetRequest(msg)) {
    return VALIDATION_TARGET_INTENT;
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

  const focusSegment = resolveValidationFocusSegment(
    opts.userMessage || '',
    segments,
    opts.priorSegmentRanking || null
  );
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
 * Deterministic follow-up for the Growth Conversation (no campaign generation).
 * Validation-target intents return a first-win definition. Ranking / compare /
 * prioritize / directional-call intents return a Segment Ranking artifact
 * instead of repeating the generic segment-mix prompt.
 *
 * @param {string} userMessage
 * @param {object} growthDirection
 * @param {object} [blueprint]
 * @param {{ priorSegmentRanking?: object|null }} [opts]
 * @returns {{ message: string, intent: string|null, segmentRanking: object|null, validationTarget: object|null }}
 */
function buildGrowthConversationReply(
  userMessage,
  growthDirection,
  blueprint,
  opts = {}
) {
  const gd = growthDirection || {};
  const intent = detectGrowthConversationIntent(userMessage);
  const focus = gd.firstFocus || 'the Blueprint first focus';
  const segments = gd.segmentsToInspect || [];
  const primary = gd.primaryArea || null;
  const towns = gd.towns || [];
  const markets = gd.marketsToInspect || [];
  const sections = (blueprint && blueprint.sections) || {};
  const goals = sectionSummary(sections, 'campaignGoals');
  const avoid = sectionSummary(sections, 'avoidCustomers');

  if (intent === VALIDATION_TARGET_INTENT) {
    const validationTarget = buildValidationTarget(gd, blueprint, {
      intent,
      userMessage,
      priorSegmentRanking: opts.priorSegmentRanking || null,
    });
    return {
      message: formatValidationTargetMessage(validationTarget),
      intent,
      segmentRanking: null,
      validationTarget,
    };
  }

  if (intent && RANKING_INTENTS.includes(intent)) {
    const segmentRanking = buildSegmentRanking(gd, blueprint, { intent });
    return {
      message: formatSegmentRankingMessage(segmentRanking),
      intent,
      segmentRanking,
      validationTarget: null,
    };
  }

  if (intent === 'dig_segments') {
    return {
      message: [
        segments.length
          ? `From the Blueprint, the segments worth comparing first are ${naturalList(segments)}.`
          : `The Blueprint's ideal-customer section is the place to sharpen segments before we rank anything.`,
        avoid
          ? `We should also keep the avoid list in view so the first focus stays selective.`
          : `We should keep selectivity explicit so growth talk does not drift into anyone-with-a-budget.`,
        ``,
        `Still directional — next we can bound the market or define what a good first win looks like.`,
      ].join('\n'),
      intent,
      segmentRanking: null,
      validationTarget: null,
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
    return {
      message: [
        marketLine,
        `I would not treat that as validated demand yet — only as the approved geographic focus.`,
        ``,
        `Want to pressure-test the segment mix next, or define the ninety-day success picture?`,
      ].join('\n'),
      intent,
      segmentRanking: null,
      validationTarget: null,
    };
  }

  if (intent === 'dig_success') {
    return {
      message: [
        goals
          ? `The Blueprint already names near-term outcomes: ${firstSentence(goals)}`
          : `The Blueprint's campaign-goals and success-metrics sections are the yardstick for this conversation.`,
        `We can translate those into a sharper “first win” definition — still without launching campaigns or building prospect lists.`,
        ``,
        `Shall we lock the first focus as “${focus}”, or refine the segment/market bound first?`,
      ].join('\n'),
      intent,
      segmentRanking: null,
      validationTarget: null,
    };
  }

  return {
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
    segmentRanking: null,
    validationTarget: null,
  };
}

module.exports = {
  ARTIFACT_KIND,
  SEGMENT_RANKING_KIND,
  VALIDATION_TARGET_KIND,
  DIRECTIONAL_LABEL,
  SEGMENT_RANKING_CONFIDENCE,
  VALIDATION_TARGET_CONFIDENCE,
  RANKING_INTENTS,
  VALIDATION_TARGET_INTENT,
  buildInitialGrowthDirection,
  buildGrowthConversationOpening,
  buildGrowthConversationReply,
  detectGrowthConversationIntent,
  isDefineValidationTargetRequest,
  buildSegmentRanking,
  formatSegmentRankingMessage,
  buildValidationTarget,
  formatValidationTargetMessage,
  naturalList,
  splitGeography,
  extractFocusQualifier,
};