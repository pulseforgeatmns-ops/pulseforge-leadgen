'use strict';

/**
 * Initial Growth Direction — post-Blueprint-approval consultant preview.
 *
 * Understanding-only. Built strictly from the approved Business Blueprint
 * (plus optional CIE normalizedFacts). Never invents campaigns, prospect lists,
 * channels, or validated-market claims.
 */

const ARTIFACT_KIND = 'initial_growth_direction';
const DIRECTIONAL_LABEL =
  'This is a directional read, not market validation.';

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
 */
function buildGrowthConversationReply(userMessage, growthDirection, blueprint) {
  const gd = growthDirection || {};
  const msg = String(userMessage || '').trim().toLowerCase();
  const focus = gd.firstFocus || 'the Blueprint first focus';
  const segments = gd.segmentsToInspect || [];
  const primary = gd.primaryArea || null;
  const towns = gd.towns || [];
  const markets = gd.marketsToInspect || [];
  const sections = (blueprint && blueprint.sections) || {};
  const goals = sectionSummary(sections, 'campaignGoals');
  const avoid = sectionSummary(sections, 'avoidCustomers');

  if (/segment|customer|icp|who\b/.test(msg)) {
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

  if (/market|geo|area|region|city|where\b/.test(msg)) {
    const marketLine = primary
      ? towns.length
        ? `From the Blueprint, I'd bound the market to ${primary}, especially ${naturalList(towns)}.`
        : `From the Blueprint, I'd bound the market to ${primary}.`
      : markets.length
        ? `From the Blueprint, the markets I'd compare first are ${naturalList(markets)}.`
        : `The Blueprint's target-markets section is the bound I'd use before widening.`;
    return [
      marketLine,
      `I would not treat that as validated demand yet — only as the approved geographic focus.`,
      ``,
      `Want to pressure-test the segment mix next, or define the ninety-day success picture?`,
    ].join('\n');
  }

  if (/success|metric|90|ninety|goal|win\b/.test(msg)) {
    return [
      goals
        ? `The Blueprint already names near-term outcomes: ${firstSentence(goals)}`
        : `The Blueprint's campaign-goals and success-metrics sections are the yardstick for this conversation.`,
      `We can translate those into a sharper “first win” definition — still without launching campaigns or building prospect lists.`,
      ``,
      `Shall we lock the first focus as “${focus}”, or refine the segment/market bound first?`,
    ].join('\n');
  }

  return [
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
  ].join('\n');
}

module.exports = {
  ARTIFACT_KIND,
  DIRECTIONAL_LABEL,
  buildInitialGrowthDirection,
  buildGrowthConversationOpening,
  buildGrowthConversationReply,
  naturalList,
  splitGeography,
  extractFocusQualifier,
};
