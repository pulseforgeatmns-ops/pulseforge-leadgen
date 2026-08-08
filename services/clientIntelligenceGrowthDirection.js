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
  'Directional only — not final strategy. Max has not validated this market yet.';

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

const SEGMENT_PATTERNS = [
  [/\bproperty managers?\b/i, 'property managers'],
  [/\bshort-?term rental(?:\s+companies)?\b/i, 'short-term rental companies'],
  [/\bfacility managers?\b/i, 'facility managers'],
  [/\bprofessional offices?\b/i, 'professional offices'],
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
    .slice(0, 5);
}

function extractGeographyHints(marketsSummary, facts) {
  if (facts && Array.isArray(facts.geography) && facts.geography.length) {
    return uniqueStrings(facts.geography).slice(0, 6);
  }
  const raw = String(marketsSummary || '');
  const places = [];
  const namedArea = raw.match(
    /\b(Greater Manchester(?:\s+area)?|Manchester(?:\s+NH)?|Charleston(?:\s+WV)?|Nashville(?:\s+TN)?|Myrtle Beach)\b/i
  );
  if (namedArea) places.push(namedArea[1].replace(/\s+/g, ' ').trim());
  for (const place of [
    'Bedford',
    'Hooksett',
    'Londonderry',
    'Auburn',
    'Goffstown',
    'Manchester',
  ]) {
    if (new RegExp(`\\b${place}\\b`, 'i').test(raw)) places.push(place);
  }
  if (places.length) return uniqueStrings(places).slice(0, 6);
  return splitPhrases(raw).slice(0, 4);
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
    if (fromFacts) return fromFacts;
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
    return uniqueStrings(facts.ideal_customers).slice(0, 5);
  }
  return extractSegments(sectionSummary(sections, 'idealCustomers')).slice(0, 5);
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
  const segments = resolveSegments(sections, facts);
  const markets = extractGeographyHints(
    sectionSummary(sections, 'targetMarkets'),
    facts
  );
  const marketLabel =
    markets.length > 1
      ? `${markets[0]} (${markets.slice(1, 4).join(', ')})`
      : markets[0] || 'the markets named in the Blueprint';
  const avoid = sectionSummary(sections, 'avoidCustomers');
  const advantages = sectionSummary(sections, 'competitiveAdvantages');
  const goals = sectionSummary(sections, 'campaignGoals');

  const heading = `${displayName}'s first growth focus`;

  const paragraphs = [];
  paragraphs.push(
    `Based on this Blueprint, ${possessive(displayName)} first growth focus should be ${firstFocus} in ${marketLabel}.`
  );

  const whyBits = [];
  if (sectionSummary(sections, 'services')) {
    whyBits.push('what the business delivers today');
  }
  if (segments.length) whyBits.push('who counts as an ideal customer');
  if (markets.length) whyBits.push('where the Blueprint says to concentrate');
  if (goals) whyBits.push('the near-term outcomes already named');
  paragraphs.push(
    whyBits.length
      ? `That focus follows directly from the approved understanding — ${whyBits.join(', ')} — not from market validation Max has not done yet.`
      : 'That focus follows from the approved Blueprint understanding, not from market validation Max has not done yet.'
  );

  if (segments.length && markets.length) {
    paragraphs.push(
      `First, Max would inspect ${segments.join('; ')} across ${markets.join(', ')}.`
    );
  } else if (segments.length) {
    paragraphs.push(
      `First segments Max would inspect: ${segments.join('; ')}.`
    );
  } else if (markets.length) {
    paragraphs.push(
      `First markets Max would inspect: ${markets.join(', ')}.`
    );
  } else {
    paragraphs.push(
      'First, Max would inspect the ideal-customer and market picture in the Blueprint before any outreach list is built.'
    );
  }

  if (avoid || advantages) {
    const extras = [];
    if (avoid) extras.push('who to decline');
    if (advantages) extras.push('why great-fit customers choose this business');
    paragraphs.push(
      `The Blueprint also clarifies ${extras.join(' and ')}, which keeps the first focus honest instead of chasing volume.`
    );
  }

  const nextConversation =
    'The next conversation will turn this directional read into a sharper growth plan: which segment to prioritize first, how tightly to bound the market, and what “good” looks like before any campaigns or prospect lists are created.';

  // Keep 3–5 body paragraphs; always end with the next-conversation preview.
  let body = paragraphs.slice(0, 4);
  body.push(nextConversation);
  while (body.length < 3) {
    body.unshift(
      'This is a directional preview from the approved Blueprint — useful for the next conversation, not a final strategy.'
    );
  }
  body = body.slice(0, 5);

  return {
    kind: ARTIFACT_KIND,
    title: 'Initial Growth Direction',
    heading,
    firstFocus,
    businessName,
    segmentsToInspect: segments,
    marketsToInspect: markets,
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
  const segments = (gd.segmentsToInspect || []).slice(0, 3);
  const markets = (gd.marketsToInspect || []).slice(0, 3);
  const segmentHint = segments.length
    ? segments.join(', ')
    : 'the ideal-customer segments in the Blueprint';
  const marketHint = markets.length
    ? markets.join(', ')
    : 'the markets named in the Blueprint';

  return [
    `Let's grow from the approved Blueprint.`,
    ``,
    `Directional first focus for ${name}: ${focus}.`,
    ``,
    `I'd start by inspecting ${segmentHint} across ${marketHint}.`,
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
  const markets = gd.marketsToInspect || [];
  const sections = (blueprint && blueprint.sections) || {};
  const goals = sectionSummary(sections, 'campaignGoals');
  const avoid = sectionSummary(sections, 'avoidCustomers');

  if (/segment|customer|icp|who\b/.test(msg)) {
    return [
      segments.length
        ? `From the Blueprint, the segments I'd inspect first are: ${segments.join('; ')}.`
        : `The Blueprint's ideal-customer section is the place to sharpen segments before we rank anything.`,
      avoid
        ? `We should also keep the avoid list in view so the first focus stays selective.`
        : `We should keep selectivity explicit so growth talk does not drift into anyone-with-a-budget.`,
      ``,
      `Still directional — next we can bound the market or define what a good first win looks like.`,
    ].join('\n');
  }

  if (/market|geo|area|region|city|where\b/.test(msg)) {
    return [
      markets.length
        ? `From the Blueprint, the markets I'd inspect first are: ${markets.join(', ')}.`
        : `The Blueprint's target-markets section is the bound I'd use before widening.`,
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
      ? `Segments to inspect: ${segments.slice(0, 4).join('; ')}.`
      : `Next I'd sharpen which segments the Blueprint implies we should inspect first.`,
    markets.length
      ? `Markets to inspect: ${markets.slice(0, 4).join(', ')}.`
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
};
