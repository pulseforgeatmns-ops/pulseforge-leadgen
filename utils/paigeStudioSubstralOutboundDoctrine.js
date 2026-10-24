'use strict';

/**
 * SPEC-PAIGE-SUBSTRAL-001 — Studio Substral humanized first-touch outbound doctrine.
 * Deterministic Paige copy for client_id=17 (studio_substral). Human review remains required.
 */

function normalizeDomain(value) {
  if (!value) return '';
  return String(value)
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, '')
    .replace(/^www\./, '')
    .replace(/\/.*$/, '');
}

function pickSpecificWebsiteIssue(options) {
  // Lazy load — batch export pulls WOI; only needed when resolving from scout rows.
  // eslint-disable-next-line global-require
  return require('../scripts/lib/studioSubstralBatch001Export').pickSpecificWebsiteIssue(options);
}

const SPEC_ID = 'SPEC-PAIGE-SUBSTRAL-001';
const DEFAULT_STUDIO_SUBSTRAL_CLIENT_ID = 17;
const DOCTRINE_BLOCKER = 'studio_substral_first_touch_doctrine_violation';
const RELATIONSHIP_HOLD_REASON = 'relationship_account_review_required';

const DEFAULT_SENDER_NAME = 'Jacob';
const SIGNATURE = DEFAULT_SENDER_NAME;

const PREFERRED_OPENING = 'I was looking at your site and saw';
const ACCEPTABLE_OPENING_RES = [
  /\bi was looking at your site and saw\b/i,
  /\bi was looking through your site and saw\b/i,
  /\bi took a look at your site and saw\b/i,
  /\bi was looking at .{2,120}'s site and saw\b/i,
];

const FORBIDDEN_PHRASES = Object.freeze([
  { id: 'i_noticed', re: /\bi noticed\b/i },
  { id: 'i_can_see', re: /\bi can see\b/i },
  { id: 'i_could_see', re: /\bi could see\b/i },
  { id: 'our_analysis', re: /\bour analysis indicates\b/i },
  { id: 'our_audit', re: /\bour audit detected\b/i },
  { id: 'credibility_gap', re: /\bcredibility gap\b/i },
  { id: 'conversion_path', re: /\bconversion path\b/i },
  { id: 'book_discovery', re: /\bbook a discovery call\b/i },
  { id: 'schedule_consultation', re: /\bschedule a consultation\b/i },
  { id: 'hop_on_call', re: /\blet's hop on a call\b/i },
  { id: 'act_now', re: /\bact now\b/i },
  { id: 'limited_availability', re: /\blimited availability\b/i },
  { id: 'diagnosis_before_design_pitch', re: /\bstructured website assessment \(diagnosis before design\)\b/i },
  { id: 'assessment_conversation', re: /\bassessment conversation\b/i },
]);

const FORBIDDEN_OPENINGS = Object.freeze([
  { id: 'noticed_opener', re: /\band noticed\b/i },
]);

const APPROVED_CTA_RES = Object.freeze([
  /\bhappy to send over a quick assessment if it(?:'|')d be useful\b/i,
  /\bwould you be open to me sending over a short website assessment\b/i,
  /\bif useful, i can send over a quick assessment of what i(?:'|')d prioritize\b/i,
  /\bhappy to send over a short assessment if useful\b/i,
]);

const DEFAULT_CTA = 'Happy to send over a quick assessment if it\'d be useful.';

const RELATIONSHIP_ACCOUNT_GUARDS = Object.freeze([
  {
    id: 'keyrenter_anchor',
    matchRe: /\bkeyrenter\b/i,
    domainRe: /(?:^|\.)keyrenter(?:newengland)?\.com\b/i,
    domains: ['keyrenternewengland.com', 'keyrenter.com'],
    reason: RELATIONSHIP_HOLD_REASON,
    detail: 'Existing Anchor Cleaning relationship — hold from generic Studio Substral cold outreach.',
  },
]);

const TECHNICAL_TO_HUMAN = Object.freeze([
  {
    test: (text, finding) => /homepage fetch took|fetch took \d+s/i.test(text)
      || finding?.id === 'perf_fetch_time' || finding?.id === 'perf_slow_fetch',
    human: 'the homepage was taking a while to load during my review',
    bridge: 'When the first page is slow, people often leave before they ever see what you actually do.',
  },
  {
    test: (text, finding) => /viewport|mobile friction|responsive layout|missing viewport/i.test(text)
      || finding?.id === 'mobile_no_viewport',
    human: 'the mobile experience could be working harder for you',
    bridge: 'On a phone, that can get in the way of a quick first impression before someone calls or books.',
  },
  {
    test: (text, finding) => /google review strength|review strength|not carried through on the homepage|weak trust proof/i.test(text)
      || (/review/i.test(text) && /homepage|home page/i.test(text)),
    human: 'you\'ve built a strong reputation online, but the homepage doesn\'t show that proof nearly as much as it could',
    bridge: 'For a local service business, that gap can slow trust before someone calls or requests a quote.',
  },
  {
    test: (text, finding) => /missing or empty page title|missing or empty document title|page title/i.test(text)
      || finding?.id === 'seo_missing_title',
    human: 'a few basic presentation details are underselling the business online, starting with how the site shows up in search and browser tabs',
    bridge: 'Small details like that can make an established business look less polished than it is in person.',
  },
  {
    test: (text, finding) => /contact path|no obvious phone|contact link|primary navigation does not include a contact|unclear homepage cta/i.test(text)
      || finding?.id === 'dom_no_contact_nav' || finding?.id === 'conv_no_obvious_path',
    human: 'it\'s harder than I expected to see the clearest next step from the homepage',
    bridge: 'When someone lands on the site, the path to call, email, or book should feel obvious.',
  },
  {
    test: (text) => /missing meta description/i.test(text) || /seo_missing_description/i.test(String(text)),
    human: 'search and social previews do not really reflect the business the way the rest of your reputation does',
    bridge: 'That first snippet is often what people see before they ever click through.',
  },
]);

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function hasEmDash(text) {
  return /[\u2014\u2013]/.test(asText(text));
}

function findPatternViolations(text, patterns) {
  const hay = asText(text);
  if (!hay) return [];
  return patterns.filter(({ re }) => re.test(hay)).map(({ id, re }) => ({
    patternId: id,
    match: hay.match(re)?.[0] || null,
  }));
}

function resolveStudioSubstralClientIdSync() {
  const configured = process.env.STUDIO_SUBSTRAL_CLIENT_ID;
  if (configured) {
    const id = Number(configured);
    if (Number.isSafeInteger(id) && id > 0) return id;
  }
  return DEFAULT_STUDIO_SUBSTRAL_CLIENT_ID;
}

function isStudioSubstralClient(clientId) {
  return Number(clientId) === resolveStudioSubstralClientIdSync();
}

function evaluateRelationshipAccountGuard({ companyName = '', website = '', domain = '' } = {}) {
  const normalizedDomain = normalizeDomain(domain || website);
  for (const guard of RELATIONSHIP_ACCOUNT_GUARDS) {
    if (guard.matchRe.test(companyName)) {
      return { held: true, reason: guard.reason, guardId: guard.id, detail: guard.detail };
    }
    if (normalizedDomain && guard.domains.some((d) => normalizedDomain === d || normalizedDomain.endsWith(`.${d}`))) {
      return { held: true, reason: guard.reason, guardId: guard.id, detail: guard.detail };
    }
    if (normalizedDomain && guard.domainRe && guard.domainRe.test(normalizedDomain)) {
      return { held: true, reason: guard.reason, guardId: guard.id, detail: guard.detail };
    }
  }
  return { held: false, reason: null, guardId: null, detail: null };
}

function resolveFirstName(firstName, decisionMakerName) {
  const direct = asText(firstName);
  if (direct) return direct.split(/\s+/)[0];
  const dm = asText(decisionMakerName);
  if (dm) return dm.split(/\s+/)[0];
  return null;
}

function buildGreeting(firstName, decisionMakerName) {
  const name = resolveFirstName(firstName, decisionMakerName);
  return name ? `Hi ${name},` : 'Hi there,';
}

function resolvePlainspokenOutcome(segment = '', companyName = '') {
  const hay = `${asText(segment)} ${asText(companyName)}`.toLowerCase();
  if (/property|management|pm\b/.test(hay)) {
    return 'make property-management websites clearer about services, trust, and the next step for owners and tenants';
  }
  if (/roof|hvac|plumb|home service|contractor|heating|air|plumbing/.test(hay)) {
    return 'make home-service websites easier to trust before someone calls for an estimate or repair';
  }
  if (/legal|law|accounting|professional/.test(hay)) {
    return 'help professional offices present credibility online before a prospect picks up the phone';
  }
  return 'make their website clearer and easier to trust for the people who find them online';
}

function humanizeWebsiteObservation(specificIssue = '', finding = null) {
  const text = asText(specificIssue);
  if (!text) return null;
  for (const rule of TECHNICAL_TO_HUMAN) {
    if (!rule.test(text, finding)) continue;
    const human = typeof rule.human === 'function' ? rule.human(text, finding) : rule.human;
    return {
      observation: human,
      bridge: rule.bridge,
    };
  }
  const cleaned = text
    .replace(/^weak trust proof\s*[—–-]\s*/i, '')
    .replace(/^mobile friction\s*[—–-]\s*/i, '')
    .replace(/^unclear homepage cta\s*[—–-]\s*/i, '')
    .replace(/^poor first-impression credibility\s*[—–-]\s*/i, '')
    .replace(/^confusing service explanation\s*[\/—–-]\s*conversion path\s*[—–-]\s*/i, '')
    .replace(/^confusing service explanation\s*[—–-]\s*/i, '')
    .replace(/\.$/, '')
    .trim();
  if (!cleaned || /\bconversion path\b/i.test(cleaned)) return null;
  const observation = cleaned.charAt(0).toLowerCase() + cleaned.slice(1);
  return {
    observation,
    bridge: 'That can make it harder for a new visitor to trust what they are seeing before they reach out.',
  };
}

function resolveSpecificIssueFromContext({
  intel = {},
  row = {},
  assessmentPayload = null,
  candidate = {},
} = {}) {
  const fromCandidate = asText(
    candidate.specificWebsiteIssue
    || candidate.specific_website_issue
    || candidate.websiteIssue
  );
  if (fromCandidate) return fromCandidate;

  const scoutIntel = intel.studio_scout_intelligence || intel;
  return pickSpecificWebsiteIssue({
    intel: scoutIntel,
    row: row || candidate,
    assessmentPayload,
    websitePainSummary: row.website_pain_summary || candidate.website_pain_summary,
  });
}

function buildOpeningLine({ companyName = '', useCompanyInOpening = false, observation }) {
  const obs = asText(observation);
  if (!obs) return null;
  if (useCompanyInOpening && companyName) {
    return `I was looking at ${companyName}'s site and saw ${obs.endsWith('.') ? obs : `${obs}.`}`;
  }
  return `${PREFERRED_OPENING} ${obs.endsWith('.') ? obs : `${obs}.`}`;
}

function buildStudioSubstralFirstTouchEmail({
  firstName = null,
  decisionMakerName = null,
  companyName = 'your team',
  segment = '',
  specificIssue = '',
  finding = null,
  assessmentPayload = null,
  intel = {},
  row = {},
  candidate = {},
  senderName = DEFAULT_SENDER_NAME,
  cta = DEFAULT_CTA,
  useCompanyInOpening = false,
} = {}) {
  const hold = evaluateRelationshipAccountGuard({
    companyName,
    website: row.website_url || candidate.website || candidate.website_url,
    domain: candidate.domain,
  });
  if (hold.held) {
    return {
      held: true,
      holdReason: hold.reason,
      guardId: hold.guardId,
      detail: hold.detail,
      subject: null,
      body: null,
      cta: null,
    };
  }

  const issue = specificIssue || resolveSpecificIssueFromContext({
    intel,
    row,
    assessmentPayload,
    candidate,
  });
  const humanized = humanizeWebsiteObservation(issue, finding);
  if (!humanized?.observation) {
    return {
      held: true,
      holdReason: 'insufficient_prospect_evidence',
      detail: 'No supported website observation for first-touch copy.',
      subject: null,
      body: null,
      cta: null,
    };
  }

  const greeting = buildGreeting(firstName, decisionMakerName);
  const opening = buildOpeningLine({
    companyName,
    useCompanyInOpening,
    observation: humanized.observation,
  });
  const outcome = resolvePlainspokenOutcome(
    segment || intel.studio_category || row.vertical,
    companyName
  );
  const subject = `Quick note on ${companyName}'s website`;

  const body = [
    greeting,
    '',
    opening,
    '',
    humanized.bridge,
    '',
    `I run Studio Substral and help local businesses ${outcome}.`,
    '',
    cta,
    '',
    senderName,
  ].join('\n');

  return {
    held: false,
    holdReason: null,
    subject,
    body,
    cta,
    observation: humanized.observation,
    specificIssue: issue,
  };
}

function validateStudioSubstralFirstTouchDoctrine({ subject = '', body = '', cta = '' } = {}) {
  const combined = [subject, body, cta].filter(Boolean).join('\n');
  const violations = [
    ...findPatternViolations(combined, FORBIDDEN_PHRASES),
    ...findPatternViolations(combined, FORBIDDEN_OPENINGS),
  ];

  if (hasEmDash(combined)) {
    violations.push({ patternId: 'em_dash', match: '—' });
  }

  if (/\bhttps?:\/\//i.test(body)) {
    violations.push({ patternId: 'link_in_first_touch', match: 'http' });
  }

  const bodyText = asText(body);
  if (bodyText && !ACCEPTABLE_OPENING_RES.some((re) => re.test(bodyText))) {
    violations.push({ patternId: 'missing_human_observation_opener', match: null });
  }

  const ctaText = asText(cta || body);
  if (ctaText && !APPROVED_CTA_RES.some((re) => re.test(ctaText))) {
    violations.push({ patternId: 'non_approved_cta', match: ctaText.split('\n').slice(-3).join(' ') });
  }

  return {
    ok: violations.length === 0,
    blocker: violations.length ? DOCTRINE_BLOCKER : null,
    violations,
  };
}

function buildPaigeFirstTouchDoctrineContext() {
  return {
    spec: SPEC_ID,
    scope: 'studio_substral_first_touch_outbound_only',
    human_review_required: true,
    voice: [
      'human',
      'concise',
      'observant',
      'commercially aware',
      'low-pressure',
      'local operator tone',
      'no agency fluff',
      'no AI-sounding phrasing',
    ],
    opening: {
      preferred: PREFERRED_OPENING,
      acceptable_variants: ACCEPTABLE_OPENING_RES.map((re) => re.source),
      avoid: ['I noticed…', 'I can see…', 'Our analysis indicates…'],
    },
    style_rules: [
      'No em dashes — use commas or separate sentences.',
      'Exactly one specific website observation per email, from real prospect evidence.',
      'Translate technical audit findings into normal human language.',
      'Do not insult the site or overstate the problem.',
      'No ROI or conversion lift claims without evidence.',
      'Keep first touch short; no links or attachments; no long Studio Substral pitch.',
      'Low-pressure assessment CTA only.',
    ],
    approved_cta_examples: [
      DEFAULT_CTA,
      'Would you be open to me sending over a short website assessment?',
      'If useful, I can send over a quick assessment of what I\'d prioritize.',
      'Happy to send over a short assessment if useful.',
    ],
    forbidden_cta_patterns: [
      'Book a discovery call',
      'Schedule a consultation',
      'Let\'s hop on a call',
      'Act now',
      'Limited availability',
    ],
    technical_to_human_examples: TECHNICAL_TO_HUMAN.slice(0, 5).map((rule) => ({
      maps_to: typeof rule.human === 'function' ? 'the homepage was taking a while to load during my review' : rule.human,
    })),
    relationship_account_guard: RELATIONSHIP_ACCOUNT_GUARDS.map((g) => ({
      id: g.id,
      hold_reason: g.reason,
      detail: g.detail,
    })),
  };
}

module.exports = {
  SPEC_ID,
  DEFAULT_STUDIO_SUBSTRAL_CLIENT_ID,
  DOCTRINE_BLOCKER,
  RELATIONSHIP_HOLD_REASON,
  DEFAULT_SENDER_NAME,
  SIGNATURE,
  DEFAULT_CTA,
  resolveStudioSubstralClientIdSync,
  isStudioSubstralClient,
  evaluateRelationshipAccountGuard,
  humanizeWebsiteObservation,
  buildStudioSubstralFirstTouchEmail,
  validateStudioSubstralFirstTouchDoctrine,
  buildPaigeFirstTouchDoctrineContext,
  buildOpeningLine,
};
