'use strict';

/**
 * Paige Anchor Copy Doctrine (client_id=10).
 *
 * Paige owns customer-facing copy quality. Other systems (knowledge stores, Astra,
 * acquisition metadata) may store approved copy or surface prior examples, but must
 * not draft Anchor voice, persuasion, or final wording.
 */

const { FACT_TYPES, PERSONALIZATION_STATUS } = require('./scoutPersonalizationEvidence');

const ANCHOR_CLIENT_ID = 10;
const ANCHOR_COPY_OWNER = 'paige';
const DEFAULT_SENDER_NAME = 'Jacob Maynard';
const DEFAULT_SERVICE_AREA = 'Manchester';

const LIFECYCLE_STAGES = Object.freeze({
  COLD: 'cold',
  FIRST_FOLLOW_UP: 'first_follow_up',
  PROPOSAL_FOLLOW_UP: 'proposal_follow_up',
  PRICE_CONCERN: 'price_concern',
  COMPARISON_QUOTE: 'comparison_quote',
  CURRENT_CUSTOMER: 'current_customer',
  FORWARDABLE_BLURB: 'forwardable_blurb',
});

const SEGMENTS = Object.freeze({
  OFFICE: 'office',
  PROPERTY_MANAGEMENT: 'property_management',
  PROFESSIONAL_OFFICE: 'professional_office',
  LAW_FIRM: 'law_firm',
  ACCOUNTING: 'accounting',
});

const AI_TELLS = Object.freeze([
  { id: 'not_just_but', re: /\bnot just\b[^.]{0,80}\bbut\b/i },
  { id: 'fast_paced_world', re: /\bin today'?s fast[- ]paced world\b/i },
  { id: 'wanted_to_reach_out', re: /\bi wanted to reach out\b/i },
  { id: 'just_checking_in', re: /\bjust checking in\b/i },
  { id: 'hope_this_finds', re: /\bhope this finds you well\b/i },
  { id: 'i_saw_observation', re: /\bi saw\b/i },
  { id: 'i_observed', re: /\bi observed that\b/i },
  { id: 'i noticed_your', re: /\bi noticed your\b/i },
  { id: 'i noticed that', re: /\bi noticed that\b/i },
]);

const GENERIC_CLOSERS = Object.freeze([
  { id: 'worth_quick_look', re: /\bworth a quick look\b/i },
  { id: 'worth_conversation', re: /\bworth a (?:quick )?conversation\b/i },
  { id: 'open_to_call', re: /\bwould you be open to a quick call\b/i },
  { id: 'any_interest', re: /\bany interest\b/i },
  { id: 'let_me_know_helpful', re: /\blet me know if this is helpful\b/i },
  { id: 'learn_more', re: /\bwould you like to learn more\b/i },
  { id: 'would_it_be_useful', re: /\bwould it be useful for us to put a quote together\b/i },
  { id: 'worth_quick_conversation', re: /\bif that(?:'|')s worth a quick conversation\b/i },
  { id: 'happy_to_connect', re: /\bi(?:'|')d be happy to connect\b/i },
  { id: 'stop_by_and_learn', re: /\bstop by and learn how you currently handle\b/i },
]);

const DISALLOWED_PHRASES = Object.freeze([
  { id: 'know_you_need_cleaner', re: /\bi know you need a cleaner\b/i },
  { id: 'since_expanding', re: /\bsince you(?:'|')re expanding, you probably need\b/i },
  { id: 'current_cleaner_may_not', re: /\bi noticed your current cleaner may not\b/i },
  { id: 'must_be_looking', re: /\byou must be looking for\b/i },
  { id: 'follow_up_needs', re: /\bi wanted to follow up on your cleaning needs\b/i },
  { id: 'need_cleaning_urgently', re: /\bneed(?:s|ed)? cleaning (?:now|urgently|immediately)\b/i },
  { id: 'unhappy_cleaner', re: /\bunhappy with (?:your|the) (?:current )?cleaner\b/i },
  { id: 'reaching_out_to_replace', re: /\bi(?:'|')m not reaching out to replace anyone\b/i },
  { id: 'trying_to_replace', re: /\bi(?:'|')m not trying to replace anyone\b/i },
  { id: 'wanted_to_see_if_useful', re: /\bi wanted to see if it would be useful\b/i },
]);

const DOCTRINE_BLOCKER = 'anchor_copy_doctrine_violation';

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function hasEmDash(text) {
  return /[\u2014\u2013]/.test(asText(text));
}

function buildGreeting(firstName) {
  const name = asText(firstName);
  return name ? `Hi ${name},` : 'Hi,';
}

function buildSignature(senderName = DEFAULT_SENDER_NAME) {
  return [
    senderName,
    'Anchor Cleaning',
    'jacob@goanchorcleaning.com',
    '(603) 420-2430',
  ].join('\n');
}

function resolveServiceAreaLabel(plan = {}, company = {}) {
  const geo = plan.geography || plan.market?.geography || {};
  const label = asText(geo.label || geo.city || geo.region);
  if (label) return label.replace(/,\s*NH$/i, '').trim() || label;
  const locality = asText(company?.places_locality || company?.location);
  if (locality) return locality.replace(/,\s*NH$/i, '').trim();
  return DEFAULT_SERVICE_AREA;
}

function normalizeSegmentText(plan = {}, mission = {}) {
  return [
    plan.market?.segment,
    plan.market?.label,
    plan.market?.industry,
    mission?.targetSegment,
    mission?.objective,
    plan.objective,
  ].filter(Boolean).join(' ').toLowerCase();
}

function resolveSegment(plan = {}, mission = {}, evidence = null) {
  const hay = normalizeSegmentText(plan, mission);
  if (/\b(str|short[- ]term rental|property manag|vacation rental|turnover)\b/i.test(hay)) {
    return SEGMENTS.PROPERTY_MANAGEMENT;
  }
  if (/\b(law firm|attorney|legal)\b/i.test(hay)) return SEGMENTS.LAW_FIRM;
  if (/\b(accounting|cpa|bookkeep)\b/i.test(hay)) return SEGMENTS.ACCOUNTING;
  if (/\b(professional|office|law|accounting)\b/i.test(hay)) return SEGMENTS.PROFESSIONAL_OFFICE;
  return SEGMENTS.OFFICE;
}

function isPropertyManagementSegment(segment) {
  return segment === SEGMENTS.PROPERTY_MANAGEMENT;
}

function buildScopeParagraph(serviceArea = DEFAULT_SERVICE_AREA) {
  return [
    `I'm {{sender}} with Anchor Cleaning. We handle recurring commercial cleaning around ${serviceArea}.`,
    '',
    'A lot of cleaning quotes are clear on price, but vague on what actually gets done. We try to make that part simple: walk the space, agree on the areas and frequency, then put the quote in writing so you know what is covered before deciding.',
  ].join('\n');
}

function renderScopeParagraph(senderName, serviceArea) {
  return buildScopeParagraph(serviceArea).replace('{{sender}}', senderName);
}

function buildDefaultColdClose({ segment = SEGMENTS.OFFICE, serviceType = null } = {}) {
  if (isPropertyManagementSegment(segment)) {
    return 'Want me to send over what we\'d need for a quote on one property first?';
  }
  if (serviceType === 'backup') {
    return 'Want me to send over our backup-cleaning info?';
  }
  return 'Want me to send over what we\'d need to price the office properly?';
}

function buildDefaultColdEmail({
  firstName = null,
  companyName = 'your team',
  senderName = DEFAULT_SENDER_NAME,
  serviceArea = DEFAULT_SERVICE_AREA,
  segment = SEGMENTS.OFFICE,
  serviceType = null,
} = {}) {
  const greeting = buildGreeting(firstName);
  const subject = `Cleaning for ${companyName}`;
  const body = [
    greeting,
    '',
    renderScopeParagraph(senderName, serviceArea),
    '',
    buildDefaultColdClose({ segment, serviceType }),
    '',
    buildSignature(senderName),
  ].join('\n');
  const cta = buildDefaultColdClose({ segment, serviceType });
  return { subject, body, cta };
}

function buildFirstFollowUpEmail({
  firstName = null,
  companyName = 'your team',
  senderName = DEFAULT_SENDER_NAME,
  segment = SEGMENTS.OFFICE,
} = {}) {
  const greeting = buildGreeting(firstName);
  const subject = `Cleaning for ${companyName}`;
  const segmentNote = isPropertyManagementSegment(segment)
    ? 'If you already have it covered, no problem. If you are comparing options or want backup across properties, we can give you a written quote that shows the areas, frequency, and price clearly.'
    : 'If you already have it covered, no problem. If you are comparing options or want a backup, we can give you a written quote that shows the areas, frequency, and price clearly.';
  const body = [
    greeting,
    '',
    `Following up on cleaning for ${companyName}.`,
    '',
    segmentNote,
    '',
    'Want me to send over what we\'d need for a quote?',
    '',
    buildSignature(senderName),
  ].join('\n');
  return {
    subject,
    body,
    cta: 'Want me to send over what we\'d need for a quote?',
  };
}

function buildProposalFollowUpEmail({
  firstName = null,
  senderName = DEFAULT_SENDER_NAME,
} = {}) {
  const greeting = buildGreeting(firstName);
  const body = [
    greeting,
    '',
    'Looking at the quote, the main question is whether the frequency fits how the space is actually used.',
    '',
    'Is there any area you\'re worried would not hold up between cleans?',
    '',
    buildSignature(senderName),
  ].join('\n');
  return {
    subject: 'Re: cleaning quote',
    body,
    cta: 'Reply with any area you\'re worried would not hold up between cleans',
  };
}

function buildPriceConcernReply({
  firstName = null,
  senderName = DEFAULT_SENDER_NAME,
} = {}) {
  const greeting = buildGreeting(firstName);
  const body = [
    greeting,
    '',
    'Is it above the budget you had in mind, or does the scope not line up with the price?',
    '',
    buildSignature(senderName),
  ].join('\n');
  return {
    subject: 'Re: cleaning quote',
    body,
    cta: 'Reply with whether budget or scope is the concern',
  };
}

function buildComparisonQuoteReply({
  firstName = null,
  competitorScopeSummary = 'their listed areas',
  anchorScopeSummary = 'the areas in our written scope',
  senderName = DEFAULT_SENDER_NAME,
} = {}) {
  const greeting = buildGreeting(firstName);
  const body = [
    greeting,
    '',
    `The quote you sent covers ${competitorScopeSummary}. Ours includes ${anchorScopeSummary}, so they are prices for different work.`,
    '',
    'Want me to separate the quote so you can compare the same areas side by side?',
    '',
    buildSignature(senderName),
  ].join('\n');
  return {
    subject: 'Re: cleaning quote comparison',
    body,
    cta: 'Want me to separate the quote so you can compare the same areas side by side?',
  };
}

function buildForwardableBlurb({ serviceArea = DEFAULT_SERVICE_AREA } = {}) {
  const areaLabel = /,\s*NH$/i.test(serviceArea) ? serviceArea : `${serviceArea}, NH`;
  return {
    subject: null,
    body: [
      'Here\'s the short version you can forward:',
      '',
      `Anchor Cleaning handles recurring commercial cleaning around ${areaLabel}. We walk the space, agree on what needs to be cleaned and how often, then put the quote in writing so the person approving it can see what the price actually covers.`,
    ].join('\n'),
    cta: null,
  };
}

function buildPropertyManagementColdEmail({
  firstName = null,
  companyName = 'your team',
  senderName = DEFAULT_SENDER_NAME,
  serviceArea = DEFAULT_SERVICE_AREA,
} = {}) {
  const greeting = buildGreeting(firstName);
  const subject = `Cleaning for ${companyName}`;
  const body = [
    greeting,
    '',
    `I'm ${senderName} with Anchor Cleaning. We handle recurring commercial cleaning around ${serviceArea}.`,
    '',
    'Property managers often need recurring cleaning, backup coverage, or clearly separated responsibilities across properties or common areas. We walk the space, agree on what gets cleaned and how often, then put the quote in writing.',
    '',
    'Want me to send over what we\'d need for a quote on one property first?',
    '',
    buildSignature(senderName),
  ].join('\n');
  return {
    subject,
    body,
    cta: 'Want me to send over what we\'d need for a quote on one property first?',
  };
}

const EVIDENCE_PROBLEM_BY_FACT_TYPE = Object.freeze({
  [FACT_TYPES.NEW_LOCATION]: 'New or recently opened space often means cleaning scope and frequency still need to be settled before things get busy.',
  [FACT_TYPES.MULTI_LOCATION]: 'When a firm runs multiple locations, it can be hard to tell whether cleaning is handled centrally or site by site.',
  [FACT_TYPES.PROPERTY_MANAGEMENT]: 'Managed properties often need recurring cleaning, backup coverage, or clearly separated responsibilities across buildings.',
  [FACT_TYPES.FACILITY_EXPANSION]: 'Expanded or renovated space can change what needs to be cleaned and how often it has to hold up.',
  [FACT_TYPES.VENDOR_PROCESS]: 'When vendor selection includes cleaning, a written scope makes it easier to compare what each quote actually covers.',
  [FACT_TYPES.COMMERCIAL_SPACE_SIGNAL]: 'Client-facing areas need to stay presentable without disrupting the workday, and that usually comes down to scope and frequency.',
  [FACT_TYPES.OTHER]: 'That kind of setup can make it harder to know exactly what a cleaning quote should cover.',
});

function buildEvidenceProblemBridge(evidence = {}) {
  if (evidence.personalization_status !== PERSONALIZATION_STATUS.SUPPORTED) return null;
  const factType = evidence.fact_type || FACT_TYPES.OTHER;
  return EVIDENCE_PROBLEM_BY_FACT_TYPE[factType] || EVIDENCE_PROBLEM_BY_FACT_TYPE[FACT_TYPES.OTHER];
}

function shouldUseEvidenceLane(evidence = {}) {
  return evidence?.personalization_status === PERSONALIZATION_STATUS.SUPPORTED
    && Boolean(buildEvidenceProblemBridge(evidence));
}

function resolveEvidenceSubject(evidence = {}, companyName = 'your team', company = {}) {
  const fact = String(evidence.observed_fact || '').toLowerCase();
  if (evidence.fact_type === FACT_TYPES.NEW_LOCATION && evidence.event_date) {
    return `Cleaning for the new ${evidence.event_date} location`;
  }
  if (/\boffice park\b/.test(fact)) return 'Cleaning for the office park';
  if (/\bbusiness center\b/.test(fact)) return 'Cleaning for the business center';
  if (/\bproperty management\b/.test(fact) || evidence.fact_type === FACT_TYPES.PROPERTY_MANAGEMENT) {
    return `Cleaning for ${companyName}`;
  }
  if (/\blocations?\b/.test(fact) || evidence.fact_type === FACT_TYPES.MULTI_LOCATION) {
    return `Cleaning for ${companyName}`;
  }
  const locality = asText(company?.places_locality || company?.location);
  if (locality) return `Cleaning for ${companyName}`;
  return `Cleaning for ${companyName}`;
}

function buildEvidenceEnhancedEmail({
  firstName = null,
  companyName = 'your team',
  senderName = DEFAULT_SENDER_NAME,
  serviceArea = DEFAULT_SERVICE_AREA,
  segment = SEGMENTS.OFFICE,
  evidence = {},
  company = {},
} = {}) {
  const bridge = buildEvidenceProblemBridge(evidence);
  if (!bridge) {
    return buildDefaultColdEmail({
      firstName,
      companyName,
      senderName,
      serviceArea,
      segment,
    });
  }

  const greeting = buildGreeting(firstName);
  const subject = resolveEvidenceSubject(evidence, companyName, company);
  const close = isPropertyManagementSegment(segment)
    ? 'Want me to send over what we\'d need for a quote on one property first?'
    : 'Want me to send over what we\'d need to price the office properly?';

  const body = [
    greeting,
    '',
    bridge,
    '',
    `I'm ${senderName} with Anchor Cleaning. We handle recurring commercial cleaning around ${serviceArea}.`,
    '',
    'We walk the space, agree on the areas and frequency, then put the quote in writing so you know what is covered before deciding.',
    '',
    close,
    '',
    buildSignature(senderName),
  ].join('\n');

  return {
    subject,
    body,
    cta: close,
    usedPersonalization: true,
    evidence,
  };
}

function buildAnchorCopy({
  lifecycleStage = LIFECYCLE_STAGES.COLD,
  firstName = null,
  companyName = 'your team',
  senderName = DEFAULT_SENDER_NAME,
  serviceArea = DEFAULT_SERVICE_AREA,
  segment = SEGMENTS.OFFICE,
  evidence = null,
  company = {},
  serviceType = null,
  competitorScopeSummary = null,
  anchorScopeSummary = null,
} = {}) {
  const normalizedEvidence = evidence || {
    personalization_status: PERSONALIZATION_STATUS.NO_USEFUL_FACT,
  };

  switch (lifecycleStage) {
    case LIFECYCLE_STAGES.FIRST_FOLLOW_UP:
      return buildFirstFollowUpEmail({ firstName, companyName, senderName, segment });
    case LIFECYCLE_STAGES.PROPOSAL_FOLLOW_UP:
      return buildProposalFollowUpEmail({ firstName, senderName });
    case LIFECYCLE_STAGES.PRICE_CONCERN:
      return buildPriceConcernReply({ firstName, senderName });
    case LIFECYCLE_STAGES.COMPARISON_QUOTE:
      return buildComparisonQuoteReply({
        firstName,
        competitorScopeSummary: competitorScopeSummary || 'their listed areas',
        anchorScopeSummary: anchorScopeSummary || 'the areas in our written scope',
        senderName,
      });
    case LIFECYCLE_STAGES.FORWARDABLE_BLURB:
      return buildForwardableBlurb({ serviceArea });
    case LIFECYCLE_STAGES.COLD:
    default:
      if (isPropertyManagementSegment(segment) && !shouldUseEvidenceLane(normalizedEvidence)) {
        return buildPropertyManagementColdEmail({
          firstName,
          companyName,
          senderName,
          serviceArea,
        });
      }
      if (shouldUseEvidenceLane(normalizedEvidence)) {
        return buildEvidenceEnhancedEmail({
          firstName,
          companyName,
          senderName,
          serviceArea,
          segment,
          evidence: normalizedEvidence,
          company,
        });
      }
      return buildDefaultColdEmail({
        firstName,
        companyName,
        senderName,
        serviceArea,
        segment,
        serviceType,
      });
  }
}

function findPatternViolations(text, patterns) {
  const hay = asText(text);
  if (!hay) return [];
  return patterns
    .filter(({ re }) => re.test(hay))
    .map(({ id, re }) => ({ patternId: id, match: hay.match(re)?.[0] || null }));
}

function validateAnchorCopyDoctrine({ subject = '', body = '', cta = '' } = {}) {
  const combined = [subject, body, cta].filter(Boolean).join('\n');
  const violations = [
    ...findPatternViolations(combined, AI_TELLS),
    ...findPatternViolations(combined, GENERIC_CLOSERS),
    ...findPatternViolations(combined, DISALLOWED_PHRASES),
  ];

  if (hasEmDash(combined)) {
    violations.push({ patternId: 'em_dash', match: '—' });
  }

  return {
    ok: violations.length === 0,
    blocker: violations.length ? DOCTRINE_BLOCKER : null,
    violations,
  };
}

const ANCHOR_SOCIAL_RULES = Object.freeze([
  { id: 'walkthrough', re: /\bwalk[- ]?throughs?\b/i },
  { id: 'aphorism_closer_thats_how', re: /that's how i think about/i },
  { id: 'aphorism_closer_loop', re: /that loop is becoming more interesting/i },
  { id: 'engagement_bait_question', re: /what do you think\?/i },
]);

function validateAnchorSocialCopy(body = '') {
  const doctrine = validateAnchorCopyDoctrine({ body });
  const doctrineViolations = doctrine.violations.map((violation) => ({
    source: 'anchor_copy_doctrine',
    patternId: violation.patternId,
    match: violation.match,
  }));
  const socialViolations = findPatternViolations(body, ANCHOR_SOCIAL_RULES).map((violation) => ({
    source: 'anchor_social_rule',
    patternId: violation.patternId,
    match: violation.match,
  }));
  const violations = [...doctrineViolations, ...socialViolations];

  return {
    ok: violations.length === 0,
    blocker: violations.length ? DOCTRINE_BLOCKER : null,
    violations,
  };
}

function buildAnchorCopyDoctrineViolationError(violations = []) {
  const err = new Error(DOCTRINE_BLOCKER);
  err.code = DOCTRINE_BLOCKER;
  err.violations = violations;
  return err;
}

module.exports = {
  ANCHOR_CLIENT_ID,
  ANCHOR_COPY_OWNER,
  DEFAULT_SENDER_NAME,
  DEFAULT_SERVICE_AREA,
  LIFECYCLE_STAGES,
  SEGMENTS,
  AI_TELLS,
  GENERIC_CLOSERS,
  DISALLOWED_PHRASES,
  DOCTRINE_BLOCKER,
  EVIDENCE_PROBLEM_BY_FACT_TYPE,
  asText,
  hasEmDash,
  buildGreeting,
  buildSignature,
  resolveServiceAreaLabel,
  resolveSegment,
  isPropertyManagementSegment,
  buildDefaultColdEmail,
  buildFirstFollowUpEmail,
  buildProposalFollowUpEmail,
  buildPriceConcernReply,
  buildComparisonQuoteReply,
  buildForwardableBlurb,
  buildPropertyManagementColdEmail,
  buildEvidenceProblemBridge,
  shouldUseEvidenceLane,
  buildEvidenceEnhancedEmail,
  buildAnchorCopy,
  validateAnchorCopyDoctrine,
  ANCHOR_SOCIAL_RULES,
  validateAnchorSocialCopy,
  buildAnchorCopyDoctrineViolationError,
};
