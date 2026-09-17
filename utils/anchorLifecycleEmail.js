'use strict';

const { deriveBusinessNameShort } = require('./businessNameShort');
const {
  PERSONALIZATION_STATUS,
  FACT_TYPES,
  readScoutPersonalizationFromMetadata,
} = require('./scoutPersonalizationEvidence');

const ANCHOR_CLIENT_ID = 10;
const DEFAULT_SENDER_NAME = 'Jacob Maynard';
const SERVICE_AREA_LABEL = 'Manchester, NH';

const ANCHOR_DIFFERENTIATOR = [
  "If you're comparing cleaners, we try to make the decision easier: after seeing the space, we put the work, frequency and price in writing, and you have a clear contact if something is missed.",
  "That way you know exactly what's supposed to be done before you decide — and who is accountable after the work starts.",
].join('\n\n');

const DISALLOWED_PHRASES = [
  /\bi know you need a cleaner\b/i,
  /\bsince you(?:'|’)re expanding, you probably need\b/i,
  /\bi noticed your current cleaner may not\b/i,
  /\byou must be looking for\b/i,
  /\bi wanted to follow up on your cleaning needs\b/i,
  /\bneed(?:s|ed)? cleaning (?:now|urgently|immediately)\b/i,
  /\bunhappy with (?:your|the) (?:current )?cleaner\b/i,
];

const BRIDGE_BY_FACT_TYPE = Object.freeze({
  [FACT_TYPES.NEW_LOCATION]: 'If cleaning for that space is still being arranged, it may be worth lining up a written quote before things get busy.',
  [FACT_TYPES.MULTI_LOCATION]: "I wasn't sure whether cleaning is handled centrally or by each location, but either setup can benefit from a written scope and one accountable contact.",
  [FACT_TYPES.PROPERTY_MANAGEMENT]: 'If you ever need backup cleaning support across managed properties, a written scope and named contact can make vendor coordination simpler.',
  [FACT_TYPES.FACILITY_EXPANSION]: 'If the expanded space changes what needs to be cleaned, a walkthrough can help put scope, frequency, and price in writing.',
  [FACT_TYPES.VENDOR_PROCESS]: 'If vendor selection includes cleaning services, a written quote with clear scope can make comparison easier.',
  [FACT_TYPES.COMMERCIAL_SPACE_SIGNAL]: 'If you ever need backup cleaning support for the space, a written scope and named contact can make comparison easier.',
  [FACT_TYPES.OTHER]: 'If you ever need backup cleaning support, a written scope and named contact can make comparison easier.',
});

function resolveCompanyName(prospect = {}, company = {}) {
  const companyName = company?.name || prospect?.company || prospect?.company_name;
  if (companyName && String(companyName).trim()) return String(companyName).trim();
  return 'your business';
}

function resolveBusinessNameShort(prospect = {}, company = {}) {
  if (company?.business_name_short) return company.business_name_short;
  if (prospect?.business_name_short) return prospect.business_name_short;
  const derived = deriveBusinessNameShort(resolveCompanyName(prospect, company));
  return derived.business_name_short || resolveCompanyName(prospect, company);
}

function resolveFirstName(prospect = {}) {
  const firstName = String(prospect?.first_name || '').trim();
  return firstName || null;
}

function buildGreeting(prospect = {}) {
  const firstName = resolveFirstName(prospect);
  return firstName ? `Hi ${firstName},` : 'Hi,';
}

function buildContextualBridge(evidence = {}) {
  if (evidence.personalization_status !== PERSONALIZATION_STATUS.SUPPORTED) return null;
  return BRIDGE_BY_FACT_TYPE[evidence.fact_type] || BRIDGE_BY_FACT_TYPE[FACT_TYPES.OTHER];
}

function resolveLocationOrPropertyContext(evidence = {}, company = {}) {
  if (evidence.personalization_status !== PERSONALIZATION_STATUS.SUPPORTED) return null;

  if (evidence.fact_type === FACT_TYPES.NEW_LOCATION && evidence.event_date) {
    return `new ${evidence.event_date} location`;
  }

  const fact = String(evidence.observed_fact || '').toLowerCase();
  if (/\boffice park\b/.test(fact)) return 'office park';
  if (/\bbusiness center\b/.test(fact)) return 'business center';
  if (/\bproperty management\b/.test(fact)) return 'managed properties';
  if (/\blocations?\b/.test(fact)) return 'locations';

  const locality = company?.places_locality || company?.location;
  if (locality && String(locality).trim()) {
    return String(locality).trim();
  }
  return null;
}

function shouldUsePersonalization(evidence = {}) {
  return evidence?.personalization_status === PERSONALIZATION_STATUS.SUPPORTED
    && Boolean(evidence?.observed_fact)
    && Boolean(buildContextualBridge(evidence));
}

function validateAnchorLifecycleCopy({ subject = '', body = '' }) {
  const combined = `${subject}\n${body}`;
  const violations = DISALLOWED_PHRASES
    .filter((pattern) => pattern.test(combined))
    .map((pattern) => pattern.source);
  return {
    ok: violations.length === 0,
    violations,
  };
}

function buildAnchorLifecycleEmail({
  prospect = {},
  company = {},
  evidence = null,
  senderName = DEFAULT_SENDER_NAME,
} = {}) {
  const normalizedEvidence = evidence || readScoutPersonalizationFromMetadata(prospect?.acquisition_metadata) || {
    personalization_status: PERSONALIZATION_STATUS.NO_USEFUL_FACT,
  };
  const companyName = resolveCompanyName(prospect, company);
  const greeting = buildGreeting(prospect);
  const signature = [
    senderName,
    'Anchor Cleaning',
    'jacob@goanchorcleaning.com',
    '(603) 420-2430',
  ].join('\n');

  if (shouldUsePersonalization(normalizedEvidence)) {
    const bridge = buildContextualBridge(normalizedEvidence);
    const locationContext = resolveLocationOrPropertyContext(normalizedEvidence, company);
    const subject = locationContext
      ? `Cleaning for the ${locationContext}`
      : `Cleaning for ${companyName}`;
    const body = [
      greeting,
      '',
      `I saw ${normalizedEvidence.observed_fact}. ${bridge}`,
      '',
      `I'm ${senderName} with Anchor Cleaning. We clean commercial properties around ${SERVICE_AREA_LABEL}.`,
      '',
      ANCHOR_DIFFERENTIATOR,
      '',
      `Would it be useful for us to put a quote together for ${companyName}?`,
      '',
      signature,
    ].join('\n');

    const validation = validateAnchorLifecycleCopy({ subject, body });
    if (!validation.ok) {
      return buildAnchorLifecycleEmail({
        prospect,
        company,
        evidence: { personalization_status: PERSONALIZATION_STATUS.NO_USEFUL_FACT },
        senderName,
      });
    }

    return {
      subject,
      body,
      usedPersonalization: true,
      evidence: normalizedEvidence,
    };
  }

  const subject = `Cleaning for ${companyName}`;
  const body = [
    greeting,
    '',
    `I'm ${senderName} with Anchor Cleaning. We clean commercial properties around ${SERVICE_AREA_LABEL}.`,
    '',
    ANCHOR_DIFFERENTIATOR,
    '',
    `Would it be useful for us to put a quote together for ${companyName}?`,
    '',
    signature,
  ].join('\n');

  return {
    subject,
    body,
    usedPersonalization: false,
    evidence: normalizedEvidence,
  };
}

function buildAnchorLifecycleVariant({
  candidate = {},
  crmRecord = null,
  senderName = DEFAULT_SENDER_NAME,
} = {}) {
  const prospect = crmRecord || {
    first_name: candidate.firstName || candidate.first_name || null,
    company_name: candidate.name || candidate.companyName || null,
    acquisition_metadata: candidate.acquisition_metadata || null,
  };
  const company = crmRecord?.company_fields || {
    name: candidate.name || candidate.companyName || null,
    business_name_short: candidate.businessNameShort || null,
    places_locality: candidate.placesLocality || candidate.places_locality || null,
    location: candidate.location || null,
  };
  const evidence = readScoutPersonalizationFromMetadata(prospect.acquisition_metadata)
    || candidate.scoutPersonalization
    || candidate.scout_personalization
    || null;

  return buildAnchorLifecycleEmail({
    prospect,
    company,
    evidence,
    senderName,
  });
}

module.exports = {
  ANCHOR_CLIENT_ID,
  ANCHOR_DIFFERENTIATOR,
  buildAnchorLifecycleEmail,
  buildAnchorLifecycleVariant,
  buildContextualBridge,
  readScoutPersonalizationFromMetadata,
  resolveBusinessNameShort,
  shouldUsePersonalization,
  validateAnchorLifecycleCopy,
};
