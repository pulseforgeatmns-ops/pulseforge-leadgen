'use strict';

const { deriveBusinessNameShort } = require('./businessNameShort');
const {
  PERSONALIZATION_STATUS,
  readScoutPersonalizationFromMetadata,
} = require('./scoutPersonalizationEvidence');
const {
  MANAGED_FACILITIES_COHORT,
  PORTAL_ARM,
  appendPortalDifferentiator,
  managedFacilitiesExperiment,
} = require('./anchorManagedFacilities');
const {
  ANCHOR_CLIENT_ID,
  DEFAULT_SENDER_NAME,
  LIFECYCLE_STAGES,
  buildAnchorCopy,
  resolveSegment,
  resolveServiceAreaLabel,
  validateAnchorCopyDoctrine,
} = require('./anchorCopyDoctrine');

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

function buildAnchorLifecycleEmail({
  prospect = {},
  company = {},
  evidence = null,
  senderName = DEFAULT_SENDER_NAME,
  plan = {},
  mission = {},
  lifecycleStage = LIFECYCLE_STAGES.COLD,
  candidateId = null,
} = {}) {
  const normalizedEvidence = evidence || readScoutPersonalizationFromMetadata(prospect?.acquisition_metadata) || {
    personalization_status: PERSONALIZATION_STATUS.NO_USEFUL_FACT,
  };
  const companyName = resolveCompanyName(prospect, company);
  const firstName = resolveFirstName(prospect);
  const serviceArea = resolveServiceAreaLabel(plan, company);
  const segment = resolveSegment(plan, mission, normalizedEvidence);

  let draft = buildAnchorCopy({
    lifecycleStage,
    firstName,
    companyName,
    senderName,
    serviceArea,
    segment,
    evidence: normalizedEvidence,
    company,
  });

  const experiment = lifecycleStage === LIFECYCLE_STAGES.COLD
    ? managedFacilitiesExperiment({ prospect, company, candidateId, mission })
    : { cohort: null, score: 0, arm: null };
  if (experiment.cohort === MANAGED_FACILITIES_COHORT && experiment.arm === PORTAL_ARM) {
    draft = { ...draft, body: appendPortalDifferentiator(draft.body, draft.cta) };
  }

  const validation = validateAnchorCopyDoctrine({
    subject: draft.subject,
    body: draft.body,
    cta: draft.cta,
  });

  if (!validation.ok && normalizedEvidence.personalization_status === PERSONALIZATION_STATUS.SUPPORTED) {
    return buildAnchorLifecycleEmail({
      prospect,
      company,
      evidence: { personalization_status: PERSONALIZATION_STATUS.NO_USEFUL_FACT },
      senderName,
      plan,
      mission,
      lifecycleStage,
      candidateId,
    });
  }

  return {
    subject: draft.subject,
    body: draft.body,
    cta: draft.cta,
    usedPersonalization: draft.usedPersonalization === true,
    evidence: normalizedEvidence,
    cohort: experiment.cohort,
    experimentArm: experiment.arm,
    managedFacilitiesScore: experiment.score,
  };
}

function buildAnchorLifecycleVariant({
  candidate = {},
  crmRecord = null,
  senderName = DEFAULT_SENDER_NAME,
  plan = {},
  mission = {},
  lifecycleStage = LIFECYCLE_STAGES.COLD,
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
    plan,
    mission,
    lifecycleStage,
    candidateId: candidate.candidateId || candidate.prospectId || candidate.id || crmRecord?.prospect_id || crmRecord?.id || null,
  });
}

module.exports = {
  ANCHOR_CLIENT_ID,
  buildAnchorLifecycleEmail,
  buildAnchorLifecycleVariant,
  readScoutPersonalizationFromMetadata,
  resolveBusinessNameShort,
  validateAnchorLifecycleCopy: validateAnchorCopyDoctrine,
};
