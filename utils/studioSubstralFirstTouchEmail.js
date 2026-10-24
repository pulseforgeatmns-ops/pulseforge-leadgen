'use strict';

/**
 * Paige first-touch email assembly for Studio Substral (SPEC-PAIGE-SUBSTRAL-001).
 */

const {
  buildStudioSubstralFirstTouchEmail,
  validateStudioSubstralFirstTouchDoctrine,
  DEFAULT_SENDER_NAME,
} = require('./paigeStudioSubstralOutboundDoctrine');

function parseStudioScoutIntel(source = {}) {
  if (!source || typeof source !== 'object') return {};
  if (source.studio_scout_intelligence && typeof source.studio_scout_intelligence === 'object') {
    return source.studio_scout_intelligence;
  }
  try {
    if (typeof source.studio_scout_intelligence === 'string') {
      return JSON.parse(source.studio_scout_intelligence);
    }
  } catch {
    /* ignore */
  }
  return source.studio_scout_intelligence || source.scout_intelligence || source;
}

function buildStudioSubstralFirstTouchVariant({
  candidate = {},
  crmRecord = null,
  plan = {},
  mission = {},
} = {}) {
  const prospect = crmRecord || {};
  const intel = parseStudioScoutIntel(prospect) || parseStudioScoutIntel(candidate);
  const companyName = candidate.name
    || candidate.companyName
    || intel.company_name
    || prospect.company_name
    || 'your team';
  const senderName = plan.senderName || mission.senderName || DEFAULT_SENDER_NAME;
  const segment = intel.studio_category
    || candidate.studio_category
    || plan.market?.segment
    || candidate.vertical
    || '';

  const draft = buildStudioSubstralFirstTouchEmail({
    firstName: prospect.first_name || candidate.firstName || candidate.first_name,
    decisionMakerName: intel.decision_maker_name || prospect.decision_maker_name,
    companyName,
    segment,
    intel,
    row: prospect,
    candidate,
    senderName,
    assessmentPayload: candidate.assessmentPayload || candidate.assessment_payload || null,
    specificIssue: candidate.specificWebsiteIssue || candidate.specific_website_issue,
  });

  if (draft.held) {
    return {
      held: true,
      holdReason: draft.holdReason,
      guardId: draft.guardId || null,
      detail: draft.detail,
      subject: null,
      body: null,
      cta: null,
      usedPersonalization: false,
      evidence: {
        hold_reason: draft.holdReason,
        specific_issue: draft.specificIssue || null,
      },
    };
  }

  const validation = validateStudioSubstralFirstTouchDoctrine({
    subject: draft.subject,
    body: draft.body,
    cta: draft.cta,
  });

  return {
    held: false,
    subject: draft.subject,
    body: draft.body,
    cta: draft.cta,
    usedPersonalization: Boolean(draft.observation),
    evidence: {
      specific_issue: draft.specificIssue,
      observation: draft.observation,
      doctrine_valid: validation.ok,
      violations: validation.ok ? [] : validation.violations,
    },
  };
}

module.exports = {
  buildStudioSubstralFirstTouchVariant,
  parseStudioScoutIntel,
  validateStudioSubstralFirstTouchDoctrine,
};
