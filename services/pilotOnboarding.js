'use strict';

/**
 * SPEC-115 — Pilot 0 onboarding gates, AIM status, Max unlock, and failure copy.
 *
 * Developer intervention is a product bug. Missing steps fail closed with
 * explicit product language — never silent, never borrowed intelligence.
 */

const AIM_STATUS = Object.freeze({
  NO_DOCUMENTS: 'no_documents',
  READY_TO_COMPILE: 'ready_to_compile',
  DRAFT: 'draft',
  PUBLISHED: 'published',
});

const AIM_STATUS_LABELS = Object.freeze({
  [AIM_STATUS.NO_DOCUMENTS]: 'No Documents',
  [AIM_STATUS.READY_TO_COMPILE]: 'Ready To Compile',
  [AIM_STATUS.DRAFT]: 'Draft',
  [AIM_STATUS.PUBLISHED]: 'Published',
});

const FAILURE = Object.freeze({
  NO_TENANT: {
    code: 'no_tenant',
    message: 'No active workspace.\n\nSelect or activate\na tenant.',
  },
  NO_BLUEPRINT: {
    code: 'no_blueprint',
    message: 'Client Intelligence\nhas not been completed.',
  },
  NO_AIM: {
    code: 'no_aim',
    message: 'Your Acquisition Intelligence\nModel has not been published.',
  },
  PASSWORD_CHANGE_REQUIRED: {
    code: 'password_change_required',
    message: 'Password must be updated\nbefore continuing.',
  },
  MAX_LOCKED: {
    code: 'max_locked',
    message:
      "I don't know enough yet.\n\nComplete Client Intelligence\nand publish your Acquisition\nIntelligence Model first.",
  },
});

const ONBOARDING_GREETING_BODY = [
  "Let's begin by understanding your business.",
  'The first step is completing Client Intelligence.',
  'Everything I learn from you becomes the foundation for prospecting, reasoning, and recommendations.',
];

const BEGIN_CLIENT_INTELLIGENCE = 'Begin Client Intelligence';

function firstName(name, fallback = 'there') {
  const token = String(name || '')
    .trim()
    .split(/\s+/)
    .find(Boolean);
  return token || fallback;
}

function buildOnboardingGreeting(name) {
  const greeting = `Welcome, ${firstName(name)}.`;
  return {
    greeting,
    body: ONBOARDING_GREETING_BODY.slice(),
    prompt: BEGIN_CLIENT_INTELLIGENCE,
    cta: BEGIN_CLIENT_INTELLIGENCE,
    fullText: [greeting, '', ...ONBOARDING_GREETING_BODY].join('\n\n'),
  };
}

function deriveAimStatus(input = {}) {
  if (input.published === true || input.status === 'published' || input.status === 'Published' || input.status === 'Published AIM') {
    return AIM_STATUS.PUBLISHED;
  }
  const documents = Number(input.documentCount ?? input.documents ?? 0);
  const compiled = Boolean(
    input.compiled ||
      input.inProgress ||
      input.draft ||
      ['draft', 'in_review', 'approved', 'Draft', 'In Progress'].includes(input.status)
  );
  if (compiled) return AIM_STATUS.DRAFT;
  if (documents > 0) return AIM_STATUS.READY_TO_COMPILE;
  return AIM_STATUS.NO_DOCUMENTS;
}

function aimStatusPublic(status) {
  const key = AIM_STATUS_LABELS[status] ? status : deriveAimStatus({ status });
  return {
    key,
    label: AIM_STATUS_LABELS[key] || AIM_STATUS_LABELS[AIM_STATUS.NO_DOCUMENTS],
  };
}

function blueprintApproved(input = {}) {
  const cie = input.clientIntelligence || input.blueprint || input;
  return Boolean(
    cie.approved === true ||
      cie.blueprintApproved === true ||
      String(cie.status || '').toLowerCase() === 'approved'
  );
}

function aimPublished(input = {}) {
  const aim = input.aim || input.publishedAim || input;
  if (!aim || typeof aim !== 'object') return false;
  if (aim.published === true) return true;
  if (aim.published === false) return false;
  return deriveAimStatus(aim) === AIM_STATUS.PUBLISHED;
}

function maxUnlocked(input = {}) {
  return blueprintApproved(input) && aimPublished(input);
}

function scoutUnlocked(input = {}) {
  return aimPublished(input);
}

function outreachUnlock(input = {}) {
  const checks = {
    aimPublished: aimPublished(input),
    prospectApproved: Boolean(input.prospectApproved),
    domainHealthy: Boolean(input.domainHealthy),
    sendingCapacityAvailable: Boolean(input.sendingCapacityAvailable),
    campaignApproved: Boolean(input.campaignApproved),
  };
  const missing = Object.entries(checks)
    .filter(([, ok]) => !ok)
    .map(([key]) => key);
  return {
    unlocked: missing.length === 0,
    checks,
    missing,
    message: missing.length
      ? outreachLockMessage(missing)
      : 'Outreach is unlocked.',
  };
}

function outreachLockMessage(missing = []) {
  const labels = {
    aimPublished: 'AIM is not published.',
    prospectApproved: 'No prospect has been approved.',
    domainHealthy: 'Domain is not healthy.',
    sendingCapacityAvailable: 'Sending capacity is not available.',
    campaignApproved: 'Campaign is not approved.',
  };
  return missing.map((key) => labels[key] || key).join(' ');
}

function resolveFailure(input = {}) {
  if (input.passwordChangeRequired) return FAILURE.PASSWORD_CHANGE_REQUIRED;
  if (input.tenantId == null && input.hasTenant !== true) return FAILURE.NO_TENANT;
  if (!blueprintApproved(input)) return FAILURE.NO_BLUEPRINT;
  if (!aimPublished(input)) return FAILURE.NO_AIM;
  return null;
}

function maxAcquisitionReply(input = {}) {
  if (input.passwordChangeRequired) return FAILURE.PASSWORD_CHANGE_REQUIRED;
  if (input.tenantId == null && input.hasTenant !== true) return FAILURE.NO_TENANT;
  if (!maxUnlocked(input)) return FAILURE.MAX_LOCKED;
  return null;
}

function publicOnboardingState(input = {}) {
  const aimKey = deriveAimStatus(input.aim || {});
  const failure = resolveFailure(input);
  const maxLock = maxAcquisitionReply(input);
  const outreach = outreachUnlock(input);
  return {
    passwordChangeRequired: Boolean(input.passwordChangeRequired),
    blueprintApproved: blueprintApproved(input),
    aim: aimStatusPublic(aimKey),
    maxUnlocked: maxUnlocked(input),
    scoutUnlocked: scoutUnlocked(input),
    outreach,
    failure: failure
      ? { code: failure.code, message: failure.message }
      : null,
    max: maxLock
      ? { unlocked: false, code: maxLock.code, message: maxLock.message }
      : { unlocked: true, code: null, message: null },
    cta: blueprintApproved(input)
      ? aimKey === AIM_STATUS.PUBLISHED
        ? { label: 'Ask Max', href: '/command-deck' }
        : { label: 'Open Acquisition Intelligence', href: '/aim' }
      : { label: BEGIN_CLIENT_INTELLIGENCE, href: '/client-intel' },
  };
}

module.exports = {
  AIM_STATUS,
  AIM_STATUS_LABELS,
  FAILURE,
  BEGIN_CLIENT_INTELLIGENCE,
  ONBOARDING_GREETING_BODY,
  firstName,
  buildOnboardingGreeting,
  deriveAimStatus,
  aimStatusPublic,
  blueprintApproved,
  aimPublished,
  maxUnlocked,
  scoutUnlocked,
  outreachUnlock,
  resolveFailure,
  maxAcquisitionReply,
  publicOnboardingState,
};
