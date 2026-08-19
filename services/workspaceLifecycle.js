'use strict';

/**
 * SPEC-115 — explicit workspace lifecycle.
 *
 * Registered → Provisioned → Client Intelligence In Progress → Blueprint Approved
 *   → AIM In Progress → AIM Published → Prospecting Active → Campaign Active → Learning
 *
 * Stages advance only when earned artifacts exist. Nothing is invented.
 */

const LIFECYCLE = Object.freeze({
  REGISTERED: 'registered',
  PROVISIONED: 'provisioned',
  CLIENT_INTELLIGENCE_IN_PROGRESS: 'client_intelligence_in_progress',
  BLUEPRINT_APPROVED: 'blueprint_approved',
  AIM_IN_PROGRESS: 'aim_in_progress',
  AIM_PUBLISHED: 'aim_published',
  PROSPECTING_ACTIVE: 'prospecting_active',
  CAMPAIGN_ACTIVE: 'campaign_active',
  LEARNING: 'learning',
});

const LIFECYCLE_ORDER = Object.freeze([
  LIFECYCLE.REGISTERED,
  LIFECYCLE.PROVISIONED,
  LIFECYCLE.CLIENT_INTELLIGENCE_IN_PROGRESS,
  LIFECYCLE.BLUEPRINT_APPROVED,
  LIFECYCLE.AIM_IN_PROGRESS,
  LIFECYCLE.AIM_PUBLISHED,
  LIFECYCLE.PROSPECTING_ACTIVE,
  LIFECYCLE.CAMPAIGN_ACTIVE,
  LIFECYCLE.LEARNING,
]);

const LIFECYCLE_LABELS = Object.freeze({
  [LIFECYCLE.REGISTERED]: 'Registered',
  [LIFECYCLE.PROVISIONED]: 'Provisioned',
  [LIFECYCLE.CLIENT_INTELLIGENCE_IN_PROGRESS]: 'Client Intelligence In Progress',
  [LIFECYCLE.BLUEPRINT_APPROVED]: 'Blueprint Approved',
  [LIFECYCLE.AIM_IN_PROGRESS]: 'AIM In Progress',
  [LIFECYCLE.AIM_PUBLISHED]: 'AIM Published',
  [LIFECYCLE.PROSPECTING_ACTIVE]: 'Prospecting Active',
  [LIFECYCLE.CAMPAIGN_ACTIVE]: 'Campaign Active',
  [LIFECYCLE.LEARNING]: 'Learning',
});

function rank(stage) {
  const idx = LIFECYCLE_ORDER.indexOf(stage);
  return idx < 0 ? 0 : idx;
}

function laterStage(a, b) {
  return rank(a) >= rank(b) ? a : b;
}

/**
 * Derive lifecycle from earned workspace artifacts.
 * `stored` is the last persisted stage (floor — never regress).
 */
function deriveWorkspaceLifecycle(status = {}, stored = LIFECYCLE.PROVISIONED) {
  let stage = stored && LIFECYCLE_ORDER.includes(stored) ? stored : LIFECYCLE.PROVISIONED;
  if (stage === LIFECYCLE.REGISTERED) stage = LIFECYCLE.PROVISIONED;

  const cie = status.clientIntelligence || {};
  const aim = status.aim || {};
  const prospects = Number(status.prospects?.count || 0);
  const campaigns = Number(status.campaigns?.count || 0);
  const outcomes = Number(status.outcomes?.count || 0);

  if (cie.present || cie.status === 'In Progress' || cie.status === 'Approved') {
    stage = laterStage(stage, LIFECYCLE.CLIENT_INTELLIGENCE_IN_PROGRESS);
  }
  if (cie.approved || cie.status === 'Approved') {
    stage = laterStage(stage, LIFECYCLE.BLUEPRINT_APPROVED);
  }
  if (aim.present || aim.inProgress || aim.status === 'In Progress') {
    stage = laterStage(stage, LIFECYCLE.AIM_IN_PROGRESS);
  }
  if (aim.published || aim.status === 'Published AIM') {
    stage = laterStage(stage, LIFECYCLE.AIM_PUBLISHED);
  }
  if (prospects > 0) {
    stage = laterStage(stage, LIFECYCLE.PROSPECTING_ACTIVE);
  }
  if (campaigns > 0) {
    stage = laterStage(stage, LIFECYCLE.CAMPAIGN_ACTIVE);
  }
  if (campaigns > 0 && outcomes > 0) {
    stage = laterStage(stage, LIFECYCLE.LEARNING);
  }

  return stage;
}

function lifecycleLabel(stage) {
  return LIFECYCLE_LABELS[stage] || LIFECYCLE_LABELS[LIFECYCLE.PROVISIONED];
}

function publicLifecycle(stage) {
  return {
    stage,
    label: lifecycleLabel(stage),
  };
}

module.exports = {
  LIFECYCLE,
  LIFECYCLE_ORDER,
  LIFECYCLE_LABELS,
  deriveWorkspaceLifecycle,
  lifecycleLabel,
  publicLifecycle,
  laterStage,
  rank,
};
