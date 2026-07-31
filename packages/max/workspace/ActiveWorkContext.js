'use strict';

/**
 * Session-level short-lived working memory for Max.
 * Preserves the current operator task across turns without long-term memory
 * or autonomous execution.
 */

const DEFAULT_CANARY_CONSTRAINTS = Object.freeze({
  preparationOnly: true,
  noMissionCreation: true,
  noLaunch: true,
  noExecution: true,
  noApproval: true,
  noPrint: true,
  noMail: true,
  noInventedEvidence: true,
});

const LAST_OUTPUT_TYPES = Object.freeze({
  CANARY_REVIEW_PACKAGE: 'canary_review_package',
  VERIFICATION_WORK_ORDER: 'verification_work_order',
  FILLABLE_TABLE: 'fillable_table',
  PROVISIONAL_DRAFTS: 'provisional_drafts',
});

/**
 * @param {object|null|undefined} session
 * @returns {object|null}
 */
function getActiveWorkContext(session) {
  if (!session) return null;
  if (session.activeWorkContext && typeof session.activeWorkContext === 'object') {
    return session.activeWorkContext;
  }
  if (
    session.context &&
    session.context.activeWorkContext &&
    typeof session.context.activeWorkContext === 'object'
  ) {
    return session.context.activeWorkContext;
  }
  return null;
}

/**
 * @param {object} session
 * @param {object|null} ctx
 */
function setActiveWorkContext(session, ctx) {
  if (!session) return null;
  const next = ctx && typeof ctx === 'object' ? cloneActiveWorkContext(ctx) : null;
  session.activeWorkContext = next;
  if (session.context && typeof session.context === 'object') {
    session.context.activeWorkContext = next;
  }
  session.updatedAt = new Date().toISOString();
  return next;
}

/**
 * @param {object} ctx
 */
function cloneActiveWorkContext(ctx) {
  return {
    workflow: ctx.workflow != null ? String(ctx.workflow) : null,
    target:
      ctx.target && typeof ctx.target === 'object'
        ? { ...ctx.target }
        : {},
    entities: Array.isArray(ctx.entities)
      ? ctx.entities.map((e) => ({ ...e }))
      : [],
    constraints:
      ctx.constraints && typeof ctx.constraints === 'object'
        ? { ...ctx.constraints }
        : {},
    lastOutputType:
      ctx.lastOutputType != null ? String(ctx.lastOutputType) : null,
    pendingFields: Array.isArray(ctx.pendingFields)
      ? ctx.pendingFields.map(String)
      : [],
    nextAction: ctx.nextAction != null ? String(ctx.nextAction) : null,
  };
}

/**
 * @param {object} input
 * @param {object[]} input.prospects
 * @param {string} [input.campaignId]
 * @param {string} [input.lastOutputType]
 * @param {string|null} [input.nextAction]
 * @param {object} [input.prior]
 */
function buildCanaryActiveWorkContext(input = {}) {
  const prospects = Array.isArray(input.prospects) ? input.prospects : [];
  const prior = input.prior && typeof input.prior === 'object' ? input.prior : null;
  const campaignId =
    input.campaignId ||
    (prior && prior.target && prior.target.campaignId) ||
    '001';

  return {
    workflow: 'campaign_canary',
    target: { campaignId: String(campaignId) },
    entities: prospects.map(prospectToEntity),
    constraints: {
      ...DEFAULT_CANARY_CONSTRAINTS,
      ...(prior && prior.constraints && typeof prior.constraints === 'object'
        ? prior.constraints
        : {}),
      ...DEFAULT_CANARY_CONSTRAINTS,
    },
    lastOutputType:
      input.lastOutputType ||
      (prior && prior.lastOutputType) ||
      LAST_OUTPUT_TYPES.CANARY_REVIEW_PACKAGE,
    pendingFields: derivePendingFields(prospects),
    nextAction:
      input.nextAction != null
        ? input.nextAction
        : prior && prior.nextAction
          ? prior.nextAction
          : 'await_operator_transform_or_verification',
  };
}

/**
 * @param {object} prospect
 */
function prospectToEntity(prospect = {}) {
  return {
    type: 'prospect',
    id: prospect.id != null ? String(prospect.id) : null,
    companyName: blankToNull(prospect.companyName),
    contactName: blankToNull(prospect.contactName),
    industry: blankToNull(prospect.industry || prospect.vertical),
    website: blankToNull(prospect.website),
    mailingAddress: blankToNull(
      prospect.mailingAddress || prospect.address
    ),
    phone: blankToNull(prospect.phone),
  };
}

/**
 * @param {object[]} entities
 */
function entitiesToProspects(entities) {
  return (Array.isArray(entities) ? entities : [])
    .filter((e) => e && (e.type === 'prospect' || e.type == null))
    .map((e) => ({
      id: e.id || null,
      companyName: e.companyName || null,
      contactName: e.contactName || null,
      industry: e.industry || null,
      website: e.website || null,
      mailingAddress: e.mailingAddress || null,
      address: e.mailingAddress || null,
      phone: e.phone || null,
    }));
}

/**
 * @param {object[]} prospects
 */
function derivePendingFields(prospects) {
  const pending = new Set();
  for (const p of prospects || []) {
    if (!String(p.website || '').trim()) pending.add('website');
    if (!String(p.mailingAddress || p.address || '').trim()) {
      pending.add('mailingAddress');
    }
    if (!String(p.phone || '').trim()) pending.add('phone');
  }
  return [...pending];
}

/**
 * Follow-up cues that should reuse activeWorkContext when no new paste is given.
 * @param {string} text
 */
function isActiveWorkFollowUpCue(text) {
  const lower = String(text || '').toLowerCase();
  if (!lower.trim()) return false;

  const cues = [
    /\bcontinue\b/,
    /\bconvert\s+this\b/,
    /\bconvert\s+the\s+(?:verification\s+)?work\s+order\b/,
    /\bmake\s+it\s+a\s+table\b/,
    /\bfillable\s+table\b/,
    /\buse\s+the\s+same\s+prospects\b/,
    /\bsame\s+prospects\b/,
    /\bkeep\s+the\s+same\s+(?:preparation[-\s]*only\s+)?constraints\b/,
    /\bkeep\s+the\s+same\s+constraints\b/,
    /\bturn\s+this\s+into\b/,
    /\bturn\s+it\s+into\b/,
    /\brevise\s+that\b/,
    /\bmake\s+it\s+more\s+concise\b/,
    /\bnext\s+step\b/,
    /\bwhat\s+should\s+i\s+do\s+first\b/,
    /\bconvert\b.+\b(?:into|to)\s+a\s+(?:fillable\s+)?table\b/,
  ];
  return cues.some((re) => re.test(lower));
}

/**
 * Strong transform cues that should clarify (ask for prospects) even when
 * desk context is missing — instead of falling through to General Conversation.
 * @param {string} text
 */
function isActiveWorkTransformCue(text) {
  const lower = String(text || '').toLowerCase();
  return (
    isFillableTableRequest(lower) ||
    /\bconvert\s+the\s+(?:verification\s+)?work\s+order\b/.test(lower) ||
    /\bverification\s+work\s+order\b/.test(lower)
  );
}

/**
 * Explicit new mission / campaign work — must not be intercepted by desk context.
 * @param {string} text
 */
function isExplicitNewMissionRequest(text) {
  const lower = String(text || '').toLowerCase();
  return (
    /\bbuild\s+campaign\b/.test(lower) ||
    /\bcreate\s+(?:a\s+)?(?:new\s+)?(?:campaign|mission)\b/.test(lower) ||
    /\bstart\s+(?:a\s+)?(?:new\s+)?(?:campaign|mission|direct\s+mail)\b/.test(
      lower
    )
  );
}

/**
 * Operator is replacing prior entities / campaign / starting over.
 * @param {string} text
 */
function isExplicitContextOverride(text) {
  const lower = String(text || '').toLowerCase();
  return (
    /\bstart\s+over\b/.test(lower) ||
    /\buse\s+these\s+\d+\s+prospects?\s+instead\b/.test(lower) ||
    (/\binstead\b/.test(lower) && /\b(?:prospects?|campaign)\b/.test(lower)) ||
    /\buse\s+a\s+different\s+campaign\b/.test(lower) ||
    /\bdifferent\s+campaign\b/.test(lower)
  );
}

/**
 * Explicit launch / mail / execute / approve language.
 * Active context never infers execution — these still require readiness checks.
 * @param {string} text
 */
function isExplicitExecutionRequest(text) {
  const lower = String(text || '').toLowerCase();
  return (
    /\bmail\s+(?:these|them|it|this)\s+now\b/.test(lower) ||
    /\b(?:please\s+)?mail\s+(?:these|them)\b/.test(lower) ||
    /\blaunch\s+(?:these|them|it|this|now)\b/.test(lower) ||
    /\bactually\s+launch\b/.test(lower) ||
    /\bexecute\s+(?:these|them|it|this|now|the\s+mail)\b/.test(lower) ||
    /\bapprove\s+(?:to\s+)?(?:mail|print|launch)\b/.test(lower) ||
    /\bprint\s+(?:and\s+)?mail\b/.test(lower) ||
    /\bsend\s+(?:these|them)\s+(?:out\s+)?now\b/.test(lower)
  );
}

/**
 * @param {string} text
 */
function isFillableTableRequest(text) {
  const lower = String(text || '').toLowerCase();
  return (
    /\bfillable\s+table\b/.test(lower) ||
    /\bmake\s+it\s+a\s+table\b/.test(lower) ||
    /\bconvert\b.+\b(?:into|to)\s+a\s+(?:fillable\s+)?table\b/.test(lower) ||
    /\bconvert\s+the\s+(?:verification\s+)?work\s+order\s+into\s+a\s+(?:fillable\s+)?table\b/.test(
      lower
    ) ||
    /\bturn\s+(?:this|it|the\s+(?:verification\s+)?work\s+order)\s+into\s+a\s+(?:fillable\s+)?table\b/.test(
      lower
    )
  );
}

/**
 * @param {string} text
 * @returns {string|null}
 */
function extractCampaignIdFromText(text) {
  const match = /\bcampaign\s+(\d+)\b/i.exec(String(text || ''));
  if (!match) return null;
  const n = String(match[1]);
  return n.length >= 3 ? n : n.padStart(3, '0');
}

/**
 * True when active canary constraints still forbid execution/mail.
 * @param {object|null} ctx
 */
function activeContextBlocksExecution(ctx) {
  if (!ctx || ctx.workflow !== 'campaign_canary') return false;
  const c = ctx.constraints || {};
  return (
    c.preparationOnly === true ||
    c.noExecution === true ||
    c.noMail === true ||
    c.noLaunch === true ||
    c.noPrint === true
  );
}

/**
 * @param {object|null} ctx
 */
function activeContextHasEntities(ctx) {
  return Boolean(
    ctx &&
      Array.isArray(ctx.entities) &&
      ctx.entities.length > 0
  );
}

function blankToNull(value) {
  if (value == null) return null;
  const s = String(value).trim();
  if (!s || /^unknown$/i.test(s) || /^n\/?a$/i.test(s)) return null;
  return s;
}

module.exports = {
  DEFAULT_CANARY_CONSTRAINTS,
  LAST_OUTPUT_TYPES,
  getActiveWorkContext,
  setActiveWorkContext,
  cloneActiveWorkContext,
  buildCanaryActiveWorkContext,
  prospectToEntity,
  entitiesToProspects,
  derivePendingFields,
  isActiveWorkFollowUpCue,
  isActiveWorkTransformCue,
  isExplicitNewMissionRequest,
  isExplicitContextOverride,
  isExplicitExecutionRequest,
  isFillableTableRequest,
  extractCampaignIdFromText,
  activeContextBlocksExecution,
  activeContextHasEntities,
};
