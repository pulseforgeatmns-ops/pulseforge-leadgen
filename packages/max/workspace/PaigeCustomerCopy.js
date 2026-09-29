'use strict';

/**
 * Customer-facing Paige outbound copy templates.
 * Internal Max/Scout intelligence informs strategy but never renders verbatim.
 *
 * Anchor Cleaning (client_id=10) copy is owned by Paige via anchorCopyDoctrine.
 */

const {
  buildAnchorCopy,
  resolveSegment,
  resolveServiceAreaLabel,
  LIFECYCLE_STAGES,
  SEGMENTS,
} = require('../../../utils/anchorCopyDoctrine');

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
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

function isPropertyManagementOutreach(plan = {}, mission = {}) {
  const hay = normalizeSegmentText(plan, mission);
  return /\b(str|short[- ]term rental|property manag|vacation rental|turnover)\b/i.test(hay);
}

function isAnchorClient(mission = {}, plan = {}) {
  const clientId = mission?.clientId ?? Number(mission?.tenantId);
  if (Number(clientId) === 10) return true;
  const hay = [
    plan.brandName,
    plan.senderName,
    mission?.title,
  ].filter(Boolean).join(' ').toLowerCase();
  return /\banchor cleaning\b/.test(hay);
}

function resolveSignOff(mission = {}, plan = {}) {
  const brand = asText(plan.brandName) || (isAnchorClient(mission, plan) ? 'Anchor Cleaning' : 'Our team');
  const sender = asText(plan.senderName || mission?.senderName);
  if (sender && brand) return `${sender}\n${brand}`;
  return brand;
}

function buildGenericCommercialCopy({ companyName, serviceArea, signOff, marketLabel }) {
  const segmentPhrase = marketLabel || 'local offices';
  const subject = `Commercial cleaning for ${companyName}`;
  const body = [
    'Hi,',
    '',
    `We help ${segmentPhrase} in ${serviceArea} keep workspaces clean and presentable.`,
    '',
    "If you're open to a brief conversation about how you handle cleaning coverage today, I'd be glad to connect.",
    '',
    signOff,
  ].join('\n');
  return {
    subject,
    body,
    cta: 'Reply if a quick conversation would be useful',
  };
}

function buildAnchorVariantCopy({ companyName, plan = {}, mission = {} }) {
  const serviceArea = resolveServiceAreaLabel(plan);
  const segment = isPropertyManagementOutreach(plan, mission)
    ? SEGMENTS.PROPERTY_MANAGEMENT
    : resolveSegment(plan, mission);
  const senderName = asText(plan.senderName || mission?.senderName) || 'Jacob Maynard';

  const copy = buildAnchorCopy({
    lifecycleStage: LIFECYCLE_STAGES.COLD,
    companyName,
    senderName,
    serviceArea,
    segment,
  });

  return {
    subject: copy.subject,
    body: copy.body,
    cta: copy.cta,
  };
}

/**
 * Build customer-facing subject/body/cta for one prospect-bound variant.
 * @param {object} opts
 * @param {string} opts.companyName
 * @param {object} [opts.plan]
 * @param {object} [opts.mission]
 */
function buildCustomerFacingVariantCopy(opts = {}) {
  const companyName = asText(opts.companyName) || 'your team';
  const plan = opts.plan || {};
  const mission = opts.mission || {};

  if (isAnchorClient(mission, plan)) {
    return buildAnchorVariantCopy({ companyName, plan, mission });
  }

  const serviceArea = resolveServiceAreaLabel(plan);
  const signOff = resolveSignOff(mission, plan);
  const marketLabel = asText(plan.market?.label).replace(/_/g, ' ') || null;
  return buildGenericCommercialCopy({ companyName, serviceArea, signOff, marketLabel });
}

module.exports = {
  buildCustomerFacingVariantCopy,
  isPropertyManagementOutreach,
  isAnchorClient,
  resolveServiceAreaLabel,
  resolveSignOff,
};
