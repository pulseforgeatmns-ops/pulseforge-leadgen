'use strict';

/**
 * Customer-facing Paige outbound copy templates.
 * Internal Max/Scout intelligence informs strategy but never renders verbatim.
 */

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

function resolveServiceAreaLabel(plan = {}) {
  const geo = plan.geography || plan.market?.geography || {};
  const label = asText(geo.label || geo.city || geo.region);
  if (label) return label;
  const cities = Array.isArray(geo.cities) ? geo.cities.filter(Boolean) : [];
  if (cities.length) return cities[0];
  return 'Manchester';
}

function resolveSignOff(mission = {}, plan = {}) {
  const brand = asText(plan.brandName) || (isAnchorClient(mission, plan) ? 'Anchor Cleaning' : 'Our team');
  const sender = asText(plan.senderName || mission?.senderName);
  if (sender && brand) return `${sender}\n${brand}`;
  return brand;
}

function buildPropertyManagementCopy({ companyName, serviceArea, signOff }) {
  const subject = `Backup cleaning coverage for ${companyName}`;
  const body = [
    'Hi,',
    '',
    `I'm with Anchor Cleaning here in ${serviceArea}. We work with local property managers that need dependable cleaning coverage when their regular team is stretched, unavailable, or a property needs extra attention between turns.`,
    '',
    "I'm not reaching out to replace anyone you already work with. I wanted to see if it would be useful to have Anchor available as a backup or overflow resource when something comes up.",
    '',
    "If that's worth a quick conversation, I'd be happy to stop by and learn how you currently handle coverage.",
    '',
    signOff,
  ].join('\n');
  return {
    subject,
    body,
    cta: 'Reply if a quick conversation would be useful',
  };
}

function buildProfessionalOfficeCopy({ companyName, serviceArea, signOff, marketLabel }) {
  const segmentPhrase = marketLabel ? `${marketLabel}` : 'local businesses';
  const subject = `Commercial cleaning coverage for ${companyName}`;
  const body = [
    'Hi,',
    '',
    `I'm with Anchor Cleaning here in ${serviceArea}. We help ${segmentPhrase} with dependable commercial cleaning — including backup and overflow coverage when schedules get tight or a workspace needs extra attention.`,
    '',
    "I'm not trying to replace anyone you already work with. I wanted to see if it would be useful to have Anchor available when something comes up.",
    '',
    "If that sounds worth a quick conversation, I'd be happy to connect.",
    '',
    signOff,
  ].join('\n');
  return {
    subject,
    body,
    cta: 'Reply to schedule a quick conversation',
  };
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
  const serviceArea = resolveServiceAreaLabel(plan);
  const signOff = resolveSignOff(mission, plan);
  const marketLabel = asText(plan.market?.label).replace(/_/g, ' ') || null;
  const anchor = isAnchorClient(mission, plan);

  if (anchor && isPropertyManagementOutreach(plan, mission)) {
    return buildPropertyManagementCopy({ companyName, serviceArea, signOff });
  }
  if (anchor) {
    return buildProfessionalOfficeCopy({ companyName, serviceArea, signOff, marketLabel });
  }
  return buildGenericCommercialCopy({ companyName, serviceArea, signOff, marketLabel });
}

module.exports = {
  buildCustomerFacingVariantCopy,
  isPropertyManagementOutreach,
  isAnchorClient,
  resolveServiceAreaLabel,
  resolveSignOff,
};
