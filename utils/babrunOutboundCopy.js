'use strict';

const { BABRUN_MAILBOX, CLIENT_ID: BABRUN_CLIENT_ID } = require('../scripts/lib/babrunCanonicalOutbound');

const ANCHOR_COPY_MARKERS = [
  /\bcommercial cleaning\b/i,
  /\banchor cleaning\b/i,
  /\bgreater manchester\b/i,
  /\bshort[- ]term rental\b/i,
  /\bfacility assessment\b/i,
  /goanchorcleaning\.com/i,
  /jacob@goanchorcleaning\.com/i,
];

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function isBabrunClient(mission = {}, plan = {}) {
  const clientId = Number(mission.clientId ?? mission.tenantId ?? plan.clientId ?? 0);
  if (clientId === BABRUN_CLIENT_ID) return true;
  const hay = [
    plan.brandName,
    mission.title,
    plan.senderEmail,
    mission.senderEmail,
  ].filter(Boolean).join(' ').toLowerCase();
  return /\bbabrun\b/.test(hay) || hay.includes('hello@babrun.com');
}

function resolveBabrunSignOff(mission = {}, plan = {}) {
  const sender = asText(plan.senderName || mission.senderName || BABRUN_MAILBOX.senderDisplayName);
  const brand = asText(plan.brandName) || 'Babrun';
  if (sender && brand) return `${sender}\n${brand}`;
  return sender || brand;
}

function resolveProgramPhrase(mission = {}, plan = {}) {
  const objective = asText(plan.objective || mission.objective);
  if (/12[- ]week/i.test(objective)) return '12-week business transformation program';
  return '12-week program for founder-led businesses';
}

function resolveGeographyLabel(plan = {}, mission = {}) {
  const region = asText(plan.geography?.region || mission.structuredMission?.geography?.region);
  return region || 'the United States';
}

/**
 * Canonical Babrun first-touch copy for founder-led SMB acquisition (tenant 13).
 */
function buildBabrunOutboundCopy({ companyName, plan = {}, mission = {} } = {}) {
  const name = asText(companyName) || 'your business';
  const signOff = resolveBabrunSignOff(mission, plan);
  const program = resolveProgramPhrase(mission, plan);
  const geography = resolveGeographyLabel(plan, mission);
  const subject = `Quick question about ${name}`;
  const body = [
    'Hi,',
    '',
    `I came across ${name} and the work you're doing as a founder-led business in ${geography}.`,
    '',
    `Babrun runs a ${program} for owners who want to strengthen how the business runs—not just add more tactics.`,
    '',
    'If you are open to a short conversation about whether that kind of operating change is relevant for you right now, I would welcome it.',
    '',
    signOff,
  ].join('\n');
  return {
    subject,
    body,
    cta: 'Reply if a brief conversation would be useful',
  };
}

function assertNoAnchorCopyMarkers(text) {
  const hay = asText(text);
  return ANCHOR_COPY_MARKERS.filter((pattern) => pattern.test(hay)).map((pattern) => String(pattern));
}

module.exports = {
  BABRUN_CLIENT_ID,
  ANCHOR_COPY_MARKERS,
  isBabrunClient,
  buildBabrunOutboundCopy,
  assertNoAnchorCopyMarkers,
};
