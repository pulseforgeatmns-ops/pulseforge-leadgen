'use strict';

const PULSEFORGE_CLIENT_ID = 1;
const PULSEFORGE_TENANT_ID = '1';
// New identity: existing reviewed repair-sprint missions are historical records.
const PULSEFORGE_CASHFLOW_OFFER_ID = 'pulseforge_missed_revenue_review_v1';
const PULSEFORGE_CASHFLOW_OFFER = Object.freeze({
  id: PULSEFORGE_CASHFLOW_OFFER_ID,
  name: 'Missed Revenue Review',
  price: 297,
  currency: 'USD',
  scope: Object.freeze([
    'Review public inquiry and booking paths, and the existing follow-up workflow with permission.',
    'Identify up to three actionable improvements and provide a prioritized plan.',
    'Provide a 30-minute walkthrough of the review and plan.',
  ]),
  exclusions: Object.freeze([
    'Implementation is separately quoted.',
    'No revenue promises or invented findings.',
    'No private customer data or system access without explicit permission.',
  ]),
});
const DOCTRINE_BLOCKER = 'pulseforge_cashflow_offer_doctrine_violation';
const SOURCE_REVIEW_BLOCKER = 'pulseforge_reviewed_offer_required';
const MANUAL_FIRST_TOUCH_BLOCKER = 'pulseforge_prior_manual_first_touch';
const PULSEFORGE_TARGET_INDUSTRIES = Object.freeze(['plumbing', 'heating', 'hvac']);
// Conservative initial coverage, explicitly reviewed as part of the new mission.
const PULSEFORGE_TARGET_CITIES = Object.freeze([
  'Manchester', 'Nashua', 'Bedford', 'Merrimack', 'Hudson', 'Londonderry', 'Derry',
  'Salem', 'Windham', 'Pelham', 'Hollis', 'Amherst', 'Milford', 'Litchfield',
  'Goffstown', 'Auburn', 'Chester', 'Hampstead', 'Atkinson', 'Plaistow',
  'Exeter', 'Epping', 'Kingston', 'Hampton', 'Seabrook', 'Portsmouth',
]);
// Operator-reported October 13 manual contacts. These records are an exclusion
// receipt, not an assertion that a provider delivered a message or of its copy.
// Never insert DNC/suppression or rewrite the actual manual offer/touchpoint.
const PULSEFORGE_MANUAL_FIRST_TOUCHES = Object.freeze([
  Object.freeze({ business: 'Done Right', email: 'Andreflynn687@gmail.com', domain: 'donerightheating.com', date: '2026-10-13' }),
  Object.freeze({ business: 'Overflow', email: 'office@overflowplumbing.co', domain: 'overflowplumbing.co', date: '2026-10-13' }),
  Object.freeze({ business: 'Clear All', email: 'ClearAllCH@gmail.com', domain: 'clearallcoolingandheating.com', date: '2026-10-13' }),
]);
const FIRST_TOUCH_EXCLUSIONS_VERSION = 'pulseforge_manual_contacts_2026_10_13_v1';
const asText = value => value == null ? '' : String(value).trim();

function isPulseforgeCashflowClient(clientId) {
  return Number(clientId) === PULSEFORGE_CLIENT_ID;
}
function domainOf(value) {
  const text = asText(value).toLowerCase();
  if (!text) return '';
  try { return new URL(text.includes('://') ? text : `https://${text}`).hostname.replace(/^www\./, '').replace(/\.$/, ''); }
  catch (_) { return ''; }
}
function pulseforgeFirstTouchExclusionReason(candidate = {}, context = {}) {
  // This helper is only for new automated first touches, never reply handling.
  if (String(context.tenantId) !== PULSEFORGE_TENANT_ID || context.firstTouch !== true) return null;
  const emails = [candidate.email, candidate.recipientEmail, candidate.toEmail].map(x => asText(x).toLowerCase());
  const domains = [candidate.domain, candidate.website, candidate.company_domain, candidate.company_website, candidate.website_url]
    .map(domainOf).filter(Boolean);
  return PULSEFORGE_MANUAL_FIRST_TOUCHES.some(row => emails.includes(row.email.toLowerCase())
    || domains.some(domain => domain === row.domain || domain.endsWith(`.${row.domain}`)))
    ? MANUAL_FIRST_TOUCH_BLOCKER : null;
}
function pulseforgeTargetReason(row = {}) {
  const excluded = pulseforgeFirstTouchExclusionReason(row, { tenantId: '1', firstTouch: true });
  if (excluded) return excluded;
  if (row.service_area_match === false) return 'service_area_not_confirmed';
  const industries = [row.vertical, row.industry].map(asText).join(' ');
  if (!/\b(?:plumb(?:ing|er|ers)?|heating|hvac|air[ _-]conditioning)\b/i.test(industries.replace(/_/g, ' '))) return 'mission_segment_mismatch';
  // Generic service_area_match=true may belong to an older/nationwide mission.
  // Require attributable geography rather than reusing that boolean as consent.
  const locations = [row.company_location, row.location, typeof row.service_area_match === 'string' ? row.service_area_match : ''].filter(Boolean);
  const inScope = locations.some(location => /\b(?:NH|New Hampshire)\b/i.test(location)
    && PULSEFORGE_TARGET_CITIES.some(name => new RegExp(`\\b${name}\\b`, 'i').test(location)));
  if (!inScope) return 'service_area_not_confirmed';
  if (!domainOf(row.company_website || row.website || row.company_domain || row.domain)) return 'public_website_required';
  return null;
}
function sameMembers(actual, expected) {
  return Array.isArray(actual) && actual.length === expected.length
    && [...actual].sort().every((value, i) => value === [...expected].sort()[i]);
}
function pulseforgeMissionReason(mission = {}, { requireReviewed = true } = {}) {
  if (!mission) return SOURCE_REVIEW_BLOCKER;
  const structured = mission.structuredMission || mission.payload?.structuredMission;
  if (!structured || (requireReviewed && structured.immutable !== true)) return SOURCE_REVIEW_BLOCKER;
  const offer = structured.offer;
  if (!offer || offer.id !== PULSEFORGE_CASHFLOW_OFFER_ID || offer.price !== 297 || offer.currency !== 'USD'
    || offer.name !== PULSEFORGE_CASHFLOW_OFFER.name || offer.paymentTerms != null || offer.turnaroundTargetHours != null
    || !sameMembers(offer.scope, PULSEFORGE_CASHFLOW_OFFER.scope)
    || !sameMembers(offer.exclusions, PULSEFORGE_CASHFLOW_OFFER.exclusions)) return SOURCE_REVIEW_BLOCKER;
  if (structured.geography?.region !== 'Southern New Hampshire' || structured.geography?.scope !== 'regional'
    || !sameMembers(structured.geography?.cities, PULSEFORGE_TARGET_CITIES)
    || !sameMembers(structured.market?.eligibleSubsegments, PULSEFORGE_TARGET_INDUSTRIES)
    || structured.firstTouchExclusionsVersion !== FIRST_TOUCH_EXCLUSIONS_VERSION) return SOURCE_REVIEW_BLOCKER;
  return null;
}

function buildPulseforgeReviewVariant({ candidate = {}, crmRecord = null } = {}) {
  const row = { ...candidate, ...(crmRecord || {}) };
  const reason = pulseforgeTargetReason(row);
  if (reason) return { held: true, holdReason: reason };
  const companyName = asText(candidate.name || candidate.companyName || row.company_name) || 'your business';
  const firstName = asText(row.first_name || candidate.firstName || candidate.first_name).split(/\s+/)[0];
  const subject = `Missed Revenue Review for ${companyName}`;
  const cta = 'Would you like the review details?';
  // An invitation to review. It does not invent an audit finding or suggest that
  // private follow-up/customer systems have already been accessed.
  const body = [
    firstName ? `Hi ${firstName},` : 'Hi there,', '',
    'I’m Jacob from Pulseforge. I offer a $297 Missed Revenue Review for plumbing, heating and HVAC businesses in southern New Hampshire.', '',
    'I review your public inquiry and booking paths and, with your permission, your existing follow-up workflow. You get up to three actionable improvements, a prioritized plan, and a 30-minute walkthrough.', '',
    'Implementation is quoted separately. Any access to private customer information or systems needs your explicit permission.', '',
    cta, '', 'Jacob',
  ].join('\n');
  return { held: false, subject, body, cta };
}
const PROHIBITED = Object.freeze([
  { id: 'unverified_loss', re: /\b(?:losing|lost|missing out on) (?:customers|leads|sales|money|revenue)\b/i },
  { id: 'revenue_promise', re: /\b(?:guarantee(?:d|s)?|will|promise|increase|boost|recover)\b.{0,50}\b(?:revenue|sales|leads)\b/i },
  { id: 'unverified_finding', re: /\b(?:I|we) (?:found|noticed|tested|checked)|\byour (?:site|website) is broken\b/i },
  { id: 'stale_offer', re: /\$500\b|repair sprint|paid up[ -]?front|payment up[ -]?front|up[ -]?front payment|48[ -]hours?/i },
  { id: 'free_offer', re: /\bfree (?:audit|assessment|consultation|strategy call)\b/i },
  { id: 'included_implementation', re: /(?:implementation|fixes|repairs) (?:is |are )?included/i },
]);
function validatePulseforgeCashflowCopy(input = {}) {
  input = input || {};
  const text = [input.subject, input.body, input.cta].map(asText).join('\n');
  const violations = PROHIBITED.filter(({ re }) => re.test(text)).map(({ id }) => id);
  const required = [
    ['price', /\$297\b/], ['offer', /Missed Revenue Review/], ['brand', /\bPulseforge\b/],
    ['bounded_improvements', /up to (?:three|3) actionable improvements/i],
    ['plan', /prioritized plan/i], ['walkthrough', /30[ -]minute walkthrough/i],
    ['separate_implementation', /implementation is quoted separately/i],
    ['permission', /with your permission.{0,20}your existing follow-up workflow/i],
    ['sender', /(?:^|\n)Jacob\s*$/m],
  ];
  for (const [id, re] of required) if (!re.test(text)) violations.push(`missing_${id}`);
  return { ok: violations.length === 0, blocker: violations.length ? DOCTRINE_BLOCKER : null, violations };
}
module.exports = {
  PULSEFORGE_CLIENT_ID, PULSEFORGE_TENANT_ID, PULSEFORGE_CASHFLOW_OFFER_ID, PULSEFORGE_CASHFLOW_OFFER,
  DOCTRINE_BLOCKER, SOURCE_REVIEW_BLOCKER, MANUAL_FIRST_TOUCH_BLOCKER,
  PULSEFORGE_TARGET_INDUSTRIES, PULSEFORGE_TARGET_CITIES,
  PULSEFORGE_MANUAL_FIRST_TOUCHES, FIRST_TOUCH_EXCLUSIONS_VERSION,
  isPulseforgeCashflowClient, pulseforgeFirstTouchExclusionReason, pulseforgeTargetReason,
  pulseforgeMissionReason, buildPulseforgeReviewVariant, validatePulseforgeCashflowCopy,
};
