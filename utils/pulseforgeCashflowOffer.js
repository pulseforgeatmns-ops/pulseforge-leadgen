'use strict';

const PULSEFORGE_CLIENT_ID = 1;
const PULSEFORGE_TENANT_ID = '1';
const PULSEFORGE_CASHFLOW_OFFER_ID = 'pulseforge_cashflow_repair_sprint_v1';
const PULSEFORGE_CASHFLOW_OFFER = Object.freeze({
  id: PULSEFORGE_CASHFLOW_OFFER_ID,
  name: 'Pulseforge Website Repair Sprint',
  price: 500,
  currency: 'USD',
  paymentTerms: 'paid_upfront',
  turnaroundTargetHours: 48,
  scope: Object.freeze([
    'One to three agreed, observable website defects that interfere with contacting, booking, or using the site on mobile.',
    'Implementation and verification of the agreed fixes on the existing website.',
  ]),
  exclusions: Object.freeze([
    'Full redesigns or rebrands',
    'New custom applications',
    'Hosting or domain migrations',
    'Unbounded copywriting or content production',
  ]),
});

const DOCTRINE_BLOCKER = 'pulseforge_cashflow_offer_doctrine_violation';

const PROHIBITED = Object.freeze([
  { id: 'unverified_loss', re: /\blosing (?:customers|leads|sales|money)\b/i },
  { id: 'guaranteed_revenue', re: /\bguarantee(?:d|s)?\b.{0,50}\b(?:revenue|sales|leads)\b/i },
  { id: 'free_offer', re: /\bfree (?:audit|assessment|consultation|strategy call)\b/i },
  { id: 'redesign_pitch', re: /\b(?:full )?(?:website )?redesign\b/i },
  { id: 'fear_claim', re: /\byour (?:site|website) is broken\b/i },
]);

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function assessmentFrom(candidate = {}, row = {}) {
  return candidate.assessmentPayload
    || candidate.assessment_payload
    || row.website_opportunity_payload
    || row.assessment_payload
    || null;
}

function evidenceFindings(assessment = {}) {
  const refs = Array.isArray(assessment.evidence_refs) ? assessment.evidence_refs : [];
  const verified = Array.isArray(assessment.assessment?.verified_findings)
    ? assessment.assessment.verified_findings
    : [];
  const diagnosed = Array.isArray(assessment.commercial_diagnosis?.what_is_wrong)
    ? assessment.commercial_diagnosis.what_is_wrong
    : [];
  return [...refs, ...verified, ...diagnosed].filter((finding) =>
    ['MEASURED', 'OBSERVED'].includes(String(finding?.evidence_class || '').toUpperCase())
    && asText(finding.summary));
}

function humanizeFinding(finding = {}) {
  const id = asText(finding.id || finding.ref);
  const summary = asText(finding.summary);
  if (/form|submit/i.test(`${id} ${summary}`)) return 'the contact form did not complete cleanly during my check';
  if (/booking|book|appointment/i.test(`${id} ${summary}`)) return 'the booking path did not lead to a working next step during my check';
  if (/contact|phone|navigation|cta/i.test(`${id} ${summary}`)) return 'the path to contact you was harder to find than it should be';
  if (/viewport|mobile|responsive/i.test(`${id} ${summary}`)) return 'the mobile layout has a usability issue worth fixing';
  if (/http_error|404|dead|broken link/i.test(`${id} ${summary}`)) return 'one of the site paths returned an error during my check';
  if (/slow|fetch|lcp|performance/i.test(`${id} ${summary}`)) return 'the homepage was slow during my check';
  return summary.charAt(0).toLowerCase() + summary.slice(1).replace(/[.]$/, '');
}

function resolvePulseforgeRepairEvidence(candidate = {}, row = {}) {
  const assessment = assessmentFrom(candidate, row);
  if (!assessment) return null;
  const diagnosis = assessment.commercial_diagnosis?.diagnosis_class
    || assessment.assessment?.commercial_diagnosis?.diagnosis_class
    || row.website_diagnosis_class;
  if (diagnosis && diagnosis !== 'TARGETED_REMEDIATION') return null;
  const finding = evidenceFindings(assessment)[0];
  if (!finding) return null;
  return {
    finding,
    observation: humanizeFinding(finding),
    assessmentId: row.website_opportunity_assessment_id || candidate.assessmentId || null,
    diagnosis: diagnosis || 'TARGETED_REMEDIATION',
  };
}

function pulseforgeAssessmentReason(assessment) {
  if (!assessment || typeof assessment !== 'object') return 'website_assessment_missing';
  const diagnosis = assessment.commercial_diagnosis?.diagnosis_class
    || assessment.assessment?.commercial_diagnosis?.diagnosis_class;
  if (diagnosis !== 'TARGETED_REMEDIATION') return 'targeted_remediation_not_established';
  if (!evidenceFindings(assessment).length) return 'observable_website_defect_missing';
  if (assessment.recommended_action === 'DO_NOT_PURSUE') return 'website_opportunity_not_pursuable';
  return null;
}

function buildPulseforgeRepairSprintVariant({ candidate = {}, crmRecord = null } = {}) {
  const row = crmRecord || {};
  const evidence = resolvePulseforgeRepairEvidence(candidate, row);
  if (!evidence) {
    return { held: true, holdReason: 'observable_website_defect_required', evidence: null };
  }
  const companyName = asText(candidate.name || candidate.companyName || row.company_name) || 'your business';
  const firstName = asText(row.first_name || candidate.firstName || candidate.first_name).split(/\s+/)[0];
  const greeting = firstName ? `Hi ${firstName},` : 'Hi there,';
  const subject = `Quick website fix for ${companyName}`;
  const cta = 'Want me to send the exact scope I’d fix for $500?';
  const body = [
    greeting,
    '',
    `I was looking at ${companyName}’s website and found ${evidence.observation}.`,
    '',
    'I run Pulseforge. I offer a fixed $500 website repair sprint for agreed issues like this. It’s paid upfront, and I target the agreed fixes within 48 hours.',
    '',
    cta,
    '',
    'Jacob',
  ].join('\n');
  return { held: false, subject, body, cta, evidence };
}

function validatePulseforgeCashflowCopy(input = {}) {
  const text = [input.subject, input.body, input.cta].map(asText).join('\n');
  const violations = PROHIBITED.filter(({ re }) => re.test(text)).map(({ id }) => id);
  const required = [
    ['price', /\$500\b/],
    ['brand', /\bPulseforge\b/],
    ['payment_terms', /\bpaid upfront\b/i],
    ['turnaround', /\b48 hours\b/i],
    ['sender', /(?:^|\n)Jacob\s*$/m],
  ];
  for (const [id, re] of required) if (!re.test(text)) violations.push(`missing_${id}`);
  return { ok: violations.length === 0, blocker: violations.length ? DOCTRINE_BLOCKER : null, violations };
}

function isPulseforgeCashflowClient(clientId) {
  return Number(clientId) === PULSEFORGE_CLIENT_ID;
}

module.exports = {
  PULSEFORGE_CLIENT_ID,
  PULSEFORGE_TENANT_ID,
  PULSEFORGE_CASHFLOW_OFFER_ID,
  PULSEFORGE_CASHFLOW_OFFER,
  DOCTRINE_BLOCKER,
  isPulseforgeCashflowClient,
  resolvePulseforgeRepairEvidence,
  pulseforgeAssessmentReason,
  buildPulseforgeRepairSprintVariant,
  validatePulseforgeCashflowCopy,
};
