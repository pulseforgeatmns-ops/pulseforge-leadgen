'use strict';

/**
 * SPEC-STUDIO-SCOUT-001 — Studio Substral Opportunity Intelligence.
 * Deterministic scoring + qualification on top of Website Opportunity Intelligence (WOI).
 * No outbound; structured intelligence for Max review and Paige draft input.
 */

const {
  SCORE_COMPONENTS,
  DIAGNOSIS_CLASS,
  RECOMMENDED_ACTIONS,
} = require('../packages/capabilities/websiteOpportunityIntelligence/types');

const STUDIO_MIN_FIT_SCORE = 65;
const STUDIO_PRIORITY_THRESHOLD = 80;

const STUDIO_OUTREACH_STATUS = Object.freeze([
  'new',
  'review_needed',
  'approved_for_outreach',
  'drafted',
  'sent',
  'follow_up_needed',
  'replied',
  'not_fit',
  'closed',
]);

const STUDIO_SCOUT_INSTRUCTION = `You are Scout running Studio Substral Opportunity Intelligence.

Your job is to find established local and regional businesses whose current website appears to undersell the quality, maturity, or value of the business.

Do not select prospects just because the site is ugly. Select prospects where a better website could plausibly improve trust, quote volume, booking volume, sales readiness, or perceived credibility.

Prioritize professional services, property and home services, medical/wellness/aesthetics, and B2B service firms.

For every prospect, evaluate business value fit, website pain, proof gap, outreach angle quality, and contactability.

Return only prospects with a Studio Fit Score of 65 or higher unless explicitly asked for raw discovery.

Every accepted prospect must include a specific outreach angle. Avoid generic website-audit language.

The best prospects are businesses that look operationally credible but digitally under-positioned.`;

const GENERIC_OUTREACH_PATTERNS = [
  /\byour website could use an update\b/i,
  /\bimprove your online presence\b/i,
  /\bwebsite is outdated\b/i,
  /\bhelp businesses grow online\b/i,
  /\bwe can (?:redesign|revamp|modernize) your website\b/i,
];

const STUDIO_CATEGORY = Object.freeze({
  PROFESSIONAL: 'professional_services',
  PROPERTY_HOME: 'property_and_home_services',
  MEDICAL_WELLNESS: 'medical_wellness_and_aesthetics',
  B2B: 'b2b_service_firms',
});

const PROFESSIONAL_VERTICALS = new Set([
  'law_firm', 'legal', 'accounting', 'accountant', 'cpa', 'consultant', 'consulting',
  'insurance', 'insurance_agency', 'financial_advisor', 'financial', 'cfo',
]);

const PROPERTY_HOME_VERTICALS = new Set([
  'property_management', 'cleaning', 'restoration', 'landscaping', 'hvac', 'roofing',
  'painting', 'home_services', 'plumbing', 'electrical', 'property',
]);

const MEDICAL_VERTICALS = new Set([
  'med_spa', 'dental', 'physical_therapy', 'chiropractic', 'therapy', 'clinic', 'wellness',
  'fitness',
]);

const B2B_VERTICALS = new Set([
  'it', 'msp', 'recruiting', 'manufacturing', 'logistics', 'engineering', 'security',
  'architecture_engineering',
]);

const HIGH_TRUST_VERTICALS = new Set([
  ...PROFESSIONAL_VERTICALS,
  ...MEDICAL_VERTICALS,
  'property_management',
  'insurance',
]);

const RESTAURANT_RE = /\brestaurant\b|\bcafe\b|\bdiner\b|\bpizzeria\b/i;
const CATERING_RE = /\bcatering\b|\bevents?\b|\bprivate dining\b/i;

const STUDIO_SUBSTRAL_SCOUT_PLAN = Object.freeze({
  state: 'NH',
  cities: [
    'Manchester', 'Bedford', 'Goffstown', 'Hooksett', 'Londonderry', 'Auburn',
    'Nashua', 'Concord', 'Derry', 'Merrimack', 'Hudson', 'Pelham', 'Salem',
  ],
  batch_mix: Object.freeze({
    professional_services: 10,
    property_and_home_services: 7,
    medical_wellness_and_aesthetics: 5,
    b2b_service_firms: 3,
  }),
  verticals: Object.freeze({
    professional_services: [
      'accounting firm {city} {state}',
      'CPA {city} {state}',
      'law firm {city} {state}',
      'insurance agency {city} {state}',
      'financial advisor {city} {state}',
      'consulting firm {city} {state}',
    ],
    property_and_home_services: [
      'property management {city} {state}',
      'HVAC company {city} {state}',
      'roofing contractor {city} {state}',
      'landscaping company {city} {state}',
      'commercial cleaning {city} {state}',
      'restoration company {city} {state}',
    ],
    medical_wellness_and_aesthetics: [
      'med spa {city} {state}',
      'dental office {city} {state}',
      'chiropractor {city} {state}',
      'physical therapy {city} {state}',
      'therapy practice {city} {state}',
    ],
    b2b_service_firms: [
      'managed IT services {city} {state}',
      'recruiting firm {city} {state}',
      'engineering firm {city} {state}',
      'security company {city} {state}',
    ],
  }),
});

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function scaleScore(raw, rawMax, targetMax) {
  if (!rawMax) return 0;
  return clamp(Math.round((raw / rawMax) * targetMax), 0, targetMax);
}

function normalizeVerticalKey(vertical) {
  return String(vertical || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_|_$/g, '');
}

function mapIndustryToStudioCategory(vertical, companyName = '', snippet = '') {
  const key = normalizeVerticalKey(vertical);
  const hay = `${companyName} ${snippet}`.toLowerCase();

  if (PROFESSIONAL_VERTICALS.has(key) || /\b(attorney|law|cpa|accounting|insurance|advisor|consult)/i.test(hay)) {
    return STUDIO_CATEGORY.PROFESSIONAL;
  }
  if (PROPERTY_HOME_VERTICALS.has(key) || /\b(hvac|roof|landscap|property manag|restoration|cleaning)\b/i.test(hay)) {
    return STUDIO_CATEGORY.PROPERTY_HOME;
  }
  if (MEDICAL_VERTICALS.has(key) || /\b(dental|med spa|chiro|therapy|clinic|wellness)\b/i.test(hay)) {
    return STUDIO_CATEGORY.MEDICAL_WELLNESS;
  }
  if (B2B_VERTICALS.has(key) || /\b(IT|MSP|recruit|logistics|engineering|security)\b/i.test(hay)) {
    return STUDIO_CATEGORY.B2B;
  }
  return STUDIO_CATEGORY.PROFESSIONAL;
}

function isGenericOutreachAngle(text) {
  const t = String(text || '').trim();
  if (!t) return true;
  return GENERIC_OUTREACH_PATTERNS.some((re) => re.test(t));
}

function componentScore(components, key) {
  return components?.[key]?.score ?? 0;
}

function extractWebsiteIssues(assessmentPayload = {}) {
  const findings = []
    .concat(assessmentPayload.evidence_refs || [])
    .concat(assessmentPayload.assessment?.verified_findings || [])
    .concat(assessmentPayload.assessment?.inferred_findings || []);

  const issues = [];
  const seen = new Set();
  for (const f of findings) {
    const summary = f?.summary;
    if (!summary || seen.has(summary)) continue;
    seen.add(summary);
    if (f.category === 'performance' || f.id?.includes('perf') || f.id?.includes('mobile')) {
      issues.push(summary);
    } else if (f.category === 'conversion' || f.id?.startsWith('conv')) {
      issues.push(summary);
    } else if (f.category === 'trust' || f.id?.includes('trust')) {
      issues.push(summary);
    } else if (['seo', 'accessibility', 'technical_health', 'design'].includes(f.category)) {
      issues.push(summary);
    }
    if (issues.length >= 8) break;
  }
  return issues;
}

function businessStrengthSignals(lead = {}, assessmentPayload = {}) {
  const signals = [];
  const rating = lead.google_rating ?? assessmentPayload.assessment?.prospect?.business_evidence?.google_rating;
  const reviews = lead.google_review_count ?? assessmentPayload.assessment?.prospect?.business_evidence?.google_review_count;
  if (reviews != null && Number(reviews) >= 10) {
    signals.push(`${reviews} Google reviews (${rating != null ? `avg ${rating}` : 'rating on file'})`);
  } else if (reviews != null && Number(reviews) >= 3) {
    signals.push(`${reviews} Google reviews suggest an operating business`);
  }
  const hay = `${lead.company || ''} ${lead.address || ''} ${lead.snippet || ''}`.toLowerCase();
  if (/\b(llc|inc|corp|pllc)\b/.test(hay)) signals.push('Registered business entity visible in listing');
  if (lead.phone) signals.push('Published phone number on discovery record');
  if (lead.address) signals.push('Physical address on discovery record');
  return signals.slice(0, 6);
}

function buildRecommendedOutreachAngle({
  companyName,
  businessStrengthSignals: strengths,
  websiteIssues,
  studioCategory,
}) {
  const name = companyName || 'This business';
  if (strengths.length && websiteIssues.length) {
    return `${name} shows real-world credibility (${strengths[0]}), but the site does not carry that same weight — ${websiteIssues[0].replace(/\.$/, '')}, which can make a ${studioCategory.replace(/_/g, ' ')} buyer hesitate before reaching out.`;
  }
  if (strengths.length) {
    return `${name} looks stronger operationally than the homepage makes it look — trust cues and a clear next step are thin relative to the service they sell.`;
  }
  if (websiteIssues.length) {
    return `${name} offers a high-trust service, but the homepage does not quickly explain who they serve or why a buyer should feel confident — ${websiteIssues[0].replace(/\.$/, '')}.`;
  }
  return `${name} appears to be an established regional operator, but the website lacks proof, process, and a clear next step relative to the value of the work.`;
}

function buildFirstMessageNotes(intelligence) {
  return [
    'Lead with the credibility-vs-site mismatch, not a redesign pitch.',
    intelligence.recommended_outreach_angle,
    intelligence.website_issues_observed?.length
      ? `Evidence hooks: ${intelligence.website_issues_observed.slice(0, 2).join('; ')}`
      : 'Use only verified website findings — do not invent issues.',
    intelligence.business_strength_signals?.length
      ? `Strength anchors: ${intelligence.business_strength_signals.slice(0, 2).join('; ')}`
      : null,
  ].filter(Boolean).join('\n');
}

function scoreOutreachAngleQuality(angle) {
  if (isGenericOutreachAngle(angle)) return 4;
  let score = 10;
  if (/stronger than|undersell|credibility|trust|buyer|reach out|next step/i.test(angle)) score += 3;
  if (angle.length > 80) score += 2;
  return clamp(score, 0, 15);
}

function computeProofGap(lead, components, websiteIssues, strengths) {
  let score = 0;
  const reviews = Number(lead.google_review_count || 0);
  const rating = Number(lead.google_rating || 0);
  const deficiency = componentScore(components, SCORE_COMPONENTS.WEBSITE_DEFICIENCY);

  if (reviews >= 15 && rating >= 4 && deficiency >= 8) score += 8;
  else if (reviews >= 5 && deficiency >= 6) score += 5;
  else if (strengths.length >= 2 && websiteIssues.length >= 2) score += 6;
  else if (websiteIssues.length >= 1 && strengths.length >= 1) score += 4;
  return clamp(score, 0, 15);
}

function trustSensitiveCategory(studioCategory, verticalKey) {
  if (HIGH_TRUST_VERTICALS.has(verticalKey)) return true;
  return [
    STUDIO_CATEGORY.PROFESSIONAL,
    STUDIO_CATEGORY.MEDICAL_WELLNESS,
    STUDIO_CATEGORY.B2B,
  ].includes(studioCategory);
}

function countQualificationSignals(ctx) {
  const signals = {
    established: false,
    website_pain: false,
    meaningful_value: false,
    trust_matters: false,
    outreach_angle: false,
  };

  const reviews = Number(ctx.lead?.google_review_count || 0);
  const rating = Number(ctx.lead?.google_rating || 0);
  const hay = `${ctx.lead?.company || ''} ${ctx.lead?.address || ''}`.toLowerCase();
  signals.established = reviews >= 3
    || (reviews >= 1 && rating >= 4)
    || /\b(llc|inc|corp|pllc)\b/.test(hay)
    || Boolean(ctx.lead?.address && ctx.lead?.phone);

  const deficiency = componentScore(ctx.components, SCORE_COMPONENTS.WEBSITE_DEFICIENCY);
  const diagnosis = ctx.diagnosisClass;
  signals.website_pain = deficiency >= 8
    || ctx.websiteIssues.length >= 2
    || (diagnosis && diagnosis !== DIAGNOSIS_CLASS.HEALTHY_SITE);

  const commercial = componentScore(ctx.components, SCORE_COMPONENTS.COMMERCIAL_VALUE);
  signals.meaningful_value = commercial >= 8 || trustSensitiveCategory(ctx.studioCategory, ctx.verticalKey);

  signals.trust_matters = trustSensitiveCategory(ctx.studioCategory, ctx.verticalKey);

  signals.outreach_angle = Boolean(ctx.recommendedOutreachAngle)
    && !isGenericOutreachAngle(ctx.recommendedOutreachAngle);

  const count = Object.values(signals).filter(Boolean).length;
  return { signals, count };
}

function hardRejectReason(ctx) {
  const { lead, assessmentPayload, components, diagnosisClass, studioCategory } = ctx;
  const hay = `${lead.company || ''} ${lead.snippet || ''} ${lead.url || ''}`.toLowerCase();
  const reviews = Number(lead.google_review_count || 0);

  if (RESTAURANT_RE.test(hay) && !CATERING_RE.test(hay)) {
    return 'Restaurant without catering/events/private dining focus';
  }
  if (reviews === 0 && !/\b(llc|inc|corp)\b/.test(hay) && !lead.address) {
    return 'No proof of established operation';
  }
  if (diagnosisClass === DIAGNOSIS_CLASS.HEALTHY_SITE) {
    return 'Website already strong — limited redesign upside';
  }
  const deficiency = componentScore(components, SCORE_COMPONENTS.WEBSITE_DEFICIENCY);
  const commercial = componentScore(components, SCORE_COMPONENTS.COMMERCIAL_VALUE);
  if (deficiency < 5 && commercial < 8) {
    return 'Cosmetic-only gap with weak commercial upside';
  }
  if (ctx.recommendedAction === RECOMMENDED_ACTIONS.DO_NOT_PURSUE && deficiency < 10) {
    return 'WOI recommends do-not-pursue with insufficient website pain';
  }
  if (studioCategory == null) {
    return 'Could not classify studio category';
  }
  return null;
}

function mapConfidence(scoringConfidence, signalCount) {
  if (scoringConfidence >= 0.7 && signalCount >= 4) return 'high';
  if (scoringConfidence >= 0.5 && signalCount >= 3) return 'medium';
  return 'low';
}

function computeStudioFitScore(ctx) {
  const components = ctx.components || {};
  const business_value_fit = clamp(
    scaleScore(componentScore(components, SCORE_COMPONENTS.COMMERCIAL_VALUE), 25, 22)
    + scaleScore(componentScore(components, SCORE_COMPONENTS.BUYING_SIGNALS), 20, 8),
    0,
    30
  );
  const website_pain = scaleScore(
    componentScore(components, SCORE_COMPONENTS.WEBSITE_DEFICIENCY),
    25,
    30
  );
  const proof_gap = computeProofGap(ctx.lead, components, ctx.websiteIssues, ctx.businessStrengthSignals);
  const outreach_angle_quality = scoreOutreachAngleQuality(ctx.recommendedOutreachAngle);
  const contactability = scaleScore(
    componentScore(components, SCORE_COMPONENTS.CONTACTABILITY),
    15,
    10
  );

  const total = business_value_fit + website_pain + proof_gap + outreach_angle_quality + contactability;
  return {
    studio_fit_score: clamp(total, 0, 100),
    score_breakdown: {
      business_value_fit,
      website_pain,
      proof_gap,
      outreach_angle_quality,
      contactability,
    },
  };
}

function buildContactPath(lead) {
  const parts = [];
  if (lead.email && lead.email.includes('@')) parts.push(`email:${lead.email}`);
  if (lead.phone) parts.push(`phone:${lead.phone}`);
  if (lead.url) parts.push(`web:${lead.url}`);
  if (lead.linkedin_url) parts.push(`linkedin:${lead.linkedin_url}`);
  return parts.join(' | ') || 'unknown';
}

function buildStudioProspectIntelligence(input) {
  const lead = input.lead || {};
  const assessmentPayload = input.assessment?.assessment
    ? input.assessment
    : (input.assessment || {});
  const payload = assessmentPayload.assessment ? assessmentPayload : { assessment: assessmentPayload };
  const inner = payload.assessment || payload;
  const components = input.assessment?.score_components
    || payload.score_components
    || inner.opportunity_score?.components
    || {};
  const scoringConfidence = input.assessment?.confidence
    ?? payload.confidence
    ?? inner.opportunity_score?.confidence
    ?? 0.45;

  const verticalKey = normalizeVerticalKey(input.vertical || lead.vertical || lead.industry);
  const studioCategory = mapIndustryToStudioCategory(verticalKey, lead.company, lead.snippet);
  const websiteIssues = extractWebsiteIssues(payload);
  const strengths = businessStrengthSignals(lead, payload);
  const companyName = lead.company || inner.prospect?.business_name || '';
  const recommendedOutreachAngle = buildRecommendedOutreachAngle({
    companyName,
    businessStrengthSignals: strengths,
    websiteIssues,
    studioCategory,
  });

  const ctx = {
    lead,
    assessmentPayload: payload,
    components,
    diagnosisClass: payload.commercial_diagnosis?.diagnosis_class
      || inner.commercial_diagnosis?.diagnosis_class
      || input.diagnosisClass,
    recommendedAction: input.assessment?.recommended_action || payload.recommended_action,
    studioCategory,
    verticalKey,
    websiteIssues,
    businessStrengthSignals: strengths,
    recommendedOutreachAngle,
  };

  const qualification = countQualificationSignals(ctx);
  const hardReject = hardRejectReason(ctx);
  const scoring = computeStudioFitScore(ctx);

  let rejectReason = hardReject || '';
  if (!rejectReason && qualification.count < 3) {
    rejectReason = `Fewer than 3 of 5 qualification signals (${qualification.count}/5)`;
  }
  if (!input.rawDiscovery && !rejectReason && scoring.studio_fit_score < STUDIO_MIN_FIT_SCORE) {
    rejectReason = `Studio Fit Score below minimum (${scoring.studio_fit_score} < ${STUDIO_MIN_FIT_SCORE})`;
  }
  if (!rejectReason && isGenericOutreachAngle(recommendedOutreachAngle)) {
    rejectReason = 'Outreach angle too generic';
  }

  const accepted = !rejectReason;

  const intelligence = {
    company_name: companyName,
    website_url: lead.url || inner.prospect?.domain || '',
    location: lead.address || inner.prospect?.location || input.location || '',
    industry: input.vertical || lead.vertical || lead.industry || '',
    studio_category: studioCategory,
    decision_maker_name: lead.contact && lead.contact !== '—' ? lead.contact : '',
    decision_maker_role: lead.job_title || '',
    contact_path: buildContactPath(lead),
    studio_fit_score: scoring.studio_fit_score,
    score_breakdown: scoring.score_breakdown,
    why_they_fit: strengths.length
      ? `Established ${studioCategory.replace(/_/g, ' ')} operator with ${strengths.join('; ')}.`
      : `Category fit for Studio Substral (${studioCategory.replace(/_/g, ' ')}) with discovery evidence on file.`,
    website_issues_observed: websiteIssues,
    business_strength_signals: strengths,
    conversion_or_trust_risk: websiteIssues.length
      ? 'Homepage and site signals may slow trust before a buyer calls, requests a quote, or books.'
      : 'Digital presentation may undersell operational credibility for a trust-sensitive service.',
    recommended_outreach_angle: recommendedOutreachAngle,
    first_message_notes: '',
    confidence: mapConfidence(scoringConfidence, qualification.count),
    reject_reason: rejectReason,
    qualification_signals: qualification.signals,
    accepted: Boolean(accepted && !rejectReason),
  };
  intelligence.first_message_notes = buildFirstMessageNotes(intelligence);
  return intelligence;
}

function evaluateStudioSubstralScoutProspect(input) {
  const intelligence = buildStudioProspectIntelligence(input);
  const outreachStatus = intelligence.accepted
    ? (intelligence.studio_fit_score >= STUDIO_PRIORITY_THRESHOLD ? 'review_needed' : 'new')
    : 'not_fit';
  return { intelligence, outreachStatus };
}

function studioOutreachStatusForScore(score, accepted) {
  if (!accepted) return 'not_fit';
  if (score >= STUDIO_PRIORITY_THRESHOLD) return 'review_needed';
  return 'new';
}

module.exports = {
  STUDIO_MIN_FIT_SCORE,
  STUDIO_PRIORITY_THRESHOLD,
  STUDIO_SCOUT_INSTRUCTION,
  STUDIO_SUBSTRAL_SCOUT_PLAN,
  STUDIO_OUTREACH_STATUS,
  STUDIO_CATEGORY,
  mapIndustryToStudioCategory,
  isGenericOutreachAngle,
  countQualificationSignals,
  computeStudioFitScore,
  buildStudioProspectIntelligence,
  evaluateStudioSubstralScoutProspect,
  studioOutreachStatusForScore,
};
