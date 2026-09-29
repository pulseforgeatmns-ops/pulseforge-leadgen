'use strict';

const {
  SCORE_COMPONENTS,
  SCORE_MAX,
  RECOMMENDED_ACTIONS,
  EVIDENCE_CLASS,
  BUYING_SIGNAL_RESEARCH,
  ECONOMIC_CONFIDENCE,
} = require('./types');

const HIGH_VALUE_VERTICALS = new Set([
  'legal', 'law_firm', 'accounting', 'dental', 'med_spa', 'home_services',
  'home_renovation', 'hvac', 'roofing', 'plumbing', 'electrical', 'fitness',
  'restaurant', 'salon', 'property_management', 'architecture_engineering',
]);

/**
 * Deterministic evidence → deficiency score mapping (max 25).
 * Each rule applies at most once; points are additive then clamped.
 */
const DEFICIENCY_SCORE_RULES = Object.freeze([
  {
    id: 'fetch_slow_4s',
    points: 6,
    reason: 'Homepage fetch exceeded 4s during audit',
    match: (f) => f.ref === 'performance:fetch_ms' && (f.measurement?.fetch_ms ?? 0) >= 4000,
  },
  {
    id: 'fetch_slow_2s',
    points: 3,
    reason: 'Homepage fetch between 2s and 4s during audit',
    match: (f) => {
      const ms = f.measurement?.fetch_ms;
      return f.ref === 'performance:fetch_ms' && ms >= 2000 && ms < 4000;
    },
  },
  {
    id: 'psi_perf_low',
    points: 8,
    reason: (f) => `Low measured performance score (${f.measurement?.performance_score})`,
    match: (f) => f.category === 'performance'
      && f.evidence_class === EVIDENCE_CLASS.MEASURED
      && f.measurement?.performance_score != null
      && f.measurement.performance_score < 50,
  },
  {
    id: 'psi_perf_moderate',
    points: 4,
    reason: (f) => `Moderate measured performance score (${f.measurement?.performance_score})`,
    match: (f) => f.category === 'performance'
      && f.evidence_class === EVIDENCE_CLASS.MEASURED
      && f.measurement?.performance_score != null
      && f.measurement.performance_score >= 50
      && f.measurement.performance_score < 70,
  },
  {
    id: 'lcp_high',
    points: 5,
    reason: 'LCP above 4s threshold',
    match: (f) => f.measurement?.metric === 'lcp' && (f.measurement?.numeric_value ?? 0) > 4000,
  },
  {
    id: 'a11y_missing_alt',
    points: (f) => Math.min(4, 2 + Math.floor((f.measurement?.missing_alt_count || 1) / 2)),
    reason: (f) => `${f.measurement?.missing_alt_count || 0} image(s) missing alt text`,
    match: (f) => f.id === 'a11y_missing_alt',
  },
  {
    id: 'a11y_psi_low',
    points: 3,
    reason: (f) => `Low measured accessibility score (${f.measurement?.accessibility_score})`,
    match: (f) => f.measurement?.accessibility_score != null && f.measurement.accessibility_score < 70,
  },
  {
    id: 'a11y_lang',
    points: 1,
    reason: 'HTML element missing lang attribute',
    match: (f) => f.id === 'a11y_missing_lang',
  },
  {
    id: 'tech_no_https',
    points: 4,
    reason: 'Homepage URL is not HTTPS',
    match: (f) => f.id === 'tech_no_https',
  },
  {
    id: 'tech_http_error',
    points: 6,
    reason: (f) => `Homepage HTTP error (${f.measurement?.status_code})`,
    match: (f) => f.id === 'tech_http_error',
  },
  {
    id: 'mobile_no_viewport',
    points: 3,
    reason: 'Missing viewport meta tag',
    match: (f) => f.id === 'mobile_no_viewport',
  },
  {
    id: 'seo_missing_title',
    points: 2,
    reason: 'Missing or empty document title',
    match: (f) => f.id === 'seo_missing_title',
  },
  {
    id: 'seo_robots_missing',
    points: 2,
    reason: (f) => `robots.txt returned HTTP ${f.measurement?.status_code}`,
    match: (f) => f.id === 'seo_robots_missing',
  },
  {
    id: 'seo_sitemap_missing',
    points: 3,
    reason: (f) => `sitemap.xml returned HTTP ${f.measurement?.status_code}`,
    match: (f) => f.id === 'seo_sitemap_missing',
  },
  {
    id: 'conv_no_path',
    points: 4,
    reason: 'No obvious conversion path detected on homepage',
    match: (f) => f.id === 'conv_no_obvious_path',
  },
  {
    id: 'dom_no_contact_nav',
    points: 2,
    reason: 'Primary navigation contains no Contact link',
    match: (f) => f.id === 'dom_no_contact_nav',
  },
  {
    id: 'fetch_failed',
    points: 5,
    reason: 'Homepage fetch failed during audit',
    match: (f) => f.id === 'fetch_failed',
  },
]);

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function resolveRulePoints(rule, finding) {
  const pts = typeof rule.points === 'function' ? rule.points(finding) : rule.points;
  const reason = typeof rule.reason === 'function' ? rule.reason(finding) : rule.reason;
  return { points: pts, reason };
}

function scoreWebsiteDeficiency(findings) {
  const max = SCORE_MAX[SCORE_COMPONENTS.WEBSITE_DEFICIENCY];
  let score = 0;
  const reasons = [];
  const applied = new Set();

  for (const rule of DEFICIENCY_SCORE_RULES) {
    if (applied.has(rule.id)) continue;
    const hit = findings.find((f) => rule.match(f));
    if (!hit) continue;
    const { points, reason } = resolveRulePoints(rule, hit);
    if (points <= 0) continue;
    score += points;
    reasons.push(reason);
    applied.add(rule.id);
  }

  if (score === 0 && findings.some((f) =>
    ['performance', 'accessibility', 'technical_health', 'conversion_structure'].includes(f.category)
  )) {
    reasons.push('Limited material deficiencies detected');
  }

  return {
    component: SCORE_COMPONENTS.WEBSITE_DEFICIENCY,
    score: clamp(score, 0, max),
    max,
    reasons,
    mapping: 'DEFICIENCY_SCORE_RULES',
  };
}

function scoreCommercialValue(business) {
  const max = SCORE_MAX[SCORE_COMPONENTS.COMMERCIAL_VALUE];
  let score = 8;
  const reasons = ['Baseline SMB operating business assumption'];

  const industry = String(business.industry || business.vertical || '').toLowerCase();
  if ([...HIGH_VALUE_VERTICALS].some((v) => industry.includes(v.replace('_', ' ')) || industry.includes(v))) {
    score += 10;
    reasons.push('Industry where website credibility/acquisition plausibly matters');
  } else if (industry) {
    score += 4;
    reasons.push('Industry present but not in high-value web-influence set');
  }

  if (business.google_review_count >= 20 || business.google_rating >= 4) {
    score += 4;
    reasons.push('Established local reputation signals');
  }

  if (business.multi_location) {
    score += 3;
    reasons.push('Multi-location business');
  }

  return {
    component: SCORE_COMPONENTS.COMMERCIAL_VALUE,
    score: clamp(score, 0, max),
    max,
    reasons,
  };
}

function scoreBuyingSignals(business, findings) {
  const max = SCORE_MAX[SCORE_COMPONENTS.BUYING_SIGNALS];
  const research = business.buying_signal_research || BUYING_SIGNAL_RESEARCH.NOT_RESEARCHED;

  if (research === BUYING_SIGNAL_RESEARCH.NOT_RESEARCHED) {
    return {
      component: SCORE_COMPONENTS.BUYING_SIGNALS,
      score: 0,
      max,
      reasons: ['Buying signal research not performed — UNKNOWN'],
      unknown: true,
    };
  }

  let score = 0;
  const reasons = [];

  const positiveFindings = findings.filter((f) =>
    f.category === 'business'
    && f.evidence_class === EVIDENCE_CLASS.OBSERVED
    && /hiring|growth|advertising|expansion|funding|marketing activity/i.test(f.summary)
  );

  if (business.hiring_signal) {
    score += 8;
    reasons.push('Active hiring signal observed');
  }
  if (business.recent_growth_signal) {
    score += 6;
    reasons.push('Recent growth/expansion signal observed');
  }
  if (business.advertising_signal) {
    score += 4;
    reasons.push('Observable advertising/acquisition activity');
  }

  for (const pf of positiveFindings) {
    if (!reasons.some((r) => r.includes(pf.summary.slice(0, 20)))) {
      score += 3;
      reasons.push(`Observed: ${pf.summary}`);
    }
  }

  const recentRedesign = findings.some((f) => /recent redesign|new website/i.test(f.summary));
  if (recentRedesign) {
    score -= 5;
    reasons.push('Recent redesign evidence reduces immediate need');
  }

  if (score === 0) {
    reasons.push('Buying signal research performed — no positive signals observed');
  }

  return {
    component: SCORE_COMPONENTS.BUYING_SIGNALS,
    score: clamp(score, 0, max),
    max,
    reasons,
    unknown: false,
  };
}

function scoreContactability(business, findings = []) {
  const max = SCORE_MAX[SCORE_COMPONENTS.CONTACTABILITY];
  let score = 0;
  const reasons = [];

  if (business.email && business.email !== '—') {
    score += 5;
    reasons.push('Business email identified');
  }
  if (business.phone) {
    score += 4;
    reasons.push('Business phone identified');
  }
  if (business.contact_name && business.contact_name !== '—') {
    score += 3;
    reasons.push('Named decision-maker contact identified');
  }

  const hasContactPage = findings.some((f) =>
    f.id === 'conv_contact_nav' || /contact link present/i.test(f.summary)
  );
  if (hasContactPage) {
    score += 2;
    reasons.push('Contact page/link observed on homepage');
  }

  const hasMailto = findings.some((f) => f.id === 'conv_email_link');
  if (hasMailto) {
    score += 1;
    reasons.push('Public mailto contact route observed');
  }

  const hasForm = findings.some((f) => f.id === 'conv_form');
  if (hasForm) {
    score += 1;
    reasons.push('Contact/inquiry form observed on homepage');
  }

  if (score === 0) reasons.push('Contactability insufficient');

  return {
    component: SCORE_COMPONENTS.CONTACTABILITY,
    score: clamp(score, 0, max),
    max,
    reasons,
  };
}

function scoreProjectEconomics(economics) {
  const max = SCORE_MAX[SCORE_COMPONENTS.PROJECT_ECONOMICS];
  const confidence = economics.economic_confidence || ECONOMIC_CONFIDENCE.UNKNOWN;

  if (confidence === ECONOMIC_CONFIDENCE.LOW || confidence === ECONOMIC_CONFIDENCE.UNKNOWN) {
    return {
      component: SCORE_COMPONENTS.PROJECT_ECONOMICS,
      score: 0,
      max,
      reasons: [`Prospect-specific economics ${confidence} — default planning only, no differentiation`],
      uses_default_only: true,
    };
  }

  let score = 0;
  const reasons = [];
  const contribution = economics.prospect_specific_economics?.estimated_contribution
    ?? economics.estimated_contribution;
  const hours = economics.prospect_specific_economics?.estimated_operator_hours
    ?? economics.estimated_operator_hours;
  const contract = economics.prospect_specific_economics?.estimated_contract_value
    ?? economics.estimated_contract_value;

  if (contribution == null) {
    return { component: SCORE_COMPONENTS.PROJECT_ECONOMICS, score: 0, max, reasons: ['Economics unavailable'] };
  }

  if (contract >= 2500 && contribution >= 1000) {
    score += 10;
    reasons.push('Prospect-specific contribution supports operator economics at floor');
  } else if (contract >= 2500) {
    score += 5;
    reasons.push('Contract at floor but prospect-specific contribution is thin');
  }

  if (hours != null && hours <= 15) {
    score += 5;
    reasons.push('Prospect-specific hours leave capacity headroom');
  } else if (hours != null && hours >= 30) {
    score -= 8;
    reasons.push('Prospect-specific hours consume most of 30-day capacity at floor');
  }

  return {
    component: SCORE_COMPONENTS.PROJECT_ECONOMICS,
    score: clamp(score, 0, max),
    max,
    reasons,
    uses_default_only: false,
  };
}

function computeOpportunityScore({ findings, business, economics, audit }) {
  const components = [
    scoreWebsiteDeficiency(findings),
    scoreCommercialValue(business),
    scoreBuyingSignals(business, findings),
    scoreContactability(business, findings),
    scoreProjectEconomics(economics),
  ];

  const total = components.reduce((s, c) => s + c.score, 0);
  const deficiencyOnly = components[0].score >= 18 && total < 45;

  const scoreBreakdown = {};
  for (const c of components) scoreBreakdown[c.component] = c;

  let confidence = 0.55;
  const measured = findings.filter((f) => f.evidence_class === EVIDENCE_CLASS.MEASURED).length;
  confidence += Math.min(0.25, measured * 0.03);
  if (!business.industry) confidence -= 0.1;
  if (!business.email && !business.phone) confidence -= 0.15;
  if (components[2].unknown) confidence -= 0.05;
  confidence = clamp(confidence, 0.2, 0.95);

  return {
    opportunity_score: clamp(total, 0, 100),
    score_components: scoreBreakdown,
    confidence: Math.round(confidence * 100) / 100,
    deficiency_only_risk: deficiencyOnly,
  };
}

function recommendAction({ opportunity_score, score_components, confidence, deficiency_only_risk, economics, diagnosis_class }) {
  const deficiency = score_components[SCORE_COMPONENTS.WEBSITE_DEFICIENCY]?.score || 0;
  const commercial = score_components[SCORE_COMPONENTS.COMMERCIAL_VALUE]?.score || 0;
  const economicsScore = score_components[SCORE_COMPONENTS.PROJECT_ECONOMICS]?.score || 0;
  const contact = score_components[SCORE_COMPONENTS.CONTACTABILITY]?.score || 0;

  if (diagnosis_class === 'HEALTHY_SITE') {
    return {
      action: RECOMMENDED_ACTIONS.MONITOR,
      why: 'Deterministic diagnosis: healthy site — no meaningful redesign case',
    };
  }

  if (diagnosis_class === 'INSUFFICIENT_EVIDENCE') {
    return {
      action: RECOMMENDED_ACTIONS.AUDIT_WORTH_REVIEWING,
      why: 'Insufficient evidence for responsible diagnosis — gather more measurements',
    };
  }

  if (contact < 5) {
    return {
      action: RECOMMENDED_ACTIONS.DO_NOT_PURSUE,
      why: 'Insufficient contactability for acquisition',
    };
  }

  if (deficiency_only_risk || (deficiency >= 20 && commercial < 10 && economicsScore < 5)) {
    return {
      action: RECOMMENDED_ACTIONS.DO_NOT_PURSUE,
      why: 'Website deficiencies present but commercial/economic case is weak',
    };
  }

  if (deficiency < 8 && commercial >= 12) {
    return {
      action: RECOMMENDED_ACTIONS.MONITOR,
      why: 'Attractive business but website appears adequate for now',
    };
  }

  if (diagnosis_class === 'TARGETED_REMEDIATION' && deficiency < 15) {
    return {
      action: RECOMMENDED_ACTIONS.AUDIT_WORTH_REVIEWING,
      why: 'Specific improvements warranted but broad redesign unsupported by evidence',
    };
  }

  if (opportunity_score >= 70 && confidence >= 0.55 && diagnosis_class === 'REDESIGN_CANDIDATE') {
    const contribution = economics?.prospect_specific_economics?.estimated_contribution
      ?? economics?.default_planning_economics?.estimated_contribution;
    if (contribution >= 1000 || economics?.economic_confidence === ECONOMIC_CONFIDENCE.HIGH) {
      return {
        action: RECOMMENDED_ACTIONS.HIGH_VALUE_WEBSITE_OPPORTUNITY,
        why: 'Strong combined website deficiency, business, contactability, and diagnosis case',
      };
    }
  }

  if (opportunity_score >= 45) {
    return {
      action: RECOMMENDED_ACTIONS.AUDIT_WORTH_REVIEWING,
      why: 'Mixed but reviewable opportunity — operator judgment required',
    };
  }

  return {
    action: RECOMMENDED_ACTIONS.DO_NOT_PURSUE,
    why: 'Overall opportunity score below review threshold',
  };
}

module.exports = {
  DEFICIENCY_SCORE_RULES,
  computeOpportunityScore,
  recommendAction,
  scoreWebsiteDeficiency,
  scoreCommercialValue,
  scoreBuyingSignals,
  scoreContactability,
  scoreProjectEconomics,
};
