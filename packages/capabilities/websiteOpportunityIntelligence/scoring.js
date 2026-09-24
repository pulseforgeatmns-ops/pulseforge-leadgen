'use strict';

const {
  SCORE_COMPONENTS,
  SCORE_MAX,
  RECOMMENDED_ACTIONS,
} = require('./types');
const { EVIDENCE_CLASS } = require('./types');

const HIGH_VALUE_VERTICALS = new Set([
  'legal', 'law_firm', 'accounting', 'dental', 'med_spa', 'home_services',
  'home_renovation', 'hvac', 'roofing', 'plumbing', 'electrical', 'fitness',
  'restaurant', 'salon', 'property_management', 'architecture_engineering',
]);

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function scoreWebsiteDeficiency(findings, audit) {
  const max = SCORE_MAX[SCORE_COMPONENTS.WEBSITE_DEFICIENCY];
  let score = 0;
  const reasons = [];

  const perf = findings.filter((f) => f.category === 'performance' && f.evidence_class === EVIDENCE_CLASS.MEASURED);
  for (const p of perf) {
    const ps = p.measurement?.performance_score;
    if (ps != null && ps < 50) {
      score += 8;
      reasons.push(`Low measured performance score (${ps})`);
    } else if (ps != null && ps < 70) {
      score += 4;
      reasons.push(`Moderate measured performance score (${ps})`);
    }
    const lcp = p.measurement?.metric === 'lcp' ? p.measurement.numeric_value : null;
    if (lcp != null && lcp > 4000) {
      score += 5;
      reasons.push('LCP above 4s threshold');
    }
  }

  const a11y = findings.filter((f) => f.category === 'accessibility');
  score += Math.min(6, a11y.length * 2);
  if (a11y.length) reasons.push(`${a11y.length} accessibility finding(s)`);

  const tech = findings.filter((f) => f.category === 'technical_health');
  for (const t of tech) {
    if (/no_https|missing viewport|missing title|http_error/i.test(t.id || t.summary)) {
      score += 2;
      reasons.push(t.summary);
    }
  }

  const conv = findings.filter((f) => f.id === 'conv_no_obvious_path');
  if (conv.length) {
    score += 4;
    reasons.push('No obvious conversion path detected');
  }

  if (score === 0 && findings.length > 0) {
    reasons.push('Limited material deficiencies detected');
  }

  return {
    component: SCORE_COMPONENTS.WEBSITE_DEFICIENCY,
    score: clamp(score, 0, max),
    max,
    reasons,
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
  let score = 0;
  const reasons = [];

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

  const recentRedesign = findings.some((f) => /recent redesign|new website/i.test(f.summary));
  if (recentRedesign) {
    score -= 5;
    reasons.push('Recent redesign evidence reduces immediate need');
  }

  if (score === 0) reasons.push('No strong buying signals detected');

  return {
    component: SCORE_COMPONENTS.BUYING_SIGNALS,
    score: clamp(score, 0, max),
    max,
    reasons,
  };
}

function scoreContactability(business) {
  const max = SCORE_MAX[SCORE_COMPONENTS.CONTACTABILITY];
  let score = 0;
  const reasons = [];

  if (business.email && business.email !== '—') {
    score += 6;
    reasons.push('Business email identified');
  }
  if (business.phone) {
    score += 5;
    reasons.push('Business phone identified');
  }
  if (business.contact_name && business.contact_name !== '—') {
    score += 4;
    reasons.push('Named contact identified');
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
  let score = 0;
  const reasons = [];

  const contribution = economics.estimated_contribution;
  const hours = economics.estimated_operator_hours;
  const contract = economics.estimated_contract_value;

  if (contribution == null) {
    return { component: SCORE_COMPONENTS.PROJECT_ECONOMICS, score: 0, max, reasons: ['Economics unavailable'] };
  }

  if (contract >= 2500 && contribution >= 1000) {
    score += 10;
    reasons.push('Estimated contribution supports operator economics at floor');
  } else if (contract >= 2500) {
    score += 5;
    reasons.push('Contract at floor but contribution is thin');
  }

  if (hours != null && hours <= 15) {
    score += 5;
    reasons.push('Estimated hours leave capacity headroom');
  } else if (hours != null && hours >= 30) {
    score -= 8;
    reasons.push('Estimated hours consume most of 30-day capacity at floor');
  }

  return {
    component: SCORE_COMPONENTS.PROJECT_ECONOMICS,
    score: clamp(score, 0, max),
    max,
    reasons,
  };
}

function computeOpportunityScore({ findings, business, economics, audit }) {
  const components = [
    scoreWebsiteDeficiency(findings, audit),
    scoreCommercialValue(business),
    scoreBuyingSignals(business, findings),
    scoreContactability(business),
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
  confidence = clamp(confidence, 0.2, 0.95);

  return {
    opportunity_score: clamp(total, 0, 100),
    score_components: scoreBreakdown,
    confidence: Math.round(confidence * 100) / 100,
    deficiency_only_risk: deficiencyOnly,
  };
}

function recommendAction({ opportunity_score, score_components, confidence, deficiency_only_risk, economics }) {
  const deficiency = score_components[SCORE_COMPONENTS.WEBSITE_DEFICIENCY]?.score || 0;
  const commercial = score_components[SCORE_COMPONENTS.COMMERCIAL_VALUE]?.score || 0;
  const economicsScore = score_components[SCORE_COMPONENTS.PROJECT_ECONOMICS]?.score || 0;
  const contact = score_components[SCORE_COMPONENTS.CONTACTABILITY]?.score || 0;

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

  if (opportunity_score >= 70 && economics?.estimated_contribution >= 1000 && confidence >= 0.55) {
    return {
      action: RECOMMENDED_ACTIONS.HIGH_VALUE_WEBSITE_OPPORTUNITY,
      why: 'Strong combined website, business, contactability, and economics case',
    };
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
  computeOpportunityScore,
  recommendAction,
  scoreWebsiteDeficiency,
  scoreCommercialValue,
  scoreBuyingSignals,
  scoreContactability,
  scoreProjectEconomics,
};
