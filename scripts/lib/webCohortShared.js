'use strict';

const { topFindings } = require('../../packages/capabilities/websiteOpportunityIntelligence');
const { EVIDENCE_CLASS, RECOMMENDED_ACTIONS } = require('../../packages/capabilities/websiteOpportunityIntelligence/types');
const { prioritizeWebOpportunities } = require('../../utils/webOpportunityMaxPrioritization');

function formatCohortRow(business, assessment, discovery = null) {
  const components = assessment.score_components || {};
  const findings = collectAllFindings(assessment);
  const top3 = topFindings(findings, 3).map((f) => ({
    summary: f.summary,
    evidence_class: f.evidence_class,
    ref: f.ref || f.id,
  }));
  const why = assessment.assessment?.acquisition_recommendation?.why || '';

  return {
    business: business.company || business.business_name || assessment.domain,
    domain: assessment.domain,
    industry: business.industry || business.vertical || null,
    location: business.location || business.address || null,
    website_deficiency: components.website_deficiency?.score ?? null,
    commercial_value: components.commercial_value?.score ?? null,
    buying_signals: components.buying_signals?.score ?? null,
    contactability: components.contactability?.score ?? null,
    project_economics: components.project_economics?.score ?? null,
    total_opportunity_score: assessment.opportunity_score,
    confidence: assessment.confidence,
    estimated_project_range: assessment.economics?.estimated_project_range,
    estimated_operator_hours: assessment.economics?.estimated_operator_hours,
    estimated_contribution: assessment.economics?.estimated_contribution,
    top_3_verified_findings: top3.map((f) => f.summary),
    recommended_action: assessment.recommended_action,
    why,
    discovery,
    raw_payload: assessment,
  };
}

function collectAllFindings(assessment) {
  const fromAssessment = assessment.assessment?.verified_findings || [];
  const fromRefs = assessment.evidence_refs || [];
  const fromTechnical = [];
  const tech = assessment.technical_evidence || {};
  for (const group of Object.values(tech)) {
    if (Array.isArray(group)) fromTechnical.push(...group);
  }
  const seen = new Set();
  const out = [];
  for (const f of [...fromAssessment, ...fromRefs, ...fromTechnical]) {
    const key = f.ref || f.id || f.summary;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(f);
  }
  return out;
}

function classifyFindings(assessment) {
  const findings = collectAllFindings(assessment);
  return {
    measured: findings.filter((f) => f.evidence_class === EVIDENCE_CLASS.MEASURED),
    observed: findings.filter((f) => f.evidence_class === EVIDENCE_CLASS.OBSERVED),
    inferred: findings.filter((f) => f.evidence_class === EVIDENCE_CLASS.INFERRED),
    unknown: findings.filter((f) => f.evidence_class === EVIDENCE_CLASS.UNKNOWN),
  };
}

function attachMaxPriority(rows) {
  const ranked = prioritizeWebOpportunities(
    rows.map((r) => ({
      ...r,
      business_name: r.business,
      payload: r.raw_payload,
    }))
  );
  const byDomain = new Map(ranked.map((r) => [r.domain, r]));
  return rows.map((row) => {
    const rankedRow = byDomain.get(row.domain) || {};
    return {
      ...row,
      max_priority: rankedRow.max_priority_score ?? null,
      max_rationale: buildMaxRationale(rankedRow),
      prioritization_factors: rankedRow.prioritization_factors || null,
    };
  });
}

function buildMaxRationale(rankedRow) {
  if (!rankedRow?.prioritization_factors) return null;
  const f = rankedRow.prioritization_factors;
  return [
    `priority=${rankedRow.max_priority_score}`,
    `opportunity_score=${f.opportunity_score}`,
    `confidence=${f.confidence}`,
    `contribution=${f.estimated_contribution}`,
    `action=${f.recommended_action}`,
    `measured_evidence=${f.measured_evidence_count}`,
    `operator_hours=${f.operator_hours}`,
  ].join('; ');
}

function countDistribution(rows) {
  const out = {};
  for (const action of Object.values(RECOMMENDED_ACTIONS)) out[action] = 0;
  for (const row of rows) {
    out[row.recommended_action] = (out[row.recommended_action] || 0) + 1;
  }
  return out;
}

function identifyFalsePositives(rows) {
  const review = [];
  for (const row of rows) {
    if (row.website_deficiency >= 15 && row.project_economics <= 3) {
      review.push({
        business: row.business,
        domain: row.domain,
        case: 'bad_site_weak_economics',
        detail: row.why,
        live_contradiction: row.max_priority != null && row.total_opportunity_score > row.max_priority
          ? 'High raw score deprioritized by Max due to economics/contactability'
          : null,
      });
    }
    if (row.website_deficiency >= 18 && row.commercial_value <= 8) {
      review.push({
        business: row.business,
        domain: row.domain,
        case: 'technical_poor_redesign_not_warranted',
        detail: row.why,
      });
    }
    if (row.commercial_value >= 15 && row.website_deficiency <= 6) {
      review.push({
        business: row.business,
        domain: row.domain,
        case: 'attractive_business_adequate_site',
        detail: row.why,
      });
    }
    if (row.confidence <= 0.35) {
      review.push({
        business: row.business,
        domain: row.domain,
        case: 'insufficient_evidence',
        detail: row.why,
      });
    }
    if (row.buying_signals >= 8) {
      const classified = classifyFindings(row.raw_payload);
      const buyingObserved = classified.observed.some((f) => /hiring|growth|advertising/i.test(f.summary));
      if (!buyingObserved) {
        review.push({
          business: row.business,
          domain: row.domain,
          case: 'buying_signals_unsupported_by_live_evidence',
          detail: 'Buying signal score present without matching OBSERVED scout/audit evidence',
        });
      }
    }
    if (row.total_opportunity_score >= 60 && row.max_priority != null && row.max_priority < 50) {
      review.push({
        business: row.business,
        domain: row.domain,
        case: 'high_raw_score_max_deprioritized',
        detail: row.max_rationale,
      });
    }
  }
  return review;
}

function buildTopFiveDetail(rows) {
  const ranked = [...rows].sort((a, b) => (b.max_priority || 0) - (a.max_priority || 0)).slice(0, 5);
  return ranked.map((row) => {
    const classified = classifyFindings(row.raw_payload);
    const diagnosis = row.raw_payload.commercial_diagnosis || {};
    return {
      business: {
        name: row.business,
        url: row.domain ? `https://${row.domain}` : null,
        industry: row.industry,
        location: row.location,
      },
      evidence: {
        measured: classified.measured.map((f) => f.summary),
        observed: classified.observed.map((f) => f.summary),
        inferred: (diagnosis.bounded_inferences || classified.inferred).map((f) =>
          typeof f === 'string' ? f : f.summary
        ),
        unknown: classified.unknown.map((f) => f.summary),
      },
      diagnosis_class: diagnosis.diagnosis_class || null,
      economics: {
        default_planning: row.raw_payload.economics?.default_planning_economics,
        prospect_specific: row.raw_payload.economics?.prospect_specific_economics,
        economic_confidence: row.raw_payload.economics?.economic_confidence,
        estimated_project_range: row.estimated_project_range,
        estimated_operator_hours: row.estimated_operator_hours,
        estimated_contribution: row.estimated_contribution,
        label: row.raw_payload.economics?.label || 'estimate',
      },
      prioritization: {
        component_scores: row.raw_payload.score_components,
        confidence: row.confidence,
        max_priority: row.max_priority,
        why_max_prioritizes: row.max_rationale,
        downgrade_triggers: diagnosis.evidence_that_would_change_conclusion || [],
        recommended_action: row.recommended_action,
      },
    };
  });
}

function compareCohorts(fixtureReport, liveReport) {
  const assumptions = [];
  if (fixtureReport && liveReport) {
    assumptions.push({
      assumption: 'Fixture PSI scores predict live mobile performance severity',
      cohort_001: 'Fixed perf scores drive deficiency component',
      cohort_002: `Live PSI/measured count avg ${averageMeasured(liveReport)} vs fixture avg ${averageMeasured(fixtureReport)}`,
      survived: Math.abs(averageMeasured(liveReport) - averageMeasured(fixtureReport)) <= 2,
    });
    assumptions.push({
      assumption: 'HIGH_VALUE distribution reflects real commercial opportunities',
      cohort_001: fixtureReport.distribution,
      cohort_002: liveReport.distribution,
      survived: fixtureReport.distribution?.HIGH_VALUE_WEBSITE_OPPORTUNITY !== liveReport.distribution?.HIGH_VALUE_WEBSITE_OPPORTUNITY,
    });
    assumptions.push({
      assumption: 'Contactability from fixture emails/phones matches live Scout discovery',
      cohort_001: 'All fixtures had phone or email',
      cohort_002: `${liveReport.rows?.filter((r) => r.contactability >= 10).length || 0}/25 with strong contactability`,
      survived: null,
    });
    assumptions.push({
      assumption: 'Buying signals (hiring/growth) appear in live discovery',
      cohort_001: 'Fixtures injected hiring_signal flags',
      cohort_002: `${liveReport.rows?.filter((r) => r.buying_signals >= 6).length || 0}/25 scored buying signals without injected flags`,
      survived: null,
    });
  }
  return assumptions;
}

function averageMeasured(report) {
  const rows = report.rows || [];
  if (!rows.length) return 0;
  let total = 0;
  for (const row of rows) {
    total += classifyFindings(row.raw_payload).measured.length;
  }
  return Math.round((total / rows.length) * 10) / 10;
}

module.exports = {
  formatCohortRow,
  collectAllFindings,
  classifyFindings,
  attachMaxPriority,
  buildMaxRationale,
  countDistribution,
  identifyFalsePositives,
  buildTopFiveDetail,
  compareCohorts,
};
