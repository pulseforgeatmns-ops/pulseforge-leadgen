'use strict';

const {
  COHORT_TAG,
  COHORT_SIZE,
  FIXTURE_BUSINESSES,
  createFixtureAuditProvider,
} = require('./webCohort001Fixtures');
const { assessDiscoveredBusiness } = require('../../services/webDesignScout');
const { ensureMaynardWebMission } = require('../../utils/maynardWebTenant');
const { prioritizeWebOpportunities } = require('../../utils/webOpportunityMaxPrioritization');
const { topFindings } = require('../../packages/capabilities/websiteOpportunityIntelligence');
const { RECOMMENDED_ACTIONS, WEB_EVENT_TYPES } = require('../../packages/capabilities/websiteOpportunityIntelligence/types');
const { emitWebEvent } = require('../../packages/capabilities/websiteOpportunityIntelligence/observability');
const { insertWebsiteOpportunityEvent } = require('../../services/websiteOpportunityPersistence');

async function runWebCohort001(pool, { dryRun = true, fixtureMode = true } = {}) {
  let client = { id: 0, slug: 'maynard-web' };
  let missionId = 'mission-web-cohort-fixture';
  if (pool) {
    const ensured = await ensureMaynardWebMission(pool);
    client = ensured.client;
    missionId = ensured.missionId;
  }
  const fixtureAudit = fixtureMode ? createFixtureAuditProvider() : null;
  const results = [];

  for (const business of FIXTURE_BUSINESSES.slice(0, COHORT_SIZE)) {
    const input = {
      ...business,
      client_id: client.id,
      mission_id: missionId,
      cohort_tag: COHORT_TAG,
      skipPuppeteer: true,
      skipPageSpeed: true,
    };

    if (dryRun) {
      const { assessWebsiteOpportunity } = require('../../packages/capabilities/websiteOpportunityIntelligence');
      const assessment = await assessWebsiteOpportunity(input, {
        fixtureAudit,
        skipPuppeteer: true,
        onEvent: async (event) => {
          if (pool) await insertWebsiteOpportunityEvent(pool, event);
        },
      });
      results.push({ business, assessment, dryRun: true });
    } else {
      const out = await assessDiscoveredBusiness(input, { pool, fixtureAudit, skipPuppeteer: true });
      results.push({ business, assessment: out.assessment, dryRun: false });
    }
  }

  const rows = results.map(({ business, assessment }) => formatCohortRow(business, assessment));
  const distribution = countDistribution(rows);
  const ranked = prioritizeWebOpportunities(rows.map((r) => ({ ...r, payload: r.raw_payload })));
  const topFive = ranked.slice(0, 5).map((r) => ({
    business: r.business,
    domain: r.domain,
    priority_score: r.max_priority_score,
    recommended_action: r.recommended_action,
    why: r.why,
    top_findings: r.top_findings,
  }));
  const falsePositives = identifyFalsePositives(rows);

  if (pool) {
    await emitWebEvent(WEB_EVENT_TYPES.COHORT_COMPLETED, {
      tenant_id: client.id,
      mission_id: missionId,
      payload: {
        cohort_tag: COHORT_TAG,
        size: rows.length,
        distribution,
        dry_run: dryRun,
        fixture_mode: fixtureMode,
      },
    }, { pool });
  }

  return {
    cohort_tag: COHORT_TAG,
    client_id: client.id,
    mission_id: missionId,
    dry_run: dryRun,
    fixture_mode: fixtureMode,
    rows,
    distribution,
    top_five: topFive,
    false_positive_review: falsePositives,
    stats: {
      total: rows.length,
      no_outreach: true,
    },
  };
}

function formatCohortRow(business, assessment) {
  const components = assessment.score_components || {};
  const findings = [
    ...(assessment.assessment?.verified_findings || []),
    ...(assessment.evidence_refs || []),
  ];
  const top3 = topFindings(findings, 3).map((f) => f.summary);
  const why = assessment.assessment?.acquisition_recommendation?.why || '';

  return {
    business: business.company,
    domain: assessment.domain,
    industry: business.industry,
    location: business.location,
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
    top_3_verified_findings: top3,
    recommended_action: assessment.recommended_action,
    why,
    raw_payload: assessment,
  };
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
      review.push({ business: row.business, case: 'bad_site_weak_economics', detail: row.why });
    }
    if (row.website_deficiency >= 18 && row.commercial_value <= 8) {
      review.push({ business: row.business, case: 'technical_poor_redesign_not_warranted', detail: row.why });
    }
    if (row.commercial_value >= 15 && row.website_deficiency <= 6) {
      review.push({ business: row.business, case: 'attractive_business_adequate_site', detail: row.why });
    }
    if (row.confidence <= 0.35) {
      review.push({ business: row.business, case: 'insufficient_evidence', detail: row.why });
    }
  }
  return review;
}

module.exports = {
  COHORT_TAG,
  COHORT_SIZE,
  runWebCohort001,
  formatCohortRow,
  countDistribution,
  identifyFalsePositives,
};
