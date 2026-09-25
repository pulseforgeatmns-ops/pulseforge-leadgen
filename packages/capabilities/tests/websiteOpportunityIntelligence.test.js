'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  createWebsiteOpportunityIntelligenceCapability,
  assessWebsiteOpportunity,
  computeOpportunityScore,
  recommendAction,
  computeProjectEconomics,
  EVIDENCE_CLASS,
  RECOMMENDED_ACTIONS,
  enforceEvidenceIntegrity,
  buildFinding,
} = require('../websiteOpportunityIntelligence');
const { createFixtureAuditProvider, FIXTURE_BUSINESSES } = require('../../../scripts/lib/webCohort001Fixtures');
const { runWebCohort001 } = require('../../../scripts/lib/webCohort001');
const { buildPaigeWebEvidenceContext, sanitizeForPaige } = require('../../../utils/paigeWebEvidenceContext');
const { prioritizeWebOpportunities } = require('../../../utils/webOpportunityMaxPrioritization');
const { BUILTIN_IDS, createBuiltinRegistry } = require('../index');

describe('SPEC-WEB-001 Website Opportunity Intelligence', () => {
  const fixtureAudit = createFixtureAuditProvider();

  it('registers website_opportunity_intelligence capability', () => {
    const registry = createBuiltinRegistry();
    assert.ok(registry.get(BUILTIN_IDS.WEBSITE_OPPORTUNITY_INTELLIGENCE));
  });

  it('preserves MEASURED vs INFERRED evidence integrity', async () => {
    const business = FIXTURE_BUSINESSES[0];
    const result = await assessWebsiteOpportunity({
      domain: business.domain,
      company: business.company,
      industry: business.industry,
      email: business.email,
      phone: business.phone,
    }, { fixtureAudit, skipPuppeteer: true });

    assert.ok(result.evidence_refs.length > 0);
    const measured = result.evidence_refs.filter((e) => e.evidence_class === EVIDENCE_CLASS.MEASURED);
    assert.ok(measured.length >= 1);
    assert.throws(() => enforceEvidenceIntegrity(result, [
      buildFinding({ evidence_class: EVIDENCE_CLASS.MEASURED, summary: 'bad', id: 'x' }),
    ]));
  });

  it('UNKNOWN remains UNKNOWN and is not upgraded to negative inference', () => {
    assert.throws(() => enforceEvidenceIntegrity({}, [
      buildFinding({
        evidence_class: EVIDENCE_CLASS.UNKNOWN,
        summary: 'Definitely losing customers',
        id: 'bad',
      }),
    ]));
  });

  it('poor site + weak business economics does not rank highly', async () => {
    const biz = FIXTURE_BUSINESSES.find((b) => b.audit_profile === 'weak_economics');
    const result = await assessWebsiteOpportunity({
      domain: biz.domain,
      company: biz.company,
      industry: biz.industry,
      email: biz.email,
      phone: biz.phone,
    }, { fixtureAudit, skipPuppeteer: true });
    assert.ok(result.opportunity_score < 55);
    assert.notEqual(result.recommended_action, RECOMMENDED_ACTIONS.HIGH_VALUE_WEBSITE_OPPORTUNITY);
  });

  it('strong business + adequate site does not rank highly', async () => {
    const biz = FIXTURE_BUSINESSES.find((b) => b.audit_profile === 'adequate_site_strong_business');
    const result = await assessWebsiteOpportunity({
      domain: biz.domain,
      company: biz.company,
      industry: biz.industry,
      email: biz.email,
      phone: biz.phone,
      google_rating: biz.google_rating,
      google_review_count: biz.google_review_count,
    }, { fixtureAudit, skipPuppeteer: true });
    assert.ok(['MONITOR', 'DO_NOT_PURSUE', 'AUDIT_WORTH_REVIEWING'].includes(result.recommended_action));
    assert.ok(result.score_components.website_deficiency.score <= 10);
  });

  it('strong business + material deficiencies + buying signals ranks higher', async () => {
    const biz = FIXTURE_BUSINESSES.find((b) => b.audit_profile === 'high_deficiency_high_value');
    const result = await assessWebsiteOpportunity({
      domain: biz.domain,
      company: biz.company,
      industry: biz.industry,
      email: biz.email,
      phone: biz.phone,
      hiring_signal: true,
      google_rating: biz.google_rating,
      google_review_count: biz.google_review_count,
    }, { fixtureAudit, skipPuppeteer: true });
    assert.ok(result.opportunity_score >= 45);
  });

  it('economics uses $50/hour and capacity penalty', () => {
    const economics = computeProjectEconomics({
      findings: [
        buildFinding({ category: 'performance', evidence_class: EVIDENCE_CLASS.MEASURED, id: 'a' }),
        buildFinding({ category: 'performance', evidence_class: EVIDENCE_CLASS.MEASURED, id: 'b' }),
        buildFinding({ category: 'performance', evidence_class: EVIDENCE_CLASS.MEASURED, id: 'c' }),
        buildFinding({ category: 'accessibility', evidence_class: EVIDENCE_CLASS.MEASURED, id: 'd' }),
      ],
      business: { industry: 'legal', multi_location: true },
    });
    assert.equal(economics.operator_hourly_rate, 50);
    assert.equal(
      economics.estimated_contribution,
      economics.estimated_contract_value - economics.estimated_operator_hours * 50 - economics.estimated_direct_costs
    );
    assert.ok(['prospect_specific_estimate', 'default_planning_only'].includes(economics.label));
  });

  it('Paige context rejects prohibited claims', () => {
    assert.throws(() => sanitizeForPaige('Your website is costing you sales'));
    const ctx = buildPaigeWebEvidenceContext({
      domain: 'example.com',
      opportunity_score: 60,
      recommended_action: RECOMMENDED_ACTIONS.AUDIT_WORTH_REVIEWING,
      evidence_refs: [
        { evidence_class: 'MEASURED', summary: 'Mobile LCP measured 5.4 seconds during audit', source: 'fixture', ref: 'x' },
      ],
      assessment: { recommended_remediation: ['Improve mobile LCP'] },
    });
    assert.ok(ctx.supported_findings_for_copy.length >= 1);
  });

  it('Max prioritization does not sort by raw score alone', () => {
    const ranked = prioritizeWebOpportunities([
      { business_name: 'A', domain: 'a.example', opportunity_score: 80, recommended_action: 'DO_NOT_PURSUE', confidence: 0.3, economics: { estimated_contribution: 200, estimated_operator_hours: 35 }, payload: {} },
      { business_name: 'B', domain: 'b.example', opportunity_score: 62, recommended_action: 'HIGH_VALUE_WEBSITE_OPPORTUNITY', confidence: 0.8, economics: { estimated_contribution: 1800, estimated_operator_hours: 12 }, payload: { evidence_refs: [{ evidence_class: 'MEASURED' }, { evidence_class: 'MEASURED' }] } },
    ]);
    assert.equal(ranked[0].business_name, 'B');
  });

  it('cohort runner completes 25 assessments with zero outbound side effects', async () => {
    const events = [];
    const cap = createWebsiteOpportunityIntelligenceCapability({
      fixtureAudit,
      onEvent: (e) => events.push(e.event_type),
    });
    assert.equal(cap.id, 'website_opportunity_intelligence');

    const result = await runWebCohort001(null, { dryRun: true, fixtureMode: true });
    assert.equal(result.rows.length, 25);
    assert.ok(result.distribution);
    assert.equal(result.stats.no_outreach, true);
    assert.ok(result.top_five.length <= 5);
    assert.ok(Object.values(result.distribution).reduce((a, b) => a + b, 0) === 25);
  });
});
