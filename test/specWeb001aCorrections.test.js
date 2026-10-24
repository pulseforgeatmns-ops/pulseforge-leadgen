'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  assessWebsiteOpportunity,
  computeOpportunityScore,
  recommendAction,
  computeProjectEconomics,
  buildInferredFindings,
  assertInferredIntegrity,
  isDuplicateOfSource,
  evaluateCohortAdmission,
  assembleStratifiedCohort,
  scoreWebsiteDeficiency,
  scoreBuyingSignals,
  scoreProjectEconomics,
  EVIDENCE_CLASS,
  RECOMMENDED_ACTIONS,
  buildFinding,
  enforceEvidenceIntegrity,
  runDeterministicAudit,
} = require('../packages/capabilities/websiteOpportunityIntelligence');
const { fetchPageSpeedMetrics } = require('../packages/capabilities/websiteOpportunityIntelligence/audit/pagespeedProvider');
const { buildCommercialDiagnosis, classifyDiagnosis } = require('../packages/capabilities/websiteOpportunityIntelligence/diagnosis');
const { DIAGNOSIS_CLASS, ECONOMIC_CONFIDENCE } = require('../packages/capabilities/websiteOpportunityIntelligence/types');
const { createFixtureAuditProvider, FIXTURE_BUSINESSES } = require('../scripts/lib/webCohort001Fixtures');
const { prioritizeWebOpportunities } = require('../utils/webOpportunityMaxPrioritization');
const { buildTopFiveDetail } = require('../scripts/lib/webCohortShared');

describe('SPEC-WEB-001A corrections', () => {
  const fixtureAudit = createFixtureAuditProvider();

  it('MEASURED evidence is not duplicated unchanged in INFERRED', () => {
    const measured = buildFinding({
      id: 'perf_fetch_time',
      evidence_class: EVIDENCE_CLASS.MEASURED,
      category: 'performance',
      summary: 'Homepage fetch completed in 4518 ms during audit',
      measurement: { fetch_ms: 4518 },
      ref: 'performance:fetch_ms',
    });
    const inferred = buildInferredFindings([measured]);
    assert.ok(inferred.length >= 1);
    for (const inf of inferred) {
      assert.notEqual(inf.summary, measured.summary);
      assert.equal(inf.evidence_class, EVIDENCE_CLASS.INFERRED);
    }
    assert.doesNotThrow(() => assertInferredIntegrity([measured], inferred));
  });

  it('OBSERVED evidence is not duplicated unchanged in INFERRED', () => {
    const observed = buildFinding({
      id: 'conv_no_obvious_path',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'conversion_structure',
      summary: 'No obvious phone, email, form, or contact link detected on homepage',
      ref: 'conversion:none_detected',
    });
    const inferred = buildInferredFindings([observed]);
    for (const inf of inferred) {
      assert.notEqual(inf.summary, observed.summary);
    }
  });

  it('enforceEvidenceIntegrity rejects verbatim INFERRED copies', () => {
    const source = buildFinding({
      evidence_class: EVIDENCE_CLASS.MEASURED,
      summary: 'Homepage HTTP status 200',
      measurement: { status_code: 200 },
      ref: 'technical:http_status',
    });
    assert.throws(() => enforceEvidenceIntegrity({}, [
      source,
      buildFinding({
        evidence_class: EVIDENCE_CLASS.INFERRED,
        summary: source.summary,
        derived_from: ['technical:http_status'],
      }),
    ]));
  });

  it('UNKNOWN buying signals when research did not execute', () => {
    const business = { buying_signal_research: 'not_researched' };
    const result = scoreBuyingSignals(business, []);
    assert.equal(result.score, 0);
    assert.equal(result.unknown, true);
    assert.match(result.reasons[0], /UNKNOWN|not performed/i);
    assert.doesNotMatch(result.reasons[0], /No strong buying signals detected/i);
  });

  it('slow deficient site scores higher deficiency than healthy fast site', () => {
    const slowFindings = [
      buildFinding({
        id: 'perf_fetch_time',
        evidence_class: EVIDENCE_CLASS.MEASURED,
        category: 'performance',
        summary: 'Homepage fetch completed in 4518 ms during audit',
        measurement: { fetch_ms: 4518 },
        ref: 'performance:fetch_ms',
      }),
      buildFinding({
        id: 'seo_sitemap_missing',
        evidence_class: EVIDENCE_CLASS.MEASURED,
        category: 'technical_health',
        summary: '/sitemap.xml returned HTTP 404',
        measurement: { status_code: 404, path: '/sitemap.xml' },
        ref: 'seo:sitemap',
      }),
      buildFinding({
        id: 'a11y_missing_alt',
        evidence_class: EVIDENCE_CLASS.MEASURED,
        category: 'accessibility',
        summary: '2 image(s) missing non-empty alt text on homepage',
        measurement: { missing_alt_count: 2, total_images: 5 },
        ref: 'a11y:img_alt',
      }),
    ];
    const healthyFindings = [
      buildFinding({
        id: 'perf_fetch_time',
        evidence_class: EVIDENCE_CLASS.MEASURED,
        category: 'performance',
        summary: 'Homepage fetch completed in 800 ms during audit',
        measurement: { fetch_ms: 800 },
        ref: 'performance:fetch_ms',
      }),
      buildFinding({
        id: 'a11y_missing_alt',
        evidence_class: EVIDENCE_CLASS.MEASURED,
        category: 'accessibility',
        summary: '1 image(s) missing non-empty alt text on homepage',
        measurement: { missing_alt_count: 1, total_images: 10 },
        ref: 'a11y:img_alt',
      }),
    ];

    const slowScore = scoreWebsiteDeficiency(slowFindings).score;
    const healthyScore = scoreWebsiteDeficiency(healthyFindings).score;
    assert.ok(slowScore > healthyScore, `slow=${slowScore} should exceed healthy=${healthyScore}`);
    assert.ok(slowScore >= 8);
    assert.ok(healthyScore <= 4);
  });

  it('generic default economics do not differentiate Max priority', () => {
    const base = {
      opportunity_score: 55,
      recommended_action: RECOMMENDED_ACTIONS.AUDIT_WORTH_REVIEWING,
      confidence: 0.6,
      score_components: {
        website_deficiency: { score: 12 },
        contactability: { score: 9 },
        buying_signals: { score: 0, unknown: true },
        project_economics: { score: 0, uses_default_only: true },
      },
      payload: {
        evidence_refs: [{ evidence_class: 'MEASURED' }],
        commercial_diagnosis: { diagnosis_class: DIAGNOSIS_CLASS.TARGETED_REMEDIATION },
      },
    };
    const a = prioritizeWebOpportunities([{
      ...base,
      business_name: 'A',
      domain: 'a.example',
      economics: {
        economic_confidence: ECONOMIC_CONFIDENCE.UNKNOWN,
        default_planning_economics: { estimated_contribution: 3000 },
        prospect_specific_economics: { estimated_contribution: 3000, estimated_operator_hours: 20 },
      },
    }]);
    const b = prioritizeWebOpportunities([{
      ...base,
      business_name: 'B',
      domain: 'b.example',
      economics: {
        economic_confidence: ECONOMIC_CONFIDENCE.UNKNOWN,
        default_planning_economics: { estimated_contribution: 3000 },
        prospect_specific_economics: { estimated_contribution: 3000, estimated_operator_hours: 20 },
      },
    }]);
    assert.equal(a[0].max_priority_score, b[0].max_priority_score);
  });

  it('rejects maps/search/directory domains at cohort admission', () => {
    assert.equal(evaluateCohortAdmission({ company: 'Acme Law', url: 'https://maps.google.com/foo' }).admitted, false);
    assert.equal(evaluateCohortAdmission({ company: 'Acme Law', url: 'https://lawinfo.com/firm/acme' }).admitted, false);
    assert.equal(evaluateCohortAdmission({ company: 'Contact', url: 'https://acmelaw.com' }).admitted, false);
    assert.equal(evaluateCohortAdmission({ company: 'Acme Law Group', url: 'https://acmelaw.com' }).admitted, true);
  });

  it('rejects duplicate canonical businesses', () => {
    const seenDomains = new Set(['acmelaw.com']);
    const seenCompanies = new Set(['acme law group']);
    const dup = evaluateCohortAdmission(
      { company: 'Acme Law Group', url: 'https://acmelaw.com' },
      { seenDomains, seenCompanies }
    );
    assert.equal(dup.admitted, false);
    assert.equal(dup.reason, 'duplicate_domain');
  });

  it('cohort stratification across verticals and markets', () => {
    const pools = [
      {
        key: 'austin_legal',
        vertical: 'legal',
        location: 'Austin TX',
        candidates: [
          { company: 'Alpha Law Austin', url: 'https://alpha-law-austin.com', vertical: 'legal', location: 'Austin TX' },
          { company: 'Beta Legal Austin', url: 'https://beta-legal-austin.com', vertical: 'legal', location: 'Austin TX' },
        ],
      },
      {
        key: 'denver_dental',
        vertical: 'dental',
        location: 'Denver CO',
        candidates: [
          { company: 'Smile Denver Dental', url: 'https://smile-denver-dental.com', vertical: 'dental', location: 'Denver CO' },
        ],
      },
      {
        key: 'nashville_hvac',
        vertical: 'hvac',
        location: 'Nashville TN',
        candidates: [
          { company: 'Cool Air Nashville', url: 'https://cool-air-nashville.com', vertical: 'hvac', location: 'Nashville TN' },
        ],
      },
    ];
    const result = assembleStratifiedCohort(pools, { targetSize: 3, minStrata: 3 });
    assert.equal(result.admitted.length, 3);
    assert.equal(result.stratification_met, true);
    assert.ok(Object.keys(result.vertical_distribution).length >= 3);
  });

  it('HEALTHY_SITE diagnosis does not become redesign opportunity', () => {
    const findings = [
      buildFinding({
        id: 'tech_https',
        evidence_class: EVIDENCE_CLASS.OBSERVED,
        category: 'technical_health',
        summary: 'Site served over HTTPS',
        ref: 'technical:https',
      }),
      buildFinding({
        id: 'tech_http_ok',
        evidence_class: EVIDENCE_CLASS.MEASURED,
        category: 'technical_health',
        summary: 'Homepage HTTP status 200',
        measurement: { status_code: 200 },
        ref: 'technical:http_status',
      }),
      buildFinding({
        id: 'perf_fetch_time',
        evidence_class: EVIDENCE_CLASS.MEASURED,
        category: 'performance',
        summary: 'Homepage fetch completed in 900 ms during audit',
        measurement: { fetch_ms: 900 },
        ref: 'performance:fetch_ms',
      }),
      buildFinding({
        id: 'conv_phone_link',
        evidence_class: EVIDENCE_CLASS.OBSERVED,
        category: 'conversion_structure',
        summary: 'Phone link present on homepage',
        ref: 'conversion:phone',
      }),
    ];
    const inferred = buildInferredFindings(findings);
    const scoring = computeOpportunityScore({
      findings,
      business: { industry: 'legal', email: 'info@example.com', phone: '555-0100', buying_signal_research: 'not_researched' },
      economics: computeProjectEconomics({ findings, business: { industry: 'legal' } }),
    });
    const diagnosis = buildCommercialDiagnosis({ findings, business: { industry: 'legal', email: 'x@y.com', phone: '1' }, economics: {}, score: scoring, inferredFindings: inferred });
    assert.equal(diagnosis.diagnosis_class, DIAGNOSIS_CLASS.HEALTHY_SITE);
    const rec = recommendAction({
      ...scoring,
      score_components: scoring.score_components,
      economics: {},
      diagnosis_class: diagnosis.diagnosis_class,
    });
    assert.notEqual(rec.action, RECOMMENDED_ACTIONS.HIGH_VALUE_WEBSITE_OPPORTUNITY);
  });

  it('TARGETED_REMEDIATION does not automatically become redesign opportunity', () => {
    const findings = [
      buildFinding({
        id: 'a11y_missing_alt',
        evidence_class: EVIDENCE_CLASS.MEASURED,
        category: 'accessibility',
        summary: '4 image(s) missing non-empty alt text on homepage',
        measurement: { missing_alt_count: 4 },
        ref: 'a11y:img_alt',
      }),
      buildFinding({
        id: 'perf_fetch_time',
        evidence_class: EVIDENCE_CLASS.MEASURED,
        category: 'performance',
        summary: 'Homepage fetch completed in 2800 ms during audit',
        measurement: { fetch_ms: 2800 },
        ref: 'performance:fetch_ms',
      }),
      buildFinding({
        id: 'mobile_no_viewport',
        evidence_class: EVIDENCE_CLASS.OBSERVED,
        category: 'technical_health',
        summary: 'Missing viewport meta tag',
        ref: 'mobile:viewport',
      }),
      buildFinding({
        id: 'conv_no_obvious_path',
        evidence_class: EVIDENCE_CLASS.OBSERVED,
        category: 'conversion_structure',
        summary: 'No obvious phone, email, form, or contact link detected on homepage',
        ref: 'conversion:none_detected',
      }),
      buildFinding({
        id: 'tech_https',
        evidence_class: EVIDENCE_CLASS.OBSERVED,
        category: 'technical_health',
        summary: 'Site served over HTTPS',
        ref: 'technical:https',
      }),
      buildFinding({
        id: 'tech_http_ok',
        evidence_class: EVIDENCE_CLASS.MEASURED,
        category: 'technical_health',
        summary: 'Homepage HTTP status 200',
        measurement: { status_code: 200 },
        ref: 'technical:http_status',
      }),
    ];
    const inferred = buildInferredFindings(findings);
    const scoring = computeOpportunityScore({
      findings,
      business: { industry: 'legal', email: 'info@example.com', phone: '555-0100', buying_signal_research: 'not_researched' },
      economics: computeProjectEconomics({ findings, business: { industry: 'legal' } }),
    });
    const diagnosis = buildCommercialDiagnosis({
      findings,
      business: { industry: 'legal', email: 'x@y.com', phone: '1' },
      economics: {},
      score: scoring,
      inferredFindings: inferred,
    });
    assert.equal(diagnosis.diagnosis_class, DIAGNOSIS_CLASS.TARGETED_REMEDIATION);
    const rec = recommendAction({
      opportunity_score: scoring.opportunity_score,
      score_components: scoring.score_components,
      confidence: scoring.confidence,
      deficiency_only_risk: scoring.deficiency_only_risk,
      economics: {},
      diagnosis_class: diagnosis.diagnosis_class,
    });
    assert.notEqual(rec.action, RECOMMENDED_ACTIONS.HIGH_VALUE_WEBSITE_OPPORTUNITY);
  });

  it('PSI failure becomes UNKNOWN with provider failure reason', async () => {
    const result = await fetchPageSpeedMetrics('example.com', {
      pagespeedApiKey: 'test-key',
      fetchImpl: {
        get: async () => { throw new Error('quota exceeded'); },
      },
    });
    assert.ok(result.telemetry.psi_attempted >= 1);
    assert.ok(result.telemetry.psi_failed >= 1);
    const unknownFindings = result.findings.filter((f) => f.evidence_class === EVIDENCE_CLASS.UNKNOWN);
    assert.ok(unknownFindings.length >= 1);
    assert.ok(unknownFindings.some((f) => f.detail && /quota exceeded/i.test(f.detail)));
  });

  it('buildTopFiveDetail uses bounded inferences not copied findings', () => {
    const row = {
      business: 'Test Co',
      domain: 'test.example',
      raw_payload: {
        commercial_diagnosis: {
          diagnosis_class: DIAGNOSIS_CLASS.TARGETED_REMEDIATION,
          bounded_inferences: [{
            summary: 'Conversion-path gaps may reduce inbound inquiries; CTA and contact visibility improvements should be evaluated first.',
            derived_from: ['conversion:none_detected'],
          }],
          commercially_important: [{
            summary: 'Homepage fetch completed in 4518 ms during audit',
            evidence_class: 'MEASURED',
          }],
        },
        evidence_refs: [
          { evidence_class: 'MEASURED', summary: 'Homepage fetch completed in 4518 ms during audit', ref: 'performance:fetch_ms' },
        ],
        score_components: {},
        economics: {},
      },
      max_priority: 50,
      max_rationale: 'test',
      recommended_action: RECOMMENDED_ACTIONS.AUDIT_WORTH_REVIEWING,
    };
    const detail = buildTopFiveDetail([row])[0];
    assert.ok(detail.evidence.inferred.some((s) => /Conversion-path gaps may reduce/i.test(s)));
    assert.ok(!detail.evidence.inferred.some((s) => /4518 ms during audit/.test(s)));
  });

  it('existing fixture cohort assessments still complete', async () => {
    const biz = FIXTURE_BUSINESSES[0];
    const result = await assessWebsiteOpportunity({
      domain: biz.domain,
      company: biz.company,
      industry: biz.industry,
      email: biz.email,
      phone: biz.phone,
      hiring_signal: true,
      buying_signal_research: 'researched',
    }, { fixtureAudit, skipPuppeteer: true });
    assert.ok(result.opportunity_score >= 0);
    assert.ok(result.commercial_diagnosis.diagnosis_class);
    assert.ok(Array.isArray(result.commercial_diagnosis.bounded_inferences));
  });
});
