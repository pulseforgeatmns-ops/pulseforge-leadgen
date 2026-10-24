'use strict';

/**
 * SPEC-144 — Scout Intelligence Credibility Framework tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  evidenceWeight,
  classifyFreshness,
  detectNumericContradictions,
  buildIntelligenceBrief,
  buildTrustAssessment,
  buildConfidenceExplanation,
  buildRankingBreakdown,
  validateBriefAcceptance,
} = require('../packages/scout/credibility');
const { buildInvestigationReport } = require('../packages/scout/investigation/InvestigationReport');
const { normalizeScoutDiscoveryPayload } = require('../packages/acquisition-mission/DiscoveryPayload');
const { formatDiscoveryResultsLines, presentationFromDiscoveryPayload } = require('../packages/acquisition-mission/DiscoveryPresentation');

describe('SPEC-144 — Scout Intelligence Credibility Framework', () => {
  it('assigns official evidence weights', () => {
    assert.equal(evidenceWeight('county_records'), 1.0);
    assert.equal(evidenceWeight('website'), 0.95);
    assert.equal(evidenceWeight('secretary_of_state'), 0.95);
    assert.equal(evidenceWeight('google_places'), 0.8);
    assert.equal(evidenceWeight('linkedin'), 0.75);
    assert.equal(evidenceWeight('facebook'), 0.55);
    assert.equal(evidenceWeight('forum_post'), 0.2);
  });

  it('classifies evidence freshness bands', () => {
    const now = new Date('2026-08-23T00:00:00.000Z');
    assert.equal(classifyFreshness('2026-08-20T00:00:00.000Z', now).band, 'excellent');
    assert.equal(classifyFreshness('2026-06-01T00:00:00.000Z', now).band, 'good');
    assert.equal(classifyFreshness('2025-10-01T00:00:00.000Z', now).band, 'low_confidence');
    assert.equal(classifyFreshness('2022-01-01T00:00:00.000Z', now).band, 'needs_verification');
    assert.equal(classifyFreshness(null, now).band, 'needs_verification');
  });

  it('detects numeric contradictions across sources', () => {
    const conflicts = detectNumericContradictions([
      { source: 'website', label: '15 managed properties listed on website.' },
      { source: 'county_records', label: '42 properties in county records.' },
    ]);

    assert.equal(conflicts.length, 1);
    assert.match(conflicts[0].description, /15/);
    assert.match(conflicts[0].description, /42/);
  });

  it('builds inspectable confidence explanation', () => {
    const explanation = buildConfidenceExplanation({
      confidence: 0.87,
      evidence: [
        { source: 'website', observedAt: '2026-08-20T00:00:00.000Z' },
        { source: 'linkedin', observedAt: '2026-08-18T00:00:00.000Z' },
        { source: 'county_records', observedAt: '2026-07-01T00:00:00.000Z' },
      ],
      missingEvidence: ['decision_maker', 'current_vendor'],
    });

    assert.equal(explanation.score, 0.87);
    assert.ok(explanation.basedOn.some((b) => b.label === 'Website' && b.present));
    assert.ok(explanation.missing.some((m) => m.label === 'Decision maker'));
    assert.equal(explanation.contradictionDetected, false);
  });

  it('reduces confidence when contradictions are present', () => {
    const explanation = buildConfidenceExplanation({
      confidence: 0.91,
      evidence: [{ source: 'website' }, { source: 'county_records' }],
      contradictions: [{ resolved: false, description: 'Portfolio count mismatch' }],
    });

    assert.ok(explanation.score < 0.91);
    assert.equal(explanation.contradictionDetected, true);
    assert.match(explanation.contradictionNote, /Contradiction detected/i);
  });

  it('separates trust from confidence', () => {
    const highConfLowDiversity = buildTrustAssessment({
      confidence: 0.94,
      evidence: [{ source: 'website', weight: 0.95 }],
    });
    assert.equal(highConfLowDiversity.level, 'medium');
    assert.match(highConfLowDiversity.reason, /evidence diversity/i);
    assert.equal(highConfLowDiversity.wouldActOn, false);

    const highConfDiverse = buildTrustAssessment({
      confidence: 0.92,
      evidence: [
        { source: 'website', observedAt: '2026-08-20T00:00:00.000Z' },
        { source: 'linkedin', observedAt: '2026-08-19T00:00:00.000Z' },
        { source: 'county_records', observedAt: '2026-08-01T00:00:00.000Z' },
      ],
    });
    assert.equal(highConfDiverse.level, 'high');
    assert.equal(highConfDiverse.wouldActOn, true);
  });

  it('builds ranking breakdown as percentages', () => {
    const breakdown = buildRankingBreakdown({
      revenue_potential: 0.22,
      buying_signals: 0.18,
      geographic_fit: 0.15,
      ease_of_access: 0.14,
      relationship_probability: 0.12,
      evidence_confidence: 0.11,
      strategic_value: 0.08,
    });

    assert.equal(breakdown.length, 7);
    const totalPercent = breakdown.reduce((sum, row) => sum + row.percent, 0);
    assert.ok(Math.abs(totalPercent - 100) < 1);
    assert.equal(breakdown[0].label, 'Revenue Opportunity');
  });

  it('builds a full intelligence brief with competing hypotheses', () => {
    const brief = buildIntelligenceBrief({
      rankingEntry: {
        rank: 1,
        name: 'Harbor Property Management',
        companyId: 'harbor-1',
        rankScore: 0.88,
        tier: 'strong',
        reasons: ['Strong fit and buying signals'],
        scores: {
          revenue_potential: 0.22,
          buying_signals: 0.18,
          geographic_fit: 0.15,
        },
      },
      candidate: {
        id: 'harbor-1',
        name: 'Harbor Property Management',
        signals: [{ type: 'expansion', label: 'Added 3 buildings', source: 'news' }],
      },
      claims: [
        {
          entityId: 'harbor-1',
          text: 'Harbor manages multiple rental properties.',
          confidence: 0.91,
          supportedBy: [
            { source: 'website', label: '15 managed properties', weight: 0.95 },
            { source: 'county_records', label: '42 properties in county records', weight: 1.0 },
          ],
          missingEvidence: ['current_vendor'],
        },
      ],
      hypotheses: [
        { id: 'h1', entityId: 'harbor-1', text: 'Growing rapidly', confidence: 0.72 },
        { id: 'h2', entityId: 'harbor-1', text: 'Replacing current vendor', confidence: 0.54 },
        { id: 'h3', entityId: 'harbor-1', text: 'No buying activity', confidence: 0.18 },
      ],
    });

    assert.equal(brief.opportunity.name, 'Harbor Property Management');
    assert.ok(brief.overallConfidence > 0);
    assert.ok(brief.whyRankedHere);
    assert.ok(brief.strongestEvidence.length >= 1);
    assert.ok(brief.competingHypotheses.length >= 2);
    assert.ok(brief.contradictions.length >= 1);
    assert.ok(brief.trust);
    assert.ok(brief.recommendedNextInvestigation.action);
    assert.ok(brief.highestRemainingUnknowns.length >= 1);

    const acceptance = validateBriefAcceptance(brief);
    assert.equal(acceptance.passes, true);
  });

  it('attaches intelligence briefs to investigation report recommendations', () => {
    const report = buildInvestigationReport({
      marketDefinition: { geography: 'Manchester NH', segment: 'property_management' },
      graph: { summary: { conflicts: 1 } },
      claims: [
        {
          id: 'c1',
          entityId: 'c1',
          confidence: 0.91,
          text: 'Granite State PM manages multiple properties.',
          missingEvidence: ['current_vendor'],
          supportedBy: [{ source: 'website', label: 'Website evidence', weight: 0.95 }],
        },
      ],
      hypotheses: [{ id: 'h1', entityId: 'c1', text: 'Growing portfolio', confidence: 0.7 }],
      missingEvidence: { missing: ['current_vendor'], currentConfidence: 0.91 },
      overallConfidence: 0.91,
      conflicts: [],
      ranking: {
        rankedOpportunities: [
          {
            rank: 1,
            name: 'Granite State PM',
            companyId: 'c1',
            rankScore: 0.9,
            tier: 'strong',
            reasons: ['High fit'],
            scores: { revenue_potential: 0.22, buying_signals: 0.18 },
          },
        ],
      },
      qualification: { qualifiedCount: 1 },
      candidateUniverse: {
        candidates: [{ id: 'c1', name: 'Granite State PM', signals: [] }],
      },
    });

    assert.equal(report.kind, 'investigation_report');
    assert.ok(report.intelligenceBriefs.length >= 1);
    assert.ok(report.recommendations[0].intelligenceBrief);
    assert.ok(report.recommendations[0].credibility.acceptance.passes);
    assert.equal(report.acceptanceCriteria.allCredibilityBriefsPass, true);
  });

  it('embeds credibility in AMO discovery payload and presentation', () => {
    const normalized = normalizeScoutDiscoveryPayload({
      status: 'completed',
      payload: {
        opportunities: [
          {
            companyId: 'co-1',
            name: 'Harbor Property Management',
            fit: 0.88,
            timing: 0.72,
            confidence: 0.86,
            signals: [{ type: 'expansion', label: 'Added downtown portfolio', source: 'news' }],
            evidenceRefs: [{ sourceKind: 'website', label: 'Company website lists services' }],
            unknowns: ['current_vendor'],
          },
        ],
        confidence: 0.86,
      },
    });

    assert.ok(normalized.credibilityFramework);
    assert.ok(normalized.rankedProspects[0].intelligenceBrief);
    assert.ok(normalized.rankedProspects[0].trust);
    assert.ok(normalized.rankedProspects[0].recommendedNextInvestigation);

    const presentation = presentationFromDiscoveryPayload(normalized);
    const lines = formatDiscoveryResultsLines(presentation);
    const prose = lines.join('\n');

    assert.match(prose, /Harbor Property Management/);
    assert.match(prose, /Trust:/);
    assert.match(prose, /Next verification:/);
  });
});
