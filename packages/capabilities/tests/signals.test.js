'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const signals = require('../signals');
const ranking = require('../ranking');

describe('SPEC-031 Business Signals', () => {
  const asOf = '2026-07-27T12:00:00.000Z';

  it('emits no signals when evidence is absent — never fabricates', () => {
    const pkg = signals.buildBusinessSignalsForProspect(
      {
        id: 'thin_1',
        companyName: 'Thin Co',
        website: 'https://thin.example',
        confidence: 0.9,
      },
      { asOf }
    );
    assert.equal(pkg.signals.length, 0);
    assert.equal(pkg.activeSignals.length, 0);
    assert.equal(pkg.buyingSignals.length, 0);
    assert.equal(pkg.knowledgeWrites.length, 0);
  });

  it('builds hiring + expansion Active signals from corroborated evidence', () => {
    const pkg = signals.buildBusinessSignalsForProspect(
      {
        id: 'law_1',
        companyName: 'Beacon Law',
        hiringActivity: true,
        expanding: true,
        evidence: [
          {
            kind: 'job_posting',
            summary: 'Hiring office manager — careers page',
            url: 'https://beacon.example/careers',
            observedAt: '2026-07-20T00:00:00.000Z',
          },
          {
            kind: 'announcement',
            summary: 'Opened a satellite office in Bedford',
            observedAt: '2026-07-15T00:00:00.000Z',
          },
        ],
        observedAt: '2026-07-20T00:00:00.000Z',
      },
      { asOf }
    );

    assert.ok(pkg.activeSignals.length >= 1);
    const titles = pkg.activeSignals.map((s) => s.title);
    assert.ok(
      titles.some((t) => /Hiring/i.test(t)) ||
        titles.some((t) => /Expanded/i.test(t))
    );
    for (const s of pkg.activeSignals) {
      assert.ok(s.evidence.length >= 1);
      assert.ok(s.observedAt);
      assert.ok(s.confidence !== 'unknown');
      assert.ok(s.influenceWeight > 0);
      assert.ok(['active', 'decaying'].includes(s.lifecycle));
    }
    assert.ok(pkg.knowledgeWrites.some((w) => w.kind === 'evidence'));
  });

  it('Unknown / empty evidence never becomes Active', () => {
    const detected = [
      signals.buildBusinessSignal({
        id: 'bad',
        type: 'hiring_office_staff',
        category: 'growth',
        title: 'Hiring',
        description: 'guess',
        confidence: 'unknown',
        confidenceScore: 0,
        lifecycle: 'detected',
        observedAt: asOf,
        source: 'test',
        evidence: [],
        evidenceRefs: [],
      }),
    ];
    const verified = signals.verifySignals(detected, { asOf });
    assert.equal(verified.length, 0);
  });

  it('decays influence and archives after hard TTL', () => {
    const observedAt = '2025-01-01T00:00:00.000Z';
    const pkgFresh = signals.buildBusinessSignalsForProspect(
      {
        id: 'old_1',
        companyName: 'Old Expand Co',
        expanding: true,
        evidence: [
          {
            kind: 'announcement',
            summary: 'Opened a second office',
            observedAt,
          },
          {
            kind: 'press',
            summary: 'Official expansion announcement',
            observedAt,
          },
        ],
        observedAt,
      },
      { asOf: '2025-01-15T00:00:00.000Z' }
    );
    assert.ok(pkgFresh.activeSignals.length >= 1);
    const weightFresh = pkgFresh.activeSignals[0].influenceWeight;

    const pkgStale = signals.buildBusinessSignalsForProspect(
      {
        id: 'old_1',
        companyName: 'Old Expand Co',
        expanding: true,
        evidence: [
          {
            kind: 'announcement',
            summary: 'Opened a second office',
            observedAt,
          },
          {
            kind: 'press',
            summary: 'Official expansion announcement',
            observedAt,
          },
        ],
        observedAt,
      },
      { asOf: '2026-07-27T00:00:00.000Z' }
    );
    assert.equal(pkgStale.activeSignals.length, 0);
    assert.ok(pkgStale.archivedCount >= 1);
    assert.ok(weightFresh > 0);
  });

  it('Low confidence activates only when preferred by profile/playbook', () => {
    const prospect = {
      id: 'hire_low',
      companyName: 'Hire Low Co',
      hiringActivity: true,
      observedAt: asOf,
    };
    const without = signals.buildBusinessSignalsForProspect(prospect, { asOf });
    assert.equal(without.activeSignals.length, 0);

    const withPref = signals.buildBusinessSignalsForProspect(prospect, {
      asOf,
      preferredSignalTypes: ['hiring_activity'],
    });
    assert.ok(withPref.activeSignals.length >= 1);
    assert.equal(withPref.activeSignals[0].confidence, 'low');
  });

  it('Opportunity Ranking consumes Active signals for buying factor + brief', () => {
    const prospect = {
      id: 'rank_sig',
      companyName: 'Expanding Partners LLP',
      industry: 'Law Firms',
      address: '1 Elm St, Manchester, NH',
      website: 'https://expanding.example',
      email: 'ops@expanding.example',
      phone: '603-555-0199',
      jobTitle: 'Office Manager',
      expanding: true,
      hiringActivity: true,
      evidence: [
        {
          kind: 'job_posting',
          summary: 'Hiring administrative staff',
          observedAt: '2026-07-20T00:00:00.000Z',
        },
        {
          kind: 'announcement',
          summary: 'Opened a second office in Bedford',
          observedAt: '2026-07-18T00:00:00.000Z',
        },
      ],
      observedAt: '2026-07-20T00:00:00.000Z',
      confidence: 0.85,
      rankingSignals: [
        {
          signal: 'target_industry',
          weight: 0.9,
          matched: true,
          detail: 'Matched — Law Firms',
        },
      ],
      enriched: true,
    };

    const thin = ranking.scoreOpportunity({
      id: 'thin',
      companyName: 'Thin',
      confidence: 0.85,
      rankingSignals: [],
    });
    const rich = ranking.scoreOpportunity(prospect);
    const buying = rich.factorScores.find((f) => f.factor === 'buying_signals');
    assert.ok(buying.score > 0);
    assert.match(buying.detail, /Active Business Signals/i);
    assert.ok(rich.overallScore > thin.overallScore);

    const brief = ranking.buildBrief(prospect, { ...rich, priority: 'high' });
    assert.match(brief.whyFit, /Expanding Partners|Hiring|Expanded|office/i);
    assert.ok(
      /expansion|operations|facility|Hiring|Expanded/i.test(brief.bestOutreachAngle)
    );
  });

  it('Campaign messaging posture keys off Active signal types', () => {
    const pkg = signals.buildBusinessSignalsForProspect(
      {
        id: 'msg_1',
        companyName: 'Growth Co',
        expanding: true,
        evidence: [
          {
            kind: 'announcement',
            summary: 'Opened a satellite office',
            observedAt: asOf,
          },
          {
            kind: 'press',
            summary: 'Official expansion announcement',
            observedAt: asOf,
          },
        ],
        observedAt: asOf,
      },
      { asOf }
    );
    assert.equal(pkg.messagingPosture, 'growth');
    assert.match(pkg.messagingDescription, /Growth messaging/i);
  });

  it('buildBusinessSignalsStage attaches fields for Company Intelligence hook', () => {
    const stage = signals.buildBusinessSignalsStage(
      [
        {
          id: 'p1',
          companyName: 'A',
          hiringActivity: true,
          evidence: [
            { kind: 'job_posting', summary: 'Hiring office staff', observedAt: asOf },
            { kind: 'careers', summary: 'Careers page lists admin role', observedAt: asOf },
          ],
          observedAt: asOf,
        },
        { id: 'p2', companyName: 'B' },
      ],
      { asOf }
    );
    assert.equal(stage.prospects.length, 2);
    assert.ok(stage.prospects[0].activeSignals.length >= 1);
    assert.equal(stage.prospects[1].activeSignals.length, 0);
    assert.ok(stage.activeCount >= 1);
  });
});
