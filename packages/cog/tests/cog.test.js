'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');

const {
  COG_VERSION,
  listDomains,
  listDomainIds,
  getDomain,
  listSuites,
  getSuite,
  listFailureTypes,
  classifyFailure,
  registerFailureType,
  createTranscript,
  appendTurn,
  checkBehavior,
  checkAllBehaviors,
  classifyFailuresFromBehaviors,
  runBenchmarkConversation,
  createStubAskFn,
  createCogEngine,
  scoreDomainResult,
  computeOverallScore,
  detectRegressions,
  buildTrendReport,
  formatRunReportText,
} = require('../index');

describe('COG framework', () => {
  describe('domains', () => {
    it('registers all ten initial cognitive domains', () => {
      const ids = listDomainIds();
      assert.equal(ids.length, 10);
      assert.ok(ids.includes('COG-101'));
      assert.ok(ids.includes('COG-110'));
    });

    it('each domain has required structure', () => {
      for (const domain of listDomains()) {
        assert.match(domain.id, /^COG-\d+$/);
        assert.ok(domain.objective);
        assert.ok(domain.conversation?.turns?.length > 0);
        assert.ok(domain.expectedBehaviors?.length > 0);
        assert.ok(domain.evaluationCriteria?.length > 0);
        assert.ok(domain.rubric?.levels?.length > 0);
      }
    });

    it('COG-103 defines assumption extraction benchmark', () => {
      const d = getDomain('COG-103');
      assert.equal(d.shortName, 'Assumption Extraction');
      assert.ok(d.conversation.turns.some(t => /assumption/i.test(t.content)));
    });
  });

  describe('suites', () => {
    it('COG-001 includes all initial domains', () => {
      const suite = getSuite('COG-001');
      assert.ok(suite);
      assert.equal(suite.domainIds.length, 10);
      assert.equal(suite.version, '0.1.0');
    });

    it('lists versioned benchmark suites', () => {
      const suites = listSuites();
      assert.ok(suites.some(s => s.id === 'COG-001'));
    });
  });

  describe('failure taxonomy', () => {
    it('defines R-001 through R-007', () => {
      const types = listFailureTypes();
      assert.equal(types.length, 7);
      for (let i = 1; i <= 7; i++) {
        const code = `R-${String(i).padStart(3, '0')}`;
        assert.ok(types.some(t => t.code === code));
      }
    });

    it('classifies failures with evidence', () => {
      const f = classifyFailure('R-003', { behaviorId: '103-explicit-assumptions', evidence: 'No assumptions listed' });
      assert.equal(f.code, 'R-003');
      assert.equal(f.label, 'Assumption Blindness');
      assert.equal(f.behaviorId, '103-explicit-assumptions');
    });

    it('supports extensible failure types', () => {
      registerFailureType({
        code: 'R-999',
        label: 'Test Failure',
        description: 'Test only',
      });
      const f = classifyFailure('R-999', {});
      assert.equal(f.label, 'Test Failure');
    });
  });

  describe('transcript capture', () => {
    it('captures multi-turn conversation', () => {
      const t = createTranscript({ domainId: 'COG-101', conversationId: 'test' });
      appendTurn(t, 'operator', 'Hello');
      appendTurn(t, 'max', 'Hi, I am Max.');
      assert.equal(t.turns.length, 2);
      assert.equal(t.turns[1].role, 'max');
    });
  });

  describe('behavior checker', () => {
    it('detects pattern matches', () => {
      const domain = getDomain('COG-101');
      const behavior = domain.expectedBehaviors.find(b => b.id === '101-identity-self');
      const transcript = [
        { turnIndex: 0, role: 'operator', content: 'Who are you?' },
        { turnIndex: 1, role: 'max', content: 'I am Max, PulseForge operator intelligence.' },
      ];
      const result = checkBehavior(behavior, transcript);
      assert.equal(result.passed, true);
    });

    it('detects absence violations', () => {
      const domain = getDomain('COG-101');
      const behavior = domain.expectedBehaviors.find(b => b.id === '101-no-role-confusion');
      const transcript = [
        { turnIndex: 0, role: 'operator', content: 'Who are you?' },
        { turnIndex: 1, role: 'max', content: 'I am Scout and I find leads.' },
      ];
      const result = checkBehavior(behavior, transcript);
      assert.equal(result.passed, false);
    });

    it('classifies failures from behavior results', () => {
      const domain = getDomain('COG-101');
      const transcript = [
        { turnIndex: 0, role: 'operator', content: 'Who are you?' },
        { turnIndex: 1, role: 'max', content: 'I am Scout.' },
      ];
      const results = checkAllBehaviors(domain.expectedBehaviors.slice(0, 2), transcript);
      const failures = classifyFailuresFromBehaviors(domain.expectedBehaviors.slice(0, 2), results, transcript);
      assert.ok(failures.some(f => f.code === 'R-006'));
    });
  });

  describe('conversation runner', () => {
    it('executes benchmark conversation with stub ask fn', async () => {
      const domain = getDomain('COG-101');
      const askFn = createStubAskFn([
        'I am Max, PulseForge operator intelligence for this workspace.',
        'I cannot send outreach or publish without operator approval.',
        'We are operating in the Anchor Cleaning workspace, client 10.',
      ]);

      const result = await runBenchmarkConversation(domain, askFn);
      assert.equal(result.domainId, 'COG-101');
      assert.ok(result.transcript.length >= 6);
      assert.ok(result.behaviorResults.length > 0);
      assert.equal(result.score, null);
    });
  });

  describe('scoring (deferred by default)', () => {
    it('returns null score when automated scoring disabled', () => {
      const domain = getDomain('COG-103');
      const domainResult = {
        behaviorResults: [{ passed: true, requiresHumanReview: false }],
        failures: [],
        reviewStatus: 'not_required',
      };
      const scored = scoreDomainResult(domainResult, domain, { automated: false });
      assert.equal(scored.score, null);
      assert.equal(scored.method, 'deferred');
    });

    it('computes automated score when enabled', () => {
      const domain = getDomain('COG-101');
      const domainResult = {
        behaviorResults: [
          { passed: true, requiresHumanReview: false },
          { passed: true, requiresHumanReview: false },
          { passed: false, requiresHumanReview: false },
        ],
        failures: [{ code: 'R-006' }],
        reviewStatus: 'pending',
      };
      const scored = scoreDomainResult(domainResult, domain, { automated: true });
      assert.ok(scored.score !== null);
      assert.ok(scored.score >= 0 && scored.score <= 10);
    });

    it('computeOverallScore ignores unscored domains', () => {
      const overall = computeOverallScore([
        { score: 8 },
        { score: null },
        { score: 6 },
      ]);
      assert.equal(overall, 7);
    });
  });

  describe('CogEngine', () => {
    let storePath;

    beforeEach(() => {
      storePath = path.join(os.tmpdir(), `cog-test-${Date.now()}-${Math.random()}.json`);
    });

    it('runs full suite and persists results', async () => {
      const engine = createCogEngine({
        storeOptions: { storePath, loadExisting: false },
        askFn: createStubAskFn((q, i) =>
          `Max response turn ${i}: I am Max. Assumptions: test. Evidence: verified and unverified. law firm Manchester 20 commercial approve.`
        ),
      });

      const run = await engine.runSuite('COG-001');
      assert.ok(run.runId);
      assert.equal(run.suiteId, 'COG-001');
      assert.equal(run.cogVersion, COG_VERSION);
      assert.equal(run.domains.length, 10);
      assert.equal(run.overallScore, null);

      const loaded = engine.store.getRun(run.runId);
      assert.ok(loaded);
    });

    it('detects regressions between runs', async () => {
      const engine = createCogEngine({
        storeOptions: { storePath, loadExisting: false },
        askFn: createStubAskFn('Good Max response with assumptions and evidence.'),
      });

      const run1 = await engine.runSuite('COG-001', { score: true });
      const run2 = await engine.runSuite('COG-001', {
        score: true,
        askFn: createStubAskFn('Bad.'),
      });

      const report = engine.getReport(run2.runId);
      assert.ok(report.regression);
      assert.ok(typeof report.regression.hasRegression === 'boolean');
      assert.ok(report.text.includes('COG Overall'));
    });

    it('builds per-domain trend report', async () => {
      const engine = createCogEngine({
        storeOptions: { storePath, loadExisting: false },
        askFn: createStubAskFn('Max with assumptions evidence law firm approve 20 commercial.'),
      });

      await engine.runSuite('COG-001', { score: true });
      await engine.runSuite('COG-001', { score: true });

      const runs = engine.listRuns({ suiteId: 'COG-001' }).reverse();
      const trend = buildTrendReport(runs);
      assert.equal(trend.runCount, 2);
      assert.ok(trend.trends['COG-103']);
    });

    it('formats human-readable report', () => {
      const text = formatRunReportText({
        runId: 'test-run',
        suiteId: 'COG-001',
        suiteVersion: '0.1.0',
        cogVersion: COG_VERSION,
        status: 'completed',
        startedAt: '2026-08-24T00:00:00.000Z',
        overallScore: 62,
        domains: [
          { domainId: 'COG-101', score: 10, failures: [] },
          { domainId: 'COG-103', score: 3, failures: [{ code: 'R-003', label: 'Assumption Blindness' }] },
        ],
      });
      assert.match(text, /COG Overall: 62/);
      assert.match(text, /Identity/);
      assert.match(text, /Assumptions/);
    });
  });

  describe('regression detector', () => {
    it('flags score regression', () => {
      const baseline = {
        runId: 'base',
        startedAt: '2026-08-23T00:00:00.000Z',
        overallScore: 8,
        domains: [
          { domainId: 'COG-103', score: 8, failures: [], behaviorResults: [{ passed: true }] },
        ],
      };
      const current = {
        runId: 'current',
        startedAt: '2026-08-24T00:00:00.000Z',
        overallScore: 3,
        domains: [
          {
            domainId: 'COG-103',
            score: 3,
            failures: [{ code: 'R-003', label: 'Assumption Blindness' }],
            behaviorResults: [{ passed: false }],
          },
        ],
      };
      const result = detectRegressions(current, baseline);
      assert.equal(result.hasRegression, true);
      assert.ok(result.domainRegressions.some(r => r.domainId === 'COG-103'));
    });
  });
});
