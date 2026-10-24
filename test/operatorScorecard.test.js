'use strict';

/**
 * SPEC-116 — Operator Scorecard Intelligence (service, routes, competency, brief).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const express = require('express');

const training = require('../packages/max/training');
const { PROFILES } = require('../packages/operator-scorecard');
const {
  getEngine,
  resetEngine,
  generateDraft,
  getRuntime,
  approve,
  briefSectionsFor,
  dailyBriefingSection,
} = require('../services/operatorScorecard');
const { buildExecutiveSummary } = require('../services/clientIntelligenceInterview');
const { BRIEFING_SECTIONS } = require('../packages/max/briefing');

function babrun() {
  return {
    tenantId: '21',
    clientId: 21,
    businessName: 'Babrun',
    businessGoal:
      'Validate founder transformation methodology and establish a repeatable acquisition process.',
    profile: PROFILES.FOUNDER_TRANSFORMATION,
  };
}

describe('SPEC-116 competency and docs', () => {
  it('registers operator_scorecard_intelligence as a graduated competency', () => {
    const competency = training.getCompetency('operator_scorecard_intelligence');
    assert.equal(competency.stage, training.STAGES.GRADUATED);
    assert.ok(competency.specRefs.includes('SPEC-116'));
    assert.match(competency.exercises[0].generalLesson, /operator-defined/i);
  });

  it('documents ADR-053 and replaces Success Looks Like', () => {
    const spec = fs.readFileSync(
      path.join(__dirname, '../docs/specs/SPEC-116_Operator_Scorecard_Intelligence.md'),
      'utf8'
    );
    const adr = fs.readFileSync(
      path.join(__dirname, '../docs/adr/ADR-053_Business_Success_Is_Operator_Defined.md'),
      'utf8'
    );
    assert.match(spec, /Max recommends/);
    assert.match(spec, /never used for reporting/i);
    assert.match(spec, /Recommended Operator Scorecard/);
    assert.match(spec, /Operator Approved Scorecard/);
    assert.match(adr, /Business success is not determined by PulseForge/i);
    const ui = fs.readFileSync(path.join(__dirname, '../public/client-intel.html'), 'utf8');
    assert.match(ui, /kind === 'scorecard'/);
    const shell = fs.readFileSync(path.join(__dirname, '../public/shared/shell.js'), 'utf8');
    assert.match(shell, /operator-scorecard/);
  });
});

describe('SPEC-116 service + brief + briefing', () => {
  it('exposes a draft-not-runtime service and brief sections', async () => {
    resetEngine();
    const draft = await generateDraft(babrun(), { persist: false });
    const runtime = await getRuntime('21', { persist: false });
    assert.equal(runtime.status, 'absent');
    const brief = briefSectionsFor(draft);
    assert.equal(brief[0].title, 'Recommended Operator Scorecard');
    assert.equal(brief[1].title, 'Operator Approved Scorecard');
    const daily = dailyBriefingSection(draft);
    assert.equal(daily.status, 'absent');
    await approve(draft.id, { operator: 'max' }, { persist: false });
    const after = await getRuntime('21', { persist: false });
    assert.equal(after.status, 'approved');
  });

  it('replaces Success Looks Like on the Executive Business Brief', () => {
    const brief = buildExecutiveSummary({
      identity: { summary: 'Babrun helps founders build a business machine.', confidence: 0.9, unknowns: [] },
      services: { summary: 'Cohort transformation for founder-led companies.', confidence: 0.8, unknowns: [] },
      idealCustomers: { summary: 'Founders stuck in the business.', confidence: 0.8, unknowns: [] },
      avoidCustomers: { summary: 'Companies that want a tactic pack.', confidence: 0.7, unknowns: [] },
      targetMarkets: { summary: 'English-speaking founder-led firms.', confidence: 0.7, unknowns: [] },
      competitiveAdvantages: { summary: 'A methodology, not a course.', confidence: 0.8, unknowns: [] },
      brandVoice: { summary: 'Direct and practical.', confidence: 0.7, unknowns: [] },
      campaignGoals: {
        summary: 'Validate founder transformation methodology and establish a repeatable acquisition process.',
        confidence: 0.8,
        unknowns: [],
      },
      successMetrics: { summary: 'Pain confirmation and enrollments.', confidence: 0.7, unknowns: [] },
    });
    const ids = brief.sections.map((s) => s.id);
    assert.equal(ids.includes('successLooksLike'), false);
    assert.ok(ids.includes('recommendedScorecard'));
    assert.ok(ids.includes('approvedScorecard'));
    assert.ok(ids.includes('metricsUnderReview'));
    assert.equal(brief.sections.length, 11);
    const rec = brief.sections.find((s) => s.id === 'recommendedScorecard');
    assert.match(rec.body, /stated business objectives/i);
    assert.ok(rec.items.length >= 1);
    const approved = brief.sections.find((s) => s.id === 'approvedScorecard');
    assert.match(approved.body, /not yet approved/i);
  });

  it('includes scorecard on the daily briefing template', () => {
    assert.ok(BRIEFING_SECTIONS.includes('scorecard'));
  });
});

describe('SPEC-116 routes', () => {
  it('is mounted from server.js and serves the review UI', () => {
    const server = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
    assert.match(server, /require\('\.\/routes\/operatorScorecard'\)/);
    const routes = fs.readFileSync(path.join(__dirname, '../routes/operatorScorecard.js'), 'utf8');
    assert.match(routes, /\/api\/v1\/osi\/scorecards\/draft/);
    assert.match(routes, /\/api\/v1\/osi\/runtime/);
    const ui = fs.readFileSync(path.join(__dirname, '../public/operator-scorecard.html'), 'utf8');
    assert.match(ui, /Approve scorecard/);
    assert.match(ui, /Drafts are not used for reporting/);
  });

  it('reviews and approves through the HTTP API with an in-memory engine', async () => {
    resetEngine();
    const engine = getEngine();
    const draft = engine.generateDraft(babrun());
    const app = express();
    app.use(express.json());
    app.post('/api/v1/osi/scorecards/:id/approve', async (req, res) => {
      const approved = engine.approve(req.params.id, { operator: 'tester' });
      res.json({ scorecard: approved });
    });
    const http = require('node:http');
    const server = app.listen(0);
    const { port } = server.address();
    const res = await fetch(`http://127.0.0.1:${port}/api/v1/osi/scorecards/${draft.id}/approve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
    });
    const body = await res.json();
    server.close();
    assert.equal(body.scorecard.status, 'approved');
    assert.equal(engine.runtime('21').status, 'approved');
  });
});
