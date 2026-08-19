'use strict';

/**
 * SPEC-117 — Emmett Outbound Infrastructure Intelligence (service, routes, competency, send gate).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const express = require('express');

const training = require('../packages/max/training');
const eoi = require('../packages/emmett-outbound');
const {
  resetEngine,
  getEngine,
  planDay,
  approvePlan,
  ingestBrevoResult,
} = require('../services/emmettOutbound');

function snapshot() {
  return {
    inboxAgeDays: 47,
    providerCeiling: 50,
    authentication: { spf: true, dkim: true, dmarc: 'reject' },
    warmup: { status: 'healthy', dailyCap: 50 },
    bounceRate: 0,
    replyRate: 0.094,
    openRate: 0.63,
    complaintRate: 0,
    blacklist: { listed: false },
    sentToday: 0,
    sentYesterday: 18,
    historicalDailyAvg: 16,
    recentSends: 80,
  };
}

describe('SPEC-117 competency and docs', () => {
  it('registers emmett_outbound_infrastructure as a graduated competency', () => {
    const competency = training.getCompetency('emmett_outbound_infrastructure');
    assert.equal(competency.stage, training.STAGES.GRADUATED);
    assert.ok(competency.specRefs.includes('SPEC-117'));
    assert.match(competency.exercises[0].generalLesson, /Reputation is capital/i);
  });

  it('documents numbering, ADR-054, and specialist boundaries', () => {
    const spec = fs.readFileSync(
      path.join(__dirname, '../docs/specs/SPEC-117_Emmett_Outbound_Infrastructure_Intelligence.md'),
      'utf8'
    );
    const adr = fs.readFileSync(
      path.join(__dirname, '../docs/adr/ADR-054_Reputation_Is_Capital.md'),
      'utf8'
    );
    assert.match(spec, /product brief called this SPEC-110/);
    assert.match(spec, /Human approval remains mandatory/);
    assert.match(spec, /Scout discovers/);
    assert.match(adr, /Reputation is capital/);
    assert.match(adr, /Max cannot override Pause or Emergency silently/);
  });
});

describe('SPEC-117 service + send gate', () => {
  it('plans, blocks Max, and allows operator-approved Paige sends', async () => {
    resetEngine();
    const day = await planDay({
      tenantId: '21',
      clientId: 21,
      snapshot: snapshot(),
      prospects: [{
        id: 1,
        email: 'a@example.com',
        vertical: 'law_firm',
        icpScore: 88,
        maxPriority: 0.9,
        contentSource: 'paige',
        paige: { author: 'paige', source: 'paige', subject: 'walkthrough', body: 'worth a look?' },
      }],
    }, { persist: false });
    assert.equal(day.capacity.recommended, 22);
    assert.ok(day.health.reasons.length >= 1);
    assert.equal(day.dashboard.inbox.score, day.health.score);

    await assert.rejects(
      () => approvePlan(day.plan.id, { role: 'max' }, {}, { persist: false }),
      (err) => err.code === 'eoi_operator_acknowledgement_required'
    );
    const approved = await approvePlan(day.plan.id, { role: 'operator', id: 'jacob' }, {}, { persist: false });
    assert.equal(approved.status, 'approved');
    const allowed = getEngine().canSend({
      tenantId: '21',
      localDate: day.plan.localDate,
      candidate: {
        paige: { author: 'paige', source: 'paige', subject: 'walkthrough', body: 'worth a look?' },
        contentSource: 'paige',
      },
      governor: day.governor,
      capacity: day.capacity,
    });
    assert.equal(allowed.allowed, true);
  });

  it('routes Brevo events into learning sinks', async () => {
    resetEngine();
    const result = await ingestBrevoResult({
      client_id: 21,
      event_type: 'hard_bounce',
      prospect_id: 9,
    }, { persist: false });
    assert.equal(result.outcome.type, 'bounce');
    assert.ok(result.learning.some((row) => row.sink === 'scout'));
    assert.ok(result.learning.some((row) => row.sink === 'emmett'));
    assert.ok(result.learning.every((row) => row.autoApplied === false));
  });
});

describe('SPEC-117 routes and send-path wiring', () => {
  it('is mounted from server.js and serves the operator dashboard', () => {
    const server = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
    assert.match(server, /require\('\.\/routes\/emmettOutbound'\)/);
    const routes = fs.readFileSync(path.join(__dirname, '../routes/emmettOutbound.js'), 'utf8');
    assert.match(routes, /\/api\/v1\/eoi\/dashboard/);
    assert.match(routes, /\/api\/v1\/eoi\/plans\/:id\/approve/);
    const ui = fs.readFileSync(path.join(__dirname, '../public/emmett-outbound.html'), 'utf8');
    assert.match(ui, /Inbox Health/);
    assert.match(ui, /Safe Send Governor/);
    assert.match(ui, /approveBtn/);
    const shell = fs.readFileSync(path.join(__dirname, '../public/shared/shell.js'), 'utf8');
    assert.match(shell, /emmett-outbound/);
  });

  it('evaluates the governor before sendEmail and requires approval', () => {
    const agent = fs.readFileSync(path.join(__dirname, '../emmettAgent.js'), 'utf8');
    assert.match(agent, /applyOutboundGate/);
    assert.match(agent, /awaiting_operator_approval/);
    assert.match(agent, /outboundIntel\.canSend/);
    const sendAt = agent.indexOf('const result = await sendEmail');
    const gateAt = agent.indexOf('outboundIntel.canSend');
    assert.ok(gateAt > 0 && sendAt > gateAt, 'governor must run before sendEmail');
    const webhooks = fs.readFileSync(path.join(__dirname, '../routes/webhooks.js'), 'utf8');
    assert.match(webhooks, /ingestBrevoResult/);
  });

  it('approves through a thin HTTP stand-in with an in-memory engine', async () => {
    resetEngine();
    const engine = getEngine();
    const day = engine.planDay({
      tenantId: '21',
      snapshot: snapshot(),
      prospects: [],
    });
    const app = express();
    app.use(express.json());
    app.post('/api/v1/eoi/plans/:id/approve', (req, res) => {
      const approved = engine.approvePlan(req.params.id, { role: 'operator', id: 'tester' });
      res.json({ plan: approved });
    });
    const server = app.listen(0);
    const { port } = server.address();
    const res = await fetch(`http://127.0.0.1:${port}/api/v1/eoi/plans/${day.plan.id}/approve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
    });
    const body = await res.json();
    server.close();
    assert.equal(body.plan.status, 'approved');
    assert.equal(eoi.PLAN_STATUS.APPROVED, 'approved');
  });
});
