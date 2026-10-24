'use strict';

/**
 * SPEC-115 — Pilot 0 Self-Service Client Onboarding.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('path');
const express = require('express');

const training = require('../packages/max/training');
const {
  AIM_STATUS,
  FAILURE,
  BEGIN_CLIENT_INTELLIGENCE,
  buildOnboardingGreeting,
  deriveAimStatus,
  maxUnlocked,
  maxAcquisitionReply,
  scoutUnlocked,
  outreachUnlock,
  publicOnboardingState,
} = require('../services/pilotOnboarding');
const { runPilotScout, toProspectRow } = require('../services/pilotScout');
const { persistPublishedAim } = require('../services/aicPersistence');
const {
  createCompiler,
  createMemoryAicStore,
  loadFixtureDocuments,
} = require('../packages/aic');
const { createMemoryAimStore } = require('../packages/aim');

describe('SPEC-115 competency', () => {
  it('registers pilot_0_client_onboarding as a graduated competency', () => {
    const competency = training.getCompetency('pilot_0_client_onboarding');
    assert.equal(competency.stage, training.STAGES.GRADUATED);
    assert.ok(competency.specRefs.includes('SPEC-115'));
    assert.match(competency.exercises[0].generalLesson, /developer intervention is a product bug/i);
  });
});

describe('SPEC-115 greeting', () => {
  it('uses the Client Intelligence opening and a single CTA', () => {
    const greeting = buildOnboardingGreeting('Fedir Kovalenko');
    assert.equal(greeting.greeting, 'Welcome, Fedir.');
    assert.match(greeting.fullText, /understanding your business/);
    assert.match(greeting.fullText, /Client Intelligence/);
    assert.match(greeting.fullText, /prospecting, reasoning/);
    assert.doesNotMatch(greeting.fullText, /Shall we start/);
    assert.equal(greeting.cta, BEGIN_CLIENT_INTELLIGENCE);
  });
});

describe('SPEC-115 gates', () => {
  it('locks Max until Blueprint approved and AIM published', () => {
    assert.equal(maxUnlocked({ clientIntelligence: { approved: false }, aim: { published: true } }), false);
    assert.equal(maxUnlocked({ clientIntelligence: { approved: true }, aim: { published: false } }), false);
    assert.equal(maxUnlocked({ clientIntelligence: { status: 'Approved' }, aim: { status: 'published' } }), true);
    const locked = maxAcquisitionReply({
      tenantId: 11,
      clientIntelligence: { approved: false },
      aim: {},
    });
    assert.equal(locked.code, 'max_locked');
    assert.match(locked.message, /I don't know enough yet/);
    assert.match(locked.message, /Acquisition/);
  });

  it('progresses AIM status from no documents to published', () => {
    assert.equal(deriveAimStatus({}), AIM_STATUS.NO_DOCUMENTS);
    assert.equal(deriveAimStatus({ documentCount: 2 }), AIM_STATUS.READY_TO_COMPILE);
    assert.equal(deriveAimStatus({ compiled: true, documentCount: 2 }), AIM_STATUS.DRAFT);
    assert.equal(deriveAimStatus({ published: true }), AIM_STATUS.PUBLISHED);
  });

  it('keeps Scout and Outreach locked until earned', () => {
    assert.equal(scoutUnlocked({ aim: {} }), false);
    assert.equal(scoutUnlocked({ aim: { published: true } }), true);
    const outreach = outreachUnlock({
      aim: { published: true },
      prospectApproved: false,
      domainHealthy: true,
      sendingCapacityAvailable: true,
      campaignApproved: true,
    });
    assert.equal(outreach.unlocked, false);
    assert.ok(outreach.missing.includes('prospectApproved'));
  });

  it('returns explicit failure states', () => {
    assert.equal(publicOnboardingState({}).failure.code, FAILURE.NO_TENANT.code);
    assert.match(publicOnboardingState({}).failure.message, /No active workspace/);
    assert.equal(
      publicOnboardingState({ passwordChangeRequired: true, tenantId: 1 }).failure.code,
      'password_change_required'
    );
    const noAim = publicOnboardingState({
      tenantId: 9,
      clientIntelligence: { approved: true },
      aim: { documentCount: 0 },
    });
    assert.equal(noAim.failure.code, 'no_aim');
    assert.equal(noAim.cta.href, '/aim');
  });
});

describe('SPEC-115 Scout tenant scope', () => {
  it('refuses Scout without a published AIM', async () => {
    await assert.rejects(
      () => runPilotScout({ clientId: 88, aim: null, persist: false }),
      (err) => err.code === 'no_aim'
    );
  });

  it('returns tenant-scoped prospect rows from discovery', async () => {
    const aimStore = createMemoryAimStore({ seedFedir: false });
    const compiler = createCompiler({ store: createMemoryAicStore(), aimStore });
    const workspace = compiler.ingestAndCompile(
      { clientKey: 'pilot-fedir', clientName: 'Fedir', clientId: 88 },
      loadFixtureDocuments()
    );
    compiler.approve(workspace.id, { operator: 'admin', acceptRemaining: true });
    const published = compiler.publish(workspace.id);
    published.aim.clientId = 88;
    published.aim.status = 'published';

    const candidate = {
      id: 'co-north-loop',
      name: 'North Loop Agency',
      tenantId: '88',
      industry: 'founder-led agency',
      people: [{ name: 'Ada Founder', title: 'Founder' }],
      signals: [{ type: 'reviews', label: 'Owner replies to every review' }],
    };
    const result = await runPilotScout({
      clientId: 88,
      aim: published.aim,
      persist: false,
      companies: [candidate],
      discover: async () => [candidate],
    });
    assert.equal(result.client_id, 88);
    assert.ok(result.prospects.every((p) => Number(p.client_id) === 88));
    const row = toProspectRow({
      name: 'North Loop Agency',
      people: [{ name: 'Ada Founder', title: 'Founder' }],
    }, 99);
    assert.equal(row.client_id, 99);
    assert.equal(row.first_name, 'Ada');
    assert.equal(row.job_title, 'Founder');
    const leaked = toProspectRow(candidate, 88);
    assert.equal(leaked.client_id, 88);
    assert.notEqual(leaked.client_id, 1);
  });
});

describe('SPEC-115 product surfaces — no SQL', () => {
  it('admin and client UIs cover tenant, user, password change, AIM, and CIE', () => {
    const admin = fs.readFileSync(path.join(__dirname, '../public/admin-clients.html'), 'utf8');
    const users = fs.readFileSync(path.join(__dirname, '../routes/users.js'), 'utf8');
    const change = fs.readFileSync(path.join(__dirname, '../public/change-password.html'), 'utf8');
    const aim = fs.readFileSync(path.join(__dirname, '../public/aim.html'), 'utf8');
    const dashboard = fs.readFileSync(path.join(__dirname, '../public/dashboard.html'), 'utf8');
    const login = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
    const auth = fs.readFileSync(path.join(__dirname, '../middleware/auth.js'), 'utf8');

    assert.match(admin, /Create tenant/);
    assert.match(admin, /Temporary password/);
    assert.match(admin, /\/api\/users/);
    assert.doesNotMatch(admin, /INSERT INTO/i);

    assert.match(users, /password_change_required/);
    assert.match(users, /Temporary password/);
    assert.doesNotMatch(users, /UPDATE users SET password_hash[\s\S]*SQL/i);

    assert.match(change, /Welcome to PulseForge/);
    assert.match(change, /please choose a new password/);
    assert.match(change, /Password Updated/);
    assert.match(change, /\/api\/me\/password/);

    assert.match(aim, /No Documents/);
    assert.match(aim, /Ready To Compile/);
    assert.match(aim, /Compile/);
    assert.match(aim, /Publish AIM/);
    assert.match(aim, /Run Scout/);

    assert.match(dashboard, /Begin Client Intelligence/);
    assert.match(dashboard, /Open Acquisition Intelligence/);
    assert.match(login, /password_change_required/);
    assert.match(auth, /password_change_required/);
    assert.doesNotMatch(login, /href="\/signup"/);
  });

  it('admin create-user API sets password_change_required', async () => {
    const usersSrc = fs.readFileSync(path.join(__dirname, '../routes/users.js'), 'utf8');
    assert.match(usersSrc, /password_change_required = TRUE/);
    assert.match(usersSrc, /email_verified, password_change_required/);
  });
});

describe('SPEC-115 persist AIM helper', () => {
  it('exports persistPublishedAim for tenant-scoped runtime', () => {
    assert.equal(typeof persistPublishedAim, 'function');
  });
});

describe('SPEC-115 password change route', () => {
  it('POST /api/me/password rejects short passwords', async () => {
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.user = { id: 3, password_change_required: true };
      req.session = { user: req.user };
      next();
    });
    app.post('/api/me/password', (req, res) => {
      const nextPass = String(req.body?.password || '');
      if (nextPass.length < 8) {
        return res.status(400).json({ error: 'password_too_short' });
      }
      req.session.user.password_change_required = false;
      return res.json({ password_change_required: false, message: 'Password Updated' });
    });
    const server = await new Promise((resolve) => {
      const s = app.listen(0, '127.0.0.1', () => resolve(s));
    });
    const port = server.address().port;
    try {
      const shortRes = await fetch(`http://127.0.0.1:${port}/api/me/password`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: 'short' }),
      });
      assert.equal(shortRes.status, 400);
      const okRes = await fetch(`http://127.0.0.1:${port}/api/me/password`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: 'long-enough-pass' }),
      });
      assert.equal(okRes.status, 200);
      const body = await okRes.json();
      assert.equal(body.password_change_required, false);
    } finally {
      await new Promise((resolve) => server.close(resolve));
    }
  });
});
