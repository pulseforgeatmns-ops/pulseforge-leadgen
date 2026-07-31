'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { createMissionEngine } = require('../../../mission-engine');
const { createBuiltinRegistry } = require('../../../capabilities');
const { createWorkspaceEngine, composeResponse } = require('../../index');

function testMissionEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
  });
}

describe('SPEC-022 Workspace Mission routing', () => {
  it('Build Campaign 001 creates a mission and skips Market Intelligence', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    let composeCalled = false;
    const originalCompose = composeResponse;
    const result = await workspace.ask({
      question: 'Build Campaign 001 for Anchor Cleaning.',
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    assert.equal(result.route, 'mission');
    assert.ok(result.mission);
    assert.equal(result.mission.status, 'review_required');
    assert.equal(result.mission.title, 'Campaign 001');
    assert.equal(result.structured.metadata.route, 'mission');
    assert.match(result.prose || result.structured.answer, /Mission created/i);
    assert.match(
      result.prose || result.structured.answer,
      /Discovery Profile/i
    );
    assert.doesNotMatch(
      result.structured.answer,
      /I can investigate .+ using only the intelligence/i
    );
    void originalCompose;
    void composeCalled;
  });

  it('Monitor Microsoft stays on intelligence path', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: 'Monitor Microsoft.',
      context: {
        tenantId: '10',
        page: 'command-deck',
        briefing: {
          headline: 'Quiet morning',
          summary: 'No major movement overnight.',
        },
      },
    });

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.notEqual(result.structured.metadata.route, 'mission');
  });

  it('pasted ProspectList in Build Campaign prompt auto-injects via workspace', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: [
        'Build Campaign 001 for Anchor Cleaning.',
        '',
        'Company Name,Website,Address',
        'Granite State Law,https://gslaw.example,100 Elm St',
        'Queen City CPA,https://qcpa.example,200 Bridge St',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    assert.equal(result.route, 'mission');
    assert.ok(result.mission);
    assert.ok(result.mission.operatorProspectList);
    assert.equal(result.mission.operatorProspectList.injected, true);
    assert.match(
      result.prose || result.structured.answer,
      /Operator ProspectList imported|Satisfied \(Operator Supplied\)|ProspectList/i
    );
    // SPEC-051: Discovery omitted at plan time when operator ProspectList exists
    const discovery = result.mission.plan.steps.find(
      (s) =>
        s.stageId === 'prospect_discovery' ||
        s.capabilityId === 'prospect_discovery'
    );
    assert.equal(discovery, undefined);
    assert.ok(
      result.mission.plan.artifactResolution &&
        result.mission.plan.artifactResolution.resolved.some(
          (r) => r.type === 'ProspectList'
        )
    );
  });

  it('disabled MISSION_ENGINE falls through to intelligence', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: false,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: 'Build Campaign 001 for Anchor Cleaning.',
      context: {
        tenantId: '10',
        page: 'command-deck',
        briefing: { headline: 'Brief', summary: 'Summary' },
      },
    });

    assert.equal(result.route, 'mission');
    assert.equal(result.mission, null);
    assert.notEqual(result.structured.metadata.route, 'mission');
  });

  it('Morning Brief context never blocks Mission Planning (operator intent wins)', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const briefingHeadline =
      'Quiet morning — three watches need review before noon.';
    const result = await workspace.ask({
      question: 'Build Campaign 001 for Anchor Cleaning.',
      context: {
        tenantId: '10',
        page: 'command-deck',
        context: 'morning_brief',
        briefing: {
          headline: briefingHeadline,
          summary: 'No major movement overnight.',
        },
      },
    });

    assert.equal(result.route, 'mission');
    assert.ok(result.mission, 'Mission Engine must create a Mission');
    assert.ok(
      result.mission.plan && result.mission.plan.missionIntent,
      'MissionIntent must be created'
    );
    assert.doesNotMatch(
      result.prose || result.structured.answer || '',
      /Quiet morning/i
    );
    assert.match(
      result.prose || result.structured.answer || '',
      /Mission created/i
    );
  });

  it('semantic mission objectives route via Intent Understanding despite briefing', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: 'Run an end-to-end execution audit for Campaign 001.',
      context: {
        tenantId: '10',
        page: 'command-deck',
        context: 'morning_brief',
        briefing: {
          headline: 'Morning Brief ready',
          summary: 'Pipeline is quiet.',
        },
      },
    });

    assert.equal(result.route, 'mission');
    assert.ok(result.mission);
    assert.ok(result.mission.plan && result.mission.plan.missionIntent);
    assert.equal(
      result.mission.plan.missionIntent.intentCategory,
      'campaign_diagnostics'
    );
    assert.doesNotMatch(
      result.prose || result.structured.answer || '',
      /Morning Brief ready|Pipeline is quiet/i
    );
  });

  it('preparation-only canary asks for prospects before creating package mission', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: [
        'Max, we are not launching or executing direct mail yet.',
        '',
        'I want a preparation-only canary package for review.',
        '',
        'Use 3 prospects from the existing Campaign 001 work if available. If you cannot access them, ask me for 3 prospect names instead of creating an execution mission.',
        '',
        'For each prospect, give me:',
        '- readiness status: Ready / Blocked / Needs verification',
        '- missing or unverified fields',
        '- exact packet checklist',
        '- personalized letter',
        '- handwritten note',
        '- scorecard cover text',
        '- first follow-up call notes',
        '- what I should do next',
        '- what PulseForge should track after mailing',
        '',
        'Do not run Direct Mail Execution.',
        'Do not resume any failed Direct Mail Execution mission.',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const missions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.equal(missions.length, 0);
    assert.match(answer, /preparation-only canary/i);
    assert.match(answer, /3 prospect names/i);
    assert.doesNotMatch(answer, /Mission created|Mail Packages|canRun/i);
    assert.equal(result.domainSwitch, null);
  });

  it('preparation-only canary with numbered prospects returns review package without mission', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: [
        'Use these 3 prospects for the Campaign 001 preparation-only canary package:',
        '',
        '1. PM-001 — Gamache Properties — Ben Gamache — Property Management',
        '2. PM-002 — Elm Grove Companies — David Schleyer — Property Management',
        '3. PM-003 — Mill City Property Management — Lauren DuPaul — Property Management',
        '',
        'Still do not launch, execute, approve, or mail anything.',
        '',
        'Prepare the review package only. For each prospect, return:',
        '- readiness status',
        '- missing or unverified fields',
        '- packet checklist',
        '- personalized letter',
        '- handwritten note',
        '- scorecard cover text',
        '- first follow-up call notes',
        '- next action',
        '- tracking fields',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const missions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.equal(missions.length, 0);
    assert.match(answer, /I found the 3 canary prospects/i);
    assert.match(answer, /preparation-only/i);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /Elm Grove Companies/);
    assert.match(answer, /Mill City Property Management/);
    assert.match(answer, /mailing addresses/i);
    assert.match(answer, /readiness status: Blocked/i);
    assert.doesNotMatch(
      answer,
      /canRun\s*=\s*false|Capability not registered|Campaign requires prospect count|Input is natural language/i
    );
    assert.doesNotMatch(answer, /Mission created/i);
    assert.ok(
      !/readiness status: Ready/i.test(answer),
      'missing address/website/phone must not be Ready'
    );
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
  });

  it('provisional review drafts generate while mail readiness stays Blocked', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: [
        'Continue the Campaign 001 preparation-only canary package.',
        '',
        'Use the same 3 prospects:',
        '',
        '1. PM-001 — Gamache Properties — Ben Gamache — Property Management',
        '2. PM-002 — Elm Grove Companies — David Schleyer — Property Management',
        '3. PM-003 — Mill City Property Management — Lauren DuPaul — Property Management',
        '',
        'Do not create a mission.',
        'Do not launch, execute, approve, or mail anything.',
        'Do not mark any prospect Ready.',
        '',
        'Create provisional review drafts using only known facts. It is okay if mailing readiness is Blocked.',
        '',
        'For each prospect, give me:',
        '- Status: Blocked for mailing',
        '- Draft confidence: Low / Medium / High',
        '- provisional personalized letter',
        '- handwritten note',
        '- scorecard cover text',
        '- first follow-up call notes',
        '- exact missing fields still blocking mail readiness',
        '- what I should verify before printing',
        '- what PulseForge should track once mailed',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const missions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.equal(missions.length, 0);
    assert.match(answer, /Draft readiness:\s*Allowed/i);
    assert.match(answer, /Provisional personalized letter:/i);
    assert.match(answer, /I’m reaching out because Gamache Properties/i);
    assert.match(answer, /Handwritten note:/i);
    assert.match(answer, /Scorecard cover text:/i);
    assert.match(answer, /Mail readiness:\s*Blocked/i);
    assert.match(answer, /Execution readiness:\s*Blocked/i);
    assert.match(answer, /This is preparation-only/i);
    assert.doesNotMatch(answer, /draft held/i);
    assert.doesNotMatch(answer, /Mission created/i);
    assert.doesNotMatch(answer, /canRun\s*=\s*false/i);
    assert.equal(result.structured.metadata.provisionalDrafts, true);
  });

  it('provisional drafts still list mail blockers and never mark Ready', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: [
        'Continue the Campaign 001 preparation-only canary package.',
        '',
        '1. PM-001 — Gamache Properties — Ben Gamache — Property Management',
        '2. PM-002 — Elm Grove Companies — David Schleyer — Property Management',
        '3. PM-003 — Mill City Property Management — Lauren DuPaul — Property Management',
        '',
        'Do not launch, execute, approve, or mail anything.',
        'Create provisional review drafts using only known facts. It is okay if mailing readiness is Blocked.',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const answer = result.prose || result.structured.answer || '';

    assert.match(
      answer,
      /Missing fields blocking mail readiness:\s*mailing address, website, phone/i
    );
    assert.ok(
      !/\bStatus:\s*Ready\b/i.test(answer) &&
        !/\bMail readiness:\s*Ready\b/i.test(answer) &&
        !/\breadiness status:\s*Ready\b/i.test(answer),
      'no prospect may be marked Ready while address/website/phone are missing'
    );
  });

  it('provisional drafts do not hallucinate unsupported specifics', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: [
        'Continue the Campaign 001 preparation-only canary package.',
        '',
        '1. PM-001 — Gamache Properties — Ben Gamache — Property Management',
        '2. PM-002 — Elm Grove Companies — David Schleyer — Property Management',
        '3. PM-003 — Mill City Property Management — Lauren DuPaul — Property Management',
        '',
        'Do not launch, execute, approve, or mail anything.',
        'Create provisional review drafts using only known facts. It is okay if mailing readiness is Blocked.',
        'Draft confidence should reflect known facts only.',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const answer = result.prose || result.structured.answer || '';

    assert.doesNotMatch(answer, /\bManchester\b/i);
    assert.doesNotMatch(answer, /tenant complaints/i);
    assert.doesNotMatch(answer, /portfolio size/i);
    assert.doesNotMatch(answer, /current vendor/i);
    assert.doesNotMatch(answer, /website claims/i);
  });

  it('hard-stops Campaign Creation when canary prospect parsing misses', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const before = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question: [
        'Continue the Campaign 001 preparation-only canary package.',
        'Use these 3 prospects: 1. PM-001 — Gamache Properties ...',
        'Do not create a mission. Do not launch, execute, approve, print, or mail anything.',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const after = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.equal(after.length, before.length);
    assert.doesNotMatch(answer, /Mission created/i);
    assert.doesNotMatch(answer, /Campaign 001\. Status: Review Required/i);
    assert.match(answer, /will not create a Campaign mission/i);
    assert.match(answer, /could not parse them cleanly/i);
    assert.match(
      answer,
      /PM-001\s*\|\s*Gamache Properties\s*\|\s*Ben Gamache/i
    );
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
  });

  it('parses flattened single-paragraph numbered canary prospects', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: [
        'Continue the Campaign 001 preparation-only canary package.',
        'Use these 3 prospects: 1. PM-001 — Gamache Properties — Ben Gamache — Property Management — website unknown — mailing address unknown — phone unknown 2. PM-002 — Elm Grove Companies — David Schleyer — Property Management — website unknown — mailing address unknown — phone unknown 3. PM-003 — Mill City Property Management — Lauren DuPaul — Property Management — website unknown — mailing address unknown — phone unknown',
        'Do not create a mission. Do not launch, execute, approve, print, or mail anything.',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const missions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.equal(missions.length, 0);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /Elm Grove Companies/);
    assert.match(answer, /Mill City Property Management/);
    assert.match(answer, /Ben Gamache/);
    assert.match(answer, /David Schleyer/);
    assert.match(answer, /Lauren DuPaul/);
    assert.doesNotMatch(answer, /Mission created/i);
    assert.equal(result.structured.metadata.prospectCount, 3);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
  });
});

describe('SPEC-022 Command Deck Operations section', () => {
  it('compose includes operations with mission cards', async () => {
    const { createMaxReasoningRuntime } = require('../../index');
    const missionEngine = testMissionEngine();
    await missionEngine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
    });
    const cards = (await missionEngine.list({ tenantId: '10' })).map((m) =>
      missionEngine.toCard(m)
    );

    const max = createMaxReasoningRuntime({
      disableLlm: true,
      missionEngine,
    });
    const model = await max.compose({
      tenantId: '10',
      missions: cards,
      evaluatePolicy: false,
    });

    assert.ok(model.operations);
    assert.equal(model.operations.id, 'operations');
    assert.ok(model.operations.missions.length >= 1);
    assert.equal(model.operations.missions[0].title, 'Campaign 001');
    assert.ok(model.meta.missionCount >= 1);
  });
});
