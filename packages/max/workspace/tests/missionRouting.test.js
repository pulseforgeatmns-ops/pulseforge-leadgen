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

  it('absolute hard stop: preparation-only canary with prospects never creates a mission', async () => {
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
        '',
        'Use these 3 prospects:',
        '',
        '1. PM-001 — Gamache Properties — Ben Gamache — Property Management — website unknown — mailing address unknown — phone unknown',
        '2. PM-002 — Elm Grove Companies — David Schleyer — Property Management — website unknown — mailing address unknown — phone unknown',
        '3. PM-003 — Mill City Property Management — Lauren DuPaul — Property Management — website unknown — mailing address unknown — phone unknown',
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
    assert.equal(result.domainSwitch, null);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(result.structured.metadata.prospectCount, 3);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /Elm Grove Companies/);
    assert.match(answer, /Mill City Property Management/);
    assert.doesNotMatch(answer, /Mission created/i);
    assert.doesNotMatch(answer, /Campaign Builder|Campaign Creation/i);
    assert.doesNotMatch(answer, /Mission Workspace|Status: Review Required/i);
    assert.doesNotMatch(answer, /\bManchester\b|tenant complaints|portfolio size|current vendor/i);
  });

  it('absolute hard stop: preparation-only canary without prospects never creates a mission', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const before = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question: 'Continue the Campaign 001 preparation-only canary package.',
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
    assert.equal(result.domainSwitch, null);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.doesNotMatch(answer, /Mission created/i);
    assert.match(answer, /preparation-only canary|3 prospect/i);
  });

  it('absolute hard stop: Build Campaign 001 control still creates a campaign mission', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: 'Build Campaign 001 for Anchor Cleaning.',
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    assert.equal(result.route, 'mission');
    assert.ok(result.mission);
    assert.equal(result.mission.title, 'Campaign 001');
    assert.match(result.prose || result.structured.answer || '', /Mission created/i);
  });

  it('verification work order returns field checklist without mission or drafts', async () => {
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
        'Use these 3 prospects:',
        '',
        '1. PM-001 — Gamache Properties — Ben Gamache — Property Management',
        '2. PM-002 — Elm Grove Companies — David Schleyer — Property Management',
        '3. PM-003 — Mill City Property Management — Lauren DuPaul — Property Management',
        '',
        'Turn the 3-prospect canary into a verification work order.',
        'For each prospect, give me:',
        '- exact fields to verify',
        '- suggested source type for each field',
        '- why the field matters',
        '- what value would make the prospect Ready vs still Blocked',
        '- what should be logged in PulseForge',
        '- what I should do first',
        '',
        'Do not create a mission.',
        'Do not launch, execute, approve, print, or mail anything.',
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
    assert.match(answer, /Verification work order/i);
    assert.match(answer, /Mailing address/);
    assert.match(answer, /Suggested source type/i);
    assert.match(answer, /Ready value/i);
    assert.match(answer, /Still Blocked if/i);
    assert.match(answer, /prospect_id/);
    assert.match(answer, /verified_mailing_address/);
    assert.match(answer, /mail_readiness/);
    assert.match(answer, /First action/i);
    assert.match(answer, /Preparation-only\. No mission created/i);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /Elm Grove Companies/);
    assert.match(answer, /Mill City Property Management/);
    assert.doesNotMatch(answer, /draft held/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /Provisional personalized letter/i);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(result.structured.metadata.verificationWorkOrder, true);
  });

  it('verification work order without prospects asks for 3 prospects and creates no mission', async () => {
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
        'Turn the 3-prospect canary into a verification work order.',
        'For each prospect, give me exact fields to verify, suggested source type, Ready vs Blocked, what should be logged, and what I should do first.',
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
    assert.match(answer, /3 prospects/i);
    assert.match(answer, /verification work order/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(result.structured.metadata.verificationWorkOrder, true);
  });
});

describe('Max activeWorkContext (session desk memory)', () => {
  const CANARY_WORK_ORDER_PROMPT = [
    'Continue the Campaign 001 preparation-only canary package.',
    '',
    'Use these 3 prospects:',
    '',
    '1. PM-001 — Gamache Properties — Ben Gamache — Property Management',
    '2. PM-002 — Elm Grove Companies — David Schleyer — Property Management',
    '3. PM-003 — Mill City Property Management — Lauren DuPaul — Property Management',
    '',
    'Turn the 3-prospect canary into a verification work order.',
    'For each prospect, give me:',
    '- exact fields to verify',
    '- suggested source type for each field',
    '- why the field matters',
    '- what value would make the prospect Ready vs still Blocked',
    '- what should be logged in PulseForge',
    '- what I should do first',
    '',
    'Do not create a mission.',
    'Do not launch, execute, approve, print, or mail anything.',
  ].join('\n');

  it('Test 1: stores canary activeWorkContext after verification work order', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: CANARY_WORK_ORDER_PROMPT,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const awc =
      result.context.activeWorkContext ||
      workspace._sessions.get(result.sessionId).activeWorkContext;

    assert.ok(awc, 'session context stores activeWorkContext');
    assert.equal(awc.workflow, 'campaign_canary');
    assert.equal(awc.target.campaignId, '001');
    assert.equal(awc.entities.length, 3);
    assert.equal(awc.entities[0].companyName, 'Gamache Properties');
    assert.equal(awc.entities[1].companyName, 'Elm Grove Companies');
    assert.equal(awc.entities[2].companyName, 'Mill City Property Management');
    assert.equal(awc.constraints.preparationOnly, true);
    assert.equal(awc.constraints.noMissionCreation, true);
    assert.equal(awc.constraints.noLaunch, true);
    assert.equal(awc.constraints.noExecution, true);
    assert.equal(awc.constraints.noMail, true);
    assert.equal(awc.constraints.noInventedEvidence, true);
    assert.equal(awc.lastOutputType, 'verification_work_order');
    assert.ok(awc.pendingFields.includes('website'));
    assert.ok(awc.pendingFields.includes('mailingAddress'));
    assert.ok(awc.pendingFields.includes('phone'));
  });

  it('stores activeWorkContext after generic preparation-only canary package', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: [
        'Continue Campaign 001 preparation-only canary package with:',
        'PM-001 | Gamache Properties | Ben Gamache | Property Management | website unknown | mailing address unknown | phone unknown',
        'PM-002 | Elm Grove Companies | David Schleyer | Property Management | website unknown | mailing address unknown | phone unknown',
        'PM-003 | Mill City Property Management | Lauren DuPaul | Property Management | website unknown | mailing address unknown | phone unknown',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const awc = workspace._sessions.get(result.sessionId).activeWorkContext;
    const answer = result.prose || result.structured.answer || '';

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.match(answer, /I found the 3 canary prospects/i);
    assert.ok(awc, 'generic canary package must persist activeWorkContext');
    assert.equal(awc.workflow, 'campaign_canary');
    assert.equal(awc.target.campaignId, '001');
    assert.equal(awc.lastOutputType, 'canary_review_package');
    assert.equal(awc.entities.length, 3);
    assert.equal(awc.entities[0].id, 'PM-001');
    assert.equal(awc.entities[1].id, 'PM-002');
    assert.equal(awc.entities[2].id, 'PM-003');
    assert.equal(awc.entities[0].companyName, 'Gamache Properties');
    assert.equal(awc.constraints.preparationOnly, true);
    assert.equal(awc.constraints.noMissionCreation, true);
    assert.equal(awc.constraints.noLaunch, true);
    assert.equal(awc.constraints.noExecution, true);
    assert.equal(awc.constraints.noMail, true);
    assert.equal(awc.constraints.noPrint, true);
    assert.ok(awc.pendingFields.includes('website'));
    assert.ok(awc.pendingFields.includes('mailingAddress'));
    assert.ok(awc.pendingFields.includes('phone'));
  });

  it('converts generic canary package into fillable table via same-prospects reuse', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: [
        'Continue Campaign 001 preparation-only canary package with:',
        'PM-001 | Gamache Properties | Ben Gamache | Property Management | website unknown | mailing address unknown | phone unknown',
        'PM-002 | Elm Grove Companies | David Schleyer | Property Management | website unknown | mailing address unknown | phone unknown',
        'PM-003 | Mill City Property Management | Lauren DuPaul | Property Management | website unknown | mailing address unknown | phone unknown',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    assert.ok(workspace._sessions.get(first.sessionId).activeWorkContext);
    assert.equal(
      workspace._sessions.get(first.sessionId).activeWorkContext.entities.length,
      3
    );

    const before = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question:
        'Convert the current Campaign 001 preparation-only canary into a fillable verification table. Use the same 3 prospects already listed.',
    });

    const after = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const awc = workspace._sessions.get(result.sessionId).activeWorkContext;
    const rows = awc.tableRows || [];

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.equal(after.length, before.length);
    assert.doesNotMatch(answer, /could not parse them cleanly/i);
    assert.doesNotMatch(answer, /I need 3 prospects|send me 3 prospect/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.match(answer, /Fillable verification table|fillable table/i);
    assert.match(answer, /PM-001/);
    assert.match(answer, /PM-002/);
    assert.match(answer, /PM-003/);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /Elm Grove Companies/);
    assert.match(answer, /Mill City Property Management/);
    assert.equal(result.structured.metadata.fillableTable, true);
    assert.equal(result.structured.metadata.activeWorkContextReused, true);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(awc.lastOutputType, 'fillable_table');
    assert.equal(awc.entities.length, 3);
    assert.equal(rows.length, 3);
    assert.equal(rows[0].prospect_id, 'PM-001');
    assert.equal(rows[1].prospect_id, 'PM-002');
    assert.equal(rows[2].prospect_id, 'PM-003');
    assert.equal(String(rows[0].mail_readiness).toLowerCase(), 'blocked');
    assert.equal(String(rows[0].draft_readiness).toLowerCase(), 'allowed');
    assert.equal(String(rows[0].execution_readiness).toLowerCase(), 'blocked');
    assert.equal(awc.constraints.noMail, true);
    assert.equal(awc.constraints.noLaunch, true);
  });

  it('Test 2: reuses activeWorkContext for fillable table follow-up', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: CANARY_WORK_ORDER_PROMPT,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question: 'Convert the verification work order into a fillable table.',
    });

    const missions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const awc =
      result.context.activeWorkContext ||
      workspace._sessions.get(result.sessionId).activeWorkContext;

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.equal(missions.length, 0);
    assert.doesNotMatch(answer, /I need 3 prospects|send me 3 prospect/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.match(answer, /Fillable verification table|fillable table/i);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /Elm Grove Companies/);
    assert.match(answer, /Mill City Property Management/);
    assert.match(answer, /prospect_id/);
    assert.match(answer, /mailing_address_value|mailing_address_status/);
    assert.match(answer, /website_value|website_status/);
    assert.match(answer, /phone_value|phone_status/);
    assert.match(answer, /verification_status/);
    assert.match(answer, /operator_next_action/);
    assert.match(answer, /No launch|no-launch|no-mail|Preparation-only/i);
    assert.equal(result.structured.metadata.fillableTable, true);
    assert.equal(result.structured.metadata.activeWorkContextReused, true);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(awc.lastOutputType, 'fillable_table');
    assert.equal(awc.entities.length, 3);
    assert.equal(awc.constraints.noMail, true);
    assert.equal(awc.constraints.noLaunch, true);
  });

  it('Test 3: explicit new prospects override activeWorkContext', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: CANARY_WORK_ORDER_PROMPT,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question: [
        'Use these 2 prospects instead for the Campaign 001 preparation-only canary:',
        '',
        '1. PM-010 — North End Holdings — Alex Rivera — Property Management',
        '2. PM-011 — Riverbend Realty — Sam Ortiz — Property Management',
        '',
        'Do not create a mission. Do not launch, execute, approve, print, or mail anything.',
      ].join('\n'),
    });

    const answer = result.prose || result.structured.answer || '';
    const awc =
      result.context.activeWorkContext ||
      workspace._sessions.get(result.sessionId).activeWorkContext;

    assert.equal(awc.entities.length, 2);
    assert.equal(awc.entities[0].companyName, 'North End Holdings');
    assert.equal(awc.entities[1].companyName, 'Riverbend Realty');
    assert.ok(!awc.entities.some((e) => e.companyName === 'Gamache Properties'));
    assert.ok(!awc.entities.some((e) => e.companyName === 'Elm Grove Companies'));
    assert.ok(
      !awc.entities.some((e) => e.companyName === 'Mill City Property Management')
    );
    assert.match(answer, /North End Holdings/);
    assert.match(answer, /Riverbend Realty/);
    assert.doesNotMatch(answer, /Gamache Properties/);
    assert.doesNotMatch(answer, /Elm Grove Companies/);
    assert.doesNotMatch(answer, /Mill City Property Management/);
  });

  it('Test 4: execution still blocked when mailing from active context', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: CANARY_WORK_ORDER_PROMPT,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const before = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question: 'Great, mail these now.',
    });

    const after = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.equal(after.length, before.length);
    assert.match(answer, /not mailing|will not|not .*launch/i);
    assert.match(answer, /explicit approval|verify|readiness|missing/i);
    assert.match(answer, /website|mailing|phone|mailingAddress|mailing address/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
  });

  it('Test 5: fillable table field update mutates PM-001 only', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: CANARY_WORK_ORDER_PROMPT,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const tableTurn = await workspace.ask({
      sessionId: first.sessionId,
      question: 'Convert the verification work order into a fillable table.',
    });

    const beforeRows =
      workspace._sessions.get(first.sessionId).activeWorkContext.tableRows || [];
    assert.equal(beforeRows.length, 3);
    const beforePm002 = { ...beforeRows.find((r) => r.prospect_id === 'PM-002') };
    const beforePm003 = { ...beforeRows.find((r) => r.prospect_id === 'PM-003') };
    assert.equal(beforePm002.draft_readiness, 'allowed');
    assert.equal(beforePm003.draft_readiness, 'allowed');

    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question: [
        'Update the fillable verification table.',
        '',
        'For PM-001 only, set:',
        '- website_value: unknown',
        '- website_status: needs verification',
        '- mailing_address_value: unknown',
        '- mailing_address_status: blocked',
        '- phone_value: unknown',
        '- phone_status: blocked',
        '- notes: still waiting on verified company website, mailing address, and phone',
        '',
        'Leave PM-002 and PM-003 unchanged.',
        '',
        'Keep this preparation-only.',
        'Do not create a mission.',
        'Do not launch, execute, approve, print, or mail anything.',
      ].join('\n'),
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const awc = workspace._sessions.get(result.sessionId).activeWorkContext;
    const rows = awc.tableRows || [];
    const pm001 = rows.find((r) => r.prospect_id === 'PM-001');
    const pm002 = rows.find((r) => r.prospect_id === 'PM-002');
    const pm003 = rows.find((r) => r.prospect_id === 'PM-003');

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.match(answer, /Fillable verification table|fillable table/i);
    assert.match(answer, /Preparation-only/i);
    assert.equal(result.structured.metadata.fillableTable, true);
    assert.equal(result.structured.metadata.tableUpdate, true);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(awc.lastOutputType, 'fillable_table');
    assert.equal(rows.length, 3);
    assert.ok(pm001);
    assert.ok(pm002);
    assert.ok(pm003);
    assert.equal(pm001.website_value, 'unknown');
    assert.equal(pm001.website_status, 'needs verification');
    assert.equal(pm001.mailing_address_value, 'unknown');
    assert.equal(pm001.mailing_address_status, 'blocked');
    assert.equal(pm001.phone_value, 'unknown');
    assert.equal(pm001.phone_status, 'blocked');
    assert.equal(
      pm001.notes,
      'still waiting on verified company website, mailing address, and phone'
    );
    assert.equal(pm001.draft_readiness, 'allowed');
    assert.deepEqual(pm002, beforePm002);
    assert.deepEqual(pm003, beforePm003);
    assert.match(answer, /PM-001/);
    assert.match(answer, /PM-002/);
    assert.match(answer, /PM-003/);
    assert.doesNotMatch(answer, /\bop_1\b/);
    assert.doesNotMatch(answer, /\bop_2\b/);
    assert.doesNotMatch(answer, /For PM-001 only:/);
    assert.doesNotMatch(answer, /I found the 2 canary prospects/i);
    void tableTurn;
  });

  it('Test 5b: strict output shape returns only table plus safety line', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: CANARY_WORK_ORDER_PROMPT,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    await workspace.ask({
      sessionId: first.sessionId,
      question: 'Convert the verification work order into a fillable table.',
    });

    const beforeRows =
      workspace._sessions.get(first.sessionId).activeWorkContext.tableRows || [];
    const beforePm002 = { ...beforeRows.find((r) => r.prospect_id === 'PM-002') };
    const beforePm003 = { ...beforeRows.find((r) => r.prospect_id === 'PM-003') };
    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question: [
        'Update the fillable verification table.',
        'For PM-001 only, set:',
        '- website_value: https://www.gamacheproperties.com',
        '- website_status: needs verification',
        '- notes: website candidate added, still needs source confirmation plus verified mailing address and phone',
        '',
        'Leave PM-002 and PM-003 unchanged.',
        'Return only the updated table plus one short preparation-only safety line.',
      ].join('\n'),
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const awc = workspace._sessions.get(result.sessionId).activeWorkContext;
    const rows = awc.tableRows || [];
    const pm001 = rows.find((r) => r.prospect_id === 'PM-001');
    const pm002 = rows.find((r) => r.prospect_id === 'PM-002');
    const pm003 = rows.find((r) => r.prospect_id === 'PM-003');

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(result.structured.metadata.strictOutputShape, true);
    assert.equal(result.structured.metadata.tableUpdate, true);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(result.presentation, 'strict_output_shape');

    assert.match(answer, /\| prospect_id \|/);
    assert.match(answer, /PM-001/);
    assert.match(answer, /PM-002/);
    assert.match(answer, /PM-003/);
    assert.match(
      answer,
      /Preparation-only:\s*no mission created;\s*no launch, approval, print, or mail\.?/i
    );

    assert.doesNotMatch(answer, /^Fillable verification table/m);
    assert.doesNotMatch(answer, /Updated the fillable verification table/i);
    assert.doesNotMatch(answer, /Other rows are unchanged/i);
    assert.doesNotMatch(answer, /Only operator-requested field changes/i);
    assert.doesNotMatch(answer, /Fill website_value, mailing_address_value/i);
    assert.doesNotMatch(answer, /^Reasoning:/m);
    assert.doesNotMatch(answer, /Unavailable in current context/i);
    assert.doesNotMatch(answer, /^Next:/m);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /activeWorkContext/i);
    assert.doesNotMatch(answer, /Handled as a table mutation/i);

    assert.equal(pm001.website_value, 'https://www.gamacheproperties.com');
    assert.equal(pm001.website_status, 'needs verification');
    assert.equal(
      pm001.notes,
      'website candidate added, still needs source confirmation plus verified mailing address and phone'
    );
    assert.deepEqual(pm002, beforePm002);
    assert.deepEqual(pm003, beforePm003);
    assert.equal(result.structured.reasoning.length, 0);
    assert.equal(result.structured.nextInvestigations.length, 0);
    assert.deepEqual(result.structured.metadata.unavailable, []);
  });

  it('Test 6: unknown prospect_id in table update asks for clarification', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: CANARY_WORK_ORDER_PROMPT,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    await workspace.ask({
      sessionId: first.sessionId,
      question: 'Convert the verification work order into a fillable table.',
    });

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question: [
        'Update the fillable verification table.',
        '',
        'For PM-999 only, set:',
        '- notes: bogus row',
        '',
        'Keep this preparation-only.',
        'Do not create a mission.',
      ].join('\n'),
    });

    const answer = result.prose || result.structured.answer || '';
    const awc = workspace._sessions.get(result.sessionId).activeWorkContext;

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.match(answer, /Unknown prospect_id:\s*PM-999/i);
    assert.match(answer, /PM-001/);
    assert.match(answer, /PM-002/);
    assert.match(answer, /PM-003/);
    assert.match(answer, /Preparation-only/i);
    assert.equal(awc.entities.length, 3);
    assert.equal(awc.tableRows.length, 3);
    assert.ok(!awc.entities.some((e) => e.id === 'op_1'));
  });
});

describe('Active work context continuation before domain routing', () => {
  const CANARY_WORK_ORDER_PROMPT = [
    'Continue the Campaign 001 preparation-only canary package.',
    '',
    'Use these 3 prospects:',
    '',
    '1. PM-001 — Gamache Properties — Ben Gamache — Property Management',
    '2. PM-002 — Elm Grove Companies — David Schleyer — Property Management',
    '3. PM-003 — Mill City Property Management — Lauren DuPaul — Property Management',
    '',
    'Turn the 3-prospect canary into a verification work order.',
    'For each prospect, give me exact fields to verify, suggested source type, Ready vs Blocked, what should be logged, and what I should do first.',
    '',
    'Do not create a mission.',
    'Do not launch, execute, approve, print, or mail anything.',
  ].join('\n');

  const CONTINUATION_PROMPT = [
    'Convert the verification work order into a fillable table.',
    'Use the same prospects and keep the same preparation-only constraints.',
  ].join('\n');

  it('Test 1: continuation preempts General Conversation', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: CANARY_WORK_ORDER_PROMPT,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    // Force prior domain to Workspace so a regression would emit a
    // "Switching from Workspace to General Conversation" domainSwitch.
    const session = workspace._sessions.get(first.sessionId);
    session.executionDomain = 'workspace';
    assert.ok(session.activeWorkContext);
    assert.equal(session.activeWorkContext.entities.length, 3);

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question: CONTINUATION_PROMPT,
    });

    const answer = result.prose || result.structured.answer || '';

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.equal(result.executionDomain, 'workspace');
    assert.equal(result.domainSwitch, null);
    assert.doesNotMatch(answer, /Switching from .* to General Conversation/i);
    assert.doesNotMatch(answer, /Policy evaluation is not available/i);
    assert.doesNotMatch(answer, /Switching from Workspace/i);
    assert.match(answer, /PM-001/);
    assert.match(answer, /PM-002/);
    assert.match(answer, /PM-003/);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /prospect_id/);
    assert.match(answer, /contact_role_status/);
    assert.match(answer, /website_status/);
    assert.match(answer, /website_value/);
    assert.match(answer, /mailing_address_status/);
    assert.match(answer, /mailing_address_value/);
    assert.match(answer, /phone_status/);
    assert.match(answer, /phone_value/);
    assert.match(answer, /source_to_check_first/);
    assert.match(answer, /operator_next_action/);
    assert.match(answer, /mail_readiness/);
    assert.match(answer, /draft_readiness/);
    assert.match(answer, /execution_readiness/);
    assert.equal(result.structured.metadata.fillableTable, true);
    assert.equal(result.structured.metadata.activeWorkContextReused, true);
  });

  it('Test 2: no active context falls back to clarification', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const result = await workspace.ask({
      question: CONTINUATION_PROMPT,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const answer = result.prose || result.structured.answer || '';

    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.equal(result.executionDomain, 'workspace');
    assert.equal(result.domainSwitch, null);
    assert.match(answer, /prospect/i);
    assert.doesNotMatch(answer, /Policy evaluation is not available/i);
    assert.doesNotMatch(answer, /Switching from .* to General Conversation/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
  });

  it('Test 3: explicit new work overrides active context and routes to mission', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: CANARY_WORK_ORDER_PROMPT,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    assert.ok(workspace._sessions.get(first.sessionId).activeWorkContext);

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question: 'Build Campaign 001 for Anchor Cleaning.',
    });

    assert.equal(result.route, 'mission');
    assert.ok(result.mission);
    assert.equal(result.mission.title, 'Campaign 001');
    assert.match(result.prose || result.structured.answer || '', /Mission created/i);
  });

  it('stale overnight recommendation must not override active canary table update', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: CANARY_WORK_ORDER_PROMPT,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    await workspace.ask({
      sessionId: first.sessionId,
      question: CONTINUATION_PROMPT,
    });

    const session = workspace._sessions.get(first.sessionId);
    session.executionDomain = 'workspace';
    assert.ok(session.activeWorkContext);
    assert.equal(session.activeWorkContext.lastOutputType, 'fillable_table');
    assert.equal(session.activeWorkContext.entities.length, 3);

    const beforeRows = (session.activeWorkContext.tableRows || []).map((r) => ({
      ...r,
    }));
    const beforePm002 = { ...beforeRows.find((r) => r.prospect_id === 'PM-002') };
    const beforePm003 = { ...beforeRows.find((r) => r.prospect_id === 'PM-003') };
    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question: [
        'Update the fillable verification table.',
        'For PM-001 only, set website_status: verified.',
        'Leave PM-002 and PM-003 unchanged.',
        'Return only the updated table plus one short preparation-only safety line.',
      ].join(' '),
      context: {
        tenantId: '10',
        page: 'command-deck',
        briefing: {
          headline: 'Quiet morning',
          summary: 'No major movement overnight.',
          recommendations: [
            {
              id: 'rec:overnight',
              title: 'What changed overnight?',
            },
          ],
        },
        recommendationId: 'rec:overnight',
        selectedEntity: {
          id: 'rec:overnight',
          type: 'recommendation',
          name: 'What changed overnight?',
        },
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const awc = workspace._sessions.get(result.sessionId).activeWorkContext;
    const rows = awc.tableRows || [];
    const pm001 = rows.find((r) => r.prospect_id === 'PM-001');
    const pm002 = rows.find((r) => r.prospect_id === 'PM-002');
    const pm003 = rows.find((r) => r.prospect_id === 'PM-003');

    assert.equal(result.route, 'intelligence');
    assert.equal(result.executionDomain, 'workspace');
    assert.equal(result.mission, null);
    assert.equal(result.domainSwitch, null);
    assert.equal(result.contextSwitch, null);
    assert.equal(afterMissions.length, beforeMissions.length);

    assert.doesNotMatch(answer, /Switching from .* to General Conversation/i);
    assert.doesNotMatch(answer, /Overnight change counts are not available/i);
    assert.doesNotMatch(answer, /We're now looking at What changed overnight/i);
    assert.doesNotMatch(answer, /Market Intelligence/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /Policy evaluation is not available/i);

    assert.equal(result.structured.metadata.tableUpdate, true);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(result.structured.metadata.fillableTable, true);
    assert.equal(result.structured.metadata.activeWorkContextReused, true);
    assert.ok(
      (result.structured.metadata.updatedProspectIds || []).some(
        (id) => String(id).toUpperCase() === 'PM-001'
      )
    );

    assert.equal(pm001.website_status, 'verified');
    assert.deepEqual(pm002, beforePm002);
    assert.deepEqual(pm003, beforePm003);
    assert.match(answer, /\| prospect_id \|/);
    assert.match(answer, /PM-001/);
    assert.match(answer, /PM-002/);
    assert.match(answer, /PM-003/);
  });

  it('missing activeWorkContext table update asks for table — not briefing fallback', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question: [
        'Update the fillable verification table for PM-001 only:',
        'website_status = verified, website_value = https://www.gamacheproperties.com.',
        'Leave PM-002 and PM-003 unchanged.',
        'Return only the updated table plus one short preparation-only safety line.',
      ].join(' '),
      context: {
        tenantId: '10',
        page: 'command-deck',
        briefing: {
          headline: 'Quiet morning',
          summary: 'No major movement overnight.',
          marketChanges: 0,
          watchAlertCount: 0,
        },
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const session = workspace._sessions.get(result.sessionId);

    assert.equal(result.route, 'intelligence');
    assert.equal(result.executionDomain, 'workspace');
    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(session.activeWorkContext, null);
    assert.equal(result.structured.metadata.tableUpdate, true);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(result.structured.metadata.fillableTable, true);
    assert.equal(result.structured.metadata.missingActiveWorkContext, true);
    assert.equal(result.structured.metadata.route, 'intelligence');
    assert.equal(
      result.structured.metadata.executionDomain,
      'workspace'
    );

    assert.match(
      answer,
      /I can update that, but I don.?t have the current fillable table in this session/i
    );
    assert.match(answer, /Paste the table|paste the 3 Campaign 001 prospects/i);
    assert.match(answer, /PM-001/);
    assert.match(
      answer,
      /Preparation-only:\s*no mission created;\s*no launch, approval, print, or mail/i
    );

    assert.doesNotMatch(answer, /Switching from .* to General Conversation/i);
    assert.doesNotMatch(
      answer,
      /No monitored companies or historical market snapshots/i
    );
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /Overnight change counts are not available/i);
    assert.doesNotMatch(answer, /Market Intelligence/i);
    assert.doesNotMatch(answer, /Policy evaluation is not available/i);
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
