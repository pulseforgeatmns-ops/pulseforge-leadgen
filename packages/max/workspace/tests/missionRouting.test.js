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

/** Slice customer-facing drafts from a packet-review answer (excludes operator caveats). */
function extractPacketReviewCustomerFacingSection(answer) {
  const text = String(answer || '');
  const start = text.search(/---\s*Customer-facing drafts\s*---/i);
  if (start < 0) return '';
  const after = text.slice(start);
  const end = after.search(/\n---\s*Operator caveats\s*---/i);
  return end >= 0 ? after.slice(0, end) : after;
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

  it('Test 5c: inline notes mutation preserves semicolons and free text', async () => {
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

    const notesValue =
      'website verified by operator; phone candidate added but not confirmed; mailing address still missing';

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question: [
        'Update the fillable verification table for PM-001 only:',
        'website_status = verified,',
        'website_value = https://www.gamacheproperties.com,',
        'phone_status = needs verification,',
        'phone_value = 603-555-0198,',
        `notes = ${notesValue}.`,
        'Leave PM-002 and PM-003 unchanged.',
        'Return only the updated table plus one short preparation-only safety line.',
      ].join(' '),
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
    assert.doesNotMatch(answer, /^Reasoning:/m);
    assert.doesNotMatch(answer, /^Next:/m);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);

    assert.equal(pm001.website_status, 'verified');
    assert.equal(pm001.website_value, 'https://www.gamacheproperties.com');
    assert.equal(pm001.phone_status, 'needs verification');
    assert.equal(pm001.phone_value, '603-555-0198');
    assert.equal(pm001.notes, notesValue);
    assert.deepEqual(pm002, beforePm002);
    assert.deepEqual(pm003, beforePm003);
    assert.equal(result.structured.reasoning.length, 0);
    assert.equal(result.structured.nextInvestigations.length, 0);
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

  it('Test 7: verified mail gates reassess PM-001 readiness without prospect sniffing', async () => {
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
        '- website_status = verified',
        '- website_value = https://www.gamacheproperties.com',
        '- phone_status = verified',
        '- phone_value = 603-555-0198',
        '- mailing_address_status = verified',
        '- mailing_address_value = 100 Market St, Manchester NH',
        '- notes = mail gates verified; contact role still needs verification',
        '',
        'Leave PM-002 and PM-003 unchanged.',
        'Reassess PM-001 readiness using the table gates.',
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
    assert.equal(result.structured.metadata.tableUpdate, true);
    assert.equal(result.structured.metadata.strictOutputShape, true);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(result.presentation, 'strict_output_shape');
    assert.ok(
      (result.structured.metadata.reassessedProspectIds || []).some(
        (id) => String(id).toUpperCase() === 'PM-001'
      )
    );

    assert.equal(pm001.website_status, 'verified');
    assert.equal(pm001.phone_status, 'verified');
    assert.equal(pm001.mailing_address_status, 'verified');
    assert.equal(pm001.mail_readiness, 'ready_for_review');
    assert.equal(pm001.execution_readiness, 'blocked');
    assert.equal(pm001.draft_readiness, 'allowed');
    assert.equal(pm001.verification_status, 'needs verification');
    assert.equal(pm001.contact_role_status, 'needs verification');
    assert.doesNotMatch(
      String(pm001.operator_next_action || ''),
      /verify mailing address first/i
    );
    assert.match(
      String(pm001.operator_next_action || ''),
      /contact role|packet|print checklist/i
    );
    assert.deepEqual(pm002, beforePm002);
    assert.deepEqual(pm003, beforePm003);

    assert.match(answer, /\| prospect_id \|/);
    assert.match(answer, /ready_for_review/);
    assert.match(
      answer,
      /Preparation-only:\s*no mission created;\s*no launch, approval, print, or mail\.?/i
    );
    assert.doesNotMatch(answer, /^Fillable verification table/m);
    assert.doesNotMatch(answer, /Prospect List Detected/i);
    assert.doesNotMatch(answer, /\bop_1\b/);
    assert.doesNotMatch(answer, /For PM-001 only:/);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.equal(awc.entities.length, 3);
    assert.ok(!awc.entities.some((e) => String(e.id || '').startsWith('op_')));
  });

  it('Test 8: verified contact_role reassess clears stale verification next-action', async () => {
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

    await workspace.ask({
      sessionId: first.sessionId,
      question: [
        'Update the fillable verification table.',
        'For PM-001 only, set:',
        '- website_status = verified',
        '- website_value = https://www.gamacheproperties.com',
        '- phone_status = verified',
        '- phone_value = 603-555-0198',
        '- mailing_address_status = verified',
        '- mailing_address_value = 100 Market St, Manchester NH',
        '- notes = mail gates verified; contact role still needs verification',
        '',
        'Leave PM-002 and PM-003 unchanged.',
        'Reassess PM-001 readiness using the table gates.',
        'Return only the updated table plus one short preparation-only safety line.',
      ].join('\n'),
    });

    const beforeRows =
      workspace._sessions.get(first.sessionId).activeWorkContext.tableRows || [];
    const beforePm001 = { ...beforeRows.find((r) => r.prospect_id === 'PM-001') };
    const beforePm002 = { ...beforeRows.find((r) => r.prospect_id === 'PM-002') };
    const beforePm003 = { ...beforeRows.find((r) => r.prospect_id === 'PM-003') };
    assert.equal(beforePm001.contact_role_status, 'needs verification');
    assert.equal(beforePm001.verification_status, 'needs verification');
    assert.match(String(beforePm001.operator_next_action || ''), /contact role/i);

    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question: [
        'Update the fillable verification table.',
        'For PM-001 only, set:',
        '- contact_role_status = verified',
        '',
        'Leave PM-002 and PM-003 unchanged.',
        'Reassess the Campaign 001 canary table.',
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
    assert.equal(result.structured.metadata.tableUpdate, true);
    assert.equal(result.structured.metadata.strictOutputShape, true);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(result.presentation, 'strict_output_shape');
    assert.ok(
      (result.structured.metadata.reassessedProspectIds || []).some(
        (id) => String(id).toUpperCase() === 'PM-001'
      )
    );

    assert.equal(pm001.website_status, 'verified');
    assert.equal(pm001.mailing_address_status, 'verified');
    assert.equal(pm001.phone_status, 'verified');
    assert.equal(pm001.contact_role_status, 'verified');
    assert.notEqual(
      String(pm001.verification_status || '').toLowerCase(),
      'needs verification'
    );
    assert.match(
      String(pm001.verification_status || ''),
      /^(verified|ready_for_review)$/i
    );
    assert.doesNotMatch(
      String(pm001.operator_next_action || ''),
      /confirm contact role/i
    );
    assert.match(
      String(pm001.operator_next_action || ''),
      /packet|print checklist|operator packet review/i
    );
    assert.equal(pm001.mail_readiness, 'ready_for_review');
    assert.equal(pm001.execution_readiness, 'blocked');
    assert.equal(pm001.draft_readiness, 'allowed');
    assert.deepEqual(pm002, beforePm002);
    assert.deepEqual(pm003, beforePm003);

    assert.match(answer, /\| prospect_id \|/);
    assert.match(
      answer,
      /Preparation-only:\s*no mission created;\s*no launch, approval, print, or mail\.?/i
    );
    assert.doesNotMatch(answer, /^Fillable verification table/m);
    assert.doesNotMatch(answer, /Prospect List Detected/i);
    assert.doesNotMatch(answer, /\bop_1\b/);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.equal(awc.entities.length, 3);
  });

  it('Test 9: mailing downgrade reassess preserves other verified source gates', async () => {
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

    await workspace.ask({
      sessionId: first.sessionId,
      question: [
        'Update the fillable verification table.',
        'For PM-001 only, set:',
        '- website_status = verified',
        '- website_value = https://www.gamacheproperties.com',
        '- phone_status = verified',
        '- phone_value = 603-555-0198',
        '- mailing_address_status = verified',
        '- mailing_address_value = 100 Market St, Manchester NH',
        '- contact_role_status = verified',
        '',
        'Leave PM-002 and PM-003 unchanged.',
        'Reassess the Campaign 001 canary table.',
      ].join('\n'),
    });

    const beforeRows =
      workspace._sessions.get(first.sessionId).activeWorkContext.tableRows || [];
    const beforePm001 = { ...beforeRows.find((r) => r.prospect_id === 'PM-001') };
    const beforePm002 = { ...beforeRows.find((r) => r.prospect_id === 'PM-002') };
    const beforePm003 = { ...beforeRows.find((r) => r.prospect_id === 'PM-003') };
    assert.equal(beforePm001.website_status, 'verified');
    assert.equal(beforePm001.phone_status, 'verified');
    assert.equal(beforePm001.mailing_address_status, 'verified');
    assert.equal(beforePm001.contact_role_status, 'verified');
    assert.equal(beforePm001.verification_status, 'verified');

    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question: [
        'Update the fillable verification table.',
        'For PM-001 only, set mailing_address_status = needs verification, notes = website, phone, and contact role remain verified.',
        'Leave PM-002 and PM-003 unchanged.',
        'Reassess the Campaign 001 canary table.',
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
    assert.equal(result.structured.metadata.tableUpdate, true);
    assert.equal(result.structured.metadata.strictOutputShape, true);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(result.presentation, 'strict_output_shape');

    // Only the mailing source gate changed.
    assert.equal(pm001.mailing_address_status, 'needs verification');
    assert.equal(pm001.contact_role_status, 'verified');
    assert.equal(pm001.website_status, 'verified');
    assert.equal(pm001.phone_status, 'verified');
    assert.equal(
      pm001.website_value,
      'https://www.gamacheproperties.com'
    );
    assert.equal(pm001.phone_value, '603-555-0198');
    assert.match(
      String(pm001.notes || ''),
      /website,\s*phone,\s*and contact role remain verified/i
    );

    // Derived readiness recalculated from gates.
    assert.equal(pm001.verification_status, 'needs verification');
    assert.match(
      String(pm001.mail_readiness || ''),
      /^(blocked|needs_verification|needs verification)$/i
    );
    assert.match(
      String(pm001.operator_next_action || ''),
      /mailing address/i
    );
    assert.equal(pm001.execution_readiness, 'blocked');
    assert.equal(pm001.draft_readiness, 'allowed');

    assert.deepEqual(pm002, beforePm002);
    assert.deepEqual(pm003, beforePm003);

    assert.match(answer, /\| prospect_id \|/);
    assert.match(
      answer,
      /Preparation-only:\s*no mission created;\s*no launch, approval, print, or mail\.?/i
    );
    assert.doesNotMatch(answer, /^Fillable verification table/m);
    assert.doesNotMatch(answer, /Prospect List Detected/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.equal(awc.entities.length, 3);
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

  it('fillable table responses surface workflow chips — not briefing defaults', async () => {
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
      question: CONTINUATION_PROMPT,
    });

    const chips = result.suggestions || result.structured.nextInvestigations || [];
    const awc = workspace._sessions.get(result.sessionId).activeWorkContext;

    assert.equal(awc.workflow, 'campaign_canary');
    assert.equal(awc.lastOutputType, 'fillable_table');
    assert.ok(chips.length >= 3, 'expected workflow suggestion chips');
    assert.ok(
      chips.some((s) => /blocked prospects|verification field|packet review|changed in this table|blocks mailing|Draft PM-/i.test(s)),
      `expected canary/table chips, got: ${JSON.stringify(chips)}`
    );
    assert.ok(!chips.some((s) => /What changed overnight/i.test(s)));
    assert.ok(!chips.some((s) => /top opportunity ranked first/i.test(s)));
    assert.ok(!chips.some((s) => /Why is .+ #1/i.test(s)));
    assert.ok(
      !chips.some((s) =>
        /^(?:Mail|Launch|Execute|Approve|Print)\b/i.test(String(s))
      )
    );
  });

  it('askWorkspace personalization keeps workflow chips after fillable table', async () => {
    const { createMaxReasoningRuntime } = require('../../index');
    const missionEngine = testMissionEngine();
    const max = createMaxReasoningRuntime({
      disableLlm: true,
      missionEngine,
      missionsEnabled: true,
    });

    const first = await max.askWorkspace({
      question: CANARY_WORK_ORDER_PROMPT,
      context: {
        tenantId: '10',
        page: 'command-deck',
        briefing: {
          headline: 'Quiet morning',
          summary: 'No major movement overnight.',
          watchAlertCount: 1,
        },
      },
    });

    const result = await max.askWorkspace({
      sessionId: first.sessionId,
      question: CONTINUATION_PROMPT,
      context: {
        tenantId: '10',
        page: 'command-deck',
        briefing: {
          headline: 'Quiet morning',
          summary: 'No major movement overnight.',
        },
        recommendationId: 'rec:overnight',
        selectedEntity: {
          id: 'rec:overnight',
          type: 'recommendation',
          name: 'What changed overnight?',
        },
      },
    });

    const chips = result.suggestions || [];
    const session = max.workspace.sessions.get(result.sessionId);

    assert.equal(session.activeWorkContext.lastOutputType, 'fillable_table');
    assert.ok(chips.length >= 3, `expected chips, got ${JSON.stringify(chips)}`);
    assert.ok(
      chips.some((s) =>
        /blocked prospects|verification field|packet review|changed in this table|blocks mailing|Draft PM-/i.test(
          s
        )
      ),
      `expected canary/table chips after askWorkspace personalization, got: ${JSON.stringify(chips)}`
    );
    assert.ok(!chips.some((s) => /What changed overnight/i.test(s)));
    assert.ok(!chips.some((s) => /top opportunity ranked first/i.test(s)));
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

  it('packet review reuses canary table for PM-001 — no prospect parse fallback', async () => {
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

    await workspace.ask({
      sessionId: first.sessionId,
      question: [
        'Update the fillable verification table.',
        'For PM-001 only, set:',
        '- website_status = verified',
        '- website_value = https://www.gamacheproperties.com',
        '- phone_status = verified',
        '- phone_value = 603-555-0198',
        '- mailing_address_status = verified',
        '- mailing_address_value = 100 Market St, Manchester NH',
        '- contact_role_status = verified',
        '',
        'Leave PM-002 and PM-003 unchanged.',
        'Reassess PM-001 readiness using the table gates.',
        'Return only the updated table plus one short preparation-only safety line.',
      ].join('\n'),
    });

    const beforeRows = (
      workspace._sessions.get(first.sessionId).activeWorkContext.tableRows || []
    ).map((r) => ({ ...r }));
    const beforePm001 = { ...beforeRows.find((r) => r.prospect_id === 'PM-001') };
    const beforePm002 = { ...beforeRows.find((r) => r.prospect_id === 'PM-002') };
    const beforePm003 = { ...beforeRows.find((r) => r.prospect_id === 'PM-003') };
    assert.equal(beforePm001.mail_readiness, 'ready_for_review');
    assert.equal(beforePm001.execution_readiness, 'blocked');

    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question: [
        'Create a preparation-only packet review checklist for PM-001.',
        'Use the current Campaign 001 canary table.',
        'PM-001 is ready_for_review for mail readiness, but execution_readiness is still blocked.',
        'Include packet contents, print/sign/mail checklist, fields to confirm before printing,',
        'personalized letter draft, handwritten note draft, scorecard cover text,',
        'first follow-up call notes, tracking fields after mailing,',
        'and the final operator decision needed before anything is mailed.',
        'Use only known table facts. Do not invent evidence.',
        'Do not create a mission. Do not launch, execute, approve, print, or mail anything.',
      ].join(' '),
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
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(result.structured.metadata.packetReview, true);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(result.structured.metadata.activeWorkContextReused, true);
    assert.equal(result.structured.metadata.tableUpdate, false);
    assert.equal(result.structured.metadata.prospectId, 'PM-001');
    assert.equal(result.structured.metadata.executionReadiness, 'blocked');
    assert.equal(awc.lastOutputType, 'packet_review');

    assert.doesNotMatch(
      answer,
      /I see you intended to provide \d+ canary prospects/i
    );
    assert.doesNotMatch(answer, /could not parse them cleanly/i);
    assert.doesNotMatch(answer, /Paste \d+ canary prospects/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /Switching from .* to General Conversation/i);

    assert.match(answer, /packet (?:contents )?checklist/i);
    assert.match(answer, /print\s*\/\s*sign\s*\/\s*mail checklist/i);
    assert.match(answer, /Fields to confirm before printing/i);
    assert.match(answer, /Personalized letter draft/i);
    assert.match(answer, /Handwritten note draft/i);
    assert.match(answer, /Scorecard cover text draft/i);
    assert.match(answer, /follow-up call notes/i);
    assert.match(answer, /tracking fields to log after mailing/i);
    assert.match(answer, /Final operator decision/i);
    assert.match(answer, /PM-001/);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /ready_for_review/);
    assert.match(answer, /Execution readiness:\s*blocked/i);
    assert.match(answer, /Preparation-only/);
    assert.match(answer, /100 Market St/);
    assert.match(answer, /Ben,/);
    assert.doesNotMatch(answer, /draft held/i);
    assert.doesNotMatch(answer, /\b\d+\s+units?\b/i);
    assert.doesNotMatch(answer, /portfolio of \d+/i);

    assert.deepEqual(pm001, beforePm001);
    assert.deepEqual(pm002, beforePm002);
    assert.deepEqual(pm003, beforePm003);
  });

  it('pasted fillable verification table + PM-001 packet review in same message', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const tableMarkdown = [
      '| prospect_id | company_name | contact_name | contact_role_status | website_status | website_value | mailing_address_status | mailing_address_value | phone_status | phone_value | verification_status | mail_readiness | draft_readiness | execution_readiness | operator_next_action | notes |',
      '| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |',
      '| PM-001 | Gamache Properties | Ben Gamache | verified | verified | https://www.gamacheproperties.com | verified | 100 Market St, Manchester NH | verified | 603-555-0198 | needs verification | ready_for_review | allowed | blocked | review packet contents / prepare print checklist | |',
      '| PM-002 | Elm Grove Companies | David Schleyer | needs verification | needs verification | unknown | blocked | unknown | needs verification | unknown | needs verification | blocked | allowed | blocked | verify mailing address first | |',
      '| PM-003 | Mill City Property Management | Lauren DuPaul | needs verification | needs verification | unknown | blocked | unknown | needs verification | unknown | needs verification | blocked | allowed | blocked | verify mailing address first | |',
    ].join('\n');

    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question: [
        'Campaign 001 preparation-only canary table:',
        '',
        tableMarkdown,
        '',
        'Create a preparation-only packet review checklist for PM-001.',
        'Use only known table facts. Do not invent evidence.',
        'Do not create a mission. Do not launch, execute, approve, print, or mail anything.',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const awc = workspace._sessions.get(result.sessionId).activeWorkContext;
    const rows = awc.tableRows || [];
    const pm001 = rows.find((r) => r.prospect_id === 'PM-001');
    const pm002 = rows.find((r) => r.prospect_id === 'PM-002');
    const pm003 = rows.find((r) => r.prospect_id === 'PM-003');

    assert.ok(awc, 'activeWorkContext populated from pasted table');
    assert.equal(awc.workflow, 'campaign_001_preparation_only_canary');
    assert.ok(
      awc.lastOutputKind === 'fillable_verification_table' ||
        awc.lastOutputType === 'packet_review' ||
        awc.lastOutputType === 'fillable_table'
    );
    assert.equal(rows.length, 3);
    assert.equal(pm001.company_name, 'Gamache Properties');
    assert.equal(pm001.mail_readiness, 'ready_for_review');
    assert.equal(pm001.execution_readiness, 'blocked');
    assert.equal(pm001.website_value, 'https://www.gamacheproperties.com');
    assert.equal(pm001.mailing_address_value, '100 Market St, Manchester NH');
    assert.equal(pm001.phone_value, '603-555-0198');
    assert.equal(pm002.company_name, 'Elm Grove Companies');
    assert.equal(pm003.company_name, 'Mill City Property Management');

    assert.equal(result.route, 'intelligence');
    assert.equal(result.executionDomain, 'workspace');
    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(result.structured.metadata.packetReview, true);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(result.structured.metadata.tableUpdate, false);
    assert.equal(result.structured.metadata.prospectId, 'PM-001');
    assert.equal(result.structured.metadata.executionReadiness, 'blocked');
    assert.equal(awc.lastOutputType, 'packet_review');

    assert.doesNotMatch(
      answer,
      /I don.?t have an active Campaign canary table/i
    );
    assert.doesNotMatch(answer, /paste the table\/prospects first/i);
    assert.doesNotMatch(answer, /Prospect List Detected/i);
    assert.doesNotMatch(
      answer,
      /I see you intended to provide \d+ canary prospects/i
    );
    assert.doesNotMatch(answer, /could not parse them cleanly/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);

    assert.match(answer, /packet (?:contents )?checklist/i);
    assert.match(answer, /print\s*\/\s*sign\s*\/\s*mail checklist/i);
    assert.match(answer, /PM-001/);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /ready_for_review/);
    assert.match(
      answer,
      /Execution readiness:\s*blocked/i
    );
    assert.match(answer, /Preparation-only/);
    assert.match(answer, /100 Market St/);

    // Provisional drafts must be returned, not held, when company+contact exist.
    assert.equal(result.structured.metadata.draftConfidence, 'low');
    assert.match(answer, /Draft confidence:\s*low/i);
    assert.match(answer, /Customer-facing drafts/i);
    assert.match(answer, /Operator caveats/i);
    assert.match(answer, /Missing personalization evidence/i);
    assert.match(answer, /Final operator decision required/i);
    assert.match(answer, /Personalized letter draft \(provisional/i);
    assert.match(answer, /Ben,/);
    assert.match(
      answer,
      /I included a short scorecard packet for Gamache Properties as a quick review item/i
    );
    assert.match(answer, /Handwritten note draft \(provisional/i);
    assert.match(
      answer,
      /Ben — included a short scorecard for Gamache Properties\. Thought it may be useful as a quick review item/i
    );
    assert.match(answer, /Scorecard cover text draft \(provisional/i);
    assert.match(answer, /Operational scorecard — Gamache Properties/i);
    assert.match(answer, /603-555-0198/);
    assert.match(answer, /Use verified phone 603-555-0198/i);
    assert.match(answer, /Confirm Ben is the right contact/i);
    assert.match(answer, /Reference the packet only after it is actually mailed/i);
    assert.match(answer, /Do not claim industry-specific context/i);
    assert.match(answer, /Log outcome/i);
    assert.match(answer, /Execution readiness:\s*blocked \(remains blocked\)/i);
    assert.doesNotMatch(answer, /best reach number/i);
    assert.doesNotMatch(answer, /draft held/i);
    assert.doesNotMatch(
      answer,
      /company, contact, and industry are required before personalizing/i
    );
    assert.match(answer, /industry \/ persona context \(missing from table/i);
    assert.match(answer, /Industry\/persona evidence:\s*not provided/i);
    assert.doesNotMatch(answer, /^Industry:\s*unknown/m);
    assert.doesNotMatch(answer, /\b\d+\s+units?\b/i);
    assert.doesNotMatch(answer, /portfolio of \d+/i);
    assert.doesNotMatch(answer, /appears to be in (?:property|unknown)/i);
    assert.equal(result.structured.metadata.strictOutputShape, true);
    assert.doesNotMatch(answer, /^Reasoning:/m);
    assert.doesNotMatch(answer, /Unavailable in current context/i);
    assert.doesNotMatch(answer, /^Next:/m);

    // Customer-facing letter / note / cover must not leak operator language
    // or invent unsupported pain/value claims.
    const customerSection = extractPacketReviewCustomerFacingSection(answer);
    assert.ok(customerSection.length > 0, 'expected customer-facing drafts section');
    assert.doesNotMatch(customerSection, /preparation-only/i);
    assert.doesNotMatch(customerSection, /execution remains blocked/i);
    assert.doesNotMatch(customerSection, /ready_for_review/i);
    assert.doesNotMatch(customerSection, /Industry not yet confirmed/i);
    assert.doesNotMatch(customerSection, /industry(?:\/persona)? not on table/i);
    assert.doesNotMatch(customerSection, /without assuming/i);
    assert.doesNotMatch(customerSection, /Draft confidence/i);
    assert.doesNotMatch(customerSection, /I(?:'|’)d like to send you/i);
    assert.doesNotMatch(customerSection, /provisional review cover/i);
    assert.doesNotMatch(customerSection, /Status: Packet ready_for_review/i);
    assert.doesNotMatch(customerSection, /Evidence note:/i);
    assert.doesNotMatch(customerSection, /Operator note on file/i);
    assert.doesNotMatch(customerSection, /vendor coordination/i);
    assert.doesNotMatch(customerSection, /may need attention/i);
    assert.doesNotMatch(customerSection, /slipping through/i);
    assert.doesNotMatch(customerSection, /pain point/i);
    assert.match(
      customerSection,
      /If useful, I(?:'|’)d be glad to walk through it after you(?:'|’)ve had a chance to review/i
    );

    // Packet review must not mutate the ingested table rows.
    assert.equal(pm001.prospect_id, 'PM-001');
    assert.equal(pm001.company_name, 'Gamache Properties');
    assert.equal(pm001.contact_name, 'Ben Gamache');
    assert.equal(pm001.mail_readiness, 'ready_for_review');
    assert.equal(pm001.execution_readiness, 'blocked');
    assert.equal(pm001.website_value, 'https://www.gamacheproperties.com');
    assert.equal(pm001.mailing_address_value, '100 Market St, Manchester NH');
    assert.equal(pm001.phone_value, '603-555-0198');
    assert.equal(String(pm001.notes || ''), '');
    assert.equal(pm002.mail_readiness, 'blocked');
    assert.equal(pm003.execution_readiness, 'blocked');
  });

  it('inline known facts fallback builds PM-001 packet review without active table', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question: [
        'Create a preparation-only packet review package for PM-001 using the current Campaign 001 canary table.',
        'I do not have the full table pasted; use these known facts:',
        '',
        '- prospect_id: PM-001',
        '- company_name: Gamache Properties',
        '- contact_name: Ben Gamache',
        '- website_status: verified',
        '- website_value: https://www.gamacheproperties.com',
        '- mailing_address_status: verified',
        '- mailing_address_value: 100 Market St, Manchester NH',
        '- phone_status: verified',
        '- phone_value: 603-555-0198',
        '- contact_role_status: verified',
        '- mail_readiness: ready_for_review',
        '- draft_readiness: allowed',
        '- execution_readiness: blocked',
        '- notes: operator-supplied known facts after refresh',
        '',
        'Use only the known facts above. Do not invent evidence.',
        'Do not create a mission. Do not launch, execute, approve, print, or mail anything.',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const session = workspace._sessions.get(result.sessionId);
    const awc = session && session.activeWorkContext;

    assert.equal(result.route, 'intelligence');
    assert.equal(result.executionDomain, 'workspace');
    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(result.structured.metadata.packetReview, true);
    assert.equal(result.structured.metadata.canaryPreparationOnly, true);
    assert.equal(result.structured.metadata.inlineKnownFacts, true);
    assert.equal(result.structured.metadata.activeWorkContextReused, false);
    assert.equal(result.structured.metadata.tableUpdate, false);
    assert.equal(result.structured.metadata.prospectId, 'PM-001');
    assert.equal(result.structured.metadata.mailReadiness, 'ready_for_review');
    assert.equal(result.structured.metadata.executionReadiness, 'blocked');
    assert.equal(result.structured.metadata.draftConfidence, 'low');

    // Temporary packetReviewContext only — do not invent a desk table.
    assert.ok(
      !awc ||
        !Array.isArray(awc.tableRows) ||
        awc.tableRows.length === 0 ||
        awc.lastOutputType !== 'fillable_table'
    );

    assert.doesNotMatch(
      answer,
      /I don.?t have an active Campaign canary table/i
    );
    assert.doesNotMatch(answer, /paste the table\/prospects first/i);
    assert.doesNotMatch(answer, /paste the table/i);
    assert.doesNotMatch(
      answer,
      /I see you intended to provide \d+ canary prospects/i
    );
    assert.doesNotMatch(answer, /could not parse them cleanly/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /Switching from .* to General Conversation/i);

    assert.match(answer, /packet (?:contents )?checklist/i);
    assert.match(answer, /print\s*\/\s*sign\s*\/\s*mail checklist/i);
    assert.match(answer, /Fields to confirm before printing/i);
    assert.match(answer, /Personalized letter draft/i);
    assert.match(answer, /Handwritten note draft/i);
    assert.match(answer, /Scorecard cover text draft/i);
    assert.match(answer, /follow-up call notes/i);
    assert.match(answer, /tracking fields to log after mailing/i);
    assert.match(answer, /Final operator decision/i);
    assert.match(answer, /PM-001/);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /ready_for_review/);
    assert.match(
      answer,
      /Execution readiness:\s*blocked/i
    );
    assert.match(answer, /Preparation-only/);
    assert.match(answer, /100 Market St/);
    assert.match(answer, /Ben,/);
    assert.doesNotMatch(answer, /draft held/i);
    assert.doesNotMatch(answer, /\b\d+\s+units?\b/i);
    assert.doesNotMatch(answer, /portfolio of \d+/i);

    assert.equal(result.structured.metadata.strictOutputShape, true);
    assert.match(answer, /Industry\/persona evidence:\s*not provided/i);
    assert.doesNotMatch(answer, /^Industry:\s*unknown/m);
    assert.doesNotMatch(answer, /- industry:\s*unknown/i);
    assert.match(answer, /Use verified phone 603-555-0198/i);
    assert.match(answer, /Confirm Ben is the right contact/i);
    assert.match(answer, /Reference the packet only after it is actually mailed/i);
    assert.match(answer, /Do not claim industry-specific context/i);
    assert.match(answer, /Log outcome/i);
    assert.doesNotMatch(answer, /^Reasoning:/m);
    assert.doesNotMatch(answer, /Unavailable in current context/i);
    assert.doesNotMatch(answer, /^Next:/m);
    assert.match(
      answer,
      /Preparation-only\.\s*No mission created\.\s*No launch, execution, approval, print, or mail/i
    );

    const customerSection = extractPacketReviewCustomerFacingSection(answer);
    assert.ok(customerSection.length > 0, 'expected customer-facing drafts section');
    assert.doesNotMatch(customerSection, /preparation-only/i);
    assert.doesNotMatch(customerSection, /execution remains blocked/i);
    assert.doesNotMatch(customerSection, /ready_for_review/i);
    assert.doesNotMatch(customerSection, /Draft confidence/i);
    assert.doesNotMatch(customerSection, /operator-supplied known facts/i);
    assert.doesNotMatch(customerSection, /vendor coordination/i);
    assert.doesNotMatch(customerSection, /may need attention/i);
    assert.doesNotMatch(customerSection, /slipping through/i);
    assert.doesNotMatch(customerSection, /pain point/i);
    assert.doesNotMatch(customerSection, /portfolio/i);
    assert.match(
      customerSection,
      /I included a short scorecard packet for Gamache Properties as a quick review item/i
    );
    assert.match(
      customerSection,
      /If useful, I(?:'|’)d be glad to walk through it after you(?:'|’)ve had a chance to review/i
    );
  });

  it('inline known facts packet review asks only for missing required fields', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question: [
        'Create a preparation-only packet review checklist for PM-001.',
        'Known facts:',
        '- prospect_id: PM-001',
        '- company_name: Gamache Properties',
        '- mail_readiness: ready_for_review',
        'Do not create a mission.',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';

    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(result.structured.metadata.packetReview, true);
    assert.equal(result.structured.metadata.inlineKnownFacts, true);
    assert.deepEqual(
      result.structured.metadata.missingRequiredFields.sort(),
      ['contact_name', 'execution_readiness'].sort()
    );

    assert.match(answer, /Still need:/i);
    assert.match(answer, /contact_name/i);
    assert.match(answer, /execution_readiness/i);
    assert.match(answer, /do not need to paste the full/i);
    assert.doesNotMatch(
      answer,
      /I don.?t have an active Campaign canary table/i
    );
    assert.doesNotMatch(
      answer,
      /Continue from a session with the fillable verification table/i
    );
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /Personalized letter draft/i);
  });

  it('Known facts available bullets parse through Return/copy-rule sections', async () => {
    const {
      parseInlinePacketReviewKnownFacts,
    } = require('../ActiveWorkContext');

    const question = [
      'Create a preparation-only packet review checklist for PM-001.',
      '',
      'Known facts available:',
      '- prospect_id: PM-001',
      '- company_name: Gamache Properties',
      '- contact_name: Ben Gamache',
      '- website_status: verified',
      '- website_value: https://www.gamacheproperties.com',
      '- mailing_address_status: verified',
      '- mailing_address_value: 123 Main Street, Manchester, NH 03101',
      '- phone_status: verified',
      '- phone_value: 603-555-0198',
      '- contact_role_status: verified',
      '- mail_readiness: ready_for_review',
      '- draft_readiness: allowed',
      '- execution_readiness: blocked',
      '- notes: mailing address reconfirmed; website, phone, and contact role remain verified',
      '',
      'Return:',
      '- Personalized letter draft',
      '- Handwritten note draft',
      '- Scorecard cover text draft',
      '- First follow-up call notes',
      '- company_name: use only known fact value',
      '- contact_name: use only known fact value',
      '- mail_readiness: do not put in customer letter',
      '- execution_readiness: never authorize',
      '',
      'Customer-facing copy rules:',
      '- Keep drafts generic when industry is missing',
      '- Do not invent vendor coordination or pain claims',
      '',
      'Operator-facing rules:',
      '- Industry/persona evidence: not provided',
      '- Keep low-confidence caveat in operator section only',
      '',
      'Do not invent industry.',
      'Do not create a mission. Do not launch, execute, approve, print, or mail.',
    ].join('\n');

    const parsed = parseInlinePacketReviewKnownFacts(question);
    assert.equal(parsed.hasInlineFacts, true);
    assert.deepEqual(parsed.missingRequired, []);
    assert.equal(parsed.row.company_name, 'Gamache Properties');
    assert.equal(parsed.row.contact_name, 'Ben Gamache');
    assert.equal(parsed.row.mail_readiness, 'ready_for_review');
    assert.equal(parsed.row.execution_readiness, 'blocked');
    assert.equal(
      parsed.row.mailing_address_value,
      '123 Main Street, Manchester, NH 03101'
    );
    assert.equal(
      parsed.row.website_value,
      'https://www.gamacheproperties.com'
    );
    assert.equal(
      parsed.row.notes,
      'mailing address reconfirmed; website, phone, and contact role remain verified'
    );
    assert.equal(parsed.row.industry, undefined);
    assert.doesNotMatch(String(parsed.row.company_name), /use only known fact/i);

    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });
    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';

    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(result.structured.metadata.packetReview, true);
    assert.equal(result.structured.metadata.inlineKnownFacts, true);
    assert.equal(result.structured.metadata.missingRequiredFields, undefined);
    assert.equal(result.structured.metadata.draftConfidence, 'low');
    assert.doesNotMatch(answer, /Still need:/i);
    assert.match(answer, /packet (?:contents )?checklist/i);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /Ben Gamache|Ben,/);
    assert.match(answer, /Industry\/persona evidence:\s*not provided/i);
    assert.doesNotMatch(answer, /appears to be in/i);
    assert.doesNotMatch(answer, /Property Management/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /use only known fact value/i);
    assert.doesNotMatch(answer, /never authorize/i);
  });

  it('Do not include Reasoning/Unavailable/Next suppresses packet-review scaffolding', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });
    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question: [
        'Create a preparation-only packet review checklist for PM-001.',
        '',
        'Known facts available:',
        '- prospect_id: PM-001',
        '- company_name: Gamache Properties',
        '- contact_name: Ben Gamache',
        '- website_status: verified',
        '- website_value: https://www.gamacheproperties.com',
        '- mailing_address_status: verified',
        '- mailing_address_value: 123 Main Street, Manchester, NH 03101',
        '- phone_status: verified',
        '- phone_value: 603-555-0198',
        '- contact_role_status: verified',
        '- mail_readiness: ready_for_review',
        '- draft_readiness: allowed',
        '- execution_readiness: blocked',
        '- notes: mailing address reconfirmed; website, phone, and contact role remain verified',
        '',
        'Return only the packet review artifact.',
        'Do not include Reasoning, Unavailable context, or Next sections.',
        'Do not invent industry.',
        'Do not create a mission. Do not launch, execute, approve, print, or mail.',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';

    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(result.structured.metadata.packetReview, true);
    assert.equal(result.structured.metadata.inlineKnownFacts, true);
    assert.equal(result.structured.metadata.strictOutputShape, true);
    assert.equal(result.presentation, 'strict_output_shape');
    assert.deepEqual(result.structured.reasoning, []);
    assert.deepEqual(result.structured.nextInvestigations, []);
    assert.deepEqual(result.structured.metadata.unavailable, []);

    assert.match(answer, /packet (?:contents )?checklist/i);
    assert.match(answer, /Gamache Properties/);
    assert.match(
      answer,
      /Preparation-only\.\s*No mission created\.\s*No launch, execution, approval, print, or mail/i
    );
    assert.doesNotMatch(answer, /^Reasoning:/m);
    assert.doesNotMatch(answer, /Unavailable in current context/i);
    assert.doesNotMatch(answer, /^Next:/m);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /Built packet review from inline known facts/i);
  });

  it('blocked PM-002 packet review shows why-blocked and pre-mail verification plan', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });
    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question: [
        'Create a preparation-only packet review checklist for PM-002.',
        '',
        'Known facts available:',
        '- prospect_id: PM-002',
        '- company_name: Elm Grove Companies',
        '- contact_name: David Schleyer',
        '- website_status: blocked',
        '- website_value: unknown',
        '- mailing_address_status: blocked',
        '- mailing_address_value: unknown',
        '- phone_status: blocked',
        '- phone_value: unknown',
        '- contact_role_status: needs verification',
        '- mail_readiness: blocked',
        '- draft_readiness: allowed',
        '- execution_readiness: blocked',
        '',
        'Do not include Reasoning, Unavailable context, or Next sections.',
        'Do not invent industry.',
        'Do not create a mission. Do not launch, execute, approve, print, or mail.',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';

    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(result.structured.metadata.packetReview, true);
    assert.equal(result.structured.metadata.inlineKnownFacts, true);
    assert.equal(result.structured.metadata.mailReadiness, 'blocked');
    assert.equal(result.structured.metadata.executionReadiness, 'blocked');
    assert.equal(result.structured.metadata.strictOutputShape, true);

    assert.match(answer, /Why blocked for mailing:/i);
    assert.match(answer, /mailing address unknown\/blocked/i);
    assert.match(answer, /website unknown\/blocked/i);
    assert.match(answer, /phone unknown\/blocked/i);
    assert.match(answer, /contact role needs verification/i);

    assert.match(
      answer,
      /Print\s*\/\s*sign\s*\/\s*mail checklist:\s*not available until verification is complete/i
    );
    assert.match(answer, /Future print\s*\/\s*sign\s*\/\s*mail checklist/i);
    assert.match(answer, /Pre-mail verification plan/i);
    assert.match(
      answer,
      /Verify (?:mailing address|phone).*trusted source or CRM/i
    );
    assert.match(answer, /do not call until a verified number exists/i);
    assert.doesNotMatch(answer, /First follow-up call notes/i);
    assert.doesNotMatch(answer, /Reference the packet only after it is actually mailed/i);

    assert.match(answer, /current_mail_readiness:\s*blocked/i);
    assert.match(answer, /mail_readiness_at_send:\s*\(set when mailed\)/i);
    assert.doesNotMatch(answer, /mail_readiness_at_send:\s*blocked/i);

    assert.match(answer, /do not print\/mail until fields are verified/i);
    assert.match(answer, /Execution readiness:\s*blocked/i);
    assert.match(
      answer,
      /Preparation-only\.\s*No mission created\.\s*No launch, execution, approval, print, or mail/i
    );
    assert.doesNotMatch(answer, /^Reasoning:/m);
    assert.doesNotMatch(answer, /Unavailable in current context/i);
    assert.doesNotMatch(answer, /^Next:/m);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /appears to be in/i);

    const chips = result.suggestions || [];
    assert.ok(chips.length >= 3, `expected packet-review chips, got ${JSON.stringify(chips)}`);
    assert.ok(
      chips.some((s) =>
        /missing verification fields|verification plan|readiness fields|packet checklist for another prospect|final operator decision|still blocks mailing/i.test(
          s
        )
      ),
      `expected packet/canary chips, got: ${JSON.stringify(chips)}`
    );
    assert.ok(!chips.some((s) => /Why is the top opportunity ranked first/i.test(s)));
    assert.ok(!chips.some((s) => /What changed overnight/i.test(s)));
    assert.ok(!chips.some((s) => /Compare today's top opportunities/i.test(s)));
    assert.ok(!chips.some((s) => /^Show risks\.?$/i.test(s)));
    assert.ok(
      !chips.some((s) =>
        /^(?:Mail|Launch|Execute|Approve|Print)\b/i.test(String(s))
      )
    );
    assert.equal(result.structured.metadata.outputKind, 'packet_review_artifact');
    assert.ok(result.structured.metadata.contextHints);
    assert.equal(
      result.structured.metadata.contextHints.lastOutputKind,
      'packet_review'
    );
  });

  it('askWorkspace keeps packet-review chips after inline known-facts PM-002 review', async () => {
    const { createMaxReasoningRuntime } = require('../../index');
    const missionEngine = testMissionEngine();
    const max = createMaxReasoningRuntime({
      disableLlm: true,
      missionEngine,
      missionsEnabled: true,
    });

    const result = await max.askWorkspace({
      question: [
        'Create a preparation-only packet review checklist for PM-002.',
        '',
        'Known facts available:',
        '- prospect_id: PM-002',
        '- company_name: Elm Grove Companies',
        '- contact_name: David Schleyer',
        '- website_status: blocked',
        '- website_value: unknown',
        '- mailing_address_status: blocked',
        '- mailing_address_value: unknown',
        '- phone_status: blocked',
        '- phone_value: unknown',
        '- contact_role_status: needs verification',
        '- mail_readiness: blocked',
        '- draft_readiness: allowed',
        '- execution_readiness: blocked',
        '',
        'Do not include Reasoning, Unavailable context, or Next sections.',
        'Do not invent industry.',
        'Do not create a mission. Do not launch, execute, approve, print, or mail.',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
        briefing: {
          headline: 'Quiet morning',
          summary: 'No major movement overnight.',
          watchAlertCount: 1,
        },
      },
    });

    const chips = result.suggestions || [];
    assert.equal(result.structured.metadata.packetReview, true);
    assert.equal(result.structured.metadata.inlineKnownFacts, true);
    assert.ok(chips.length >= 3, `expected chips, got ${JSON.stringify(chips)}`);
    assert.ok(
      chips.some((s) =>
        /missing verification fields|verification plan|readiness fields|packet checklist|operator decision|blocks mailing/i.test(
          s
        )
      ),
      `expected packet chips after askWorkspace personalization, got: ${JSON.stringify(chips)}`
    );
    assert.ok(!chips.some((s) => /What changed overnight/i.test(s)));
    assert.ok(!chips.some((s) => /top opportunity ranked first/i.test(s)));
    assert.ok(!chips.some((s) => /Compare today's top opportunities/i.test(s)));
    assert.ok(!chips.some((s) => /^Show risks\.?$/i.test(s)));
  });

  it('packet review preserves prior entity industry when table omits it', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
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
        'Do not create a mission.',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const tableMarkdown = [
      '| prospect_id | company_name | contact_name | contact_role_status | website_status | website_value | mailing_address_status | mailing_address_value | phone_status | phone_value | verification_status | mail_readiness | draft_readiness | execution_readiness | operator_next_action | notes |',
      '| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |',
      '| PM-001 | Gamache Properties | Ben Gamache | verified | verified | https://www.gamacheproperties.com | verified | 100 Market St, Manchester NH | verified | 603-555-0198 | needs verification | ready_for_review | allowed | blocked | review packet contents / prepare print checklist | |',
      '| PM-002 | Elm Grove Companies | David Schleyer | needs verification | needs verification | unknown | blocked | unknown | needs verification | unknown | needs verification | blocked | allowed | blocked | verify mailing address first | |',
      '| PM-003 | Mill City Property Management | Lauren DuPaul | needs verification | needs verification | unknown | blocked | unknown | needs verification | unknown | needs verification | blocked | allowed | blocked | verify mailing address first | |',
    ].join('\n');

    const result = await workspace.ask({
      sessionId: first.sessionId,
      question: [
        tableMarkdown,
        '',
        'Create a preparation-only packet review checklist for PM-001.',
        'Do not create a mission. Do not launch, execute, approve, print, or mail anything.',
      ].join('\n'),
    });

    const answer = result.prose || result.structured.answer || '';
    const awc = workspace._sessions.get(result.sessionId).activeWorkContext;
    const entity = (awc.entities || []).find((e) => e.id === 'PM-001');

    assert.equal(result.mission, null);
    assert.equal(entity.industry, 'Property Management');
    assert.equal(result.structured.metadata.draftConfidence, 'medium');
    assert.equal(result.structured.metadata.strictOutputShape, true);
    assert.match(answer, /Draft confidence:\s*medium/i);
    assert.match(answer, /Industry:\s*Property Management/i);
    assert.doesNotMatch(answer, /Industry:\s*unknown/i);
    assert.doesNotMatch(answer, /Industry\/persona evidence:\s*not provided/i);
    assert.match(answer, /appears to be in property management/i);
    assert.match(answer, /Ben,/);
    assert.match(
      answer,
      /I included a short scorecard packet for Gamache Properties as a quick review item/i
    );
    assert.doesNotMatch(answer, /draft held/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /^Reasoning:/m);
    assert.doesNotMatch(answer, /Unavailable in current context/i);
    assert.doesNotMatch(answer, /^Next:/m);

    const customerSection = extractPacketReviewCustomerFacingSection(answer);
    assert.doesNotMatch(customerSection, /Draft confidence/i);
    assert.doesNotMatch(customerSection, /preparation-only/i);
    assert.doesNotMatch(customerSection, /I(?:'|’)d like to send you/i);
    assert.doesNotMatch(customerSection, /ready_for_review/i);
    assert.doesNotMatch(customerSection, /execution remains blocked/i);
    assert.doesNotMatch(customerSection, /vendor coordination/i);
    assert.doesNotMatch(customerSection, /may need attention/i);
    assert.doesNotMatch(customerSection, /owner confidence/i);
    assert.doesNotMatch(customerSection, /slipping through/i);
  });

  it('known current-state bullets parse into readiness rows', () => {
    const {
      parseKnownCurrentStateBullets,
      isCanarySummaryJudgmentRequest,
    } = require('../ActiveWorkContext');

    const text = [
      'Summarize the Campaign 001 preparation-only canary status across PM-001, PM-002, and PM-003.',
      '',
      'Known current state:',
      '- PM-001: Gamache Properties, Ben Gamache, website/address/phone/contact role verified, mail_readiness ready_for_review, draft_readiness allowed, execution_readiness blocked',
      '- PM-002: Elm Grove Companies, David Schleyer, website/address/phone unknown or blocked, contact role needs verification, mail_readiness blocked, draft_readiness allowed, execution_readiness blocked',
      '- PM-003: Mill City Property Management, Lauren DuPaul, website/address/phone unknown or blocked, contact role needs verification, mail_readiness blocked, draft_readiness allowed, execution_readiness blocked',
    ].join('\n');

    assert.equal(isCanarySummaryJudgmentRequest(text), true);

    const parsed = parseKnownCurrentStateBullets(text);
    assert.equal(parsed.hasKnownState, true);
    assert.equal(parsed.rows.length, 3);

    const pm001 = parsed.rows.find((r) => r.prospect_id === 'PM-001');
    const pm002 = parsed.rows.find((r) => r.prospect_id === 'PM-002');
    const pm003 = parsed.rows.find((r) => r.prospect_id === 'PM-003');

    assert.ok(pm001);
    assert.equal(pm001.company_name, 'Gamache Properties');
    assert.equal(pm001.contact_name, 'Ben Gamache');
    assert.equal(pm001.website_status, 'verified');
    assert.equal(pm001.mailing_address_status, 'verified');
    assert.equal(pm001.phone_status, 'verified');
    assert.equal(pm001.contact_role_status, 'verified');
    assert.equal(pm001.mail_readiness, 'ready_for_review');
    assert.equal(pm001.draft_readiness, 'allowed');
    assert.equal(pm001.execution_readiness, 'blocked');

    assert.ok(pm002);
    assert.equal(pm002.company_name, 'Elm Grove Companies');
    assert.equal(pm002.contact_name, 'David Schleyer');
    assert.equal(pm002.website_status, 'unknown');
    assert.equal(pm002.mailing_address_status, 'unknown');
    assert.equal(pm002.phone_status, 'unknown');
    assert.equal(pm002.contact_role_status, 'needs verification');
    assert.equal(pm002.mail_readiness, 'blocked');
    assert.equal(pm002.draft_readiness, 'allowed');
    assert.equal(pm002.execution_readiness, 'blocked');

    assert.ok(pm003);
    assert.equal(pm003.company_name, 'Mill City Property Management');
    assert.equal(pm003.contact_name, 'Lauren DuPaul');
    assert.equal(pm003.mail_readiness, 'blocked');
    assert.equal(pm003.draft_readiness, 'allowed');
    assert.equal(pm003.execution_readiness, 'blocked');
  });

  it('preparation-only canary summary/judgment from known current state — no prospect parse fallback', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });
    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question: [
        'Summarize the Campaign 001 preparation-only canary status across PM-001, PM-002, and PM-003.',
        'Which prospect should be worked next and why?',
        'Exact next operator action for each prospect.',
        'What is safe to draft now?',
        'What is blocked from printing/mailing?',
        '',
        'Known current state:',
        '- PM-001: Gamache Properties, Ben Gamache, website/address/phone/contact role verified, mail_readiness ready_for_review, draft_readiness allowed, execution_readiness blocked',
        '- PM-002: Elm Grove Companies, David Schleyer, website/address/phone unknown or blocked, contact role needs verification, mail_readiness blocked, draft_readiness allowed, execution_readiness blocked',
        '- PM-003: Mill City Property Management, Lauren DuPaul, website/address/phone unknown or blocked, contact role needs verification, mail_readiness blocked, draft_readiness allowed, execution_readiness blocked',
        '',
        'Do not include Reasoning, Unavailable context, or Next sections.',
        'Do not create a mission. Do not launch, execute, approve, print, or mail.',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const meta = result.structured.metadata || {};

    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(meta.canaryPreparationOnly, true);
    assert.equal(meta.canarySummary, true);
    assert.equal(meta.knownCurrentState, true);
    assert.equal(meta.strictOutputShape, true);
    assert.equal(meta.prioritizedProspectId, 'PM-001');
    assert.equal(meta.prospectCount, 3);
    assert.equal(meta.outputKind, 'canary_summary');
    assert.equal(meta.lastOutputKind, 'canary_summary');

    assert.doesNotMatch(answer, /could not parse them cleanly/i);
    assert.doesNotMatch(answer, /paste\s+(?:the\s+)?(?:3\s+)?prospects?/i);
    assert.doesNotMatch(answer, /pipe\s*[-\s]?format/i);
    assert.doesNotMatch(answer, /I see you intended to provide/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /^Reasoning:/m);
    assert.doesNotMatch(answer, /Unavailable in current context/i);
    assert.doesNotMatch(answer, /^Next:/m);

    assert.match(answer, /preparation-only canary/i);
    assert.match(answer, /Readiness table/i);
    assert.match(answer, /PM-001/);
    assert.match(answer, /PM-002/);
    assert.match(answer, /PM-003/);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /Elm Grove Companies/);
    assert.match(answer, /Mill City Property Management/);
    assert.match(answer, /ready_for_review/);
    assert.match(
      answer,
      /PM-001[\s\S]*packet review|packet review[\s\S]*PM-001|prioritize[\s\S]*PM-001|PM-001[\s\S]*priorit/i
    );
    assert.match(
      answer,
      /Create a preparation-only packet review checklist and complete operator packet-content review\. Do not print, mail, launch, or execute\./
    );
    assert.match(
      answer,
      /PM-001 may be packet-reviewed now, but is still blocked from print\/mail until explicit approval\./
    );
    assert.match(
      answer,
      /Decide whether to run preparation-only packet review for PM-001 next\. Explicit future launch\/mail approval is still required before any print or mail\./
    );
    assert.doesNotMatch(answer, /final human approval/i);
    assert.match(answer, /verification/i);
    assert.match(answer, /safe to draft now/i);
    assert.match(answer, /PM-001[\s\S]*allowed|allowed[\s\S]*PM-001/i);
    assert.match(answer, /blocked from printing\/mailing/i);
    assert.match(answer, /execution_readiness remains blocked/i);
    assert.match(answer, /PulseForge should track next/i);
    assert.match(answer, /Final operator decision required/i);
    assert.match(answer, /No mission created/i);
    assert.match(answer, /No launch, execution, approval, print, or mail/i);
    assert.doesNotMatch(answer, /\b(?:was|were|has been|have been)\s+(?:launched|printed|mailed|approved)\b/i);
    assert.doesNotMatch(answer, /\b(?:successfully|already)\s+(?:launched|printed|mailed)\b/i);

    const chips = Array.isArray(result.suggestions) ? result.suggestions : [];
    assert.ok(
      chips.some((s) =>
        /packet review checklist for PM-001/i.test(String(s || ''))
      ),
      `expected packet-review chip, got: ${JSON.stringify(chips)}`
    );
    assert.ok(
      !chips.some((s) =>
        /^(?:mail|launch|execute|approve|print)\b/i.test(String(s || '').trim())
      ),
      `unsafe action chips: ${JSON.stringify(chips)}`
    );
  });

  it('canary summary reuses desk tableRows without prospect parse fallback', async () => {
    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });

    const setup = await workspace.ask({
      question: [
        'Preparation-only canary for Campaign 001. Use these 3 prospects:',
        '1. PM-001 — Gamache Properties — Ben Gamache — Property Management',
        '2. PM-002 — Elm Grove Companies — David Schleyer — Property Management',
        '3. PM-003 — Mill City Property Management — Lauren DuPaul — Property Management',
        'Convert this into a fillable verification table.',
        'Do not create a mission. Do not launch, execute, approve, print, or mail.',
      ].join('\n'),
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const tableMarkdown = [
      '| prospect_id | company_name | contact_name | contact_role_status | website_status | website_value | mailing_address_status | mailing_address_value | phone_status | phone_value | verification_status | mail_readiness | draft_readiness | execution_readiness | operator_next_action | notes |',
      '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|',
      '| PM-001 | Gamache Properties | Ben Gamache | verified | verified | https://www.gamacheproperties.com | verified | 100 Market St, Manchester NH | verified | 603-555-0198 | needs verification | ready_for_review | allowed | blocked | review packet contents / prepare print checklist | |',
      '| PM-002 | Elm Grove Companies | David Schleyer | needs verification | needs verification | unknown | blocked | unknown | needs verification | unknown | needs verification | blocked | allowed | blocked | verify mailing address first | |',
      '| PM-003 | Mill City Property Management | Lauren DuPaul | needs verification | needs verification | unknown | blocked | unknown | needs verification | unknown | needs verification | blocked | allowed | blocked | verify mailing address first | |',
    ].join('\n');

    // Ingest the fillable desk table in its own turn, then summarize without
    // re-pasting so canarySummaryRows come from activeWorkContext.tableRows.
    await workspace.ask({
      sessionId: setup.sessionId,
      question: [
        tableMarkdown,
        '',
        'Update the fillable verification table from this paste.',
        'Do not create a mission. Do not launch, execute, approve, print, or mail.',
      ].join('\n'),
    });

    const beforeMissions = await missionEngine.list({ tenantId: '10' });
    const result = await workspace.ask({
      sessionId: setup.sessionId,
      question: [
        'Summarize the Campaign 001 preparation-only canary status across PM-001, PM-002, and PM-003.',
        'Which prospect should be worked next and why?',
        'Do not include Reasoning, Unavailable context, or Next sections.',
        'Do not create a mission. Do not launch, execute, approve, print, or mail.',
      ].join('\n'),
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const meta = result.structured.metadata || {};

    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(meta.canarySummary, true);
    assert.equal(meta.activeWorkContextReused, true);
    assert.equal(meta.knownCurrentState, undefined);
    assert.equal(meta.prioritizedProspectId, 'PM-001');
    assert.doesNotMatch(answer, /could not parse them cleanly/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.match(answer, /PM-001[\s\S]*ready_for_review|ready_for_review[\s\S]*PM-001/i);
    assert.match(
      answer,
      /packet review|operator packet-content (?:review|approval)|final packet review decision/i
    );
    assert.doesNotMatch(answer, /final human approval/i);
    assert.match(answer, /PM-002/);
    assert.match(answer, /PM-003/);
    assert.match(answer, /verification/i);
  });

  it('preparation-only canary summary from readiness table — no prospect parse fallback', async () => {
    const {
      isCanarySummaryJudgmentRequest,
      isPacketReviewRequest,
      looksLikeReadinessSummaryTablePaste,
      parseReadinessSummaryTableFromMessage,
      isExplicitNewMissionRequest,
    } = require('../ActiveWorkContext');

    const question = [
      'Summarize the Campaign 001 preparation-only canary status',
      'Which prospect should be worked next and why?',
      'What is safe to draft now?',
      'What is blocked from printing/mailing?',
      '',
      'current canary readiness table for all 3 prospects:',
      '| prospect_id | company_name | contact_name | verification_summary | mail_readiness | draft_readiness | execution_readiness |',
      '|---|---|---|---|---|---|---|',
      '| PM-001 | Gamache Properties | Ben Gamache | website/address/phone/contact role verified | ready_for_review | allowed | blocked |',
      '| PM-002 | Elm Grove Companies | David Schleyer | website/address/phone unknown or blocked, contact role needs verification | blocked | allowed | blocked |',
      '| PM-003 | Mill City Property Management | Lauren DuPaul | website/address/phone unknown or blocked, contact role needs verification | blocked | allowed | blocked |',
      '',
      'Do not include Reasoning, Unavailable context, or Next sections.',
      'Do not create a mission. Do not launch, execute, approve, print, or mail.',
    ].join('\n');

    assert.equal(isExplicitNewMissionRequest(question), false);
    assert.equal(isCanarySummaryJudgmentRequest(question), true);
    assert.equal(isPacketReviewRequest(question), false);
    assert.equal(looksLikeReadinessSummaryTablePaste(question), true);

    const parsed = parseReadinessSummaryTableFromMessage(question);
    assert.ok(parsed);
    assert.equal(parsed.rows.length, 3);
    assert.equal(parsed.rows[0].prospect_id, 'PM-001');
    assert.match(String(parsed.rows[0].gate_summary || ''), /verified/i);

    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });
    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const meta = result.structured.metadata || {};

    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(meta.canaryPreparationOnly, true);
    assert.equal(meta.canarySummary, true);
    assert.equal(meta.packetReview, undefined);
    assert.equal(meta.prioritizedProspectId, 'PM-001');
    assert.equal(meta.prospectCount, 3);
    assert.equal(meta.outputKind, 'canary_summary');
    assert.equal(meta.strictOutputShape, true);

    assert.doesNotMatch(answer, /could not parse them cleanly/i);
    assert.doesNotMatch(answer, /paste\s+(?:the\s+)?(?:3\s+)?prospects?/i);
    assert.doesNotMatch(answer, /pipe\s*[-\s]?format/i);
    assert.doesNotMatch(answer, /I see you intended to provide/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /^Reasoning:/m);
    assert.doesNotMatch(answer, /Unavailable in current context/i);
    assert.doesNotMatch(answer, /^Next:/m);
    assert.doesNotMatch(answer, /Preparation-only packet review —/i);
    assert.doesNotMatch(answer, /--- Customer-facing drafts ---/i);
    assert.doesNotMatch(answer, /Personalized letter draft/i);
    assert.doesNotMatch(answer, /verified:\s*unknown/i);

    assert.match(answer, /preparation-only canary/i);
    assert.match(answer, /Readiness table/i);
    assert.match(answer, /PM-001/);
    assert.match(answer, /PM-002/);
    assert.match(answer, /PM-003/);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /ready_for_review/);
    assert.match(
      answer,
      /PM-001[\s\S]*packet review|packet review[\s\S]*PM-001|prioritize[\s\S]*PM-001|PM-001[\s\S]*priorit/i
    );
    assert.match(
      answer,
      /Create a preparation-only packet review checklist and complete operator packet-content review\. Do not print, mail, launch, or execute\./
    );
    assert.match(
      answer,
      /PM-001 may be packet-reviewed now, but is still blocked from print\/mail until explicit approval\./
    );
    assert.match(
      answer,
      /Decide whether to run preparation-only packet review for PM-001 next\. Explicit future launch\/mail approval is still required before any print or mail\./
    );
    assert.doesNotMatch(answer, /final human approval/i);
    assert.match(answer, /verification/i);
    assert.match(answer, /safe to draft now/i);
    assert.match(answer, /blocked from printing\/mailing/i);
    assert.match(answer, /No mission created/i);
  });

  it('focused next work-order cues return one work order, not full canary summary', async () => {
    const {
      isCanarySummaryJudgmentRequest,
      isFocusedCanaryWorkOrderRequest,
      hasFocusedCanaryWorkOrderCues,
      hasCanarySummaryOutputCues,
    } = require('../ActiveWorkContext');

    const question = [
      'Create the next preparation-only work order for Campaign 001.',
      'Choose one next work order only.',
      'Do not return the full canary summary.',
      'Include exact steps for the operator, what Max can prepare next, what Max must not do, and deferred prospects.',
      '',
      'current canary readiness table for all 3 prospects:',
      '| prospect_id | company_name | contact_name | verification_summary | mail_readiness | draft_readiness | execution_readiness |',
      '|---|---|---|---|---|---|---|',
      '| PM-001 | Gamache Properties | Ben Gamache | website/address/phone/contact role verified | ready_for_review | allowed | blocked |',
      '| PM-002 | Elm Grove Companies | David Schleyer | website/address/phone unknown or blocked, contact role needs verification | blocked | allowed | blocked |',
      '| PM-003 | Mill City Property Management | Lauren DuPaul | website/address/phone unknown or blocked, contact role needs verification | blocked | allowed | blocked |',
      '',
      'Do not include Reasoning, Unavailable context, or Next sections.',
      'Do not create a mission. Do not launch, execute, approve, print, or mail.',
    ].join('\n');

    assert.equal(hasFocusedCanaryWorkOrderCues(question), true);
    assert.equal(isFocusedCanaryWorkOrderRequest(question), true);
    assert.equal(isCanarySummaryJudgmentRequest(question), true);

    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });
    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const meta = result.structured.metadata || {};

    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(meta.canaryPreparationOnly, true);
    assert.equal(meta.focusedWorkOrder, true);
    assert.equal(meta.canarySummary, undefined);
    assert.equal(meta.prioritizedProspectId, 'PM-001');
    assert.equal(meta.outputKind, 'focused_work_order');
    assert.equal(meta.lastOutputKind, 'focused_work_order');
    assert.equal(meta.outputSubtype, 'focused_work_order');
    assert.equal(meta.strictOutputShape, true);

    assert.match(answer, /Recommended next work order:/i);
    assert.match(answer, /PM-001 packet-content review/i);
    assert.match(answer, /Why this work order is first:/i);
    assert.match(
      answer,
      /PM-001 is the only prospect with mail_readiness=ready_for_review while execution_readiness remains blocked/i
    );
    assert.match(answer, /PM-002 and PM-003 still need verification/i);
    assert.match(answer, /Exact operator steps:/i);
    assert.match(answer, /Review PM-001 packet contents/i);
    assert.match(answer, /Confirm customer-facing drafts are acceptable/i);
    assert.match(answer, /Confirm print\/sign\/mail checklist is complete/i);
    assert.match(answer, /Record packet-content review decision/i);
    assert.match(
      answer,
      /Do not print\/mail unless a separate future launch\/mail approval is given/i
    );
    assert.match(answer, /What Max can prepare next:/i);
    assert.match(answer, /PM-001 packet review checklist/i);
    assert.match(
      answer,
      /provisional letter \/ handwritten note \/ scorecard cover/i
    );
    assert.match(answer, /tracking fields for review/i);
    assert.match(answer, /What Max must not do:/i);
    assert.match(answer, /create a mission/i);
    assert.match(answer, /launch, execute, approve, print, or mail/i);
    assert.match(answer, /mark execution ready/i);
    assert.match(answer, /Deferred prospects:/i);
    assert.match(answer, /PM-002/);
    assert.match(answer, /PM-003/);
    assert.match(answer, /No mission created/i);
    assert.match(answer, /No launch, execution, approval, print, or mail/i);
    assert.doesNotMatch(answer, /Future mailing eligibility/i);
    assert.doesNotMatch(answer, /Final approval gate/i);

    // Full canary summary sections must not appear.
    assert.doesNotMatch(answer, /Readiness table:/i);
    assert.doesNotMatch(answer, /Exact next operator action per prospect:/i);
    assert.doesNotMatch(answer, /Safe to draft now:/i);
    assert.doesNotMatch(answer, /Blocked from printing\/mailing:/i);
    assert.doesNotMatch(answer, /What PulseForge should track next:/i);
    assert.doesNotMatch(answer, /Final operator decision required/i);
    assert.doesNotMatch(answer, /preparation-only canary:\s*\d+\s+prospect/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /^Reasoning:/m);
    assert.doesNotMatch(answer, /Unavailable in current context/i);
    assert.doesNotMatch(answer, /^Next:/m);

    const chips = Array.isArray(result.suggestions) ? result.suggestions : [];
    assert.ok(
      chips.some((s) =>
        /packet review checklist for PM-001/i.test(String(s || ''))
      ),
      `expected packet-review chip, got: ${JSON.stringify(chips)}`
    );
    assert.ok(
      !chips.some((s) =>
        /^(?:mail|launch|execute|approve|print)\b/i.test(String(s || '').trim())
      ),
      `unsafe action chips: ${JSON.stringify(chips)}`
    );

    // Summary-subtype ask still returns the full canary summary.
    const summaryQuestion = [
      'Summarize the Campaign 001 preparation-only canary status across PM-001, PM-002, and PM-003.',
      'Which prospect should be worked next and why?',
      'What is safe to draft now?',
      'What is blocked from printing/mailing?',
      '',
      'current canary readiness table for all 3 prospects:',
      '| prospect_id | company_name | contact_name | verification_summary | mail_readiness | draft_readiness | execution_readiness |',
      '|---|---|---|---|---|---|---|',
      '| PM-001 | Gamache Properties | Ben Gamache | website/address/phone/contact role verified | ready_for_review | allowed | blocked |',
      '| PM-002 | Elm Grove Companies | David Schleyer | website/address/phone unknown or blocked, contact role needs verification | blocked | allowed | blocked |',
      '| PM-003 | Mill City Property Management | Lauren DuPaul | website/address/phone unknown or blocked, contact role needs verification | blocked | allowed | blocked |',
      '',
      'Do not include Reasoning, Unavailable context, or Next sections.',
      'Do not create a mission. Do not launch, execute, approve, print, or mail.',
    ].join('\n');

    assert.equal(hasCanarySummaryOutputCues(summaryQuestion), true);
    assert.equal(isFocusedCanaryWorkOrderRequest(summaryQuestion), false);

    const summaryResult = await workspace.ask({
      question: summaryQuestion,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });
    const summaryAnswer =
      summaryResult.prose || summaryResult.structured.answer || '';
    const summaryMeta = summaryResult.structured.metadata || {};
    assert.equal(summaryMeta.canarySummary, true);
    assert.equal(summaryMeta.focusedWorkOrder, undefined);
    assert.equal(summaryMeta.outputKind, 'canary_summary');
    assert.equal(summaryMeta.outputSubtype, 'summary');
    assert.match(summaryAnswer, /Readiness table:/i);
    assert.match(summaryAnswer, /Safe to draft now:/i);
    assert.doesNotMatch(summaryAnswer, /Recommended next work order:/i);
  });

  it('focused work-order includes future mailing eligibility and final approval gate when requested', async () => {
    const {
      isFocusedCanaryWorkOrderRequest,
      wantsFocusedFutureMailingEligibilitySection,
      wantsFocusedFinalApprovalGateSection,
    } = require('../ActiveWorkContext');

    const question = [
      'Create the next preparation-only work order for Campaign 001.',
      'Choose one next work order only.',
      'Do not return the full canary summary.',
      'Include exact steps for the operator, what Max can prepare next, what Max must not do, and deferred prospects.',
      'What would make PM-001 eligible for future mailing approval?',
      'Include the final approval gate before any outbound action.',
      '',
      'current canary readiness table for all 3 prospects:',
      '| prospect_id | company_name | contact_name | verification_summary | mail_readiness | draft_readiness | execution_readiness |',
      '|---|---|---|---|---|---|---|',
      '| PM-001 | Gamache Properties | Ben Gamache | website/address/phone/contact role verified | ready_for_review | allowed | blocked |',
      '| PM-002 | Elm Grove Companies | David Schleyer | website/address/phone unknown or blocked, contact role needs verification | blocked | allowed | blocked |',
      '| PM-003 | Mill City Property Management | Lauren DuPaul | website/address/phone unknown or blocked, contact role needs verification | blocked | allowed | blocked |',
      '',
      'Do not include Reasoning, Unavailable context, or Next sections.',
      'Do not create a mission. Do not launch, execute, approve, print, or mail.',
    ].join('\n');

    assert.equal(isFocusedCanaryWorkOrderRequest(question), true);
    assert.equal(wantsFocusedFutureMailingEligibilitySection(question), true);
    assert.equal(wantsFocusedFinalApprovalGateSection(question), true);

    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });
    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const meta = result.structured.metadata || {};

    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(meta.focusedWorkOrder, true);
    assert.equal(meta.outputKind, 'focused_work_order');
    assert.equal(meta.prioritizedProspectId, 'PM-001');

    assert.match(answer, /Recommended next work order:/i);
    assert.match(answer, /Why this work order is first:/i);
    assert.match(answer, /Exact operator steps:/i);
    assert.match(answer, /What Max can prepare next:/i);
    assert.match(answer, /What Max must not do:/i);
    assert.match(answer, /Future mailing eligibility for PM-001:/i);
    assert.match(answer, /packet-content review completed/i);
    assert.match(answer, /readiness remains complete at send time/i);
    assert.match(
      answer,
      /operator gives separate explicit launch\/mail approval/i
    );
    assert.match(answer, /Deferred prospects:/i);
    assert.match(answer, /Final approval gate before outbound action:/i);
    assert.match(
      answer,
      /No outbound action can happen until the operator explicitly approves launch\/mail in a future step/i
    );
    assert.match(
      answer,
      /Packet-content review is not mail approval/i
    );
    assert.match(
      answer,
      /Preparation-only\. No mission created\. No launch, execution, approval, print, or mail/i
    );
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /Readiness table:/i);
  });

  it('compact readiness table with bold/alias headers populates canary summary rows', async () => {
    const {
      isCanarySummaryJudgmentRequest,
      isPacketReviewRequest,
      looksLikeReadinessSummaryTablePaste,
      looksLikeFillableVerificationTablePaste,
      parseReadinessSummaryTableFromMessage,
    } = require('../ActiveWorkContext');

    const question = [
      'Summarize the Campaign 001 preparation-only canary status',
      'one-line overall status',
      'readiness table for all 3 prospects',
      'which prospect should be worked next',
      'exact next operator action for each prospect',
      'what is safe to draft now',
      'what is blocked from printing/mailing',
      'what PulseForge should track next',
      '',
      '| **Prospect ID** | **Company** | **Contact** | **Status Summary** | **Mail Readiness** | **Draft Readiness** | **Execution Readiness** |',
      '| --- | --- | --- | --- | --- | --- | --- |',
      '| PM-001 | Gamache Properties | Ben Gamache | website/address/phone/contact role verified | ready_for_review | allowed | blocked |',
      '| PM-002 | Elm Grove Companies | David Schleyer | website/address/phone unknown or blocked; contact role needs verification | blocked | allowed | blocked |',
      '| PM-003 | Mill City Property Management | Lauren DuPaul | website/address/phone unknown or blocked; contact role needs verification | blocked | allowed | blocked |',
      '',
      'Do not include Reasoning, Unavailable context, or Next sections.',
      'Do not create a mission. Do not launch, execute, approve, print, or mail.',
    ].join('\n');

    assert.equal(isCanarySummaryJudgmentRequest(question), true);
    assert.equal(isPacketReviewRequest(question), false);
    assert.equal(looksLikeReadinessSummaryTablePaste(question), true);
    assert.equal(
      looksLikeFillableVerificationTablePaste(question),
      false,
      'compact readiness must not be treated as a fillable / ProspectList table'
    );

    const parsed = parseReadinessSummaryTableFromMessage(question);
    assert.ok(parsed);
    assert.equal(parsed.rows.length, 3);
    assert.equal(parsed.rows[0].prospect_id, 'PM-001');
    assert.equal(parsed.rows[0].company_name, 'Gamache Properties');
    assert.equal(parsed.rows[0].contact_name, 'Ben Gamache');
    assert.match(
      String(parsed.rows[0].verification_summary || ''),
      /website\/address\/phone\/contact role verified/i
    );
    assert.equal(parsed.rows[0].mail_readiness, 'ready_for_review');
    assert.match(
      String(parsed.rows[1].verification_summary || ''),
      /unknown or blocked;\s*contact role needs verification/i
    );

    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });
    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const meta = result.structured.metadata || {};

    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(meta.canarySummary, true);
    assert.equal(meta.missingActiveWorkContext, undefined);
    assert.equal(meta.packetReview, undefined);
    assert.equal(meta.prospectCount, 3);
    assert.equal(meta.prioritizedProspectId, 'PM-001');
    assert.equal(meta.outputKind, 'canary_summary');
    assert.equal(meta.strictOutputShape, true);

    assert.doesNotMatch(
      answer,
      /don.?t have the current table or known state/i
    );
    assert.doesNotMatch(answer, /could not parse them cleanly/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /^Reasoning:/m);
    assert.doesNotMatch(answer, /Unavailable in current context/i);
    assert.doesNotMatch(answer, /^Next:/m);
    assert.doesNotMatch(answer, /Preparation-only packet review —/i);
    assert.doesNotMatch(answer, /--- Customer-facing drafts ---/i);
    assert.doesNotMatch(answer, /verified:\s*unknown/i);

    assert.match(answer, /preparation-only canary/i);
    assert.match(answer, /Readiness table/i);
    assert.match(answer, /PM-001/);
    assert.match(answer, /PM-002/);
    assert.match(answer, /PM-003/);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /Elm Grove Companies/);
    assert.match(answer, /Mill City Property Management/);
    assert.match(answer, /ready_for_review/);
    assert.match(
      answer,
      /PM-001[\s\S]*packet review|packet review[\s\S]*PM-001|prioritize[\s\S]*PM-001|PM-001[\s\S]*priorit/i
    );
    assert.match(
      answer,
      /Create a preparation-only packet review checklist and complete operator packet-content review\. Do not print, mail, launch, or execute\./
    );
    assert.match(
      answer,
      /PM-001 may be packet-reviewed now, but is still blocked from print\/mail until explicit approval\./
    );
    assert.match(
      answer,
      /Decide whether to run preparation-only packet review for PM-001 next\. Explicit future launch\/mail approval is still required before any print or mail\./
    );
    assert.doesNotMatch(answer, /final human approval/i);
    assert.match(answer, /verification/i);
    assert.match(answer, /safe to draft now/i);
    assert.match(answer, /blocked from printing\/mailing/i);
    assert.match(answer, /PulseForge should track next/i);
    assert.match(answer, /Final operator decision required/i);
    assert.match(answer, /No mission created/i);
  });

  it('canary summary outranks packet-review residue in readiness/state rows', async () => {
    const {
      isCanarySummaryJudgmentRequest,
      isPacketReviewRequest,
      extractOperatorIntentProse,
    } = require('../ActiveWorkContext');

    // Compact readiness table (no field values) + fillable residue where
    // operator_next_action literally says "Create packet review checklist".
    // Summary cues must still win — do not emit a PM-001 packet-review artifact.
    const question = [
      'Summarize the Campaign 001 preparation-only canary status',
      'one-line overall status',
      'readiness table for all 3 prospects',
      'which prospect should be worked next',
      'exact next operator action for each prospect',
      'what is safe to draft now',
      'what is blocked from printing/mailing',
      'what PulseForge should track next',
      '',
      'current canary readiness table for all 3 prospects:',
      '| prospect_id | company_name | contact_name | verification_summary | mail_readiness | draft_readiness | execution_readiness |',
      '|---|---|---|---|---|---|---|',
      '| PM-001 | Gamache Properties | Ben Gamache | website/address/phone/contact role verified | ready_for_review | allowed | blocked |',
      '| PM-002 | Elm Grove Companies | David Schleyer | website/address/phone unknown or blocked, contact role needs verification | blocked | allowed | blocked |',
      '| PM-003 | Mill City Property Management | Lauren DuPaul | website/address/phone unknown or blocked, contact role needs verification | blocked | allowed | blocked |',
      '',
      'Desk note (do not treat as an ask to generate a packet):',
      '| prospect_id | company_name | contact_name | contact_role_status | website_status | website_value | mailing_address_status | mailing_address_value | phone_status | phone_value | verification_status | mail_readiness | draft_readiness | execution_readiness | operator_next_action | notes |',
      '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|',
      '| PM-001 | Gamache Properties | Ben Gamache | verified | verified | unknown | verified | unknown | verified | unknown | needs verification | ready_for_review | allowed | blocked | Create packet review checklist | |',
      '| PM-002 | Elm Grove Companies | David Schleyer | needs verification | needs verification | unknown | blocked | unknown | needs verification | unknown | needs verification | blocked | allowed | blocked | verify mailing address first | |',
      '| PM-003 | Mill City Property Management | Lauren DuPaul | needs verification | needs verification | unknown | blocked | unknown | needs verification | unknown | needs verification | blocked | allowed | blocked | review packet contents | |',
      '',
      'Do not include Reasoning, Unavailable context, or Next sections.',
      'Do not create a mission. Do not launch, execute, approve, print, or mail.',
    ].join('\n');

    const prose = extractOperatorIntentProse(question);
    assert.doesNotMatch(prose, /Create packet review checklist/i);
    assert.doesNotMatch(prose, /\breview packet contents\b/i);
    assert.equal(isCanarySummaryJudgmentRequest(question), true);
    assert.equal(isPacketReviewRequest(question), false);

    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });
    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const meta = result.structured.metadata || {};

    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(meta.canarySummary, true);
    assert.equal(meta.packetReview, undefined);
    assert.equal(meta.outputKind, 'canary_summary');
    assert.equal(meta.prospectCount, 3);
    assert.equal(meta.prioritizedProspectId, 'PM-001');
    assert.equal(meta.strictOutputShape, true);

    assert.match(answer, /Readiness table/i);
    assert.match(answer, /PM-001/);
    assert.match(answer, /PM-002/);
    assert.match(answer, /PM-003/);
    assert.match(answer, /ready_for_review/);
    assert.match(answer, /safe to draft now/i);
    assert.match(answer, /blocked from printing\/mailing/i);
    assert.match(answer, /PulseForge should track next/i);
    assert.match(answer, /Final operator decision required/i);

    assert.doesNotMatch(answer, /Preparation-only packet review —/i);
    assert.doesNotMatch(answer, /--- Customer-facing drafts ---/i);
    assert.doesNotMatch(answer, /Personalized letter draft/i);
    assert.doesNotMatch(answer, /Handwritten note draft/i);
    assert.doesNotMatch(answer, /Scorecard cover text draft/i);
    assert.doesNotMatch(answer, /verified:\s*unknown/i);
    assert.doesNotMatch(answer, /^Reasoning:/m);
    assert.doesNotMatch(answer, /Unavailable in current context/i);
    assert.doesNotMatch(answer, /^Next:/m);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
  });

  it('UI-submitted compact readiness fixture parses canary summary (blank after separator)', async () => {
    const fs = require('node:fs');
    const path = require('node:path');
    const {
      isCanarySummaryJudgmentRequest,
      isPacketReviewRequest,
      looksLikeReadinessSummaryTablePaste,
      parseReadinessSummaryTableFromMessage,
      diagnoseCanaryReadinessTableIngestion,
      hasCanaryReadinessTableCues,
      isExplicitNewMissionRequest,
    } = require('../ActiveWorkContext');

    // Exact Command Deck UI-submitted shape: blank line after the markdown
    // separator (common paste artifact). Do not use the idealized contiguous table.
    const fixturePath = path.join(
      __dirname,
      'fixtures',
      'ui-submitted-compact-readiness-canary-summary.txt'
    );
    const question = fs.readFileSync(fixturePath, 'utf8');

    assert.match(question, /\|\s*prospect_id\s*\|/i);
    assert.match(question, /verification_summary/i);
    assert.match(
      question,
      /\|---\|[\s\S]*?\n\n\| PM-001/,
      'fixture must keep the blank line after the separator (UI paste shape)'
    );

    const diagnostics = diagnoseCanaryReadinessTableIngestion(question);
    assert.ok(diagnostics.latestUserMessageLength > 0);
    assert.equal(diagnostics.containsPipeProspectId, true);
    assert.equal(diagnostics.containsVerificationSummary, true);
    assert.ok(diagnostics.markdownTableRowCount >= 5);
    assert.equal(diagnostics.parsedCanarySummaryRowsCount, 3);
    assert.equal(diagnostics.parseFailureReason, null);

    assert.equal(isExplicitNewMissionRequest(question), false);
    assert.equal(isCanarySummaryJudgmentRequest(question), true);
    assert.equal(isPacketReviewRequest(question), false);
    assert.equal(looksLikeReadinessSummaryTablePaste(question), true);
    assert.equal(hasCanaryReadinessTableCues(question), true);

    const parsed = parseReadinessSummaryTableFromMessage(question);
    assert.ok(parsed);
    assert.equal(parsed.rows.length, 3);
    assert.equal(parsed.rows[0].prospect_id, 'PM-001');
    assert.equal(parsed.rows[1].prospect_id, 'PM-002');
    assert.equal(parsed.rows[2].prospect_id, 'PM-003');

    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });
    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const meta = result.structured.metadata || {};

    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(meta.canarySummary, true);
    assert.equal(meta.missingActiveWorkContext, undefined);
    assert.equal(meta.readinessTableNotIngested, undefined);
    assert.equal(meta.packetReview, undefined);
    assert.equal(meta.prospectCount, 3);
    assert.equal(meta.prioritizedProspectId, 'PM-001');
    assert.equal(meta.outputKind, 'canary_summary');

    assert.doesNotMatch(
      answer,
      /don.?t have the current table or known state/i
    );
    assert.doesNotMatch(
      answer,
      /readiness table did not come through/i
    );
    assert.doesNotMatch(answer, /could not parse them cleanly/i);
    assert.doesNotMatch(answer, /paste\s+(?:the\s+)?(?:3\s+)?prospects?/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /Preparation-only packet review —/i);
    assert.doesNotMatch(answer, /--- Customer-facing drafts ---/i);

    assert.match(answer, /preparation-only canary/i);
    assert.match(answer, /Readiness table/i);
    assert.match(answer, /PM-001/);
    assert.match(answer, /PM-002/);
    assert.match(answer, /PM-003/);
    assert.match(answer, /Gamache Properties/);
    assert.match(answer, /ready_for_review/);
    assert.match(answer, /No mission created/i);
  });

  it('summary intent with readiness cues but unparseable table asks for paste, not generic missing-state', async () => {
    const {
      isCanarySummaryJudgmentRequest,
      diagnoseCanaryReadinessTableIngestion,
      hasCanaryReadinessTableCues,
    } = require('../ActiveWorkContext');

    // Summary cues + readiness-table wording, but table body stripped the way
    // a collapsed Expand preview / truncated paste would — no pipe rows.
    const question = [
      'Summarize the Campaign 001 preparation-only canary status',
      'one-line overall status',
      'readiness table for all 3 prospects',
      'which prospect should be worked next',
      'exact next operator action for each prospect',
      'what is safe to draft now',
      '…',
    ].join('\n');

    assert.equal(isCanarySummaryJudgmentRequest(question), true);
    assert.equal(hasCanaryReadinessTableCues(question), true);

    const diagnostics = diagnoseCanaryReadinessTableIngestion(question);
    assert.equal(diagnostics.parsedCanarySummaryRowsCount, 0);
    assert.equal(diagnostics.parseFailureReason, 'no_table_block_found');
    assert.equal(diagnostics.containsPipeProspectId, false);

    const missionEngine = testMissionEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      disableLlm: true,
    });
    const beforeMissions = await missionEngine.list({ tenantId: '10' });

    const result = await workspace.ask({
      question,
      context: {
        tenantId: '10',
        page: 'command-deck',
      },
    });

    const afterMissions = await missionEngine.list({ tenantId: '10' });
    const answer = result.prose || result.structured.answer || '';
    const meta = result.structured.metadata || {};

    assert.equal(result.mission, null);
    assert.equal(afterMissions.length, beforeMissions.length);
    assert.equal(meta.canarySummary, true);
    assert.equal(meta.readinessTableNotIngested, true);
    assert.equal(meta.packetReview, undefined);
    assert.match(
      answer,
      /readiness table did not come through/i
    );
    assert.match(
      answer,
      /Paste either the compact readiness table or one state line per prospect/i
    );
    assert.doesNotMatch(
      answer,
      /don.?t have the current table or known state/i
    );
    assert.doesNotMatch(answer, /could not parse them cleanly/i);
    assert.doesNotMatch(answer, /(?<!No )Mission created/i);
    assert.doesNotMatch(answer, /Preparation-only packet review —/i);
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
