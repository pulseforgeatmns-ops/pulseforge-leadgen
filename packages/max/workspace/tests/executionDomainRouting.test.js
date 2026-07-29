'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { createMissionEngine } = require('../../../mission-engine');
const { createBuiltinRegistry } = require('../../../capabilities');
const {
  createWorkspaceEngine,
  selectExecutionDomain,
  EXECUTION_DOMAINS,
  isMissionDomain,
} = require('../../index');

function testMissionEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
  });
}

const BRIEFING_CTX = {
  tenantId: '10',
  page: 'command-deck',
  context: 'morning_brief',
  briefing: {
    headline: 'Quiet morning — three watches need review.',
    summary: 'No major movement overnight.',
  },
};

const MARKET_CTX = {
  tenantId: '10',
  page: 'market',
  briefing: {
    headline: 'Should not answer missions',
    summary: 'Briefing residue',
  },
};

describe('SPEC-057 Execution Domain selection', () => {
  it('classifies end-to-end audit as Mission Diagnostics', () => {
    const d = selectExecutionDomain(
      'Run an end-to-end execution audit for Campaign 001.'
    );
    assert.equal(d.domain, EXECUTION_DOMAINS.MISSION_DIAGNOSTICS);
    assert.equal(d.routeKind, 'mission');
    assert.ok(d.missionIntent);
    assert.equal(d.missionIntent.intentCategory, 'campaign_diagnostics');
    assert.ok(isMissionDomain(d.domain));
  });

  it('classifies Build Campaign as Mission Execution', () => {
    const d = selectExecutionDomain('Build Campaign 001 for Anchor Cleaning.');
    assert.equal(d.domain, EXECUTION_DOMAINS.MISSION_EXECUTION);
    assert.ok(d.missionIntent);
  });

  it('classifies Monitor Microsoft as Market Intelligence', () => {
    const d = selectExecutionDomain('Monitor Microsoft.');
    assert.equal(d.domain, EXECUTION_DOMAINS.MARKET_INTELLIGENCE);
    assert.equal(d.routeKind, 'intelligence');
  });

  it('classifies morning brief questions as Morning Briefing', () => {
    const d = selectExecutionDomain("What's in today's briefing?");
    assert.equal(d.domain, EXECUTION_DOMAINS.MORNING_BRIEFING);
  });

  it('ignores previousDomain when selecting (intent wins)', () => {
    const d = selectExecutionDomain(
      'Run an end-to-end execution audit for Campaign 001.',
      { previousDomain: EXECUTION_DOMAINS.MORNING_BRIEFING }
    );
    assert.equal(d.domain, EXECUTION_DOMAINS.MISSION_DIAGNOSTICS);
    assert.equal(d.domainSwitched, true);
    assert.equal(d.previousDomain, EXECUTION_DOMAINS.MORNING_BRIEFING);
  });
});

describe('SPEC-057 Cross-domain routing', () => {
  it('Briefing → Mission Diagnostics: audit invokes Mission Engine', async () => {
    const workspace = createWorkspaceEngine({
      missionEngine: testMissionEngine(),
      missionsEnabled: true,
      disableLlm: true,
    });

    const opened = workspace.open(BRIEFING_CTX);
    assert.equal(opened.executionDomain, EXECUTION_DOMAINS.MORNING_BRIEFING);

    const result = await workspace.ask({
      sessionId: opened.sessionId,
      question: 'Run an end-to-end execution audit for Campaign 001.',
      context: BRIEFING_CTX,
    });

    assert.equal(result.executionDomain, EXECUTION_DOMAINS.MISSION_DIAGNOSTICS);
    assert.equal(result.route, 'mission');
    assert.ok(result.mission);
    assert.ok(result.mission.plan && result.mission.plan.missionIntent);
    assert.equal(
      result.mission.plan.missionIntent.intentCategory,
      'campaign_diagnostics'
    );
    assert.equal(result.structured.metadata.surface, 'mission_workspace');
    assert.doesNotMatch(
      result.prose || result.structured.answer || '',
      /Quiet morning|No major movement/i
    );
    assert.match(
      result.prose || result.structured.answer || '',
      /Mission|Diagnostics|Workspace/i
    );
  });

  it('Briefing → Mission Execution: Build Campaign leaves briefing', async () => {
    const workspace = createWorkspaceEngine({
      missionEngine: testMissionEngine(),
      missionsEnabled: true,
      disableLlm: true,
    });

    const opened = workspace.open(BRIEFING_CTX);
    const result = await workspace.ask({
      sessionId: opened.sessionId,
      question: 'Build Campaign 001 for Anchor Cleaning.',
      context: BRIEFING_CTX,
    });

    assert.equal(result.executionDomain, EXECUTION_DOMAINS.MISSION_EXECUTION);
    assert.equal(result.route, 'mission');
    assert.ok(result.mission);
    assert.ok(result.domainSwitch);
    assert.doesNotMatch(
      result.structured.answer || '',
      /Quiet morning/i
    );
  });

  it('Mission → Market Intelligence: does not continue Mission path', async () => {
    const workspace = createWorkspaceEngine({
      missionEngine: testMissionEngine(),
      missionsEnabled: true,
      disableLlm: true,
    });

    const missionTurn = await workspace.ask({
      question: 'Build Campaign 001 for Anchor Cleaning.',
      context: { tenantId: '10', page: 'command-deck' },
    });
    assert.equal(missionTurn.route, 'mission');
    assert.ok(missionTurn.mission);

    const marketTurn = await workspace.ask({
      sessionId: missionTurn.sessionId,
      question: 'Monitor Microsoft.',
      context: MARKET_CTX,
    });

    assert.equal(
      marketTurn.executionDomain,
      EXECUTION_DOMAINS.MARKET_INTELLIGENCE
    );
    assert.equal(marketTurn.route, 'intelligence');
    assert.equal(marketTurn.mission, null);
    assert.ok(marketTurn.domainSwitch);
    assert.doesNotMatch(
      marketTurn.prose || marketTurn.structured.answer || '',
      /Should not answer missions|Briefing residue/i
    );
  });

  it('Market Intelligence → Briefing: briefing owns the answer', async () => {
    const workspace = createWorkspaceEngine({
      missionEngine: testMissionEngine(),
      missionsEnabled: true,
      disableLlm: true,
    });

    const marketTurn = await workspace.ask({
      question: 'Monitor Microsoft.',
      context: MARKET_CTX,
    });
    assert.equal(
      marketTurn.executionDomain,
      EXECUTION_DOMAINS.MARKET_INTELLIGENCE
    );

    const briefTurn = await workspace.ask({
      sessionId: marketTurn.sessionId,
      question: "What's in today's briefing?",
      context: BRIEFING_CTX,
    });

    assert.equal(
      briefTurn.executionDomain,
      EXECUTION_DOMAINS.MORNING_BRIEFING
    );
    assert.equal(briefTurn.route, 'intelligence');
    assert.equal(briefTurn.mission, null);
    assert.match(
      briefTurn.prose || briefTurn.structured.answer || '',
      /Quiet morning|No major movement/i
    );
  });

  it('active briefing conversation never selects Mission subsystem', async () => {
    const workspace = createWorkspaceEngine({
      missionEngine: testMissionEngine(),
      missionsEnabled: true,
      disableLlm: true,
    });

    // Open as briefing, then issue a semantic mission with sticky briefing envelope
    const opened = workspace.open(BRIEFING_CTX);
    const result = await workspace.ask({
      sessionId: opened.sessionId,
      question: 'Build business intelligence for Anchor Cleaning.',
      context: BRIEFING_CTX,
    });

    assert.equal(result.executionDomain, EXECUTION_DOMAINS.MISSION_EXECUTION);
    assert.equal(result.route, 'mission');
    assert.ok(result.mission);
    assert.ok(result.mission.plan && result.mission.plan.missionIntent);
  });
});
