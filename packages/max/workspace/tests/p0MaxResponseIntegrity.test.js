'use strict';

/**
 * P0 Max reliability — response integrity.
 *
 *   1. A turn handled as a session setting never dispatches; when the operator's
 *      words also command execution, Max must say "Not executed" — never a bare
 *      "Acknowledged." that reads like the work was accepted or done.
 *   2. A failed model completion (auth, rate limit, overload, timeout, network,
 *      spend guard) is recorded as a failure with a sanitized, actionable code;
 *      raw provider text never reaches the response.
 *
 * Deterministic: no network, no database, no provider.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const { createHermeticWorkspaceEngine: createWorkspaceEngine } = require('./workspaceTestHarness');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { getSessionState, EXECUTION_POLICIES } = require('../SessionState');
const {
  buildSessionConfigurationResponse,
  detectUnexecutedCommand,
} = require('../SessionConfigurationAcknowledgement');
const {
  PresentationEngine,
  formatDeterministicProse,
  classifyModelError,
  MODEL_ERROR_ACTIONS,
  PRESENTATION_TIMEOUT_MS,
} = require('../PresentationEngine');
const { buildStructuredResponse } = require('../WorkspaceTypes');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

function seedMission(tenantId = '10') {
  const amoEngine = amo.createAcquisitionMissionEngine();
  const mission = amoEngine.create({ tenantId, objective: OBJECTIVE, targetSegment: 'Law Firms' });
  return { amoEngine, mission, runtime: createTestAmoRuntime({ engine: amoEngine }) };
}

// Setting-like phrases that remain session configuration: the setting applies
// and Max says plainly that nothing ran. Operational commands ("let's run it",
// "go ahead and launch the campaign") no longer take this path at all — see
// p0ExecutionIntentRouting.test.js.
const SETTING_PHRASES_WITH_EXECUTION_WORDING = ['Resume execution', 'Execute autonomously'];
const PURE_SETTING_PHRASES = ['Enable execution', 'Stop theorizing'];

describe('P0 response integrity — session-setting turns never masquerade as execution', () => {
  beforeEach(() => amo.clearExecutionRouterAudit());

  it('detects execution commands but not negated or pure settings', () => {
    for (const q of [...SETTING_PHRASES_WITH_EXECUTION_WORDING, "Let's run it", 'Go ahead and launch the campaign']) {
      assert.ok(detectUnexecutedCommand(q), `expected execution signal for: ${q}`);
    }
    for (const q of ["Don't execute anything.", 'Be concise.', 'Read-only mode', 'Explain your reasoning naturally.', ...PURE_SETTING_PHRASES]) {
      assert.equal(detectUnexecutedCommand(q), null, `unexpected execution signal for: ${q}`);
    }
  });

  for (const question of SETTING_PHRASES_WITH_EXECUTION_WORDING) {
    it(`"${question}" with no mission → session setting answered "Not executed", nothing dispatched`, async () => {
      const engine = createWorkspaceEngine();
      const opened = await engine.open({ tenantId: '10' });
      const result = await engine.ask({ sessionId: opened.sessionId, question });

      assert.equal(result.resolution.action, 'session_configured', 'precondition: session-setting bypass');
      assert.match(result.prose, /^Not executed:/);
      assert.doesNotMatch(result.prose, /^Acknowledged/);
      assert.equal(result.resolution.reason, 'session_configuration_not_executed');
      assert.equal(result.structured.metadata.executionState, 'not_executed');
      assert.equal(result.structured.metadata.executionVerified, false);
      assert.equal(result.structured.metadata.blocker, 'session_configuration_only');
      assert.equal(amo.listExecutionRouterAudit().length, 0, 'no execution request may be routed');
    });
  }

  for (const question of PURE_SETTING_PHRASES) {
    it(`"${question}" → acknowledged setting that states no operational work ran`, async () => {
      const engine = createWorkspaceEngine();
      const opened = await engine.open({ tenantId: '10' });
      const result = await engine.ask({ sessionId: opened.sessionId, question });
      assert.equal(result.resolution.action, 'session_configured');
      assert.match(result.prose, /^Acknowledged\..*No operational work was executed\./);
      assert.notEqual(result.prose.trim(), 'Acknowledged.');
      assert.equal(result.structured.metadata.executionState, 'not_executed');
      assert.equal(amo.listExecutionRouterAudit().length, 0);
    });
  }

  it('repeated pure setting: unchanged state is stated, not a bare "Acknowledged."', async () => {
    const engine = createWorkspaceEngine();
    const opened = await engine.open({ tenantId: '10' });
    await engine.ask({ sessionId: opened.sessionId, question: 'For the rest of this conversation: be concise.' });
    const second = await engine.ask({ sessionId: opened.sessionId, question: 'For the rest of this conversation: be concise.' });
    assert.equal(second.resolution.action, 'session_configured');
    assert.notEqual(second.prose.trim(), 'Acknowledged.');
    assert.match(second.prose, /No session settings changed\. No operational work was executed\./);
    assert.equal(second.structured.metadata.executionState, 'not_executed');
  });

  it('a genuine read-only directive is still acknowledged with the session snapshot', async () => {
    const engine = createWorkspaceEngine();
    const opened = await engine.open({ tenantId: '10' });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: "For the rest of this conversation: Don't execute anything.",
    });
    assert.match(result.prose, /^Acknowledged\. Session settings updated\. No operational work was executed\./);
    assert.match(result.prose, /Execution Policy\s+Read Only/);
    assert.equal(result.resolution.reason, 'session_configuration_acknowledged');
    const state = getSessionState(engine._sessions.get(opened.sessionId));
    assert.equal(state.executionPolicy, EXECUTION_POLICIES.READ_ONLY);
  });

  it('session state stays scoped to its own workspace session (tenant isolation)', async () => {
    const engine = createWorkspaceEngine();
    const a = await engine.open({ tenantId: '10' });
    const b = await engine.open({ tenantId: '2' });
    await engine.ask({ sessionId: a.sessionId, question: "For the rest of this conversation: Don't execute anything." });
    const stateB = getSessionState(engine._sessions.get(b.sessionId));
    assert.notEqual(stateB && stateB.executionPolicy, EXECUTION_POLICIES.READ_ONLY);
    assert.equal(engine._sessions.get(b.sessionId).context.tenantId, '2');
  });

  it('builder marks every session-configuration turn as not executed', () => {
    // Defense in depth: even if an operational command reached this builder,
    // it could only be answered "Not executed".
    const out = buildSessionConfigurationResponse({ sessionState: null, changed: false, question: "Let's execute" });
    assert.match(out.prose, /^Not executed:/);
    assert.equal(out.structured.metadata.executionExpected, true);
    assert.equal(out.structured.metadata.executionSignal, 'execution_command');
  });
});

describe('P0 response integrity — model failures are reported, sanitized and actionable', () => {
  const SECRET_TEXT = 'invalid x-api-key sk-ant-SECRET request_id=req_123';

  function providerError(props) {
    return Object.assign(new Error(SECRET_TEXT), props);
  }

  const CASES = [
    ['auth 401', providerError({ status: 401, name: 'AuthenticationError' }), 'provider_auth'],
    ['permission 403', providerError({ status: 403 }), 'provider_auth'],
    ['rate limit 429', providerError({ status: 429, name: 'RateLimitError' }), 'rate_limited'],
    ['overloaded 529', providerError({ status: 529 }), 'provider_overloaded'],
    ['server 500', providerError({ status: 500, name: 'InternalServerError' }), 'provider_overloaded'],
    ['timeout', Object.assign(new Error('Request timed out.'), { name: 'APIConnectionTimeoutError' }), 'timeout'],
    ['connection', Object.assign(new Error('Connection error.'), { name: 'APIConnectionError' }), 'provider_unreachable'],
    ['spend guard', Object.assign(new Error('api_daily_usd_limit'), { name: 'ApiSpendError', code: 'api_daily_usd_limit' }), 'spend_blocked'],
    ['bad model 404', providerError({ status: 404, name: 'NotFoundError' }), 'invalid_request'],
    ['unknown', new Error(SECRET_TEXT), 'provider_error'],
  ];

  function structured() {
    return buildStructuredResponse({ answer: 'Pipeline has 3 qualified prospects.', reasoning: ['Counted from CRM.'] });
  }

  const savedKey = process.env.ANTHROPIC_API_KEY;
  beforeEach(() => { process.env.ANTHROPIC_API_KEY = 'test-key-not-used'; });
  process.on('exit', () => { if (savedKey === undefined) delete process.env.ANTHROPIC_API_KEY; });

  for (const [label, err, code] of CASES) {
    it(`${label} → fallback_error with sanitized code ${code}`, async () => {
      assert.equal(classifyModelError(err), code);
      const warn = console.warn;
      const logged = [];
      console.warn = (...args) => logged.push(args);
      try {
        const engine = new PresentationEngine({ anthropic: { messages: { create: async () => { throw err; } } } });
        const s = structured();
        const out = await engine.present(s);
        assert.equal(out.presentation, 'fallback_error');
        assert.equal(out.prose, formatDeterministicProse(s), 'answer stays the verified structured content');
        assert.deepEqual(out.metadata.modelCompletion, {
          attempted: true, ok: false, errorCode: code, action: MODEL_ERROR_ACTIONS[code],
        });
        assert.equal(out.metadata.presentationError, code);
        const serialized = JSON.stringify(out) + JSON.stringify(logged);
        assert.doesNotMatch(serialized, /sk-ant|x-api-key|req_123/, 'raw provider text must not leak');
      } finally {
        console.warn = warn;
      }
    });
  }

  it('successful completion is recorded and bounded by a timeout', async () => {
    let seenOptions = null;
    const engine = new PresentationEngine({
      anthropic: { messages: { create: async (_params, options) => { seenOptions = options; return { content: [{ type: 'text', text: 'Three qualified prospects.' }] }; } } },
    });
    const out = await engine.present(structured());
    assert.equal(out.presentation, 'claude');
    assert.deepEqual(out.metadata.modelCompletion, { attempted: true, ok: true });
    assert.equal(out.metadata.presentationError, undefined);
    assert.equal(seenOptions.timeout, PRESENTATION_TIMEOUT_MS);
  });

  it('no model configured is never reported as a completion', async () => {
    const out = await new PresentationEngine({ disableLlm: true }).present(structured());
    assert.equal(out.presentation, 'fallback');
    assert.equal(out.metadata.modelCompletion.ok, false);
    assert.equal(out.metadata.modelCompletion.attempted, false);
    assert.equal(out.metadata.modelCompletion.errorCode, 'model_disabled');
  });

  it('end-to-end: a provider auth failure during a workspace turn is visible in the result', async () => {
    const warn = console.warn;
    console.warn = () => {};
    try {
      const engine = createWorkspaceEngine({
        disableLlm: false,
        anthropic: { messages: { create: async () => { throw providerError({ status: 401, name: 'AuthenticationError' }); } } },
      });
      const opened = await engine.open({ tenantId: '10' });
      const result = await engine.ask({ sessionId: opened.sessionId, question: 'What is the weather like for prospecting today?' });
      const mc = result.metadata && result.metadata.modelCompletion;
      if (mc && mc.attempted) {
        assert.equal(mc.ok, false);
        assert.equal(mc.errorCode, 'provider_auth');
        assert.equal(result.presentation, 'fallback_error');
      }
      assert.doesNotMatch(JSON.stringify(result), /sk-ant|x-api-key|req_123/);
    } finally {
      console.warn = warn;
    }
  });
});
