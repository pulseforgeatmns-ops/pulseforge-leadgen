'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { buildFailureDiagnostic, withPresentationTurn, bindPresentationTurn } = require('../PresentationDiagnostics');
const { PresentationEngine, formatDeterministicProse } = require('../PresentationEngine');
const { createHermeticWorkspaceEngine, unavailableOperatingEvidence } = require('./workspaceTestHarness');

const body = (message, extra = {}) => ({ status: 400, requestID: 'req_01123456789',
  error: { type: 'error', error: { type: 'invalid_request_error', message, ...extra } } });
const s = { answer: 'Six prospects from the tenant repository.', reasoning: ['Source: prospects.'],
  metadata: { unavailable: ['closing dates'] }, nextInvestigations: ['Review evidence.'] };

test('canonical validation summaries and explicit allowlisted fields only', () => {
  for (const [message, parameter, description] of [
    ['max_tokens: Input should be greater than 0', 'max_tokens', 'Field is outside the permitted numeric range.'],
    ['messages.0.content.0.text: Input should be a valid string', 'messages.0.content.0.text', 'Field has an invalid type.'],
    ['model: Field required', 'model', 'Required field missing.'],
    ['temperature: Extra inputs are not permitted', 'temperature', 'Field is not permitted.'],
  ]) {
    const d = buildFailureDiagnostic(body(message), 'invalid_request');
    assert.equal(d.parameter, parameter);
    assert.equal(d.validationDescription, description);
    assert.equal(d.providerErrorType, 'invalid_request_error');
    assert.equal(d.httpStatus, 400);
    assert.equal(d.requestId, 'req_01123456789');
    assert.ok(d.validationDescription.length <= 160);
  }
  assert.equal(buildFailureDiagnostic(body('Field required', { param: 'model' })).parameter, 'model');
  assert.equal(buildFailureDiagnostic(body('Field required', { parameter: 'max_tokens' })).parameter, 'max_tokens');
  assert.equal(buildFailureDiagnostic(body('model does not exist')).parameter, null, 'never infer a field from prose');
  assert.equal(buildFailureDiagnostic({ status: 400 }).parameter, null, 'never infer a field from status');
  const flat = buildFailureDiagnostic({status: 400, error: {type: 'invalid_request_error', message: 'model: Field required', request_id: 'req_abcdef12345'}});
  assert.equal(flat.providerErrorType, 'invalid_request_error');
  assert.equal(flat.requestId, 'req_abcdef12345');
});

test('sensitive, unrecognized and oversized values cannot escape the fixed projection', () => {
  const secret = 'sk-ant-SECRET Authorization: Bearer SECRET prospect Alice alice@example.com PROMPT RESPONSE';
  for (const error of [
    body(`messages.0.content: ${secret}`),
    body(`max_tokens: Input should be greater than 0 ${secret}`),
    body(secret, {param: secret, type: secret}),
    body(`model: ${'x'.repeat(5000)}${secret}`),
    {status: secret, requestID: secret, error: {error:{message:secret, type:secret}}},
    {status:400, message:secret, headers:{authorization:secret}, response:secret, prompt:secret},
    {status:400, requestID:'req_'+'a'.repeat(101), error:{error:{type:'invalid_request_error',param:'messages.1234567.content'}}},
  ]) {
    const d = buildFailureDiagnostic(error, secret);
    assert.doesNotMatch(JSON.stringify(d), /SECRET|Alice|alice@|PROMPT|RESPONSE|Authorization/);
    assert.ok(JSON.stringify(d).length < 1100);
    assert.equal(d.errorCode, 'provider_error');
  }
  const d = buildFailureDiagnostic(body(`messages.0.content: ${secret}`));
  assert.equal(d.parameter, 'messages.0.content');
  assert.equal(d.validationDescription, 'Provider rejected an explicit request field; message omitted.');
});

test('missing and malformed fields are null, getters are not invoked', () => {
  for (const error of [null, undefined, 'secret', 42, [], {}, {error:[]}, {error:{error:{type:{}, message:[]}}}]) {
    const d = buildFailureDiagnostic(error);
    assert.equal(d.httpStatus, null); assert.equal(d.requestId, null); assert.equal(d.parameter, null);
  }
  const error = {};
  for (const key of ['error','status','requestID']) Object.defineProperty(error,key,{get(){throw new Error('getter invoked');}});
  assert.equal(buildFailureDiagnostic(error).providerErrorType,null);
});

test('turn correlation is isolated across concurrent asynchronous presentations', async () => {
  const saved = process.env.RAILWAY_GIT_COMMIT_SHA;
  process.env.RAILWAY_GIT_COMMIT_SHA = 'a'.repeat(40);
  try {
    const run = (tenantId, sessionId, seq) => withPresentationTurn(async () => {
      bindPresentationTurn({ id:sessionId, context:{tenantId}, operatorTurnSeq:seq });
      await new Promise(resolve=>setImmediate(resolve));
      return buildFailureDiagnostic(body('model: Field required'), 'invalid_request');
    });
    const aId='11111111-1111-4111-8111-111111111111', bId='22222222-2222-4222-8222-222222222222';
    const [a,b] = await Promise.all([run('21',aId,7),run('22',bId,9)]);
    assert.equal(a.sessionId,aId); assert.equal(a.tenantId,'21'); assert.equal(a.operatorTurnSeq,7);
    assert.equal(b.sessionId,bId); assert.equal(b.tenantId,'22'); assert.equal(b.operatorTurnSeq,9);
    assert.notEqual(a.turnId,b.turnId); assert.match(a.turnId,/^[a-f0-9-]{36}$/);
    assert.equal(a.stage,'presentation.messages.create'); assert.equal(a.revision,'a'.repeat(40));
    assert.equal(buildFailureDiagnostic({}).turnId,null);
    process.env.RAILWAY_GIT_COMMIT_SHA='secret revision';
    assert.equal(buildFailureDiagnostic({}).revision,null);
  } finally { if(saved===undefined) delete process.env.RAILWAY_GIT_COMMIT_SHA; else process.env.RAILWAY_GIT_COMMIT_SHA=saved; }
});

test('logger failures and hostile SDK exceptions cannot interrupt deterministic fallback', async () => {
  const saved=process.env.ANTHROPIC_API_KEY, warn=console.warn;
  process.env.ANTHROPIC_API_KEY='unused-fixture';
  try {
    for (const err of [body('model: Field required'), new Proxy({}, {get(){throw new Error('bad getter');},getOwnPropertyDescriptor(){throw new Error('bad descriptor');}})]) {
      console.warn=()=>{throw new Error('sink unavailable');};
      const engine=new PresentationEngine({anthropic:{messages:{create:async()=>{throw err;}}}});
      const out=await engine.present(s);
      assert.equal(out.presentation,'fallback_error');
      assert.equal(out.prose,formatDeterministicProse(s));
      assert.equal(out.structured,s);
      assert.equal(out.metadata.modelCompletion.ok,false);
      assert.doesNotMatch(JSON.stringify(out),/req_01123456789|invalid_request_error|Required field missing/);
    }
  } finally {console.warn=warn;if(saved===undefined) delete process.env.ANTHROPIC_API_KEY;else process.env.ANTHROPIC_API_KEY=saved;}
});

test('real WorkspaceEngine ask binds the resolved session and monotonic turn to failure logs', async () => {
  const saved=process.env.ANTHROPIC_API_KEY, warn=console.warn, logs=[];
  process.env.ANTHROPIC_API_KEY='unused-fixture'; console.warn=(...args)=>logs.push(args);
  try {
    const ws=createHermeticWorkspaceEngine({disableLlm:false,anthropic:{messages:{create:async()=>{throw body('model: Field required');}}},
      operatingEvidenceOpts:{...unavailableOperatingEvidence(),loadProspects:async()=>({available:true,status:'found',counts:{total:6},segments:[]})}});
    const opened=await ws.open({tenantId:'21'});
    const before=logs.length;
    const r=await ws.ask({sessionId:opened.sessionId,question:'Brief me on my prospects.'});
    const events=logs.slice(before).map(x=>x[1]).filter(x=>x?.event==='MAX_PRESENTATION_PROVIDER_FAILURE');
    assert.equal(r.presentation,'fallback_error'); assert.equal(events.length,1);
    assert.equal(events[0].sessionId,opened.sessionId); assert.equal(events[0].tenantId,'21');
    assert.equal(events[0].operatorTurnSeq,1); assert.ok(events[0].turnId);
    assert.equal(r.prose,formatDeterministicProse(r.structured));
    await ws.ask({sessionId:opened.sessionId,question:'Brief me on my prospects.'});
    const second=logs.map(x=>x[1]).filter(x=>x?.event==='MAX_PRESENTATION_PROVIDER_FAILURE').at(-1);
    assert.equal(second.operatorTurnSeq,2); assert.notEqual(second.turnId,events[0].turnId);
    const fresh=await ws.ask({context:{tenantId:'22'},question:'Brief me on my prospects.'});
    const freshEvent=logs.map(x=>x[1]).filter(x=>x?.event==='MAX_PRESENTATION_PROVIDER_FAILURE').at(-1);
    assert.equal(freshEvent.tenantId,'22'); assert.equal(freshEvent.sessionId,fresh.sessionId);
    assert.equal(freshEvent.operatorTurnSeq,1);
    assert.doesNotMatch(JSON.stringify(r),/MAX_PRESENTATION_PROVIDER_FAILURE|req_01123456789|invalid_request_error/);
  } finally {console.warn=warn;if(saved===undefined) delete process.env.ANTHROPIC_API_KEY;else process.env.ANTHROPIC_API_KEY=saved;}
});
