'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { buildFailureDiagnostic, withPresentationTurn, bindPresentationTurn } = require('../PresentationDiagnostics');
const { PresentationEngine, formatDeterministicProse } = require('../PresentationEngine');
const { createHermeticWorkspaceEngine, unavailableOperatingEvidence } = require('./workspaceTestHarness');

const body = (message, extra = {}) => ({ status: 400, requestID: 'google-request-01123456789',
  error: { code: 400, status: 'INVALID_ARGUMENT', message, ...extra } });
const violation = (field, description) => body('Request contains an invalid argument.', { details: [{
  '@type': 'type.googleapis.com/google.rpc.BadRequest', fieldViolations: [{ field, description }],
}] });
const s = { answer: 'Six prospects from the tenant repository.', reasoning: ['Source: prospects.'],
  metadata: { unavailable: ['closing dates'] }, nextInvestigations: ['Review evidence.'] };

test('canonical Gemini validation summaries and explicit allowlisted fields only', () => {
  for (const [error, parameter, description] of [
    [violation('generationConfig.maxOutputTokens', 'Input should be greater than 0'), 'generationConfig.maxOutputTokens', 'Field is outside the permitted numeric range.'],
    [violation('contents.0.parts.0.text', 'Input should be a valid string'), 'contents.0.parts.0.text', 'Field has an invalid type.'],
    [violation('model', 'Field required'), 'model', 'Required field missing.'],
    [violation('generation_config', 'Extra inputs are not permitted'), 'generation_config', 'Field is not permitted.'],
  ]) {
    const d = buildFailureDiagnostic(error, 'invalid_request');
    assert.equal(d.parameter, parameter); assert.equal(d.validationDescription, description);
    assert.equal(d.providerErrorType, 'INVALID_ARGUMENT'); assert.equal(d.httpStatus, 400);
    assert.equal(d.requestId, 'google-request-01123456789'); assert.ok(d.validationDescription.length <= 160);
  }
  assert.equal(buildFailureDiagnostic(body('Field required', { parameter: 'model' })).parameter, 'model');
  assert.equal(buildFailureDiagnostic(body('model does not exist')).parameter, null, 'never infer a field from prose');
  assert.equal(buildFailureDiagnostic({ status: 400 }).parameter, null, 'never infer a field from status');
});

test('sensitive, unrecognized and oversized values cannot escape the fixed projection', () => {
  const secret = 'AIza-SECRET Authorization: Bearer SECRET prospect Alice alice@example.com PROMPT RESPONSE';
  for (const error of [
    violation('contents.0.parts', secret), body(`contents.0.parts: ${secret}`),
    body(secret, {parameter: secret, status: secret}), body(`model: ${'x'.repeat(5000)}${secret}`),
    {status:secret,requestID:secret,error:{message:secret,status:secret,details:[{'@type':'type.googleapis.com/google.rpc.BadRequest',fieldViolations:[{field:secret,description:secret}]}]}},
    {status:400,message:secret,headers:{authorization:secret},response:secret,prompt:secret},
    {status:400,requestID:'a'.repeat(129),error:{status:'INVALID_ARGUMENT',parameter:'contents.1234567.parts'}},
  ]) {
    const d = buildFailureDiagnostic(error, secret);
    assert.doesNotMatch(JSON.stringify(d), /SECRET|Alice|alice@|PROMPT|RESPONSE|Authorization/);
    assert.ok(JSON.stringify(d).length < 1100); assert.equal(d.errorCode, 'provider_error');
  }
  const d = buildFailureDiagnostic(body(`contents.0.parts: ${secret}`));
  assert.equal(d.parameter, 'contents.0.parts');
  assert.equal(d.validationDescription, 'Provider rejected an explicit request field; message omitted.');
});

test('missing and malformed fields are null, getters are not invoked', () => {
  for (const error of [null, undefined, 'secret', 42, [], {}, {error:[]}, {error:{status:{},message:[],details:{}}}]) {
    const d = buildFailureDiagnostic(error);
    assert.equal(d.httpStatus, null); assert.equal(d.requestId, null); assert.equal(d.parameter, null);
  }
  const error = {};
  for (const key of ['error','status','requestID']) Object.defineProperty(error,key,{get(){throw new Error('getter invoked');}});
  assert.equal(buildFailureDiagnostic(error).providerErrorType,null);
});

test('turn correlation is isolated across concurrent asynchronous presentations', async () => {
  const saved = process.env.RAILWAY_GIT_COMMIT_SHA; process.env.RAILWAY_GIT_COMMIT_SHA = 'a'.repeat(40);
  try {
    const run = (tenantId, sessionId, seq) => withPresentationTurn(async () => {
      bindPresentationTurn({id:sessionId,context:{tenantId},operatorTurnSeq:seq});
      await new Promise(resolve=>setImmediate(resolve));
      return buildFailureDiagnostic(violation('model', 'Field required'), 'invalid_request');
    });
    const aId='11111111-1111-4111-8111-111111111111', bId='22222222-2222-4222-8222-222222222222';
    const [a,b]=await Promise.all([run('21',aId,7),run('22',bId,9)]);
    assert.equal(a.sessionId,aId); assert.equal(a.tenantId,'21'); assert.equal(a.operatorTurnSeq,7);
    assert.equal(b.sessionId,bId); assert.equal(b.tenantId,'22'); assert.equal(b.operatorTurnSeq,9);
    assert.notEqual(a.turnId,b.turnId); assert.match(a.turnId,/^[a-f0-9-]{36}$/);
    assert.equal(a.stage,'presentation.models.generateContent'); assert.equal(a.revision,'a'.repeat(40));
    assert.equal(buildFailureDiagnostic({}).turnId,null);
    process.env.RAILWAY_GIT_COMMIT_SHA='secret revision'; assert.equal(buildFailureDiagnostic({}).revision,null);
  } finally { if(saved===undefined) delete process.env.RAILWAY_GIT_COMMIT_SHA; else process.env.RAILWAY_GIT_COMMIT_SHA=saved; }
});

test('logger failures and hostile provider exceptions cannot interrupt deterministic fallback', async () => {
  const saved=process.env.GEMINI_API_KEY,warn=console.warn; process.env.GEMINI_API_KEY='unused-fixture';
  try {
    for (const err of [violation('model','Field required'),new Proxy({},{get(){throw new Error('bad getter');},getOwnPropertyDescriptor(){throw new Error('bad descriptor');}})]) {
      console.warn=()=>{throw new Error('sink unavailable');};
      const out=await new PresentationEngine({gemini:{generateContent:async()=>{throw err;}}}).present(s);
      assert.equal(out.presentation,'fallback_error'); assert.equal(out.prose,formatDeterministicProse(s));
      assert.equal(out.structured,s); assert.equal(out.metadata.modelCompletion.ok,false);
      assert.doesNotMatch(JSON.stringify(out),/google-request|INVALID_ARGUMENT|Required field missing/);
    }
  } finally {console.warn=warn;if(saved===undefined) delete process.env.GEMINI_API_KEY;else process.env.GEMINI_API_KEY=saved;}
});

test('real WorkspaceEngine ask binds the resolved session and monotonic turn to failure logs', async () => {
  const saved=process.env.GEMINI_API_KEY,warn=console.warn,logs=[];
  process.env.GEMINI_API_KEY='unused-fixture'; console.warn=(...args)=>logs.push(args);
  try {
    const ws=createHermeticWorkspaceEngine({disableLlm:false,gemini:{generateContent:async()=>{throw violation('model','Field required');}},
      operatingEvidenceOpts:{...unavailableOperatingEvidence(),loadProspects:async()=>({available:true,status:'found',counts:{total:6},segments:[]})}});
    const opened=await ws.open({tenantId:'21'}),before=logs.length;
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
    assert.doesNotMatch(JSON.stringify(r),/MAX_PRESENTATION_PROVIDER_FAILURE|google-request|INVALID_ARGUMENT/);
  } finally {console.warn=warn;if(saved===undefined) delete process.env.GEMINI_API_KEY;else process.env.GEMINI_API_KEY=saved;}
});
