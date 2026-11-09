'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { ShadowInterpreter } = require('../ShadowInterpreter');
const { ModelFrameProducer } = require('../producers/ModelFrameProducer');
const { validateTurnFrame } = require('../TurnFrame');
const context = { tenantId: '72', candidates: [], focusIds: [], pendingDecisionId: null };
const raw = { intent: 'question', subjectKind: 'portfolio', relationToPrior: 'new_topic', referents: [], evidenceNeeds: ['mission_lifecycle'], facets: ['status'], requestedOutcome: 'status', requestedAction: {verb:'none', targetCandidateId:null, polarity:'none'}, scope:'portfolio', foreignTenantReference:false, ambiguities:[], confidence:0.9 };

test('flag off performs no capture, call or persistence', async () => {
  const shadow = new ShadowInterpreter({env:{}, producer:{produce(){throw Error('called');}}, persist(){throw Error('written');}});
  assert.equal(shadow.capture({context:{tenantId:'72'}}), null);
  shadow.observe({context, text:'Any updates?', result:{}});
  await shadow.drain();
  assert.equal(shadow.counts.completed, 0);
});
test('injected model runs after the response task and persists bounded metadata without text', async () => {
  const rows = [];
  let called = false;
  const producer = new ModelFrameProducer({client:{messages:{create:async () => {
    called = true;
    return {stop_reason:'end_turn', content:[{type:'text',text:JSON.stringify(raw)}], usage:{input_tokens:50,output_tokens:30}};
  }}}});
  const observer = new ShadowInterpreter({env:{MAX_INTERPRETATION_SHADOW:'true'},producer,persist:row => rows.push(row)});
  const session = {context:{tenantId:'72',missionId:'m72'}, operationalFocus:{tenantId:'72',missionIds:['m72'],at:new Date().toISOString()}};
  const before = JSON.stringify(session);
  const captured = observer.capture(session);
  observer.observe({context:captured,text:'Confidential operator content',result:{route:'inspection',workspaceOwnership:{owner:'mission_inspection'}}});
  assert.equal(called,false);
  await observer.drain();
  assert.equal(JSON.stringify(session),before);
  assert.equal(rows.length,1);
  assert.equal(rows[0].disagreement,false);
  assert.equal(rows[0].tenantId,'72');
  assert.equal(rows[0].usage.input_tokens,50);
  assert.equal(rows[0].shadowOnly,true);
  assert.doesNotMatch(JSON.stringify(rows),/Confidential operator content/);
});
test('failure, queue bound and rollback cannot reject a production turn', async () => {
  let release;
  const block = new Promise(resolve => {release=resolve;});
  const observer = new ShadowInterpreter({env:{MAX_INTERPRETATION_SHADOW:'true'},producer:{produce:async () => {await block;throw Error('secret');}}});
  for(let n=0;n<25;n++) observer.observe({context,text:'x',result:{}});
  assert.equal(observer.pending.size,16);
  assert.equal(observer.counts.dropped,9);
  observer.enabled=false;
  observer.observe({context,text:'x',result:{}});
  release();
  await observer.drain();
  assert.equal(observer.counts.failed,16);
});

test('real WorkspaceEngine keeps a shadow approval nomination away from execution', async()=>{
  const {createHermeticWorkspaceEngine}=require('../../workspace/tests/workspaceTestHarness');
  const amo=require('../../../acquisition-mission');
  const rows=[];
  const observer=new ShadowInterpreter({env:{MAX_INTERPRETATION_SHADOW:'true'},producer:{produce:async()=>({frame:{...validateTurnFrame(raw,{...context,producer:'model'}),intent:'authorization',executionCandidate:true},telemetry:{latencyMs:1,usage:null,model:'test',degradedReason:null}})},persist:row=>rows.push(row)});
  const ws=createHermeticWorkspaceEngine({interpretationShadow:observer});
  const opened=await ws.open({tenantId:'72',page:'command-deck'});
  amo.clearExecutionRouterAudit();
  const answer=await ws.ask({sessionId:opened.sessionId,question:'Hello Max'});
  await observer.drain();
  assert.ok(answer.prose);
  assert.equal(rows.length,1);
  assert.deepEqual(amo.listExecutionRouterAudit(),[]);
  assert.equal(answer.interpretationFrame,undefined);
});

test('malformed session/legacy metadata and rejecting telemetry stay isolated',async()=>{
  const observer=new ShadowInterpreter({env:{MAX_INTERPRETATION_SHADOW:'true'},audit:async()=>{throw Error('audit down');}});
  assert.equal(observer.capture({context:{tenantId:'72'},operationalEvidence:{tenantId:'72',missions:{}}}),null);
  observer.observe({context,text:'x',result:{workspaceOwnership:{owner:42}}});
  await observer.drain();
  assert.equal(observer.counts.failed,2);
});
