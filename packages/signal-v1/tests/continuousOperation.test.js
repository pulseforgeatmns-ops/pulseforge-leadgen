'use strict';
const {test}=require('node:test');const assert=require('node:assert/strict');
const {operationFromEnv}=require('../../../services/signalOperator/operationGate');
const {buildActivationBundle}=require('../../../scripts/buildSignalDeploymentBundle');
const base={SIGNAL_OPERATION_MODE:'continuous',SIGNAL_OPERATION_START_AT:'2026-10-07T00:00:00Z',SIGNAL_MONTHLY_BUDGET_USD:'10'};
test('continuous operation requires an explicit monthly model but has no pilot dependencies or expiry',()=>{
  const guard=new Proxy({...base,SIGNAL_PILOT_REQUIRED:'1',SIGNAL_PILOT_EXPIRES_AT:'2026-10-08T00:00:00Z'},{get(target,key){
    if(['SIGNAL_RAILWAY_PROJECT_TOKEN','SIGNAL_PILOT_READINESS_PATH','SIGNAL_PILOT_EXPIRES_AT'].includes(key))throw new Error('continuous must not read pilot prerequisites');return target[key];}});
  const gate=operationFromEnv(guard,{now:()=>new Date('2027-10-07T00:00:00Z')});
  assert.equal(gate().ok,true);assert.equal(gate().expiresAt,undefined);assert.equal(gate().budgetEnforcement,'monitoring_only');
  assert.equal(operationFromEnv({})().ok,false);
  for(const value of ['', '0', '-1','NaN'])assert.equal(operationFromEnv({...base,SIGNAL_MONTHLY_BUDGET_USD:value})().ok,false);
});
test('unselected deployment cannot silently impose a pilot or start collection; continuous bundle has one auth dependency',()=>{
  const probe={source:'frontrunz',username:'frontrunz',accessible:true,messageReadSucceeded:true,channelId:'123'};
  const blocked=buildActivationBundle(probe);assert.equal(blocked.activationBlocked,true);
  assert.equal(blocked.phase1.variables.SIGNAL_OPERATION_MODE,'unselected');
  const bundle=buildActivationBundle(probe,{mode:'continuous',monthlyBudgetUsd:10,startAt:'2026-10-07T00:00:00Z'});
  assert.equal(bundle.activationBlocked,false);assert.equal(bundle.phase2.variables.SIGNAL_OPERATION_MODE,'continuous');
  assert.equal(bundle.phase2.variables.SIGNAL_PILOT_REQUIRED,undefined);
  assert.equal(JSON.stringify(bundle).includes('SIGNAL_RAILWAY_PROJECT_TOKEN'),false);
  assert.equal(JSON.stringify(bundle).includes('SIGNAL_PILOT_READINESS_PATH'),false);
});
test('continuous mode cannot start the optional management controller even if a stale enable flag remains',async()=>{
  const oldMode=process.env.SIGNAL_OPERATION_MODE,oldFlag=process.env.SIGNAL_PILOT_CONTROLLER_ENABLED;
  process.env.SIGNAL_OPERATION_MODE='continuous';process.env.SIGNAL_PILOT_CONTROLLER_ENABLED='1';
  try{assert.equal(await require('../../../services/signalOperator/optionalController').startOptionalPilotController(),null);}
  finally{if(oldMode===undefined)delete process.env.SIGNAL_OPERATION_MODE;else process.env.SIGNAL_OPERATION_MODE=oldMode;
    if(oldFlag===undefined)delete process.env.SIGNAL_PILOT_CONTROLLER_ENABLED;else process.env.SIGNAL_PILOT_CONTROLLER_ENABLED=oldFlag;}
});
