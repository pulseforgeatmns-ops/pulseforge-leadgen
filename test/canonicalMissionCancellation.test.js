'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const amo=require('../packages/acquisition-mission');
const {executeCanonical,activeMissionFor}=require('../services/acquisitionMission');

test('canonical cancellation persists before hydration and stays cancelled during refresh',async()=>{
 const engine=amo.createAcquisitionMissionEngine();
 const mission=engine.create({tenantId:'13',objective:'Acquire customers from law firms in Boston, MA.'});
 let persisted;
 const runtime={hydrate:async()=>{},engine:()=>engine,persistMissionState:async id=>{persisted=structuredClone(engine.get(id,'13'));}};
 const result=await executeCanonical({tenantId:'13',missionId:mission.id,intent:amo.EXECUTION_INTENTS.CANCEL_PLAN,operatorId:'operator',question:'Cancel failed validation.'},{runtime});
 assert.equal(result.executionResult.cancelled,true);assert.equal(persisted.planCancelled,true);assert.equal(persisted.status,'Cancelled');
 const restored=amo.createAcquisitionMissionEngine();restored.store.putMission(persisted);
 assert.equal(restored.inspect(mission.id,{tenantId:'13'}).mission.status,'Cancelled');
 assert.equal(activeMissionFor('13',{runtime}),null);
});
