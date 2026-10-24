'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { loadTenantRepository, loadRepository } = require('../packages/max/scoutAcquisition/ExistingIntelligence');
const { loadScoutFunnelStock } = require('../services/outboundInventory');
const { runDiscoveryPipeline } = require('../packages/scout/DiscoveryPipeline');

test('existing intelligence fails explicitly for first or second database read and preserves genuine empty', async () => {
  for (const failAt of [1, 2]) {
    let calls = 0;
    await assert.rejects(loadTenantRepository({ tenantId: '10', pool: { query: async () => {
      if (++calls === failAt) throw Error('private database detail');
      return { rows: [] };
    } } }), { code: 'existing_intelligence_unavailable', message: 'Existing tenant intelligence is unavailable.' });
  }
  assert.deepEqual(await loadTenantRepository({tenantId:'10', pool:{query:async()=>({rows:[]})}}), {companies:[],people:[]});
  await assert.rejects(loadTenantRepository({tenantId:'10',pool:{query:async()=>({})}}), {code:'existing_intelligence_unavailable'});
  const empty = await loadRepository({tenantId:'10', companies:[]});
  assert.equal(empty.sufficient, false);
  assert.equal(empty.retrievedBeforeInvestigate, true);
  await assert.rejects(loadRepository({tenantId:'10',loadCompanies:async()=>null}), {code:'existing_intelligence_unavailable'});
});

test('funnel failure is unknown, not measured zero; successful empty aggregate stays zero', async () => {
  for (const failAt of [1, 2]) {
    let calls = 0;
    const result = await loadScoutFunnelStock({query:async()=>{
      if (++calls === failAt) throw Error('fixture failure');
      return {rows:[{discovered:0,admitted:0,pending:0,unresolved:0,n:0}]};
    }},10);
    assert.equal(result.available,false);
    for (const key of ['discovered','fit','admittedToEnrichment','enrichmentPending','unresolved','promotedVerified','permanentlyRejected']) assert.equal(result[key],null);
  }
  const zero = await loadScoutFunnelStock({query:async()=>({rows:[{discovered:0,admitted:0,pending:0,unresolved:0,n:0}]})},10);
  assert.equal(zero.available,true);
  assert.equal(zero.discovered,0);
  assert.equal(zero.promotedVerified,0);
  assert.equal((await loadScoutFunnelStock(null,10)).available,false);
  for (const rows of [[], [{}], [{discovered:'bad', admitted:0, pending:0, unresolved:0, n:0}]]) {
    assert.equal((await loadScoutFunnelStock({query:async()=>({rows})},10)).available,false);
  }
});

test('pipeline blocks on unavailable existing intelligence before invoking discovery adapters', async () => {
  let providerCalls = 0;
  const result = await runDiscoveryPipeline({mission:{tenantId:'10',objective:'Find law firms in Manchester NH'},opts:{
    persistMemory:false,
    loadCompanies:async()=>{throw Error('fixture retrieval unavailable');},
    discover:async()=>{providerCalls++; return [];},
    discoveryAdapters:[{discover:async()=>{providerCalls++;return [];}}],
  }});
  assert.equal(providerCalls,0);
  assert.equal(result.outcome,'DISCOVERY_BLOCKED');
  assert.equal(result.blockReason,'existing_intelligence_unavailable');
  assert.equal(result.emptyMarketDecision,false);
  assert.equal(result.existingIntelligence.available,false);
  assert.equal(result.existingIntelligence.companyCount,null);
  assert.equal(result.confidence,null);
  assert.equal(result.qualifiedCount,null);
  assert.equal(result.coverageEngineUsed,false);
  assert.equal(result.coverageMetrics,null);
});

test('multi-tenant tick records one failure without starving the next tenant or changing its result', async t => {
  const governed = require('../services/governedOutbound');
  const seen = [];
  t.mock.method(governed,'productionService',(_pool,{tenantId})=>({tick:async()=>{
    seen.push(tenantId);
    if (tenantId === '13') throw Object.assign(Error('private detail'),{code:'fixture_tick_error'});
    return {halted:'uncertain_send',sent:0};
  }}));
  const result = await require('../anchorDailyOutboundCron').run({pool:{},tenantIds:['13','17'],allTenants:true});
  assert.deepEqual(seen,['13','17']);
  assert.deepEqual(result.tenants['13'],{status:'failed',halted:'tenant_tick_failed',error:'fixture_tick_error'});
  assert.deepEqual(result.tenants['17'],{halted:'uncertain_send',sent:0});
});

test('control retains unavailable funnel through reporting and never invokes Scout for that deficit', async () => {
  const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
  let scoutCalls = 0;
  const events = [];
  const result = await runMaxOutboundControlLoop({
    tenantId:'13', now:new Date('2026-10-21T15:00:00Z'),
    pool:{query:async()=>{throw Error('fixture evidence unavailable');}},
    program:{id:'fixture',tenant_id:'13',mode:'active',source_mission_id:'source',policy:{dailyCap:5}},
    source:{id:'source',payload:{}},
    store:{tenantId:'13',clientId:13,event:async(type,key,payload)=>events.push(payload)},
    infrastructure:{cap:5,snapshot:{sentToday:0},operating:{recommendedSafeDailyCapacity:5,authorizationLimitedCapacity:5,
      dispatchCapacityNow:5,planningDailyCapacity:5,dispatchableDailyCapacity:5,effectiveDailyCapacity:5,governor:'proceed'}},
    inventory:{clean:[],excluded:[]},inventoryAfter:{clean:[],excluded:[]},
    scoutRamp:async()=>{scoutCalls++;throw Error('must not run');},
    logger:{log(){},warn(){}},
  });
  assert.equal(scoutCalls,0);
  assert.equal(result.funnel.available,false);
  assert.equal(result.funnel.discovered,null);
  assert.equal(events[0].discoveryBlockedReason,'scout_funnel_unavailable');
  assert.equal(events[0].funnel.promotedVerified,null);
  assert.equal(events[0].scoutInvoked,false);
});

test('legacy intelligence consumers cannot convert repository failure into completed empty coverage', async () => {
  const mission = {id:'fixture',tenantId:'10',clientId:10,
    objectiveText:'Acquire one recurring commercial cleaning client in Manchester NH',
    constraints:{vertical:'property_management',locationHint:'Manchester NH',industry:'commercial_cleaning'}};
  let providerCalls = 0;
  const opts = {persistMemory:false,loadCompanies:async()=>{throw Error('fixture unavailable');},
    discover:async()=>{providerCalls++;return [];}};
  const {runIntelligencePipeline} = require('../packages/scout/intelligence/Pipeline');
  await assert.rejects(runIntelligencePipeline({mission,opts}),{code:'existing_intelligence_unavailable'});
  const {runInvestigationEngine} = require('../packages/scout/investigation/InvestigationLoop');
  await assert.rejects(runInvestigationEngine({mission,opts}),{code:'existing_intelligence_unavailable'});
  assert.equal(providerCalls,0);
});

test('canonical discovery preserves unknown evidence without completing or overwriting the mission', async t => {
  const {discover,syncMissionFromPipeline} = require('../packages/scout/Discovery');
  const {formatDiscoveryOperatorResponse} = require('../packages/mission-engine/discoveryExecutionReport');
  const facade = require('../services/acquisitionMission');
  let attached = 0, updates = 0;
  t.mock.method(facade,'attachScoutDiscovery',async()=>{attached++;});
  const mission = {id:'fixture',tenantId:'10',status:'executing',orchestrationMissionId:'fixture',
    objectiveText:'Find law firms in Manchester NH',plan:{steps:[{capabilityId:'prospect_discovery',status:'pending'}]},
    deliverables:{stepResults:[{capabilityId:'prior',result:{verified:true}}]}};
  const before = structuredClone(mission);
  const missionEngine = {store:{update:async()=>{updates++;}}};
  const result = await discover({mission,missionEngine,opts:{persistMemory:false,
    loadCompanies:async()=>{throw Error('fixture retrieval unavailable');}}});
  assert.equal(result.outcome,'DISCOVERY_BLOCKED');
  assert.equal(result.prospectCount,null);
  assert.equal(result.discoveryReport.prospectCount,null);
  assert.equal(result.discoveryReport.externalDiscoveryAttempted,false);
  assert.equal(result.discoveryReport.coverageEngineUsed,false);
  assert.equal(result.discoveryReport.missionUpdated,false);
  assert.equal(result.pipeline.qualifiedCount,null);
  assert.equal(result.pipeline.coverageEngineUsed,false);
  assert.equal(updates,0);
  assert.equal(attached,0);
  assert.deepEqual(mission,before);
  assert.match(formatDiscoveryOperatorResponse(result.discoveryReport),/unverified/);
  assert.doesNotMatch(formatDiscoveryOperatorResponse(result.discoveryReport),/completed|No verified prospects|Mission Updated/);
  assert.equal(await syncMissionFromPipeline({mission,missionEngine,pipelineResult:result.pipeline}),mission);
  assert.equal(updates,0);
});
