'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const {evaluatePostConfirmation:e}=require('../services/signalOpportunities/postConfirmationResearch');
const record={partition:'HISTORICAL_DISCOVERY',dataClass:'HISTORICAL_BACKFILL',tokenAddress:'A'};
const base=Date.parse('2026-10-01T00:00:00Z');
const at=m=>new Date(base+m*60000).toISOString();
const observation=(m,price,known=m)=>({tokenAddress:'A',observedAt:at(m),knownAt:at(known),priceUsd:price,liquidityUsd:10000});
const confirmation={observedAt:at(0),knownAt:at(2)};
test('holdout rejected before touching market observations',()=>{
 assert.throws(()=>e({record:{...record,partition:'HISTORICAL_HOLDOUT'},confirmation,observations:[]}),/discovery_historical_only/);
});
test('never enters before observable confirmation plus delay',()=>{
 const result=e({record,confirmation,observations:[observation(1,1),observation(2,2),observation(3,4),observation(4,8),observation(13,12)],delayMs:60000});
 assert.equal(result.entryAt,at(3));
 assert.equal(result.entryPriceUsd,4);
 assert.equal(result.outcomes['10m'].netLastPct,200);
});
test('unknown confirmation knowledge clock is not guessed',()=>{
 assert.equal(e({record,confirmation:{observedAt:at(0)},observations:[]}).status,'UNOBSERVABLE_CONFIRMATION');
});
test('no timely market observation means no entry',()=>{
 assert.equal(e({record,confirmation,observations:[observation(10,2)]}).status,'NO_EXECUTABLE_ENTRY');
});
test('late-received market observations are excluded',()=>{
 assert.equal(e({record,confirmation,observations:[observation(3,2,9),observation(4,3,4)]}).status,'EVALUATED');
});
test('costs reduce net outcomes and empty horizons are not fabricated',()=>{
 const result=e({record,confirmation,observations:[observation(3,1),observation(5,1.1)],feeBps:100,slippageBps:100});
 assert.ok(result.outcomes['10m'].netLastPct<10);
 assert.equal(result.outcomes['30m'].status,'OBSERVED_PARTIAL');
 assert.equal(result.quality,'OBSERVATION_BASED_NOT_EXECUTION_PROOF');
});

test('entry is not backdated to market observation time',()=>{
 const result=e({record,confirmation,observations:[observation(3,2,4),observation(5,3,5),observation(14,4,14)]});
 assert.equal(result.entryAt,at(4));
 assert.equal(result.entryMarketObservedAt,at(3));
 assert.equal(result.outcomes['10m'].lastKnownAt,at(14));
});
test('market quote learned after permitted entry lag is ineligible',()=>{
 const result=e({record,confirmation,observations:[observation(3,2,6)]});
 assert.equal(result.status,'NO_EXECUTABLE_ENTRY');
});
