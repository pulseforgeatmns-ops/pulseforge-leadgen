'use strict';
const test=require('node:test');const assert=require('node:assert/strict');const {sourceStats,sequenceStats}=require('../services/signalOpportunities/historicalStats');
const outcome={minutes:60,status:'AVAILABLE',mfePct:120,maePct:-10,endReturnPct:40,timeTo2xMinutes:30,timeTo3xMinutes:null};
test('stats exclude frozen holdout',()=>{const rows=[{partition:'HISTORICAL_DISCOVERY',sourceId:'a',market:{outcomes:[outcome]},attention:[{sourceId:'b',lagMinutes:12}]},{partition:'HISTORICAL_HOLDOUT',sourceId:'a',market:{outcomes:[{...outcome,mfePct:-99}]},attention:[]}];const s=sourceStats(rows)[0];assert.equal(s.n,1);assert.equal(s.medianMfePct,120);assert.equal(sequenceStats(rows)[0].sequence,'a->b');});
