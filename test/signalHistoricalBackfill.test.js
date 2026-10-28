'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const {historicalRecord}=require('../services/telegramCallerFeed/historicalBackfill');
const {canonicalize,assignPartition,assertNoProspectiveLeak}=require('../services/signalOpportunities/historicalDataset');
const source={sourceId:'madapes-gambles',sourceRole:'AMPLIFIER',clusterId:'cluster-madapes'};
const msg={channelId:'1758611100',messageId:123,occurredAt:new Date('2026-01-01T00:00:00Z'),text:'CA 11111111111111111111111111111111',url:'https://t.me/x/123',forwarding:null,editDate:null};
test('historical evidence cannot masquerade as prospective knowledge',()=>{const r=canonicalize(historicalRecord(msg,source));assert.equal(r.dataClass,'HISTORICAL_BACKFILL');assert.equal(r.knowledgeAt,null);assert.equal(r.eligibleForLiveScoring,false);assert.doesNotThrow(()=>assertNoProspectiveLeak(r));});
test('holdout assignment is deterministic',()=>{const r=historicalRecord(msg,source);assert.equal(assignPartition(r),assignPartition(r));});
test('leak guard fails closed',()=>assert.throws(()=>assertNoProspectiveLeak({dataClass:'HISTORICAL_BACKFILL',knowledgeAt:'2026-01-01T00:00:00Z',eligibleForLiveScoring:false}),/historical_prospective_leak/));

test('all mentions of the same token stay in one partition across sources',()=>{const a=historicalRecord(msg,source);const b={...a,sourceId:'other-source',channelId:'999',messageId:999};assert.equal(assignPartition(a),assignPartition(b));});
