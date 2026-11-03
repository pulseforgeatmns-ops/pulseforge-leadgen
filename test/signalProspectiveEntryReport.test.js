'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const {reportProspectiveEntries:report}=require('../services/signalOpportunities/prospectiveEntryReport');
const at=m=>new Date(Date.parse('2026-11-01T00:00:00Z')+m*60000).toISOString();
const call=(role,m,tokenAddress='A')=>({category:'CALLER',sourceRole:role,tokenAddress,source:role,occurredAt:at(m),knownAt:at(m)});
const quote=(m,p,known=m)=>({tokenAddress:'A',observedAt:at(m),knownAt:at(known),priceUsd:p,liquidityUsd:20000});
test('reports prospective entries without including historical or holdout records',()=>{
 const result=report({asOf:at(70),calls:[call('EARLY_CALLER',0),call('AMPLIFIER',2),{...call('AMPLIFIER',1,'B'),dataClass:'HISTORICAL_BACKFILL',partition:'HISTORICAL_HOLDOUT'}],observations:[quote(3,1),quote(5,1.5),quote(13,2)]});
 assert.equal(result.eligibleEntries,1);assert.equal(result.summary['10m'].observed,1);assert.equal(result.records[0].entryAt,at(3));
});
test('missing amplifier produces an explicit exclusion',()=>{
 const result=report({asOf:at(20),calls:[call('EARLY_CALLER',0)],observations:[]});
 assert.equal(result.eligibleEntries,0);assert.equal(result.exclusions.NO_AMPLIFIER,1);
});
test('unknown confirmation clock cannot be inferred from historical posting time',()=>{
 const result=report({asOf:at(20),calls:[call('EARLY_CALLER',0),{...call('AMPLIFIER',2),occurredAt:null}],observations:[quote(3,1)]});
 assert.equal(result.exclusions.UNOBSERVABLE_CONFIRMATION,1);
});
test('unmatured horizons remain pending',()=>{
 const result=report({asOf:at(6),calls:[call('EARLY_CALLER',0),call('AMPLIFIER',2)],observations:[quote(3,1),quote(5,2)]});
 assert.equal(result.summary['60m'].pending,1);
});
