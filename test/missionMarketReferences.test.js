'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const {resolveMarketScopeFromObjective}=require('../packages/acquisition-mission/MissionNaming');
const {resolveCanonicalObjective}=require('../packages/max/workspace/ResolvedObjective');

test('ICP and engagement references do not invent a market segment',()=>{
 const objective='Over the next 90 days, the most important outcome is proving that Babrun can reliably acquire the right founder customers for the 12-week program. The immediate milestone is enrolling the first founder student from the target ICP and learning from that real engagement. Beyond that, success would mean validating which customer segments respond most strongly, which problems actually drive them to act, and establishing a repeatable acquisition process that can consistently create qualified conversations and enrollments. I’d rather prove the model with a small number of genuinely good-fit customers than optimize for lead volume.';
 assert.equal(resolveMarketScopeFromObjective(objective).primarySegment,null);
 const result=resolveCanonicalObjective({question:'',targetSegment:'Small Business Owners',context:{tenantId:'13',summary:{approved:true,campaignGoals:objective,geography:'United States'}}});
 assert.equal(result.objective,objective);assert.equal(result.segmentKey,'small_business_owners');assert.equal(result.ready,true);
});
test('explicit acquisition segments still resolve',()=>{
 assert.equal(resolveMarketScopeFromObjective('Acquire customers from law firms in Greater Manchester, NH.').primarySegment,'law_firm');
 assert.equal(resolveMarketScopeFromObjective('Acquire customers from independent bookshops.').primarySegment,'independent_bookshops');
});
