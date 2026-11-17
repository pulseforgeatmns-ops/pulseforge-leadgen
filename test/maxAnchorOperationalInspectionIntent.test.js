'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {classifyOperationalRead}=require('../packages/max/workspace/OperationalReadIntent');
const {claimsOperationalRead}=require('../packages/max/workspace/WorkspaceOwnershipResolver');
const {shouldClaimClientIntelligenceTurn}=require('../packages/max/workspace/ClientIntelligenceContext');
const {detectOperationalInspection}=require('../packages/max/workspace/OperationalInspection');

const PROMPT=[
 'Inspect Anchor Cleaning (tenant 10) specifically.',
 'Show the five highest-priority active opportunities, why each is ranked where it is,',
 'the evidence supporting its priority, and the next action required.',
 "Also identify what's preventing Scout and Emmett from generating new qualified outreach.",
 'Read-only inspection. Do not execute anything.'
].join(' ');

test('exact Anchor prompt is classified as an existing-record read with ranking',()=>{
 const read=classifyOperationalRead(PROMPT);
 assert.equal(read.isRead,true,JSON.stringify(read));
 assert.equal(read.wantsRanking,true);
 assert.ok(read.objects.includes('prospects'),JSON.stringify(read));
 assert.equal(read.topCount,5);
});
test('exact Anchor prompt claims operating records before Blueprint strategy',()=>{
 const claim=claimsOperationalRead(PROMPT);
 assert.ok(claim, 'Max may not answer a ranked record request from Blueprint alone');
 assert.equal(claim.reason,'operational_read');
 assert.equal(claim.operationalRead.wantsRanking,true);
 assert.equal(shouldClaimClientIntelligenceTurn(PROMPT,{context:{tenantId:'10'}},{approvedBlueprint:true}),false);
});
test('exact Anchor prompt is read-only and does not trigger arbitrary actions',()=>{
 const d=detectOperationalInspection(PROMPT);
 assert.notEqual(d.reason,'affirmative_action_command');
 const read=classifyOperationalRead(PROMPT);
 assert.notEqual(read.reason,'execution_language');
});
