'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {createTelegramCallerFeedEngine}=require('../services/telegramCallerFeed/engine');
const {candidateSources,RELATIONSHIPS}=require('../services/signalOpportunities/sourceUniverse');
test('source universe never asserts independence and unavailable identities remain candidates',()=>{
 assert.ok(candidateSources().length>=20);assert.ok(candidateSources().every(s=>s.identityStatus==='CANDIDATE'));assert.ok(RELATIONSHIPS.every(r=>r.status==='CORRELATED'&&r.evidence));assert.equal(candidateSources().find(s=>s.sourceId==='ourcryptohood').username,null);
});
test('activation requires authenticated public-channel read, matching title and username; numeric identity is pinned across restart',async t=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'source-pin-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
 const key='TELEGRAM_CALLER_SOURCES_JSON',prior=process.env[key];t.after(()=>prior===undefined?delete process.env[key]:process.env[key]=prior);
 const source=candidateSources().find(s=>s.sourceId==='pows-gem-calls');process.env[key]=JSON.stringify([source]);
 let entity={id:123,username:'PowsGemCalls',broadcast:true,title:"Pow’s Gem Calls"},deny=false;
 const client={getEntity:async()=>entity,getMessages:async()=>{if(deny)throw Error('CHANNEL_PRIVATE');return [{id:9}];}};
 const make=()=>createTelegramCallerFeedEngine({client,statePath:path.join(dir,'state.json')});
 let engine=make(),r=await engine.bootstrapChannels(client);assert.equal(r[0].identityStatus,'VERIFIED');assert.equal(r[0].channelId,'123');
 entity={...entity,id:456};r=await make().bootstrapChannels(client);assert.equal(r[0].active,false);assert.equal(r[0].reason,'pinned_channel_identity_mismatch');
 entity={...entity,id:123,title:'Imposter'};r=await make().bootstrapChannels(client);assert.equal(r[0].active,false);
 entity={...entity,title:"Pow’s Gem Calls",broadcast:false};r=await make().bootstrapChannels(client);assert.equal(r[0].active,false);
 entity={...entity,broadcast:true};deny=true;r=await make().bootstrapChannels(client);assert.equal(r[0].active,false);assert.equal(r[0].identityStatus,'UNVERIFIED');
});
