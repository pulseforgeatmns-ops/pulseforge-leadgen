'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const { enforcePolicy, mergePolicy, validatePolicyPatch } = require('../utils/apiSpendControl');
const { estimateClaudeUsage, estimateClaudeReservation, sanitizedUsage } = require('../utils/apiPricing');
const { meterClient } = require('../utils/meteredAnthropic');
const { buildReport, getApiUsageReport, isApiUsageQuestion, parseApiSpendCommand } = require('../services/apiUsageService');
const { parseArgs } = require('../scripts/apiUsageAudit');

const empty = { daily_calls: 0, monthly_calls: 0, daily_usd: 0, monthly_usd: 0, daily_unknown: 0, monthly_unknown: 0 };
test('zero, daily and monthly budgets stop attempts; unknown pricing cannot pass a dollar cap', () => {
  assert.throws(() => enforcePolicy({ daily_request_limit: 0 }, empty, 0), /request_limit/);
  assert.throws(() => enforcePolicy({ monthly_request_limit: 2 }, { ...empty, monthly_calls: 2 }, 0), /monthly_request_limit/);
  assert.throws(() => enforcePolicy({ daily_usd_limit: 1 }, { ...empty, daily_usd: 0.99 }, 0.025), /daily_usd_limit/);
  assert.throws(() => enforcePolicy({ monthly_usd_limit: 50 }, empty, null), /cost_unknown/);
  assert.throws(() => enforcePolicy({ daily_usd_limit: 1 }, { ...empty, daily_unknown: 1 }, 0), /cost_unknown/);
  assert.throws(() => enforcePolicy({ paused: true }, empty, 0), /provider_paused/);
  assert.doesNotThrow(() => enforcePolicy({ daily_usd_limit: 0.05 }, { ...empty, daily_usd: 0.025 }, 0.025));
});
test('Max can tighten or pause, but never raise, remove or resume a limit', () => {
  const policy = { daily_usd_limit: '5', monthly_request_limit: 100, paused: true };
  assert.equal(mergePolicy(policy, { daily_usd_limit: 3 }, 'max').daily_usd_limit, 3);
  assert.throws(() => mergePolicy(policy, { daily_usd_limit: 6 }, 'max'), /cannot_increase/);
  assert.throws(() => mergePolicy(policy, { daily_usd_limit: null }, 'max'), /cannot_increase/);
  assert.throws(() => mergePolicy(policy, { paused: false }, 'max'), /cannot_resume/);
  assert.equal(mergePolicy(policy, { paused: false, monthly_request_limit: null }, 'operator').paused, false);
  for (const patch of [{ daily_usd_limit: -1 }, { daily_usd_limit: '10' }, { daily_request_limit: 1.5 }, { actor: 'operator' }, { paused: 'false' }]) assert.throws(() => validatePolicyPatch(patch), /invalid_api_policy/);
});
test('Claude cost uses separate uncached, cached, cache-write and output tokens', () => {
  const usage = { input_tokens: 1000, output_tokens: 100, cache_read_input_tokens: 1000,
    cache_creation_input_tokens: 2000, cache_creation: { ephemeral_5m_input_tokens: 1000, ephemeral_1h_input_tokens: 1000 } };
  assert.equal(estimateClaudeUsage('claude-sonnet-4-6', usage), (1000+100+1250+2000)*3/1e6 + 100*15/1e6);
  assert.equal(estimateClaudeUsage('unpriced-future-model', usage), null);
  assert.equal(estimateClaudeUsage('claude-sonnet-4-6', usage, { inference_geo: 'us' }), null);
  assert.equal(estimateClaudeUsage('claude-sonnet-4-6', {}), null);
  assert.equal(estimateClaudeReservation({ model: 'claude-sonnet-4-6', max_tokens: 100, messages: [{role:'user',content:[{type:'image',source:{type:'base64',data:'fake'}}]}] }), null);
  assert.deepEqual(sanitizedUsage({ input_tokens: 12, output_tokens: -1, prompt: 'secret', api_key: 'secret' }), { input_tokens: 12 });
  assert.equal(estimateClaudeUsage('claude-opus-5-5', { input_tokens:1000, output_tokens:100, cache_read_input_tokens:1000 }), 0.0062);
});
test('meter records identity and usage without storing prompts and disables untracked retries', async () => {
  const response = { model: 'claude-haiku-4-5', content: [{ type: 'text', text: 'answer' }], usage: { input_tokens: 20, output_tokens: 10 } };
  const records = [], optionsSeen = []; let tenant = 10;
  const client = meterClient({ messages: { create: async (_p, opts) => { optionsSeen.push(opts); return response; } } }, {
    agentName: 'paige', tenantId: () => tenant,
    reserve: async metadata => { records.push(metadata); return 'reservation'; },
    settle: async (_id, outcome) => records.push(outcome),
  });
  const request = { model: response.model, max_tokens: 100, messages: [{role:'user',content:'private prompt'}] };
  assert.equal(await client.messages.create(request), response);
  tenant = 13;
  await client.messages.create(request, { maxRetries: 10 });
  assert.equal(records[0].tenantId,10); assert.equal(records[2].tenantId,13);
  assert.equal(optionsSeen[1].maxRetries,0);
  assert.ok(!JSON.stringify(records).includes('private prompt'));
  assert.equal(records[1].estimatedUsd,0.00007);
});
test('no paid request on denied reservation, and settlement failures never repeat a successful call', async () => {
  let calls=0;
  const blocked=meterClient({messages:{create:async()=>{calls++;}}},{reserve:async()=>{throw new Error('api_daily_request_limit');}});
  await assert.rejects(blocked.messages.create({model:'claude-sonnet-4-6',max_tokens:100}),/api_daily_request_limit/);
  assert.equal(calls,0);
  const response={content:[{type:'text',text:'done'}],usage:{input_tokens:1,output_tokens:1}};
  const client=meterClient({messages:{create:async()=>{calls++;return response;}}},{reserve:async()=> 'id',settle:async()=>{throw new Error('db unavailable');}});
  assert.equal(await client.messages.create({model:'claude-sonnet-4-6',max_tokens:100}),response);
  assert.equal(calls,1);
});
test('failed provider call remains uncertain rather than refunded or treated as free', async () => {
  const outcomes=[];
  const client=meterClient({messages:{create:async()=>{throw Object.assign(new Error('provider failed'),{status:503});}}},{reserve:async()=> 'id',settle:async(_id,outcome)=>outcomes.push(outcome)});
  await assert.rejects(client.messages.create({model:'claude-sonnet-4-6',max_tokens:100}),/provider failed/);
  assert.equal(outcomes[0].status,'uncertain');assert.equal(outcomes[0].estimatedUsd,undefined);
});
test('missing meters and unknown prices never become a complete zero-dollar bill', async () => {
  const queries=[];
  const r=await getApiUsageReport({db:{query:async sql=>{queries.push(sql);return {rows:[{places:null,events:null,policies:null}]};}},now:new Date('2026-10-14T12:00:00Z')});
  assert.equal(r.billing.actualUsd,null);assert.equal(r.periods.requested.estimatedGrossUsd,null);
  assert.equal(r.providers[1].requested.estimatedGrossUsd,null);
  assert.ok(queries.every(sql=>!/^\s*(CREATE|INSERT|UPDATE|ALTER|DELETE)/i.test(sql)));
  const priced=buildReport({rows:[{day:'2026-10-14',provider:'anthropic',requests:1,known_gross_usd:0,unknown_cost_requests:1}],availability:{events:true,policies:true},earliest:{anthropic:'2026-10-14T10:00:00Z'},requestedSince:'2026-10-01T00:00:00.000Z',until:'2026-10-14T12:00:00.000Z',now:new Date('2026-10-14T12:00:00Z')});
  assert.equal(priced.periods.requested.estimatedGrossUsd,null);
  assert.ok(priced.alerts.some(a=>a.code==='partial_history'));
});
test('spend questions are deterministic and unrelated sales/ads requests are not intercepted', () => {
  for (const q of ['audit our API usage','API spend for the last two weeks','Google Places billing','Anthropic token cost']) assert.equal(isApiUsageQuestion(q),true,q);
  for (const q of ['spend $25 on Google Ads','create an outbound campaign','what is our sales budget']) assert.equal(isApiUsageQuestion(q),false,q);
  assert.deepEqual(parseApiSpendCommand('Max, cap Google Places at $5 per day'),{provider:'google_places',patch:{daily_usd_limit:5}});
  assert.deepEqual(parseApiSpendCommand('Pause Claude API requests'),{provider:'anthropic',patch:{paused:true}});
  assert.equal(parseApiSpendCommand('could we perhaps increase the API budget?'),null);
});
test('audit CLI requires explicit production flag and rejects SQL, URLs or unexpected arguments', () => {
  assert.deepEqual(parseArgs(['--days','14','--json','--confirm-production']),{days:14,json:true,confirm:true,help:false});
  for (const args of [['--sql','SELECT 1'],['--days','NaN'],['--days','500']])assert.throws(()=>parseArgs(args),/invalid_arguments/);
});
test('runtime clients use the shared meter, and operator route excludes client/viewer roles', () => {
  for (const file of ['maxAgent.js','paigeAgent.js','rileyAgent.js','rexAgent.js','routes/maxChat.js','routes/maxWorkspace.js','routes/cron.js','routes/webhooks.js']) {
    const source=fs.readFileSync(require('path').join(__dirname,'..',file),'utf8');
    assert.ok(source.includes('meteredAnthropic'),file);
    assert.ok(!source.includes("require('@anthropic-ai/sdk')"),file);
  }
  const route=fs.readFileSync(require('path').join(__dirname,'../routes/apiUsage.js'),'utf8');
  assert.match(route,/requireRole\('admin', 'manager'\)/);
});

test('budget denial does not become a phantom paid Places entry, and cached results still work', async () => {
  const { legacyTextSearch } = require('../utils/placesApi');
  const { clearPlacesQueryCache } = require('../utils/placesQueryCache');
  clearPlacesQueryCache();
  let calls=0, entries=0, settled=0;
  const deps={db:{query:async sql=>{if (/INSERT INTO places_api_requests/.test(sql)) entries++;return {rows:[]};}},
    reserveApiCall:async()=>{throw new Error('api_daily_request_limit');},settleApiCall:async()=>{settled++;}};
  const params={query:'guard-only test query',apiKey:'test-only',fetchImpl:async()=>{calls++;return {status:200,ok:true,json:async()=>({status:'OK',results:[]})};}};
  await assert.rejects(legacyTextSearch(params,deps),/api_daily_request_limit/);
  assert.equal(calls,0);assert.equal(entries,0);
  deps.reserveApiCall=async()=> 'reservation';
  await legacyTextSearch(params,deps);
  deps.reserveApiCall=async()=>{throw new Error('api_provider_paused');};
  await legacyTextSearch(params,deps);
  assert.equal(calls,1);assert.equal(settled,1);
});
test('empty saved policies remain visibly unbounded and partial-day report buckets are respected', () => {
  const r=buildReport({rows:[{day:'2026-09-30',in_requested:true,provider:'google_places',requests:1,known_gross_usd:0.04,unknown_cost_requests:0},
    {day:'2026-09-30',in_requested:false,provider:'google_places',requests:2,known_gross_usd:0.08,unknown_cost_requests:0}],
    policies:[{provider:'google_places',paused:false}],availability:{events:true,places:true,policies:true},earliest:{google_places:'2026-09-01T00:00:00Z'},
    requestedSince:'2026-09-30T12:00:00.000Z',until:'2026-10-14T12:00:00.000Z',now:new Date('2026-10-14T12:00:00Z')});
  assert.equal(r.periods.requested.requests,1);assert.equal(r.providers[0].limitsConfigured,false);
  assert.ok(r.alerts.some(a=>a.provider==='google_places' && a.code==='budget_not_configured'));
});
