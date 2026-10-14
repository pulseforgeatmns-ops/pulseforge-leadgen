'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { GeckoTerminalDistributionProvider } = require('../../../services/signalDistribution/marketProvider');
const { GeckoTerminalMarketDataProvider } = require('../providers/GeckoTerminalMarketDataProvider');
const { sharedRequestBudget, configureSharedRequestBudget } = require('../../../services/signalDistribution/requestBudget');
const token = '11111111111111111111111111111111';
const pool = '22222222222222222222222222222222';
function payload(overrides = {}) {
  return { data: { id: `solana_${pool}`, relationships: { base_token: { data: { id: `solana_${token}` } } }, attributes: {
    address: pool, base_token_price_usd: '0.01', market_cap_usd: null, reserve_in_usd: '20000',
    transactions: { m5: { buys: 0, sells: 4, buyers: 0, sellers: 3 } },
    volume_usd: { m5: '25' }, buy_volume_usd: { m5: '0' }, sell_volume_usd: { m5: '25' },
    net_buy_volume_usd: { m5: '-25' }, ...overrides,
  } } };
}
function setup(data = payload()) {
  let at = Date.parse('2026-10-14T13:00:00Z'); const calls = []; const lanes = []; const cooldowns = [];
  const requestBudget = { consume: async lane => { lanes.push(lane); return true; }, backoff: async at => cooldowns.push(at) };
  const p = new GeckoTerminalDistributionProvider({ now: () => new Date(at), requestBudget,
    fetchFn: async (url, options) => { calls.push({url,options}); return { ok: true, json: async () => data }; } });
  return { p, calls, lanes, cooldowns, advance: ms => { at += ms; }, requestBudget };
}
test('pool flow normalization preserves null and zero; never claims dev or fresh quote', async () => {
  const {p,calls,lanes} = setup(); const s = await p.getSnapshot({tokenAddress:token,poolAddress:pool});
  assert.equal(s.pairId,pool); assert.equal(s.buys5m,0); assert.equal(s.marketCapUsd,null);
  assert.equal(s.buyVolume5mUsd,0); assert.equal(s.netBuyVolume5mUsd,-25);
  assert.equal(s.freshness,'UNKNOWN'); assert.equal(s.providerTimestamp,null); assert.equal(s.devIdentityStatus,'UNKNOWN');
  assert.equal(s.provenance.timestampBasis,'response_received'); assert.deepEqual(lanes,['distribution']);
  assert.match(calls[0].url,/include_volume_breakdown=true/);
});
test('missing, boolean and malformed numeric fields never become zero', async () => {
  const {p} = setup(payload({reserve_in_usd: false, volume_usd:{m5:' '}, transactions:{m5:{buys:null,sells:1.5}}, buy_volume_usd:{m5:'NaN'}}));
  const s=await p.getSnapshot({tokenAddress:token,poolAddress:pool});
  for(const key of ['liquidityUsd','volume5mUsd','buys5m','sells5m','buyVolume5mUsd']) assert.equal(s[key],null);
});
test('cache retains observation time and isolates caller mutation; minute expiry fetches again', async () => {
  const {p,calls,advance}=setup(); const a=await p.getSnapshot({tokenAddress:token,poolAddress:pool});
  a.priceUsd=999; advance(59000); const b=await p.getSnapshot({tokenAddress:token});
  assert.equal(b.observedAt,a.observedAt); assert.equal(b.priceUsd,0.01); assert.equal(calls.length,1);
  advance(1000); const c=await p.getSnapshot({tokenAddress:token}); assert.notEqual(c.observedAt,a.observedAt); assert.equal(calls.length,2);
});
test('wrong identity and quote orientation fail closed; pin cannot silently change', async () => {
  const wrong=payload(); wrong.data.relationships.base_token.data.id='solana_other';
  await assert.rejects(setup(wrong).p.getSnapshot({tokenAddress:token,poolAddress:pool}),/identity_or_orientation/);
  const {p}=setup(); await p.getSnapshot({tokenAddress:token,poolAddress:pool});
  await assert.rejects(p.getSnapshot({tokenAddress:token,poolAddress:'33333333333333333333333333333333'}),/pinned_pair_changed/);
});
test('discovery and detail both reserve calls; unsupported pools excluded', async () => {
  const {p,calls,lanes}=setup(); p.fetchFn=async url=>{ calls.push(url); return {ok:true,json:async()=>url.includes('/tokens/')?{data:[payload().data]}:payload()}; };
  const s=await p.getSnapshot({tokenAddress:token}); assert.equal(s.pairId,pool); assert.equal(lanes.length,2);
});
test('budget denial makes no request and no retry; 429 propagates global cooldown', async () => {
  const {p,calls,cooldowns}=setup(); p.requestBudget.consume=async()=>false;
  await assert.rejects(p.getSnapshot({tokenAddress:token,poolAddress:pool}),/budget_unavailable/); assert.equal(calls.length,0);
  p.requestBudget.consume=async()=>true; p.fetchFn=async()=>({status:429,ok:false,headers:{get:()=> '120'}});
  await assert.rejects(p.getSnapshot({tokenAddress:token,poolAddress:pool}),/rate_limited/);
  assert.equal(cooldowns[0],Date.parse('2026-10-14T13:02:00Z'));
});
test('concurrent identical requests coalesce, and timeout aborts fetch', async () => {
  const {p,calls}=setup(); await Promise.all([p.getSnapshot({tokenAddress:token,poolAddress:pool}),p.getSnapshot({tokenAddress:token,poolAddress:pool})]);
  assert.equal(calls.length,1);
  const slow = new GeckoTerminalDistributionProvider({requestBudget:p.requestBudget,timeoutMs:5,fetchFn:async(_url,{signal})=>new Promise((_,reject)=>signal.addEventListener('abort',()=>reject(new Error('aborted'))))});
  await assert.rejects(slow.getSnapshot({tokenAddress:token,poolAddress:pool}),/aborted/);
});
test('existing adapter participates in same injected budget before every request', async () => {
  let fetched=0; const lanes=[];
  const p=new GeckoTerminalMarketDataProvider({rateLimitMs:0,requestBudget:{consume:async lane=>{lanes.push(lane);return false;}},fetchFn:async()=>{fetched++;}});
  await assert.rejects(p.getLiveTokenSnapshot(token),/budget_unavailable/); assert.equal(fetched,0); assert.deepEqual(lanes,['existing']);
});
test('env request alone never disrupts existing adapter before budget activation', async () => {
  const old=process.env.SIGNAL_DISTRIBUTION_ENABLED;process.env.SIGNAL_DISTRIBUTION_ENABLED='1';
  let calls=0;
  try {
    const p=new GeckoTerminalMarketDataProvider({rateLimitMs:0,fetchFn:async()=>{calls++;return{ok:true,status:200,text:async()=>JSON.stringify({data:{attributes:{address:token,price_usd:'1'}}})};}});
    await p.getLiveTokenSnapshot(token);assert.equal(calls,1);
  } finally {if(old===undefined)delete process.env.SIGNAL_DISTRIBUTION_ENABLED;else process.env.SIGNAL_DISTRIBUTION_ENABLED=old;}
});
test('unconfigured shared budget fails closed, configuration delegates without captured old references', async () => {
  assert.equal(await sharedRequestBudget.consume('distribution'),false);
  const seen=[];configureSharedRequestBudget({consume:async lane=>{seen.push(lane);return true;},backoff:async at=>seen.push(at)});
  assert.equal(await sharedRequestBudget.consume('existing'),true);await sharedRequestBudget.backoff(123);
  assert.deepEqual(seen,['existing',123]);
});
test('expired sample is not returned after upstream failure', async () => {
  const {p,advance}=setup();await p.getSnapshot({tokenAddress:token,poolAddress:pool});advance(60000);
  p.fetchFn=async()=>({ok:false,status:503});
  await assert.rejects(p.getSnapshot({tokenAddress:token,poolAddress:pool}),/provider_unavailable/);
});
test('existing adapter 429 consumes once and stops retry loop when globally budgeted', async () => {
  let calls=0;const cooldown=[];
  const p=new GeckoTerminalMarketDataProvider({rateLimitMs:0,requestBudget:{consume:async()=>true,backoff:async until=>cooldown.push(until)},fetchFn:async()=>{calls++;return{ok:false,status:429,text:async()=> '{}'};}});
  await assert.rejects(p.getLiveTokenSnapshot(token),/rate_limited/);assert.equal(calls,1);assert.equal(cooldown.length,1);
});
test('already-created existing adapter joins shared budget only after activation', async () => {
  let fetched=0;const p=new GeckoTerminalMarketDataProvider({rateLimitMs:0,fetchFn:async()=>{fetched++;throw new Error('unexpected_network');}});
  configureSharedRequestBudget({consume:async lane=>{assert.equal(lane,'existing');return false;},backoff:async()=>{}});
  await assert.rejects(p.getLiveTokenSnapshot(token),/budget_unavailable/);assert.equal(fetched,0);
});
