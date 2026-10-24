'use strict';
const { test, before, after, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { reserveApiCall, settleApiCall, updateApiPolicy, ensureApiUsageSchema } = require('../utils/apiSpendControl');
const { getApiUsageReport } = require('../services/apiUsageService');
const connection = process.env.API_USAGE_TEST_DATABASE_URL;
const enabled = Boolean(connection);
let db;
before(async () => {
  if (!enabled) return;
  const url = new URL(connection);
  // This suite truncates ONLY its named disposable local database.
  assert.ok(['localhost','127.0.0.1'].includes(url.hostname) && url.pathname === '/api_usage_test');
  const { Pool } = require('pg');
  db = new Pool({ connectionString: connection, max: 25 });
  await ensureApiUsageSchema(db);
  await db.query(`CREATE TABLE IF NOT EXISTS places_api_requests (
    caller TEXT, tenant_id INT, mission_id TEXT, endpoint TEXT, cache_hit BOOLEAN,
    normalized_query TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`);
});
beforeEach(async () => { if (enabled) await db.query('TRUNCATE api_usage_events, api_spend_policies, api_spend_policy_changes, places_api_requests'); });
after(async () => { if (db) await db.end(); });
const attempt = () => reserveApiCall({ provider:'google_places', agentName:'scout', tenantId:10, operation:'text_search', estimatedUsd:0.04, pricingBasis:'test' }, { db });

test('Postgres: concurrent tenants cannot exceed a shared request limit', { skip: !enabled }, async () => {
  await updateApiPolicy('google_places',{ daily_request_limit:5 },{ db });
  const results = await Promise.allSettled(Array.from({length:20},attempt));
  assert.equal(results.filter(r=>r.status==='fulfilled').length,5);
  assert.equal(results.filter(r=>r.status==='rejected').length,15);
  assert.equal(Number((await db.query('SELECT COUNT(*) AS n FROM api_usage_events')).rows[0].n),5);
});
test('Postgres: in-flight costs and uncertain failures keep their reserved allowance', { skip: !enabled }, async () => {
  await updateApiPolicy('google_places',{ daily_usd_limit:0.08 },{ db });
  const a = await attempt(), b = await attempt();
  await settleApiCall(a,{status:'uncertain'},{db});
  await assert.rejects(attempt(),/daily_usd_limit/);
  const r = await getApiUsageReport({db});
  assert.equal(r.providers[0].today.knownGrossUsd,0.08);
  assert.ok(r.alerts.some(a=>a.code==='budget_reached'));
  assert.ok(r.alerts.some(a=>a.code==='unsettled_requests'));
  assert.ok(b);
});
test('Postgres: historical Places calls count toward caps without double counting the new ledger', { skip: !enabled }, async () => {
  await db.query("INSERT INTO places_api_requests(caller,tenant_id,endpoint,cache_hit) SELECT 'legacy',10,'text_search',false FROM generate_series(1,3)");
  await updateApiPolicy('google_places',{ daily_request_limit:4 },{ db });
  const id=await attempt();
  await settleApiCall(id,{status:'ok',estimatedUsd:0.04},{db});
  await db.query("INSERT INTO places_api_requests(caller,tenant_id,endpoint,cache_hit) VALUES ('scout',10,'text_search',false),('scout',10,'text_search',true)");
  await assert.rejects(attempt(),/daily_request_limit/);
  const r=await getApiUsageReport({db});
  assert.equal(r.providers[0].today.requests,4);
  assert.equal(r.providers[0].today.estimatedGrossUsd,0.16);
});
test('Postgres: actual token estimate settles once and releases only unused reservation', { skip: !enabled }, async () => {
  const id=await reserveApiCall({provider:'anthropic',agentName:'paige',tenantId:13,operation:'messages.create',model:'claude-haiku-4-5',estimatedUsd:0.05,pricingBasis:'test'},{db});
  await settleApiCall(id,{status:'ok',estimatedUsd:0.001,usage:{input_tokens:100,output_tokens:180}},{db});
  await settleApiCall(id,{status:'ok',estimatedUsd:0.5},{db});
  const row=(await db.query('SELECT * FROM api_usage_events WHERE id=$1',[id])).rows[0];
  assert.equal(Number(row.estimated_usd),0.001);assert.equal(row.tenant_id,13);
});
test('Postgres: policy changes are auditable and Max cannot race an operator to raise a cap', { skip: !enabled }, async () => {
  await updateApiPolicy('google_places',{ daily_usd_limit:5, paused:true },{ db });
  await assert.rejects(updateApiPolicy('google_places',{ daily_usd_limit:6 },{db,actor:'max'}),/cannot_increase/);
  await assert.rejects(updateApiPolicy('google_places',{ paused:false },{db,actor:'max'}),/cannot_resume/);
  await updateApiPolicy('google_places',{daily_usd_limit:2},{db,actor:'max'});
  const changes=(await db.query('SELECT * FROM api_spend_policy_changes ORDER BY id')).rows;
  assert.equal(changes.length,2);assert.equal(changes[1].actor,'max');
  assert.equal(Number(changes[1].before_policy.daily_usd_limit),5);
  assert.equal(Number(changes[1].after_policy.daily_usd_limit),2);
});
test('Postgres: report works inside a read-only transaction without schema writes', { skip: !enabled }, async () => {
  const c=await db.connect();
  try{await c.query('BEGIN READ ONLY');const r=await getApiUsageReport({db:c});assert.equal(r.billing.actualUsd,null);await c.query('COMMIT');}
  finally{c.release();}
});
