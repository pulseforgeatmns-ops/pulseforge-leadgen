'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { buildReadinessReport, latencyPercentiles, uniqueReviewCount } = require('../utils/maxReadiness');

test('readiness queries supply exactly the parameters referenced by SQL', async () => {
  const calls = [];
  const db = {
    async query(sql, params = []) {
      calls.push({ sql, params });
      const placeholders = [...String(sql).matchAll(/\$(\d+)/g)].map(match => Number(match[1]));
      const required = placeholders.length ? Math.max(...placeholders) : 0;
      assert.equal(params.length, required, `bind mismatch for: ${String(sql).slice(0, 80)}`);
      return { rows: [{}] };
    },
  };

  const report = await buildReadinessReport({ clientId: 10, sinceDays: 30 }, db);
  assert.equal(report.client_id, 10);
  assert.equal(report.window_days, 30);
  assert.equal(report.warm_without_reachable_channels.status, 'available');
  assert.ok(calls.length >= 16);
  assert.equal(calls.some(call => call.sql.includes("metric_name='signal_to_decision_duration'")), false);
  assert.equal(calls.some(call => call.params.includes('live_signal_to_decision_latency')), true);
});

test('live latency is unavailable when no qualifying live decisions exist', async () => {
  const db = { async query() { return { rows: [{ median_ms: null, p95_ms: null, samples: 0 }] }; } };
  const result = await latencyPercentiles(db, 'live_signal_to_decision_latency', [10, 30], 'no live decisions');
  assert.equal(result.status, 'unavailable');
  assert.equal(result.reason, 'no live decisions');
});

test('live latency percentiles remain unavailable below the documented five-sample minimum',async()=>{
  const db={query:async()=>({rows:[{median_ms:8,p95_ms:12,samples:4}]})};
  const result=await latencyPercentiles(db,'live_signal_to_decision_latency',[10,30],'insufficient',{minimumSamples:5});
  assert.equal(result.status,'unavailable');
  assert.equal(result.samples,4);
  assert.equal(result.minimum_samples,5);
});

test('the review gate counts unique decisions rather than raw review rows',()=>{
  assert.equal(uniqueReviewCount({review_consistency:{value:{total_review_rows:100,unique_decisions_reviewed:20}}}),20);
});
