'use strict';

const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const test=require('node:test');
const read=file=>fs.readFileSync(path.join(__dirname,'..',file),'utf8');

test('Phase 2.5 utilities contain no operational action mutations',()=>{
  const files=['scripts/backfillMaxSignals.js','scripts/smokeMaxShadow.js','utils/maxSequenceState.js','utils/maxReviewSampling.js','utils/maxReadiness.js','utils/maxLifecycleSignals.js'];
  for(const file of files){
    const source=read(file);
    assert.doesNotMatch(source,/SET\s+status\s*=/i,file);
    assert.doesNotMatch(source,/UPDATE\s+(?:agent_actions|cal_queue)/i,file);
    assert.doesNotMatch(source,/sendEmail\s*\(|enrollSequence\s*\(|retryEnrichment\s*\(/i,file);
  }
});

test('Scout and confirmed calendar paths use isolated canonical adapters',()=>{
  assert.match(read('leadgen.js'),/safeIngestScoutLifecycleSignal/);
  assert.match(read('routes/webhooks.js'),/safeIngestMeetingSignal/);
  assert.match(read('routes/webhooks.js'),/parsed\.booked && calendarCreated/);
});
