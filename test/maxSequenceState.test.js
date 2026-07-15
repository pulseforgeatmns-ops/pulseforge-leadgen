'use strict';

const assert=require('node:assert/strict');
const test=require('node:test');
const { resolveSequenceState }=require('../utils/maxSequenceState');

const base={id:'p',status:'contacted',do_not_contact:false,client_sequence:'cleaning',active_sequence_type:null};

test('sequence resolver favors explicit operational evidence',()=>{
  assert.equal(resolveSequenceState({...base,has_pending_send:true,pending_send_at:'2026-07-16'}).confidence,'high');
  assert.equal(resolveSequenceState({...base,has_pending_send:true}).enrollment_status,'active');
  assert.equal(resolveSequenceState({...base,email_sequence_completed_at:'2026-07-15'}).enrollment_status,'completed');
  assert.equal(resolveSequenceState({...base,do_not_contact:true,has_pending_send:true}).enrollment_status,'suppressed');
});

test('sequence resolver labels field-only enrollment as inferred',()=>{
  const state=resolveSequenceState({...base,last_contacted_at:'2026-07-14',next_touch_at:'2026-07-18'});
  assert.equal(state.source,'prospects.next_touch_at'); assert.equal(state.confidence,'medium');
});
