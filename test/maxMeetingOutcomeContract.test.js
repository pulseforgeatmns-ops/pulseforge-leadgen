'use strict';
const assert=require('node:assert/strict');
const test=require('node:test');
const {normalizeMeetingOutcomeEvent}=require('../utils/maxMeetingOutcomeContract');

test('meeting outcome contract requires stable confirmed canonical identity',()=>{
  const event=normalizeMeetingOutcomeEvent({
    client_id:10,prospect_id:'5128ba03-dc0b-44fe-aeb1-f9419142d3e3',
    event_type:'meeting_showed',source:'calendar_provider',source_record_id:'evt_123:attendance',
    event_timestamp:'2026-07-15T18:00:00Z',original_event_timestamp:'2026-07-15T17:55:00Z',
    confidence:'confirmed_provider',
  });
  assert.equal(event.event_timestamp.toISOString(),'2026-07-15T18:00:00.000Z');
  assert.equal(event.source_record_id,'evt_123:attendance');
});

test('meeting outcome contract rejects inferred or unidentified outcomes',()=>{
  assert.throws(()=>normalizeMeetingOutcomeEvent({event_type:'meeting_no_showed'}),/requires client_id/);
  assert.throws(()=>normalizeMeetingOutcomeEvent({client_id:10,prospect_id:'p',event_type:'meeting_no_showed',source:'elapsed_time',source_record_id:'x',event_timestamp:'2026-07-15T18:00:00Z',confidence:'inferred'}),/Unsupported meeting outcome confidence/);
});
