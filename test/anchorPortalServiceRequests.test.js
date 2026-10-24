'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const make = require('../services/anchorPortalServiceRequests');
class PortalError extends Error {constructor(code,message,status=400){super(message);this.code=code;this.status=status;}}
function fixture(){let calls=[];return {calls,service:make({pool:{query:async (...args)=>{calls.push(args);return {rows:[]};}},AnchorPortalError:PortalError,assertPortalUser:()=>{},assertLocationAccess:async()=>1,isOperator:u=>u.role==='manager',isFacilityClient:u=>u.role==='facility_client'})};}
const client={id:1,role:'facility_client'};
const good={location_id:1,request_key:'valid-key-123',preferred_date:'2026-10-20',preferred_window:'Morning',notes:''};
test('request validation rejects spoofed server fields, invalid dates, oversized and invalid inputs before writes',async()=>{
 for(const patch of [{operator_client_id:11},{requester_id:2},{status:'acknowledged'},{request_key:'x'},{preferred_date:'2026-02-30'},{preferred_date:'bad'},{preferred_window:''},{preferred_window:'x'.repeat(201)},{notes:'x'.repeat(5001)}]){
  const {service,calls}=fixture();await assert.rejects(service.createServiceRequest(client,{...good,...patch}));assert.equal(calls.length,0);
 }
});
test('cleaners cannot create/list requests and invalid transitions never query',async()=>{
 const {service,calls}=fixture();for (const body of [null,[],true]) await assert.rejects(service.createServiceRequest(client,body),e=>e.status===400);await assert.rejects(service.createServiceRequest({role:'cleaner'},good),e=>e.status===403);await assert.rejects(service.listServiceRequests({role:'cleaner'},1),e=>e.status===403);
 for(const status of ['booked','confirmed','requested']) await assert.rejects(service.updateServiceRequest(client,1,{status}));assert.equal(calls.length,0);
});
test('migration is explicit only, includes constrained state and documented restricted grants',()=>{
 const fs=require('node:fs');const sql=fs.readFileSync('migrations/2026-10-16-anchor-portal-service-requests.sql','utf8');assert.match(sql,/UNIQUE \(requester_id, request_key\)/);assert.match(sql,/UPDATE \(status, updated_by, updated_at\)/);assert.doesNotMatch(fs.readFileSync('standalone/anchor-portal/runtime.js','utf8'),/2026-10-16-anchor-portal-service-requests/);
});
