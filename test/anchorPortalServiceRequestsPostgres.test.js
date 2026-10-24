'use strict';
const {test,before,after}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const {createExeterPortalHarness}=require('./helpers/exeterPortalHarness');
let h,client,operator;
before(async()=>{h=await createExeterPortalHarness();await h.pool.query(fs.readFileSync('migrations/2026-10-16-anchor-portal-service-requests.sql','utf8'));client=await h.login('client');operator=await h.login('operator');});
after(async()=>{if(h)await h.close();});
const call=(path='',cookie=client,method='GET',body)=>h.request('/api/anchor-portal/service-requests'+path,{cookie,method,body});
const payload=key=>({location_id:h.location.id,request_key:key,preferred_date:'2026-10-20',preferred_window:'9–11 a.m.',notes:'Synthetic request only'});
test('concurrent retries persist once, replay returns same record, mismatched key conflicts',async()=>{
 const body=payload('same-request-key');const results=await Promise.all([call('',client,'POST',body),call('',client,'POST',body)]);for(const r of results)assert.equal(r.status,201,JSON.stringify(r.body));assert.equal(results[0].body.request.id,results[1].body.request.id);
 assert.equal((await call('',client,'POST',{...body,notes:'Changed'})).status,409);
 const list=await call('?location_id='+h.location.id,await h.login('client'));assert.equal(list.body.requests.length,1);assert.equal(list.body.requests[0].operator_client_id,10);assert.equal(list.body.requests[0].requester_id,h.users.client.id);
});
test('operator acknowledgement, requester cancellation and closed-state protection persist',async()=>{
 const created=await call('',client,'POST',payload('transition-request'));const id=created.body.request.id;
 assert.equal((await call('/'+id,client,'PATCH',{status:'acknowledged'})).status,403);
 assert.equal((await call('/'+id,operator,'PATCH',{status:'cancelled'})).status,403);
 const acknowledged=await call('/'+id,operator,'PATCH',{status:'acknowledged'});assert.equal(acknowledged.body.request.status,'acknowledged');
 const cancelled=await call('/'+id,client,'PATCH',{status:'cancelled'});assert.equal(cancelled.body.request.status,'cancelled');
 assert.equal((await call('/'+id,operator,'PATCH',{status:'declined'})).status,409);
 const row=(await h.pool.query('SELECT * FROM anchor_portal_service_requests WHERE id=$1',[id])).rows[0];assert.equal(row.status,'cancelled');assert.equal(row.updated_by,h.users.client.id);
});
test('operator decline and tenant, location, role, session, requester isolation',async()=>{
 const created=await call('',client,'POST',payload('isolation-request'));const id=created.body.request.id;
 assert.equal((await call('/'+id,operator,'PATCH',{status:'declined'})).body.request.status,'declined');
 for(const who of ['foreignOperator','foreignClient','unassigned','cleaner']){const cookie=await h.login(who);for(const r of [await call('?location_id='+h.location.id,cookie),await call('',cookie,'POST',payload('forbidden-'+who)),await call('/'+id,cookie,'PATCH',{status:'cancelled'})])assert.ok([403,404].includes(r.status),who+JSON.stringify(r));}
 assert.equal((await call('?location_id='+h.location.id,null)).status,401);
 assert.equal((await call('',client,'POST',{...payload('foreign-location'),location_id:h.unassignedLocation.id})).status,403);
 const own=await call('',operator,'POST',payload('operator-request'));assert.equal(own.status,201);const list=await call('?location_id='+h.location.id);assert.ok(!list.body.requests.some(r=>r.id===own.body.request.id));
 assert.equal((await call('/'+own.body.request.id,client,'PATCH',{status:'cancelled'})).status,403);
});
test('restricted runtime grants support create/list/update but prohibit deletes and field rewriting',async()=>{
 await h.pool.query(`CREATE ROLE service_request_test_runtime; GRANT USAGE ON SCHEMA public TO service_request_test_runtime; GRANT SELECT,INSERT ON anchor_portal_service_requests TO service_request_test_runtime; GRANT UPDATE(status,updated_by,updated_at) ON anchor_portal_service_requests TO service_request_test_runtime; GRANT USAGE ON SEQUENCE anchor_portal_service_requests_id_seq TO service_request_test_runtime`);
 const db=await h.pool.connect();try{await db.query('SET ROLE service_request_test_runtime');const {rows}=await db.query(`INSERT INTO anchor_portal_service_requests(operator_client_id,location_id,requester_id,request_key,preferred_date,preferred_window) VALUES(10,$1,$2,'restricted-role-test','2026-10-20','Morning') RETURNING *`,[h.location.id,h.users.client.id]);await db.query("UPDATE anchor_portal_service_requests SET status='acknowledged',updated_by=$2,updated_at=now() WHERE id=$1",[rows[0].id,h.users.operator.id]);await assert.rejects(db.query('DELETE FROM anchor_portal_service_requests'),e=>e.code==='42501');await assert.rejects(db.query('UPDATE anchor_portal_service_requests SET requester_id=2'),e=>e.code==='42501');}finally{await db.query('RESET ROLE');db.release();}
});
