'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const {resolveSpreadsheetScope}=require('../utils/maxSpreadsheetAuthorization');
function database(actor) {
  return {query:async(sql, values)=>{
    if(sql.includes('ANY')) return {rows:[actor]};
    if(sql.includes("role = 'ao'")) return {rows: values[0]===20 && values[1]===2 ? [{id:20}] : []};
    throw new Error('Unexpected query');
  }};
}
for(const oldRole of ['manager','admin']) {
  test(`current manager tenant binding overrides stale ${oldRole} session without granting approval`,async()=>{
    const actor={id:12,role:'manager',client_id:2,active:true};
    const request={session:{user:{...actor,role:oldRole,client_id:1},active_client_id:1},body:{client_id:1,ao_id:10}};
    await assert.rejects(resolveSpreadsheetScope(request,database(actor),{approverId:12}),{code:'tenant_scope_mismatch'});
    request.body={client_id:2,ao_id:20};
    const scope=await resolveSpreadsheetScope(request,database(actor),{approverId:12});
    assert.equal(scope.clientId,2); assert.equal(scope.aoId,20); assert.equal(scope.canApprove,false);
  });
}
test('unbound non-admin cannot inherit a session tenant',async()=>{
  const actor={id:12,role:'manager',client_id:null,active:true};
  await assert.rejects(resolveSpreadsheetScope({session:{user:actor,active_client_id:1},body:{ao_id:10}},database(actor)),{code:'authenticated_tenant_required'});
});
test('configured current admin may upload directly for an explicitly selected AO',async()=>{
  const actor={id:11,role:'admin',client_id:1,active:true};
  const scope=await resolveSpreadsheetScope({session:{user:actor,active_client_id:2},body:{ao_id:20}},database(actor),{approverId:11});
  assert.equal(scope.clientId,2); assert.equal(scope.actorId,11); assert.equal(scope.aoId,20); assert.equal(scope.canApprove,true);
});
