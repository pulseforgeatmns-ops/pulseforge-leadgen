'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {spawnSync}=require('node:child_process');
const path=require('node:path');
const {validateManifest}=require('../services/scoutBoundedAcquisition');
const {hash}=require('../services/scoutBoundedAcquisitionBudget');
const {queryIdentity}=require('../services/scoutQueryCheckpoints');
const template=()=>({version:1,approvalRef:'synthetic-bounded-test',tenantId:'10',programId:'synthetic-program',missionId:'synthetic-mission',
  programHash:hash('program'),sourceHash:hash('source'),fingerprint:hash('mission'),query:'Property Management Concord NH',rootQueryKey:queryIdentity('Property Management Concord NH',false),
  evidenceRequest:{tenantId:'10',scope:'market',segment:'property_manager',evidenceType:'identity',discoveryConcept:'Property Management',geography:{cities:['Concord'],state:'NH'}},
  grant:{version:2,epoch:1,approvalRef:'synthetic-bounded-test',tenantId:'10',fingerprint:hash('mission'),rootQueryKeys:[queryIdentity('Property Management Concord NH',false)],maxNewPages:1,notBefore:'2026-10-20T00:00:00.000Z',expiresAt:'2026-10-21T00:00:00.000Z'},maxModeledUsdMicros:620000});
test('manifest cannot widen tenant, query, root, page, money or evidence scope',()=>{
  assert.equal(validateManifest(template()).maxModeledUsdMicros,620000);
  for(const mutate of [m=>m.tenantId='13',m=>m.query+=' elsewhere',m=>m.rootQueryKey=hash('other'),m=>m.grant.maxNewPages=2,
    m=>m.maxModeledUsdMicros=620001,m=>m.evidenceRequest.geography.cities.push('Elsewhere'),m=>m.evidenceRequest.entity={name:'other'},m=>m.grant.approvalRef='other']) {
    const m=template();mutate(m);assert.throws(()=>validateManifest(m));
  }
});
test('timestamp-only source revisions alter immutable binding',()=>{
  assert.notEqual(hash({authorized_at:new Date('2026-01-01')}),hash({authorized_at:new Date('2026-01-02')}));
});
test('standalone process denies fetch, HTTP, HTTPS, arbitrary TCP, UDP, subprocesses and redirected endpoints',()=>{
  const script=`const assert=require('node:assert/strict');const {installTransport}=require(${JSON.stringify(path.resolve(__dirname,'../scripts/scout-bounded/transport.cjs'))});
    const fetchPlaces=installTransport({databaseUrl:'postgresql://synthetic:synthetic@127.0.0.1:5432/synthetic'});
    for(const run of [()=>fetch('https://other.invalid'),()=>require('node:http').request('http://other.invalid'),()=>require('node:https').get('https://other.invalid'),
      ()=>require('node:net').connect(443,'other.invalid'),()=>require('node:tls').connect({host:'other.invalid',port:443}),
      ()=>require('node:dgram').createSocket('udp4'),()=>require('node:dns').resolveMx('other.invalid'),
      ()=>require('node:dns').promises.resolve('other.invalid'),()=>require('node:dns').lookup('other.invalid'),
      ()=>require('node:tls').connect({socket:new (require('node:net').Socket)(),servername:'127.0.0.1'}),
      ()=>require('node:tls').connect({socket:new (require('node:net').Socket)(),host:'127.0.0.1',port:5432}),()=>require('node:child_process').spawn('curl',[]),
      ()=>fetchPlaces('https://other.invalid',{redirect:'error'}),()=>fetchPlaces('https://maps.googleapis.com/maps/api/geocode/json',{redirect:'error'})]) assert.throws(run,/bounded_external_transport_denied/);
    process.stdout.write('14 denied');`;
  const result=spawnSync(process.execPath,['-e',script],{encoding:'utf8'});
  assert.equal(result.status,0,result.stderr);assert.equal(result.stdout,'14 denied');
});
test('CLI requires explicit invocation and activation without starting any network',()=>{
  const result=spawnSync(process.execPath,[path.resolve(__dirname,'../scripts/scout-bounded/run.cjs')],{encoding:'utf8'});
  assert.equal(result.status,1);assert.match(result.stderr,/Bounded Scout stopped/);
});
