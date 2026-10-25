'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {startDisposablePostgres}=require('./helpers/disposablePostgres');
const {createTrackedPostgresPool,closePostgresFixture}=require('./helpers/closePostgresPool');
const {DashboardStore,DurableBudget}=require('../services/signalDashboard/store');
const {DashboardService}=require('../services/signalDashboard/service');
const {CA,W,at,later,observation,tx}=require('./helpers/signalDashboardFixtures');
async function exercise(pool) {
 const sql=fs.readFileSync(path.join(__dirname,'../migrations/2026-10-25-signal-dashboard.sql'),'utf8');await pool.query(sql);await pool.query(sql);
 let s=new DashboardStore(pool,{tenantId:'1',wallet:W});
 await s.put('watch',CA,'watch',{tokenAddress:CA,symbol:'SYNTHETIC QA',occurredAt:at,knownAt:at});
 await Promise.all(Array.from({length:12},()=>s.put('transaction',CA,'t',tx(1))));assert.equal((await s.list('transaction',CA)).length,1);
 await s.put('transaction',CA,'t',tx(1,'BUY',{tokenAmount:'999'}));assert.equal((await s.list('transaction',CA))[0].tokenAmount,'10');
 await assert.rejects(()=>s.put('transaction',CA,'t',tx(1),{mutable:true}),/immutable_evidence/);
 const otherTenant=new DashboardStore(pool,{tenantId:'2',wallet:W}),otherWallet=new DashboardStore(pool,{tenantId:'1',wallet:CA});assert.equal((await otherTenant.list('transaction',CA)).length,0);assert.equal((await otherWallet.list('transaction',CA)).length,0);
 await s.put('observation',CA,'o1',observation(0));await s.put('observation',CA,'o2',observation(1000,{priceUsd:2}));await s.put('observation',CA,'o3',observation(2000,{priceUsd:1.5}));await s.put('observation',CA,'future',observation(5000,{priceUsd:100}));
 const peak=await s.peakChronology(CA,at,later(2000));assert.equal(peak.priceUsd,2);assert.equal(peak.drawdown,-.25);assert.equal(peak.chronology.length,2);
 s=new DashboardStore(pool,{tenantId:'1',wallet:W});assert.equal((await s.list('transaction',CA)).length,1);assert.equal((await s.list('observation',CA,{asOf:later(2000)})).length,3);
 let release,entered;const enter=new Promise(r=>entered=r),held=s.lock(async()=>{entered();await new Promise(r=>release=r);});await enter;assert.deepEqual(await new DashboardStore(pool,{tenantId:'1',wallet:W}).lock(()=>{throw Error('must not enter');}),{skipped:true});release();await held;
 const budget=new DurableBudget(pool);const attempts=await Promise.allSettled(Array.from({length:10},()=>budget.take('test-provider',60000)));assert.equal(attempts.filter(x=>x.status==='fulfilled').length,1);
 await budget.backoff('test-provider',60000);await assert.rejects(()=>new DurableBudget(pool).take('test-provider',60000),/budget_deferred/);
 const service=new DashboardService(s,{now:()=>new Date(later(3000))});await service.annotate(CA,'Recovered principal','7');assert.equal((await s.list('annotation',CA)).length,1);
 assert.equal((await service.detail(CA)).peak.postEntry.priceUsd,2);
}
test('native PostgreSQL migration, immutable evidence, tenant/wallet scope, restart, peaks, locks and durable quota',{timeout:90000},async t=>{
 const instance=await startDisposablePostgres('signal-dashboard-'),pool=createTrackedPostgresPool({connectionString:instance.connectionString});t.after(()=>closePostgresFixture(pool,instance));await exercise(pool);
});
module.exports={exercise};
