'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),{createRequire}=require('node:module');
const {Pool}=require('pg');
const {startDisposablePostgres}=require('./helpers/disposablePostgres');
const {seedScopedResume}=require('./helpers/anchorScopedResumePostgresFixture');
const {applyCrmPreparationSchema}=require('./helpers/anchorCrmPreparationSchema');
const {hash}=require('../packages/acquisition-mission/DailyOutboundPolicy');
const control=require('../services/anchorScopedResumeControl');
function loadHarness() {
  const filename=path.join(__dirname,'anchorScopedResumeInvocation.test.js'),local=createRequire(filename),module={exports:{}};
  vm.runInNewContext(fs.readFileSync(filename,'utf8'),{require:id=>id==='node:test'?()=>{}:local(id),module,exports:module.exports,
    __dirname,path,console,process},{filename});return module.exports;
}
test('native one-shot default is a real read-only snapshot, preserves every application row and never loads bridge', {timeout:120000},async t=>{
  const pg=await startDisposablePostgres('anchor-crm-invocation-'),pool=new Pool({connectionString:pg.connectionString,max:8});
  t.after(async()=>{await pool.end();await pg.stop();});
  const f=await seedScopedResume(pool,{preparation:true});await applyCrmPreparationSchema(pool);
  f.manifest.scopes.email={enabled:false};f.manifest.scopes.preparation.operations.push('prepare_existing_crm');
  f.sourceMission.stage='discover';f.sourceMission.status='Discovering';
  await pool.query('UPDATE acquisition_missions SET payload=$1 WHERE id=$2',[f.sourceMission,f.sourceMission.id]);
  await control.installScopedResume(pool,{manifest:f.manifest,approvalReference:f.manifest.approvalReference},f.actor,{env:f.env,now:f.now});
  f.env.RAILWAY_GIT_COMMIT_SHA='a'.repeat(40);f.env.DATABASE_URL='local-private-not-logged';
  f.argv=['--tenant','10','--revision',f.env.RAILWAY_GIT_COMMIT_SHA,'--manifest-sha256',hash(f.manifest),'--source-sha256',hash(f.sourceMission)];
  async function snapshot(){const tables=(await pool.query("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")).rows;
    const rows={};for(const {tablename} of tables) rows[tablename]=(await pool.query(`SELECT row_to_json(t) AS row FROM "${tablename}" t`)).rows.map(r=>JSON.stringify(r.row)).sort();return rows;}
  const before=await snapshot(),calls=[],readOnly=[];let closes=0;
  const audited={on(){},async connect(){const db=await pool.connect();return {async query(q,args){
    const sql=typeof q==='string'?q:q.text;calls.push(sql);
    if(/^SELECT /.test(sql))readOnly.push((await db.query('SHOW transaction_read_only')).rows[0].transaction_read_only);
    return db.query(q,args);},release:discard=>db.release(discard)};},async end(){closes++;}};
  const h=loadHarness().load(f,{poolOverride:audited});const result=await h.run(f.argv,f.env);
  assert.equal(result.mode,'read_only');assert.equal(result.writesAttempted,false);assert.equal(result.completeEligibilityChecked,false);
  assert.ok(readOnly.length>0&&readOnly.every(x=>x==='on'));assert.ok(!calls.some(sql=>/FOR SHARE|FOR UPDATE|advisory|INSERT |UPDATE |DELETE |ALTER |CREATE /i.test(sql)));
  assert.deepEqual(await snapshot(),before);assert.equal(closes,1);assert.ok(!h.events.includes('bridge-import'));
  await assert.rejects(h.run(f.argv.map(v=>v===hash(f.manifest)?'f'.repeat(64):v),f.env),/invocation_rejected/);
  assert.deepEqual(await snapshot(),before);
});
