'use strict';
const assert=require('node:assert/strict');
const crypto=require('crypto');
const fs=require('fs');
const os=require('os');
const path=require('path');
const test=require('node:test');
const {REQUIRED_OPERATIONAL_TABLES,verifyRecoveryArtifact}=require('../utils/maxRecoveryVerification');

test('recovery verification separates local archive validity from durable storage',t=>{
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'max-recovery-test-'));
  t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
  const archive=path.join(dir,'backup.dump');
  const procedure=path.join(dir,'restore.md');
  fs.writeFileSync(archive,'test archive');
  fs.chmodSync(archive,0o600);
  fs.writeFileSync(procedure,'restore steps');
  const expected=crypto.createHash('sha256').update('test archive').digest('hex');
  const toc=['; Format: CUSTOM',...REQUIRED_OPERATIONAL_TABLES.map(name=>`1; 1259 1 TABLE public ${name} postgres`)].join('\n');
  const report=verifyRecoveryArtifact({path:archive,expectedSha256:expected,restoreProcedurePath:procedure,listArchive:()=>toc});
  assert.equal(report.artifact_found,true);
  assert.equal(report.restrictive_permissions,true);
  assert.equal(report.hash_verified,true);
  assert.equal(report.archive_readable,true);
  assert.equal(Object.values(report.required_tables).every(Boolean),true);
  assert.equal(report.restore_procedure_documented,true);
  assert.equal(report.durable_storage_verified,false);
  assert.equal(report.fully_verified,false);
});

test('missing recovery artifact fails closed',()=>{
  const report=verifyRecoveryArtifact({path:'/definitely/missing.dump',expectedSha256:'abc'});
  assert.equal(report.artifact_found,false);
  assert.equal(report.fully_verified,false);
  assert.deepEqual(report.errors,['artifact_not_found']);
});
