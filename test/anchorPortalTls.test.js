'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');const os=require('node:os');const path=require('node:path');const tls=require('node:tls');
const {execFileSync}=require('node:child_process');
const {tlsOptions,poolFor}=require('../standalone/anchor-portal/database');
const {Pool}=require('pg');
const {startDisposablePostgres}=require('./helpers/disposablePostgres');
test('hosted TLS trusts only supplied authentic CA and still verifies intended hostname',async()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'portal-tls-'));let server,postgres,plain,verified,untrusted;
 const openssl=(...args)=>execFileSync('openssl',args,{cwd:dir,stdio:'ignore'});
 try{
  openssl('req','-x509','-newkey','rsa:2048','-nodes','-days','1','-keyout','ca.key','-out','ca.crt','-subj','/CN=Ephemeral Portal Test CA','-addext','basicConstraints=critical,CA:TRUE');
  openssl('req','-newkey','rsa:2048','-nodes','-keyout','server.key','-out','server.csr','-subj','/CN=private-db.example.invalid');
  fs.writeFileSync(path.join(dir,'san.cnf'),'subjectAltName=DNS:private-db.example.invalid,DNS:localhost\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n');
  openssl('x509','-req','-in','server.csr','-CA','ca.crt','-CAkey','ca.key','-CAcreateserial','-days','1','-out','server.crt','-extfile','san.cnf');
  const ca=fs.readFileSync(path.join(dir,'ca.crt'),'utf8');const key=fs.readFileSync(path.join(dir,'server.key'));const cert=fs.readFileSync(path.join(dir,'server.crt'));
  const options=tlsOptions({mode:'hosted',databaseCa:ca});assert.equal(options.rejectUnauthorized,true);assert.equal(options.checkServerIdentity,tls.checkServerIdentity);
  assert.throws(()=>tlsOptions({mode:'hosted',databaseCa:key.toString()}),/Public CA/);
  assert.throws(()=>tlsOptions({mode:'hosted',databaseCa:cert.toString()}),/Valid CA/);
  server=tls.createServer({key,cert},socket=>socket.end());server.on('tlsClientError',()=>{});await new Promise(r=>server.listen(0,'127.0.0.1',r));
  const connect=(name,ssl)=>new Promise((resolve,reject)=>{const socket=tls.connect({host:'127.0.0.1',port:server.address().port,servername:name,...ssl},()=>{const valid=socket.authorized;socket.end();resolve(valid);});socket.once('error',reject);});
  assert.equal(await connect('private-db.example.invalid',options),true);
  await assert.rejects(connect('public-proxy.example.invalid',options),e=>e.code==='ERR_TLS_CERT_ALTNAME_INVALID');
  await assert.rejects(connect('private-db.example.invalid',tlsOptions({mode:'hosted'})));
  openssl('req','-x509','-newkey','rsa:2048','-nodes','-days','1','-keyout','other.key','-out','other.crt','-subj','/CN=Wrong Ephemeral CA','-addext','basicConstraints=critical,CA:TRUE');
  await assert.rejects(connect('private-db.example.invalid',tlsOptions({mode:'hosted',databaseCa:fs.readFileSync(path.join(dir,'other.crt'),'utf8')})));
  // Exercise the actual pg Pool TLS path against our disposable server too.
  postgres=await startDisposablePostgres('portal-pg-tls-');plain=new Pool({connectionString:postgres.connectionString});
  fs.chmodSync(path.join(dir,'server.key'),0o600);
  await plain.query(`ALTER SYSTEM SET ssl_cert_file='${path.join(dir,'server.crt')}'`);
  await plain.query(`ALTER SYSTEM SET ssl_key_file='${path.join(dir,'server.key')}'`);
  await plain.query("ALTER SYSTEM SET ssl='on'");await plain.query('SELECT pg_reload_conf()');
  const deadline=Date.now()+5000;
  while((await plain.query("SELECT setting FROM pg_settings WHERE name='ssl'")).rows[0].setting!=='on'){
   if(Date.now()>deadline)throw new Error('Disposable PostgreSQL TLS did not activate');await new Promise(r=>setTimeout(r,20));
  }
  const databaseUrl=`postgresql://postgres@localhost:${postgres.port}/postgres`;
  verified=poolFor({mode:'hosted',databaseUrl,databaseCa:ca});
  assert.equal((await verified.query('SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()')).rows[0].ssl,true);
  untrusted=poolFor({mode:'hosted',databaseUrl});await assert.rejects(untrusted.query('SELECT 1'));

 }finally{if(verified)await verified.end();if(untrusted)await untrusted.end();if(plain)await plain.end();if(postgres)await postgres.stop();if(server)await new Promise(r=>server.close(r));fs.rmSync(dir,{recursive:true,force:true});}
});
