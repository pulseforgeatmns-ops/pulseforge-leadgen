'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true,
  exports: {query:async()=>{throw Error('unexpected default database access');}} };
const { persistProviderChainEmail } = require('../scripts/lib/anchorMissionBoundEnrichment');
const companyId = '00000000-0000-4000-8000-000000000001';
const row = {prospect_id:'synthetic-p',company_id:companyId,client_id:13,domain:'fixture.example',
  website:null,website_url:null,company_name:'Fixture',email:null,do_not_contact:false};
const enriched = {email:'ada@fixture.example',contact:'Ada Lovelace',title:'Owner',source:['hunter']};
const receipt = {emailVerified:true,emailStatus:'valid',doNotContact:false};

test('governed founder evidence persistence rejects stale identity on real disposable PostgreSQL',async t=>{
  const pg=await startDisposablePostgres('babrun-governed-evidence-');
  const db=createTrackedPostgresPool({connectionString:pg.connectionString,max:3,statement_timeout:10000});
  t.after(()=>closePostgresFixture(db,pg));
  await db.query(`CREATE TABLE companies(id uuid PRIMARY KEY,client_id int,domain text,website text,name text);
    CREATE TABLE prospects(id text PRIMARY KEY,company_id uuid,client_id int,website_url text,email text,
      first_name text,last_name text,email_verified boolean,email_verification_method text,email_status text,
      verified_at timestamptz,verifier_checked_at timestamptz,verifier_response jsonb,do_not_contact boolean,
      enrichment_provenance jsonb,acquisition_metadata jsonb,updated_at timestamptz);
    CREATE TABLE agent_log(agent_name text,action text,prospect_id text,payload jsonb,status text,ran_at timestamptz,client_id int)`);
  const reset=async()=>{
    await db.query('TRUNCATE companies,prospects,agent_log');
    await db.query('INSERT INTO companies VALUES($1,13,$2,NULL,$3)',[companyId,'fixture.example','Fixture']);
    await db.query('INSERT INTO prospects(id,company_id,client_id,do_not_contact) VALUES($1,$2,13,false)',[row.prospect_id,companyId]);
  };
  const mutations=[null,"UPDATE companies SET domain='new.example'","UPDATE companies SET website='https://new.example'",
    "UPDATE companies SET name='Other'","UPDATE companies SET client_id=10",
    "UPDATE prospects SET company_id='00000000-0000-4000-8000-000000000002'",
    "UPDATE prospects SET website_url='https://new.example'","UPDATE prospects SET email='other@fixture.example'",
    'UPDATE prospects SET do_not_contact=true'];
  for(const mutation of mutations) await t.test(mutation || 'unchanged identity persists',async()=>{
    await reset(); if(mutation) await db.query(mutation);
    const result=await persistProviderChainEmail(db,row,enriched,receipt,false);
    assert.equal(result.persisted,!mutation);
    assert.equal((await db.query('SELECT count(*)::int n FROM agent_log')).rows[0].n,mutation?0:1);
    if(!mutation){
      const saved=(await db.query('SELECT * FROM prospects')).rows[0];
      assert.equal(saved.acquisition_metadata.contactResolution.finalState,'VERIFIED_FOUNDER_EMAIL');
      assert.equal(saved.acquisition_metadata.contactResolution.bestEmail,saved.email);
    }
  });
  for(const mutation of ["UPDATE companies SET domain='concurrent.example'",'UPDATE prospects SET do_not_contact=true']) {
    await t.test('overlapping transaction: '+mutation,async()=>{
      await reset(); const updater=await db.connect(),writer=await db.connect(); let pending;
      try {
        await updater.query('BEGIN'); await updater.query(mutation);
        const pid=(await writer.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
        pending=persistProviderChainEmail(writer,row,enriched,receipt,false);
        let locked=false; const deadline=Date.now()+5000;
        while(Date.now()<deadline){
          const activity=(await db.query('SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1',[pid])).rows[0];
          if(activity?.wait_event_type==='Lock'){locked=true;break;}
          await new Promise(resolve=>setTimeout(resolve,10));
        }
        assert.equal(locked,true,'writer must wait for changed identity/suppression transaction');
        await updater.query('COMMIT');
        const result=await pending; pending=null;
        assert.equal(result.persisted,false); assert.equal(result.reason,'contact_update_not_applied');
        assert.equal((await db.query('SELECT count(*)::int n FROM agent_log')).rows[0].n,0);
        assert.equal((await db.query('SELECT email FROM prospects')).rows[0].email,null);
      } finally {
        await updater.query('ROLLBACK');
        if(pending) await pending.catch(()=>{});
        updater.release();writer.release();
      }
    });
  }
});
