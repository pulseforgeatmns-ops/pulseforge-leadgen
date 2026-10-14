'use strict';
const { test,before,after } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const bcrypt = require('bcryptjs');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('../helpers/disposablePostgres');
const { initializeSchema, provisionDemo } = require('../../standalone/anchor-portal/initialize');
const { createApp } = require('../../standalone/anchor-portal/runtime');
const { config,MARKER } = require('../../standalone/anchor-portal/config');
const { verifyDatabase } = require('../../standalone/anchor-portal/database');
const { illustrativePhoto } = require('../../standalone/anchor-portal/fixtures');
let pg,pool,server,base,fixture,password,hashes,store;
const keys=['client','operator'];
before(async()=>{
 pg=await startDisposablePostgres('portal-standalone-');
 const admin=new Pool({connectionString:pg.connectionString});
 await admin.query('CREATE DATABASE anchor_portal_synthetic');await admin.end();
 pool=new Pool({connectionString:pg.connectionString.replace(/\/postgres$/,'/anchor_portal_synthetic')});
 password=crypto.randomBytes(32).toString('base64url');
 const hash=await bcrypt.hash(password,12); hashes=Object.fromEntries(keys.map(k=>[k,hash]));
 await initializeSchema(pool,MARKER);
 fixture=await provisionDemo(pool,MARKER,hashes,new Date(Date.now()+3600000));
 // Reserve selected loopback listener before constructing canonical configuration.
 const express=require('express');const outer=express();
 server=await new Promise(resolve=>{const s=outer.listen(0,'127.0.0.1',()=>resolve(s));});
 base=`http://127.0.0.1:${server.address().port}`;
 const cfg=config({PORTAL_MODE:'local',PORT:String(server.address().port),PORTAL_ORIGIN:base,PORTAL_DATABASE_URL:pool.options.connectionString,PORTAL_SYNTHETIC_MARKER:MARKER,PORTAL_SESSION_SECRET:crypto.randomBytes(32).toString('hex')});
 const runtime=await createApp(cfg,pool);store=runtime.store;outer.use(runtime.app);
});
after(async()=>{if(store)store.close();if(server)await new Promise(r=>server.close(r));if(pool)await pool.end();if(pg)await pg.stop();password=undefined;});
async function call(path, actor={},method='GET',body,headers={}) {
 const r=await fetch(base+path,{method,redirect:'manual',headers:{Cookie:actor.cookie||'',Origin:base,'Content-Type':'application/json','X-CSRF-Token':actor.csrf||'',...headers},body:body?JSON.stringify(body):undefined});
 const text=await r.text();let value;try{value=JSON.parse(text);}catch{value=text;}
 return {status:r.status,body:value,cookie:r.headers.get('set-cookie')?.split(';')[0],headers:r.headers};
}
async function login(key) {
 const pre=await call('/auth/csrf');const old={cookie:pre.cookie,csrf:pre.body.csrf};
 const result=await call('/login',old,'POST',{email:`${key}@exeter-fixture.invalid`,password});assert.equal(result.status,200,JSON.stringify(result.body));assert.notEqual(result.cookie,old.cookie);
 const csrf=await call('/auth/csrf',{cookie:result.cookie});return {cookie:result.cookie,csrf:csrf.body.csrf};
}
test('real browser signs in, loads portal, signs out and cannot reuse session', {timeout:60000}, async()=>{
 const browser=await require('puppeteer').launch({executablePath:process.env.CHROME_BIN||'/usr/bin/chromium',headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
 try {
  const page=await browser.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.setRequestInterception(true);page.on('request',r=>r.url().startsWith(base)||r.url().startsWith('data:')||r.url().startsWith('blob:')?r.continue():r.abort());
  await page.goto(base+'/portal/anchor');await page.waitForSelector('#login');
  await page.type('[name=email]','client@exeter-fixture.invalid');await page.type('[name=password]',password);await page.click('#login button');
  await page.waitForSelector('#userLabel');await page.waitForFunction(()=>document.querySelector('#userLabel').textContent.includes('Synthetic'));
  await page.click('[data-tab="history"]');await page.waitForSelector('[data-visit]');
  await page.click('[data-visit]');await page.waitForSelector('.gallery img');
  // DOM insertion precedes lazy image fetch/decode; exercise the visible image.
  await page.$eval('.gallery img',async img=>{img.scrollIntoView();await img.decode();});
  assert.equal(await page.$eval('.gallery img',i=>i.complete&&i.naturalWidth>0),true);
  await page.goto(base+'/logout');await page.click('#logout');await page.waitForSelector('#login:not([hidden])');
  await page.goto(base+'/portal/anchor');await page.waitForSelector('#login');assert.deepEqual(errors,[]);
 } finally {await browser.close();}
});
