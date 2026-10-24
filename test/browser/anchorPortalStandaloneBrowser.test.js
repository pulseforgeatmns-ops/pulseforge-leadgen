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

test('standalone CSP permits mobile issue photo upload and durable resolution proof', {timeout:90000}, async()=>{
 const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
 const files=fs.mkdtempSync(path.join(os.tmpdir(),'portal-upload-regression-'));
 const photo=path.join(files,'synthetic-upload.png');
 fs.writeFileSync(photo,Buffer.from(illustrativePhoto(0).data_url.split(',')[1],'base64'));
 let browser;
 const contexts=[],errors=[];
 const description='Synthetic standalone browser upload regression';
 const resolution='Synthetic resolution proof persisted after browser reload';
 try {
  browser=await require('puppeteer').launch({executablePath:process.env.CHROME_BIN||'/usr/bin/chromium',headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
  async function signedInPage(role,mobile=false) {
   const context=await browser.createBrowserContext();contexts.push(context);
   const page=await context.newPage();
   await page.setViewport(mobile?{width:390,height:844,isMobile:true,hasTouch:true}:{width:1365,height:900});
   page.on('pageerror',e=>errors.push(e.message));
   await page.setRequestInterception(true);
   page.on('request',r=>r.url().startsWith(base)||r.url().startsWith('data:')||r.url().startsWith('blob:')?r.continue():r.abort());
   await page.evaluateOnNewDocument(()=>{
    window.portalTestCspViolations=[];
    document.addEventListener('securitypolicyviolation',e=>window.portalTestCspViolations.push({directive:e.effectiveDirective,blocked:e.blockedURI}));
   });
   await page.goto(base+'/portal/anchor');await page.waitForSelector('#login');
   await page.type('[name=email]',`${role}@exeter-fixture.invalid`);await page.type('[name=password]',password);await page.click('#login button');
   await page.waitForFunction(()=>document.querySelector('#userLabel')?.textContent.includes('Synthetic'));
   return page;
  }
  async function upload(page,selector) {
   await (await page.$(selector)).uploadFile(photo);
   await page.waitForFunction(s=>{const text=document.querySelector(s+'Status')?.textContent;return text&&!text.startsWith('Preparing');},{timeout:10000},selector);
   const result=await page.evaluate(s=>({status:document.querySelector(s+'Status').textContent,violations:window.portalTestCspViolations}),selector);
   assert.match(result.status,/1 photo ready/,`Upload failed under standalone CSP: ${JSON.stringify(result)}`);
   await page.$eval(selector+'Preview img',async img=>{img.scrollIntoView();await img.decode();});
   assert.equal(await page.$eval(selector+'Preview img',img=>img.complete&&img.naturalWidth>0),true);
   assert.deepEqual(result.violations,[]);
  }
  const client=await signedInPage('client',true);
  assert.ok(await client.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'mobile layout overflows');
  await client.click('[data-tab="issues"]');await client.waitForSelector('#issueDesc');
  await client.type('#issueRoom','Synthetic browser test room');await client.type('#issueDesc',description);await client.select('#issueSeverity','low');
  await upload(client,'#issuePhoto');await client.click('#saveForm');
  await client.waitForFunction(()=>document.querySelector('#flash').textContent.includes('Problem submitted'));
  const rows=await pool.query('SELECT id FROM anchor_portal_issues WHERE description=$1',[description]);
  assert.equal(rows.rowCount,1);const issueId=rows.rows[0].id;
  const reported=await pool.query('SELECT mime_type,data_url FROM anchor_portal_evidence WHERE issue_id=$1 AND purpose=$2',[issueId,'reported']);
  assert.equal(reported.rowCount,1);assert.equal(reported.rows[0].mime_type,'image/jpeg');assert.match(reported.rows[0].data_url,/^data:image\/jpeg;base64,/);
  const operator=await signedInPage('operator');await operator.click('[data-tab="issues"]');await operator.waitForSelector(`[data-resolve="${issueId}"]`);
  await operator.click(`[data-resolve="${issueId}"]`);await operator.waitForSelector('#resolution');await operator.type('#resolution',resolution);
  await upload(operator,'#resolutionPhoto');await operator.click('#saveForm');
  await operator.waitForFunction(()=>document.querySelector('#flash').textContent.includes('Resolution saved'));
  const fresh=await signedInPage('client',true);
  async function readResolved() {
   await fresh.click('[data-tab="issues"]');await fresh.waitForSelector('[data-go="resolved"]');await fresh.click('[data-go="resolved"]');
   await fresh.waitForFunction(value=>document.querySelector('#view').textContent.includes(value),{},resolution);
  }
  await readResolved();await fresh.reload();await fresh.waitForSelector('[data-tab="issues"]');await readResolved();
  const saved=await call('/api/anchor-portal/issues?location_id='+fixture.location.id,await login('client'));
  const issue=saved.body.issues.find(i=>i.id===issueId);
  assert.equal(issue.status,'resolved');assert.equal(issue.resolution,resolution);
  assert.deepEqual(issue.evidence.map(e=>e.purpose).sort(),['reported','resolution']);
  const rendered=await fresh.evaluate(async description=>{
   const cards=Array.from(document.querySelectorAll('.issue-card')).filter(card=>card.textContent.includes(description));
   const images=cards.flatMap(card=>Array.from(card.querySelectorAll('.gallery img')));
   for(const img of images){img.scrollIntoView();await img.decode();}
   return {cards:cards.length,images:images.length,decoded:images.every(i=>i.complete&&i.naturalWidth>0)};
  },description);
  assert.deepEqual(rendered,{cards:1,images:2,decoded:true});
  assert.ok(await fresh.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'reloaded mobile layout overflows');
  assert.deepEqual(await fresh.evaluate(()=>window.portalTestCspViolations),[]);assert.deepEqual(errors,[]);
 } finally {
  for(const context of contexts)await context.close();if(browser)await browser.close();fs.rmSync(files,{recursive:true,force:true});
 }
});
