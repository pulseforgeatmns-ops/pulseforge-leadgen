'use strict';
// Offline UI gate: isolated loopback fixtures only, never the repository server.
const test=require('node:test'),assert=require('node:assert/strict'),http=require('node:http'),fs=require('node:fs'),path=require('node:path');
const puppeteer=require('puppeteer');
test('distribution browser: mobile/desktop, auth, unknown/stale, async navigation and explicit save',async t=>{
 const html=fs.readFileSync(path.join(__dirname,'../public/signal-distribution.html'));
 let auth=false,detailDelay=0,saveDelay=0,writes=0,saved=false;
 const iso=ms=>new Date(Date.now()+ms).toISOString();
 function monitor(namespace){return{id:'monitor-1',token_address:'<img src=x onerror=alert(1)>TEST_TOKEN',created_at:iso(-3600000),expires_at:iso(-1000),state:{riskState:'CRITICAL',pairId:'TEST_POOL',lastObservedAt:iso(-600000),lastPollError:'PROVIDER_UNAVAILABLE',lastPollErrorAt:iso(-60000),coverage:{sourceFreshness:'UNKNOWN',walletDistribution:'UNKNOWN',developerIdentity:'UNKNOWN'},metrics:{priceUsd:.001,liquidityUsd:null,referenceMultiple:null},reasons:['SOURCE_FRESHNESS_UNKNOWN'],reference:saved?{priceUsd:.01,recordedAt:iso(-60000),provenance:'EXPLICIT_MANUAL_OPERATOR'}:undefined}};}
 const server=http.createServer(async(req,res)=>{
  const u=new URL(req.url,'http://127.0.0.1');
  if(u.pathname==='/'){res.setHeader('Content-Type','text/html');res.end(html);return;}
  if(u.pathname==='/login'){res.setHeader('Content-Type','text/html');res.end('Login fixture');return;}
  if(auth){res.writeHead(302,{Location:'/login'});res.end();return;}
  res.setHeader('Content-Type','application/json');
  if(req.method==='POST'){writes++;await new Promise(r=>setTimeout(r,saveDelay));saved=true;res.end(JSON.stringify({saved:true}));return;}
  const namespace=u.searchParams.get('namespace')||'operational';const m=monitor(namespace);
  if(u.pathname.endsWith('/distribution'))res.end(JSON.stringify({namespace,monitors:[m],admissions:[{token_address:'DEFERRED_CA',reason:'active_capacity_reached',created_at:iso(-60000)}]}));
  else{await new Promise(r=>setTimeout(r,detailDelay));m.expires_at=iso(3600000);res.end(JSON.stringify({namespace,monitor:m,observations:[{observed_at:iso(-600000),payload:{priceUsd:.001,liquidityUsd:null}}],transitions:[],deliveries:[{channel:'sms',status:'UNKNOWN',attempts:1,created_at:iso(-60000)}]}));}
 });
 await new Promise(r=>server.listen(0,'127.0.0.1',r));t.after(()=>new Promise(r=>server.close(r)));
 const browser=await puppeteer.launch({executablePath:process.env.PUPPETEER_EXECUTABLE_PATH||'/usr/bin/chromium',headless:true,pipe:true,args:['--no-sandbox','--disable-dev-shm-usage','--no-process-singleton','--disable-breakpad','--disable-crash-reporter']});t.after(()=>browser.close());
 const page=await browser.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.setRequestInterception(true);page.on('request',r=>new URL(r.url()).hostname==='127.0.0.1'?r.continue():r.abort());
 await page.setViewport({width:375,height:812,deviceScaleFactor:1});const url=`http://127.0.0.1:${server.address().port}/`;await page.goto(url);await page.waitForFunction(()=>document.querySelector('#monitors button'));
 assert.equal(await page.$eval('body',e=>e.scrollWidth<=375),true);assert.match(await page.$eval('#monitors',e=>e.textContent),/EXPIRED|STALE/);assert.match(await page.$eval('#admissions',e=>e.textContent),/active_capacity_reached/);assert.equal(await page.$('#monitors img'),null);
 await page.click('#monitors button');await page.waitForSelector('#detail form');assert.match(await page.$eval('#detail',e=>e.textContent),/Receipt and display on your device are unverified/);
 fs.mkdirSync('/workspace/shared',{recursive:true});await page.screenshot({path:'/workspace/shared/signal-distribution-mobile.png',fullPage:true});
 await page.setViewport({width:1280,height:900});await page.screenshot({path:'/workspace/shared/signal-distribution-desktop.png',fullPage:true});
 // Cancelled confirmation never writes.
 async function fill(){await page.$eval('#detail input[type=number]',e=>e.value='0.01');await page.$eval('#detail input[type=datetime-local]',e=>{const d=new Date(Date.now()-60000);d.setMinutes(d.getMinutes()-d.getTimezoneOffset());e.value=d.toISOString().slice(0,19);});}
 await fill();page.once('dialog',d=>d.dismiss());await page.click('#detail form button');await new Promise(r=>setTimeout(r,100));assert.equal(writes,0);
 // Duplicate submit while pending produces exactly one POST. Namespace switch must win.
 await fill();saveDelay=250;page.once('dialog',d=>d.accept());await page.click('#detail form button');await page.waitForFunction(()=>document.querySelector('#detail form button').disabled);await page.$eval('#detail form',e=>e.dispatchEvent(new Event('submit',{bubbles:true,cancelable:true})));
 await page.select('#namespace','test');await page.waitForFunction(()=>document.querySelector('#status').textContent.startsWith('TEST ONLY.'));await new Promise(r=>setTimeout(r,400));assert.equal(writes,1);assert.equal(await page.$eval('#detail',e=>e.classList.contains('hidden')),true);
 // Pending old detail cannot repopulate after namespace/refresh.
 saved=false;detailDelay=250;await page.click('#monitors button');await page.select('#namespace','operational');await page.click('#refresh');await new Promise(r=>setTimeout(r,400));assert.equal(await page.$eval('#detail',e=>e.classList.contains('hidden')),true);
 // Fresh explicit read shows saved reference and no implicit second save control.
 detailDelay=0;saved=true;await page.click('#monitors button');await page.waitForFunction(()=>document.querySelector('#detail').textContent.includes('Saved once:'));assert.equal(await page.$('#detail form'),null);
 auth=true;await page.click('#refresh');await page.waitForFunction(()=>document.querySelector('#status').textContent.includes('Sign in again'));assert.deepEqual(errors,[]);
});
