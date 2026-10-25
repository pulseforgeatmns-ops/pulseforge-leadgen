'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),express=require('express'),path=require('node:path'),fs=require('node:fs');
const {DashboardService}=require('../services/signalDashboard/service');
const {MemoryStore,seed,tx,observation,CA,at,later}=require('./helpers/signalDashboardFixtures');
test('mobile/desktop candles, marker taps, notes, intervals, offline poll recovery and session expiry',{timeout:30000},async t=>{
 const s=new MemoryStore();await seed(s);await s.put('transaction',CA,'t',tx(1));for(let i=0;i<40;i++)await s.put('observation',CA,String(i),observation(i*60000,{priceUsd:1+i*.01,symbol:'SYNTHETIC QA'}));
 await s.put('balance',CA,'b',{tokens:'10',slot:5,observedAt:later(2400000),knownAt:later(2400000)});
 const service=new DashboardService(s,{now:()=>new Date(later(2400000))});let offline=false,expired=false,polls=0;
 const app=express();app.use(express.json());app.use(express.static(path.resolve(__dirname,'../public')));
 app.get('/signal-v1/positions/:ca',(_req,res)=>res.sendFile(path.resolve(__dirname,'../public/signal-positions.html')));
 app.get('/api/v1/signal/stream/:ca',async(_req,res)=>{polls++;if(expired)return res.sendStatus(401);if(offline)return res.sendStatus(503);return res.json(await service.detail(CA));});
 app.get('/api/v1/signal/positions/:ca/candles',async(req,res)=>res.json(await service.candles(CA,req.query.interval)));
 app.post('/api/v1/signal/positions/:ca/annotations',async(req,res)=>res.json({annotation:await service.annotate(CA,req.body.text,'7')}));
 const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));t.after(()=>new Promise(r=>server.close(r)));
 const executablePath=process.env.PUPPETEER_EXECUTABLE_PATH||undefined,restricted=executablePath?.includes('headless-shell');
 const browser=await require('puppeteer').launch({executablePath,headless:true,pipe:Boolean(restricted),args:['--no-sandbox','--disable-dev-shm-usage',...(restricted?['--no-process-singleton','--disable-breakpad','--disable-crash-reporter','--single-process','--no-zygote']:[])]});t.after(()=>browser.close());
 const page=await browser.newPage(),errors=[];await page.emulateTimezone('America/New_York');page.on('pageerror',e=>errors.push(e.message));await page.setRequestInterception(true);page.on('request',r=>new URL(r.url()).hostname==='127.0.0.1'?r.continue():r.abort());
 // Speed only the browser polling timer; providers are fixture-backed.
 await page.evaluateOnNewDocument(()=>{const original=window.setTimeout;window.setTimeout=(fn,ms,...args)=>original(fn,ms===10000||ms===20000||ms===40000||ms===60000?250:ms,...args);});
 await page.setViewport({width:375,height:812,isMobile:true,hasTouch:true});await page.goto(`http://127.0.0.1:${server.address().port}/signal-v1/positions/${CA}`);
 await page.waitForFunction(()=>document.querySelector('#chartQuality').textContent.includes('PARTIAL'));
 assert.equal(await page.$eval('body',e=>e.scrollWidth<=375),true);assert.match(await page.$eval('#summary',e=>e.textContent),/Position value|Cash realized|Total P&L|Drawdown/);assert.match(await page.$eval('#risk',e=>e.textContent),/DISTRIBUTION RISK|No alert does not mean safe/);
 // Pan to the first buy if needed, then tap the marker using its exact event time.
 await page.evaluate(()=>{const canvas=document.getElementById('chart');const rect=canvas.getBoundingClientRect();canvas.dispatchEvent(new PointerEvent('pointerdown',{pointerId:1,clientX:rect.left+8,clientY:rect.top+258,bubbles:true}));canvas.dispatchEvent(new PointerEvent('pointerup',{pointerId:1,clientX:rect.left+8,clientY:rect.top+258,bubbles:true}));});
 await page.waitForFunction(()=>!document.querySelector('#markerDetail').hidden);assert.match(await page.$eval('#markerDetail',e=>e.textContent),/BUY/);
 await page.click('#intervals button:nth-child(3)');await page.waitForFunction(()=>document.querySelector('#intervals button:nth-child(3)').getAttribute('aria-pressed')==='true');
 await page.type('#note','Recovered principal <script>alert(1)</script>');await page.click('#noteForm button');await page.waitForFunction(()=>document.querySelector('#noteStatus').textContent.includes('saved'));assert.equal((await s.list('annotation',CA)).length,1);assert.equal(await page.$('#timeline script'),null);
 const n=polls;offline=true;await page.waitForFunction(()=>document.querySelector('#connection').textContent.includes('unavailable'));offline=false;await page.waitForFunction(()=>document.querySelector('#connection').textContent.includes('Connected'));assert.ok(polls>n+1);
 fs.mkdirSync('/tmp/signal-dashboard-qa',{recursive:true});await page.screenshot({path:'/tmp/signal-dashboard-qa/mobile.png',fullPage:true});
 await page.setViewport({width:1280,height:900});assert.equal(await page.$eval('body',e=>e.scrollWidth<=1280),true);await page.screenshot({path:'/tmp/signal-dashboard-qa/desktop.png',fullPage:true});
 expired=true;await page.waitForFunction(()=>document.querySelector('#connection').textContent.includes('Session expired'));assert.deepEqual(errors,[]);
});
