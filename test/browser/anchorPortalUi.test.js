'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const puppeteer = require('puppeteer');
const root = path.join(__dirname,'..','..');
const photo = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a3ioAAAAASUVORK5CYII=';
async function setup(t,{role='admin',width=1280,locations,delayVisits=0}={}) {
 const browser = await puppeteer.launch({executablePath:process.env.CHROMIUM_PATH || '/usr/bin/chromium',headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
 t.after(() => browser.close()); const page = await browser.newPage(); await page.setViewport({width,height:900});
 const mutations = [], errors = []; page.on('pageerror',e=>errors.push(e.message));
 let issues = [{id:2,location_id:1,status:'open',description:'<img src=x onerror="window.injected=true">',room_category:'West wing',severity:'high',evidence:[{data_url:photo,caption:'Synthetic illustration',purpose:'reported'}]}];
 const visit = {id:5,location_id:1,location_name:'Demo building',completed_at:'2026-10-14T12:00:00Z',status:'completed',completed_by_name:'Sample Team',visit_notes:'<script>window.injected=true</script>',items:[{section_title:'West wing',item_label:'Floor',outcome:'completed'}],evidence:[{data_url:photo,caption:'Synthetic illustration',purpose:'service'}]};
 await page.setRequestInterception(true);
 page.on('request',async req => {
  const u = new URL(req.url());
  if (u.origin !== 'http://localhost') return req.abort();
  if (u.pathname === '/portal/anchor') return req.respond({status:200,contentType:'text/html',body:fs.readFileSync(path.join(root,'public/anchor-portal.html'),'utf8')});
  if (u.pathname === '/anchor-portal.js') return req.respond({status:200,contentType:'application/javascript',body:fs.readFileSync(path.join(root,'public/anchor-portal.js'),'utf8')});
  let data = {};
  if (req.method() !== 'GET') {
   const body = JSON.parse(req.postData() || '{}'); mutations.push({path:u.pathname,body});
   await new Promise(r=>setTimeout(r,100));
   if (u.pathname.endsWith('/issues')) {issues.push({id:3,...body,status:'open',created_at:'2026-10-14T12:00:00Z',evidence:body.photo_data_url ? [{data_url:body.photo_data_url,caption:'Submitted photo',purpose:'reported'}] : []}); data={issue:issues.at(-1)};}
   else if (/\/issues\/\d+$/.test(u.pathname)) {Object.assign(issues[0],body);if(body.resolution_photo_data_url) issues[0].evidence.push({data_url:body.resolution_photo_data_url,purpose:'resolution'});data={issue:issues[0]};}
   else data={visit};
  } else if (u.pathname.endsWith('/me')) data={user:{name:'Sample <Admin>',role},capabilities:{operator:role==='admin',facility_client:role==='client',cleaner:role==='cleaner'},locations:locations || [{id:1,name:'Phillips Exeter Academy — Demo Building',is_demo:true},{id:2,name:'Other <Building>',is_demo:false}]};
  else if (u.pathname.endsWith('/home')) data={last_service:visit.completed_at,service_status:'All clear',open_issues_count:issues.filter(i=>i.status!=='resolved').length,recent_visits:[visit]};
  else if (u.pathname.endsWith('/scope')) data={scope:[{title:'West <Wing>',items:[{label:'Floor <test>'}]}]};
  else if (u.pathname.endsWith('/issues')) data={issues};
  else if (u.pathname.endsWith('/visits')) {await new Promise(r=>setTimeout(r,delayVisits));data={visits:[visit]};}
  else if (u.pathname.endsWith('/visits/today')) data={visit:{...visit,status:'scheduled',scheduled_at:'2026-10-14T12:00:00Z'}};
  else if (/\/visits\/\d+$/.test(u.pathname)) data={visit};
  return req.respond({status:200,contentType:'application/json',body:JSON.stringify(data)});
 });
 await page.goto('http://localhost/portal/anchor'); await page.waitForFunction(()=>document.querySelector('#tabs button'));
 return {page,mutations,errors};
}
const tab = async (p,id) => {await p.click(`[data-tab="${id}"]`);};
test('desktop: escaped reports/photos, selected demo badge and stale navigation',async t=>{
 const {page:p,errors}=await setup(t,{delayVisits:180});
 await p.waitForSelector('[data-visit]'); await p.click('[data-visit]'); await p.waitForSelector('#backReports');
 assert.equal(await p.$eval('.preserve',e=>e.textContent),'<script>window.injected=true</script>');
 assert.equal(await p.evaluate(()=>window.injected),undefined); await p.click('[data-photo]'); await p.waitForSelector('dialog[open]'); await p.keyboard.press('Escape'); assert.equal(await p.$('dialog'),null);
 await tab(p,'history'); await tab(p,'scope'); await p.waitForFunction(()=>document.querySelector('#view h2')?.textContent==='Facility Areas'); await new Promise(r=>setTimeout(r,250)); assert.equal(await p.$eval('#view h2',e=>e.textContent),'Facility Areas');
 await p.select('#building','2'); assert.equal(await p.$eval('#demoBanner',e=>e.classList.contains('hidden')),true);
 assert.deepEqual(errors,[]);
});
test('mobile: issue photo preview, repeated submit lock, persisted render and cancel resolution',async t=>{
 const {page:p,mutations,errors}=await setup(t,{width:390});
 await tab(p,'report'); await p.waitForSelector('#issueRoom'); await p.type('#issueRoom','West corridor'); await p.type('#issueDesc','Dust at doorway');
 await p.evaluate(async data=>{const blob=await (await fetch(data)).blob();const dt=new DataTransfer();dt.items.add(new File([blob],'sample.png',{type:'image/png'}));const el=document.querySelector('#issuePhoto');el.files=dt.files;el.dispatchEvent(new Event('change'));},photo);
 await p.waitForFunction(()=>document.querySelector('#issuePhotoStatus').textContent.includes('ready'));
 await p.$eval('#portalForm',f=>{f.dispatchEvent(new Event('submit',{cancelable:true}));f.dispatchEvent(new Event('submit',{cancelable:true}));});
 await p.waitForFunction(()=>document.querySelector('#view h2')?.textContent==='Open Issues');
 assert.equal(mutations.filter(x=>x.path.endsWith('/issues')).length,1); assert.match(mutations[0].body.photo_data_url,/^data:image\/jpeg;base64,/); assert.match(mutations[0].body.request_id,/^[a-f0-9-]{36}$/);
 assert.equal(await p.evaluate(()=>window.injected),undefined); assert.ok(await p.$('[data-photo]'));
 await p.click('[data-resolve]');await p.waitForSelector('#resolution');await p.type('#resolution','Draft only');await p.click('#cancelForm');await p.waitForFunction(()=>document.querySelector('#view h2')?.textContent==='Open Issues'); assert.equal(mutations.length,1);
 assert.equal(await p.evaluate(()=>document.documentElement.scrollWidth > innerWidth),false); assert.deepEqual(errors,[]);
});
test('operator authors report with areas and photos; invalid file blocks submission',async t=>{
 const {page:p,mutations,errors}=await setup(t);await tab(p,'author');await p.waitForSelector('#reportNotes');await p.click('[name=area]');await p.type('#reportNotes','Synthetic report');
 await p.evaluate(()=>{const dt=new DataTransfer();dt.items.add(new File(['<svg/>'],'bad.svg',{type:'image/svg+xml'}));const el=document.querySelector('#reportPhotos');el.files=dt.files;el.dispatchEvent(new Event('change'));});
 await p.waitForFunction(()=>document.querySelector('#reportPhotosStatus').textContent.includes('JPEG'));await p.click('#saveForm');await p.waitForFunction(()=>!document.querySelector('[data-form-error]').classList.contains('hidden'));assert.equal(mutations.length,0);
 await p.evaluate(async data=>{const blob=await (await fetch(data)).blob();const dt=new DataTransfer();dt.items.add(new File([blob],'sample.png',{type:'image/png'}));const el=document.querySelector('#reportPhotos');el.files=dt.files;el.dispatchEvent(new Event('change'));},photo);
 await p.waitForFunction(()=>document.querySelector('#reportPhotosStatus').textContent.includes('ready'));await p.click('#saveForm');await p.waitForFunction(()=>document.querySelector('#view h2')?.textContent==='Service Reports');assert.equal(mutations.length,1);assert.equal(mutations[0].body.areas[0].title,'West <Wing>');assert.equal(mutations[0].body.photos.length,1);assert.deepEqual(errors,[]);
});
test('cleaner viewing visit does not start it; unassigned account has useful empty state',async t=>{
 const {page:p,mutations}=await setup(t,{role:'cleaner'});await p.waitForSelector('#startVisit');assert.equal(mutations.length,0);
 const {page:q}=await setup(t,{locations:[]});await q.waitForFunction(()=>document.querySelector('#view h2')?.textContent==='No building assigned');assert.ok(await q.$eval('#view',e=>e.textContent.includes('Anchor contact')));
});
