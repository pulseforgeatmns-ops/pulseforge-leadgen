'use strict';
// Local-only synthetic fixture verification. Never connects to the application database.
const http=require('node:http'),fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const puppeteer=require('puppeteer');
const root=path.join(__dirname,'../public');
const output=process.env.ANCHOR_SCREENSHOT_DIR || '/tmp/anchor-portal-reference';
const location={id:1,name:'Example Office Center',address:'100 Example Street\nManchester, NH',is_demo:true,next_scheduled_at:'2026-10-19T18:00:00Z'};
const visits=Array.from({length:5},(_,i)=>({id:i+1,status:'completed',completed_at:`2026-10-${15-i}T19:00:00Z`,completed_by_name:['Alex T.','Jordan S.'][i%2]}));
const scope=['Lobby','Restrooms','Break Room','Offices'].map((title,i)=>({title,items:[{id:i+1,label:['Floors, entry, glass, seating','Clean, sanitize, restock','Counters, appliances, floors','Dust, trash, surfaces'][i]}]}));
const issues=[{id:1,title:'Entry door glass',description:'Marks reported on the entry door glass.',status:'resolved',created_at:'2026-10-13T14:00:00Z',resolved_at:'2026-10-14T18:00:00Z',resolution:'Entry glass cleaned and checked.',evidence:[]}];
let role=process.env.ANCHOR_FIXTURE_ROLE || 'facility_client',scopeFails=false,empty=false;
const home=()=>({location,last_service:empty?null:visits[0].completed_at,next_service:empty?null:location.next_scheduled_at,service_status:empty?'No completed visits yet':'All scheduled work completed',open_issues_count:0,recent_visits:empty?[]:visits});
const server=http.createServer((req,res)=>{
 const url=new URL(req.url,'http://localhost');
 if(url.pathname.startsWith('/api/anchor-portal')){
  const route=url.pathname.slice('/api/anchor-portal'.length);let data;
  if(route==='/me')data={user:{id:1,name:'Example User',email:'fixture@example.test',role},capabilities:{facility_client:role==='facility_client',cleaner:role==='cleaner',operator:role==='operator'},locations:[location]};
  else if(route==='/home')data=home();
  else if(route==='/locations/1/scope'){if(scopeFails){res.writeHead(503,{'Content-Type':'application/json'});res.end(JSON.stringify({message:'Fixture scope unavailable'}));return;}data={scope:empty?[]:scope};}
  else if(route==='/issues')data={issues:empty||url.searchParams.get('status')==='active'?[]:issues};
  else if(route==='/visits')data={visits:empty?[]:visits};
  else if(route==='/visits/today')data={visit:{id:9,location_id:1,location_name:location.name,status:'in_progress',scheduled_at:location.next_scheduled_at,items:scope.map((s,i)=>({id:i+1,section_title:s.title,item_label:s.items[0].label,outcome:i<2?'completed':''}))}};
  else if(/^\/visits\/\d+$/.test(route))data={visit:{...visits[0],location_name:location.name,items:[],evidence:[],visit_notes:'Synthetic report for local browser verification.'}};
  else {res.writeHead(404);res.end('{}');return;}
  res.writeHead(200,{'Content-Type':'application/json'});res.end(JSON.stringify(data));return;
 }
 const file=url.pathname==='/'?path.join(root,'anchor-portal.html'):path.join(root,url.pathname);
 if(!file.startsWith(root)||!fs.existsSync(file)){res.writeHead(404);res.end();return;}
 res.setHeader('Content-Type',file.endsWith('.js')?'application/javascript':file.endsWith('.png')?'image/png':'text/html');fs.createReadStream(file).pipe(res);
});
(async()=>{
 fs.mkdirSync(output,{recursive:true});await new Promise(r=>server.listen(process.env.ANCHOR_FIXTURE_ONLY ? 4179 : 0,'127.0.0.1',r));const port=server.address().port;
 if(process.env.ANCHOR_FIXTURE_ONLY){console.log(`Synthetic portal fixture: http://127.0.0.1:${port}/`);return;}
 const browser=await puppeteer.launch({executablePath:process.env.CHROMIUM_PATH||'/usr/bin/chromium',headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{
 const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.setRequestInterception(true);page.on('request',req=>req.url().startsWith(`http://127.0.0.1:${port}/`)||req.url().startsWith('data:')?req.continue():req.abort());
 const ready=()=>page.waitForFunction(()=>document.querySelector('#view').textContent.includes('Recent Visits'));
 await page.setViewport({width:1448,height:1086,deviceScaleFactor:1});await page.goto(`http://127.0.0.1:${port}/`);await ready();await page.screenshot({path:path.join(output,'dashboard-desktop.png'),fullPage:true});
 for(const width of [360,390,768,1448]){await page.setViewport({width,height:1086,deviceScaleFactor:1});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,`no page overflow at ${width}`);if(width!==1448)await page.screenshot({path:path.join(output,`dashboard-${width}.png`),fullPage:true});}
 await page.click('[data-tab="account"]');await page.waitForFunction(()=>document.querySelector('#view').textContent.includes('fixture@example.test'));
 await page.click('[data-tab="home"]');await ready();await page.click('[data-go="history"]');await page.waitForSelector('[data-visit]');await page.click('[data-visit]');await page.waitForSelector('#backReports');await page.click('#backReports');await page.waitForSelector('[data-visit]');
 await page.click('[data-tab="issues"]');await page.waitForSelector('[data-go="resolved"]');await page.click('[data-go="resolved"]');await page.waitForFunction(()=>document.querySelector('#view').textContent.includes('Entry glass cleaned'));await page.screenshot({path:path.join(output,'issues-desktop.png'),fullPage:true});
 await page.waitForSelector('#issueDesc');await page.type('#issueDesc','Unsaved synthetic issue');await page.click('#cancelForm');await page.waitForSelector('#portalForm');assert.equal(await page.$eval('#issueDesc',e=>e.value),'');
 scopeFails=true;await page.click('[data-tab="home"]');await ready();assert.match(await page.$eval('#view',e=>e.textContent),/scope is temporarily unavailable/);scopeFails=false;
 empty=true;await page.reload();await ready();assert.match(await page.$eval('#view',e=>e.textContent),/No service reports/);await page.screenshot({path:path.join(output,'dashboard-empty.png'),fullPage:true});empty=false;
 role='cleaner';await page.setViewport({width:390,height:1000,deviceScaleFactor:1});await page.reload();await page.waitForSelector('#checklistProgress');assert.equal(await page.$eval('#checklistProgress',e=>e.value),2);const outcomes=await page.$$('.outcome');await outcomes[2].select('completed');assert.equal(await page.$eval('#checklistProgress',e=>e.value),3);await page.screenshot({path:path.join(output,'checklist-mobile.png'),fullPage:true});
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'cleaner no mobile overflow');assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,output,checks:['1448px desktop','360/390/768px responsive','Account','visits and Back','resolved issues','issue form Cancel','scope failure','empty dashboard','cleaner progress','no browser errors']}));
 }finally{await browser.close();await new Promise(r=>server.close(r));}
})().catch(e=>{console.error(e);server.close();process.exitCode=1;});
