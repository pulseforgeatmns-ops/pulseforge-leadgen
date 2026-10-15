'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const source=fs.readFileSync(require('node:path').join(__dirname,'../public/anchor-portal.js'),'utf8');
function context(extra={}) {
 const elements={view:{innerHTML:'',querySelectorAll:()=>[]},flash:{textContent:'',className:''}};
 const ctx=vm.createContext({document:{getElementById:id=>elements[id]},console,...extra});
 vm.runInContext(source.replace(/boot\(\)\.catch\(e => loadError\(state.generation,e\)\);\s*$/,''),ctx);
 return {ctx,elements,run:code=>vm.runInContext(code,ctx)};
}
test('portal escapes text and attributes and rejects non-raster photo URLs',()=>{
 const {run}=context();assert.equal(run('esc(' + JSON.stringify('<img src=x onerror="alert(1)">&\'') + ')'),'&lt;img src=x onerror=&quot;alert(1)&quot;&gt;&amp;&#39;');
 for(const url of ['javascript:alert(1)','https://example.com/a.jpg','data:image/svg+xml;base64,PHN2Zz4=','data:image/png;base64,AAA" onerror="x']) assert.equal(run(`safePhoto(${JSON.stringify(url)})`),'');
 assert.equal(run(`safePhoto('data:image/png;base64,YQ==')`),'data:image/png;base64,YQ==');
 assert.ok(!run(`gallery([{data_url:'javascript:alert(1)',caption:'bad'}])`).includes('<img'));
 assert.ok(run(`reportRows([{id:'1" onclick="x',title:'<script>x</script>',status:'<img>'}])`).includes('&lt;script&gt;'));
});
test('stale async responses never replace the newer view',()=>{
 const {run,elements}=context();run('state.generation=3');assert.equal(run("paint(2,'stale')"),false);assert.equal(elements.view.innerHTML,'');assert.equal(run("paint(3,'new')"),true);assert.equal(elements.view.innerHTML,'new');
});
test('submission lock blocks repeated actions and unlocks after failures',async()=>{
 const {ctx,run}=context();run('renderTabs=()=>{}');let resolve;let calls=0;ctx.button={textContent:'Save',disabled:false,isConnected:true,closest:()=>null};ctx.work=()=>{calls++;return new Promise(r=>{resolve=r;});};
 const first=run('mutate(button,work)');await run('mutate(button,work)');assert.equal(calls,1);assert.equal(ctx.button.disabled,true);resolve();await first;assert.equal(ctx.button.disabled,false);assert.equal(run('state.busy'),false);
 ctx.work=async()=>{throw new Error('<b>failed</b>');};await run('mutate(button,work)');assert.equal(run('state.busy'),false);assert.equal(ctx.button.textContent,'Save');
});
test('photo preparation rejects wrong type and oversize source before decoding',async()=>{
 const {ctx,run}=context();ctx.file={type:'image/svg+xml',size:10};await assert.rejects(run('compressPhoto(file)'),/JPEG, PNG or WebP/);ctx.file={type:'image/png',size:16*1024*1024};await assert.rejects(run('compressPhoto(file)'),/15 MB/);
});
test('cleaner workflow has explicit start, atomic completion photos and no prompt-based resolution',()=>{
 assert.ok(!source.includes('prompt('));assert.match(source,/id="startVisit"/);assert.match(source,/visit_notes:\$\('visitNotes'\)\.value\.trim\(\),photos:p/);assert.match(source,/request_id:requestId/);assert.ok(!source.includes("/evidence`,{method:'POST'"));
 const html=fs.readFileSync(require('node:path').join(__dirname,'../public/anchor-portal.html'),'utf8');assert.ok(!html.includes('fonts.googleapis.com'));assert.match(html,/aria-label="Assigned building"/);
});
test('photo compression is bounded and revokes local image handles',async()=>{
 let revoked=0,draws=0;
 class ImageMock {set src(_) {this.naturalWidth=4000;this.naturalHeight=3000;queueMicrotask(()=>this.onload());}}
 const canvas={getContext:()=>({fillRect(){},drawImage(){draws++;}}),toDataURL:()=> 'data:image/jpeg;base64,'+'A'.repeat(2100000)};
 const {ctx,run}=context({Image:ImageMock,URL:{createObjectURL:()=> 'blob:local',revokeObjectURL:()=>revoked++},document:{createElement:()=>canvas,getElementById:()=>null}});
 ctx.file={type:'image/jpeg',size:100};await assert.rejects(run('compressPhoto(file)'),/still too large/);assert.equal(draws,5);assert.equal(revoked,1);
});
test('demo marker follows only the selected building; cleaner can author reports',()=>{
 const elements={tabs:{innerHTML:'',querySelectorAll:()=>[]},building:{},demoBanner:{classList:{toggle(name,value){this[name]=value;}}},pageTitle:{}};
 const {run}=context({document:{getElementById:id=>elements[id]}});
 run("state.me={capabilities:{cleaner:true},locations:[{id:1,name:'Demo',is_demo:true},{id:2,name:'Live',is_demo:false}]};state.locationId=2;renderTabs()");
 assert.equal(elements.demoBanner.classList.hidden,true);assert.ok(elements.tabs.innerHTML.includes('New Service Report'));
 run('state.locationId=1;renderTabs()');assert.equal(elements.demoBanner.classList.hidden,false);assert.match(elements.demoBanner.textContent,/Synthetic/);assert.ok(!elements.demoBanner.textContent.includes('PASSWORD'));
});
