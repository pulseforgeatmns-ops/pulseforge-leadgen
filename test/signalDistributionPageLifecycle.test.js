'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../public/signal-distribution.html'),'utf8').match(/<script>([\s\S]*?)<\/script>/)[1];
class Element {
 constructor(tag){this.tag=tag;this.children=[];this.listeners={};this.value='';this.classList={add(){},remove(){}};this._text='';}
 set textContent(s){this._text=String(s);this.children=[];}get textContent(){return this._text+this.children.map(c=>c.textContent).join('');}
 append(...children){this.children.push(...children);}replaceChildren(...children){this._text='';this.children=children;}
 setAttribute(){}addEventListener(name,fn){this.listeners[name]=fn;}
 all(){return [this,...this.children.flatMap(c=>c.all())];}
}
function setup(){
 const ids=new Map();const get=id=>{if(!ids.has(id))ids.set(id,new Element('div'));return ids.get(id);};
 const context=vm.createContext({document:{getElementById:get,createElement:t=>new Element(t)},fetch:()=>new Promise(()=>{}),window:{confirm:()=>false},Date,Number,String,Error,encodeURIComponent,JSON});
 vm.runInContext(source,context);vm.runInContext("namespace='operational';selected='monitor-1';",context);
 const target=new Element('div');const monitor={id:'monitor-1',token_address:'TEST_CA',created_at:new Date(Date.now()-1000).toISOString(),expires_at:new Date(Date.now()+3600000).toISOString(),state:{}};
 context.renderReference(target,monitor);
 const form=target.all().find(e=>e.tag==='form'),price=target.all().find(e=>e.type==='number'),time=target.all().find(e=>e.type==='datetime-local');
 price.value='0.01';time.value=new Date(Date.now()-3600000).toISOString();
 const submit=()=>form.listeners.submit({preventDefault(){}});
 return{context,target,monitor,submit,price,time};
}
test('executed reference form: cancel shows preview and issues no write',async()=>{
 const h=setup();let writes=0;h.context.api=async()=>{writes++;};await h.submit();assert.equal(writes,0);assert.match(h.target.textContent,/Preview: USD 0.01/);
});
test('executed reference form: confirmed backdated reference accepted; duplicate in-flight submission suppressed',async()=>{
 const h=setup();let writes=0,details=0,release;h.context.window.confirm=()=>true;
 h.context.api=async(_url,options)=>{writes++;const body=JSON.parse(options.body);assert.equal(body.confirm,true);assert.equal(body.source,'EXPLICIT_MANUAL_OPERATOR');return new Promise(r=>release=r);};
 h.context.showDetail=async()=>{details++;};const first=h.submit();await h.submit();assert.equal(writes,1);release({saved:true});await first;assert.equal(details,1);
});
test('executed reference form: completed old save cannot reopen a changed namespace or selection',async()=>{
 for(const navigation of ["namespace='test';selected=null;","selected='another-monitor';"]){const h=setup();let details=0,release;h.context.window.confirm=()=>true;h.context.api=()=>new Promise(r=>release=r);h.context.showDetail=async()=>{details++;};const pending=h.submit();vm.runInContext(navigation,h.context);release({saved:true});await pending;assert.equal(details,0);}
});
test('executed reference form: errors allow explicit retry; invalid/future values never prompt or write',async()=>{
 const h=setup();let prompts=0,writes=0;h.context.window.confirm=()=>{prompts++;return true;};h.context.api=async()=>{writes++;throw Error('offline');};
 h.price.value='0';await h.submit();h.price.value='.01';h.time.value=new Date(Date.now()+3600000).toISOString();await h.submit();assert.equal(prompts,0);assert.equal(writes,0);
 h.time.value=new Date(Date.now()-1000).toISOString();await h.submit();await h.submit();assert.equal(writes,2);assert.match(h.target.textContent,/offline/);
});
test('executed saved reference separates entered time and forward saved boundary, without save form',()=>{
 const h=setup();const target=new Element('div');h.monitor.state.reference={priceUsd:1,recordedAt:'2026-10-14T10:00:00Z',savedAt:'2026-10-14T13:00:00Z',provenance:'EXPLICIT_MANUAL_OPERATOR'};h.context.renderReference(target,h.monitor);assert.equal(target.all().some(e=>e.tag==='form'),false);assert.match(target.textContent,/Entered reference time: 2026-10-14T10/);assert.match(target.textContent,/forward observation boundary: 2026-10-14T13/);assert.match(target.textContent,/earlier samples are not recomputed/);
});
