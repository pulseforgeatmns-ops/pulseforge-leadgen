#!/usr/bin/env node
// Read-only by default. An invalid POST cannot create a request. --submit is an
// explicit operator option for the final, clearly labelled real queue smoke.
import { randomUUID } from 'node:crypto';
const base = 'https://studiosubstral.com';
const api = 'https://pulseforge-leadgen-production.up.railway.app/api/public/website-assessment';
const results = [];
const request = (url, options = {}) => fetch(url, { signal: AbortSignal.timeout(15000), ...options });
const check = async (name, fn) => { try { await fn(); results.push({ name, pass: true }); } catch (error) { results.push({ name, pass: false, reason: error.message }); } };
const ensure = (condition, message) => { if (!condition) throw new Error(message); };
await Promise.all([
 check('canonical HTML, metadata and assessment route', async () => {
  const res = await request(`${base}/`); ensure(res.status === 200, `HTTP ${res.status}`);
  const html = await res.text();
  ensure(html.includes('href="https://studiosubstral.com/"'), 'canonical missing');
  ensure(html.includes(`action="${api}"`), 'intake action missing');
  ensure(html.includes('Human reviewed'), 'human review wording missing');
  ensure(html.includes('application/ld+json') && html.includes('twitter:card'), 'structured/social metadata missing');
  ensure(!/name="robots"[^>]*noindex/.test(html), 'production is noindex');
 }),
 ...['robots.txt','sitemap.xml','favicon.ico','favicon-16x16.png','favicon-32x32.png','apple-touch-icon.png','site.webmanifest','assets/brand/social-preview.png','assets/brand/favicon.svg','assets/brand/favicon-32.png','assets/brand/apple-touch-icon.png','assets/brand/site.webmanifest','assets/css/substral.css','assets/js/substral.js','assets/js/assessment.js','assets/js/dimensional.js','assets/work/anchor-cleaning-home.webp'].map(file => check(file, async () => {
  const res=await request(`${base}/${file}`); ensure(res.status===200, `HTTP ${res.status}`);
  const content=await res.arrayBuffer(); ensure(content.byteLength>0,'empty asset');
  if(file==='robots.txt'||file==='sitemap.xml')ensure(new TextDecoder().decode(content).includes(base),'wrong sitemap/canonical host');
 })),
 ...['http://studiosubstral.com/','https://www.studiosubstral.com/'].map(url=>check(`redirect ${url}`,async()=>{
  const res=await request(url,{redirect:'manual'});ensure([301,302,307,308].includes(res.status),`HTTP ${res.status}`);
  ensure(new URL(res.headers.get('location'),url).href === `${base}/`,'does not redirect to canonical HTTPS apex');
 })),
 check('API preflight',async()=>{
  const res=await request(api,{method:'OPTIONS',headers:{Origin:base,'Access-Control-Request-Method':'POST','Access-Control-Request-Headers':'content-type'}});
  ensure(res.status===204,`HTTP ${res.status}`);ensure(res.headers.get('access-control-allow-origin')===base,'unexpected allowed origin');
 }),
 check('API validation without creating a request',async()=>{
  const res=await request(api,{method:'POST',headers:{Origin:base,'Content-Type':'application/json'},body:'{}'});
  ensure(res.status===400,`HTTP ${res.status}`);ensure((await res.json()).details?.domain==='empty','unexpected validation body');
 }),
]);
if(process.argv.includes('--submit')) {
 const email=process.env.SUBSTRAL_SMOKE_EMAIL;
 if(!email)throw new Error('Set SUBSTRAL_SMOKE_EMAIL to an operator-controlled inbox.');
 if(results.some(r=>!r.pass))throw new Error('Resolve read-only smoke failures before submitting.');
 await check('one persisted human-review request, safe replay',async()=>{
  const body=JSON.stringify({domain:'example.com',email,context:'LAUNCH SMOKE TEST — operator to mark reviewed; no assessment needed.',request_key:randomUUID()});
  const options={method:'POST',headers:{Origin:base,'Content-Type':'application/json'},body};
  const first=await request(api,options);const a=await first.json();ensure(first.status===201&&a.ok&&a.review_mode==='human'&&a.request_id,'first request not confirmed');
  const retry=await request(api,options);const b=await retry.json();ensure(retry.status===200&&a.request_id===b.request_id,'replay did not match');
  results.push({name:'operator queue read-back required',request_id:a.request_id,client_id:1,pass:null});
 });
}
console.log(JSON.stringify({checkedAt:new Date().toISOString(),results},null,2));
if(results.some(r=>r.pass===false))process.exitCode=1;
