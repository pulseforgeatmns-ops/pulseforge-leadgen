'use strict';
const test=require('node:test');const assert=require('node:assert/strict');const fs=require('node:fs');const vm=require('node:vm');const path=require('node:path');
const html=fs.readFileSync(path.join(__dirname,'../public/signal-distribution.html'),'utf8');
const source=html.match(/<script>([\s\S]*?)<\/script>/)[1];
test('mobile page script parses and contains no executable HTML rendering or external dependencies',()=>{
 new vm.Script(source);assert.match(html,/name="viewport"/);assert.doesNotMatch(source,/innerHTML|outerHTML|insertAdjacentHTML|eval\(/);assert.doesNotMatch(html,/<script[^>]+src=|<link[^>]+href="https?:/);assert.match(source,/textContent/);
});
test('reference form has explicit preview, affirmative confirmation, bounded time and same-origin JSON write',()=>{
 assert.match(source,/Preview reference/);assert.match(source,/window\.confirm/);assert.match(source,/if\(!window\.confirm[\s\S]*?\)\)return/);assert.match(source,/confirm:true/);assert.match(source,/credentials:'same-origin'/);assert.match(source,/EXPLICIT_MANUAL_OPERATOR/);assert.doesNotMatch(source,/date<Date\.parse\(m\.created_at\)/);assert.match(source,/earlier samples are not recomputed/);assert.match(source,/stamp\(reference\.savedAt\)/);assert.match(source,/Reference saved\. No trade executed/);
});
test('coverage, expired data, failed polls, admissions and acceptance distinction are prominent',()=>{
 for(const expected of ['UNKNOWN','STALE / EXPIRED / UNAVAILABLE','LATEST POLL FAILED','Admission decisions','Receipt and display on your device are unverified','Recent saved samples','No trade will be executed'])assert.ok(html.includes(expected),expected);
 assert.match(source,/if\(reference\)[\s\S]*?return;/);assert.doesNotMatch(source,/setInterval|localStorage|sessionStorage/);
});
