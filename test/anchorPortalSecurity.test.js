'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {assertPortalUser,validatePhoto}=require('../services/anchorPortal');

test('portal identity requires Anchor tenant or explicitly unscoped platform admin',()=>{
  for(const user of [null,{role:'admin'},{id:1,role:'manager'},{id:1,role:'admin',client_id:20},{id:1,role:'facility_client',client_id:20},{id:1,role:'cleaner',client_id:10,active:false}]) assert.throws(()=>assertPortalUser(user),{code:'forbidden'});
  for(const user of [{id:1,role:'admin',client_id:null},{id:1,role:'manager',client_id:10},{id:1,role:'facility_client',client_id:10}]) assert.doesNotThrow(()=>assertPortalUser(user));
});

test('photo validation rejects remote URLs, SVG, malformed base64 and mismatched signatures',()=>{
  for(const value of ['https://example.test/photo.jpg','data:image/svg+xml;base64,PHN2Zz4=', 'data:image/png;base64,aGVsbG8=', 'data:image/png;base64,%%%%', 'data:image/jpeg;base64,iVBORw0KGgo=', 'data:image/png;base64,iVBORw0KGgo=']) assert.throws(()=>validatePhoto(value),{code:'invalid_photo'});
  const png=require('./helpers/exeterPortalHarness').illustrativePhoto(0).data_url;
  assert.equal(validatePhoto(png,'Sample').mime_type,'image/png');
  assert.throws(()=>validatePhoto(png,'x'.repeat(301)),{code:'invalid_text'});
});

test('schema startup does not seed demo accounts or contain a fallback credential',()=>{
  const schema=fs.readFileSync(path.join(__dirname,'../utils/anchorPortalSchema.js'),'utf8');
  assert.match(schema,/ensureAnchorPortalSchema\(\{ seedDemo = false \}/);
  assert.doesNotMatch(schema,/ANCHOR_PORTAL_DEMO_PASSWORD\s*\|\|/);
  const server=fs.readFileSync(path.join(__dirname,'../server.js'),'utf8');
  assert.ok(server.indexOf("app.use('/api/anchor-portal', express.json({ limit: '8mb' }))")<server.indexOf('app.use(express.json({ verify: captureBrevoRawBody }))'));
});


test('canonical public origin works behind TLS termination without trusting forwarded headers',()=>{
  const {expectedOrigin}=require('../routes/anchorPortal');
  const prior=process.env.APP_URL;
  const req={protocol:'http',get:key=>key==='host'?'portal.example.test':key==='x-forwarded-proto'?'https':undefined};
  try {
    process.env.APP_URL='https://portal.example.test/path';
    assert.equal(expectedOrigin(req),'https://portal.example.test');
    process.env.APP_URL='javascript:invalid'; assert.equal(expectedOrigin(req),null);
    delete process.env.APP_URL; assert.equal(expectedOrigin(req),null);
    assert.equal(expectedOrigin({protocol:'http',get:()=> '127.0.0.1:12345'}),'http://127.0.0.1:12345');
  } finally {if(prior===undefined)delete process.env.APP_URL;else process.env.APP_URL=prior;}
});
