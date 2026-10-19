'use strict';
const express = require('express');
const session = require('express-session');
const PgStore = require('connect-pg-simple')(session);
const bcrypt = require('bcryptjs');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { verifyDatabase, poolFor } = require('./database');
const { config } = require('./config');
const { publicState, IDENTITIES, EXPIRES_AT } = require('./public-demo');
const cookieName = 'anchor.demo.sid';
async function createApp(cfg, pool, { sessionMs = 30 * 60 * 1000 } = {}) {
  await verifyDatabase(pool);
  if(cfg.publicDemo)await publicState(pool);
  const dummyHash = await bcrypt.hash(crypto.randomBytes(32).toString('hex'), 12);
  const app = express();
  app.disable('x-powered-by');
  // Hosted runtime assumes exactly one trusted ingress proxy, not arbitrary hops.
  app.set('trust proxy', cfg.mode === 'hosted' ? 1 : false);
  app.use((req, res, next) => {
    res.set({ 'Cache-Control':'no-store', 'X-Content-Type-Options':'nosniff', 'Referrer-Policy':'no-referrer', 'Content-Security-Policy':"default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'" });
    if (req.get('host') !== new URL(cfg.origin).host || (cfg.mode === 'hosted' && !req.secure)) return res.sendStatus(400);
    next();
  });
  app.use(async (_req,res,next)=>{
    try{if(cfg.publicDemo&&Date.now()>=EXPIRES_AT)return res.status(410).json({error:'demo_expired'});if(cfg.publicDemo)await publicState(pool);const {rows:[state]}=await pool.query('SELECT expires_at FROM portal_demo_marker');
      if(!state?.expires_at||new Date(state.expires_at)<=new Date())return res.status(410).json({error:'demo_expired'});next();
    }catch(error){next(error);}
  });
  const store = new PgStore({ pool, tableName:'portal_sessions', createTableIfMissing:false, pruneSessionInterval: 900 });
  app.use(session({ name:cookieName, store, secret:cfg.secret, resave:false, saveUninitialized:false, rolling:false, cookie:{ httpOnly:true, sameSite:'strict', secure:cfg.mode === 'hosted', maxAge:sessionMs } }));
  app.use(express.json({ limit:'8mb' }));
  app.use(express.urlencoded({ extended:false, limit:'2kb' }));
  app.use((req, res, next) => {
    if (!['GET','HEAD','OPTIONS'].includes(req.method)) {
      if (req.get('origin') !== cfg.origin || !req.session.csrf || req.get('x-csrf-token') !== req.session.csrf) return res.status(403).json({ error:'forbidden' });
    }
    next();
  });
  // Public mode never adopts an arbitrary authenticated identity or an old mode's session.
  app.use((req,res,next)=>{
    if(req.session.publicDemo && !cfg.publicDemo)return req.session.destroy(error=>error?next(error):res.status(401).json({error:'unauthorized'}));
    next();
  });
  let issuance={count:0,until:0};
  const sources=new Map();
  function allowIssuance(req){
    const now=Date.now();
    if(issuance.until<=now)issuance={count:0,until:now+60000};
    for(const [key,value] of sources)if(value.until<=now)sources.delete(key);
    const source=sources.get(req.ip)||{count:0,until:now+60000};
    if(issuance.count>=120||source.count>=20||(!sources.has(req.ip)&&sources.size>=10000))return false;
    issuance.count++;source.count++;sources.set(req.ip,source);return true;
  }
  async function establishDemo(req,key){
    await publicState(pool);
    await new Promise((resolve,reject)=>req.session.regenerate(e=>e?reject(e):resolve()));
    req.session.userId=IDENTITIES[key].id;
    req.session.publicDemo=key;
    req.session.expiresAt=Math.min(Date.now()+sessionMs,EXPIRES_AT);
    req.session.cookie.maxAge=Math.max(0,req.session.expiresAt-Date.now());
    req.session.csrf=crypto.randomBytes(32).toString('hex');
    await new Promise((resolve,reject)=>req.session.save(e=>e?reject(e):resolve()));
  }
  function validDemoSession(req){return Object.hasOwn(IDENTITIES,req.session.publicDemo||'')&&req.session.userId===IDENTITIES[req.session.publicDemo].id&&Number.isFinite(req.session.expiresAt)&&req.session.expiresAt>Date.now()&&req.session.expiresAt<=EXPIRES_AT;}
  if(cfg.publicDemo){
    const writes=new Map();
    let writeWindow={count:0,until:0},totalWrites=0;
    app.use((req,res,next)=>{
      if(['GET','HEAD','OPTIONS'].includes(req.method))return next();
      const now=Date.now();
      if(writeWindow.until<=now)writeWindow={count:0,until:now+60000};
      if(writeWindow.count>=120||totalWrites>=200)return res.status(429).json({error:'demo_write_limit'});
      for(const [key,value] of writes)if(value.until<=now)writes.delete(key);
      const entry=writes.get(req.ip)||{count:0,until:now+60000};
      if(entry.count>=60||(!writes.has(req.ip)&&writes.size>=10000))return res.status(429).json({error:'try_later'});
      entry.count++;writeWindow.count++;totalWrites++;writes.set(req.ip,entry);next();
    });
    app.get(['/','/portal/anchor','/login'],async(req,res,next)=>{
      try{if(!validDemoSession(req)){if(!allowIssuance(req))return res.status(429).json({error:'try_later'});await establishDemo(req,'client');}if(req.path==='/login')return res.redirect('/portal/anchor');next();}catch(error){next(error);}
    });
    app.post('/auth/demo-role',async(req,res,next)=>{
      try{if(!validDemoSession(req))return res.status(401).json({error:'unauthorized'});
        if(!req.body||Object.keys(req.body).length!==1||typeof req.body.role!=='string'||!Object.hasOwn(IDENTITIES,req.body.role))return res.status(400).json({error:'invalid_role'});
        if(!allowIssuance(req))return res.status(429).json({error:'try_later'});
        await establishDemo(req,req.body.role);res.json({ok:true});
      }catch(error){next(error);}
    });
    app.get('/logout',(_req,res)=>res.redirect('/portal/anchor'));
    app.get('/demo-ui.js',requireAuth,(_req,res)=>res.sendFile(path.join(__dirname,'demo-ui.js')));
  }
  app.get('/auth/csrf', (req,res) => { if(cfg.publicDemo&&!validDemoSession(req))return res.status(401).json({error:'unauthorized'});req.session.csrf ||= crypto.randomBytes(32).toString('hex'); res.json({ csrf:req.session.csrf }); });
  // Bounded per-source limiter. Reverse proxy must overwrite forwarding headers.
  const attempts = new Map();
  app.post('/login', async (req,res,next) => {
    try {
      if(cfg.publicDemo)return res.status(404).json({error:'not_found'});
      const now = Date.now();
      for (const [key, entry] of attempts) if (entry.until < now) attempts.delete(key);
      const entry = attempts.get(req.ip) || { count:0, until:now+15*60*1000 };
      if (entry.count >= 10 || attempts.size > 10000) return res.status(429).json({ error:'try_later' });
      entry.count++; attempts.set(req.ip,entry);
      const { email,password } = req.body || {};
      if (typeof email !== 'string' || typeof password !== 'string' || email.length > 254 || password.length < 1 || Buffer.byteLength(password) > 72) return res.status(401).json({ error:'invalid_login' });
      const { rows } = await pool.query('SELECT id,password_hash,valid_until FROM users WHERE lower(email)=$1 AND active=TRUE AND valid_until>NOW()', [email.toLowerCase().trim()]);
      const validPassword = await bcrypt.compare(password, rows[0]?.password_hash || dummyHash);
      if (!rows[0] || !validPassword) return res.status(401).json({ error:'invalid_login' });
      await new Promise((resolve,reject) => req.session.regenerate(e => e ? reject(e) : resolve()));
      attempts.delete(req.ip);
      req.session.userId = rows[0].id;
      req.session.expiresAt = Math.min(now + sessionMs,new Date(rows[0].valid_until).getTime());
      req.session.csrf = crypto.randomBytes(32).toString('hex');
      await new Promise((resolve,reject) => req.session.save(e => e ? reject(e) : resolve()));
      res.json({ ok:true });
    } catch(error) { next(error); }
  });
  app.post('/logout', (req,res,next) => req.session.destroy(error => { if(error) return next(error); res.clearCookie(cookieName,{path:'/'}); res.json({ok:true}); }));
  async function requireAuth(req,res,next) {
    try {
      if(cfg.publicDemo&&!validDemoSession(req))return res.status(401).json({error:'unauthorized'});
      if (!req.session.userId || req.session.expiresAt <= Date.now()) return req.path.startsWith('/api/') ? res.status(401).json({error:'unauthorized'}) : res.redirect('/login');
      const { rows } = await pool.query('SELECT id,name,email,role,client_id,active FROM users WHERE id=$1 AND active=TRUE AND valid_until>NOW()',[req.session.userId]);
      if (!rows[0]) return res.status(401).json({error:'unauthorized'});
      req.user=rows[0]; next();
    } catch(error) { next(error); }
  }
  const requireRole = (...roles) => (req,res,next) => roles.includes(req.user?.role) ? next() : res.sendStatus(403);
  app.get(['/login','/logout'], (_req,res) => res.sendFile(path.join(__dirname,'login.html')));
  app.get('/auth-ui.js', (_req,res) => res.sendFile(path.join(__dirname,'auth-ui.js')));
  app.get('/portal-bootstrap.js', requireAuth, (_req,res) => res.type('js').send("fetch('/auth/csrf').then(r=>r.json()).then(v=>{window.PORTAL_CSRF=v.csrf;const s=document.createElement('script');s.src='/anchor-portal.js';document.body.appendChild(s);});"));
  app.get('/anchor-portal.js', requireAuth, (_req,res) => res.sendFile(path.join(__dirname,'../../public/anchor-portal.js')));
  app.get('/portal/anchor', requireAuth, requireRole('manager','cleaner','facility_client'), (_req,res) => res.type('html').send(fs.readFileSync(path.join(__dirname,'../../public/anchor-portal.html'),'utf8').replace('src="/anchor-portal.js"','src="/portal-bootstrap.js"').replace('<a class="btn ghost small" href="/logout">Sign out</a>',cfg.publicDemo?'<button class="btn ghost small" id="demoRole" type="button">Demo client / operator</button><script src="/demo-ui.js" defer></script>':'<a class="btn ghost small" href="/logout">Sign out</a>')));
  app.use(require('../../routes/anchorPortal').createPortalRouter({requireAuth,requireRole,canonicalOrigin:cfg.origin,service:require('../../services/anchorPortal').createPortalService(pool)}));
  app.get('/', (_req,res) => res.redirect('/portal/anchor'));
  app.use((_req,res) => res.sendStatus(404));
  app.use((_err,_req,res,_next) => res.status(500).json({error:'internal_error'}));
  return { app, store };
}
if (require.main === module) {
  (async () => {
    const cfg = config(process.env); const pool = poolFor(cfg);
    try {
      const { app } = await createApp(cfg,pool);
      const server = app.listen(cfg.port,cfg.mode === 'local' ? '127.0.0.1' : '0.0.0.0');
      const close = () => server.close(() => pool.end().then(() => process.exit(0)));
      process.once('SIGTERM',close); process.once('SIGINT',close);
    } catch(error) { await pool.end(); throw error; }
  })().catch(error => { if(error.code==='PORTAL_SETUP_PENDING')console.error('Portal setup pending: approved unexpired demo accounts required.'); console.error('Portal startup refused. Check explicit configuration and initialized synthetic database.'); process.exitCode=1; });
}
module.exports = { createApp };
