'use strict';
const express=require('express'),session=require('express-session'),bcrypt=require('bcryptjs');
const PgSession=require('connect-pg-simple')(session);
const {createHmac}=require('node:crypto');
const path=require('node:path');
const {createDashboardRouter}=require('../../routes/signalDashboard');
const LOGIN=`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#0c0e14"><title>Signal · Sign in</title><link rel="stylesheet" href="/signal-positions.css"></head><body class="login-page"><main><a class="brand" href="/login"><span class="brand-mark" aria-hidden="true">s</span><span>signal<span class="brand-dot">.</span></span></a><section class="panel"><p class="eyebrow">YOUR TRADING, IN FOCUS</p><h1>Welcome back.</h1><p class="subtitle">A clearer view of your money, your moves, and what Signal sees.</p><form method="post" action="/login"><label>Email<input name="email" type="email" autocomplete="username" placeholder="Your Pulseforge email" required></label><label>Password<input name="password" type="password" autocomplete="current-password" placeholder="Your Pulseforge password" required></label><button class="primary-button" type="submit">Sign in to Signal ↗</button></form><p class="data-note">Use your existing Pulseforge account.</p></section><p class="login-footer">Read-only intelligence. Your wallet stays yours.<br>Trade execution remains in FOMO.</p></main></body></html>`;
async function resolveOwner(pool,env){
 const email=String(env.SIGNAL_DASHBOARD_OWNER_EMAIL||'').trim().toLowerCase(),configuredId=env.SIGNAL_DASHBOARD_OWNER_USER_ID;
 if(!email&&!/^\d+$/.test(configuredId||''))throw Error('signal_owner_identity_required');
 const r=await pool.query(`SELECT id,email,role,client_id,active,password_change_required FROM users WHERE ($1::text<>'' AND lower(email)=$1) OR ($2::text IS NOT NULL AND id::text=$2)`,[email,configuredId||null]);
 const users=r.rows.filter(u=>u.active&&['admin','manager'].includes(u.role)&&(!email||String(u.email).toLowerCase()===email)&&(!configuredId||String(u.id)===configuredId));
 if(users.length!==1)throw Error('signal_owner_identity_unresolved');const owner=users[0];
 if(owner.role==='manager'&&String(owner.client_id)!==String(env.SIGNAL_DASHBOARD_TENANT_ID))throw Error('signal_owner_workspace_mismatch');return owner;
}
function createSignalApp({pool,service,config,sessionSecret,secureCookies=true,sessionStore,coreAppUrl=null,now=()=>new Date(),ready=()=>true}){
 if(typeof sessionSecret!=='string'||sessionSecret.length<32)throw Error('signal_session_secret_required');
 const app=express();app.disable('x-powered-by');app.set('trust proxy',1);
 app.use((_req,res,next)=>{res.set('Cache-Control','no-store');res.set('X-Content-Type-Options','nosniff');res.set('Referrer-Policy','same-origin');res.set('X-Frame-Options','DENY');next();});
 app.get('/health',(_req,res)=>res.status(ready()?200:503).json({service:'signal-dashboard',ready:ready(),readOnly:true}));
 app.use(express.urlencoded({extended:false,limit:'8kb'}));app.use(express.json({limit:'8kb'}));
 app.use(session({name:'signal.sid',store:sessionStore||new PgSession({pool,tableName:'signal_dashboard_session',createTableIfMissing:false,pruneSessionInterval:900}),
  secret:sessionSecret,resave:false,saveUninitialized:false,cookie:{httpOnly:true,secure:secureCookies,sameSite:'strict',maxAge:12*3600000}}));
 const originOK=req=>req.get('origin')===`${req.protocol}://${req.get('host')}`;
 const publicFiles={'/signal-positions.css':'signal-positions.css','/signal-positions.js':'signal-positions.js','/signal-positions-chart.js':'signal-positions-chart.js'};
 for(const [route,file]of Object.entries(publicFiles))app.get(route,(_req,res)=>res.sendFile(path.resolve(__dirname,'../../public',file)));
 app.get('/login',(_req,res)=>res.type('html').send(LOGIN));
 app.post('/login',async(req,res)=>{
  if(!originOK(req))return res.status(403).send('Sign-in origin denied.');
  try{
   const key=createHmac('sha256',sessionSecret).update(req.ip||'unknown').digest('hex');
   const gate=await pool.query(`INSERT INTO signal_dashboard_auth_attempts(ip_hash,attempts,reset_at) VALUES($1,1,clock_timestamp()+interval '15 minutes')
    ON CONFLICT(ip_hash) DO UPDATE SET attempts=CASE WHEN signal_dashboard_auth_attempts.reset_at<=clock_timestamp() THEN 1 ELSE signal_dashboard_auth_attempts.attempts+1 END,
    reset_at=CASE WHEN signal_dashboard_auth_attempts.reset_at<=clock_timestamp() THEN clock_timestamp()+interval '15 minutes' ELSE signal_dashboard_auth_attempts.reset_at END RETURNING attempts`,[key]);
   if(gate.rows[0].attempts>10)return res.status(429).send('Too many sign-in attempts. Try again later.');
   const email=typeof req.body?.email==='string'?req.body.email.trim().toLowerCase():'',password=typeof req.body?.password==='string'?req.body.password:'';
   if(email.length>254||password.length>1024)return res.status(400).send('Invalid sign-in.');
   const r=await pool.query('SELECT id,email,name,password_hash,role,client_id,active,password_change_required FROM users WHERE id=$1 AND lower(email)=$2',[config.ownerUserId,email]);
   const u=r.rows[0];if(!u?.active||!['admin','manager'].includes(u.role)||!await bcrypt.compare(password,u.password_hash))return res.status(401).send('Sign-in failed.');
   if(u.password_change_required)return res.status(403).send('Change your password in Pulseforge before using Signal.');
   if(u.role==='manager'&&String(u.client_id)!==config.tenantId)return res.status(403).send('Workspace access denied.');
   await new Promise((resolve,reject)=>req.session.regenerate(e=>e?reject(e):resolve()));
   req.session.user={id:u.id,email:u.email,name:u.name,role:u.role,client_id:u.client_id,password_change_required:false};req.session.active_client_id=config.tenantId;
   req.session.passwordVersion=createHmac('sha256',sessionSecret).update(u.password_hash).digest('hex');
   await new Promise((resolve,reject)=>req.session.save(e=>e?reject(e):resolve()));
   return res.redirect('/signal-v1/positions');
  }catch{return res.status(503).send('Sign-in is temporarily unavailable.');}
 });
 app.post('/logout',async(req,res)=>{if(!originOK(req))return res.sendStatus(403);await new Promise(resolve=>req.session.destroy(()=>resolve()));res.clearCookie('signal.sid');res.redirect('/login');});
 const requireAuth=async(req,res,next)=>{
  if(!req.session.user)return req.path.startsWith('/api/')?res.status(401).json({error:'Unauthorized'}):res.redirect('/login');
  try{
   const r=await pool.query('SELECT id,password_hash,role,client_id,active,password_change_required FROM users WHERE id=$1',[req.session.user.id]);const u=r.rows[0];
   if(!u?.active||u.password_change_required||!['admin','manager'].includes(u.role)||String(u.id)!==config.ownerUserId||u.role==='manager'&&String(u.client_id)!==config.tenantId
    ||req.session.passwordVersion!==createHmac('sha256',sessionSecret).update(u.password_hash).digest('hex'))return res.status(403).json({error:'signal_session_revoked'});
   req.session.user={...req.session.user,role:u.role,client_id:u.client_id};req.user=req.session.user;return next();
  }catch{return res.status(503).json({error:'signal_auth_unavailable'});}
 };
 const requireRole=(...roles)=>(req,res,next)=>roles.includes(req.user?.role)?next():res.sendStatus(403);
 app.get('/signal-v1',requireAuth,(_req,res)=>{let target='/signal-v1/positions';try{const u=new URL(coreAppUrl);if(u.protocol==='https:')target=new URL('/signal-v1',u).href;}catch{}return res.redirect(target);});
 app.get('/',requireAuth,(_req,res)=>res.redirect('/signal-v1/positions'));
 app.use(createDashboardRouter({Router:express.Router,requireAuth,requireRole,getService:()=>service,getConfig:()=>config}));
 app.use((_req,res)=>res.status(404).json({error:'not_found'}));return app;
}
module.exports={createSignalApp,resolveOwner};
