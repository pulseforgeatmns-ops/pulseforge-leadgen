'use strict';
const path=require('node:path');
const {config}=require('../services/signalDashboard/config');
const {isValidSolanaAddress:address}=require('../packages/signal-v1/acquisition/solanaAddress');
function authorize(req,c) {
  if(!c.enabled)return false;
  const user=req.session?.user;
  return !req.session?.impersonation?.active&&['admin','manager'].includes(user?.role)&&String(user.id)===c.ownerUserId
    &&String(user.client_id??req.session.active_client_id)===c.tenantId
    &&!Object.keys(req.query||{}).some(k=>['tenantId','tenant_id','wallet','namespace','userId'].includes(k));
}
function createDashboardRouter({Router,requireAuth,requireRole,getService,getConfig=config}={}) {
  const router=Router(),guard=[requireAuth,requireRole('admin','manager')];
  const handle=fn=>async(req,res)=>{
    res.set('Cache-Control','no-store');res.set('X-Content-Type-Options','nosniff');
    let c;try{c=getConfig();}catch{return res.status(503).json({error:'dashboard_not_configured'});}
    if(!c.enabled)return res.status(503).json({error:'dashboard_disabled'});
    if(!authorize(req,c))return res.status(403).json({error:'dashboard_scope_denied'});
    if(req.params.ca&&!address(req.params.ca))return res.status(400).json({error:'invalid_token_address'});
    try{return await fn(req,res);}catch{return res.status(503).json({error:'dashboard_data_unavailable'});}
  };
  for(const route of ['/signal-v1/positions','/signal-v1/positions/:ca'])router.get(route,...guard,handle((_req,res)=>res.sendFile(path.resolve(__dirname,'../public/signal-positions.html'))));
  router.get('/api/v1/signal/positions',...guard,handle(async(_req,res)=>res.json(await getService().positions())));
  router.get('/api/v1/signal/opportunities',...guard,handle(async(_req,res)=>res.json(await getService().discovery())));
  router.get('/api/v1/signal/opportunities/:ca/history',...guard,handle(async(req,res)=>{
    const service=getService();return res.json({decisions:service.opportunities?await service.opportunities.history(req.params.ca):[],readOnly:true});
  }));
  router.get('/api/v1/signal/positions/:ca',...guard,handle(async(req,res)=>{
    const p=await getService().detail(req.params.ca);return p?res.json(p):res.status(404).json({error:'position_not_found'});
  }));
  router.get('/api/v1/signal/positions/:ca/zones',...guard,handle(async(req,res)=>{
    const service=getService();if(!await service.detail(req.params.ca))return res.status(404).json({error:'position_not_found'});return res.json(await service.zones(req.params.ca));
  }));
  router.get('/api/v1/signal/positions/:ca/zones/:zoneId',...guard,handle(async(req,res)=>{
    const service=getService();if(!/^[a-f0-9]{64}$/.test(req.params.zoneId))return res.status(400).json({error:'invalid_zone_id'});
    if(!await service.detail(req.params.ca))return res.status(404).json({error:'position_not_found'});
    const events=service.zoneStore?await service.zoneStore.history(req.params.ca,req.params.zoneId):[];return events.length?res.json({events,mode:'PAPER_OBSERVATION',validated:false}):res.status(404).json({error:'zone_not_found'});
  }));
  router.post('/api/v1/signal/positions/:ca/zones/:zoneId/override',...guard,handle(async(req,res)=>{
    const origin=req.get('origin');if(!origin||origin!==`${req.protocol}://${req.get('host')}`||!req.is('application/json'))return res.status(403).json({error:'zone_override_origin_denied'});
    if(!/^[a-f0-9]{64}$/.test(req.params.zoneId)||!req.body||Object.keys(req.body).some(k=>!['lower','upper'].includes(k)))return res.status(400).json({error:'invalid_zone_override'});
    const lower=Number(req.body.lower),upper=Number(req.body.upper);if(!Number.isFinite(lower)||!Number.isFinite(upper)||lower<=0||upper<=lower)return res.status(400).json({error:'invalid_zone_bounds'});
    try{return res.status(201).json({zone:await getService().overrideZone(req.params.ca,req.params.zoneId,{lower,upper},req.session.user.id)});}catch(e){return res.status(e.message==='zone_not_found'?404:400).json({error:e.message});}
  }));
  for(const kind of ['candles','events','transactions','risk'])router.get(`/api/v1/signal/positions/:ca/${kind}`,...guard,handle(async(req,res)=>{
    const service=getService(),p=await service.detail(req.params.ca);if(!p)return res.status(404).json({error:'position_not_found'});
    if(kind==='candles'){
      const interval=req.query.interval||'1m';if(!['1m','5m','15m','1h','4h'].includes(interval))return res.status(400).json({error:'invalid_interval'});
      return res.json(await service.candles(req.params.ca,interval));
    }
    if(kind==='transactions')return res.json({transactions:await service.transactions(req.params.ca),asOf:p.asOf});
    return res.json({[kind]:p[kind],asOf:p.asOf});
  }));
  // Bounded polling transport: no long-lived connections or per-client provider requests.
  router.get('/api/v1/signal/stream/:ca',...guard,handle(async(req,res)=>{
    const p=await getService().detail(req.params.ca);return p?res.json({...p,transport:'bounded-polling'}):res.status(404).json({error:'position_not_found'});
  }));
  router.post('/api/v1/signal/positions/:ca/annotations',...guard,handle(async(req,res)=>{
    // Mandatory same-origin check, JSON only. Only note text is accepted.
    const origin=req.get('origin');
    if(!origin||origin!==`${req.protocol}://${req.get('host')}`||!req.is('application/json'))return res.status(403).json({error:'annotation_origin_denied'});
    if(!req.body||Object.keys(req.body).some(k=>k!=='text')||typeof req.body.text!=='string'||!req.body.text.trim()||req.body.text.length>2000)return res.status(400).json({error:'invalid_annotation'});
    return res.status(201).json({annotation:await getService().annotate(req.params.ca,req.body.text,req.session.user.id)});
  }));
  return router;
}
module.exports={createDashboardRouter,authorize};
